"""TDD Red — 프로젝트 완료 되돌림(반려) API (GCP 외주 LS 포크 전용, 자체완결형).

이번 사이클 범위: 신규 엔드포인트 **POST /api/projects/{id}/reject** 한정.

배경:
  - F1 Submit(POST /api/projects/{id}/submit)은 이미 구현됨(admin/reviewer 모두 허용).
  - Reject 는 그 반대 방향 전이 = "제출 완료를 되돌린다".
  - Submit 과 달리 **admin(최종검수자) 전용** — reviewer 는 되돌릴 수 없다(403).

검증하려는 목표 동작:
  1) admin 이 제출된(is_submitted=True) 프로젝트를 note 와 함께 reject
       → 200, project.is_submitted=False 로 복귀,
         label_state_history 에 action='rejected' + note=사유 row 1건(actor=admin).
  2) [핵심] reviewer reject → 403 (submit 은 reviewer 허용이나 reject 는 admin 전용).
  3) labeler reject → 403 (격리 유지).
  4) 비인증 reject → 401/403.
  5) note 가 비었거나 없으면 → 400 (사유 필수).

현재 구현 상태 (Red 근거):
  - projects/urls.py 에 reject 라우트가 없고 projects/api.py 에 뷰가 없다
    → 어느 역할이 POST 해도 404. 기대(200/403/401/400)와 불일치하여 RED.
  - LabelStateHistory 에 note 필드가 없고 Action 에 'rejected' 가 없다
    → 감사로그(note 포함) 조회가 컬럼 부재로 실패하여 RED.

주의(구현 금지 준수):
  - 아직 없는 필드/컬럼을 ORM 어트리뷰트로 top-level 참조하면 수집 에러가 날 수 있으므로,
    label_state_history 의 note 컬럼은 raw SQL 로만 접근하고 컬럼 부재는 각 테스트 안에서
    명확한 실패 메시지로 처리한다(실패 사유 = 구현 부재).

green 단계 예상 최소 구현:
  - LabelStateHistory 에 note 필드 추가 + Action.REJECTED('rejected') 추가 + 마이그레이션.
  - 신규 뷰 POST /api/projects/{id}/reject (projects/api.py) + projects/urls.py 라우트.
  - 권한: admin 전용(reviewer/labeler/비인증 차단). note 필수 검증(빈/누락 → 400).
  - is_submitted=True → False 로 되돌리고 감사 row(action='rejected', note, actor) 기록.
"""
from django.db import connection, utils as db_utils
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

HISTORY_TABLE = 'label_state_history'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestProjectReject(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.organization)

        cls.admin = cls.organization.created_by
        cls.reviewer = UserFactory(active_organization=cls.organization)
        cls.labeler = UserFactory(active_organization=cls.organization)

        set_role(cls.admin, cls.organization, ROLE_ADMIN)
        set_role(cls.reviewer, cls.organization, ROLE_REVIEWER)
        set_role(cls.labeler, cls.organization, ROLE_LABELER)

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    def _url(self):
        # 명세 문자 그대로: POST /api/projects/{id}/reject (trailing slash 없음, submit 관례 동일)
        return f'/api/projects/{self.project.id}/reject'

    def _mark_submitted(self):
        """되돌림 대상 상태를 만든다: 프로젝트를 제출 완료로 마킹(ORM)."""
        self.project.is_submitted = True
        self.project.save(update_fields=['is_submitted'])

    def _reject(self, user, note='검수 반려 사유'):
        self.client.force_authenticate(user=user)
        body = {} if note is None else {'note': note}
        return self.client.post(self._url(), body, format='json')

    def _rejected_history_count(self, note=None):
        """label_state_history 에서 이 프로젝트의 action='rejected' row 수.

        note 필터를 주면 note 컬럼까지 조회한다. note 컬럼/테이블 부재 시(구현 부재)
        명확한 메시지로 실패시켜 RED 사유를 '구현 부재' 로 고정한다.
        """
        sql = f"SELECT COUNT(*) FROM {HISTORY_TABLE} WHERE project_id = %s AND action = 'rejected'"
        params = [self.project.id]
        if note is not None:
            sql += ' AND note = %s'
            params.append(note)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone()[0]
        except db_utils.OperationalError as exc:
            self.fail(
                f"감사로그 조회 실패 — '{HISTORY_TABLE}' 의 rejected/note 컬럼이 없음(구현 부재): {exc}. "
                f"green 단계가 LabelStateHistory.note 필드 + Action.REJECTED + 마이그레이션을 만들어야 함."
            )

    # ------------------------------------------------------------------ #
    # 1) admin reject(note) → 200 + 되돌림 + 감사 row
    # ------------------------------------------------------------------ #
    def test_admin_reject_reverts_submit_and_appends_history(self):
        """[RED 핵심] admin 이 제출된 프로젝트를 note 와 함께 reject → 200 + is_submitted=False + rejected row."""
        self._mark_submitted()
        note = '라벨 품질 미달 — 재작업 요망'
        response = self._reject(self.admin, note=note)
        assert response.status_code == 200, (
            f'admin reject 는 200 이어야 함, 실제 status={response.status_code} (엔드포인트 미구현 시 404)'
        )

        self.project.refresh_from_db()
        assert self.project.is_submitted is False, (
            f'reject 후 is_submitted 는 False 여야 함, 실제={self.project.is_submitted}'
        )
        assert self._rejected_history_count(note=note) == 1, (
            "reject 후 label_state_history 에 action='rejected' + note 일치 row 1건이 있어야 함"
        )

    # ------------------------------------------------------------------ #
    # 2) reviewer reject → 403 (submit 과 다르게 admin 전용)
    # ------------------------------------------------------------------ #
    def test_reviewer_cannot_reject_project(self):
        """[RED 핵심] reviewer 는 reject 에서 403 — submit 은 허용이나 reject 는 admin 전용."""
        self._mark_submitted()
        response = self._reject(self.reviewer)
        assert response.status_code == 403, (
            f'reviewer reject 는 차단(403)돼야 함, 실제 status={response.status_code} '
            f'(엔드포인트 미구현 시 404)'
        )

    # ------------------------------------------------------------------ #
    # 3) labeler reject → 403
    # ------------------------------------------------------------------ #
    def test_labeler_cannot_reject_project(self):
        """[RED] labeler 는 reject 에서 403(격리 유지)."""
        self._mark_submitted()
        response = self._reject(self.labeler)
        assert response.status_code == 403, (
            f'labeler reject 는 차단(403)돼야 함, 실제 status={response.status_code}'
        )

    # ------------------------------------------------------------------ #
    # 4) 비인증 reject → 401/403
    # ------------------------------------------------------------------ #
    def test_anonymous_cannot_reject_project(self):
        """[RED] 비인증 요청은 reject 에서 401/403 이어야 한다."""
        self._mark_submitted()
        response = self.client.post(self._url(), {'note': 'x'}, format='json')
        assert response.status_code in (401, 403), (
            f'비인증 reject 은 401/403 이어야 함, 실제 status={response.status_code}'
        )

    # ------------------------------------------------------------------ #
    # 5) note 필수 — 없으면 400
    # ------------------------------------------------------------------ #
    def test_reject_without_note_returns_400(self):
        """[RED 핵심] note 가 없으면 reject 는 400(사유 필수)."""
        self._mark_submitted()
        response = self._reject(self.admin, note=None)
        assert response.status_code == 400, (
            f'note 없는 reject 은 400 이어야 함, 실제 status={response.status_code} '
            f'(엔드포인트 미구현 시 404)'
        )

    # ------------------------------------------------------------------ #
    # 가드 (독립 리뷰 MED-2): 미제출 반려 / note 타입
    # ------------------------------------------------------------------ #
    def test_reject_when_not_submitted_returns_400(self):
        """제출되지 않은(is_submitted=False) 프로젝트 반려는 400 — 되돌릴 것이 없음(허위 이력 방지)."""
        # _mark_submitted 하지 않음 → is_submitted=False 상태
        self.client.force_authenticate(user=self.admin)
        response = self.client.post(self._url(), {'note': '되돌릴 것 없음'}, format='json')
        assert response.status_code == 400, (
            f'미제출 프로젝트 반려는 400 이어야 함, 실제={response.status_code}'
        )

    def test_reject_nonstring_note_returns_400_not_500(self):
        """note 가 문자열이 아니면 400(서버오류 500 아님)."""
        self._mark_submitted()
        self.client.force_authenticate(user=self.admin)
        response = self.client.post(self._url(), {'note': 123}, format='json')
        assert response.status_code == 400, (
            f'비문자열 note 는 400 이어야 함(500 금지), 실제={response.status_code}'
        )
