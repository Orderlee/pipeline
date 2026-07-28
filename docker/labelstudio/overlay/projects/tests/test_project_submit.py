"""TDD Red — F1 Project Submit / Re-submit (GCP 외주 LS 포크 전용, 자체완결형).

이번 사이클 범위: 신규 엔드포인트 **POST /api/projects/{id}/submit** 한정.

①(label_studio_customizing)의 F1 은 파이프라인 결합형이었다
  (finalize_project → Dagster trigger / staging PG / ls-webhook, to_state='finalized').
이 GCP 포크는 **격리·자체완결형** — 파이프라인 훅이 전혀 없다.
Submit = "로컬 프로젝트 완료 상태 마킹 + 감사기록(label_state_history)" 뿐.

검증하려는 목표 동작:
  1) 권한:
       - role='labeler'  → 403 (submit 불가, 격리 유지)
       - role='reviewer' → 허용(200, 중간검수)
       - role='admin'    → 허용(200, 최종검수)
       - 비인증          → 401/403
  2) 첫 Submit: reviewer POST → 200, 응답 메타에
       is_resubmit=False, submitted_at, submitted_by 포함. 프로젝트가 submitted 상태로 마킹.
  3) Re-submit(멱등·soft): 같은 프로젝트 두 번째 submit → 200, is_resubmit=True.
       에러 없이 멱등. 종착점 막지 않음(버튼 항상 활성 정책).
  4) 감사 로그: 매 submit 마다 label_state_history 테이블에 1 row 추가
       (project, user/actor, action ∈ {submitted, resubmitted}, timestamp).
  5) 자체완결형: 외부 호출(Dagster/webhook/파이프라인 PG) 없음 — 로컬 DB 상태만 변경.

현재 구현 상태 (Red 근거):
  - projects/urls.py 에 submit 라우트가 없고, projects/api.py 에 뷰가 없다
    → 어느 역할이 POST 해도 404. 기대(200/403/401)와 불일치하여 RED.
  - label_state_history 테이블/모델이 없다 → 감사로그 조회가 실패(테이블 부재)하여 RED.

주의(구현 금지 준수):
  - 아직 없는 모델을 top-level import 하면 수집 단계 ImportError 로 전체가 깨진다.
    그러면 실패 사유가 "구현 부재" 가 아니라 "테스트 결함" 이 되어 부적격.
  - 그래서 label_state_history 는 django.db 연결의 raw SQL(테이블명 'label_state_history')
    로만 접근하고, 테이블 부재는 각 테스트 안에서 명확한 실패 메시지로 처리한다.

green 단계 예상 최소 구현:
  - 신규 모델 label_state_history (db_table='label_state_history'):
        project FK, user/actor, action(char, submitted/resubmitted), created_at + 마이그레이션.
  - 프로젝트 submit 상태 (Project 필드 추가 또는 별도 모델) — 최소 설계로.
  - 신규 뷰 POST /api/projects/{id}/submit (projects/api.py) + projects/urls.py 라우트.
  - 권한: labeler 차단(reviewer/admin/owner 허용). 미들웨어 allowlist 미등재로 이중 차단.
  - 외부 호출 없음.
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


class TestProjectSubmit(APITestCase):
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
        # 태스크 명세의 문자 그대로: POST /api/projects/{id}/submit (trailing slash 없음)
        return f'/api/projects/{self.project.id}/submit'

    def _submit(self, user):
        self.client.force_authenticate(user=user)
        return self.client.post(self._url(), {}, format='json')

    def _history_count(self, action=None):
        """label_state_history 에서 이 프로젝트의 row 수. 테이블 부재 시 명확히 실패시킨다."""
        sql = f'SELECT COUNT(*) FROM {HISTORY_TABLE} WHERE project_id = %s'
        params = [self.project.id]
        if action is not None:
            sql += ' AND action = %s'
            params.append(action)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone()[0]
        except db_utils.OperationalError as exc:
            self.fail(
                f"감사로그 조회 실패 — '{HISTORY_TABLE}' 테이블이 없음(구현 부재): {exc}. "
                f'green 단계가 label_state_history 모델+마이그레이션을 만들어야 함.'
            )

    # ------------------------------------------------------------------ #
    # 1) 권한
    # ------------------------------------------------------------------ #
    def test_labeler_cannot_submit_project(self):
        """[RED 핵심] labeler 는 submit 에서 403 을 받아야 한다(격리 유지)."""
        response = self._submit(self.labeler)
        assert response.status_code == 403, (
            f'labeler 는 submit 차단(403)돼야 함, 실제 status={response.status_code} '
            f'(엔드포인트 미구현 시 404)'
        )

    def test_anonymous_cannot_submit_project(self):
        """[RED] 비인증 요청은 submit 에서 401/403 이어야 한다."""
        response = self.client.post(self._url(), {}, format='json')
        assert response.status_code in (401, 403), (
            f'비인증 submit 은 401/403 이어야 함, 실제 status={response.status_code}'
        )

    def test_admin_can_submit_project(self):
        """admin(최종검수) 은 submit 이 허용되어 200 을 받는다."""
        response = self._submit(self.admin)
        assert response.status_code == 200, (
            f'admin 은 submit 허용(200)돼야 함, 실제 status={response.status_code}'
        )

    # ------------------------------------------------------------------ #
    # 2) 첫 Submit — reviewer, is_resubmit=False + 메타
    # ------------------------------------------------------------------ #
    def test_reviewer_first_submit_returns_200_and_meta(self):
        """[RED 핵심] reviewer 의 첫 submit → 200 + is_resubmit=False + submitted_at/submitted_by."""
        response = self._submit(self.reviewer)
        assert response.status_code == 200, (
            f'reviewer 의 첫 submit 은 200 이어야 함, 실제 status={response.status_code}'
        )
        body = response.json()
        assert body.get('is_resubmit') is False, f'첫 submit 은 is_resubmit=False 여야 함: {body}'
        assert body.get('submitted_at'), f'응답에 submitted_at 메타가 있어야 함: {body}'
        assert body.get('submitted_by'), f'응답에 submitted_by 메타가 있어야 함: {body}'

    # ------------------------------------------------------------------ #
    # 3) Re-submit — 멱등·soft, is_resubmit=True
    # ------------------------------------------------------------------ #
    def test_resubmit_is_idempotent_and_flags_resubmit(self):
        """[RED 핵심] 두 번째 submit → 200 + is_resubmit=True (에러 없이 멱등, 종착점 안 막음)."""
        first = self._submit(self.reviewer)
        assert first.status_code == 200, (
            f'첫 submit 은 200 이어야 함, 실제 status={first.status_code}'
        )
        second = self._submit(self.reviewer)
        assert second.status_code == 200, (
            f'재submit 도 200(멱등)이어야 함, 실제 status={second.status_code}'
        )
        assert second.json().get('is_resubmit') is True, (
            f'두 번째 submit 은 is_resubmit=True 여야 함: {second.json()}'
        )

    # ------------------------------------------------------------------ #
    # 4) 감사 로그 — label_state_history
    # ------------------------------------------------------------------ #
    def test_first_submit_appends_submitted_history_row(self):
        """[RED 핵심] 첫 submit 시 label_state_history 에 action='submitted' row 1건 추가."""
        assert self._history_count() == 0, '사전 상태: 감사로그가 비어 있어야 함'
        response = self._submit(self.reviewer)
        assert response.status_code == 200, response.content
        assert self._history_count(action='submitted') == 1, (
            "첫 submit 후 label_state_history 에 action='submitted' row 1건이 있어야 함"
        )

    def test_resubmit_appends_second_history_row(self):
        """[RED] 재submit 시 label_state_history 에 두 번째 row(action='resubmitted') 추가 → 총 2건."""
        self._submit(self.reviewer)
        self._submit(self.reviewer)
        assert self._history_count() == 2, (
            '첫 submit + 재submit 로 label_state_history row 가 총 2건이어야 함(멱등이어도 매 transition 기록)'
        )
        assert self._history_count(action='resubmitted') == 1, (
            "재submit 은 action='resubmitted' row 1건을 남겨야 함"
        )
