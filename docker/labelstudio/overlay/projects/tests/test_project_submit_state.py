"""TDD Red — 갭 A: GET /api/projects/{id}/submit-state (GCP 외주 LS 포크 전용, 자체완결형).

배경: F1 프론트 SubmitButtonWrapper 가 마운트 시 프로젝트의 finalize 상태를
fetch 해서 버튼 라벨/상태(제출됨 / 재제출)를 결정해야 한다. 기존 POST submit 은
있으나(ProjectSubmitAPI), 상태를 조회하는 GET 엔드포인트가 없다.

검증하려는 목표 동작:
  1) 권한(격리 유지):
       - role='labeler'  → 403 (submit 계열은 검수자 기능, 노출/조회 금지)
       - role='reviewer' → 200
       - role='admin'    → 200
  2) 첫 조회(미제출): is_submitted=false, submitted_at 은 없음/None.
  3) submit 후 조회: is_submitted=true 로 반영, submitted_at/submitted_by 채워짐.
  4) 응답 스키마: is_submitted(bool), submitted_at, submitted_by 키를 항상 포함.

현재 구현 상태 (Red 근거):
  - projects/urls.py 에 submit-state 라우트 없음, projects/api.py 에 뷰 없음
    → 어느 역할이 GET 해도 404. 기대(200/403)와 불일치하여 RED.

주의(구현 금지 준수):
  - 상태를 만드는 데는 이미 존재하는 POST /submit 엔드포인트만 사용(구현 추가 없음).

green 단계 예상 최소 구현:
  - 신규 뷰 GET /api/projects/{id}/submit-state (projects/api.py) + urls 라우트.
  - permission_classes = [IsAuthenticated, DenyLabelers] (labeler 403).
  - 응답: is_submitted=project.is_submitted, submitted_at=project.submitted_at,
    submitted_by=최근 label_state_history submit actor(있으면).
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestProjectSubmitState(APITestCase):
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
    def _state_url(self):
        return f'/api/projects/{self.project.id}/submit-state'

    def _submit_url(self):
        return f'/api/projects/{self.project.id}/submit'

    def _get_state(self, user):
        self.client.force_authenticate(user=user)
        return self.client.get(self._state_url())

    def _do_submit(self, user):
        """이미 존재하는 POST /submit 로 상태만 만든다(신규 구현 아님)."""
        self.client.force_authenticate(user=user)
        return self.client.post(self._submit_url(), {}, format='json')

    # ------------------------------------------------------------------ #
    # 1) 권한
    # ------------------------------------------------------------------ #
    def test_labeler_submit_state_of_unassigned_project_is_404(self):
        """정책(2026-07-07): submit-state 는 읽기라 labeler 도 허용하되 **접근 가능한 프로젝트로 스코프**.

        labeler 가 배정 태스크 없는 프로젝트의 submit-state 조회 → 404(접근 불가).
        (배정된 프로젝트는 200 — 프론트 SubmitButton 래퍼 마운트 fetch 용. submit POST 는 DenyLabelers 로 여전히 차단.)
        """
        response = self._get_state(self.labeler)
        assert response.status_code == 404, (
            f'labeler 는 배정 없는 프로젝트 submit-state 에서 404(스코프)여야 함, 실제 status={response.status_code}'
        )

    def test_reviewer_can_read_submit_state(self):
        """[RED 핵심] reviewer 는 submit-state 조회가 허용되어 200 을 받는다."""
        response = self._get_state(self.reviewer)
        assert response.status_code == 200, (
            f'reviewer 는 submit-state 조회(200)돼야 함, 실제 status={response.status_code}'
        )

    def test_admin_can_read_submit_state(self):
        """admin 은 submit-state 조회가 허용되어 200 을 받는다."""
        response = self._get_state(self.admin)
        assert response.status_code == 200, (
            f'admin 은 submit-state 조회(200)돼야 함, 실제 status={response.status_code}'
        )

    # ------------------------------------------------------------------ #
    # 2) 미제출 상태 조회
    # ------------------------------------------------------------------ #
    def test_first_read_reports_not_submitted(self):
        """[RED 핵심] 미제출 프로젝트는 is_submitted=false, submitted_at 미설정."""
        response = self._get_state(self.reviewer)
        assert response.status_code == 200, response.content
        body = response.json()
        assert 'is_submitted' in body, f'응답에 is_submitted 키가 있어야 함: {body}'
        assert body['is_submitted'] is False, f'미제출이면 is_submitted=False 여야 함: {body}'
        assert body.get('submitted_at') in (None, ''), (
            f'미제출이면 submitted_at 은 비어 있어야 함: {body}'
        )

    # ------------------------------------------------------------------ #
    # 3) submit 후 상태 반영
    # ------------------------------------------------------------------ #
    def test_state_reflects_true_after_submit(self):
        """[RED 핵심] submit 후 조회하면 is_submitted=true + submitted_at/submitted_by 반영."""
        submit_resp = self._do_submit(self.reviewer)
        assert submit_resp.status_code == 200, submit_resp.content
        response = self._get_state(self.reviewer)
        assert response.status_code == 200, response.content
        body = response.json()
        assert body.get('is_submitted') is True, f'submit 후 is_submitted=True 여야 함: {body}'
        assert body.get('submitted_at'), f'submit 후 submitted_at 이 채워져야 함: {body}'
        assert body.get('submitted_by'), f'submit 후 submitted_by 가 채워져야 함: {body}'

    # ------------------------------------------------------------------ #
    # 4) 프론트 계약(SubmitButtonWrapper) — is_finalized/last_finalized_* 필드
    #    (구 반환은 is_submitted/submitted_at 만 있어 프론트가 상태를 못 읽어 버튼이
    #     'Re-submit' 로 안 바뀌던 버그 회귀 방지)
    # ------------------------------------------------------------------ #
    def test_response_uses_frontend_contract_fields(self):
        # 미제출: is_finalized=False, changed_task_count 키 존재
        body = self._get_state(self.reviewer).json()
        assert body.get('is_finalized') is False, f'미제출이면 is_finalized=False: {body}'
        assert 'last_finalized_at' in body and 'last_finalized_by' in body, f'프론트 필드 누락: {body}'
        assert 'changed_task_count' in body, f'changed_task_count 키 필요: {body}'
        # submit 후: is_finalized=True, last_finalized_at/by 채워짐
        self._do_submit(self.reviewer)
        body = self._get_state(self.reviewer).json()
        assert body.get('is_finalized') is True, f'submit 후 is_finalized=True: {body}'
        assert body.get('last_finalized_at'), f'submit 후 last_finalized_at 채워져야: {body}'
        assert body.get('last_finalized_by'), f'submit 후 last_finalized_by 채워져야: {body}'
