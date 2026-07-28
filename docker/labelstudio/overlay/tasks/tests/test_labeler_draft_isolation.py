"""TDD Red — 라벨러별 AnnotationDraft 접근 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **AnnotationDraftListAPI / AnnotationDraftAPI 의 assignee 스코핑** 한정.

대상 뷰 (label_studio/tasks/api.py):
  - AnnotationDraftListAPI (889): GET /api/tasks/{task_id}/drafts
  - AnnotationDraftAPI     (911): GET|PATCH|DELETE /api/drafts/{draft_pk}/

검증하려는 목표 동작:
  - role='labeler' 는 **본인 배정 태스크(assignee=self) 의 draft 만** 접근/수정/삭제 가능.
  - 타인 배정 태스크(task_b) 의 draft 목록/상세/수정/삭제 → 거부(404).
  - 본인 태스크(task_a) draft 는 정상(가드). reviewer/admin 은 격리 없음(가드).

현재 구현 상태 (Red 근거):
  - AnnotationDraftListAPI.queryset = AnnotationDraft.objects.all(), filter_queryset 은
    task_id 로만 필터 → assignee 스코핑 없음 → labeler_a 가 task_b 의 draft 목록을 200 으로 받음.
  - AnnotationDraftAPI.queryset = AnnotationDraft.objects.all(), get_queryset 오버라이드 없음
    → labeler_a 가 타인 draft 상세/수정/삭제에 성공 → 격리 테스트 실패(Red).

green 단계 예상 최소 구현:
  두 뷰의 queryset 을 OrganizationMember.scope_tasks_for_request 로 좁힌 태스크 기준
  (task__in=scoped_tasks) 으로 필터 → 타인 draft 면 404.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import AnnotationDraftFactory, TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

DENIED = (403, 404)


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerDraftIsolation(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.organization)

        cls.admin = cls.organization.created_by
        cls.reviewer = UserFactory(active_organization=cls.organization)
        cls.labeler_a = UserFactory(active_organization=cls.organization)
        cls.labeler_b = UserFactory(active_organization=cls.organization)

        set_role(cls.admin, cls.organization, ROLE_ADMIN)
        set_role(cls.reviewer, cls.organization, ROLE_REVIEWER)
        set_role(cls.labeler_a, cls.organization, ROLE_LABELER)
        set_role(cls.labeler_b, cls.organization, ROLE_LABELER)

        cls.task_a = TaskFactory(project=cls.project, data={'text': 'a'}, assignee=cls.labeler_a)
        cls.task_b = TaskFactory(project=cls.project, data={'text': 'b'}, assignee=cls.labeler_b)

        cls.draft_a = AnnotationDraftFactory(task=cls.task_a, user=cls.labeler_a)
        cls.draft_b = AnnotationDraftFactory(task=cls.task_b, user=cls.labeler_b)

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    # --- 1) 태스크별 draft 목록 격리 --------------------------------------------

    def test_labeler_cannot_list_drafts_of_foreign_task(self):
        """[RED] labeler_a 가 타인 태스크(task_b) draft 목록을 요청하면 거부/빈결과."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/tasks/{self.task_b.id}/drafts')
        if response.status_code in DENIED:
            return
        assert response.status_code == 200, response.content
        returned_ids = {row['id'] for row in response.json()}
        assert self.draft_b.id not in returned_ids, (
            f'labeler_a 가 타인 태스크 draft 목록을 조회함(격리 실패): {response.content}'
        )

    # --- 2) draft 상세 격리 ------------------------------------------------------

    def test_labeler_cannot_retrieve_foreign_draft(self):
        """[RED] labeler_a 가 타인 draft(draft_b) 상세를 요청하면 404."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/drafts/{self.draft_b.id}/')
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 draft 상세에 접근됨(격리 실패): status={response.status_code}, {response.content}'
        )

    # --- 3) draft 수정 격리 ------------------------------------------------------

    def test_labeler_cannot_update_foreign_draft(self):
        """[RED] labeler_a 가 타인 draft(draft_b) 수정을 시도하면 거부(404)."""
        self._auth(self.labeler_a)
        response = self.client.patch(
            f'/api/drafts/{self.draft_b.id}/', data={'result': []}, format='json'
        )
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 draft 수정에 성공함(격리 실패): status={response.status_code}, {response.content}'
        )

    # --- 4) draft 삭제 격리 ------------------------------------------------------

    def test_labeler_cannot_delete_foreign_draft(self):
        """[RED] labeler_a 가 타인 draft(draft_b) 삭제를 시도하면 거부(404)."""
        self._auth(self.labeler_a)
        response = self.client.delete(f'/api/drafts/{self.draft_b.id}/')
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 draft 삭제에 성공함(격리 실패): status={response.status_code}, {response.content}'
        )

    # --- 가드: 본인 draft 는 정상 -----------------------------------------------

    def test_labeler_can_retrieve_own_draft(self):
        """[가드] labeler_a 는 본인 draft(draft_a) 상세를 200 으로 받는다."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/drafts/{self.draft_a.id}/')
        assert response.status_code == 200, response.content
        assert response.json()['id'] == self.draft_a.id

    # --- 가드: reviewer / admin 은 격리 없음 ------------------------------------

    def test_reviewer_can_access_any_draft(self):
        """[가드] reviewer 는 타인 draft 상세에 접근 가능(200)."""
        self._auth(self.reviewer)
        response = self.client.get(f'/api/drafts/{self.draft_b.id}/')
        assert response.status_code == 200, response.content

    def test_admin_can_access_any_draft(self):
        """[가드] admin 은 타인 draft 상세에 접근 가능(200)."""
        self._auth(self.admin)
        response = self.client.get(f'/api/drafts/{self.draft_b.id}/')
        assert response.status_code == 200, response.content
