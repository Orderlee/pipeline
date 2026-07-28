"""TDD Red — 라벨러 draft 생성(POST) 부모 태스크 소유 검증 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **AnnotationDraftListAPI.perform_create 의 부모 태스크 소유 스코핑** 한정.

대상 뷰 (label_studio/tasks/api.py:908):
  - AnnotationDraftListAPI.perform_create
      POST /api/tasks/{pk}/drafts                         (url 'tasks:api:task-drafts')
      POST /api/tasks/{pk}/annotations/{aid}/drafts       (url 'tasks:api:task-annotations-drafts')
      → serializer.save(task_id=self.kwargs['pk'], ...) 로 URL kwarg 를 검증 없이 저장.

검증하려는 목표 동작:
  - role='labeler' 는 **본인 배정 태스크(assignee=self) 에만** draft 를 생성할 수 있다.
  - 타인 배정 태스크(task_b) 의 task_id 로 draft 생성 → 거부(403/404).
  - 본인 태스크(task_a) 생성은 정상(가드). reviewer 는 격리 없음(가드).

Red 근거 (현재 구현):
  - default-deny 미들웨어가 'tasks:api:task-drafts' POST 를 labeler 에게 허용하고,
    perform_create 는 URL 의 task_id 를 소유 검증 없이 그대로 save 한다.
  - 따라서 labeler_a 가 task_b(타인 배정) 로 POST 시 draft 가 생성된다(201) → 격리 실패(Red).

green 단계 예상 최소 구현:
  - perform_create 에서 부모 task 가 OrganizationMember.scope_tasks_for_request 로 좁힌
    태스크에 속하는지 검증 → 아니면 403/404.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.models import AnnotationDraft
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

DENIED = (403, 404)


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerDraftCreateIsolation(APITestCase):
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

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    def _create_draft(self, task):
        return self.client.post(
            f'/api/tasks/{task.id}/drafts', data={'result': []}, format='json'
        )

    # --- 1) 타인 태스크에 draft 생성 격리 --------------------------------------

    def test_labeler_cannot_create_draft_on_foreign_task(self):
        """[RED] labeler_a 가 타인 배정 태스크(task_b) 로 draft 를 생성하면 거부(403/404)."""
        self._auth(self.labeler_a)
        before = AnnotationDraft.objects.filter(task=self.task_b).count()
        response = self._create_draft(self.task_b)
        after = AnnotationDraft.objects.filter(task=self.task_b).count()
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 태스크에 draft 를 생성함(격리 실패): '
            f'status={response.status_code}, body={response.content}'
        )
        assert after == before, (
            f'labeler_a 가 타인 태스크 draft 를 실제로 생성함(side effect): {before} -> {after}'
        )

    # --- 가드: 본인 태스크 생성은 정상 -----------------------------------------

    def test_labeler_can_create_draft_on_own_task(self):
        """[가드] labeler_a 는 본인 배정 태스크(task_a) 로 draft 를 생성할 수 있다(거부 아님)."""
        self._auth(self.labeler_a)
        response = self._create_draft(self.task_a)
        assert response.status_code not in DENIED, (
            f'labeler_a 가 본인 태스크 draft 생성을 못함(과차단): '
            f'status={response.status_code}, body={response.content}'
        )

    # --- 가드: reviewer 는 격리 없음 -------------------------------------------

    def test_reviewer_can_create_draft_on_any_task(self):
        """[가드] reviewer 는 타인 배정 태스크에도 draft 를 생성할 수 있다(거부 아님)."""
        self._auth(self.reviewer)
        response = self._create_draft(self.task_b)
        assert response.status_code not in DENIED, (
            f'reviewer 가 draft 생성을 못함(과차단): status={response.status_code}, body={response.content}'
        )
