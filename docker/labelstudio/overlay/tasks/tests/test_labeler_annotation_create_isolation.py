"""TDD Red — 라벨러별 annotation *생성(POST)* 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **annotation 생성(POST) 격리** 한정.
(조회/수정/삭제 = test_labeler_annotation_isolation.py 에서 이미 다룸.)

검증하려는 목표 동작:
  - role='labeler' 유저는 **본인 배정 태스크(assignee=self)** 에만 annotation 을 생성할 수 있다.
      POST /api/tasks/{task_a}/annotations/  → 201 (본인 태스크, 가드)
  - 타인 배정 태스크(task_b, assignee=labeler_b)에는 생성이 거부된다(404/403):
      POST /api/tasks/{task_b}/annotations/  → 404 또는 403 (격리, RED)
  - role='reviewer' / 'admin' 은 격리되지 않고 타인 태스크에도 생성 가능(201) — 가드.

현재 구현 상태 (Red 근거):
  tasks/api.py:AnnotationsListAPI 는 get_queryset() 에서만 scope_tasks_for_user 를 적용하는데,
  POST 경로(perform_create)는 GetParentObjectMixin 이 제공하는 self.parent_object 를 쓴다.
  parent_object 는 parent_queryset = Task.objects.all() (org/assignee 무스코핑) 에서 pk 로만 해석되고,
  객체 권한도 Task.has_permission(=project.has_permission, org 레벨) 만 통과시키므로
  labeler_a 가 타인 태스크(task_b)에 annotation 을 201 로 생성하게 된다 → 격리 실패(RED).

green 단계 예상 최소 구현:
  AnnotationsListAPI 의 parent_queryset 을 assignee 스코핑하거나(예: get_parent_object 오버라이드로
  scope_tasks_for_user 적용), perform_create 진입 전 self.parent_object 해석을 라벨러 스코프로 좁혀
  타인 태스크면 404 가 나게 한다.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.models import Annotation
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

DENIED = (403, 404)


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerAnnotationCreateIsolation(APITestCase):
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

        # labeler_a / labeler_b 에게 각각 배타적으로 배정된 태스크
        cls.task_a = TaskFactory(project=cls.project, data={'text': 'a'}, assignee=cls.labeler_a)
        cls.task_b = TaskFactory(project=cls.project, data={'text': 'b'}, assignee=cls.labeler_b)

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    def _post_annotation(self, task):
        return self.client.post(
            f'/api/tasks/{task.id}/annotations/', data={'result': []}, format='json'
        )

    # --- RED: 타인 태스크에 생성 거부 ------------------------------------------

    def test_labeler_cannot_create_annotation_on_foreign_task(self):
        """[RED] labeler_a 가 타인 태스크(task_b)에 annotation 생성 시 거부(404/403)."""
        self._auth(self.labeler_a)
        before = Annotation.objects.filter(task=self.task_b).count()
        response = self._post_annotation(self.task_b)
        after = Annotation.objects.filter(task=self.task_b).count()
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 태스크에 annotation 을 생성함(격리 실패): '
            f'status={response.status_code}, body={response.content}'
        )
        assert after == before, f'거부됐어야 할 annotation 이 실제로 생성됨: {before}->{after}'

    # --- 가드: 본인 태스크엔 생성 가능 ------------------------------------------

    def test_labeler_can_create_annotation_on_own_task(self):
        """[가드] labeler_a 는 본인 태스크(task_a)에 annotation 을 201 로 생성한다."""
        self._auth(self.labeler_a)
        response = self._post_annotation(self.task_a)
        assert response.status_code == 201, response.content

    # --- 가드: reviewer / admin 은 격리되지 않음 --------------------------------

    def test_reviewer_can_create_annotation_on_any_task(self):
        """[가드] reviewer 는 타인 태스크(task_b)에도 annotation 생성 가능(201)."""
        self._auth(self.reviewer)
        response = self._post_annotation(self.task_b)
        assert response.status_code == 201, response.content

    def test_admin_can_create_annotation_on_any_task(self):
        """[가드] admin 은 타인 태스크(task_b)에도 annotation 생성 가능(201)."""
        self._auth(self.admin)
        response = self._post_annotation(self.task_b)
        assert response.status_code == 201, response.content
