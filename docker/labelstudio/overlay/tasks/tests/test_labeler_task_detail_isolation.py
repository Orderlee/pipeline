"""TDD Red — 라벨러별 태스크 상세 조회 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **태스크 상세 조회 격리** 한정.
(export / next_task / labels / 목록 그리드는 여기서 다루지 않음 — 그리드 목록은 Cycle 1
 `test_labeler_task_isolation.py` 에서 이미 격리 완료.)

검증하려는 목표 동작:
  - role='labeler' 유저가 **타인에게 배정된 태스크**를 GET /api/tasks/{id}/ 로 직접 요청하면
    접근이 거부된다 (LS 관례상 404, 또는 403 도 허용). 본인 배정 태스크는 200.
  - role='reviewer' / 'admin' 은 격리되지 않고 어떤 태스크든 접근 가능(200).

현재 구현 상태 (Red 근거):
  tasks/api.py:TaskAPI.get_object() 는
      Task.objects.filter(project__organization=active_organization)
  로만 좁힌 뒤 check_object_permissions 만 통과시킨다. assignee 스코핑이 없으므로
  labeler 가 같은 org 안 타인 태스크 상세를 200 으로 받아 이 테스트가 실패(Red)해야 정상이다.

설계 계약(Cycle 1 에서 이미 도입됨):
  - Task.assignee : User FK, nullable (태스크당 라벨러 1인, 배타적)
  - OrganizationMember.role : labeler/reviewer/admin
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerTaskDetailIsolation(APITestCase):
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

    def _get_task(self, user, task):
        self.client.force_authenticate(user=user)
        return self.client.get(f'/api/tasks/{task.id}/')

    def test_labeler_cannot_retrieve_other_labelers_task(self):
        """[RED 핵심] labeler_a 가 labeler_b 의 task_b 상세를 요청하면 접근 거부(404 또는 403)."""
        response = self._get_task(self.labeler_a, self.task_b)
        assert response.status_code in (403, 404), (
            f'타 라벨러 태스크 상세는 접근 거부돼야 함, 실제 status={response.status_code} '
            f'(격리 실패 — 타인 태스크 상세 노출)'
        )

    def test_labeler_can_retrieve_own_assigned_task(self):
        """본인 배정 태스크 상세는 정상 접근(200) — 격리가 본인 것까지 막으면 안 됨."""
        response = self._get_task(self.labeler_a, self.task_a)
        assert response.status_code == 200, response.content
        assert response.json()['id'] == self.task_a.id

    def test_reviewer_can_retrieve_any_task(self):
        """reviewer 는 격리되지 않고 어떤 태스크 상세든 접근 가능(200)."""
        response = self._get_task(self.reviewer, self.task_a)
        assert response.status_code == 200, response.content
        assert response.json()['id'] == self.task_a.id

    def test_admin_can_retrieve_any_task(self):
        """admin 은 격리되지 않고 어떤 태스크 상세든 접근 가능(200)."""
        response = self._get_task(self.admin, self.task_b)
        assert response.status_code == 200, response.content
        assert response.json()['id'] == self.task_b.id
