"""통합 검증 — 기존 프로젝트 일괄 배정 흐름 (scripts/assign_project.py 의 ORM 계약).

cutover 시나리오: 동료가 커뮤니티 LS 에 올린 **기존 프로젝트**(태스크 assignee=None)를
특정 라벨러(예: labeler_a)에게 배정해 커스텀 포크 LS 에 표출시킨다.

assign_project.py 의 핵심 ORM 동작을 그대로 재현해, 다음을 보장한다:
  - 배정 전: 라벨러는 미배정(assignee=None) 태스크를 못 본다 (0건).
  - 배정 후(Task.objects.filter(project=p).update(assignee=user)): 그 라벨러가 정확히 그 프로젝트 태스크만 본다.
  - 타 프로젝트(다른 assignee) 태스크는 여전히 안 보인다.
이 테스트가 통과하면 assign_project.py 의 배정 로직이 포크 격리와 정합함을 의미한다.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.models import Task
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


def response_task_ids(response):
    data = response.json()
    tasks = data['tasks'] if isinstance(data, dict) else data
    return {t['id'] for t in tasks}


class TestAssignExistingProjectFlow(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.admin = cls.organization.created_by  # owner (격리 예외)

        # 동료가 올린 '기존' 프로젝트 — 태스크는 아직 미배정(assignee=None)
        cls.existing_project = ProjectFactory(organization=cls.organization)
        cls.tasks = [
            TaskFactory(project=cls.existing_project, data={'text': f't{i}'}, assignee=None)
            for i in range(3)
        ]

        # 배정 대상 라벨러(labeler_a 역할) + 무관한 타 라벨러의 태스크(다른 프로젝트)
        cls.labeler_a = UserFactory(active_organization=cls.organization)
        cls.other_labeler = UserFactory(active_organization=cls.organization)
        set_role(cls.labeler_a, cls.organization, ROLE_LABELER)
        set_role(cls.other_labeler, cls.organization, ROLE_LABELER)

        cls.other_project = ProjectFactory(organization=cls.organization)
        cls.other_task = TaskFactory(
            project=cls.other_project, data={'text': 'other'}, assignee=cls.other_labeler
        )

    def _list(self, user, project):
        self.client.force_authenticate(user=user)
        return self.client.get(f'/api/tasks/?project={project.id}')

    def test_unassigned_project_hidden_before_assign(self):
        """배정 전: 라벨러는 미배정 태스크(assignee=None)를 못 본다."""
        resp = self._list(self.labeler_a, self.existing_project)
        assert resp.status_code == 200, resp.content
        assert response_task_ids(resp) == set(), '미배정 태스크가 라벨러에게 노출됨'

    def test_assign_makes_project_visible(self):
        """assign_project.py 핵심 동작(프로젝트 태스크 전부 assignee=labeler_a)을 재현하면
        labeler_a 이 정확히 그 프로젝트 태스크만 본다."""
        # scripts/assign_project.py 의 bulk update 와 동일한 ORM 연산
        updated = Task.objects.filter(project=self.existing_project).update(assignee=self.labeler_a)
        assert updated == 3

        resp = self._list(self.labeler_a, self.existing_project)
        assert resp.status_code == 200, resp.content
        ids = response_task_ids(resp)
        assert ids == {t.id for t in self.tasks}, f'배정된 태스크 전부 보여야 함, 실제: {ids}'

    def test_other_labelers_task_still_hidden_after_assign(self):
        """배정 후에도 타 라벨러(다른 프로젝트) 태스크는 labeler_a 에게 안 보인다."""
        Task.objects.filter(project=self.existing_project).update(assignee=self.labeler_a)
        resp = self._list(self.labeler_a, self.other_project)
        assert resp.status_code == 200, resp.content
        assert self.other_task.id not in response_task_ids(resp), '타 라벨러 태스크 노출됨 (격리 실패)'
