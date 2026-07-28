"""TDD Red — 라벨러별 태스크 목록 조회 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **태스크 목록 조회 격리** 한정.
export / next_task / labels 등은 다음 사이클 (여기서 다루지 않음).

검증하려는 목표 동작:
  - role='labeler' 유저가 태스크 목록(GET /api/tasks/?project=)을 조회하면
    본인에게 배정된 태스크(Task.assignee == 본인)만 반환된다.
    같은 org 안 타 라벨러의 태스크는 0건.
  - role='reviewer' / 'admin' 은 전체 태스크를 본다 (격리 안 함).

아직 구현되지 않은 설계 계약(green 이 만듦):
  - Task.assignee : User 로의 FK, nullable (태스크당 라벨러 1인, 배타적).
  - 역할 3계층 labeler/reviewer/admin : 여기서는 OrganizationMember.role 필드로 표현한다고 가정.
    (green 이 다른 표현을 택하면 아래 set_role 헬퍼만 바꾸면 됨.)

현재 OSS 는 project__organization=active_organization 로만 필터하므로
labeler 가 타인 태스크를 전부 보게 되어 이 테스트는 실패(Red)해야 정상이다.
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
    """org 안에서 user 의 역할을 지정한다.

    설계 계약: OrganizationMember.role 필드. 아직 없으므로 이 호출이 실패(FieldError)하는 것이
    Red 의 일부다. green 이 role 표현을 다르게 택하면 이 헬퍼만 교체하면 된다.
    """
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


def response_task_ids(response):
    """GET /api/tasks/ 응답에서 태스크 id 목록을 추출한다.

    DataManager 페이지네이션 응답은 {'tasks': [...], 'total': N}, 비페이지 응답은 [...] 형태.
    """
    data = response.json()
    tasks = data['tasks'] if isinstance(data, dict) else data
    return {t['id'] for t in tasks}


class TestLabelerTaskListIsolation(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.organization)

        # 같은 org 소속 유저들 (UserFactory(active_organization=org) 가 OrganizationMember 생성)
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

    def _list_tasks(self, user):
        self.client.force_authenticate(user=user)
        return self.client.get(f'/api/tasks/?project={self.project.id}')

    def test_labeler_sees_only_own_assigned_tasks(self):
        """labeler_a 는 본인 배정 태스크(task_a)만 보고, labeler_b 의 task_b 는 0건."""
        response = self._list_tasks(self.labeler_a)
        assert response.status_code == 200, response.content

        ids = response_task_ids(response)
        assert ids == {self.task_a.id}, f'labeler_a 는 본인 태스크만 봐야 함, 실제: {ids}'
        assert self.task_b.id not in ids, '타 라벨러 태스크가 노출됨 (격리 실패)'

    def test_reviewer_sees_all_tasks_in_project(self):
        """reviewer 는 격리되지 않고 프로젝트 전체 태스크를 본다."""
        response = self._list_tasks(self.reviewer)
        assert response.status_code == 200, response.content

        ids = response_task_ids(response)
        assert {self.task_a.id, self.task_b.id} <= ids, f'reviewer 는 전체 태스크를 봐야 함, 실제: {ids}'

    def test_admin_sees_all_tasks_in_project(self):
        """admin 은 격리되지 않고 프로젝트 전체 태스크를 본다."""
        response = self._list_tasks(self.admin)
        assert response.status_code == 200, response.content

        ids = response_task_ids(response)
        assert {self.task_a.id, self.task_b.id} <= ids, f'admin 은 전체 태스크를 봐야 함, 실제: {ids}'
