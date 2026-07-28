"""TDD Red — 라벨러 대상 프로젝트 태스크 카운트 누수 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **프로젝트 상태 조회 시 task_count 누수** 한정.

검증하려는 목표 동작:
  - role='labeler' 유저가 프로젝트 상태(GET /api/dm/project/?project=)를 조회할 때
    노출되는 task_count 는 **전체 태스크 수**가 아니라 **본인 배정분(assignee=self)** 기준이어야 한다.
    (또는 아예 노출하지 않음. 일관성/최소노출 원칙.)
  - role='reviewer' / 'admin' 은 전체 태스크 수를 본다 — 가드.

경로: data_manager/api.py:ProjectStateAPI.get (url name 'dm-project', GET /api/dm/project/)

현재 구현 상태 (Red 근거):
  ProjectStateAPI 는 task_count 를 project.tasks.count() 로 계산한다 (assignee 무스코핑).
  labeler_a 에게 본인 태스크 1건만 배정돼 있어도 project 전체 태스크(2건)를 카운트로 돌려주므로
  타 라벨러 배정 태스크 존재/규모가 누수된다 → 격리 실패(RED).

green 단계 예상 최소 구현:
  ProjectStateAPI.get 의 task_count 계산을
  OrganizationMember.scope_tasks_for_user(request.user, active_org, project.tasks) 기준으로 좁힌다.
"""
from data_manager.models import View  # noqa: F401 (앱 로드 보장용)
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


class TestLabelerProjectCountLeak(APITestCase):
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

        # 프로젝트 총 3건: labeler_a 1건, labeler_b 1건, 미배정 1건
        cls.task_a = TaskFactory(project=cls.project, data={'text': 'a'}, assignee=cls.labeler_a)
        cls.task_b = TaskFactory(project=cls.project, data={'text': 'b'}, assignee=cls.labeler_b)
        cls.task_c = TaskFactory(project=cls.project, data={'text': 'c'}, assignee=None)

    def _state(self, user):
        self.client.force_authenticate(user=user)
        return self.client.get(f'/api/dm/project/?project={self.project.id}')

    # --- RED: labeler 는 본인 배정분 카운트만 봐야 함 ---------------------------

    def test_labeler_project_state_task_count_is_scoped(self):
        """[RED] labeler_a 의 project state task_count 는 본인 배정분(1)이어야 한다 (전체 3 누수 금지)."""
        response = self._state(self.labeler_a)
        assert response.status_code == 200, response.content
        task_count = response.json().get('task_count')
        assert task_count == 1, (
            f'labeler_a 에게 전체 태스크 수가 누수됨(격리 실패): '
            f'task_count={task_count} (기대: 본인 배정분 1건)'
        )

    # --- 가드: reviewer / admin 은 전체 카운트를 본다 ---------------------------

    def test_reviewer_project_state_task_count_is_total(self):
        """[가드] reviewer 는 프로젝트 전체 태스크 수(3)를 본다."""
        response = self._state(self.reviewer)
        assert response.status_code == 200, response.content
        assert response.json().get('task_count') == 3, response.content

    def test_admin_project_state_task_count_is_total(self):
        """[가드] admin 은 프로젝트 전체 태스크 수(3)를 본다."""
        response = self._state(self.admin)
        assert response.status_code == 200, response.content
        assert response.json().get('task_count') == 3, response.content
