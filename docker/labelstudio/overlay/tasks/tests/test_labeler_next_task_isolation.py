"""TDD Red — 라벨링 큐(next_task) 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **라벨링 스트림의 "다음 태스크"(next_task) 격리** 한정.
(목록 그리드 = Cycle 1, 상세 = Cycle 2, export = 별도 사이클에서 이미 다룸.)

검증하려는 목표 동작:
  - role='labeler' 유저가 라벨링 화면에서 "다음 태스크"를 받을 때
    (GET /api/projects/{id}/next/) 본인 assignee 태스크에서만 후보가 나온다.
    타 라벨러(assignee=labeler_b) / 미배정(assignee=None) 태스크는 절대 서빙되지 않는다.
  - role='reviewer' / 'admin' 은 격리되지 않고 프로젝트 전체 후보를 받는다.

현재 구현 상태 (Red 근거):
  projects/api.py:ProjectNextTaskAPI.get() 는
      prepared_tasks = get_prepared_queryset(request, project)   # 프로젝트 전체
  를 그대로 projects/functions/next_task.py:get_next_task 에 넘긴다.
  assignee 스코핑이 전혀 없으므로 labeler 에게 타인/미배정 태스크가 서빙된다.

  프로젝트 기본 sampling 은 SEQUENCE(Project.SEQUENCE)이고 DataManager 기본 정렬은
  order_by('id') 오름차순(data_manager/managers.py). 따라서 아래 셋업에서 가장 먼저
  생성된(=가장 낮은 id) task_b 가 labeler_a 에게도 서빙되어 이 테스트가 실패(Red)해야 정상이다.

설계 계약(Cycle 1 에서 이미 도입됨):
  - Task.assignee : User FK, nullable (태스크당 라벨러 1인, 배타적)
  - OrganizationMember.role : labeler/reviewer/admin
  - OrganizationMember.scope_tasks_for_user(user, org, qs) : labeler 는 assignee=본인으로 좁힘

green 단계 예상 최소 구현:
  ProjectNextTaskAPI.get() 에서 prepared_tasks 를
  OrganizationMember.scope_tasks_for_user(request.user, active_org, prepared_tasks)
  로 감싼 뒤 get_next_task 에 넘긴다(목록/상세 격리와 동일 헬퍼 재사용).
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.models import Project
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerNextTaskIsolation(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        # sampling=SEQUENCE(기본) → next_task 가 id 오름차순으로 결정적으로 뽑힘 (flakiness 제거)
        cls.project = ProjectFactory(organization=cls.organization, sampling=Project.SEQUENCE)

        cls.admin = cls.organization.created_by
        cls.reviewer = UserFactory(active_organization=cls.organization)
        cls.labeler_a = UserFactory(active_organization=cls.organization)
        cls.labeler_b = UserFactory(active_organization=cls.organization)

        set_role(cls.admin, cls.organization, ROLE_ADMIN)
        set_role(cls.reviewer, cls.organization, ROLE_REVIEWER)
        set_role(cls.labeler_a, cls.organization, ROLE_LABELER)
        set_role(cls.labeler_b, cls.organization, ROLE_LABELER)

        # 생성 순서 = id 오름차순. 타인/미배정 태스크를 먼저(=낮은 id) 만들어
        # 격리가 없으면 SEQUENCE 큐가 labeler_a 에게 task_b 부터 서빙하도록 유도한다.
        cls.task_b = TaskFactory(project=cls.project, data={'text': 'b'}, assignee=cls.labeler_b)
        cls.task_c = TaskFactory(project=cls.project, data={'text': 'c'}, assignee=None)
        # labeler_a 본인 배정 태스크는 가장 마지막(=가장 높은 id)에 생성
        cls.task_a = TaskFactory(project=cls.project, data={'text': 'a'}, assignee=cls.labeler_a)

        cls.foreign_ids = {cls.task_b.id, cls.task_c.id}

    def _next(self, user):
        self.client.force_authenticate(user=user)
        return self.client.get(f'/api/projects/{self.project.id}/next/')

    def test_labeler_next_task_never_serves_others_or_unassigned(self):
        """[RED 핵심] labeler_a 의 next_task 는 타인/미배정 태스크(task_b/task_c)를 절대 서빙하지 않는다.

        현재 구현은 프로젝트 전체에서 id 최소인 task_b 를 서빙하므로 이 단언이 깨진다(Red).
        """
        response = self._next(self.labeler_a)
        assert response.status_code == 200, response.content
        served_id = response.json()['id']
        assert served_id not in self.foreign_ids, (
            f'labeler_a 에게 타인/미배정 태스크가 서빙됨(격리 실패): served_id={served_id}, '
            f'foreign={self.foreign_ids}'
        )

    def test_labeler_next_task_serves_only_own_assigned_task(self):
        """labeler_a 의 next_task 는 본인 배정 태스크(task_a)만 후보로 반환한다."""
        response = self._next(self.labeler_a)
        assert response.status_code == 200, response.content
        assert response.json()['id'] == self.task_a.id, (
            f'labeler_a 는 본인 배정 태스크만 받아야 함, 실제 served_id={response.json()["id"]}'
        )

    def test_labeler_next_task_never_serves_foreign_across_repeated_calls(self):
        """반복 호출해도 labeler_a 에게 타인/미배정 태스크가 한 번도 새어나오지 않는다(안정성 확인)."""
        for i in range(5):
            response = self._next(self.labeler_a)
            assert response.status_code == 200, response.content
            served_id = response.json()['id']
            assert served_id not in self.foreign_ids, (
                f'{i}번째 호출에서 타인/미배정 태스크 서빙됨(격리 실패): served_id={served_id}'
            )

    def test_reviewer_next_task_is_not_scoped(self):
        """[가드] reviewer 는 격리되지 않고 프로젝트 전체 후보(타인 배정 포함)를 받을 수 있다."""
        response = self._next(self.reviewer)
        assert response.status_code == 200, response.content
        served_id = response.json()['id']
        assert served_id in {self.task_a.id, self.task_b.id, self.task_c.id}, (
            f'reviewer 는 전체 후보에서 받아야 함, 실제 served_id={served_id}'
        )

    def test_admin_next_task_is_not_scoped(self):
        """[가드] admin 은 격리되지 않고 프로젝트 전체 후보(타인 배정 포함)를 받을 수 있다."""
        response = self._next(self.admin)
        assert response.status_code == 200, response.content
        served_id = response.json()['id']
        assert served_id in {self.task_a.id, self.task_b.id, self.task_c.id}, (
            f'admin 은 전체 후보에서 받아야 함, 실제 served_id={served_id}'
        )
