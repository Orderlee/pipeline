"""TDD Red — 라벨러별 prediction 조회 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **PredictionAPI(list) 의 assignee 스코핑** 한정.

대상 뷰 (label_studio/tasks/api.py:1082 PredictionAPI):
  - GET /api/predictions/?task={task_id}
  - GET /api/predictions/?project={project_id}
  - GET /api/predictions/           (전체)

검증하려는 목표 동작:
  - role='labeler' 는 **본인 배정 태스크(assignee=self) 의 prediction 만** 볼 수 있다.
  - 타인 배정 태스크(task_b) prediction 을 ?task / ?project / 전체 조회로 얻으려 해도
    거부되거나 결과에 포함되지 않는다.
  - 본인 태스크(task_a) prediction 은 정상 반환(가드). reviewer/admin 은 격리 없음(가드).

현재 구현 상태 (Red 근거):
  PredictionAPI.get_queryset() 은
      Prediction.objects.filter(project__organization=user.active_organization)
  즉 **org 기준만** 좁힌다. assignee 스코핑이 없어 labeler_a 가 ?task=task_b / ?project /
  전체 조회로 타인 prediction(pred_b)을 200 으로 받는다 → 격리 테스트가 실패(Red).

green 단계 예상 최소 구현:
  PredictionAPI.get_queryset() 을
      OrganizationMember.scope_tasks_for_request 로 좁힌 태스크 기준
      (task__in=scoped_tasks 또는 task__assignee) 으로 필터.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import PredictionFactory, TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

DENIED = (403, 404)


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


def _ids(response):
    body = response.json()
    results = body['results'] if isinstance(body, dict) and 'results' in body else body
    return {row['id'] for row in results}


class TestLabelerPredictionIsolation(APITestCase):
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

        cls.pred_a = PredictionFactory(task=cls.task_a, project=cls.project)
        cls.pred_b = PredictionFactory(task=cls.task_b, project=cls.project)

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    # --- 1) ?task=타인 태스크 격리 -----------------------------------------------

    def test_labeler_cannot_list_predictions_of_foreign_task(self):
        """[RED] labeler_a 가 ?task=task_b 로 타인 prediction 을 조회하면 거부/빈결과."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/predictions/?task={self.task_b.id}')
        if response.status_code in DENIED:
            return
        assert response.status_code == 200, response.content
        assert self.pred_b.id not in _ids(response), (
            f'labeler_a 가 타인 태스크 prediction(pred_b)을 조회함(격리 실패): {response.content}'
        )

    # --- 2) ?project= 로 프로젝트 전체 격리 --------------------------------------

    def test_labeler_cannot_list_predictions_of_whole_project(self):
        """[RED] labeler_a 가 ?project= 로 프로젝트 전체 prediction 을 조회해도 타인 것은 제외."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/predictions/?project={self.project.id}')
        assert response.status_code == 200, response.content
        assert self.pred_b.id not in _ids(response), (
            f'labeler_a 가 프로젝트 전체 조회로 타인 prediction 을 봄(격리 실패): {response.content}'
        )

    # --- 3) 필터 없는 전체 조회 격리 ---------------------------------------------

    def test_labeler_prediction_list_excludes_foreign(self):
        """[RED] labeler_a 의 무필터 prediction 목록에 타인 prediction 은 포함되지 않는다."""
        self._auth(self.labeler_a)
        response = self.client.get('/api/predictions/')
        assert response.status_code == 200, response.content
        assert self.pred_b.id not in _ids(response), (
            f'labeler_a 의 prediction 전체 목록에 타인 것이 포함됨(격리 실패): {response.content}'
        )

    # --- 가드: 본인 prediction 은 정상 ------------------------------------------

    def test_labeler_can_list_own_predictions(self):
        """[가드] labeler_a 는 본인 태스크(task_a) prediction 을 200 으로 받는다."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/predictions/?task={self.task_a.id}')
        assert response.status_code == 200, response.content
        assert self.pred_a.id in _ids(response), response.content

    # --- 가드: reviewer / admin 은 전체 접근 ------------------------------------

    def test_reviewer_can_list_any_prediction(self):
        """[가드] reviewer 는 타인 prediction 을 조회 가능(200 + 포함)."""
        self._auth(self.reviewer)
        response = self.client.get(f'/api/predictions/?task={self.task_b.id}')
        assert response.status_code == 200, response.content
        assert self.pred_b.id in _ids(response), response.content

    def test_admin_can_list_any_prediction(self):
        """[가드] admin 은 타인 prediction 을 조회 가능(200 + 포함)."""
        self._auth(self.admin)
        response = self.client.get(f'/api/predictions/?task={self.task_b.id}')
        assert response.status_code == 200, response.content
        assert self.pred_b.id in _ids(response), response.content
