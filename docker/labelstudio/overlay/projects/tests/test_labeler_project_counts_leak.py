"""TDD Red — 라벨러 대상 Project with_counts 누수 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **프로젝트 상세/목록의 with_counts 카운트 누수** 한정.

검증하려는 목표 동작:
  - role='labeler' 유저가
        프로젝트 상세  GET /api/projects/{id}/   (ProjectAPI)
        프로젝트 목록  GET /api/projects/        (ProjectListAPI)
    를 조회할 때, 노출되는 통계 카운트(task_number, total_annotations_number 등)는
    **org 전체**가 아니라 **본인 배정 태스크(assignee=self)** 기준이어야 한다.
  - role='reviewer' / 'admin' 은 프로젝트 전체 카운트를 본다 — 가드.

경로:
  - projects/api.py:ProjectAPI.get_queryset -> Project.objects.with_counts(...)
  - projects/api.py:ProjectListAPI.get_queryset -> ProjectManager.with_counts_annotate(...)
  - projects/models.py:ProjectManager.with_counts / with_counts_annotate
  - projects/functions/__init__.py:annotate_task_number / annotate_total_annotations_number ...
    (전부 project=OuterRef 서브쿼리 기반 DB annotation. assignee 무스코핑.)
  - 노출 필드: projects/serializers.py:ProjectSerializer.task_number / total_annotations_number ...

현재 구현 상태 (Red 근거):
  with_counts 계열 annotate 함수는 Task/Annotation 을 project=OuterRef 로만 좁혀
  프로젝트 전체를 센다. 유저 역할/배정과 무관하므로 labeler_a 에게도 전체 3건이 노출된다.
  → 본인 배정분(task_number=1, total_annotations_number=1)만 봐야 하는데 3 이 나와 RED.

green 단계 예상 최소 구현:
  with_counts_annotate / annotate_* 에 user(또는 assignee 필터)를 스레딩해
  labeler 일 때 서브쿼리를 task__assignee=user / assignee=user 로 좁힌다.
  (DB annotation 재작성이 필요하므로 green 부담이 큰 지점 — 별도 평가 참조.)
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import AnnotationFactory, TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerProjectCountsLeak(APITestCase):
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

        # 프로젝트 총 3건 + 각 태스크 annotation 1건 (was_cancelled=False 전체 3건)
        cls.task_a = TaskFactory(project=cls.project, data={'text': 'a'}, assignee=cls.labeler_a)
        cls.task_b = TaskFactory(project=cls.project, data={'text': 'b'}, assignee=cls.labeler_b)
        cls.task_c = TaskFactory(project=cls.project, data={'text': 'c'}, assignee=None)

        cls.ann_a = AnnotationFactory(
            task=cls.task_a, project=cls.project, completed_by=cls.labeler_a, result=[]
        )
        cls.ann_b = AnnotationFactory(
            task=cls.task_b, project=cls.project, completed_by=cls.labeler_b, result=[]
        )
        cls.ann_c = AnnotationFactory(
            task=cls.task_c, project=cls.project, completed_by=cls.admin, result=[]
        )

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    def _detail(self, user):
        self._auth(user)
        return self.client.get(f'/api/projects/{self.project.id}/')

    def _list_entry(self, user):
        self._auth(user)
        response = self.client.get('/api/projects/')
        assert response.status_code == 200, response.content
        payload = response.json()
        results = payload['results'] if isinstance(payload, dict) and 'results' in payload else payload
        entries = [p for p in results if p['id'] == self.project.id]
        assert entries, f'project {self.project.id} 가 목록에 없음: {results}'
        return entries[0]

    # --- RED: 프로젝트 상세 카운트는 labeler 본인 배정분 기준이어야 함 ------------

    def test_labeler_project_detail_task_number_is_scoped(self):
        """[RED] labeler_a 의 project detail task_number 는 본인 배정분(1)이어야 한다 (전체 3 누수 금지)."""
        response = self._detail(self.labeler_a)
        assert response.status_code == 200, response.content
        task_number = response.json().get('task_number')
        assert task_number == 1, (
            f'labeler_a 에게 프로젝트 전체 task 수가 누수됨(격리 실패): '
            f'task_number={task_number} (기대: 본인 배정분 1건)'
        )

    def test_labeler_project_detail_total_annotations_number_is_scoped(self):
        """[RED] labeler_a 의 project detail total_annotations_number 는 본인 배정분(1)이어야 한다."""
        response = self._detail(self.labeler_a)
        assert response.status_code == 200, response.content
        total = response.json().get('total_annotations_number')
        assert total == 1, (
            f'labeler_a 에게 프로젝트 전체 annotation 수가 누수됨(격리 실패): '
            f'total_annotations_number={total} (기대: 본인 배정 태스크 annotation 1건)'
        )

    # --- RED: 프로젝트 목록 카운트도 동일하게 스코핑되어야 함 --------------------

    def test_labeler_project_list_task_number_is_scoped(self):
        """[RED] labeler_a 의 project list task_number 는 본인 배정분(1)이어야 한다 (전체 3 누수 금지)."""
        entry = self._list_entry(self.labeler_a)
        task_number = entry.get('task_number')
        assert task_number == 1, (
            f'labeler_a 에게 프로젝트 목록에서 전체 task 수가 누수됨(격리 실패): '
            f'task_number={task_number} (기대: 본인 배정분 1건)'
        )

    # --- 가드: reviewer / admin 은 프로젝트 전체 카운트를 본다 -------------------

    def test_reviewer_project_detail_counts_are_total(self):
        """[가드] reviewer 는 프로젝트 전체 카운트(task 3, annotation 3)를 본다."""
        response = self._detail(self.reviewer)
        assert response.status_code == 200, response.content
        body = response.json()
        assert body.get('task_number') == 3, body
        assert body.get('total_annotations_number') == 3, body

    def test_admin_project_detail_counts_are_total(self):
        """[가드] admin 은 프로젝트 전체 카운트(task 3, annotation 3)를 본다."""
        response = self._detail(self.admin)
        assert response.status_code == 200, response.content
        body = response.json()
        assert body.get('task_number') == 3, body
        assert body.get('total_annotations_number') == 3, body
