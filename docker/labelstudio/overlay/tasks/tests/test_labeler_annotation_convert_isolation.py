"""TDD Red — 라벨러별 annotation convert-to-draft 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **AnnotationConvertAPI 의 assignee 스코핑** 한정.

대상 뷰 (label_studio/tasks/api.py:1104 AnnotationConvertAPI):
  - POST /api/annotations/{annotation_pk}/convert-to-draft

검증하려는 목표 동작:
  - role='labeler' 는 **본인 배정 태스크(assignee=self) 의 annotation 만** draft 로 변환 가능.
  - 타인 배정 태스크(task_b) 의 annotation 변환 시도 → 거부(404), 원본 annotation 은 보존.
  - 본인 태스크(task_a) annotation 변환은 정상(가드). reviewer/admin 은 격리 없음(가드).

현재 구현 상태 (Red 근거):
  AnnotationConvertAPI.queryset = Annotation.objects.all(), get_queryset 오버라이드 없음.
  get_object() 가 타인 annotation(ann_b) 을 그대로 반환 → draft 생성 후 annotation.delete()
  까지 수행되어 201 을 돌려준다 → 격리 테스트가 실패(Red)해야 정상.

green 단계 예상 최소 구현:
  AnnotationConvertAPI 에 get_queryset() 추가 →
  Annotation queryset 을 OrganizationMember.scope_tasks_for_request 로 좁힌 태스크 기준
  (task__in=scoped_tasks) 으로 필터 → 타인 annotation 이면 404, 파괴적 변환 차단.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.models import Annotation
from tasks.tests.factories import AnnotationFactory, TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

DENIED = (403, 404)


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerAnnotationConvertIsolation(APITestCase):
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

        cls.ann_a = AnnotationFactory(task=cls.task_a, project=cls.project, completed_by=cls.labeler_a, result=[])
        cls.ann_b = AnnotationFactory(task=cls.task_b, project=cls.project, completed_by=cls.labeler_b, result=[])

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    def _convert(self, annotation_id):
        return self.client.post(f'/api/annotations/{annotation_id}/convert-to-draft')

    # --- 1) 타인 annotation 변환 격리 -------------------------------------------

    def test_labeler_cannot_convert_foreign_annotation(self):
        """[RED] labeler_a 가 타인 annotation(ann_b) 을 draft 로 변환하면 거부(404)."""
        self._auth(self.labeler_a)
        response = self._convert(self.ann_b.id)
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 annotation 을 변환함(격리 실패): status={response.status_code}, {response.content}'
        )
        # 파괴적 side effect 방지: 원본 annotation 은 보존되어야 한다.
        assert Annotation.objects.filter(pk=self.ann_b.id).exists(), (
            'labeler_a 의 변환 시도로 타인 annotation 이 삭제됨(격리 실패)'
        )

    # --- 가드: 본인 annotation 변환은 정상 -------------------------------------

    def test_labeler_can_convert_own_annotation(self):
        """[가드] labeler_a 는 본인 annotation(ann_a) 을 draft 로 변환 가능(거부 아님)."""
        self._auth(self.labeler_a)
        response = self._convert(self.ann_a.id)
        assert response.status_code not in DENIED, (
            f'labeler_a 가 본인 annotation 변환을 거부당함(과차단): status={response.status_code}, {response.content}'
        )

    # --- 가드: reviewer / admin 은 격리 없음 ------------------------------------

    def test_reviewer_can_convert_any_annotation(self):
        """[가드] reviewer 는 타인 annotation 변환 가능(거부 아님)."""
        self._auth(self.reviewer)
        response = self._convert(self.ann_b.id)
        assert response.status_code not in DENIED, response.content

    def test_admin_can_convert_any_annotation(self):
        """[가드] admin 은 타인 annotation 변환 가능(거부 아님)."""
        self._auth(self.admin)
        response = self._convert(self.ann_b.id)
        assert response.status_code not in DENIED, response.content
