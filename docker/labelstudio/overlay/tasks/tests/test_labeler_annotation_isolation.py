"""TDD Red — 라벨러별 annotation 접근 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **annotation 조회/수정/삭제 격리** 한정.
(목록 그리드 = Cycle 1, 태스크 상세 = Cycle 2, next_task/export 는 별도 사이클에서 이미 다룸.)

검증하려는 목표 동작:
  - role='labeler' 유저는 **본인 배정 태스크(assignee=self)에 달린 annotation** 만 접근 가능하다.
  - 타인 배정 태스크(task_b, assignee=labeler_b)에 달린 annotation 에 대해
    아래 경로 모두 접근이 거부된다(LS 관례상 404, 403 도 허용):
      1) 태스크별 annotation 목록:  GET  /api/tasks/{task_b.id}/annotations/
      2) annotation 상세:          GET  /api/annotations/{ann_b.id}/
      3) annotation 수정:          PATCH  /api/annotations/{ann_b.id}/
      4) annotation 삭제:          DELETE /api/annotations/{ann_b.id}/
  - 본인 배정 태스크(task_a)의 annotation 은 정상 접근(200) — 가드.
  - role='reviewer' / 'admin' 은 격리되지 않고 전체 annotation 접근 가능(200) — 가드.

현재 구현 상태 (Red 근거):
  1) tasks/api.py:AnnotationsListAPI.get_queryset() 는
         task = get_object_or_404(Task.objects.for_user(request.user), pk=...)
     로 태스크를 해석하는데, TaskManager.for_user 는
         filter(project__organization=user.active_organization)
     즉 **org 기준만** 좁힌다. assignee 스코핑이 없어 labeler_a 가 타인 태스크(task_b)의
     annotation 목록을 200 으로 받는다.
  2) tasks/api.py:AnnotationAPI 는 queryset = Annotation.objects.all() 를 쓰고
     get_queryset 오버라이드가 없다. assignee 스코핑이 전혀 없으므로 labeler_a 가
     타인 annotation 상세/수정/삭제에 성공해 이 테스트들이 실패(Red)해야 정상이다.

설계 계약(Cycle 1 에서 이미 도입됨):
  - Task.assignee : User FK, nullable (태스크당 라벨러 1인, 배타적)
  - OrganizationMember.role : labeler/reviewer/admin
  - OrganizationMember.scope_tasks_for_user(user, org, qs) : labeler 는 assignee=본인으로 좁힘

green 단계 예상 최소 구현:
  - AnnotationsListAPI.get_queryset() 의 태스크 해석을
    scope_tasks_for_user(request.user, active_org, Task.objects.for_user(...)) 로 감싸
    타인 태스크면 404 가 나게 한다.
  - AnnotationAPI 에 get_queryset() 를 추가해 annotation queryset 을
    task__in=scope_tasks_for_user(...) (혹은 task__assignee 기준)로 좁힌다.
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

DENIED = (403, 404)


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerAnnotationIsolation(APITestCase):
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

        # labeler_a / labeler_b 에게 각각 배타적으로 배정된 태스크 + 그 위의 annotation
        cls.task_a = TaskFactory(project=cls.project, data={'text': 'a'}, assignee=cls.labeler_a)
        cls.task_b = TaskFactory(project=cls.project, data={'text': 'b'}, assignee=cls.labeler_b)

        cls.ann_a = AnnotationFactory(
            task=cls.task_a, project=cls.project, completed_by=cls.labeler_a, result=[]
        )
        cls.ann_b = AnnotationFactory(
            task=cls.task_b, project=cls.project, completed_by=cls.labeler_b, result=[]
        )

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    # --- 1) 태스크별 annotation 목록 격리 --------------------------------------

    def test_labeler_cannot_list_annotations_of_foreign_task(self):
        """[RED] labeler_a 가 타인 태스크(task_b)의 annotation 목록을 요청하면 거부(404/403)."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/tasks/{self.task_b.id}/annotations/')
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 태스크 annotation 목록에 접근됨(격리 실패): '
            f'status={response.status_code}, body={response.content}'
        )

    def test_labeler_can_list_annotations_of_own_task(self):
        """[가드] labeler_a 는 본인 태스크(task_a)의 annotation 목록을 200 으로 받는다."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/tasks/{self.task_a.id}/annotations/')
        assert response.status_code == 200, response.content
        returned_ids = {a['id'] for a in response.json()}
        assert self.ann_a.id in returned_ids, response.content

    # --- 2) annotation 상세 조회 격리 -------------------------------------------

    def test_labeler_cannot_retrieve_foreign_annotation(self):
        """[RED] labeler_a 가 타인 annotation(ann_b) 상세를 요청하면 거부(404/403)."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/annotations/{self.ann_b.id}/')
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 annotation 상세에 접근됨(격리 실패): '
            f'status={response.status_code}, body={response.content}'
        )

    def test_labeler_can_retrieve_own_annotation(self):
        """[가드] labeler_a 는 본인 annotation(ann_a) 상세를 200 으로 받는다."""
        self._auth(self.labeler_a)
        response = self.client.get(f'/api/annotations/{self.ann_a.id}/')
        assert response.status_code == 200, response.content
        assert response.json()['id'] == self.ann_a.id

    # --- 3) annotation 수정 격리 ------------------------------------------------

    def test_labeler_cannot_update_foreign_annotation(self):
        """[RED] labeler_a 가 타인 annotation(ann_b) 수정을 시도하면 거부(404/403)."""
        self._auth(self.labeler_a)
        response = self.client.patch(
            f'/api/annotations/{self.ann_b.id}/', data={'result': []}, format='json'
        )
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 annotation 수정에 성공함(격리 실패): '
            f'status={response.status_code}, body={response.content}'
        )

    # --- 4) annotation 삭제 격리 ------------------------------------------------

    def test_labeler_cannot_delete_foreign_annotation(self):
        """[RED] labeler_a 가 타인 annotation(ann_b) 삭제를 시도하면 거부(404/403)."""
        self._auth(self.labeler_a)
        response = self.client.delete(f'/api/annotations/{self.ann_b.id}/')
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 annotation 삭제에 성공함(격리 실패): '
            f'status={response.status_code}, body={response.content}'
        )

    # --- 가드: reviewer / admin 은 격리되지 않음 --------------------------------

    def test_reviewer_can_access_any_annotation(self):
        """[가드] reviewer 는 타인 annotation 상세/목록에 접근 가능(200)."""
        self._auth(self.reviewer)
        detail = self.client.get(f'/api/annotations/{self.ann_b.id}/')
        assert detail.status_code == 200, detail.content
        listing = self.client.get(f'/api/tasks/{self.task_b.id}/annotations/')
        assert listing.status_code == 200, listing.content

    def test_admin_can_access_any_annotation(self):
        """[가드] admin 은 타인 annotation 상세/목록에 접근 가능(200)."""
        self._auth(self.admin)
        detail = self.client.get(f'/api/annotations/{self.ann_b.id}/')
        assert detail.status_code == 200, detail.content
        listing = self.client.get(f'/api/tasks/{self.task_b.id}/annotations/')
        assert listing.status_code == 200, listing.content
