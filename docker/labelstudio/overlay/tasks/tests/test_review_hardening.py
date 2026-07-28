"""TDD Red — 검수 로직 견고성 (GCP 외주 LS 포크 전용, 자체완결형).

이 사이클 범위: 이미 구현된 2단계 검수 위에서 발견된 두 가지 견고성 결함을
실패 테스트로 고정한다. 구현은 하지 않는다(RED 전담).

A1. 반려 태스크의 is_labeled 재계산 보호
  배경: 반려(reject) 시 review_status='rejected', is_labeled=False 로 라벨러 큐에
    복귀시킨다. 그러나 annotation 은 그대로 남는다. 프로젝트 설정 변경/임포트 시
    is_labeled 를 annotation 기준으로 재계산하는 경로
      - Task.update_is_labeled() → TaskMixin._get_is_labeled_value()
      - tasks.functions.bulk_update_is_labeled_by_overlap()
    는 review_status 를 보지 않고 오직 distinct completed_by >= overlap 로만 판정한다.
    따라서 반려됐지만 annotation 이 남은 태스크가 재계산 시 is_labeled=True 로 되살아나
    라벨러 큐에서 사라지고 rejected 로 고착된다.
  기대: review_status='rejected' 태스크는 재계산 후에도 is_labeled=False.

  현행 구현 상태 (RED 근거):
    - _get_is_labeled_value / bulk_update_is_labeled_by_overlap 모두 review_status 무시
      → 재계산 후 is_labeled=True 로 바뀌어 기대(False)와 불일치.

A2. 2단계 전이 가드 — reviewer 는 first_approved(최종 대기) 태스크를 재승인 못 함
  배경: 2단계에서 reviewer 승인=first_approved(최종 대기, admin 몫). reviewer 가 이미
    first_approved 인 태스크를 다시 approve 하면 reviewed_by/reviewed_at 이 덮여
    최종검수 큐가 교란된다.
  기대: reviewer 가 first_approved 태스크 approve → 400(이미 1차 통과), 상태 불변.

  현행 구현 상태 (RED 근거):
    - TaskReviewAPI 는 reviewer approve 시 review_status==APPROVED 만 강등 금지로 막고,
      FIRST_APPROVED 는 그대로 다시 FIRST_APPROVED 로 기록하며 200 을 반환
      → 기대(400 + 상태/메타 불변)와 불일치.

주의(구현 금지 준수):
  - annotation post_save 신호가 rejected→pending 으로 리셋하므로, A1 의 rejected 상태는
    annotation 을 먼저 만든 뒤 ORM .update() 로 세팅해 신호 재트리거를 피한다.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.functions import bulk_update_is_labeled_by_overlap
from tasks.models import Task
from tasks.tests.factories import AnnotationFactory, TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class _ReviewTestBase(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.organization)

        cls.admin = cls.organization.created_by
        cls.reviewer = UserFactory(active_organization=cls.organization)
        cls.labeler = UserFactory(active_organization=cls.organization)

        set_role(cls.admin, cls.organization, ROLE_ADMIN)
        set_role(cls.reviewer, cls.organization, ROLE_REVIEWER)
        set_role(cls.labeler, cls.organization, ROLE_LABELER)

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    def _make_task(self):
        return TaskFactory(
            project=self.project, data={'text': 'x'}, assignee=self.labeler, is_labeled=True
        )

    def _add_annotation(self, task):
        return AnnotationFactory(
            task=task, project=self.project, completed_by=self.labeler, result=[]
        )

    def _review_url(self, task):
        return f'/api/tasks/{task.id}/review'

    def _review(self, user, task, action, note=''):
        self.client.force_authenticate(user=user)
        return self.client.post(
            self._review_url(task), {'action': action, 'note': note}, format='json'
        )


class TestRejectedTaskStaysUnlabeledOnRecompute(_ReviewTestBase):
    """A1. 반려 태스크는 is_labeled 재계산 경로에서도 False 를 유지해야 한다."""

    def _make_rejected_task_with_annotation(self):
        """annotation 이 남아있는 rejected 태스크 (현실적: 라벨→반려, annotation 잔존).

        annotation post_save 가 rejected→pending 리셋을 트리거하므로, annotation 을
        먼저 만든 뒤 ORM .update() 로 rejected/is_labeled=False 를 직접 세팅한다.
        """
        task = self._make_task()
        self._add_annotation(task)
        Task.objects.filter(pk=task.pk).update(
            review_status=Task.ReviewStatus.REJECTED, is_labeled=False
        )
        return Task.objects.get(pk=task.pk)

    # -------------------- 케이스 1: 단건 재계산 -------------------- #
    def test_single_update_is_labeled_keeps_rejected_task_unlabeled(self):
        """[RED 핵심] rejected + annotation 태스크에서 update_is_labeled() 후에도 is_labeled=False.

        RED 근거: _get_is_labeled_value 는 review_status 를 보지 않고
        distinct completed_by(=1) >= overlap(=1) 로만 판정 → is_labeled 를 True 로 되살린다.
        """
        task = self._make_rejected_task_with_annotation()
        assert task.review_status == Task.ReviewStatus.REJECTED
        assert task.is_labeled is False

        task.update_is_labeled()
        task.save()
        task.refresh_from_db()

        assert task.review_status == Task.ReviewStatus.REJECTED, (
            f'재계산이 review_status 를 바꾸면 안 됨, 실제={task.review_status!r}'
        )
        assert task.is_labeled is False, (
            'rejected 태스크는 update_is_labeled() 재계산 후에도 is_labeled=False 여야 함 '
            f'(라벨러 큐 유지), 실제={task.is_labeled}'
        )

    # -------------------- 케이스 2: 대량 재계산 -------------------- #
    def test_bulk_update_is_labeled_keeps_rejected_task_unlabeled(self):
        """[RED 핵심] bulk_update_is_labeled_by_overlap 후에도 rejected 태스크 is_labeled=False.

        RED 근거: bulk 경로는 annotator_count>=overlap 인 태스크를 무조건 is_labeled=True 로
        UPDATE 하며 review_status 를 배제하지 않는다 → rejected 태스크가 되살아난다.
        """
        task = self._make_rejected_task_with_annotation()

        bulk_update_is_labeled_by_overlap([task.id], self.project)

        task.refresh_from_db()
        assert task.review_status == Task.ReviewStatus.REJECTED, (
            f'재계산이 review_status 를 바꾸면 안 됨, 실제={task.review_status!r}'
        )
        assert task.is_labeled is False, (
            'rejected 태스크는 bulk 재계산 후에도 is_labeled=False 여야 함, '
            f'실제={task.is_labeled}'
        )

    # -------------------- 대조(회귀 방지 가드): rejected 아닌 태스크는 True 가능 -------------------- #
    def test_pending_task_recomputes_to_labeled(self):
        """대조 가드: rejected 가 아닌(pending) 태스크는 annotation 기준대로 True 로 재계산돼야 한다.

        이 가드는 현행 구현에서 이미 통과(green)해야 한다. 견고성 수정이 pending/approved
        태스크의 정상 재계산까지 막지 않도록 회귀 방지용으로 함께 고정한다.
        """
        task = self._make_task()
        self._add_annotation(task)
        # annotation 존재 상태에서 is_labeled 만 인위적으로 내려두고(pending 유지) 재계산.
        Task.objects.filter(pk=task.pk).update(
            review_status=Task.ReviewStatus.PENDING, is_labeled=False
        )
        task = Task.objects.get(pk=task.pk)

        bulk_update_is_labeled_by_overlap([task.id], self.project)

        task.refresh_from_db()
        assert task.is_labeled is True, (
            'pending 태스크는 annotation 충족 시 재계산으로 is_labeled=True 가 돼야 함(정상), '
            f'실제={task.is_labeled}'
        )


class TestSecondStageApproveGuard(_ReviewTestBase):
    """A2. reviewer 는 first_approved(최종 대기) 태스크를 재승인하지 못한다."""

    def _make_first_approved_task(self, reviewed_by):
        task = self._make_task()
        Task.objects.filter(pk=task.pk).update(
            review_status=Task.ReviewStatus.FIRST_APPROVED, reviewed_by=reviewed_by, is_labeled=True
        )
        return Task.objects.get(pk=task.pk)

    # -------------------- 핵심 RED -------------------- #
    def test_reviewer_cannot_reapprove_first_approved_task(self):
        """[RED 핵심] reviewer 가 first_approved 태스크 approve → 400, 상태/메타 불변.

        RED 근거: 현행 뷰는 reviewer approve 시 review_status==APPROVED 만 막고,
        FIRST_APPROVED 는 다시 FIRST_APPROVED 로 기록 + reviewed_by/reviewed_at 덮어쓰기 후
        200 을 반환한다 → 최종검수 큐 교란.
        """
        # 1차 통과는 다른 reviewer 가 처리한 것으로 가정 (재승인 시 메타 덮임 여부 확인용)
        first_reviewer = UserFactory(active_organization=self.organization)
        set_role(first_reviewer, self.organization, ROLE_REVIEWER)
        task = self._make_first_approved_task(reviewed_by=first_reviewer)

        response = self._review(self.reviewer, task, 'approve')

        assert response.status_code == 400, (
            'reviewer 의 first_approved 재승인은 400(이미 1차 통과, 최종검수 대기)이어야 함, '
            f'실제 status={response.status_code}, body={response.content}'
        )
        task.refresh_from_db()
        assert task.review_status == Task.ReviewStatus.FIRST_APPROVED, (
            f'거부된 재승인이 review_status 를 바꾸면 안 됨, 실제={task.review_status!r}'
        )
        assert task.reviewed_by_id == first_reviewer.id, (
            '거부된 재승인이 reviewed_by(1차 검수자)를 덮으면 안 됨, '
            f'실제={task.reviewed_by_id!r}'
        )

    # -------------------- 대조(회귀 방지 가드) -------------------- #
    def test_admin_can_finalize_first_approved_task(self):
        """대조 가드: admin 이 first_approved 태스크 approve → 200, approved (정상 최종 승인)."""
        task = self._make_first_approved_task(reviewed_by=self.reviewer)

        response = self._review(self.admin, task, 'approve')

        assert response.status_code == 200, (
            f'admin 최종 승인은 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )
        task.refresh_from_db()
        assert task.review_status == Task.ReviewStatus.APPROVED, (
            f'admin 최종 승인 후 review_status=approved 여야 함, 실제={task.review_status!r}'
        )
        assert task.reviewed_by_id == self.admin.id, (
            f'최종 승인의 reviewed_by 가 admin 이어야 함, 실제={task.reviewed_by_id!r}'
        )
