"""TDD Red — 태스크 검수를 **2단계 순차 검수**로 전환 (GCP 외주 LS 포크 전용, 자체완결형).

이번 사이클 범위: 단일 단계 승인(reviewer approve → approved)을
2단계 순차 승인으로 바꾼다.

  1) reviewer 1차 승인 : pending 태스크 → review_status='first_approved'
     (is_labeled 유지, 아직 최종 완료 아님).
  2) admin 최종 승인   : first_approved 태스크 → review_status='approved'.
  3) reviewer 반려     : → review_status='rejected', is_labeled=False, note 필수(공통).
  4) admin 반려        : → review_status='rejected', is_labeled=False, note 필수(공통).
  5) 재라벨            : rejected 태스크에 annotation 재생성 → review_status='pending'(reopened).
  6) 감사 이력         : first_approved 전이가 TaskReviewHistory 에 기록(신규 Action 값).

현행 구현 상태 (RED 근거):
  - Task.ReviewStatus 에 FIRST_APPROVED='first_approved' 값이 없음.
  - TaskReviewAPI 는 reviewer/admin 구분 없이 approve → 'approved' 로 즉시 확정(단일 단계).
  - TaskReviewHistory.Action 에 'first_approved' 값이 없어 1차 승인 이력을 남기지 못함.

주의(구현 금지 준수):
  - 아직 없는 상태값/Action 값을 enum 멤버로 import 하지 않는다(수집단계 오류 회피).
  - 문자열 'first_approved' 로 직접 비교하여 값 부재를 깨끗한 assertion 실패(RED)로 보고한다.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.models import Annotation, TaskReviewHistory
from tasks.tests.factories import AnnotationFactory, TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

_MISSING = object()


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


def _history_model():
    """미존재 Action 값 등을 수집단계 오류 없이 런타임 참조."""
    import tasks.models as tasks_models

    return getattr(tasks_models, 'TaskReviewHistory', None)


class TestReviewTwoStage(APITestCase):
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

    def _review_url(self, task):
        return f'/api/tasks/{task.id}/review'

    def _review(self, user, task, action, note=''):
        self.client.force_authenticate(user=user)
        return self.client.post(
            self._review_url(task), {'action': action, 'note': note}, format='json'
        )

    # ------------------------------------------------------------------ #
    # 1) reviewer 1차 승인: pending → first_approved
    # ------------------------------------------------------------------ #
    def test_reviewer_approve_moves_pending_to_first_approved(self):
        """[RED 핵심] reviewer 의 승인은 최종 완료가 아니라 1차 통과(first_approved)여야 한다.

        RED 근거: 현행은 곧바로 'approved' 로 확정하며 'first_approved' 상태값 자체가 없다.
        """
        task = self._make_task()
        response = self._review(self.reviewer, task, 'approve')
        assert response.status_code == 200, (
            f'reviewer 1차 승인은 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )
        task.refresh_from_db()
        assert getattr(task, 'review_status', _MISSING) == 'first_approved', (
            "reviewer 1차 승인 후 review_status='first_approved' 여야 함 "
            f'(실제={getattr(task, "review_status", _MISSING)!r})'
        )
        assert task.is_labeled is True, (
            f'1차 승인은 is_labeled 를 유지해야 함, 실제={task.is_labeled}'
        )

    # ------------------------------------------------------------------ #
    # 2) admin 최종 승인: first_approved → approved
    # ------------------------------------------------------------------ #
    def test_admin_approve_moves_first_approved_to_approved(self):
        """[RED 핵심] admin 이 first_approved 태스크를 승인하면 최종 approved 로 확정.

        RED 근거: 'first_approved' 값 부재 + 뷰에 1차→최종 승인 전이 로직 없음.
        """
        task = self._make_task()
        task.review_status = 'first_approved'
        task.save(update_fields=['review_status'])

        response = self._review(self.admin, task, 'approve')
        assert response.status_code == 200, (
            f'admin 최종 승인은 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )
        task.refresh_from_db()
        assert getattr(task, 'review_status', _MISSING) == 'approved', (
            "admin 최종 승인 후 review_status='approved' 여야 함 "
            f'(실제={getattr(task, "review_status", _MISSING)!r})'
        )
        assert getattr(task, 'reviewed_by_id', _MISSING) == self.admin.id, (
            f'최종 승인의 reviewed_by 가 admin 이어야 함, 실제={getattr(task, "reviewed_by_id", _MISSING)!r}'
        )

    # ------------------------------------------------------------------ #
    # 3) reviewer 반려 (공통 규칙 유지)
    # ------------------------------------------------------------------ #
    def test_reviewer_reject_sets_rejected_and_unlabels(self):
        """reviewer 반려 → rejected + is_labeled=False + note 저장 (현행 유지/보강)."""
        task = self._make_task()
        response = self._review(self.reviewer, task, 'reject', note='라벨 누락')
        assert response.status_code == 200, (
            f'reviewer 반려는 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )
        task.refresh_from_db()
        assert getattr(task, 'review_status', _MISSING) == 'rejected', (
            f"reviewer 반려 후 review_status='rejected' 여야 함, 실제={getattr(task, 'review_status', _MISSING)!r}"
        )
        assert task.is_labeled is False, (
            f'반려 후 is_labeled=False 여야 함, 실제={task.is_labeled}'
        )

    # ------------------------------------------------------------------ #
    # 4) admin 반려 (공통 규칙 유지)
    # ------------------------------------------------------------------ #
    def test_admin_reject_sets_rejected_and_unlabels(self):
        """admin 반려 → rejected + is_labeled=False (first_approved 여부와 무관하게)."""
        task = self._make_task()
        task.review_status = 'first_approved'
        task.save(update_fields=['review_status'])

        response = self._review(self.admin, task, 'reject', note='최종 검수 반려')
        assert response.status_code == 200, (
            f'admin 반려는 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )
        task.refresh_from_db()
        assert getattr(task, 'review_status', _MISSING) == 'rejected', (
            f"admin 반려 후 review_status='rejected' 여야 함, 실제={getattr(task, 'review_status', _MISSING)!r}"
        )
        assert task.is_labeled is False, (
            f'admin 반려 후 is_labeled=False 여야 함, 실제={task.is_labeled}'
        )

    # ------------------------------------------------------------------ #
    # 5) 재라벨: rejected → pending (reopened)
    # ------------------------------------------------------------------ #
    def test_relabel_after_reject_resets_to_pending(self):
        """반려된 태스크에 annotation 재생성 → review_status='pending' 리셋 (기존 동작 유지)."""
        task = self._make_task()
        self._review(self.reviewer, task, 'reject', note='재작업 필요')

        AnnotationFactory(task=task, project=self.project, completed_by=self.labeler, result=[])

        task.refresh_from_db()
        assert getattr(task, 'review_status', _MISSING) == 'pending', (
            f"재라벨 후 review_status='pending' 으로 리셋돼야 함, 실제={getattr(task, 'review_status', _MISSING)!r}"
        )

    # ------------------------------------------------------------------ #
    # 6) 감사 이력: first_approved 전이 기록 (신규 Action 값)
    # ------------------------------------------------------------------ #
    def test_first_approval_writes_first_approved_audit_row(self):
        """[RED 핵심] reviewer 1차 승인 시 TaskReviewHistory 에 action='first_approved' row 기록.

        RED 근거: 현행은 approve 를 action='approved' 로 기록하며 'first_approved' Action 값이 없다.
        """
        task = self._make_task()
        response = self._review(self.reviewer, task, 'approve')
        assert response.status_code == 200, (
            f'reviewer 1차 승인은 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )

        Hist = _history_model()
        assert Hist is not None, 'TaskReviewHistory 모델이 존재해야 함'
        rows = Hist.objects.filter(task=task, action='first_approved')
        assert rows.count() == 1, (
            f"1차 승인 후 action='first_approved' 이력 row 1개여야 함, 실제={rows.count()} "
            f'(현행은 approved 로 기록 → RED)'
        )
        assert rows.first().actor_id == self.reviewer.id, (
            f'1차 승인 이력 actor 가 reviewer 여야 함, 실제={getattr(rows.first(), "actor_id", _MISSING)!r}'
        )

    # ------------------------------------------------------------------ #
    # 7) 전이 가드 (독립 리뷰 HIGH-1: 강등/모순 방지)
    # ------------------------------------------------------------------ #
    def test_reviewer_cannot_downgrade_approved(self):
        """이미 최종 승인(approved)된 태스크를 reviewer 가 approve 해도 강등되면 안 된다.
        (프로덕션 백필 approved 데이터 보호.)"""
        task = self._make_task()
        # admin 최종 승인 → approved
        assert self._review(self.admin, task, 'approve').status_code == 200
        task.refresh_from_db()
        assert task.review_status == 'approved'
        # reviewer 가 다시 approve 시도 → 강등 금지(400) + 상태 유지
        resp = self._review(self.reviewer, task, 'approve')
        assert resp.status_code == 400, (
            f'approved 태스크의 reviewer 재승인은 강등 금지(400)여야 함, 실제={resp.status_code}'
        )
        task.refresh_from_db()
        assert task.review_status == 'approved', (
            f'reviewer 재승인이 approved 를 강등시키면 안 됨, 실제={task.review_status}'
        )

    def test_approve_requires_labeled_task(self):
        """라벨(제출)되지 않은 태스크는 승인 불가(400). rejected(is_labeled=False)도 마찬가지 —
        재라벨 후에만 다시 검수 대상."""
        task = TaskFactory(project=self.project, data={'text': 'x'}, assignee=self.labeler, is_labeled=False)
        resp_r = self._review(self.reviewer, task, 'approve')
        resp_a = self._review(self.admin, task, 'approve')
        assert resp_r.status_code == 400 and resp_a.status_code == 400, (
            f'미라벨 태스크 승인은 400 이어야 함, 실제 reviewer={resp_r.status_code} admin={resp_a.status_code}'
        )

    def test_annotation_create_persists_active_seconds(self):
        """어노테이션 생성 시 프론트가 보낸 active_seconds(순수 활성시간)가 DB에 저장된다.
        lead_time 과 병행 — 둘 다 보존되고 active < lead 도 허용."""
        task = TaskFactory(project=self.project, data={'text': 'x'}, assignee=self.labeler)
        self.client.force_authenticate(user=self.labeler)
        resp = self.client.post(
            f'/api/tasks/{task.id}/annotations/',
            data={'result': [], 'lead_time': 300.0, 'active_seconds': 180.0},
            format='json',
        )
        assert resp.status_code == 201, resp.content
        ann = Annotation.objects.get(task=task)
        assert ann.active_seconds == 180.0 and ann.lead_time == 300.0
        # 응답 직렬화에도 노출(AnnotationSerializer exclude 방식 → 자동 포함)
        assert resp.json().get('active_seconds') == 180.0

    def test_annotation_create_active_seconds_optional(self):
        """active_seconds 미전송(구 프론트/도입 전)도 정상 201, 필드는 null."""
        task = TaskFactory(project=self.project, data={'text': 'x'}, assignee=self.labeler)
        self.client.force_authenticate(user=self.labeler)
        resp = self.client.post(
            f'/api/tasks/{task.id}/annotations/', data={'result': [], 'lead_time': 50.0}, format='json'
        )
        assert resp.status_code == 201, resp.content
        assert Annotation.objects.get(task=task).active_seconds is None

    def test_reject_stores_reason_code(self):
        """반려 시 사유 코드(Error Class)가 이력에 저장된다 — 사유별 통계의 원천."""
        task = self._make_task()
        self.client.force_authenticate(user=self.reviewer)
        resp = self.client.post(
            self._review_url(task), {'action': 'reject', 'note': '박스 어긋남', 'reason': 'boundary'}, format='json'
        )
        assert resp.status_code == 200
        row = TaskReviewHistory.objects.filter(task=task, action=TaskReviewHistory.Action.REJECTED).latest('id')
        assert row.reason_code == 'boundary'

    def test_reject_reason_optional_and_validated(self):
        """reason 미지정(구 클라이언트)은 허용(null=미분류), 미정의 코드는 400."""
        task = self._make_task()
        assert self._review(self.reviewer, task, 'reject', note='사유없이').status_code == 200
        assert (
            TaskReviewHistory.objects.filter(task=task, action=TaskReviewHistory.Action.REJECTED)
            .latest('id')
            .reason_code
            is None
        )
        task2 = self._make_task()
        self.client.force_authenticate(user=self.reviewer)
        resp = self.client.post(
            self._review_url(task2), {'action': 'reject', 'note': 'x', 'reason': 'not-a-code'}, format='json'
        )
        assert resp.status_code == 400, f'미정의 사유 코드는 400 이어야 함, 실제={resp.status_code}'

    def test_admin_reapprove_is_idempotent(self):
        """admin 이 approved 태스크를 다시 approve → approved 유지(멱등, 오류 없음)."""
        task = self._make_task()
        assert self._review(self.admin, task, 'approve').status_code == 200
        resp = self._review(self.admin, task, 'approve')
        assert resp.status_code == 200
        task.refresh_from_db()
        assert task.review_status == 'approved'
