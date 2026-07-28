"""검수 워크플로 DataManager 일괄 액션 테스트 (GCP 외주 LS 포크).

data_manager/actions/review.py 의 approve_tasks / reject_tasks 가
- reviewer/admin 이 선택 태스크를 일괄 승인/반려하고 감사 이력을 남기며,
- 반려는 사유(note) 필수(없으면 400),
- labeler 는 거부(PermissionDenied)
하는지 검증한다. (프론트 빌드 없이 DM 액션으로 검수자 UI 제공)
"""
from data_manager.actions.review import approve_tasks, reject_tasks
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.exceptions import PermissionDenied, ValidationError
from tasks.models import Task, TaskReviewHistory
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory
from django.test import TestCase

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class _Req:
    """action entry_point 은 kwargs['request'].user / .data 만 사용한다."""

    def __init__(self, user, data=None):
        self.user = user
        self.data = data or {}


class TestReviewActions(TestCase):
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

    def _tasks(self, n=2):
        for _ in range(n):
            TaskFactory(project=self.project, is_labeled=True, assignee=self.labeler)
        return Task.objects.filter(project=self.project)

    def test_reviewer_approve_marks_first_approved_with_audit(self):
        """2단계: reviewer 일괄 승인 → first_approved(1차 통과) + 감사 first_approved."""
        qs = self._tasks(2)
        result = approve_tasks(self.project, qs, request=_Req(self.reviewer))
        assert result['processed_items'] == 2
        for t in qs:
            t.refresh_from_db()
            assert t.review_status == 'first_approved'
            assert t.reviewed_by_id == self.reviewer.id
        assert TaskReviewHistory.objects.filter(action='first_approved', actor=self.reviewer).count() == 2

    def test_admin_approve_marks_final_approved_with_audit(self):
        """2단계: admin 일괄 승인 → approved(최종) + 감사 approved."""
        qs = self._tasks(2)
        result = approve_tasks(self.project, qs, request=_Req(self.admin))
        assert result['processed_items'] == 2
        for t in qs:
            t.refresh_from_db()
            assert t.review_status == 'approved'
        assert TaskReviewHistory.objects.filter(action='approved', actor=self.admin).count() == 2

    def test_reject_sends_back_with_note_and_audit(self):
        qs = self._tasks(2)
        result = reject_tasks(self.project, qs, request=_Req(self.reviewer, {'note': '라벨 누락'}))
        assert result['processed_items'] == 2
        for t in qs:
            t.refresh_from_db()
            assert t.review_status == 'rejected'
            assert t.is_labeled is False
            assert t.review_note == '라벨 누락'
        assert TaskReviewHistory.objects.filter(action='rejected', note='라벨 누락').count() == 2

    def test_reject_without_note_is_rejected_400(self):
        qs = self._tasks(1)
        with self.assertRaises(ValidationError):
            reject_tasks(self.project, qs, request=_Req(self.reviewer, {'note': '   '}))

    def test_reject_missing_note_key_is_rejected_400(self):
        qs = self._tasks(1)
        with self.assertRaises(ValidationError):
            reject_tasks(self.project, qs, request=_Req(self.reviewer, {}))

    def test_labeler_cannot_approve(self):
        qs = self._tasks(1)
        with self.assertRaises(PermissionDenied):
            approve_tasks(self.project, qs, request=_Req(self.labeler))

    def test_labeler_cannot_reject(self):
        qs = self._tasks(1)
        with self.assertRaises(PermissionDenied):
            reject_tasks(self.project, qs, request=_Req(self.labeler, {'note': 'x'}))

    def test_admin_can_approve(self):
        qs = self._tasks(1)
        result = approve_tasks(self.project, qs, request=_Req(self.admin))
        assert result['processed_items'] == 1
