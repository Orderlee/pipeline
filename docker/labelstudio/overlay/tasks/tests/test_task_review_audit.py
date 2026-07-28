"""TDD Red (2차 사이클) — 태스크 검수의 감사 이력 + 방어 검증 + org 격리.

1차 사이클(POST /api/tasks/{pk}/review)은 이미 구현·통과됨:
  - Task.review_status / reviewed_by / reviewed_at / review_note 필드 존재
  - TaskReviewAPI (approve→approved, reject→rejected+is_labeled=False+note)
  - 재라벨 시 rejected→pending 리셋 (Annotation post_save 훅)

이번 사이클 범위(아직 미구현 → RED):

  1) 감사 이력 모델 TaskReviewHistory (아직 없음):
       필드 task(FK) / actor(FK User) / action('approved'|'rejected'|'reopened')
       / note / created_at, Meta.db_table='task_review_history'.
       - approve  → action='approved' row 1개 (task/actor 일치)
       - reject   → action='rejected' row 1개 + note 저장
       - 재라벨로 pending 리셋 시 → action='reopened' row 1개

  2) 방어 검증:
       - action='reject' 인데 note 비어있음/누락 → 400 (반려 사유 필수)
       (참고: 부정 action / action 키 누락 → 400 은 현재 뷰가 이미 처리(GREEN)라
        이번 RED 사이클에서 제외.)

  3) org 격리:
       - reviewer 가 자신의 active_organization 과 다른 org 의 태스크를
         review 시도 → 404 (존재 누설 금지)

주의(구현 금지 준수):
  - 아직 없는 모델(TaskReviewHistory)을 top-level import 하지 않는다
    (수집단계 ImportError 회피). 런타임 getattr(tasks.models, ...) 로 접근해
    부재를 깨끗한 assertion 실패(RED 근거)로 보고한다.
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

_MISSING = object()


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


def _history_model():
    """미존재 모델을 수집단계 ImportError 없이 런타임 참조 (1차 파일의 getattr 안전참조 패턴)."""
    import tasks.models as tasks_models

    return getattr(tasks_models, 'TaskReviewHistory', None)


class TestTaskReviewAudit(APITestCase):
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

        # org 격리 검증용 별도 조직 + 태스크 (org B, reviewer 의 active_organization 아님).
        cls.other_organization = OrganizationFactory()
        cls.other_project = ProjectFactory(organization=cls.other_organization)

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    def _make_task(self, project=None):
        return TaskFactory(
            project=project or self.project,
            data={'text': 'x'},
            assignee=self.labeler,
            is_labeled=True,
        )

    def _review_url(self, task):
        return f'/api/tasks/{task.id}/review'

    def _review(self, user, task, action=_MISSING, note=_MISSING):
        self.client.force_authenticate(user=user)
        payload = {}
        if action is not _MISSING:
            payload['action'] = action
        if note is not _MISSING:
            payload['note'] = note
        return self.client.post(self._review_url(task), payload, format='json')

    # ------------------------------------------------------------------ #
    # 1) 감사 이력 모델 TaskReviewHistory
    # ------------------------------------------------------------------ #
    def test_final_approve_writes_approved_audit_history_row(self):
        """2단계 스펙: 최종검수자(admin) 승인 시 TaskReviewHistory 에 action='approved' row 1개(actor=admin).

        (reviewer 1차 승인 → action='first_approved' 는 test_review_two_stage 에서 검증.)
        """
        task = self._make_task()
        response = self._review(self.admin, task, 'approve')
        assert response.status_code == 200, (
            f'approve 는 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )

        Hist = _history_model()
        assert Hist is not None, 'TaskReviewHistory 모델이 존재해야 함'
        rows = Hist.objects.filter(task=task, action='approved')
        assert rows.count() == 1, (
            f"admin 최종 승인 후 action='approved' 이력 row 1개여야 함, 실제={rows.count()}"
        )
        assert rows.first().actor_id == self.admin.id, (
            f'이력 actor 가 admin 여야 함, 실제={getattr(rows.first(), "actor_id", _MISSING)!r}'
        )

    def test_reject_writes_rejected_audit_history_row_with_note(self):
        """[RED 핵심] reject 시 action='rejected' row 1개 + note 저장.

        RED 근거: TaskReviewHistory 모델 부재.
        """
        task = self._make_task()
        response = self._review(self.reviewer, task, 'reject', note='라벨 누락')
        assert response.status_code == 200, (
            f'reject 는 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )

        Hist = _history_model()
        assert Hist is not None, 'TaskReviewHistory 모델이 존재해야 함 (아직 미구현 → RED)'
        rows = Hist.objects.filter(task=task, action='rejected')
        assert rows.count() == 1, (
            f"reject 후 action='rejected' 이력 row 1개여야 함, 실제={rows.count()}"
        )
        assert rows.first().note == '라벨 누락', (
            f'이력 note 에 반려 사유가 저장돼야 함, 실제={getattr(rows.first(), "note", _MISSING)!r}'
        )

    def test_relabel_after_reject_writes_reopened_audit_history_row(self):
        """[RED 핵심] 반려된 태스크 재라벨(annotation 재생성)로 pending 리셋될 때
        action='reopened' 이력 row 생성.

        RED 근거: TaskReviewHistory 모델 부재 + reopened 기록 훅 미구현.
        """
        task = self._make_task()
        reject = self._review(self.reviewer, task, 'reject', note='재작업 필요')
        assert reject.status_code == 200, (
            f'선행 reject 는 200 이어야 함, 실제 status={reject.status_code}, body={reject.content}'
        )

        # 라벨러 재작업 시뮬레이션: 새 annotation 생성 → update_is_labeled 경로에서 pending 리셋.
        AnnotationFactory(task=task, project=self.project, completed_by=self.labeler, result=[])

        Hist = _history_model()
        assert Hist is not None, 'TaskReviewHistory 모델이 존재해야 함 (아직 미구현 → RED)'
        rows = Hist.objects.filter(task=task, action='reopened')
        assert rows.count() == 1, (
            f"재라벨 pending 리셋 시 action='reopened' 이력 row 1개여야 함, 실제={rows.count()}"
        )

    # ------------------------------------------------------------------ #
    # 2) 방어 검증
    # ------------------------------------------------------------------ #
    # 주: action='foo'(부정 action) / action 키 누락 → 400 은 현재 뷰가 이미
    # ValidationError 로 처리(GREEN)하므로 이번 RED 사이클에서 제외했다.
    # (green 단계에서 방어 검증을 serializer 로 통합할 때 회귀 테스트로 추가 검토.)

    def test_reject_without_note_returns_400(self):
        """[RED 핵심] action='reject' 인데 note 비어있음 → 400 (반려 사유 필수).

        RED 근거: 현재 뷰는 note='' 여도 그대로 rejected 처리하여 200 반환.
        """
        task = self._make_task()
        response = self._review(self.reviewer, task, 'reject', note='')
        assert response.status_code == 400, (
            f'note 없는 reject 는 400 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )

    def test_missing_note_on_reject_returns_400(self):
        """[RED 핵심] action='reject' 인데 note 키 누락 → 400.

        RED 근거: 현재 뷰는 note 기본값 '' 로 처리하여 200 반환.
        """
        task = self._make_task()
        response = self._review(self.reviewer, task, 'reject')  # note 키 자체 누락
        assert response.status_code == 400, (
            f'note 누락 reject 는 400 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )

    # ------------------------------------------------------------------ #
    # 3) org 격리
    # ------------------------------------------------------------------ #
    def test_cross_org_task_review_returns_404(self):
        """[RED 핵심] org A reviewer 가 org B 태스크 pk 로 review → 404 (존재 누설 금지).

        RED 근거: 현재 뷰는 Task.objects.filter(pk=pk) 로 org 스코프 없이 조회하여
        교차 org 태스크에도 200 으로 검수가 새어나간다.
        """
        other_task = self._make_task(project=self.other_project)
        # reviewer 의 active_organization 은 org A, other_task 는 org B 소속.
        response = self._review(self.reviewer, other_task, 'approve')
        assert response.status_code == 404, (
            f'교차 org 태스크 review 는 404 여야 함(존재 누설 금지), 실제 status={response.status_code} '
            f'(200 이면 교차 org 누수)'
        )
