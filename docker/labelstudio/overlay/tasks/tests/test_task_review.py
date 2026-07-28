"""TDD Red — 태스크 단위 검수 워크플로 (GCP 외주 LS 포크 전용, 자체완결형).

이번 사이클 범위: 신규 엔드포인트 **POST /api/tasks/{pk}/review** 한정.
검수자(reviewer)/관리자(admin)가 라벨러의 태스크 결과를 건별 승인/반려하고,
반려분이 라벨러 큐(is_labeled=False)로 돌아가는 루프.

  body: {"action": "approve" | "reject", "note": "..."}

신규 모델 필드/상태(아직 없음 → 그래서 RED):
  - Task.review_status : pending(기본) / approved / rejected
  - Task.reviewed_by   : FK User, nullable
  - Task.reviewed_at   : datetime, nullable
  - Task.review_note   : text

검증하려는 목표 동작:
  1) 권한 매트릭스:
       - role='labeler'  → 403 (검수 write 불가, 격리 유지)
       - role='reviewer' → 200
       - role='admin'    → 200
       - 비인증          → 401/403
  2) 반려(reject): reviewer 가 action=reject, note="사유" →
       review_status='rejected', is_labeled=False(라벨러 큐 복귀),
       review_note=사유, reviewed_by=reviewer.
  3) 재작업 후 pending 복귀: 반려된 태스크에 라벨러가 annotation 을
       다시 만들면(=재라벨) review_status 가 pending 으로 리셋.
  4) 승인(approve): reviewer 가 action=approve →
       review_status='approved', reviewed_by/reviewed_at 설정.
  5) 라벨러 격리 유지(읽기 허용): labeler 는 review write 불가지만,
       본인 태스크의 review_note/review_status 는 태스크 상세 GET 응답에서 읽을 수 있어야 함.

현재 구현 상태 (Red 근거):
  - tasks/urls.py 에 review 라우트 없음, tasks/api.py 에 뷰 없음
    → 어느 역할이 POST 해도 404. 기대(200/403/401)와 불일치하여 RED.
  - Task.review_status / reviewed_by / reviewed_at / review_note 필드가 없음
    → 상태 전이 검증 및 태스크 상세 GET 노출 검증이 실패하여 RED.

주의(구현 금지 준수):
  - 아직 없는 모델/필드를 top-level import 하지 않는다(수집단계 ImportError 회피).
  - review_status 등은 getattr(task, ..., <sentinel>) 로 접근해 필드 부재를
    깨끗한 assertion 실패(RED 근거)로 보고한다.

green 단계 예상 최소 구현:
  - Task 에 review_status/reviewed_by/reviewed_at/review_note 필드 + 마이그레이션.
  - 신규 뷰 POST /api/tasks/{pk}/review (tasks/api.py) + tasks/urls.py 라우트,
    permission: labeler 차단(reviewer/admin 허용).
  - reject → review_status='rejected', is_labeled=False, review_note 저장, reviewed_by 세팅.
  - approve → review_status='approved', reviewed_by/reviewed_at 세팅.
  - annotation (재)생성 시 review_status='pending' 리셋 (update_is_labeled 경로 훅).
  - 태스크 상세 serializer 에 review_status/review_note 노출.
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


class TestTaskReview(APITestCase):
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
        # labeler 에게 배정된, 라벨링 완료된(is_labeled) 태스크 하나.
        return TaskFactory(
            project=self.project, data={'text': 'x'}, assignee=self.labeler, is_labeled=True
        )

    def _review_url(self, task):
        # 명세 그대로: POST /api/tasks/{pk}/review
        return f'/api/tasks/{task.id}/review'

    def _review(self, user, task, action, note=''):
        self.client.force_authenticate(user=user)
        return self.client.post(
            self._review_url(task), {'action': action, 'note': note}, format='json'
        )

    # ------------------------------------------------------------------ #
    # 1) 권한 매트릭스
    # ------------------------------------------------------------------ #
    def test_labeler_cannot_review(self):
        """[RED 핵심] labeler 는 review write 에서 403 (구현 전엔 엔드포인트 부재로 404)."""
        task = self._make_task()
        response = self._review(self.labeler, task, 'approve')
        assert response.status_code == 403, (
            f'labeler 는 review 차단(403)돼야 함, 실제 status={response.status_code} '
            f'(엔드포인트 미구현 시 404)'
        )

    def test_reviewer_can_review(self):
        """[RED 핵심] reviewer 의 review 호출 → 200 (구현 전엔 404)."""
        task = self._make_task()
        response = self._review(self.reviewer, task, 'approve')
        assert response.status_code == 200, (
            f'reviewer 는 review 허용(200)돼야 함, 실제 status={response.status_code}'
        )

    def test_admin_can_review(self):
        """[RED] admin 의 review 호출 → 200 (구현 전엔 404)."""
        task = self._make_task()
        response = self._review(self.admin, task, 'approve')
        assert response.status_code == 200, (
            f'admin 은 review 허용(200)돼야 함, 실제 status={response.status_code}'
        )

    def test_anonymous_cannot_review(self):
        """[RED] 비인증 요청은 review 에서 401/403 이어야 함."""
        task = self._make_task()
        response = self.client.post(
            self._review_url(task), {'action': 'approve'}, format='json'
        )
        assert response.status_code in (401, 403), (
            f'비인증 review 는 401/403 이어야 함, 실제 status={response.status_code} '
            f'(엔드포인트 미구현 시 404)'
        )

    # ------------------------------------------------------------------ #
    # 2) 반려(reject)
    # ------------------------------------------------------------------ #
    def test_reject_sends_task_back_to_labeler_queue(self):
        """[RED 핵심] reviewer 반려 → review_status='rejected', is_labeled=False,
        review_note=사유, reviewed_by=reviewer.

        RED 근거: /review 엔드포인트 부재(404)로 첫 assertion 부터 실패하며,
        review_status/review_note/reviewed_by 필드도 없어 상태 전이 검증 불가.
        """
        task = self._make_task()
        response = self._review(self.reviewer, task, 'reject', note='라벨 누락')
        assert response.status_code == 200, (
            f'reject 는 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )
        task.refresh_from_db()
        assert getattr(task, 'review_status', _MISSING) == 'rejected', (
            "reject 후 review_status='rejected' 여야 함 "
            f'(실제={getattr(task, "review_status", _MISSING)!r}; 필드 부재면 <object>)'
        )
        assert task.is_labeled is False, (
            'reject 후 is_labeled=False (라벨러 큐 복귀)여야 함, 실제={}'.format(task.is_labeled)
        )
        assert getattr(task, 'review_note', _MISSING) == '라벨 누락', (
            f'reject 사유가 review_note 에 저장돼야 함, 실제={getattr(task, "review_note", _MISSING)!r}'
        )
        assert getattr(task, 'reviewed_by_id', _MISSING) == self.reviewer.id, (
            f'reviewed_by 가 reviewer 여야 함, 실제={getattr(task, "reviewed_by_id", _MISSING)!r}'
        )

    # ------------------------------------------------------------------ #
    # 3) 재작업 후 pending 복귀
    # ------------------------------------------------------------------ #
    def test_relabel_after_reject_resets_status_to_pending(self):
        """[RED 핵심] 반려된 태스크에 라벨러가 annotation 을 다시 만들면 review_status='pending' 리셋.

        RED 근거: reject 자체가 엔드포인트 부재(404)로 불가하고, review_status 필드도 없어
        재라벨 시 pending 리셋 훅이 존재하지 않는다.
        """
        task = self._make_task()
        reject = self._review(self.reviewer, task, 'reject', note='재작업 필요')
        assert reject.status_code == 200, (
            f'선행 reject 는 200 이어야 함, 실제 status={reject.status_code}, body={reject.content}'
        )

        # 라벨러 재작업 시뮬레이션: 새 annotation 생성 → update_is_labeled 경로 트리거
        AnnotationFactory(task=task, project=self.project, completed_by=self.labeler, result=[])

        task.refresh_from_db()
        assert getattr(task, 'review_status', _MISSING) == 'pending', (
            "재라벨 후 review_status 가 'pending' 으로 리셋돼야 함, "
            f'실제={getattr(task, "review_status", _MISSING)!r}'
        )

    # ------------------------------------------------------------------ #
    # 4) 승인(approve) — 2단계 순차 검수로 변경
    #     reviewer 승인 → first_approved(1차 통과) → admin 승인 → approved(최종)
    # ------------------------------------------------------------------ #
    def test_approve_marks_task_approved_with_reviewer_metadata(self):
        """[RED 핵심] 2단계 스펙: reviewer 의 1차 승인 → review_status='first_approved'
        (approved 아님), reviewed_by/reviewed_at 설정, is_labeled 유지.

        RED 근거: 현행 뷰는 reviewer approve 를 곧바로 'approved' 로 확정하고,
        Task.ReviewStatus 에 'first_approved' 값 자체가 없어 1차 승인 상태를 표현하지 못한다.
        """
        task = self._make_task()
        response = self._review(self.reviewer, task, 'approve')
        assert response.status_code == 200, (
            f'reviewer 1차 승인은 200 이어야 함, 실제 status={response.status_code}, body={response.content}'
        )
        task.refresh_from_db()
        assert getattr(task, 'review_status', _MISSING) == 'first_approved', (
            "reviewer 1차 승인 후 review_status='first_approved' 여야 함 "
            f'(실제={getattr(task, "review_status", _MISSING)!r}; 현행은 곧바로 approved 확정 → RED)'
        )
        assert task.is_labeled is True, (
            f'1차 승인은 is_labeled 를 유지해야 함, 실제={task.is_labeled}'
        )
        assert getattr(task, 'reviewed_by_id', _MISSING) == self.reviewer.id, (
            f'reviewed_by 가 reviewer 여야 함, 실제={getattr(task, "reviewed_by_id", _MISSING)!r}'
        )
        assert getattr(task, 'reviewed_at', _MISSING) not in (_MISSING, None), (
            f'1차 승인 후 reviewed_at 이 채워져야 함, 실제={getattr(task, "reviewed_at", _MISSING)!r}'
        )

    def test_admin_approve_of_first_approved_marks_approved(self):
        """[RED 핵심] 2단계 스펙: admin 이 first_approved 태스크를 승인 → review_status='approved'
        (최종 완료), reviewed_by=admin.

        RED 근거: Task.ReviewStatus 에 'first_approved' 값이 없어 초기 상태 세팅부터
        의미가 없고, 뷰에 1차→최종 승인 전이 로직이 없다.
        """
        task = self._make_task()
        # 1차 통과 상태를 ORM 으로 직접 세팅 (구현 부재 시에도 admin 최종승인 전이만 검증)
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

    def test_approve_clears_prior_rejection_note(self):
        """반려(사유 기록) → reviewer 재승인 시, 이전 반려사유가 남지 않아야 한다(MED-2).

        2단계 스펙: reviewer 재승인은 1차 통과(first_approved)로 가며 이전 반려사유는 제거돼야 한다.
        RED 근거: 현행은 reviewer approve 를 'approved' 로 확정한다('first_approved' 미도달).
        """
        task = self._make_task()
        self._review(self.reviewer, task, 'reject', note='라벨 누락')
        task.refresh_from_db()
        assert task.review_note == '라벨 누락'
        assert task.is_labeled is False  # 반려로 라벨러 큐 복귀
        # 라벨러 재작업(재라벨) 시뮬레이션: 다시 라벨됨(승인은 라벨된 태스크에만 가능).
        # 재작업 후에도 이전 반려사유(review_note)가 남아 있는 상태에서 재검수 승인.
        task.is_labeled = True
        task.save(update_fields=['is_labeled'])
        response = self._review(self.reviewer, task, 'approve')
        assert response.status_code == 200, response.content
        task.refresh_from_db()
        assert task.review_status == 'first_approved', (
            f"reviewer 재승인은 1차 통과(first_approved)여야 함, 실제={task.review_status!r}"
        )
        assert (task.review_note or '') == '', (
            f'승인 후 이전 반려사유가 제거돼야 함, 실제={task.review_note!r}'
        )

    # ------------------------------------------------------------------ #
    # 5) 라벨러 격리 유지(읽기 허용)
    # ------------------------------------------------------------------ #
    def test_labeler_can_read_own_review_status_in_task_detail(self):
        """[RED 핵심] labeler 는 review write 불가지만, 본인 태스크 상세 GET 응답에서
        review_status/review_note 를 읽을 수 있어야 한다.

        RED 근거: Task 에 review_status/review_note 필드가 없어 태스크 상세 serializer 가
        해당 키를 노출하지 못한다.
        """
        task = self._make_task()
        self.client.force_authenticate(user=self.labeler)
        response = self.client.get(f'/api/tasks/{task.id}/')
        assert response.status_code == 200, (
            f'labeler 는 본인 태스크 상세 조회(200)돼야 함, 실제 status={response.status_code}'
        )
        body = response.json()
        assert 'review_status' in body, (
            f'태스크 상세 응답에 review_status 키가 있어야 함(라벨러 읽기 허용): keys={list(body)}'
        )
        assert 'review_note' in body, (
            f'태스크 상세 응답에 review_note 키가 있어야 함(라벨러 읽기 허용): keys={list(body)}'
        )
