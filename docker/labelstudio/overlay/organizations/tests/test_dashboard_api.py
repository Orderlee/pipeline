"""admin 전용 현황 대시보드 집계 API (GCP 외주 LS 포크 전용).

계약:
  1) 권한: admin(멤버십 role=admin / org owner / superuser)만 200.
     reviewer·labeler 는 403 (labeler 는 미들웨어 default-deny, reviewer 는 뷰 내 is_admin 게이트).
     비인증은 401.
  2) 집계: org 스코프. summary(프로젝트/태스크/완료/reopened + 검수상태별) ·
     projects[](프로젝트별 총/완료/검수상태별) · workers[](Annotation.completed_by 기준,
     was_cancelled 제외 — 라이브 데이터가 assignee 미설정이라 completed_by 가 실제 작업자).
  3) 타 org 데이터는 집계에 포함되지 않는다.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.models import Task, TaskReviewHistory
from tasks.tests.factories import AnnotationFactory, TaskFactory
from users.tests.factories import UserFactory

URL = '/api/dashboard/summary'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestDashboardSummaryAPI(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = cls.org = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.org)
        cls.project2 = ProjectFactory(organization=cls.org)

        cls.owner = cls.org.created_by  # is_admin=True (owner 관례)
        cls.admin = UserFactory(active_organization=cls.org)
        cls.reviewer = UserFactory(active_organization=cls.org)
        cls.labeler = UserFactory(active_organization=cls.org)
        set_role(cls.admin, cls.org, 'admin')
        set_role(cls.reviewer, cls.org, 'reviewer')
        set_role(cls.labeler, cls.org, 'labeler')

        # project1: pending 2(그중 1 라벨완료) / first_approved 1 / approved 1 / rejected 1 = 총 5
        cls.t_pending_unlabeled = TaskFactory(project=cls.project, data={'x': 1})
        cls.t_pending_labeled = TaskFactory(project=cls.project, data={'x': 2}, is_labeled=True)
        cls.t_first = TaskFactory(
            project=cls.project, data={'x': 3}, is_labeled=True, review_status=Task.ReviewStatus.FIRST_APPROVED
        )
        cls.t_approved = TaskFactory(
            project=cls.project, data={'x': 4}, is_labeled=True, review_status=Task.ReviewStatus.APPROVED
        )
        cls.t_rejected = TaskFactory(project=cls.project, data={'x': 5}, review_status=Task.ReviewStatus.REJECTED)
        # project2: pending 1 = 총 1 (작업 전)
        TaskFactory(project=cls.project2, data={'x': 6})

        # 작업자별: labeler 가 project1 태스크 2건에 주석 3개(+skip 1건은 제외돼야 함)
        AnnotationFactory(task=cls.t_pending_labeled, project=cls.project, completed_by=cls.labeler)
        AnnotationFactory(task=cls.t_first, project=cls.project, completed_by=cls.labeler)
        AnnotationFactory(task=cls.t_first, project=cls.project, completed_by=cls.labeler)
        AnnotationFactory(
            task=cls.t_pending_unlabeled, project=cls.project, completed_by=cls.labeler, was_cancelled=True
        )
        # reviewer 도 1건 주석(작업자 표에 나옴 — completed_by 기준의 사실 기록)
        AnnotationFactory(task=cls.t_approved, project=cls.project, completed_by=cls.reviewer)

        # 생산성 지표 시드: labeler 의 3개 주석에 lead_time(체류)·active_seconds(순수)·result_count(객체) 부여.
        # 활성시간은 체류시간보다 작아야 정상(자리비움 제외). 합계 lead=300·active=180, 객체=6.
        from tasks.models import Annotation as _Ann

        for ann, lead, active, objs in zip(
            _Ann.objects.filter(completed_by=cls.labeler, was_cancelled=False).order_by('id'),
            [100.0, 120.0, 80.0],
            [60.0, 80.0, 40.0],
            [2, 3, 1],
        ):
            _Ann.objects.filter(pk=ann.pk).update(lead_time=lead, active_seconds=active, result_count=objs)

        # reopened 이력 1건 (반려→재작업 왕복 흔적) + labeler 태스크의 누적 반려 이벤트 1건(품질 이력)
        TaskReviewHistory.objects.create(
            task=cls.t_pending_labeled, actor=cls.admin, action=TaskReviewHistory.Action.REOPENED
        )
        TaskReviewHistory.objects.create(
            task=cls.t_first, actor=cls.reviewer, action=TaskReviewHistory.Action.REJECTED, note='다시'
        )

        # AnnotationFactory 는 result 미설정이라 저장 시그널이 is_labeled 를 False 로 재계산한다.
        # 이 테스트는 집계 검증이 목적이므로 시드 후 상태를 update()(시그널 우회)로 고정.
        Task.objects.filter(pk__in=[cls.t_pending_labeled.pk, cls.t_first.pk, cls.t_approved.pk]).update(
            is_labeled=True
        )

        # 타 org 노이즈: 집계에 포함되면 안 됨
        other_org = OrganizationFactory()
        other_project = ProjectFactory(organization=other_org)
        t_other = TaskFactory(project=other_project, data={'x': 99}, review_status=Task.ReviewStatus.APPROVED)
        AnnotationFactory(task=t_other, project=other_project, completed_by=other_org.created_by)

    # ------------------------------ 권한 ------------------------------ #
    def test_admin_role_gets_200(self):
        self.client.force_authenticate(self.admin)
        assert self.client.get(URL).status_code == 200

    def test_owner_gets_200(self):
        self.client.force_authenticate(self.owner)
        assert self.client.get(URL).status_code == 200

    def test_reviewer_gets_403(self):
        self.client.force_authenticate(self.reviewer)
        assert self.client.get(URL).status_code == 403

    def test_labeler_gets_403(self):
        self.client.force_authenticate(self.labeler)
        assert self.client.get(URL).status_code == 403

    def test_anonymous_gets_401(self):
        assert self.client.get(URL).status_code == 401

    # ------------------------------ 집계 ------------------------------ #
    def _get(self):
        self.client.force_authenticate(self.admin)
        resp = self.client.get(URL)
        assert resp.status_code == 200
        return resp.json()

    def test_summary_counts(self):
        s = self._get()['summary']
        assert s['projects'] == 2
        assert s['tasks'] == 6
        assert s['labeled'] == 3  # is_labeled=True 3건 (타 org 제외)
        assert s['reopened'] == 1
        assert s['review'] == {'pending': 3, 'first_approved': 1, 'approved': 1, 'rejected': 1}

    def test_projects_breakdown(self):
        rows = {p['id']: p for p in self._get()['projects']}
        assert set(rows) == {self.project.id, self.project2.id}
        p1 = rows[self.project.id]
        assert p1['total'] == 5 and p1['labeled'] == 3
        assert (p1['pending'], p1['first_approved'], p1['approved'], p1['rejected']) == (2, 1, 1, 1)
        p2 = rows[self.project2.id]
        assert p2['total'] == 1 and p2['labeled'] == 0 and p2['pending'] == 1

    def test_workers_by_completed_by_excluding_cancelled(self):
        workers = {w['email']: w for w in self._get()['workers']}
        assert set(workers) == {self.labeler.email, self.reviewer.email}
        lab = workers[self.labeler.email]
        assert lab['tasks'] == 2, f'skip(was_cancelled) 주석은 제외돼야 함, 실제={lab}'
        assert lab['annotations'] == 3
        assert lab['last_activity'] is not None
        assert workers[self.reviewer.email]['annotations'] == 1

    def test_worker_quality_metrics(self):
        """작업자 품질 지표: 상태 분포·반려 태스크 비율(0~100 보장)·1차/최종 반려 분리·활동일."""
        workers = {w['email']: w for w in self._get()['workers']}
        lab = workers[self.labeler.email]
        assert lab['status'] == {'pending': 1, 'first_approved': 1, 'approved': 0, 'rejected': 0}
        assert lab['rejected_events'] == 1 and lab['reopened_events'] == 1
        # 반려 태스크 비율 = 반려된 적 있는 태스크(중복 제거) 1 / 라벨 태스크 2 = 50%
        assert lab['rejected_tasks'] == 1 and lab['rejected_task_rate'] == 50.0
        assert lab['avg_rejects'] == 1.0 and lab['approval_rate'] == 0.0
        # 반려 행위자 분리: reviewer 가 반려 → 1차 반려 1·최종 반려 0
        assert lab['rejected_first'] == 1 and lab['rejected_final'] == 0
        assert lab['active_days'] == 1 and lab['avg_per_day'] == 3.0
        rev = workers[self.reviewer.email]
        assert rev['status']['approved'] == 1 and rev['approval_rate'] == 100.0
        assert rev['rejected_events'] == 0 and rev['rejected_task_rate'] == 0.0

    def test_rejected_task_rate_capped_on_multiple_rejects(self):
        """같은 태스크가 여러 번 반려돼도 '반려 태스크 비율'은 100% 를 넘지 않는다(누적은 평균 반려로)."""
        TaskReviewHistory.objects.create(
            task=self.t_first, actor=self.reviewer, action=TaskReviewHistory.Action.REJECTED, note='again'
        )
        TaskReviewHistory.objects.create(
            task=self.t_first, actor=self.admin, action=TaskReviewHistory.Action.REJECTED, note='final'
        )
        lab = {w['email']: w for w in self._get()['workers']}[self.labeler.email]
        assert lab['rejected_events'] == 3 and lab['rejected_tasks'] == 1
        assert lab['rejected_task_rate'] == 50.0  # 1/2 — 300% 같은 값이 나오지 않음
        assert lab['avg_rejects'] == 3.0
        assert lab['rejected_first'] == 2 and lab['rejected_final'] == 1  # admin 반려는 최종으로 분리

    def test_reviewer_overturn_attribution(self):
        """검수자 뒤집힘율: reviewer 가 1차 통과시킨 태스크를 admin 이 최종 반려하면 그 reviewer 에 귀속."""
        TaskReviewHistory.objects.create(
            task=self.t_first, actor=self.reviewer, action=TaskReviewHistory.Action.FIRST_APPROVED
        )
        TaskReviewHistory.objects.create(
            task=self.t_first, actor=self.admin, action=TaskReviewHistory.Action.REJECTED, note='miss'
        )
        reviewers = {r['email']: r for r in self._get()['reviewers']}
        rev = reviewers[self.reviewer.email]
        assert rev['first_approved'] == 1 and rev['overturned'] == 1
        assert rev['overturn_rate'] == 100.0
        assert rev['rejected_first'] == 1  # setUpTestData 의 reviewer 반려 1건(1차 반려 활동량)

    def test_daily_series(self):
        """최근 14일 일별 추이: 오늘 = 제출 4(비취소만)·반려 이벤트 1, 항상 14칸."""
        daily = self._get()['daily']
        assert len(daily) == 14
        today = daily[-1]
        assert today['annotations'] == 4  # cancelled 1건 제외
        assert today['rejected'] == 1 and today['approved'] == 0 and today['first_approved'] == 0
        assert all(d['annotations'] == 0 for d in daily[:-1])

    def test_active_seconds_metrics(self):
        """순수 활성 작업시간 지표: 합계·평균·객체/활성시간, lead_time 과 병행(대체 아님)."""
        lab = {w['email']: w for w in self._get()['workers']}[self.labeler.email]
        # 합계 active=180, lead=300 → active < lead (자리비움 제외됨)
        assert lab['total_active_seconds'] == 180.0
        assert lab['total_lead_time'] == 300.0
        assert lab['total_active_seconds'] < lab['total_lead_time']
        assert lab['avg_active_seconds'] == 60.0  # 180/3 주석
        assert lab['active_coverage'] == 3
        assert lab['avg_active_per_task'] == 90.0  # 180/2 태스크
        # 객체 6개 / 활성 180초 → 시간당 120, 체류(lead) 기반은 72
        assert lab['objects_per_active_hour'] == 120.0
        assert lab['objects_per_hour'] == 72.0

    def test_active_seconds_null_safe(self):
        """active_seconds 미설정(도입 전) 작업자는 0.0/0 으로 안전 집계."""
        rev = {w['email']: w for w in self._get()['workers']}[self.reviewer.email]
        assert rev['total_active_seconds'] == 0.0 and rev['active_coverage'] == 0
        assert rev['objects_per_active_hour'] == 0.0 and rev['avg_active_per_task'] == 0.0

    def test_reject_reason_stats(self):
        """사유별 통계: 코드별 전체 분포 + 작업자별 매트릭스, 코드 없는 과거 이력은 미분류."""
        TaskReviewHistory.objects.create(
            task=self.t_first, actor=self.reviewer, action=TaskReviewHistory.Action.REJECTED, reason_code='fp'
        )
        TaskReviewHistory.objects.create(
            task=self.t_pending_labeled,
            actor=self.admin,
            action=TaskReviewHistory.Action.REJECTED,
            reason_code='boundary',
        )
        data = self._get()
        totals = data['reject_reasons']['totals']
        # setUpTestData 의 코드 없는 반려 1건 = 미분류
        assert totals == {'unclassified': 1, 'fp': 1, 'boundary': 1}
        assert 'fp' in data['reject_reasons']['labels']
        lab = {w['email']: w for w in data['workers']}[self.labeler.email]
        assert lab['reasons'] == {'unclassified': 1, 'fp': 1, 'boundary': 1}

    def test_other_org_excluded(self):
        data = self._get()
        assert data['summary']['tasks'] == 6  # 타 org approved 태스크 미포함
        emails = {w['email'] for w in data['workers']}
        assert all('@' in e for e in emails) and len(emails) == 2
