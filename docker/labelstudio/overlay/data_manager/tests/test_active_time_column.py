"""DM 태스크별 'Active Time'(avg_active_seconds) 컬럼 — admin 전용 (GCP 외주 포크).

계약:
  - admin(owner/superuser/role=admin)만 컬럼 목록(`GET /api/dm/columns`)에 avg_active_seconds 노출.
  - reviewer/labeler 에겐 미노출(UI 게이팅). Lead Time 컬럼 바로 옆에 위치.
  - 컬럼 값은 태스크 어노테이션의 active_seconds 평균(avg_lead_time 과 동형) — annotate 동작.
"""
from data_manager.functions import get_all_columns
from data_manager.managers import annotate_avg_active_seconds
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.models import Annotation, Task
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


def _col_ids(project, user):
    return [c['id'] for c in get_all_columns(project, user)['columns']]


class TestActiveTimeColumn(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.org = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.org)
        cls.admin = cls.org.created_by
        cls.reviewer = UserFactory(active_organization=cls.org)
        cls.labeler = UserFactory(active_organization=cls.org)
        set_role(cls.admin, cls.org, 'admin')
        set_role(cls.reviewer, cls.org, 'reviewer')
        set_role(cls.labeler, cls.org, 'labeler')

    def test_admin_sees_active_time_column_next_to_lead_time(self):
        cols = _col_ids(self.project, self.admin)
        assert 'avg_active_seconds' in cols
        # Lead Time 바로 옆(직후)에 위치
        assert cols.index('avg_active_seconds') == cols.index('avg_lead_time') + 1

    def test_reviewer_and_labeler_do_not_see_column(self):
        assert 'avg_active_seconds' not in _col_ids(self.project, self.reviewer)
        assert 'avg_active_seconds' not in _col_ids(self.project, self.labeler)
        # 익명(user=None)도 미노출
        assert 'avg_active_seconds' not in _col_ids(self.project, None)

    def test_annotate_computes_avg_active_seconds(self):
        task = TaskFactory(project=self.project, data={'text': 'x'})
        Annotation.objects.create(task=task, project=self.project, completed_by=self.labeler, active_seconds=40.0)
        Annotation.objects.create(task=task, project=self.project, completed_by=self.labeler, active_seconds=80.0)
        qs = annotate_avg_active_seconds(Task.objects.filter(pk=task.pk))
        assert qs.first().avg_active_seconds == 60.0

    def test_serializer_outputs_avg_active_seconds(self):
        """annotate 후 DM 직렬화 출력에 avg_active_seconds 가 포함돼야 한다
        (직렬화기 명시 필드 누락 시 컬럼이 빈 값으로 나오는 회귀 방지)."""
        from data_manager.serializers import DataManagerTaskSerializer

        task = TaskFactory(project=self.project, data={'text': 'x'})
        Annotation.objects.create(task=task, project=self.project, completed_by=self.labeler, active_seconds=42.0)
        qs = annotate_avg_active_seconds(Task.objects.filter(pk=task.pk))
        data = DataManagerTaskSerializer(qs.first()).data
        assert 'avg_active_seconds' in data and data['avg_active_seconds'] == 42.0
