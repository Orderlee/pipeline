"""DM 태스크별 'Assignee'(담당 라벨러) 컬럼 — admin·reviewer 노출 (GCP 외주 포크).

계약:
  - admin·reviewer·owner 는 컬럼 목록(`GET /api/dm/columns`)에 assignee 노출.
  - labeler·익명 에겐 미노출(labeler 는 본인 배정분만 보므로 무의미 + 게이팅).
  - 값은 updated_by 동형: assignee 있으면 [{'user_id': N}], 없으면 [].
"""
from data_manager.functions import get_all_columns
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


def _col_ids(project, user):
    return [c['id'] for c in get_all_columns(project, user)['columns']]


class TestAssigneeColumn(APITestCase):
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

    def test_admin_and_reviewer_see_assignee_column(self):
        assert 'assignee' in _col_ids(self.project, self.admin)
        assert 'assignee' in _col_ids(self.project, self.reviewer)

    def test_labeler_and_anon_do_not_see_column(self):
        assert 'assignee' not in _col_ids(self.project, self.labeler)
        assert 'assignee' not in _col_ids(self.project, None)

    def test_column_is_list_type_with_member_schema(self):
        col = next(c for c in get_all_columns(self.project, self.admin)['columns'] if c['id'] == 'assignee')
        assert col['type'] == 'List'
        assert 'schema' in col  # project_members 로 이름 렌더

    def test_serializer_outputs_assignee(self):
        from data_manager.serializers import DataManagerTaskSerializer

        task = TaskFactory(project=self.project, data={'text': 'x'}, assignee=self.labeler)
        data = DataManagerTaskSerializer(task).data
        assert data['assignee'] == [{'user_id': self.labeler.id}]

    def test_serializer_empty_when_no_assignee(self):
        from data_manager.serializers import DataManagerTaskSerializer

        task = TaskFactory(project=self.project, data={'text': 'x'}, assignee=None)
        data = DataManagerTaskSerializer(task).data
        assert data['assignee'] == []
