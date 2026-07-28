"""회귀방지(가드) — 미배정(assignee=None) 태스크 비노출 (GCP 외주 LS 포크 전용).

이번 파일 성격: **가드/회귀방지**. 현재 이미 통과(PASS)해야 정상이다.

배경:
  OrganizationMember.scope_tasks_for_user 는 labeler 에 대해 queryset.filter(assignee=user) 로
  좁힌다. assignee=None 태스크는 assignee=user 조건에 자연히 걸러진다(자동 배제).
  따라서 미배정 태스크는 목록/next_task 에서 labeler 에게 노출되지 않아야 하며,
  이 파일은 그 불변식이 향후 리팩터에도 깨지지 않도록 고정한다.

  (next_task 의 미배정 배제는 test_labeler_next_task_isolation.py 에서 이미 검증됨.
   여기서는 태스크 *목록* 그리드 경로에 대한 회귀 가드를 추가한다.)

만약 이 테스트가 실패한다면(=미배정 태스크가 노출된다면) 그것은 진짜 Red 이며
scope_tasks_for_user 또는 목록 뷰의 스코핑 결손을 뜻한다.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


def response_task_ids(response):
    data = response.json()
    tasks = data['tasks'] if isinstance(data, dict) else data
    return {t['id'] for t in tasks}


class TestLabelerUnassignedTaskIsolation(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.organization)

        cls.reviewer = UserFactory(active_organization=cls.organization)
        cls.labeler_a = UserFactory(active_organization=cls.organization)

        set_role(cls.reviewer, cls.organization, ROLE_REVIEWER)
        set_role(cls.labeler_a, cls.organization, ROLE_LABELER)

        cls.task_a = TaskFactory(project=cls.project, data={'text': 'a'}, assignee=cls.labeler_a)
        cls.task_unassigned = TaskFactory(project=cls.project, data={'text': 'u'}, assignee=None)

    def _list_tasks(self, user):
        self.client.force_authenticate(user=user)
        return self.client.get(f'/api/tasks/?project={self.project.id}')

    def test_labeler_does_not_see_unassigned_task_in_list(self):
        """[가드/PASS 기대] labeler_a 목록에 미배정 태스크가 나타나지 않는다."""
        response = self._list_tasks(self.labeler_a)
        assert response.status_code == 200, response.content
        ids = response_task_ids(response)
        assert self.task_unassigned.id not in ids, (
            f'미배정 태스크가 labeler 목록에 노출됨(격리 실패): ids={ids}'
        )
        assert ids == {self.task_a.id}, f'labeler 는 본인 배정분만 봐야 함, 실제: {ids}'

    def test_reviewer_sees_unassigned_task_in_list(self):
        """[가드] reviewer 는 미배정 태스크를 포함한 전체를 본다."""
        response = self._list_tasks(self.reviewer)
        assert response.status_code == 200, response.content
        ids = response_task_ids(response)
        assert {self.task_a.id, self.task_unassigned.id} <= ids, f'실제: {ids}'
