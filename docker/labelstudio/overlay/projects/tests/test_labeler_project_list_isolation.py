"""TDD Red — 라벨러 대상 프로젝트 목록/상세 메타 누수 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **프로젝트 목록/상세의 존재(메타) 격리** 한정.
(카운트 누수는 test_labeler_project_counts_leak.py 에서 별도로 다룸.)

검증하려는 목표 동작:
  - role='labeler' 유저는
        프로젝트 목록  GET /api/projects/      (ProjectListAPI)
    에서 **본인 배정 태스크(Task.assignee==self)가 있는 프로젝트만** 봐야 한다.
    배정 태스크가 하나도 없는 프로젝트는 목록에 노출되면 안 된다.
  - role='labeler' 유저가
        프로젝트 상세  GET /api/projects/{id}/  (ProjectAPI)
    에서 본인 배정 태스크가 없는 프로젝트를 요청하면 404(존재 미노출).
    본인 배정 태스크가 있는 프로젝트는 200.
  - role='reviewer'/'admin'/owner 는 org 전체 프로젝트를 보고 상세도 전부 200 — 가드.

경로:
  - projects/api.py:ProjectListAPI.get_queryset (~186)
        Project.objects.filter(organization=active_organization) — assignee 무스코핑.
  - projects/api.py:ProjectAPI.get_queryset (~387)
        Project.objects.with_counts(...).filter(organization=active_organization) — 동일.
  - Task→project related_name = 'tasks', Task.assignee (labeler 전용 FK).

현재 구현 상태 (Red 근거):
  두 get_queryset 모두 organization 단위 필터만 있고 assignee 스코핑이 없다.
  → labeler_a 가 배정 태스크가 없는 P2 를 목록에서 보고, P2 상세도 200 으로 열려 RED.

green 단계 예상 최소 구현:
  두 get_queryset 에서 is_labeler(user, org) 인 경우
  projects.filter(tasks__assignee=user).distinct() 로 좁힌다.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerProjectListIsolation(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()

        cls.admin = cls.organization.created_by
        cls.reviewer = UserFactory(active_organization=cls.organization)
        cls.labeler_a = UserFactory(active_organization=cls.organization)
        cls.labeler_b = UserFactory(active_organization=cls.organization)

        set_role(cls.admin, cls.organization, ROLE_ADMIN)
        set_role(cls.reviewer, cls.organization, ROLE_REVIEWER)
        set_role(cls.labeler_a, cls.organization, ROLE_LABELER)
        set_role(cls.labeler_b, cls.organization, ROLE_LABELER)

        # P1: labeler_a 배정 태스크 있음.  P2: labeler_b 배정 태스크만(labeler_a 없음).
        cls.p1 = ProjectFactory(organization=cls.organization, title='P1 - labeler_a assigned')
        cls.p2 = ProjectFactory(organization=cls.organization, title='P2 - labeler_b only')

        TaskFactory(project=cls.p1, data={'text': 'p1-a'}, assignee=cls.labeler_a)
        TaskFactory(project=cls.p2, data={'text': 'p2-b'}, assignee=cls.labeler_b)

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    def _list_ids(self, user):
        self._auth(user)
        response = self.client.get('/api/projects/')
        assert response.status_code == 200, response.content
        payload = response.json()
        results = payload['results'] if isinstance(payload, dict) and 'results' in payload else payload
        return {p['id'] for p in results}

    def _detail_status(self, user, project):
        self._auth(user)
        return self.client.get(f'/api/projects/{project.id}/').status_code

    # --- RED: 목록은 본인 배정 프로젝트만 -------------------------------------

    def test_labeler_project_list_only_assigned(self):
        """[RED] labeler_a 목록에는 P1 만 있고 P2(배정 없음)는 없어야 한다."""
        ids = self._list_ids(self.labeler_a)
        assert self.p1.id in ids, f'본인 배정 프로젝트 P1 이 목록에서 사라짐: {ids}'
        assert self.p2.id not in ids, (
            f'labeler_a 에게 배정 없는 프로젝트 P2 의 존재/제목이 누수됨(격리 실패): {ids}'
        )

    # --- RED: 상세는 배정 없는 프로젝트면 404 ---------------------------------

    def test_labeler_project_detail_unassigned_is_404(self):
        """[RED] labeler_a 가 배정 없는 P2 상세를 요청하면 404(존재 미노출)여야 한다."""
        status = self._detail_status(self.labeler_a, self.p2)
        assert status == 404, (
            f'labeler_a 가 배정 없는 P2 상세에 접근됨(격리 실패): status={status} (기대 404)'
        )

    def test_labeler_project_detail_assigned_is_200(self):
        """[가드/RED] labeler_a 는 본인 배정 P1 상세에는 정상 접근(200)해야 한다."""
        status = self._detail_status(self.labeler_a, self.p1)
        assert status == 200, f'본인 배정 P1 상세가 막힘: status={status} (기대 200)'

    # --- 가드: reviewer / admin 은 org 전체 ------------------------------------

    def test_reviewer_sees_all_projects(self):
        """[가드] reviewer 목록에는 P1·P2 둘 다 있고 상세도 둘 다 200."""
        ids = self._list_ids(self.reviewer)
        assert {self.p1.id, self.p2.id} <= ids, ids
        assert self._detail_status(self.reviewer, self.p1) == 200
        assert self._detail_status(self.reviewer, self.p2) == 200

    def test_admin_sees_all_projects(self):
        """[가드] admin 목록에는 P1·P2 둘 다 있고 상세도 둘 다 200."""
        ids = self._list_ids(self.admin)
        assert {self.p1.id, self.p2.id} <= ids, ids
        assert self._detail_status(self.admin, self.p1) == 200
        assert self._detail_status(self.admin, self.p2) == 200
