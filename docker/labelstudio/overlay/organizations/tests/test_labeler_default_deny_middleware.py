"""TDD Red — labeler default-deny 백스톱 미들웨어 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **요청 경로 기반 default-deny 미들웨어**의 동작 검증 한정.
(개별 뷰 scope 격리는 다른 사이클에서 이미 다룸 — 여기서는 '미감사 뷰까지 기본 차단'
 이라는 미들웨어 계약만 본다.)

근본 원인:
  OSS `User.has_permission` 이 사실상 더미 True 라, 뷰별 permission_required 로는
  감사되지 않은 뷰가 labeler 에게 전부 노출된다. 이를 막기 위해 요청 진입 지점에서
  **labeler 요청을 기본 차단하고, 허용 화이트리스트(url name + HTTP method)만 통과**
  시키는 미들웨어를 둔다.

검증하려는 목표 동작:
  1) 차단: labeler 가 화이트리스트 밖 엔드포인트를 호출하면 403.
  2) 통과: labeler 가 화이트리스트 엔드포인트를 호출하면 미들웨어가 막지 않는다(403 아님).
  3) 역할 가드: reviewer / admin 은 어떤 경로에서도 미들웨어 영향이 없다(전부 403 아님).
  4) 비인증: 로그인하지 않은 요청은 미들웨어 영향이 없다(로그인 플로우 정상, 미들웨어發 403 아님).

현재 구현 상태 (Red 근거):
  default-deny 미들웨어가 아직 없다. 따라서 org 멤버이기만 하면 labeler 가
  org memberships / prediction 쓰기 / project CRUD / import / ml / webhooks / storages
  같은 미감사 엔드포인트에서 403 이 아닌 응답(200/201/204/400/405 등)을 받는다.
  → 차단 테스트들이 "403 이어야 하는데 아님"으로 실패(Red)해야 정상이다.
  (export·dm-actions 등 일부는 기존 뷰 가드로 이미 403 일 수 있으나, 미들웨어 부재가 핵심 Red.)

설계 계약(이전 사이클에서 이미 도입됨):
  - OrganizationMember.role : labeler/reviewer/admin
  - OrganizationMember.is_labeler(user, organization)
  green 단계는 resolver_match 의 (url name, HTTP method) 로 판정하는 미들웨어를
  추가하고 settings 에 등록하며, 허용목록 상수를 정의해야 한다.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import AnnotationDraftFactory, AnnotationFactory, PredictionFactory, TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class LabelerDefaultDenyBase(APITestCase):
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

        # labeler 에게 배정된 태스크 + 본인 annotation / draft / prediction (허용 경로 검증용)
        cls.task = TaskFactory(project=cls.project, data={'text': 'x'}, assignee=cls.labeler)
        cls.annotation = AnnotationFactory(
            task=cls.task, project=cls.project, completed_by=cls.labeler, result=[]
        )
        cls.draft = AnnotationDraftFactory(task=cls.task, user=cls.labeler)
        cls.prediction = PredictionFactory(task=cls.task, project=cls.project)

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    def _logout(self):
        self.client.force_authenticate(user=None)


# --------------------------------------------------------------------------- #
# 1) 차단: labeler 는 화이트리스트 밖 엔드포인트에서 403                        #
# --------------------------------------------------------------------------- #
class TestLabelerBlockedByDefaultDeny(LabelerDefaultDenyBase):
    def test_labeler_blocked_on_easy_export(self):
        """[RED] export(GET /api/projects/{id}/export) 는 화이트리스트 밖 → 403."""
        self._auth(self.labeler)
        r = self.client.get(f'/api/projects/{self.project.id}/export?exportType=JSON')
        assert r.status_code == 403, f'export 는 미들웨어가 차단(403)해야 함, 실제={r.status_code}'

    def test_labeler_blocked_on_dm_destructive_action(self):
        """[RED] DM 파괴 액션(POST /api/dm/actions?id=delete_tasks) → 403."""
        self._auth(self.labeler)
        r = self.client.post(
            f'/api/dm/actions/?project={self.project.id}&id=delete_tasks',
            {'selectedItems': {'all': False, 'included': [self.task.id]}},
            format='json',
        )
        assert r.status_code == 403, f'dm-actions 파괴 액션은 차단(403)돼야 함, 실제={r.status_code}'

    def test_labeler_blocked_on_org_memberships(self):
        """[RED] org 멤버 목록(GET /api/organizations/{id}/memberships) → 403."""
        self._auth(self.labeler)
        r = self.client.get(f'/api/organizations/{self.organization.id}/memberships')
        assert r.status_code == 403, f'org memberships 는 차단(403)돼야 함, 실제={r.status_code}'

    def test_labeler_blocked_on_prediction_write(self):
        """[RED] prediction 쓰기(POST /api/predictions/) → 403 (읽기만 허용)."""
        self._auth(self.labeler)
        r = self.client.post(
            '/api/predictions/',
            {'task': self.task.id, 'result': [], 'score': 0.1},
            format='json',
        )
        assert r.status_code == 403, f'prediction 쓰기는 차단(403)돼야 함, 실제={r.status_code}'

    def test_labeler_blocked_on_project_create(self):
        """[RED] 프로젝트 생성(POST /api/projects/) → 403."""
        self._auth(self.labeler)
        r = self.client.post('/api/projects/', {'title': 'x'}, format='json')
        assert r.status_code == 403, f'project 생성은 차단(403)돼야 함, 실제={r.status_code}'

    def test_labeler_blocked_on_project_delete(self):
        """[RED] 프로젝트 삭제(DELETE /api/projects/{id}/) → 403."""
        self._auth(self.labeler)
        r = self.client.delete(f'/api/projects/{self.project.id}/')
        assert r.status_code == 403, f'project 삭제는 차단(403)돼야 함, 실제={r.status_code}'

    def test_labeler_blocked_on_import(self):
        """[RED] import(POST /api/projects/{id}/import) → 403."""
        self._auth(self.labeler)
        r = self.client.post(
            f'/api/projects/{self.project.id}/import',
            [{'data': {'text': 'y'}}],
            format='json',
        )
        assert r.status_code == 403, f'import 는 차단(403)돼야 함, 실제={r.status_code}'

    def test_labeler_allowed_on_ml_list(self):
        """ML backend 목록(GET /api/ml/) 은 DM/에디터가 조회하므로 labeler 에게 허용(미들웨어 미차단).

        정책 변경(2026-07-07): 프로젝트 열기 시 DM/에디터가 ml-list 를 조회 → 차단하면 UI 오류.
        읽기 전용이고 태스크 데이터가 아니므로 허용(미들웨어가 403 하지 않음).
        """
        self._auth(self.labeler)
        r = self.client.get(f'/api/ml/?project={self.project.id}')
        assert r.status_code != 403, f'ml 목록은 labeler 에게 허용(비403)돼야 함, 실제={r.status_code}'

    def test_labeler_blocked_on_webhooks_list(self):
        """[RED] webhooks 목록(GET /api/webhooks/) → 403."""
        self._auth(self.labeler)
        r = self.client.get('/api/webhooks/')
        assert r.status_code == 403, f'webhooks 목록은 차단(403)돼야 함, 실제={r.status_code}'

    def test_labeler_blocked_on_storages_list(self):
        """[RED] storage 목록(GET /api/storages/) → 403."""
        self._auth(self.labeler)
        r = self.client.get(f'/api/storages/?project={self.project.id}')
        assert r.status_code == 403, f'storages 목록은 차단(403)돼야 함, 실제={r.status_code}'


# --------------------------------------------------------------------------- #
# 2) 통과: labeler 는 화이트리스트 엔드포인트에서 미들웨어發 403 이 아니어야 함 #
#    (미들웨어가 막지 않고 통과 → 이후 뷰 로직대로. 여기선 403 만 아니면 통과)   #
# --------------------------------------------------------------------------- #
class TestLabelerAllowedByWhitelist(LabelerDefaultDenyBase):
    def test_labeler_allowed_whoami(self):
        self._auth(self.labeler)
        r = self.client.get('/api/current-user/whoami')
        assert r.status_code != 403, f'whoami 는 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_project_detail(self):
        self._auth(self.labeler)
        r = self.client.get(f'/api/projects/{self.project.id}/')
        assert r.status_code != 403, f'project 상세는 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_dm_project(self):
        self._auth(self.labeler)
        r = self.client.get(f'/api/dm/project/?project={self.project.id}')
        assert r.status_code != 403, f'dm-project 는 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_dm_columns(self):
        self._auth(self.labeler)
        r = self.client.get(f'/api/dm/columns/?project={self.project.id}')
        assert r.status_code != 403, f'dm-columns 는 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_task_list(self):
        self._auth(self.labeler)
        r = self.client.get(f'/api/tasks/?project={self.project.id}')
        assert r.status_code != 403, f'task 목록은 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_task_detail(self):
        self._auth(self.labeler)
        r = self.client.get(f'/api/tasks/{self.task.id}/')
        assert r.status_code != 403, f'본인 task 상세는 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_task_annotations_get(self):
        self._auth(self.labeler)
        r = self.client.get(f'/api/tasks/{self.task.id}/annotations/')
        assert r.status_code != 403, f'task annotations 조회는 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_annotation_detail_get(self):
        self._auth(self.labeler)
        r = self.client.get(f'/api/annotations/{self.annotation.id}/')
        assert r.status_code != 403, f'annotation 상세는 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_task_drafts_get(self):
        self._auth(self.labeler)
        r = self.client.get(f'/api/tasks/{self.task.id}/drafts')
        assert r.status_code != 403, f'task drafts 조회는 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_prediction_read(self):
        self._auth(self.labeler)
        r = self.client.get(f'/api/predictions/?task={self.task.id}')
        assert r.status_code != 403, f'prediction 읽기는 통과해야 함, 실제={r.status_code}'

    def test_labeler_allowed_media_resolve(self):
        self._auth(self.labeler)
        r = self.client.get(f'/tasks/{self.task.id}/resolve/?fileuri=abc')
        assert r.status_code != 403, f'미디어 resolve 는 통과해야 함, 실제={r.status_code}'


# --------------------------------------------------------------------------- #
# 3) 역할 가드: reviewer / admin 은 미들웨어 영향 없음(차단 대상서도 403 아님)  #
# --------------------------------------------------------------------------- #
class TestNonLabelerUnaffectedByMiddleware(LabelerDefaultDenyBase):
    def test_reviewer_not_blocked_on_org_memberships(self):
        self._auth(self.reviewer)
        r = self.client.get(f'/api/organizations/{self.organization.id}/memberships')
        assert r.status_code != 403, f'reviewer 는 미들웨어 영향 없어야 함, 실제={r.status_code}'

    def test_admin_not_blocked_on_org_memberships(self):
        self._auth(self.admin)
        r = self.client.get(f'/api/organizations/{self.organization.id}/memberships')
        assert r.status_code != 403, f'admin 은 미들웨어 영향 없어야 함, 실제={r.status_code}'

    def test_reviewer_blocked_on_export(self):
        self._auth(self.reviewer)
        r = self.client.get(f'/api/projects/{self.project.id}/export?exportType=JSON')
        assert r.status_code == 403, f'reviewer export 는 미들웨어 영향 없어야 함, 실제={r.status_code}'

    def test_admin_not_blocked_on_project_create(self):
        self._auth(self.admin)
        r = self.client.post('/api/projects/', {'title': 'z'}, format='json')
        assert r.status_code != 403, f'admin project 생성은 미들웨어 영향 없어야 함, 실제={r.status_code}'


# --------------------------------------------------------------------------- #
# 4) 비인증: 로그인 안 한 요청은 미들웨어發 403 이 아니어야 함(로그인 플로우)    #
# --------------------------------------------------------------------------- #
class TestUnauthenticatedUnaffectedByMiddleware(LabelerDefaultDenyBase):
    def test_anonymous_not_blocked_by_middleware_on_whoami(self):
        """비인증 whoami: 미들웨어가 아니라 인증 계층이 처리(401 등). 미들웨어發 403 은 아님."""
        self._logout()
        r = self.client.get('/api/current-user/whoami')
        assert r.status_code != 403, f'비인증 요청은 미들웨어가 건드리면 안 됨, 실제={r.status_code}'

    def test_anonymous_not_blocked_on_blocked_endpoint(self):
        """비인증 요청은 role 판정 대상이 아니므로 미들웨어가 관여하지 않는다(403 아님)."""
        self._logout()
        r = self.client.get(f'/api/organizations/{self.organization.id}/memberships')
        assert r.status_code != 403, f'비인증 요청은 미들웨어 default-deny 대상 아님, 실제={r.status_code}'
