"""TDD Red — 라벨러별 미디어 프록시(resolve/presign) 접근 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **io_storages/proxy_api.py 의 URI resolve/presign 격리** 한정.

대상 뷰 (label_studio/io_storages/proxy_api.py):
  - TaskResolveStorageUri  : GET /tasks/{task_id}/resolve/?fileuri=...
                             GET /tasks/{task_id}/presign/?fileuri=...
  - ProjectResolveStorageUri: GET /projects/{project_id}/resolve|presign?fileuri=...

검증하려는 목표 동작:
  - role='labeler' 는 **본인 배정 태스크(assignee=self) 의 미디어만** resolve/presign 가능.
  - 타인 배정 태스크(task_b, assignee=labeler_b) 의 미디어 resolve/presign → 거부(403/404).
  - 본인 태스크(task_a) 는 정상(가드). reviewer/admin 은 격리 없음(가드).
  - project-level resolve/presign 은 labeler 접근 자체 차단(현재 default-deny 미들웨어가 이미 담당).

현재 구현 상태 (Red 근거):
  ResolveStorageUriAPIMixin.resolve() 의 권한 게이트는 instance.has_permission(user) 뿐이고,
  Project/Task.has_permission 은 OSS 더미(True)라 assignee 스코핑이 전혀 없다. 미들웨어는
  task-storage-data-resolve/presign 을 labeler 에게 허용하므로, 뷰가 타인 task_id 로도 미디어를
  resolve 해 준다 → 타인 태스크 요청이 성공(303 redirect)해 격리 테스트가 실패(Red)한다.

스토리지 목킹:
  실제 s3/gcs 로 나가지 않도록 get_storage_by_url(presign=True) 와 <Model>.resolve_storage_uri 를
  성공값으로 목킹한다. 이렇게 해야 "스토리지 미설정 404"(우연한 통과)와 "격리 거부"를 구분할 수 있고,
  현재 코드에서 타인 태스크가 303(성공)을 돌려주는 진짜 Red 를 관찰할 수 있다.

green 단계 예상 최소 구현:
  TaskResolveStorageUri.get() 의 task 해석을
  scope_tasks_for_request(request, Task.objects.filter(pk=task_id)) 로 좁혀(라벨러면 assignee=self),
  타인 태스크면 404. project-level 은 미들웨어가 이미 차단하므로 view 변경 불필요.
"""
from unittest import mock

from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

DENIED = (403, 404)
FILEURI = 'gs://bucket/object.mp4'

_PRESIGN_RESULT = {'url': 'https://signed.example/object.mp4', 'presign_ttl': 5}


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


def _fake_storage():
    """Storage stub that advertises presign support so resolve() takes the redirect branch."""
    storage = mock.Mock()
    storage.presign = True
    return storage


class TestLabelerMediaProxyIsolation(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.organization)

        cls.admin = cls.organization.created_by
        cls.reviewer = UserFactory(active_organization=cls.organization)
        cls.labeler_a = UserFactory(active_organization=cls.organization)
        cls.labeler_b = UserFactory(active_organization=cls.organization)

        set_role(cls.admin, cls.organization, ROLE_ADMIN)
        set_role(cls.reviewer, cls.organization, ROLE_REVIEWER)
        set_role(cls.labeler_a, cls.organization, ROLE_LABELER)
        set_role(cls.labeler_b, cls.organization, ROLE_LABELER)

        cls.task_a = TaskFactory(project=cls.project, data={'video': FILEURI}, assignee=cls.labeler_a)
        cls.task_b = TaskFactory(project=cls.project, data={'video': FILEURI}, assignee=cls.labeler_b)

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    def _resolve(self, url):
        # Storage layer is mocked so any "not denied" response means the view resolved media.
        with mock.patch('io_storages.proxy_api.get_storage_by_url', return_value=_fake_storage()), mock.patch(
            'tasks.models.Task.resolve_storage_uri', return_value=_PRESIGN_RESULT
        ), mock.patch('projects.models.Project.resolve_storage_uri', return_value=_PRESIGN_RESULT):
            return self.client.get(url, data={'fileuri': FILEURI})

    # --- 1) 타인 태스크 미디어 resolve/presign 격리 --------------------------------

    def test_labeler_cannot_resolve_foreign_task_media(self):
        """[RED] labeler_a 가 타인 태스크(task_b) 미디어 resolve 를 요청하면 거부(403/404)."""
        self._auth(self.labeler_a)
        response = self._resolve(f'/tasks/{self.task_b.id}/resolve/')
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 태스크 미디어 resolve 에 성공함(격리 실패): status={response.status_code}'
        )

    def test_labeler_cannot_presign_foreign_task_media(self):
        """[RED] labeler_a 가 타인 태스크(task_b) 미디어 presign 을 요청하면 거부(403/404)."""
        self._auth(self.labeler_a)
        response = self._resolve(f'/tasks/{self.task_b.id}/presign/')
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 태스크 미디어 presign 에 성공함(격리 실패): status={response.status_code}'
        )

    # --- 가드: 본인 태스크는 정상 ------------------------------------------------

    def test_labeler_can_resolve_own_task_media(self):
        """[가드] labeler_a 는 본인 태스크(task_a) 미디어 resolve 에 접근 가능(거부 아님)."""
        self._auth(self.labeler_a)
        response = self._resolve(f'/tasks/{self.task_a.id}/resolve/')
        assert response.status_code not in DENIED, (
            f'labeler_a 가 본인 태스크 미디어에 접근 못함(과차단): status={response.status_code}'
        )

    # --- 가드: reviewer / admin 은 격리 없음 -------------------------------------

    def test_reviewer_can_resolve_any_task_media(self):
        """[가드] reviewer 는 타인 태스크 미디어 resolve 에 접근 가능(거부 아님)."""
        self._auth(self.reviewer)
        response = self._resolve(f'/tasks/{self.task_b.id}/resolve/')
        assert response.status_code not in DENIED, response.status_code

    def test_admin_can_resolve_any_task_media(self):
        """[가드] admin 은 타인 태스크 미디어 resolve 에 접근 가능(거부 아님)."""
        self._auth(self.admin)
        response = self._resolve(f'/tasks/{self.task_b.id}/resolve/')
        assert response.status_code not in DENIED, response.status_code

    # --- project-level: 미들웨어가 이미 차단(문서화 가드) ------------------------

    def test_labeler_cannot_use_project_level_resolve(self):
        """[가드] labeler 의 project-level resolve 는 default-deny 미들웨어가 이미 403 으로 차단."""
        self._auth(self.labeler_a)
        response = self._resolve(f'/projects/{self.project.id}/resolve/')
        assert response.status_code in DENIED, (
            f'labeler 가 project-level resolve 로 임의 fileuri 에 접근됨: status={response.status_code}'
        )
