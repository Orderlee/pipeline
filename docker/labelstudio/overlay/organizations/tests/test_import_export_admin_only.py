"""import/export admin 전용 게이트 (GCP 외주 포크 — 최소권한).

계약: 데이터 반출(export)·투입(import) 표면은 admin(owner/superuser/role=admin)만.
  reviewer 는 검수 업무에 이 표면이 불필요하며, 외주 확대 시 반출 우회 경로가 됨 → 403.
  라벨러는 기존 default-deny 로 이미 차단. 미디어 서빙(data-upload)은 게이트 비대상(전 역할 필요).
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from users.tests.factories import UserFactory


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestImportExportAdminOnly(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.org = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.org)
        cls.admin = UserFactory(active_organization=cls.org)
        cls.reviewer = UserFactory(active_organization=cls.org)
        set_role(cls.admin, cls.org, 'admin')
        set_role(cls.reviewer, cls.org, 'reviewer')

    EXPORT_GETS = [
        '/api/projects/{pk}/export?exportType=JSON',
        '/api/projects/{pk}/export/formats',
        '/api/projects/{pk}/exports/',
    ]
    IMPORT_URLS = [
        ('post', '/api/projects/{pk}/import'),
        ('post', '/api/projects/{pk}/reimport'),
        ('get', '/api/projects/{pk}/file-uploads'),
    ]

    def test_reviewer_blocked_everywhere(self):
        self.client.force_authenticate(self.reviewer)
        for url in self.EXPORT_GETS:
            resp = self.client.get(url.format(pk=self.project.pk))
            assert resp.status_code == 403, f'reviewer {url} 은 403 이어야 함, 실제={resp.status_code}'
        for method, url in self.IMPORT_URLS:
            resp = getattr(self.client, method)(url.format(pk=self.project.pk), {}, format='json')
            assert resp.status_code == 403, f'reviewer {method} {url} 은 403 이어야 함, 실제={resp.status_code}'

    def test_admin_passes_gate(self):
        """admin 은 게이트를 통과한다(403 이 아니어야 함 — 200/201/400 등 뷰 자체 응답)."""
        self.client.force_authenticate(self.admin)
        for url in self.EXPORT_GETS:
            resp = self.client.get(url.format(pk=self.project.pk))
            assert resp.status_code != 403, f'admin {url} 이 게이트에 막힘: {resp.status_code}'
        for method, url in self.IMPORT_URLS:
            resp = getattr(self.client, method)(url.format(pk=self.project.pk), {}, format='json')
            assert resp.status_code != 403, f'admin {method} {url} 이 게이트에 막힘: {resp.status_code}'

    def test_owner_passes_gate(self):
        self.client.force_authenticate(self.org.created_by)
        resp = self.client.get(f'/api/projects/{self.project.pk}/exports/')
        assert resp.status_code != 403

    def test_media_serving_not_gated(self):
        """업로드 미디어 서빙 뷰(에디터가 전 역할로 호출)는 AdminOnly 게이트 비대상이어야 한다.
        (뷰 자체는 없는 파일에 403 을 주므로 상태코드가 아닌 퍼미션 구성으로 검증.)"""
        from core.api_permissions import AdminOnly
        from data_import.api import DownloadStorageData, UploadedFileResponse

        for view in (UploadedFileResponse, DownloadStorageData):
            assert AdminOnly not in tuple(view.permission_classes), f'{view.__name__} 이 게이트에 포함됨'
