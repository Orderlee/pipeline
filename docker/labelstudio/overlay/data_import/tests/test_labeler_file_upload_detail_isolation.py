"""TDD Red — 라벨러별 file-upload-detail(FileUploadAPI) IDOR 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **FileUploadAPI (GET /api/import/file-upload/{pk}) 의 소유 스코핑** 한정.

대상 뷰 (label_studio/data_import/api.py:882):
  - FileUploadAPI (RetrieveUpdateDestroyAPIView)
      GET|PATCH|DELETE /api/import/file-upload/{pk}
      url name = 'data_import:api:file-upload-detail'
      permission_classes = (IsAuthenticated,), queryset = FileUpload.objects.all()  ← scope 없음.

검증하려는 목표 동작:
  - role='labeler' 는 **타인/타org 의 FileUpload pk 상세를 조회할 수 없다** → 거부(403/404).
  - reviewer/admin 은 접근 가능(가드).

Red 근거 (현재 구현):
  - default-deny 미들웨어 허용목록(LABELER_ALLOWED)에 'data_import:api:file-upload-detail':{'GET'}
    이 등재되어 있고, 뷰 queryset 은 FileUpload.objects.all() 로 소유 스코핑이 없다.
  - 따라서 labeler_a 가 타인 소유 FileUpload pk 로 GET 시 200 + file 경로/크기가 노출된다 → 격리 실패(Red).

green 단계 예상 최소 구현(방식 무관, status 로만 단언):
  - 허용목록에서 'file-upload-detail' 제거(미들웨어 403)  또는
  - 뷰에 get_queryset scope(파일↔프로젝트/assignee) 적용 → 타인 파일이면 403/404.
"""
from django.core.files.base import ContentFile
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from users.tests.factories import UserFactory

from data_import.models import FileUpload

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

DENIED = (403, 404)


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerFileUploadDetailIsolation(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.project = ProjectFactory(organization=cls.organization)

        cls.admin = cls.organization.created_by
        cls.reviewer = UserFactory(active_organization=cls.organization)
        cls.labeler_a = UserFactory(active_organization=cls.organization)

        set_role(cls.admin, cls.organization, ROLE_ADMIN)
        set_role(cls.reviewer, cls.organization, ROLE_REVIEWER)
        set_role(cls.labeler_a, cls.organization, ROLE_LABELER)

        # labeler_a 에게 배정되지 않은(타인 소유) 업로드 파일.
        cls.foreign_file = cls._make_upload(cls.admin, b'foreign-media-bytes')

    @classmethod
    def _make_upload(cls, user, payload):
        fu = FileUpload(user=user, project=cls.project)
        fu.file.save('clip.bin', ContentFile(payload), save=True)
        return fu

    @classmethod
    def tearDownClass(cls):
        fu = getattr(cls, 'foreign_file', None)
        try:
            if fu is not None:
                fu.file.delete(save=False)
        except Exception:
            pass
        super().tearDownClass()

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    def _detail(self, file_upload):
        return self.client.get(f'/api/import/file-upload/{file_upload.id}')

    # --- 1) 타인 FileUpload 상세 IDOR 격리 --------------------------------------

    def test_labeler_cannot_retrieve_foreign_file_upload_detail(self):
        """[RED] labeler_a 가 타인 소유 FileUpload pk 상세를 조회하면 거부(403/404)."""
        self._auth(self.labeler_a)
        response = self._detail(self.foreign_file)
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 file-upload-detail 을 조회함(IDOR 격리 실패): '
            f'status={response.status_code}, body={response.content}'
        )

    # --- 가드: reviewer / admin 은 격리 없음 ------------------------------------

    def test_reviewer_blocked_retrieve_file_upload_detail(self):
        """reviewer 는 file-upload-detail 도 403 (2026-07-10 import/export admin 전용 정책)."""
        self._auth(self.reviewer)
        response = self._detail(self.foreign_file)
        assert response.status_code == 403, response.status_code

    def test_admin_can_retrieve_file_upload_detail(self):
        """[가드] admin 은 file-upload-detail 에 접근 가능(거부 아님)."""
        self._auth(self.admin)
        response = self._detail(self.foreign_file)
        assert response.status_code not in DENIED, response.status_code
