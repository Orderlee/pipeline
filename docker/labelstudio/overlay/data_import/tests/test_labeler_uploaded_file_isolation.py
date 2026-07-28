"""TDD Red — 라벨러별 업로드 파일 다운로드 격리 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **UploadedFileResponse 의 assignee 스코핑** 한정.

대상 뷰 (label_studio/data_import/api.py):
  - UploadedFileResponse (918): GET /data/upload/{filename}   (default-deny 허용목록에 있음)
  - DownloadStorageData  (946): GET /storage-data/uploaded/?filepath=
      → url name 'data_import:storage-data-upload' 은 허용목록에 없어 미들웨어가 이미 403 차단
        (문서화 가드로만 검증).

파일 ↔ 태스크 링크:
  Task.file_upload (FK, related_name='tasks') 로 업로드 파일이 태스크에 연결된다.
  따라서 "labeler 가 타인 배정 태스크(task_b)에 연결된 업로드 파일을 다운로드"를 재현할 수 있다.

검증하려는 목표 동작:
  - role='labeler' 는 **본인 배정 태스크에 연결된 업로드 파일만** 다운로드 가능.
  - 타인 배정 태스크(task_b) 에 연결된 파일 다운로드 → 거부(403/404).
  - 본인 태스크(task_a) 파일은 정상(가드). reviewer/admin 은 격리 없음(가드).

현재 구현 상태 (Red 근거):
  UploadedFileResponse.get() 은 file_upload.has_permission(user) 만 확인하는데,
  FileUpload.has_permission → project.has_permission → OSS 더미 True 라 assignee 스코핑이 없다.
  실제 파일이 존재하면(스토리지 목킹 대신 실파일 생성) 200/206 을 돌려주므로,
  labeler_a 가 타인 파일을 받는 격리 실패(Red)를 관찰할 수 있다.
  (실파일을 만들어 '스토리지 미존재 404'와 '격리 거부'를 구분한다.)

green 단계 예상 최소 구현:
  UploadedFileResponse 에서 file_upload 를 OrganizationMember.scope_tasks_for_request 로 좁힌
  태스크(file_upload__tasks__in=scoped_tasks) 로 검증 → 타인 파일이면 403/404.
"""
from django.core.files.base import ContentFile
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import TaskFactory
from users.tests.factories import UserFactory

from data_import.models import FileUpload

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

DENIED = (403, 404)
UPLOAD_PREFIX = 'upload/'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerUploadedFileIsolation(APITestCase):
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

        cls.file_a = cls._make_upload(cls.admin, b'own-media-bytes')
        cls.file_b = cls._make_upload(cls.admin, b'foreign-media-bytes')

        # 업로드 파일 ↔ 태스크 배타적 연결
        cls.task_a = TaskFactory(
            project=cls.project, data={'text': 'a'}, assignee=cls.labeler_a, file_upload=cls.file_a
        )
        cls.task_b = TaskFactory(
            project=cls.project, data={'text': 'b'}, assignee=cls.labeler_b, file_upload=cls.file_b
        )

    @classmethod
    def _make_upload(cls, user, payload):
        fu = FileUpload(user=user, project=cls.project)
        fu.file.save('clip.bin', ContentFile(payload), save=True)
        return fu

    @classmethod
    def tearDownClass(cls):
        for fu in (getattr(cls, 'file_a', None), getattr(cls, 'file_b', None)):
            try:
                if fu is not None:
                    fu.file.delete(save=False)
            except Exception:
                pass
        super().tearDownClass()

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    def _download(self, file_upload):
        # file_upload.file.name == 'upload/<project>/<uuid>-clip.bin'; URL param drops 'upload/' prefix.
        filename = file_upload.file.name[len(UPLOAD_PREFIX):]
        return self.client.get(f'/data/upload/{filename}')

    # --- 1) 타인 태스크 파일 다운로드 격리 --------------------------------------

    def test_labeler_cannot_download_foreign_task_file(self):
        """[RED] labeler_a 가 타인 태스크(task_b) 에 연결된 업로드 파일을 받으면 거부(403/404)."""
        self._auth(self.labeler_a)
        response = self._download(self.file_b)
        assert response.status_code in DENIED, (
            f'labeler_a 가 타인 태스크 업로드 파일을 다운로드함(격리 실패): status={response.status_code}'
        )

    # --- 가드: 본인 태스크 파일은 정상 ------------------------------------------

    def test_labeler_can_download_own_task_file(self):
        """[가드] labeler_a 는 본인 태스크(task_a) 파일을 받을 수 있다(거부 아님)."""
        self._auth(self.labeler_a)
        response = self._download(self.file_a)
        assert response.status_code not in DENIED, (
            f'labeler_a 가 본인 태스크 파일을 못 받음(과차단): status={response.status_code}'
        )

    # --- 가드: reviewer / admin 은 격리 없음 ------------------------------------

    def test_reviewer_can_download_any_task_file(self):
        """[가드] reviewer 는 타인 태스크 파일을 받을 수 있다(거부 아님)."""
        self._auth(self.reviewer)
        response = self._download(self.file_b)
        assert response.status_code not in DENIED, response.status_code

    # --- DownloadStorageData: 미들웨어가 이미 차단(문서화 가드) ------------------

    def test_labeler_cannot_use_storage_data_download(self):
        """[가드] labeler 의 /storage-data/uploaded/ 는 default-deny 미들웨어가 이미 403 차단."""
        self._auth(self.labeler_a)
        filepath = self.file_b.file.name
        response = self.client.get(f'/storage-data/uploaded/?filepath={filepath}')
        assert response.status_code in DENIED, (
            f'labeler 가 storage-data 다운로드로 타인 파일에 접근됨: status={response.status_code}'
        )
