"""TDD Red — 라벨러의 export 하위경로(잔여 경로) 완전 차단 (GCP 외주 LS 포크 전용).

앞선 사이클에서 ExportAPI / ExportListAPI / ExportDownloadAPI 는
EXPORT_PERMISSION_CLASSES(= HasObjectPermission, IsAuthenticated, DenyLabelers)로
라벨러 차단(403)이 이미 걸려 있다(test_labeler_export_block.py, 9 PASS).

이번 사이클 범위: **아직 DenyLabelers 가 걸리지 않은 잔여 export 경로**.
이 경로들이 안 막히면 labeler 가 우회 다운로드/변환/포맷조회로 export 격리를 무력화할 수 있다.

검증하려는 목표 동작 (labeler → 403, reviewer/admin → 비-403):
  1) ProjectExportFilesAuthCheck : GET  /api/auth/export/
       - nginx auth_request(파일 다운로드 인증). X-Original-URI 헤더로 project 판별.
  2) ExportDetailAPI            : GET|DELETE /api/projects/{id}/exports/{export_pk}
  3) ExportConvertAPI           : POST /api/projects/{id}/exports/{export_pk}/convert
  4) ExportFormatsListAPI       : GET  /api/projects/{id}/export/formats

현재 구현 상태 (Red 근거):
  위 네 뷰는 permission_classes 를 지정하지 않고 permission_required 만 갖는다
  (ProjectExportFilesAuthCheck/ExportFormatsListAPI/ExportDetailAPI/ExportConvertAPI).
  즉 DenyLabelers 가 없어 org 멤버인 labeler 도 통과 → 403 이 아닌 응답을 받는다.

green 단계는 이 네 뷰에 EXPORT_PERMISSION_CLASSES(= DenyLabelers 포함)를 부여하면 된다.
"""
from unittest.mock import patch

from data_export.models import Export
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from projects.tests.factories import ProjectFactory
from rest_framework.test import APITestCase
from tasks.tests.factories import AnnotationFactory, TaskFactory
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestLabelerExportSubpathsBlocked(APITestCase):
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

        cls.task = TaskFactory(project=cls.project, data={'text': 'x'}, assignee=cls.labeler)
        AnnotationFactory(task=cls.task, project=cls.project, completed_by=cls.admin, result=[])

        # convert/detail/download 대상 완료 스냅샷
        cls.snapshot = Export.objects.create(project=cls.project, status=Export.Status.COMPLETED)

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    # ------------------------------------------------------------------ #
    # 1) nginx auth_request : GET /api/auth/export/
    #    X-Original-URI 헤더로 project id 판별 (원본 파일명 = "{project_id}-...json")
    # ------------------------------------------------------------------ #
    def _auth_check_headers(self):
        return {'HTTP_X_ORIGINAL_URI': f'/export/{self.project.id}-annotations.json'}

    def test_labeler_cannot_pass_export_files_auth_check(self):
        """[RED 핵심] labeler 는 nginx auth check(GET /api/auth/export/) 에서 403 을 받아야 한다."""
        self._auth(self.labeler)
        response = self.client.get('/api/auth/export/', **self._auth_check_headers())
        assert response.status_code == 403, (
            f'labeler 는 export 파일 auth check 차단(403)돼야 함, 실제 status={response.status_code} '
            f'(정책 미구현 — labeler 가 nginx 다운로드 인증을 통과함)'
        )

    def test_reviewer_blocked_pass_export_files_auth_check(self):
        """reviewer 는 auth check 가 차단된다(2026-07-10 admin 전용 정책)."""
        self._auth(self.reviewer)
        response = self.client.get('/api/auth/export/', **self._auth_check_headers())
        assert response.status_code == 403, (
            f'reviewer 의 export auth check 가 admin 전용이라 403 이어야 함, 실제 status={response.status_code}'
        )

    # ------------------------------------------------------------------ #
    # 2) ExportDetailAPI : GET|DELETE /api/projects/{id}/exports/{export_pk}
    # ------------------------------------------------------------------ #
    def test_labeler_cannot_retrieve_export_detail(self):
        """[RED 핵심] labeler 는 스냅샷 상세(GET /exports/{pk}) 에서 403 을 받아야 한다."""
        self._auth(self.labeler)
        response = self.client.get(f'/api/projects/{self.project.id}/exports/{self.snapshot.id}')
        assert response.status_code == 403, (
            f'labeler 는 export 상세 조회 차단(403)돼야 함, 실제 status={response.status_code}'
        )

    def test_labeler_cannot_delete_export_detail(self):
        """[RED 핵심] labeler 는 스냅샷 삭제(DELETE /exports/{pk}) 에서 403 을 받아야 한다."""
        self._auth(self.labeler)
        response = self.client.delete(f'/api/projects/{self.project.id}/exports/{self.snapshot.id}')
        assert response.status_code == 403, (
            f'labeler 는 export 삭제 차단(403)돼야 함, 실제 status={response.status_code}'
        )

    def test_reviewer_blocked_retrieve_export_detail(self):
        """reviewer 는 스냅샷 상세 조회가 차단된다(2026-07-10 admin 전용 정책)."""
        self._auth(self.reviewer)
        response = self.client.get(f'/api/projects/{self.project.id}/exports/{self.snapshot.id}')
        assert response.status_code == 403, (
            f'reviewer 의 export 상세 조회가 admin 전용이라 403 이어야 함, 실제 status={response.status_code}'
        )

    # ------------------------------------------------------------------ #
    # 3) ExportConvertAPI : POST /api/projects/{id}/exports/{export_pk}/convert
    # ------------------------------------------------------------------ #
    @patch('data_export.api.start_job_async_or_sync')
    def test_labeler_cannot_convert_export(self, _mock_job):
        """[RED 핵심] labeler 는 스냅샷 변환(POST /exports/{pk}/convert) 에서 403 을 받아야 한다."""
        self._auth(self.labeler)
        response = self.client.post(
            f'/api/projects/{self.project.id}/exports/{self.snapshot.id}/convert',
            {'export_type': 'JSON'},
            format='json',
        )
        assert response.status_code == 403, (
            f'labeler 는 export 변환 차단(403)돼야 함, 실제 status={response.status_code} '
            f'(정책 미구현 — labeler 가 포맷 변환 작업을 트리거함)'
        )

    @patch('data_export.api.start_job_async_or_sync')
    def test_reviewer_blocked_convert_export(self, _mock_job):
        """reviewer 는 스냅샷 변환이 차단된다(2026-07-10 admin 전용 정책)."""
        self._auth(self.reviewer)
        response = self.client.post(
            f'/api/projects/{self.project.id}/exports/{self.snapshot.id}/convert',
            {'export_type': 'JSON'},
            format='json',
        )
        assert response.status_code == 403, (
            f'reviewer 의 export 변환이 admin 전용이라 403 이어야 함, 실제 status={response.status_code}'
        )

    # ------------------------------------------------------------------ #
    # 4) ExportFormatsListAPI : GET /api/projects/{id}/export/formats
    # ------------------------------------------------------------------ #
    def test_labeler_cannot_list_export_formats(self):
        """[RED 핵심] labeler 는 export 포맷 목록(GET /export/formats) 에서 403 을 받아야 한다."""
        self._auth(self.labeler)
        response = self.client.get(f'/api/projects/{self.project.id}/export/formats')
        assert response.status_code == 403, (
            f'labeler 는 export 포맷 목록 차단(403)돼야 함, 실제 status={response.status_code}'
        )

    def test_reviewer_blocked_list_export_formats(self):
        """reviewer 는 export 포맷 목록이 차단된다(2026-07-10 admin 전용 정책)."""
        self._auth(self.reviewer)
        response = self.client.get(f'/api/projects/{self.project.id}/export/formats')
        assert response.status_code == 403, (
            f'reviewer 의 export 포맷 목록이 admin 전용이라 403 이어야 함, 실제 status={response.status_code}'
        )
