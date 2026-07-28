"""TDD Red — 라벨러의 export(내보내기) 경로 완전 차단 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **export 경로의 라벨러 차단** 한정.
정책 = 라벨러는 export 를 아예 못 함(HTTP 403). export/클립 생성은
admin·reviewer 및 관리 스크립트의 몫. (본인 태스크 스코핑이 아니라 완전 차단.)

검증하려는 목표 동작:
  - role='labeler' 유저가 아래 export 경로를 호출하면 403 (거부):
      1) easy export      : GET  /api/projects/{id}/export?exportType=JSON
                            (및 download_all_tasks=true)
      2) snapshot 생성    : POST /api/projects/{id}/exports/
      3) snapshot 목록    : GET  /api/projects/{id}/exports/
         snapshot 다운로드: GET  /api/projects/{id}/exports/{export_pk}/download
  - role='reviewer' / 'admin' 은 export 허용(기존 동작 유지, 차단되지 않음).

현재 구현 상태 (Red 근거):
  data_export/api.py 의 ExportAPI / ExportListAPI / ExportDownloadAPI 는 모두
      permission_required = all_permissions.projects_change
  만 요구하고 role 을 보지 않는다. 따라서 org 멤버이기만 하면 labeler 도 통과하여
  export 경로에서 403 이 아닌 응답(200/201/파일)을 받는다 → 이 테스트가 실패(Red)해야 정상.

설계 계약(Cycle 1~2 에서 이미 도입됨):
  - OrganizationMember.role : labeler/reviewer/admin
  - OrganizationMember.is_labeler(user, organization)
  green 단계는 export APIView 들에 "labeler 면 403" 규칙(permission/check)을 넣어야 한다.
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


class TestLabelerExportBlocked(APITestCase):
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

        # 내보낼 데이터가 있어야 easy export / snapshot 이 정상 동작하므로 태스크+annotation 최소 구성
        cls.task = TaskFactory(project=cls.project, data={'text': 'x'}, assignee=cls.labeler)
        AnnotationFactory(task=cls.task, project=cls.project, completed_by=cls.admin, result=[])

        # snapshot 다운로드 대상 (완료 상태의 export 스냅샷)
        cls.snapshot = Export.objects.create(project=cls.project, status=Export.Status.COMPLETED)

    def _auth(self, user):
        self.client.force_authenticate(user=user)

    # ------------------------------------------------------------------ #
    # 1) easy export : GET /api/projects/{id}/export
    # ------------------------------------------------------------------ #
    def test_labeler_cannot_easy_export(self):
        """[RED 핵심] labeler 는 easy export(GET /export) 에서 403 을 받아야 한다."""
        self._auth(self.labeler)
        response = self.client.get(f'/api/projects/{self.project.id}/export?exportType=JSON')
        assert response.status_code == 403, (
            f'labeler 는 easy export 차단(403)돼야 함, 실제 status={response.status_code} '
            f'(정책 미구현 — labeler 가 export 파일을 받음)'
        )

    def test_labeler_cannot_easy_export_all_tasks(self):
        """[RED 핵심] download_all_tasks=true easy export 도 labeler 는 403."""
        self._auth(self.labeler)
        response = self.client.get(
            f'/api/projects/{self.project.id}/export?exportType=JSON&download_all_tasks=true'
        )
        assert response.status_code == 403, (
            f'labeler 는 전체 태스크 easy export 도 차단(403)돼야 함, 실제 status={response.status_code}'
        )

    def test_reviewer_blocked_easy_export(self):
        """reviewer 는 easy export 가 차단된다(2026-07-10 admin 전용 정책)."""
        self._auth(self.reviewer)
        response = self.client.get(f'/api/projects/{self.project.id}/export?exportType=JSON')
        assert response.status_code == 403, (
            f'reviewer 의 easy export 가 admin 전용이라 403 이어야 함, 실제 status={response.status_code}'
        )

    def test_admin_can_easy_export(self):
        """admin 은 easy export 가 차단되지 않는다(기존 동작 유지)."""
        self._auth(self.admin)
        response = self.client.get(f'/api/projects/{self.project.id}/export?exportType=JSON')
        assert response.status_code != 403, (
            f'admin 의 easy export 가 막히면 안 됨, 실제 status={response.status_code}'
        )

    # ------------------------------------------------------------------ #
    # 2) snapshot 생성 : POST /api/projects/{id}/exports/
    # ------------------------------------------------------------------ #
    @patch('data_export.models.Export.run_file_exporting')
    def test_labeler_cannot_create_export_snapshot(self, _mock_run):
        """[RED 핵심] labeler 는 snapshot 생성(POST /exports/) 에서 403 을 받아야 한다."""
        self._auth(self.labeler)
        response = self.client.post(f'/api/projects/{self.project.id}/exports/', {}, format='json')
        assert response.status_code == 403, (
            f'labeler 는 snapshot 생성 차단(403)돼야 함, 실제 status={response.status_code} '
            f'(정책 미구현 — labeler 가 export 스냅샷을 생성함)'
        )

    @patch('data_export.models.Export.run_file_exporting')
    def test_reviewer_blocked_create_export_snapshot(self, _mock_run):
        """reviewer 는 snapshot 생성이 차단된다(2026-07-10 admin 전용 정책)."""
        self._auth(self.reviewer)
        response = self.client.post(f'/api/projects/{self.project.id}/exports/', {}, format='json')
        assert response.status_code == 403, (
            f'reviewer 의 snapshot 생성이 admin 전용이라 403 이어야 함, 실제 status={response.status_code}'
        )

    # ------------------------------------------------------------------ #
    # 3) snapshot 목록 / 다운로드
    # ------------------------------------------------------------------ #
    def test_labeler_cannot_list_export_snapshots(self):
        """[RED 핵심] labeler 는 snapshot 목록(GET /exports/) 에서 403 을 받아야 한다."""
        self._auth(self.labeler)
        response = self.client.get(f'/api/projects/{self.project.id}/exports/')
        assert response.status_code == 403, (
            f'labeler 는 snapshot 목록 차단(403)돼야 함, 실제 status={response.status_code}'
        )

    def test_labeler_cannot_download_export_snapshot(self):
        """[RED 핵심] labeler 는 snapshot 다운로드(GET /exports/{pk}/download) 에서 403."""
        self._auth(self.labeler)
        response = self.client.get(
            f'/api/projects/{self.project.id}/exports/{self.snapshot.id}/download?exportType=JSON'
        )
        assert response.status_code == 403, (
            f'labeler 는 snapshot 다운로드 차단(403)돼야 함, 실제 status={response.status_code}'
        )

    def test_reviewer_blocked_list_export_snapshots(self):
        """reviewer 는 snapshot 목록이 차단된다(2026-07-10 admin 전용 정책)."""
        self._auth(self.reviewer)
        response = self.client.get(f'/api/projects/{self.project.id}/exports/')
        assert response.status_code == 403, (
            f'reviewer 의 snapshot 목록이 admin 전용이라 403 이어야 함, 실제 status={response.status_code}'
        )
