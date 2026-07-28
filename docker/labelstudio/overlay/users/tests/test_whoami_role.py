"""TDD Red — 갭 B: whoami 응답에 현재 유저의 organization role 노출 (프론트 게이팅용).

배경: F1 프론트는 reviewer/admin 에게만 SubmitButton 을 노출해야 한다. 그러려면
현재 유저가 active_organization 에서 어떤 role(labeler/reviewer/admin)인지 알아야
한다. whoami 는 default-deny 미들웨어 allowlist 에 있어 labeler 도 자기 정보를
조회할 수 있으므로(정상), role 파생 필드를 여기에 얹는 것이 자연스럽다.

검증하려는 목표 동작:
  - GET /api/current-user/whoami 응답에 현재 유저의 active_organization 기준
    OrganizationMember.role 파생 'role' 필드가 포함된다.
  - labeler 유저 → role='labeler'
  - reviewer 유저 → role='reviewer'
  - admin 유저   → role='admin'
  - labeler 도 whoami(자기 정보)는 200 으로 호출 가능해야 한다(격리 예외, allowlist).

현재 구현 상태 (Red 근거):
  - WhoAmIUserSerializer(BaseWhoAmIUserSerializer) 필드에 role 없음
    → 응답 body 에 'role' 키가 없어 RED.

green 단계 예상 최소 구현:
  - BaseWhoAmIUserSerializer 에 SerializerMethodField('role') 추가.
    get_role = OrganizationMember.get_user_role(user, user.active_organization).
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from rest_framework.test import APITestCase
from users.tests.factories import UserFactory

ROLE_LABELER = 'labeler'
ROLE_REVIEWER = 'reviewer'
ROLE_ADMIN = 'admin'

WHOAMI_URL = '/api/current-user/whoami'


def set_role(user, organization, role):
    OrganizationMember.objects.filter(user=user, organization=organization).update(role=role)


class TestWhoAmIRole(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()

        cls.admin = cls.organization.created_by
        cls.reviewer = UserFactory(active_organization=cls.organization)
        cls.labeler = UserFactory(active_organization=cls.organization)

        set_role(cls.admin, cls.organization, ROLE_ADMIN)
        set_role(cls.reviewer, cls.organization, ROLE_REVIEWER)
        set_role(cls.labeler, cls.organization, ROLE_LABELER)

    def _whoami(self, user):
        self.client.force_authenticate(user=user)
        return self.client.get(WHOAMI_URL)

    def test_whoami_exposes_labeler_role(self):
        """[RED 핵심] labeler 는 whoami 를 200 으로 호출 가능하고 role='labeler' 를 본다."""
        response = self._whoami(self.labeler)
        assert response.status_code == 200, (
            f'labeler 도 whoami(자기 정보)는 200 이어야 함(allowlist), 실제={response.status_code}'
        )
        body = response.json()
        assert 'role' in body, f'whoami 응답에 role 필드가 있어야 함: {list(body.keys())}'
        assert body['role'] == ROLE_LABELER, f"labeler 의 role 은 'labeler' 여야 함: {body.get('role')}"

    def test_whoami_exposes_reviewer_role(self):
        """[RED 핵심] reviewer 의 whoami role='reviewer'."""
        response = self._whoami(self.reviewer)
        assert response.status_code == 200, response.content
        body = response.json()
        assert body.get('role') == ROLE_REVIEWER, (
            f"reviewer 의 role 은 'reviewer' 여야 함: {body.get('role')}"
        )

    def test_whoami_exposes_admin_role(self):
        """[RED] admin 의 whoami role='admin'."""
        response = self._whoami(self.admin)
        assert response.status_code == 200, response.content
        body = response.json()
        assert body.get('role') == ROLE_ADMIN, f"admin 의 role 은 'admin' 여야 함: {body.get('role')}"
