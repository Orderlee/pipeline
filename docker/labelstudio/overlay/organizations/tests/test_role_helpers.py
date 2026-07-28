"""TDD Red — 역할 판정 헬퍼 is_admin / is_reviewer (GCP 외주 LS 포크, 2단계 검수 지원).

2단계 순차 검수(reviewer 1차 → admin 최종)를 구현하려면 뷰가 "이 사용자가 관리자냐/
검수자냐"를 판정할 수 있어야 한다. 기존 is_labeler 와 대칭되는 두 헬퍼를 신설한다.

  - OrganizationMember.is_admin(user, org):
        superuser  → True (admin 취급 관례)
        owner(org.created_by) → True
        role == 'admin' → True
        그 외(reviewer/labeler) → False
  - OrganizationMember.is_reviewer(user, org):
        role == 'reviewer' → True
        그 외(admin/labeler/owner/superuser) → False

현행 구현 상태 (RED 근거):
  - OrganizationMember 에 is_admin / is_reviewer 메서드가 아직 없다.
    → getattr 로 안전 참조(수집단계 오류 회피)하되, 부재 시 assertion 실패(RED).
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from rest_framework.test import APITestCase
from users.tests.factories import UserFactory

ROLE_LABELER = OrganizationMember.ROLE_LABELER
ROLE_REVIEWER = OrganizationMember.ROLE_REVIEWER
ROLE_ADMIN = OrganizationMember.ROLE_ADMIN


def _call(name, *args):
    """미존재 헬퍼를 수집단계 오류 없이 런타임 참조. 부재면 sentinel 반환 → assertion 실패(RED)."""
    fn = getattr(OrganizationMember, name, None)
    if fn is None:
        return '<no such helper>'
    return fn(*args)


class TestRoleHelpers(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.owner = cls.organization.created_by

        cls.superuser = UserFactory(is_superuser=True)
        OrganizationMember.objects.create(
            user=cls.superuser, organization=cls.organization, role=ROLE_LABELER
        )

        cls.admin = UserFactory()
        OrganizationMember.objects.create(
            user=cls.admin, organization=cls.organization, role=ROLE_ADMIN
        )

        cls.reviewer = UserFactory()
        OrganizationMember.objects.create(
            user=cls.reviewer, organization=cls.organization, role=ROLE_REVIEWER
        )

        cls.labeler = UserFactory()
        OrganizationMember.objects.create(
            user=cls.labeler, organization=cls.organization, role=ROLE_LABELER
        )

    # ------------------------------------------------------------------ #
    # is_admin
    # ------------------------------------------------------------------ #
    def test_is_admin_true_for_owner(self):
        assert _call('is_admin', self.owner, self.organization) is True, (
            'org owner(created_by) 는 is_admin==True 여야 함 (헬퍼 부재면 RED)'
        )

    def test_is_admin_true_for_superuser(self):
        assert _call('is_admin', self.superuser, self.organization) is True, (
            'superuser 는 is_admin==True 여야 함 (admin 취급 관례)'
        )

    def test_is_admin_true_for_admin_role(self):
        assert _call('is_admin', self.admin, self.organization) is True, (
            'role==admin 멤버는 is_admin==True 여야 함'
        )

    def test_is_admin_false_for_reviewer(self):
        assert _call('is_admin', self.reviewer, self.organization) is False, (
            'reviewer 는 is_admin==False 여야 함'
        )

    def test_is_admin_false_for_labeler(self):
        assert _call('is_admin', self.labeler, self.organization) is False, (
            'labeler 는 is_admin==False 여야 함'
        )

    # ------------------------------------------------------------------ #
    # is_reviewer
    # ------------------------------------------------------------------ #
    def test_is_reviewer_true_for_reviewer(self):
        assert _call('is_reviewer', self.reviewer, self.organization) is True, (
            'role==reviewer 멤버는 is_reviewer==True 여야 함 (헬퍼 부재면 RED)'
        )

    def test_is_reviewer_false_for_admin(self):
        assert _call('is_reviewer', self.admin, self.organization) is False, (
            'admin 은 is_reviewer==False 여야 함'
        )

    def test_is_reviewer_false_for_labeler(self):
        assert _call('is_reviewer', self.labeler, self.organization) is False, (
            'labeler 는 is_reviewer==False 여야 함'
        )
