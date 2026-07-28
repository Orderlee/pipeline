"""TDD Red — 최소권한 default role + superuser/owner 예외 (GCP 외주 LS 포크 전용).

이번 사이클 범위: **OrganizationMember.role 의 default 를 최소권한(labeler)으로 바꾸되,
관리자/오너가 자기 조직에서 잠기지 않도록 is_labeler 에 예외를 두는 동작 계약** 한정.
(개별 뷰 scope 격리·default-deny 미들웨어는 다른 사이클에서 이미 다룸.)

근본 원인:
  현재 OrganizationMember.role default = 'reviewer' 라, role 을 명시하지 않고 생성된
  멤버가 전부 reviewer 권한을 갖는다(최소권한 원칙 위배). 반대로 default 를 labeler 로
  바꾸면 org 를 만든 owner 와 superuser 까지 격리 대상이 되어 자기 조직에서 잠긴다.
  → default 는 labeler 로 내리되, is_labeler 는 superuser/owner 를 예외로 둬야 한다.

검증하려는 목표 동작:
  1) 최소권한 default: role 미지정 멤버의 role == 'labeler'.
  2) superuser 예외: is_labeler(superuser, org) == False (role 이 labeler 여도).
  3) owner 예외: is_labeler(owner, org) == False (role 이 labeler 여도).
  4) 일반 멤버: owner/superuser 아닌 labeler 멤버는 is_labeler == True.
  5) 회귀 가드: 명시적 reviewer / admin 은 is_labeler == False (기존과 동일).

현재 구현 상태 (Red 근거):
  - role default = ROLE_REVIEWER → (1) 실패.
  - is_labeler 는 get_user_role == ROLE_LABELER 만 본다. superuser/owner 예외가 없어
    role 이 labeler 이면 (2)(3) 모두 True 를 반환 → 실패.

green 단계가 바꿀 것:
  - role default 를 ROLE_LABELER 로 변경 + 마이그레이션.
  - is_labeler 에 superuser(user.is_superuser) / owner(organization.created_by == user
    또는 OrganizationMember.is_owner) 예외 분기 추가.
"""
from organizations.models import OrganizationMember
from organizations.tests.factories import OrganizationFactory
from rest_framework.test import APITestCase
from users.tests.factories import UserFactory

ROLE_LABELER = OrganizationMember.ROLE_LABELER
ROLE_REVIEWER = OrganizationMember.ROLE_REVIEWER
ROLE_ADMIN = OrganizationMember.ROLE_ADMIN


class LabelerRoleDefaultBase(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.organization = OrganizationFactory()
        cls.owner = cls.organization.created_by

    def _add_member(self, role=None):
        """org 에 새 멤버를 붙인다. role=None 이면 모델 default 에 맡긴다."""
        user = UserFactory()
        kwargs = {'user': user, 'organization': self.organization}
        if role is not None:
            kwargs['role'] = role
        member = OrganizationMember.objects.create(**kwargs)
        return user, member


# --------------------------------------------------------------------------- #
# 1) 최소권한 default: role 미지정 멤버의 role == labeler                       #
# --------------------------------------------------------------------------- #
class TestLeastPrivilegeDefaultRole(LabelerRoleDefaultBase):
    def test_unspecified_role_defaults_to_labeler(self):
        """[RED] role 을 주지 않고 만든 멤버의 role 은 최소권한 labeler 여야 함."""
        _, member = self._add_member(role=None)
        member.refresh_from_db()
        assert member.role == ROLE_LABELER, (
            f'role 미지정 default 는 최소권한 labeler 여야 함, 실제={member.role!r}'
        )


# --------------------------------------------------------------------------- #
# 2) superuser 예외: role 이 labeler 여도 is_labeler == False                   #
# --------------------------------------------------------------------------- #
class TestSuperuserExemptFromLabeler(LabelerRoleDefaultBase):
    def test_superuser_is_not_labeler_even_with_labeler_role(self):
        """[RED] superuser 는 role 이 labeler 여도 격리 대상 아님(is_labeler False)."""
        su = UserFactory(is_superuser=True)
        OrganizationMember.objects.create(
            user=su, organization=self.organization, role=ROLE_LABELER
        )
        assert OrganizationMember.is_labeler(su, self.organization) is False, (
            'superuser 는 role 이 labeler 여도 is_labeler==False 여야 함'
        )


# --------------------------------------------------------------------------- #
# 3) owner 예외: org 를 만든 owner 는 role 이 labeler 여도 is_labeler == False   #
# --------------------------------------------------------------------------- #
class TestOwnerExemptFromLabeler(LabelerRoleDefaultBase):
    def test_owner_is_not_labeler_even_with_labeler_role(self):
        """[RED] org owner(created_by) 는 role 이 labeler 여도 안 잠김(is_labeler False)."""
        # owner 멤버십을 명시적으로 labeler 로 낮춰도 예외가 적용돼야 한다.
        OrganizationMember.objects.filter(
            user=self.owner, organization=self.organization
        ).update(role=ROLE_LABELER)
        # 전제 확인: created_by 가 owner 로 판정되는 방식(is_owner)이 실제로 존재/일치.
        member = OrganizationMember.objects.get(
            user=self.owner, organization=self.organization
        )
        assert member.is_owner is True, 'created_by 는 is_owner==True 여야 함(전제)'
        assert OrganizationMember.is_labeler(self.owner, self.organization) is False, (
            'org owner 는 role 이 labeler 여도 is_labeler==False 여야 함'
        )


# --------------------------------------------------------------------------- #
# 4) 일반 멤버는 labeler: owner/superuser 아닌 labeler 멤버는 is_labeler True    #
# --------------------------------------------------------------------------- #
class TestOrdinaryMemberIsLabeler(LabelerRoleDefaultBase):
    def test_plain_labeler_member_is_labeler(self):
        """[RED-guard] owner/superuser 아닌 labeler 멤버는 격리 적용(is_labeler True)."""
        user, _ = self._add_member(role=ROLE_LABELER)
        assert OrganizationMember.is_labeler(user, self.organization) is True, (
            '일반 labeler 멤버는 is_labeler==True 여야 함'
        )

    def test_unspecified_role_member_is_labeler(self):
        """[RED] default 가 labeler 로 바뀌면 role 미지정 일반 멤버도 is_labeler True."""
        user, _ = self._add_member(role=None)
        assert OrganizationMember.is_labeler(user, self.organization) is True, (
            'role 미지정 일반 멤버는 (default=labeler 이므로) is_labeler==True 여야 함'
        )


# --------------------------------------------------------------------------- #
# 5) 회귀 가드: 명시적 reviewer / admin 은 is_labeler == False                   #
# --------------------------------------------------------------------------- #
class TestExplicitNonLabelerRolesUnaffected(LabelerRoleDefaultBase):
    def test_reviewer_is_not_labeler(self):
        user, _ = self._add_member(role=ROLE_REVIEWER)
        assert OrganizationMember.is_labeler(user, self.organization) is False, (
            'reviewer 는 is_labeler==False 여야 함'
        )

    def test_admin_is_not_labeler(self):
        user, _ = self._add_member(role=ROLE_ADMIN)
        assert OrganizationMember.is_labeler(user, self.organization) is False, (
            'admin 은 is_labeler==False 여야 함'
        )
