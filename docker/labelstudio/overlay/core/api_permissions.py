from rest_framework.permissions import SAFE_METHODS, BasePermission


class HasObjectPermission(BasePermission):
    def has_object_permission(self, request, view, obj):
        return obj.has_permission(request.user)


class DenyLabelers(BasePermission):
    """Deny access to users whose role in their active organization is 'labeler'.

    Reuses OrganizationMember.is_labeler so role logic stays in one place. Returning
    False here yields a 403 (PermissionDenied) before the view runs.
    """

    def has_permission(self, request, view):
        from organizations.models import OrganizationMember

        user = request.user
        return not OrganizationMember.is_labeler(user, user.active_organization)


class AdminOnly(BasePermission):
    """Allow only org admins (owner/superuser/membership role=admin).

    Unlike DenyLabelers (labeler-only block), this also blocks reviewers —
    used for data-egress/ingress surfaces (import/export) per least privilege.
    """

    def has_permission(self, request, view):
        from organizations.models import OrganizationMember

        user = request.user
        return OrganizationMember.is_admin(user, getattr(user, 'active_organization', None))


class MemberHasOwnerPermission(BasePermission):
    def has_object_permission(self, request, view, obj):
        if request.method not in SAFE_METHODS and not request.user.own_organization:
            return False

        return obj.has_permission(request.user)
