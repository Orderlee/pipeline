"""This file and its contents are licensed under the Apache License 2.0. Please see the included NOTICE for copyright information and LICENSE for a copy of the license.
"""
import logging

from django.http import HttpResponseForbidden
from organizations.models import Organization, OrganizationMember

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Labeler default-deny whitelist (GCP outsourced LS fork only).               #
#                                                                             #
# Maps a fully namespaced URL name -> set of HTTP methods a labeler may use.  #
# Anything not present here is denied (403) for labeler-role users.           #
# Reviewer / admin / anonymous requests are never touched by this middleware. #
# --------------------------------------------------------------------------- #
LABELER_ALLOWED = {
    # --- auth / bootstrap -------------------------------------------------- #
    'current-user-whoami': {'GET'},
    'current-user-hotkeys': {'GET', 'PATCH', 'POST'},
    'product-tour': {'GET', 'POST', 'PATCH'},
    'user-login': {'GET', 'POST'},
    'logout': {'GET', 'POST'},
    'api-version': {'GET'},
    'version': {'GET'},
    'health': {'GET'},
    'main': {'GET'},
    'projects:project-index': {'GET'},        # /projects/ SPA HTML
    'data_manager:project-data': {'GET'},     # /projects/<pk>/data/ SPA HTML (DM 페이지)
    'user-list': {'GET'},                     # DM 사용자 컬럼(멤버명 메타 — 데이터 아님)
    'ml:api:ml-list': {'GET'},                # 에디터/DM 의 ML 백엔드 조회(읽기)
    # --- entry: project / data manager ------------------------------------- #
    #   ※ 태스크/프로젝트 데이터는 queryset 스코프로 이미 격리됨. 아래 뷰(=탭 필터)/컬럼은
    #     DM UI 운영 엔드포인트라 write 허용해도 데이터 유출 아님(프로젝트 스코프 내 탭 관리).
    'projects:api:project-list': {'GET'},
    'projects:api:project-detail': {'GET'},
    'projects:api:project-next': {'GET'},
    'projects:api:project-model-versions': {'GET'},
    'projects:api:project-submit-state': {'GET'},   # F1 버튼 래퍼가 마운트 시 조회(스코프됨)
    'projects:api:label-stream-history': {'GET'},    # 라벨링 스트림 히스토리(tasks_view)
    'data_manager:dm-project': {'GET'},
    'data_manager:dm-columns': {'GET'},
    'data_manager:api:view-list': {'GET', 'POST'},
    'data_manager:api:view-detail': {'GET', 'PATCH', 'DELETE'},
    'data_manager:api:view-update-order': {'POST'},
    'data_manager:api:view-reset': {'POST'},
    # --- labeling flow ----------------------------------------------------- #
    'tasks:api:task-list': {'GET'},
    'tasks:api:task-detail': {'GET'},
    'tasks:api:task-annotations': {'GET', 'POST'},
    'tasks:api:task-drafts': {'GET', 'POST'},
    'tasks:api:task-annotations-drafts': {'POST'},
    'tasks:api-annotations:annotation-detail': {'GET', 'PATCH', 'DELETE'},
    'tasks:api-annotations:annotation-convert-to-draft': {'POST'},
    'tasks:api-drafts:draft-detail': {'GET', 'PATCH', 'DELETE'},
    'tasks:api-predictions:prediction-list': {'GET'},
    'tasks:api-predictions:prediction-detail': {'GET'},
    'labels_manager:api-labels:label_link-list': {'GET'},
    'labels_manager:api-labels:label-list': {'POST'},
    # --- media ------------------------------------------------------------- #
    'storages:task-storage-data-resolve': {'GET'},
    'storages:task-storage-data-presign': {'GET'},
    'data_import:data-upload': {'GET'},
}


class LabelerDefaultDenyMiddleware:
    """Default-deny backstop for labeler-role users on the outsourced LS fork.

    OSS ``User.has_permission`` is effectively a dummy ``True``, so per-view
    permission checks do not isolate labelers on un-audited views. This
    middleware blocks (403) any request from a labeler whose resolved
    ``(namespaced url name, HTTP method)`` is not on ``LABELER_ALLOWED``.
    Non-labeler and unauthenticated requests pass through untouched.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        return self.get_response(request)

    @staticmethod
    def _resolve_user(request):
        """Best-effort resolution of the acting authenticated user.

        - Session and JWT auth populate ``request.user`` via upstream middleware.
        - DRF's test client sets ``request._force_auth_user`` before the chain runs.
        - DRF token/JWT header auth otherwise happens inside the view, so attempt
          it here to avoid a labeler bypassing default-deny via an API token.
        """
        user = getattr(request, 'user', None)
        if user is not None and user.is_authenticated:
            return user

        forced = getattr(request, '_force_auth_user', None)
        if forced is not None and getattr(forced, 'is_authenticated', False):
            return forced

        # Try DRF authenticators (Token / JWT via headers) which normally only
        # run inside the view. Any failure means "not authenticated here".
        try:
            from rest_framework.request import Request as DRFRequest
            from rest_framework.views import APIView

            drf_request = DRFRequest(request, authenticators=APIView().get_authenticators())
            resolved = drf_request.user
            if resolved is not None and resolved.is_authenticated:
                return resolved
        except Exception:
            return None

        return None

    def process_view(self, request, view_func, view_args, view_kwargs):
        user = self._resolve_user(request)
        if user is None or not user.is_authenticated:
            return None

        organization = getattr(user, 'active_organization', None)
        if organization is None:
            return None

        if not OrganizationMember.is_labeler(user, organization):
            return None

        resolver_match = getattr(request, 'resolver_match', None)
        if resolver_match is None:
            return HttpResponseForbidden('Labeler access denied.')

        allowed_methods = LABELER_ALLOWED.get(resolver_match.view_name)
        if allowed_methods and request.method in allowed_methods:
            return None

        return HttpResponseForbidden('Labeler access denied.')


class DummyGetSessionMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        org = Organization.objects.first()
        user = request.user
        if user and user.is_authenticated and user.active_organization is None:
            user.active_organization = org
            user.save(update_fields=['active_organization'])
        if org is not None:
            request.session['organization_pk'] = org.id
        response = self.get_response(request)
        return response
