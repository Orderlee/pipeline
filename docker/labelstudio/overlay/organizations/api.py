"""This file and its contents are licensed under the Apache License 2.0. Please see the included NOTICE for copyright information and LICENSE for a copy of the license.
"""
import logging

from core.api_permissions import DenyLabelers
from core.feature_flags import flag_set
from core.mixins import GetParentObjectMixin
from core.utils.common import load_func
from django.conf import settings
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.utils.functional import cached_property
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiParameter, OpenApiResponse, extend_schema
from organizations.models import Organization, OrganizationMember
from organizations.serializers import (
    OrganizationIdSerializer,
    OrganizationInviteSerializer,
    OrganizationMemberListParamsSerializer,
    OrganizationMemberListSerializer,
    OrganizationMemberSerializer,
    OrganizationSerializer,
)
from projects.models import Project
from rest_framework import generics, status
from rest_framework.exceptions import NotFound, PermissionDenied
from rest_framework.generics import get_object_or_404
from rest_framework.pagination import PageNumberPagination
from rest_framework.parsers import FormParser, JSONParser, MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.settings import api_settings
from rest_framework.views import APIView
from tasks.models import Annotation
from users.models import User

from label_studio.core.permissions import ViewClassPermission, all_permissions
from label_studio.core.utils.params import bool_from_request

logger = logging.getLogger(__name__)

HasObjectPermission = load_func(settings.MEMBER_PERM)


@method_decorator(
    name='get',
    decorator=extend_schema(
        tags=['Organizations'],
        summary='List your organizations',
        description="""
        Return a list of the organizations you've created or that you have access to.
        """,
        extensions={
            'x-fern-sdk-group-name': 'organizations',
            'x-fern-sdk-method-name': 'list',
            'x-fern-audiences': ['public'],
        },
    ),
)
class OrganizationListAPI(generics.ListCreateAPIView):
    queryset = Organization.objects.all()
    parser_classes = (JSONParser, FormParser, MultiPartParser)
    permission_required = ViewClassPermission(
        GET=all_permissions.organizations_view,
        PUT=all_permissions.organizations_change,
        POST=all_permissions.organizations_create,
        PATCH=all_permissions.organizations_change,
        DELETE=all_permissions.organizations_change,
    )
    serializer_class = OrganizationIdSerializer

    def filter_queryset(self, queryset):
        return queryset.filter(
            organizationmember__in=self.request.user.om_through.filter(deleted_at__isnull=True)
        ).distinct()

    def get(self, request, *args, **kwargs):
        return super(OrganizationListAPI, self).get(request, *args, **kwargs)

    @extend_schema(exclude=True)
    def post(self, request, *args, **kwargs):
        return super(OrganizationListAPI, self).post(request, *args, **kwargs)


class OrganizationMemberListPagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = 'page_size'

    def get_page_size(self, request):
        # emulate "unlimited" page_size
        if (
            self.page_size_query_param in request.query_params
            and request.query_params[self.page_size_query_param] == '-1'
        ):
            return 1000000
        return super().get_page_size(request)


@method_decorator(
    name='get',
    decorator=extend_schema(
        tags=['Organizations'],
        summary='Get organization members list',
        description='Retrieve a list of the organization members and their IDs.',
        parameters=[
            OpenApiParameter(
                name='contributed_to_projects',
                type=OpenApiTypes.BOOL,
                location='query',
                description='Whether to include projects created and contributed to by the members.',
            ),
        ],
        extensions={
            'x-fern-sdk-group-name': ['organizations', 'members'],
            'x-fern-sdk-method-name': 'list',
            'x-fern-audiences': ['public'],
            'x-fern-pagination': {
                'offset': '$request.page',
                'results': '$response.results',
            },
        },
    ),
)
class OrganizationMemberListAPI(generics.ListAPIView):
    parser_classes = (JSONParser, FormParser, MultiPartParser)
    permission_required = ViewClassPermission(
        GET=all_permissions.organizations_view,
        PUT=all_permissions.organizations_change,
        PATCH=all_permissions.organizations_change,
        DELETE=all_permissions.organizations_change,
    )
    serializer_class = OrganizationMemberListSerializer
    pagination_class = OrganizationMemberListPagination

    @cached_property
    def paginated_members(self):
        return self.paginate_queryset(self.filter_queryset(self.get_queryset()))

    def _get_created_projects_map(self):
        members = self.paginated_members
        user_ids = [member.user_id for member in members]
        projects = (
            Project.objects.filter(created_by_id__in=user_ids, organization=self.request.user.active_organization)
            .values('created_by_id', 'id', 'title')
            .distinct()
        )
        projects_map = {}
        for project in projects:
            projects_map.setdefault(project['created_by_id'], []).append(
                {
                    'id': project['id'],
                    'title': project['title'],
                }
            )
        return projects_map

    def _get_contributed_to_projects_map(self):
        members = self.paginated_members
        user_ids = [member.user_id for member in members]
        org_project_ids = Project.objects.filter(organization=self.request.user.active_organization).values_list(
            'id', flat=True
        )
        annotations = (
            Annotation.objects.filter(completed_by__in=list(user_ids), project__in=list(org_project_ids))
            .values('completed_by', 'project_id')
            .distinct()
        )
        project_ids = [annotation['project_id'] for annotation in annotations]
        projects_map = Project.objects.in_bulk(id_list=project_ids, field_name='id')

        contributed_to_projects_map = {}
        for annotation in annotations:
            project = projects_map[annotation['project_id']]
            contributed_to_projects_map.setdefault(annotation['completed_by'], []).append(
                {
                    'id': project.id,
                    'title': project.title,
                }
            )
        return contributed_to_projects_map

    def get_serializer_context(self):
        context = super().get_serializer_context()
        contributed_to_projects = bool_from_request(self.request.GET, 'contributed_to_projects', False)
        return {
            'contributed_to_projects': contributed_to_projects,
            'created_projects_map': self._get_created_projects_map() if contributed_to_projects else None,
            'contributed_to_projects_map': self._get_contributed_to_projects_map()
            if contributed_to_projects
            else None,
            **context,
        }

    def get_queryset(self):
        org = generics.get_object_or_404(self.request.user.organizations, pk=self.kwargs[self.lookup_field])
        if flag_set('fix_backend_dev_3134_exclude_deactivated_users', self.request.user):
            serializer = OrganizationMemberListParamsSerializer(data=self.request.GET)
            serializer.is_valid(raise_exception=True)
            active = serializer.validated_data.get('active')

            # return only active users (exclude DISABLED and NOT_ACTIVATED)
            if active:
                return org.active_members.prefetch_related('user__om_through').order_by('user__username')

            # organization page to show all members
            return org.members.prefetch_related('user__om_through').order_by('user__username')
        else:
            return org.members.prefetch_related('user__om_through').order_by('user__username')

    def list(self, request, *args, **kwargs):
        page = self.paginated_members   # Using cached property to avoid multiple queries
        serializer = self.get_serializer(page, many=True)
        return self.get_paginated_response(serializer.data)


@method_decorator(
    name='get',
    decorator=extend_schema(
        tags=['Organizations'],
        summary='Get organization member details',
        description='Get organization member details by user ID.',
        parameters=[
            OpenApiParameter(
                name='user_pk',
                type=OpenApiTypes.INT,
                location='path',
                description='A unique integer value identifying the user to get organization details for.',
            ),
            OpenApiParameter(
                name='contributed_to_projects',
                type=OpenApiTypes.BOOL,
                location='query',
                description='Whether to include projects created and contributed to by the member.',
            ),
        ],
        responses={200: OrganizationMemberSerializer()},
        extensions={
            'x-fern-sdk-group-name': ['organizations', 'members'],
            'x-fern-sdk-method-name': 'get',
            'x-fern-audiences': ['public'],
        },
    ),
)
@method_decorator(
    name='delete',
    decorator=extend_schema(
        tags=['Organizations'],
        summary='Soft delete an organization member',
        description='Soft delete a member from the organization.',
        parameters=[
            OpenApiParameter(
                name='user_pk',
                type=OpenApiTypes.INT,
                location='path',
                description='A unique integer value identifying the user to be deleted from the organization.',
            ),
        ],
        responses={
            204: OpenApiResponse(description='Member deleted successfully.'),
            405: OpenApiResponse(description='User cannot soft delete self.'),
            404: OpenApiResponse(description='Member not found'),
            403: OpenApiResponse(description='You can delete members only for your current active organization'),
        },
        extensions={
            'x-fern-sdk-group-name': ['organizations', 'members'],
            'x-fern-sdk-method-name': 'delete',
            'x-fern-audiences': ['public'],
        },
    ),
)
class OrganizationMemberDetailAPI(GetParentObjectMixin, generics.RetrieveDestroyAPIView):
    permission_required = ViewClassPermission(
        GET=all_permissions.organizations_view,
        DELETE=all_permissions.organizations_change,
    )
    parent_queryset = Organization.objects.all()
    parser_classes = (JSONParser, FormParser, MultiPartParser)
    serializer_class = OrganizationMemberSerializer
    http_method_names = ['delete', 'get']

    @property
    def permission_classes(self):
        if self.request.method == 'DELETE':
            return [IsAuthenticated, HasObjectPermission]
        return api_settings.DEFAULT_PERMISSION_CLASSES

    def get_queryset(self):
        return OrganizationMember.objects.filter(organization=self.parent_object).select_related('user')

    def get_serializer_context(self):
        return {
            **super().get_serializer_context(),
            'organization': self.parent_object,
            'contributed_to_projects': bool_from_request(self.request.GET, 'contributed_to_projects', False),
        }

    def get(self, request, pk, user_pk):
        queryset = self.get_queryset()
        member = get_object_or_404(queryset, user=user_pk)
        self.check_object_permissions(request, member)
        serializer = self.get_serializer(member)
        return Response(serializer.data)

    def delete(self, request, pk=None, user_pk=None):
        org = self.parent_object
        if org != request.user.active_organization:
            raise PermissionDenied('You can delete members only for your current active organization')

        user = get_object_or_404(User, pk=user_pk)
        member = get_object_or_404(OrganizationMember, user=user, organization=org)
        if member.deleted_at is not None:
            raise NotFound('Member not found')

        if member.user_id == request.user.id:
            return Response({'detail': 'User cannot soft delete self'}, status=status.HTTP_405_METHOD_NOT_ALLOWED)

        member.soft_delete()
        return Response(status=204)  # 204 No Content is a common HTTP status for successful delete requests


@method_decorator(
    name='get',
    decorator=extend_schema(
        tags=['Organizations'],
        summary='Get organization settings',
        description='Retrieve the settings for a specific organization by ID.',
        extensions={
            'x-fern-sdk-group-name': 'organizations',
            'x-fern-sdk-method-name': 'get',
            'x-fern-audiences': ['public'],
        },
    ),
)
@method_decorator(
    name='patch',
    decorator=extend_schema(
        tags=['Organizations'],
        summary='Update organization settings',
        description='Update the settings for a specific organization by ID.',
        extensions={
            'x-fern-sdk-group-name': 'organizations',
            'x-fern-sdk-method-name': 'update',
            'x-fern-audiences': ['public'],
        },
    ),
)
class OrganizationAPI(generics.RetrieveUpdateAPIView):

    parser_classes = (JSONParser, FormParser, MultiPartParser)
    queryset = Organization.objects.all()
    permission_required = all_permissions.organizations_change
    serializer_class = OrganizationSerializer

    redirect_route = 'organizations-dashboard'
    redirect_kwarg = 'pk'

    def get(self, request, *args, **kwargs):
        return super(OrganizationAPI, self).get(request, *args, **kwargs)

    def patch(self, request, *args, **kwargs):
        return super(OrganizationAPI, self).patch(request, *args, **kwargs)

    @extend_schema(exclude=True)
    def put(self, request, *args, **kwargs):
        return super(OrganizationAPI, self).put(request, *args, **kwargs)


@method_decorator(
    name='get',
    decorator=extend_schema(
        tags=['Invites'],
        summary='Get organization invite link',
        description='Get a link to use to invite a new member to an organization in Label Studio Enterprise.',
        responses={200: OrganizationInviteSerializer()},
        extensions={
            'x-fern-sdk-group-name': 'organizations',
            'x-fern-sdk-method-name': 'get_invite',
            'x-fern-audiences': ['public'],
        },
    ),
)
class OrganizationInviteAPI(generics.RetrieveAPIView):
    parser_classes = (JSONParser,)
    queryset = Organization.objects.all()
    permission_required = all_permissions.organizations_invite

    def get(self, request, *args, **kwargs):
        org = request.user.active_organization
        invite_url = '{}?token={}'.format(reverse('user-signup'), org.token)
        if hasattr(settings, 'FORCE_SCRIPT_NAME') and settings.FORCE_SCRIPT_NAME:
            invite_url = invite_url.replace(settings.FORCE_SCRIPT_NAME, '', 1)
        serializer = OrganizationInviteSerializer(data={'invite_url': invite_url, 'token': org.token})
        serializer.is_valid()
        return Response(serializer.data, status=200)


@method_decorator(
    name='post',
    decorator=extend_schema(
        tags=['Invites'],
        summary='Reset organization token',
        description='Reset the token used in the invitation link to invite someone to an organization.',
        responses={200: OrganizationInviteSerializer()},
        extensions={
            'x-fern-sdk-group-name': 'organizations',
            'x-fern-sdk-method-name': 'reset_token',
            'x-fern-audiences': ['public'],
        },
    ),
)
class OrganizationResetTokenAPI(APIView):
    permission_required = all_permissions.organizations_invite
    parser_classes = (JSONParser,)

    def post(self, request, *args, **kwargs):
        org = request.user.active_organization
        org.reset_token()
        logger.debug(f'New token for organization {org.pk} is {org.token}')
        invite_url = '{}?token={}'.format(reverse('user-signup'), org.token)
        serializer = OrganizationInviteSerializer(data={'invite_url': invite_url, 'token': org.token})
        serializer.is_valid()
        return Response(serializer.data, status=201)


@extend_schema(exclude=True)
class DashboardSummaryAPI(APIView):
    """admin 전용 현황 대시보드 집계 (GCP 외주 포크 전용, 읽기 전용).

    org 스코프 집계: summary(전체) · projects[](프로젝트별 검수 현황) ·
    workers[](Annotation.completed_by 기준 — 라이브 데이터가 assignee 미배정이라
    '실제로 라벨한 사람'이 작업자 사실 기록). 라벨러는 미들웨어 default-deny +
    DenyLabelers, reviewer 는 아래 is_admin 게이트로 차단된다.
    """

    permission_classes = [IsAuthenticated, DenyLabelers]

    def get(self, request, *args, **kwargs):
        import datetime

        from django.db.models import Avg, Count, Max, Min, Q, Sum
        from django.db.models.functions import TruncDate
        from django.utils import timezone
        from tasks.models import Task, TaskReviewHistory

        org = request.user.active_organization
        if not OrganizationMember.is_admin(request.user, org):
            raise PermissionDenied('Only admins can view the dashboard')

        def review_counts(qs):
            counts = {r['review_status']: r['c'] for r in qs.values('review_status').annotate(c=Count('id'))}
            return {choice.value: counts.get(choice.value, 0) for choice in Task.ReviewStatus}

        org_tasks = Task.objects.filter(project__organization=org)
        org_history = TaskReviewHistory.objects.filter(task__project__organization=org)

        projects = []
        # (project, review_status) 인덱스(0066)를 타는 프로젝트별 상태 집계 + 완료 수.
        for project in (
            Project.objects.filter(organization=org)
            .order_by('id')
            .only('id', 'title', 'is_submitted', 'is_mid_reviewed')
        ):
            project_tasks = org_tasks.filter(project=project)
            # 2단계 검수 단계: open → mid(최종 검수 대기) → final(완료)
            stage = 'final' if project.is_submitted else ('mid' if project.is_mid_reviewed else 'open')
            projects.append(
                {
                    'id': project.id,
                    'title': project.title,
                    'total': project_tasks.count(),
                    'labeled': project_tasks.filter(is_labeled=True).count(),
                    'stage': stage,
                    **review_counts(project_tasks),
                }
            )

        # ----- 작업자별 (Annotation.completed_by = 실제 라벨 수행자) -------------------- #
        org_annotations = Annotation.objects.filter(project__organization=org, was_cancelled=False).exclude(
            completed_by=None
        )
        workers = {
            w['completed_by__email']: {
                'email': w['completed_by__email'],
                'tasks': w['tasks'],
                'annotations': w['annotations'],
                'first_activity': w['first_activity'],
                'last_activity': w['last_activity'],
                'active_days': w['active_days'],
                'status': {choice.value: 0 for choice in Task.ReviewStatus},
                'rejected_events': 0,
                'rejected_tasks': 0,
                'rejected_first': 0,
                'rejected_final': 0,
                'reopened_events': 0,
                # lead_time(에디터 체류시간 합, 초) — 자리비움 포함 가능·재편집 누적: 정산 보조 지표
                'total_lead_time': round(w['total_lead_time'] or 0.0, 1),
                'avg_lead_time': round(w['avg_lead_time'] or 0.0, 1),
                'lead_time_coverage': w['lead_timed'],
                # active_seconds(순수 활성 작업시간 합, 초) — 60s+ idle 제외. 프론트 활동 기반.
                'total_active_seconds': round(w['total_active_seconds'] or 0.0, 1),
                'avg_active_seconds': round(w['avg_active_seconds'] or 0.0, 1),
                'active_coverage': w['active_timed'],
                'total_objects': w['total_objects'] or 0,
            }
            for w in org_annotations.values('completed_by__email').annotate(
                tasks=Count('task', distinct=True),
                annotations=Count('id'),
                first_activity=Min('created_at'),
                last_activity=Max('updated_at'),
                active_days=Count(TruncDate('created_at'), distinct=True),
                total_lead_time=Sum('lead_time'),
                avg_lead_time=Avg('lead_time'),
                lead_timed=Count('id', filter=Q(lead_time__isnull=False)),
                total_active_seconds=Sum('active_seconds'),
                avg_active_seconds=Avg('active_seconds'),
                active_timed=Count('id', filter=Q(active_seconds__isnull=False)),
                total_objects=Sum('result_count'),
            )
        }
        # 라벨한 태스크의 '현재' 검수 상태 분포 (품질 스냅샷)
        for row in org_annotations.values('completed_by__email', 'task__review_status').annotate(
            c=Count('task', distinct=True)
        ):
            status = row['task__review_status'] or Task.ReviewStatus.PENDING.value
            workers[row['completed_by__email']]['status'][status] = row['c']

        # ----- 반려 이벤트: 행위자 역할(1차=reviewer/최종=admin)별 분리 + 태스크 중복 제거 --- #
        role_map = dict(OrganizationMember.objects.filter(organization=org).values_list('user_id', 'role'))
        superuser_ids = set(
            User.objects.filter(is_superuser=True, id__in=set(role_map)).values_list('id', flat=True)
        )

        def actor_is_admin(uid):
            return uid == org.created_by_id or uid in superuser_ids or role_map.get(uid) == 'admin'

        # 태스크 → 작업자 이메일 매핑 (완료 주석 기준)
        task_workers = {}
        for task_id, email in org_annotations.values_list('task_id', 'completed_by__email').distinct():
            task_workers.setdefault(task_id, set()).add(email)

        reject_events = list(
            org_history.filter(action=TaskReviewHistory.Action.REJECTED).values_list(
                'id', 'task_id', 'actor_id', 'created_at', 'reason_code'
            )
        )
        # 반려 사유(Error Class) 분포 — 전체 + 작업자별. 코드 없는 과거 이력은 '미분류(unclassified)'.
        reason_totals = {}
        rejected_task_sets = {}
        for _hid, task_id, actor_id, _ts, reason_code in reject_events:
            reason = reason_code or 'unclassified'
            reason_totals[reason] = reason_totals.get(reason, 0) + 1
            for email in task_workers.get(task_id, ()):
                if email in workers:
                    workers[email]['rejected_events'] += 1
                    workers[email]['rejected_final' if actor_is_admin(actor_id) else 'rejected_first'] += 1
                    workers[email].setdefault('reasons', {})
                    workers[email]['reasons'][reason] = workers[email]['reasons'].get(reason, 0) + 1
                    rejected_task_sets.setdefault(email, set()).add(task_id)
        for email, task_set in rejected_task_sets.items():
            workers[email]['rejected_tasks'] = len(task_set)
        for row in (
            org_history.filter(action=TaskReviewHistory.Action.REOPENED, task__annotations__was_cancelled=False)
            .exclude(task__annotations__completed_by=None)
            .values('task__annotations__completed_by__email')
            .annotate(c=Count('id', distinct=True))
        ):
            email = row['task__annotations__completed_by__email']
            if email in workers:
                workers[email]['reopened_events'] = row['c']

        for w in workers.values():
            # 반려 태스크 비율(0~100%) = 반려된 적 있는 태스크(중복 제거)/라벨 태스크.
            # 평균 반려 횟수 = 누적 반려/반려된 태스크(재반려 반복 감지). 승인율 = 현재 최종 승인/라벨 태스크.
            w['rejected_task_rate'] = round(100.0 * w['rejected_tasks'] / w['tasks'], 1) if w['tasks'] else 0.0
            w['avg_rejects'] = round(w['rejected_events'] / w['rejected_tasks'], 1) if w['rejected_tasks'] else 0.0
            w['approval_rate'] = round(100.0 * w['status']['approved'] / w['tasks'], 1) if w['tasks'] else 0.0
            w['avg_per_day'] = round(w['annotations'] / w['active_days'], 1) if w['active_days'] else 0.0
            # 체류시간(lead_time) 기반 — 자리비움 포함(보조). 활성시간(active_seconds) 기반 — 순수 작업(주지표).
            w['objects_per_hour'] = (
                round(w['total_objects'] / w['total_lead_time'] * 3600.0, 1) if w['total_lead_time'] else 0.0
            )
            w['objects_per_active_hour'] = (
                round(w['total_objects'] / w['total_active_seconds'] * 3600.0, 1) if w['total_active_seconds'] else 0.0
            )
            # 태스크당 평균 활성 작업시간(초) — 정산/숙련도
            w['avg_active_per_task'] = round(w['total_active_seconds'] / w['tasks'], 1) if w['tasks'] else 0.0
        workers = sorted(workers.values(), key=lambda w: -w['annotations'])

        # ----- 검수자별 1차 검수 품질 (뒤집힘율: 1차 승인 → admin 최종 반려 역추적) ---------- #
        fa_events = sorted(
            org_history.filter(action=TaskReviewHistory.Action.FIRST_APPROVED).values_list(
                'task_id', 'actor_id', 'actor__email', 'created_at'
            ),
            key=lambda r: r[3],
        )
        fa_by_task = {}
        reviewers = {}
        for task_id, actor_id, actor_email, ts in fa_events:
            fa_by_task.setdefault(task_id, []).append((ts, actor_email))
            entry = reviewers.setdefault(
                actor_email, {'email': actor_email, 'first_approved': 0, 'overturned': 0, 'rejected_first': 0}
            )
            entry['first_approved'] += 1
        for _hid, task_id, actor_id, ts, _reason in reject_events:
            if actor_is_admin(actor_id):
                # 이 최종 반려 직전에 해당 태스크를 1차 통과시킨 검수자에게 귀속
                prior = [fa for fa in fa_by_task.get(task_id, []) if fa[0] <= ts]
                if prior:
                    reviewers[prior[-1][1]]['overturned'] += 1
            else:
                actor_email = User.objects.filter(id=actor_id).values_list('email', flat=True).first()
                if actor_email:
                    entry = reviewers.setdefault(
                        actor_email,
                        {'email': actor_email, 'first_approved': 0, 'overturned': 0, 'rejected_first': 0},
                    )
                    entry['rejected_first'] += 1
        for r in reviewers.values():
            r['overturn_rate'] = round(100.0 * r['overturned'] / r['first_approved'], 1) if r['first_approved'] else 0.0
        reviewers = sorted(reviewers.values(), key=lambda r: -r['first_approved'])

        # ----- 최근 14일 일별 추이 (제출 vs 검수 이벤트) ------------------------------- #
        today = timezone.localdate()
        since = today - datetime.timedelta(days=13)
        daily = {
            (since + datetime.timedelta(days=i)).isoformat(): {
                'date': (since + datetime.timedelta(days=i)).isoformat(),
                'annotations': 0,
                'first_approved': 0,
                'approved': 0,
                'rejected': 0,
            }
            for i in range(14)
        }
        for row in (
            org_annotations.filter(created_at__date__gte=since)
            .annotate(d=TruncDate('created_at'))
            .values('d')
            .annotate(c=Count('id'))
        ):
            key = row['d'].isoformat()
            if key in daily:
                daily[key]['annotations'] = row['c']
        for row in (
            org_history.filter(
                created_at__date__gte=since,
                action__in=[
                    TaskReviewHistory.Action.FIRST_APPROVED,
                    TaskReviewHistory.Action.APPROVED,
                    TaskReviewHistory.Action.REJECTED,
                ],
            )
            .annotate(d=TruncDate('created_at'))
            .values('d', 'action')
            .annotate(c=Count('id'))
        ):
            key = row['d'].isoformat()
            if key in daily:
                daily[key][row['action']] = row['c']

        return Response(
            {
                'generated': timezone.now(),
                'summary': {
                    'projects': len(projects),
                    'tasks': sum(p['total'] for p in projects),
                    'labeled': sum(p['labeled'] for p in projects),
                    'reopened': org_history.filter(action=TaskReviewHistory.Action.REOPENED).count(),
                    'review': review_counts(org_tasks),
                },
                'projects': projects,
                'workers': workers,
                'reviewers': reviewers,
                'reject_reasons': {
                    'labels': {
                        **dict(TaskReviewHistory.RejectReason.choices),
                        'unclassified': '미분류(도입 전 이력)',
                    },
                    'totals': reason_totals,
                },
                'daily': list(daily.values()),
            },
            status=200,
        )
