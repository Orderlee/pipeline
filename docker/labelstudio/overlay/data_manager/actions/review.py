"""DataManager bulk review actions (GCP outsourced LS fork).

Reviewer/admin bulk **approve/reject** of labeler tasks, mirroring the single
``POST /api/tasks/<pk>/review`` endpoint (``tasks.api.TaskReviewAPI``) so the
review workflow is usable straight from the Data Manager grid without any
frontend build.

Isolation: labelers are already blocked from the actions endpoint by the
default-deny middleware (``data_manager:api:dm-actions`` is not in
``LABELER_ALLOWED``). We additionally refuse here as defense in depth, since the
OSS ``user.has_perm`` used by the action permission check is effectively a dummy
``True`` and does not distinguish roles on its own.
"""
import logging

from core.permissions import AllPermissions
from data_manager.actions import DataManagerAction
from django.utils import timezone
from organizations.models import OrganizationMember
from rest_framework.exceptions import PermissionDenied, ValidationError
from tasks.models import Task, TaskReviewHistory

all_permissions = AllPermissions()
logger = logging.getLogger(__name__)


def _guard(user, project):
    """Refuse review actions for labelers, and (defense in depth) for cross-org
    projects — the single-org design makes the latter unreachable today, but we
    do not want the bulk path to be the weaker sibling of the single-task API."""
    organization = getattr(user, 'active_organization', None)
    if organization is not None and OrganizationMember.is_labeler(user, organization):
        raise PermissionDenied('Review actions are not allowed for labelers.')
    if organization is not None and project.organization_id != organization.id:
        raise PermissionDenied('Project is outside the current organization.')


def approve_tasks(project, queryset, **kwargs):
    """2단계 승인 — 요청자 역할에 따라 목표 상태가 다름:
    reviewer → first_approved(1차 통과), admin → approved(최종 완료). (단일 review API와 동일 규칙)"""
    user = kwargs['request'].user
    _guard(user, project)

    is_final = OrganizationMember.is_admin(user, project.organization)
    status = Task.ReviewStatus.APPROVED if is_final else Task.ReviewStatus.FIRST_APPROVED
    action = TaskReviewHistory.Action.APPROVED if is_final else TaskReviewHistory.Action.FIRST_APPROVED

    total = queryset.count()
    # 승인 대상은 라벨(제출)된 태스크만. reviewer 는 이미 최종 승인(approved)된 태스크를
    # 강등시키지 않도록 제외(데이터 보호). admin 최종 승인은 approved 재확정 허용(멱등).
    eligible = queryset.filter(is_labeled=True)
    if not is_final:
        eligible = eligible.exclude(review_status=Task.ReviewStatus.APPROVED)
    task_ids = list(eligible.values_list('id', flat=True))
    now = timezone.now()
    # clear any stale rejection reason from a prior reject cycle
    Task.objects.filter(id__in=task_ids).update(
        review_status=status, review_note='', reviewed_by=user, reviewed_at=now
    )
    TaskReviewHistory.objects.bulk_create(
        [TaskReviewHistory(task_id=tid, actor=user, action=action, note='') for tid in task_ids]
    )
    label = 'Final approval' if is_final else 'First-pass approval'
    skipped = total - len(task_ids)
    detail = f'{label}: {len(task_ids)} task(s)' + (
        f' ({skipped} skipped: not labeled / already final-approved)' if skipped else ''
    )
    return {'processed_items': len(task_ids), 'detail': detail}


def reject_tasks(project, queryset, **kwargs):
    """Send the selected tasks back to their labeler for rework, with a reason."""
    user = kwargs['request'].user
    _guard(user, project)

    note = (kwargs['request'].data.get('note') or '').strip()
    if not note:
        raise ValidationError('A rejection note is required.')
    # 사유 코드(Error Class, 선택) — 대시보드 사유별 통계용. 미지정은 null=미분류.
    reason_code = kwargs['request'].data.get('reason') or None
    if reason_code is not None and reason_code not in TaskReviewHistory.RejectReason.values:
        raise ValidationError(f'invalid reason; must be one of {list(TaskReviewHistory.RejectReason.values)}')

    task_ids = list(queryset.values_list('id', flat=True))
    now = timezone.now()
    Task.objects.filter(id__in=task_ids).update(
        review_status=Task.ReviewStatus.REJECTED, is_labeled=False, review_note=note, reviewed_by=user, reviewed_at=now
    )
    TaskReviewHistory.objects.bulk_create(
        [
            TaskReviewHistory(
                task_id=tid, actor=user, action=TaskReviewHistory.Action.REJECTED, note=note, reason_code=reason_code
            )
            for tid in task_ids
        ]
    )
    return {'processed_items': len(task_ids), 'detail': f'Rejected {len(task_ids)} task(s)'}


def reject_tasks_form(user, project):
    return [
        {
            'columnCount': 1,
            'fields': [
                {
                    'type': 'select',
                    'name': 'reason',
                    'label': 'Reason category 사유 분류 (통계용)',
                    'options': [
                        {'value': code, 'label': label} for code, label in TaskReviewHistory.RejectReason.choices
                    ],
                },
                {
                    'type': 'textarea',
                    'name': 'note',
                    'label': 'Rejection note (shown to the labeler)',
                    'placeholder': 'Describe what needs to be fixed',
                },
            ],
        }
    ]


actions: list[DataManagerAction] = [
    {
        'entry_point': approve_tasks,
        'permission': all_permissions.tasks_change,
        'title': 'Approve (Review)',
        'order': 110,
        'dialog': {
            'title': 'Approve selected tasks',
            'text': 'Mark the selected tasks as reviewed & approved. Please confirm.',
            'type': 'confirm',
        },
    },
    {
        'entry_point': reject_tasks,
        'permission': all_permissions.tasks_change,
        'title': 'Reject (Review)',
        'order': 111,
        'dialog': {
            'title': 'Reject selected tasks',
            'text': 'Send the selected tasks back to the labeler for rework with a reason.',
            'type': 'confirm',
            'form': reject_tasks_form,
        },
    },
]
