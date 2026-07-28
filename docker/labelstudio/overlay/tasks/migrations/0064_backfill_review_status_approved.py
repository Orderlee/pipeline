"""Non-destructive backfill for the task review workflow (GCP outsourced LS fork).

Existing tasks predate ``Task.review_status`` (added in 0062 with default
``'pending'``). Treating already-labeled pre-existing work as an unreviewed
backlog would flood reviewers with a false review queue on the shared
production instance (e.g. the colleague's 922 labeled tasks).

Forward: mark every already-labeled task (``is_labeled=True``) as
``'approved'`` — i.e. pre-existing labeled work is considered accepted, so it
does not appear as pending review. Unlabeled tasks stay ``'pending'`` (they are
simply not reviewed yet). No rows are deleted or otherwise mutated.

This only touches tasks whose status is still the ``'pending'`` default, so it
is safe to re-run and does not clobber any explicit review decisions.
"""
from django.db import migrations


def backfill_labeled_as_approved(apps, schema_editor):
    Task = apps.get_model('tasks', 'task')
    Task.objects.filter(is_labeled=True, review_status='pending').update(review_status='approved')


def reverse_noop(apps, schema_editor):
    # Irreversible in a meaningful way (we cannot tell which 'approved' rows were
    # backfilled vs explicitly approved). Leave data as-is on reverse.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('tasks', '0063_task_review_history'),
    ]

    operations = [
        migrations.RunPython(backfill_labeled_as_approved, reverse_noop),
    ]
