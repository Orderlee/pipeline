# GCP 외주 포크: 검수 큐 필터 성능 — (project, review_status) 복합 인덱스.
# 비파괴·additive (CREATE INDEX). 롤백 시 인덱스만 드롭.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('tasks', '0065_review_first_approved'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='task',
            index=models.Index(fields=['project', 'review_status'], name='task_proj_review_idx'),
        ),
    ]
