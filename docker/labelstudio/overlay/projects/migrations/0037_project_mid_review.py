# GCP 외주 포크: 프로젝트 2단계 검수 — 중간 검수 완료(is_mid_reviewed/mid_reviewed_at) + MID_SUBMITTED 이력.
# 추가형·비파괴(AddField null/default, action choices 확장은 max_length 내라 DB 무변경).
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('projects', '0036_labelstatehistory_note'),
    ]

    operations = [
        migrations.AddField(
            model_name='project',
            name='is_mid_reviewed',
            field=models.BooleanField(
                default=False, help_text='중간 검수 완료(최종 검수 대기). 1차 검수자 완료 표시.', verbose_name='is mid reviewed'
            ),
        ),
        migrations.AddField(
            model_name='project',
            name='mid_reviewed_at',
            field=models.DateTimeField(
                default=None, help_text='중간 검수 완료 시각.', null=True, verbose_name='mid reviewed at'
            ),
        ),
        migrations.AlterField(
            model_name='labelstatehistory',
            name='action',
            field=models.CharField(
                choices=[
                    ('submitted', 'Submitted'),
                    ('resubmitted', 'Resubmitted'),
                    ('rejected', 'Rejected'),
                    ('mid_submitted', 'Mid submitted'),
                ],
                help_text='Transition action',
                max_length=32,
            ),
        ),
    ]
