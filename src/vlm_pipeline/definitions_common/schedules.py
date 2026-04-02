"""공통 Dagster schedule 빌더."""

from __future__ import annotations

from dagster import ScheduleDefinition

from vlm_pipeline.defs.gcp.assets import DEFAULT_GCP_BUCKETS
from vlm_pipeline.lib.motherduck_sync_policy import COLD_MOTHERDUCK_SYNC_TABLES


def build_gcs_download_schedule(job) -> ScheduleDefinition:
    return ScheduleDefinition(
        name="gcs_download_schedule",
        job=job,
        cron_schedule="0 4 * * *",
        execution_timezone="Asia/Seoul",
        run_config={
            "ops": {
                "pipeline__incoming_nas": {
                    "config": {
                        "mode": "date-folders",
                        "download_dir": "/nas/incoming/gcp",
                        "backend": "gcloud",
                        "skip_existing": True,
                        "dry_run": False,
                        "buckets": DEFAULT_GCP_BUCKETS,
                        "bucket_subdir": True,
                    }
                }
            }
        },
    )


def build_motherduck_daily_schedule(job) -> ScheduleDefinition:
    return ScheduleDefinition(
        name="motherduck_daily_schedule",
        job=job,
        cron_schedule="0 5 * * *",
        execution_timezone="Asia/Seoul",
        run_config={
            "ops": {
                "pipeline__motherduck_sync": {
                    "config": {
                        "enabled": True,
                        "tables": list(COLD_MOTHERDUCK_SYNC_TABLES),
                        "attach_mode": "single",
                        "analyze_local": True,
                        "checkpoint_local": True,
                        "freshness_mode": "auto",
                    }
                }
            }
        },
    )
