"""Places365 환경 분류 백필 job.

ingest 당시 env_method='deferred' 로 처리된 비디오에 대해
classify_video_environment 를 재실행하여 video_metadata 를 채운다.

Layer 3/4: @op + @job 정의.
"""

from __future__ import annotations

import os

from dagster import Field, job, op

from vlm_pipeline.defs.ingest.env_backfill_helpers import (
    OUTCOME_FAILED,
    OUTCOME_SKIP,
    process_one_video,
)
from vlm_pipeline.resources.postgres import PostgresResource

_ARCHIVE_MOUNT_PROBE = "/nas/data/archive"


@op(
    name="video_env_backfill",
    description="Places365 환경 분류 백필 — env_method='deferred' 비디오를 배치 처리",
    config_schema={
        "limit": Field(int, default_value=1000, description="처리할 최대 비디오 수"),
    },
)
def video_env_backfill(context, db: PostgresResource) -> dict:
    # FIX 3: archive mount preflight — abort early to avoid mass-mis-marking during NFS outage
    if not os.path.isdir(_ARCHIVE_MOUNT_PROBE):
        raise RuntimeError(
            f"archive mount not available ({_ARCHIVE_MOUNT_PROBE}): "
            "aborting env_backfill to avoid mis-marking rows as deferred_missing_archive"
        )

    # FIX 4: Places365 availability — log only, heuristic is a valid fallback
    try:
        from vlm_pipeline.lib.video_env import _load_places365_runtime

        rt = _load_places365_runtime()
        if rt is None:
            context.log.warning(
                "Places365 runtime unavailable — this run will use heuristic fallback "
                "(env_method='heuristic'). Check torch/model availability if Places365 is expected."
            )
    except Exception as _exc:
        context.log.warning(f"Places365 availability check failed ({_exc}); continuing with heuristic fallback")

    limit = int(context.op_config.get("limit", 1000))
    candidates = db.find_deferred_env_videos(limit=limit)

    total = len(candidates)
    context.log.info(f"env_backfill 시작: 대상 {total}건 (limit={limit})")

    done = 0
    failed = 0
    skipped = 0
    skipped_terminal = 0

    for video in candidates:
        asset_id = video["asset_id"]

        outcome, env_result, err_msg = process_one_video(video)

        if outcome == OUTCOME_SKIP:
            context.log.warning(f"skip: asset_id={asset_id} — {err_msg}")
            skipped += 1
            # FIX 2: write terminal marker so the row leaves 'deferred' selection
            if env_result is not None:
                try:
                    db.update_video_env(
                        asset_id,
                        environment_type=None,
                        daynight_type=None,
                        outdoor_score=None,
                        avg_brightness=None,
                        env_method=env_result["env_method"],
                    )
                    skipped_terminal += 1
                except Exception as exc:
                    context.log.error(f"terminal marker update 실패: asset_id={asset_id} — {exc}")
            continue

        if outcome == OUTCOME_FAILED:
            context.log.error(f"classify 실패: asset_id={asset_id} — {err_msg}")
            failed += 1
            continue

        assert env_result is not None
        try:
            db.update_video_env(
                asset_id,
                environment_type=env_result["environment_type"],
                daynight_type=env_result["daynight_type"],
                outdoor_score=env_result["outdoor_score"],
                avg_brightness=env_result["avg_brightness"],
                env_method=env_result["env_method"],
            )
            done += 1
        except Exception as exc:
            context.log.error(f"update 실패: asset_id={asset_id} — {exc}")
            failed += 1

    context.log.info(
        f"env_backfill 완료: done={done} failed={failed} "
        f"skipped={skipped} (terminal={skipped_terminal}) / total={total}"
    )
    return {"done": done, "failed": failed, "skipped": skipped, "skipped_terminal": skipped_terminal, "total": total}


@job(
    name="video_env_backfill_job",
    description="[백필] deferred 비디오에 Places365 환경 분류 재실행 (평일 19:00 KST 스케줄)",
)
def video_env_backfill_job():
    video_env_backfill()
