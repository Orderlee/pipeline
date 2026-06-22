"""env_backfill 순수 헬퍼 — dagster 의존 없음.

Layer 3: lib + resources 만 import 가능. dagster import 금지.
"""

from __future__ import annotations

import os
from typing import Any, Callable

from vlm_pipeline.lib.video_env import classify_video_environment

OUTCOME_SKIP = "skip"
OUTCOME_DONE = "done"
OUTCOME_FAILED = "failed"


def process_one_video(
    video: dict[str, Any],
    *,
    classify_fn: Callable[[str, float], dict[str, Any]] = classify_video_environment,
    path_exists_fn: Callable[[str], bool] = os.path.exists,
) -> tuple[str, dict[str, Any] | None, str | None]:
    """단일 비디오에 대한 환경 분류 결과를 반환.

    Returns:
        (outcome, env_result, error_msg)
        outcome: OUTCOME_SKIP / OUTCOME_DONE / OUTCOME_FAILED
        env_result: classify 결과 dict or terminal marker dict (OUTCOME_SKIP 시 terminal marker, OUTCOME_DONE 시 full result)
        error_msg: 실패/스킵 사유 (OUTCOME_SKIP/OUTCOME_FAILED 시에만 non-None)

    Terminal markers (OUTCOME_SKIP with non-None env_result):
        - archive_path 없음: {"env_method": "deferred_missing_archive"}
        - 프레임 추출 불가: {"env_method": "deferred_no_frames"}
    These rows must be written to DB so they leave the 'deferred' selection bucket.

    Exceptions (OUTCOME_FAILED) leave the row as 'deferred' for legitimate retry.
    """
    archive_path = video.get("archive_path", "")
    duration_sec = float(video.get("duration_sec") or 0.0)

    if not path_exists_fn(archive_path):
        return (
            OUTCOME_SKIP,
            {"env_method": "deferred_missing_archive"},
            f"archive_path 없음: {archive_path}",
        )

    try:
        result = classify_fn(archive_path, duration_sec)
    except Exception as exc:
        return OUTCOME_FAILED, None, str(exc)

    if result.get("env_method") is None:
        return (
            OUTCOME_SKIP,
            {"env_method": "deferred_no_frames"},
            "프레임 추출 불가",
        )

    return OUTCOME_DONE, result, None
