"""clip_timestamp 구현 — MVP / routed (spec·dispatch) 분기.

assets.py의 @asset clip_timestamp 래퍼에서 호출됩니다.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

from vlm_pipeline.lib.env_utils import (
    dispatch_folder_for_source_unit,
    dispatch_raw_key_prefix_folder,
    is_dispatch_yolo_only_requested,
    requested_outputs_require_timestamp,
    should_run_output,
)
from vlm_pipeline.lib.gemini import extract_clean_json_text
from vlm_pipeline.lib.gemini_prompts import VIDEO_EVENT_PROMPT
from vlm_pipeline.lib.spec_config import (
    is_standard_spec_run,
    parse_requested_outputs,
    resolve_and_persist_spec_config,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .label_helpers import (
    analyze_routed_video_events,
    build_gemini_label_key,
    cleanup_temp,
    clone_gemini_analyzer,
    init_gemini_analyzer,
    int_env,
    materialize_video,
    prepare_gemini_video_for_request,
    serialize_gemini_events,
)


def _build_preview_metadata(
    *,
    video_path: Path,
    gemini_video_path: Path,
    duration_val: float | int | None,
) -> dict[str, Any]:
    max_dur = int_env("GEMINI_MAX_DURATION_SEC", 3600, minimum=60)
    orig_dur = float(duration_val or 0.0)
    return {
        "original_bytes": int(video_path.stat().st_size),
        "preview_bytes": int(gemini_video_path.stat().st_size),
        "original_duration": orig_dur,
        "trimmed": bool(orig_dur > max_dur),
        "max_duration": max_dur,
    }


def _process_mvp_candidate(
    *,
    minio: MinIOResource,
    analyzer,
    video_prompt: str,
    cand: dict[str, Any],
) -> dict[str, Any]:
    asset_id = cand["asset_id"]
    raw_key = str(cand.get("raw_key") or "")
    duration_val = cand.get("duration_sec")
    temp_paths: list[Path] = []

    try:
        worker_analyzer = clone_gemini_analyzer(analyzer)
        video_path, temp_path = materialize_video(minio, cand)
        if temp_path is not None:
            temp_paths.append(temp_path)
        gemini_video_path, gemini_temp_path = prepare_gemini_video_for_request(
            video_path,
            duration_sec=duration_val,
        )
        if gemini_temp_path is not None:
            temp_paths.append(gemini_temp_path)
        label_key = build_gemini_label_key(raw_key)
        response_text = worker_analyzer.analyze_video(
            str(gemini_video_path),
            prompt=video_prompt,
            mime_type="video/mp4" if gemini_temp_path is not None else None,
        )
        label_bytes = extract_clean_json_text(response_text).encode("utf-8")
        minio.ensure_bucket("vlm-labels")
        minio.upload("vlm-labels", label_key, label_bytes, "application/json")
        return {
            "asset_id": asset_id,
            "label_key": label_key,
            "preview": (
                _build_preview_metadata(
                    video_path=video_path,
                    gemini_video_path=gemini_video_path,
                    duration_val=duration_val,
                )
                if gemini_temp_path is not None
                else None
            ),
        }
    except Exception as exc:  # noqa: BLE001
        return {"asset_id": asset_id, "error": exc}
    finally:
        for path in reversed(temp_paths):
            cleanup_temp(path)


def _process_routed_candidate(
    *,
    context,
    minio: MinIOResource,
    analyzer,
    video_prompt: str,
    cand: dict[str, Any],
) -> dict[str, Any]:
    asset_id = cand["asset_id"]
    raw_key = str(cand.get("raw_key") or "")
    temp_paths: list[Path] = []

    try:
        worker_analyzer = clone_gemini_analyzer(analyzer)
        video_path, temp_path = materialize_video(minio, cand)
        if temp_path is not None:
            temp_paths.append(temp_path)
        events = analyze_routed_video_events(
            context,
            worker_analyzer,
            video_path,
            duration_sec=cand.get("duration_sec"),
            temp_paths=temp_paths,
            video_prompt=video_prompt,
        )
        label_key = build_gemini_label_key(raw_key)
        minio.ensure_bucket("vlm-labels")
        minio.upload(
            "vlm-labels",
            label_key,
            serialize_gemini_events(events),
            "application/json",
        )
        return {"asset_id": asset_id, "label_key": label_key}
    except Exception as exc:  # noqa: BLE001
        return {"asset_id": asset_id, "error": exc}
    finally:
        for path in reversed(temp_paths):
            cleanup_temp(path)


def clip_timestamp_mvp(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """completed video 중 auto_label_status='pending'인 건에 Gemini 호출 (MVP job)."""
    if not should_run_output(context, "timestamp"):
        context.log.info("clip_timestamp 스킵: outputs에 timestamp가 없습니다.")
        return {"processed": 0, "skipped": True}

    db.ensure_runtime_schema()
    folder_name = dispatch_raw_key_prefix_folder(context.run.tags if context.run else None)
    limit = int(context.op_config.get("limit", 50))
    candidates = db.find_auto_label_pending_videos(limit=limit, folder_name=folder_name)
    if not candidates:
        context.log.info("AUTO LABEL 대상 없음")
        return {"processed": 0, "failed": 0}

    analyzer = init_gemini_analyzer(context)
    video_prompt = VIDEO_EVENT_PROMPT
    total_candidates = len(candidates)
    max_workers = min(total_candidates, int_env("GEMINI_MAX_WORKERS", 5, minimum=1))
    context.log.info(f"clip_timestamp 시작: 총 {total_candidates}건 처리 예정")
    processed = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                _process_mvp_candidate,
                minio=minio,
                analyzer=analyzer,
                video_prompt=video_prompt,
                cand=cand,
            ): (idx, cand["asset_id"])
            for idx, cand in enumerate(candidates, start=1)
        }
        for future in as_completed(future_map):
            idx, asset_id = future_map[future]
            result = future.result()
            error = result.get("error")
            if error is None:
                label_key = str(result["label_key"])
                preview_meta = result.get("preview")
                db.update_auto_label_status(
                    asset_id,
                    "generated",
                    label_key=label_key,
                    labeled_at=datetime.now(),
                )
                processed += 1
                if isinstance(preview_meta, dict):
                    context.log.info(
                        "AUTO LABEL preview 사용: asset_id=%s original_bytes=%s preview_bytes=%s "
                        "original_duration=%.0fs trimmed=%s max_duration=%ds",
                        asset_id,
                        preview_meta["original_bytes"],
                        preview_meta["preview_bytes"],
                        float(preview_meta["original_duration"]),
                        bool(preview_meta["trimmed"]),
                        int(preview_meta["max_duration"]),
                    )
                context.log.info(
                    f"clip_timestamp 진행: [{idx}/{total_candidates}] "
                    f"({idx * 100 // total_candidates}%) "
                    f"asset={asset_id} label_key={label_key} ✅"
                )
                continue

            failed += 1
            db.update_auto_label_status(
                asset_id,
                "failed",
                error=str(error)[:500],
            )
            context.log.info(
                f"clip_timestamp 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} ❌ {error}"
            )

    summary = {"processed": processed, "failed": failed}
    context.add_output_metadata(summary)
    context.log.info(f"AUTO LABEL 완료: {summary}")
    return summary


def clip_timestamp_routed_impl(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """spec/dispatch: requested_outputs·spec_id 기준 timestamp 단계."""
    tags = context.run.tags if context.run else {}
    video_prompt = VIDEO_EVENT_PROMPT
    spec_id = tags.get("spec_id")
    requested = parse_requested_outputs(tags)
    standard_spec_run = is_standard_spec_run(tags)
    if is_dispatch_yolo_only_requested(tags):
        context.log.info("clip_timestamp 스킵: dispatch labeling_method가 YOLO 전용입니다.")
        return {"processed": 0, "failed": 0, "skipped": True}
    if not requested_outputs_require_timestamp(requested) and not standard_spec_run:
        context.log.info("clip_timestamp 스킵: outputs에 timestamp 없음")
        return {"processed": 0, "failed": 0, "skipped": True}

    db.ensure_runtime_schema()
    resolved_config_id = None
    if spec_id:
        config_bundle = resolve_and_persist_spec_config(db, spec_id)
        resolved_config_id = config_bundle["resolved_config_id"]
        context.log.info(
            "clip_timestamp: spec_id=%s resolved_config_id=%s scope=%s",
            spec_id,
            resolved_config_id,
            config_bundle["resolved_config_scope"],
        )

    limit = int(context.op_config.get("limit", 50))
    folder_name = dispatch_folder_for_source_unit(tags)
    if spec_id:
        candidates = db.find_ready_for_labeling_timestamp_backlog(spec_id, limit=limit)
    elif folder_name:
        candidates = db.find_timestamp_pending_by_folder(folder_name, limit=limit)
    else:
        candidates = []
    if not candidates:
        context.log.info("clip_timestamp: 대상 없음")
        return {"processed": 0, "failed": 0}

    analyzer = init_gemini_analyzer(context)
    processed = 0
    failed = 0
    max_workers = min(len(candidates), int_env("GEMINI_MAX_WORKERS", 5, minimum=1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                _process_routed_candidate,
                context=context,
                minio=minio,
                analyzer=analyzer,
                video_prompt=video_prompt,
                cand=cand,
            ): (idx, cand["asset_id"])
            for idx, cand in enumerate(candidates, start=1)
        }
        for future in as_completed(future_map):
            idx, asset_id = future_map[future]
            result = future.result()
            error = result.get("error")
            if error is None:
                label_key = str(result["label_key"])
                db.update_timestamp_status(
                    asset_id,
                    "completed",
                    label_key=label_key,
                    completed_at=datetime.now(),
                )
                processed += 1
                context.log.info(
                    "clip_timestamp 완료: [%d/%d] asset_id=%s label_key=%s ✅",
                    idx,
                    len(candidates),
                    asset_id,
                    label_key,
                )
                continue

            failed += 1
            db.update_timestamp_status(
                asset_id, "failed", error=str(error)[:500], completed_at=datetime.now()
            )
            context.log.error(
                "clip_timestamp 실패: [%d/%d] asset_id=%s: %s",
                idx,
                len(candidates),
                asset_id,
                error,
            )
    return {
        "processed": processed,
        "failed": failed,
        "resolved_config_id": resolved_config_id,
    }
