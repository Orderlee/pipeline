"""clip_captioning — MVP 및 spec/dispatch 라우팅 구현."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from vlm_pipeline.lib.env_utils import (
    dispatch_folder_for_source_unit,
    dispatch_raw_key_prefix_folder,
    is_dispatch_yolo_only_requested,
    requested_outputs_require_caption_labels,
    should_run_output,
)
from vlm_pipeline.lib.gemini import load_clean_json
from vlm_pipeline.lib.spec_config import (
    is_standard_spec_run,
    load_persisted_spec_config,
    parse_requested_outputs,
)
from vlm_pipeline.lib.staging_vertex import normalize_gemini_events
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .helpers import (
    _build_gemini_label_rows,
    _filter_valid_events,
    _stable_gemini_label_id,
)


def clip_captioning_mvp(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """auto_label_status='generated'인 video의 Gemini JSON을 labels에 upsert (MVP)."""
    if not should_run_output(context, "captioning"):
        context.log.info("clip_captioning 스킵: outputs에 captioning이 없습니다.")
        return {"processed": 0, "failed": 0, "labels_inserted": 0, "skipped": True}

    db.ensure_runtime_schema()
    folder_name = dispatch_raw_key_prefix_folder(context.run.tags if context.run else None)
    limit = int(context.op_config.get("limit", 200))
    candidates = db.find_captioning_pending_videos(limit=limit, folder_name=folder_name)
    if not candidates:
        context.log.info("CAPTIONING 대상 없음")
        return {"processed": 0, "failed": 0, "labels_inserted": 0}

    total_candidates = len(candidates)
    context.log.info(f"clip_captioning 시작: 총 {total_candidates}건 처리 예정")
    processed = 0
    failed = 0
    labels_inserted = 0

    for idx, cand in enumerate(candidates, start=1):
        asset_id = cand["asset_id"]
        auto_label_key = str(cand.get("auto_label_key") or "")

        if not auto_label_key:
            context.log.warning(f"auto_label_key 없음: asset_id={asset_id}")
            db.update_auto_label_status(
                asset_id,
                "failed",
                error="captioning_missing_auto_label_key",
            )
            failed += 1
            continue

        try:
            json_bytes = minio.download("vlm-labels", auto_label_key)
            raw_text = json_bytes.decode("utf-8", errors="replace")
            events = load_clean_json(raw_text)

            if not isinstance(events, list):
                events = [events] if isinstance(events, dict) else []

            valid_events = _filter_valid_events(events)
            event_count = len(valid_events)
            label_rows: list[dict[str, Any]] = []

            for event_index, event in enumerate(valid_events):
                ts = event.get("timestamp") or []
                start_sec = float(ts[0]) if len(ts) > 0 else None
                end_sec = float(ts[1]) if len(ts) > 1 else None
                if start_sec is not None and end_sec is not None and end_sec <= start_sec:
                    continue

                ko_caption = str(event.get("ko_caption") or "").strip()
                en_caption = str(event.get("en_caption") or "").strip()
                caption_text = ko_caption or en_caption or None

                label_id = _stable_gemini_label_id(asset_id, event_index, start_sec, end_sec)
                label_rows.append(
                    {
                        "label_id": label_id,
                        "asset_id": asset_id,
                        "labels_bucket": "vlm-labels",
                        "labels_key": auto_label_key,
                        "label_format": "gemini_event_json",
                        "label_tool": "gemini",
                        "label_source": "auto",
                        "review_status": "auto_generated",
                        "event_index": event_index,
                        "event_count": event_count,
                        "timestamp_start_sec": start_sec,
                        "timestamp_end_sec": end_sec,
                        "caption_text": caption_text,
                        "object_count": 0,
                        "label_status": "completed",
                    }
                )

            inserted = db.replace_gemini_labels(asset_id, auto_label_key, label_rows)
            labels_inserted += inserted
            db.update_auto_label_status(
                asset_id,
                "completed",
                label_key=auto_label_key,
                labeled_at=datetime.now(),
            )
            processed += 1
            context.log.info(
                f"clip_captioning 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} events={event_count} labels={inserted} ✅"
            )

        except json.JSONDecodeError as exc:
            failed += 1
            db.update_auto_label_status(asset_id, "failed", error=f"json_parse_error: {exc}")
            context.log.error(
                f"clip_captioning 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} ❌ JSON 파싱 실패: {exc}"
            )
        except Exception as exc:
            failed += 1
            context.log.error(
                f"clip_captioning 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} ❌ {exc}"
            )

    summary = {"processed": processed, "failed": failed, "labels_inserted": labels_inserted}
    context.add_output_metadata(summary)
    context.log.info(f"CAPTIONING 완료: {summary}")
    return summary


def clip_captioning_routed_impl(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """spec/dispatch: requested_outputs·spec_id 기준 captioning 단계."""
    tags = context.run.tags if context.run else {}
    requested = parse_requested_outputs(tags)
    spec_id = str(tags.get("spec_id") or "").strip()
    standard_spec_run = is_standard_spec_run(tags)
    if is_dispatch_yolo_only_requested(tags):
        context.log.info("clip_captioning 스킵: dispatch labeling_method가 YOLO 전용입니다.")
        return {"processed": 0, "failed": 0, "labels_inserted": 0, "skipped": True}
    if not requested_outputs_require_caption_labels(requested) and not standard_spec_run:
        context.log.info("clip_captioning 스킵: outputs에 captioning 없음")
        return {"processed": 0, "failed": 0, "labels_inserted": 0, "skipped": True}

    db.ensure_runtime_schema()
    resolved_config_id = None
    if spec_id:
        config_bundle = load_persisted_spec_config(db, spec_id)
        resolved_config_id = config_bundle["resolved_config_id"]
        captioning_config = config_bundle["config_json"].get("captioning", {})
        context.log.info(
            "clip_captioning: spec_id=%s resolved_config_id=%s captioning_keys=%s",
            spec_id,
            resolved_config_id,
            sorted(captioning_config.keys()),
        )

    limit = int(context.op_config.get("limit", 200))
    folder_name = dispatch_folder_for_source_unit(tags)
    if spec_id:
        candidates = db.find_ready_for_labeling_caption_backlog(spec_id, limit=limit)
    elif folder_name:
        candidates = db.find_caption_pending_by_folder(folder_name, limit=limit)
    else:
        candidates = []
    if not candidates:
        context.log.info("clip_captioning: 대상 없음")
        return {
            "processed": 0,
            "failed": 0,
            "labels_inserted": 0,
            "resolved_config_id": resolved_config_id,
        }

    processed = 0
    failed = 0
    labels_inserted = 0
    total_candidates = len(candidates)
    for idx, cand in enumerate(candidates, start=1):
        asset_id = cand["asset_id"]
        label_key = str(cand.get("timestamp_label_key") or "").strip()
        if not label_key:
            failed += 1
            db.update_caption_status(
                asset_id,
                "failed",
                error="captioning_missing_timestamp_label_key",
                completed_at=datetime.now(),
            )
            continue

        try:
            json_bytes = minio.download("vlm-labels", label_key)
            events = normalize_gemini_events(
                load_clean_json(json_bytes.decode("utf-8", errors="replace"))
            )
            label_rows = _build_gemini_label_rows(asset_id, label_key, events)
            inserted = db.replace_gemini_labels(asset_id, label_key, label_rows)
            labels_inserted += inserted
            db.update_caption_status(asset_id, "completed", completed_at=datetime.now())
            processed += 1
            context.log.info(
                "clip_captioning 진행: [%d/%d] asset=%s events=%d labels=%d ✅",
                idx,
                total_candidates,
                asset_id,
                len(events),
                inserted,
            )
        except json.JSONDecodeError as exc:
            failed += 1
            db.update_caption_status(
                asset_id,
                "failed",
                error=f"json_parse_error: {exc}",
                completed_at=datetime.now(),
            )
            context.log.error(
                "clip_captioning 진행: [%d/%d] asset=%s ❌ JSON 파싱 실패: %s",
                idx,
                total_candidates,
                asset_id,
                exc,
            )
        except Exception as exc:
            failed += 1
            db.update_caption_status(
                asset_id,
                "failed",
                error=str(exc)[:500],
                completed_at=datetime.now(),
            )
            context.log.error(
                "clip_captioning 진행: [%d/%d] asset=%s ❌ %s",
                idx,
                total_candidates,
                asset_id,
                exc,
            )

    return {
        "processed": processed,
        "failed": failed,
        "labels_inserted": labels_inserted,
        "resolved_config_id": resolved_config_id,
    }
