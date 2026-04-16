"""LABEL @asset — Gemini 기반 자동 이벤트 라벨링.

비디오당 1회 Gemini 호출 → 이벤트 JSON 생성 → vlm-labels 업로드 →
video_metadata.auto_label_* 상태 갱신.

구현은 하위 모듈(timestamp, label_helpers)에 위치하며,
이 파일은 @asset 데코레이터 + 라우팅 래퍼만 유지합니다.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from dagster import Field, asset

from vlm_pipeline.lib.env_utils import (
    dispatch_folder_for_source_unit,
)
from vlm_pipeline.lib.gemini import extract_clean_json_text
from vlm_pipeline.lib.spec_config import (
    is_standard_spec_run,
    is_unscoped_mvp_autolabel_run,
    parse_requested_outputs,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .label_helpers import (
    build_video_classification_key,
    build_video_classification_prompt,
    cleanup_temp,
    find_dispatch_video_classification_candidates,
    init_gemini_analyzer,
    materialize_video,
    parse_video_classification_response,
    resolve_dispatch_video_class_candidates,
    stable_video_classification_label_id,
)
from .timestamp import clip_timestamp_mvp, clip_timestamp_routed_impl


@asset(
    name="clip_timestamp",
    deps=["raw_ingest"],
    description="Gemini 이벤트 구간(timestamp) → vlm-labels; MVP는 auto_label_*, spec/dispatch는 timestamp_*·ready_for_labeling",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=50)},
)
def clip_timestamp(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    tags = context.run.tags if context.run else {}
    if is_unscoped_mvp_autolabel_run(tags):
        return clip_timestamp_mvp(context, db, minio)
    return clip_timestamp_routed_impl(context, db, minio)


@asset(
    name="classification_video",
    deps=["raw_ingest"],
    description="Dispatch 전용 Gemini 비디오 단일분류 → vlm-labels + labels",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=50)},
)
def classification_video(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    tags = context.run.tags if context.run else {}
    if is_standard_spec_run(tags):
        context.log.info("classification_video 스킵: staging spec 흐름은 이번 범위에서 제외합니다.")
        return {"processed": 0, "failed": 0, "skipped": True}

    requested = parse_requested_outputs(tags)
    if "classification_video" not in requested:
        context.log.info("classification_video 스킵: outputs에 classification_video 없음")
        return {"processed": 0, "failed": 0, "skipped": True}

    folder_name = dispatch_folder_for_source_unit(tags)
    if not folder_name:
        context.log.info("classification_video 스킵: dispatch folder 없음")
        return {"processed": 0, "failed": 0, "skipped": True}

    candidate_classes = resolve_dispatch_video_class_candidates(tags)
    if not candidate_classes:
        raise RuntimeError("classification_video_requires_categories_or_classes")

    db.ensure_runtime_schema()
    limit = int(context.op_config.get("limit", 50))
    candidates = find_dispatch_video_classification_candidates(
        db,
        folder_name=folder_name,
        limit=limit,
    )
    if not candidates:
        context.log.info("classification_video: 대상 없음")
        return {"processed": 0, "failed": 0, "predicted": 0}

    analyzer = init_gemini_analyzer(context)
    processed = 0
    failed = 0

    for idx, cand in enumerate(candidates, start=1):
        asset_id = str(cand.get("asset_id") or "")
        raw_key = str(cand.get("raw_key") or "")
        temp_paths: list[Path] = []
        try:
            video_path, temp_path = materialize_video(minio, cand)
            if temp_path:
                temp_paths.append(temp_path)
            response_text = analyzer.analyze_video(
                str(video_path),
                prompt=build_video_classification_prompt(candidate_classes),
            )
            cleaned = extract_clean_json_text(response_text)
            payload = parse_video_classification_response(cleaned, candidate_classes)
            label_key = build_video_classification_key(raw_key)
            generated_at = datetime.now()
            artifact_payload = {
                "asset_id": asset_id,
                "raw_key": raw_key,
                "predicted_class": payload["predicted_class"],
                "candidate_classes": candidate_classes,
                "rationale": payload.get("rationale"),
                "model": getattr(analyzer, "model_name", None),
                "generated_at": generated_at.isoformat(),
            }
            minio.ensure_bucket("vlm-labels")
            minio.upload(
                "vlm-labels",
                label_key,
                json.dumps(artifact_payload, ensure_ascii=False).encode("utf-8"),
                "application/json",
            )
            db.insert_label(
                {
                    "label_id": stable_video_classification_label_id(asset_id, label_key, payload["predicted_class"]),
                    "asset_id": asset_id,
                    "labels_bucket": "vlm-labels",
                    "labels_key": label_key,
                    "label_format": "video_classification_json",
                    "label_tool": "gemini",
                    "label_source": "auto",
                    "review_status": "auto_generated",
                    "event_index": 0,
                    "event_count": 1,
                    "timestamp_start_sec": None,
                    "timestamp_end_sec": None,
                    "caption_text": payload["predicted_class"],
                    "object_count": 1,
                    "label_status": "completed",
                    "created_at": generated_at,
                }
            )
            processed += 1
            context.log.info(
                "classification_video 진행: [%d/%d] asset=%s predicted_class=%s ✅",
                idx,
                len(candidates),
                asset_id,
                payload["predicted_class"],
            )
        except Exception as exc:
            failed += 1
            context.log.error(
                "classification_video 진행: [%d/%d] asset=%s ❌ %s",
                idx,
                len(candidates),
                asset_id,
                exc,
            )
        finally:
            for path in reversed(temp_paths):
                cleanup_temp(path)

    summary = {"processed": processed, "failed": failed, "predicted": processed}
    context.add_output_metadata(summary)
    return summary
