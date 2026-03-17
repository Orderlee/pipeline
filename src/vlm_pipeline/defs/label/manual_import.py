"""LABEL @asset — 수동 라벨 JSON 임포트 (기존 clip_timestamp 로직 보존).

로컬 incoming 디렉터리의 JSON 파일을 읽어 raw_files와 매칭 후
vlm-labels 업로드 + labels 테이블 INSERT.
"""

from __future__ import annotations

import json
from hashlib import sha1
from pathlib import Path

from dagster import Field, asset

from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    name="manual_label_import",
    description="수동 라벨 JSON 임포트 — incoming/{auto_labels,reviewed_labels,labels,annotations}",
    group_name="label",
    config_schema={"limit": Field(int, default_value=5000)},
)
def manual_label_import(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """로컬 JSON 파일을 vlm-labels + labels 테이블에 등록."""
    config = PipelineConfig()

    label_dirs = [
        Path(config.incoming_dir) / "auto_labels",
        Path(config.incoming_dir) / "reviewed_labels",
        Path(config.incoming_dir) / "labels",
        Path(config.incoming_dir) / "annotations",
    ]

    loaded = 0
    inserted = 0
    skipped = 0
    not_matched = 0

    for label_dir in label_dirs:
        if not label_dir.exists():
            continue

        for json_path in _iter_label_files(label_dir):
            try:
                label_data = json.loads(json_path.read_text(encoding="utf-8"))
                matching_asset = _resolve_matching_asset(db, json_path, label_data)
                if not matching_asset:
                    context.log.warning(f"매칭 raw_key 없음: {json_path.name}")
                    not_matched += 1
                    continue

                raw_key = matching_asset["raw_key"]
                label_source, review_status = _detect_label_source(json_path)
                label_key = _build_label_key(raw_key, label_source, review_status)
                label_bytes = json.dumps(label_data, ensure_ascii=False).encode("utf-8")
                minio.upload("vlm-labels", label_key, label_bytes, "application/json")

                events = _extract_label_events(label_data)
                if not events:
                    events = [{}]

                event_count = len(events)
                label_format = _detect_label_format(label_data)
                label_tool = _detect_label_tool(label_data, label_source)
                for event_index, event in enumerate(events):
                    db.insert_label(
                        {
                            "label_id": _stable_label_id(
                                str(matching_asset["asset_id"]),
                                label_source,
                                review_status,
                                event_index,
                                event,
                            ),
                            "asset_id": matching_asset["asset_id"],
                            "labels_bucket": "vlm-labels",
                            "labels_key": label_key,
                            "label_format": label_format,
                            "label_tool": label_tool,
                            "label_source": label_source,
                            "review_status": review_status,
                            "event_index": event_index,
                            "event_count": event_count,
                            "timestamp_start_sec": event.get("timestamp_start_sec"),
                            "timestamp_end_sec": event.get("timestamp_end_sec"),
                            "caption_text": event.get("caption_text"),
                            "object_count": event.get("object_count", 0),
                            "label_status": "completed",
                        }
                    )
                    inserted += 1
                loaded += 1

            except Exception as e:
                context.log.error(f"라벨 로딩 실패: {json_path}: {e}")
                skipped += 1

    summary = {"loaded": loaded, "inserted": inserted, "skipped": skipped, "not_matched": not_matched}
    context.add_output_metadata(summary)
    context.log.info(f"MANUAL LABEL IMPORT 완료: {summary}")
    return summary


# ── helpers ──

def _iter_label_files(label_dir: Path) -> list[Path]:
    return sorted(path for path in label_dir.rglob("*.json") if path.is_file())


def _detect_label_source(json_path: Path) -> tuple[str, str]:
    lowered_parts = [part.lower() for part in json_path.parts]
    if any("review" in part for part in lowered_parts):
        return "manual_review", "reviewed"
    if any("auto" in part for part in lowered_parts):
        return "auto", "auto_generated"
    return "manual", "pending"


def _build_label_key(raw_key: str, label_source: str, review_status: str) -> str:
    base_key = str(raw_key).rsplit(".", 1)[0]
    if label_source == "manual" and review_status == "pending":
        return f"{base_key}.json"
    suffix = "reviewed" if review_status == "reviewed" else label_source
    return f"{base_key}.{suffix}.json"


def _resolve_matching_asset(db: DuckDBResource, json_path: Path, payload: dict | list) -> dict | None:
    candidate_stems: list[str] = []
    if isinstance(payload, dict):
        for key in ("raw_key", "source_raw_key", "media_key", "clip_key", "source_path"):
            raw = str(payload.get(key) or "").strip()
            if raw:
                candidate_stems.append(Path(raw).stem)
        raw_name = str(payload.get("original_name") or "").strip()
        if raw_name:
            candidate_stems.append(Path(raw_name).stem)

    candidate_stems.extend(
        [
            json_path.stem.replace(".auto", "").replace(".reviewed", ""),
            json_path.stem,
        ]
    )
    seen: set[str] = set()
    for stem in candidate_stems:
        normalized = str(stem or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        matched = db.find_by_raw_key_stem(normalized)
        if matched:
            return matched
    return None


def _extract_label_events(data: dict | list) -> list[dict]:
    if isinstance(data, list):
        return [_normalize_event_payload(item, index) for index, item in enumerate(data)]

    if not isinstance(data, dict):
        return []

    candidates = data.get("events") or data.get("segments") or data.get("predictions")
    if isinstance(candidates, list) and candidates:
        return [_normalize_event_payload(item, index) for index, item in enumerate(candidates)]

    return [_normalize_event_payload(data, 0)]


def _normalize_event_payload(item: object, index: int) -> dict:
    payload = item if isinstance(item, dict) else {}
    start_sec = _coerce_float(
        payload.get("timestamp_start_sec")
        or payload.get("start_sec")
        or payload.get("start")
        or payload.get("begin_sec")
    )
    end_sec = _coerce_float(
        payload.get("timestamp_end_sec")
        or payload.get("end_sec")
        or payload.get("end")
        or payload.get("finish_sec")
    )
    if start_sec is not None and end_sec is not None and end_sec < start_sec:
        start_sec, end_sec = end_sec, start_sec

    detections = (
        payload.get("detections")
        or payload.get("objects")
        or payload.get("annotations")
        or payload.get("boxes")
        or []
    )
    object_count = len(detections) if isinstance(detections, list) else 0
    caption_text = (
        str(payload.get("caption_text") or payload.get("caption") or payload.get("description") or payload.get("text") or "").strip()
        or None
    )
    if start_sec is None and end_sec is None and not caption_text and object_count == 0:
        return {
            "event_index": index,
            "timestamp_start_sec": None,
            "timestamp_end_sec": None,
            "caption_text": None,
            "object_count": 0,
        }
    return {
        "event_index": index,
        "timestamp_start_sec": start_sec,
        "timestamp_end_sec": end_sec,
        "caption_text": caption_text,
        "object_count": object_count,
    }


def _coerce_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _detect_label_format(data: dict) -> str:
    if "images" in data and "annotations" in data:
        return "coco"
    if "events" in data or "segments" in data or "predictions" in data:
        return "auto_event_json"
    if "shapes" in data:
        return "labelme"
    if isinstance(data, list):
        return "yolo"
    return "custom"


def _detect_label_tool(data: dict | list, label_source: str) -> str:
    if isinstance(data, dict):
        explicit_tool = str(data.get("label_tool") or data.get("tool") or "").strip()
        if explicit_tool:
            return explicit_tool
    if label_source == "auto":
        return "auto-model"
    if label_source == "manual_review":
        return "review-tool"
    return "pre-built"


def _stable_label_id(
    asset_id: str,
    label_source: str,
    review_status: str,
    event_index: int,
    event: dict,
) -> str:
    token = "|".join(
        [
            asset_id,
            label_source,
            review_status,
            str(event_index),
            str(event.get("timestamp_start_sec")),
            str(event.get("timestamp_end_sec")),
            str(event.get("caption_text") or ""),
            str(event.get("object_count") or 0),
        ]
    )
    return sha1(token.encode("utf-8")).hexdigest()
