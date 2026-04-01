"""Shared helpers for importing prebuilt label JSON artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha1
from pathlib import Path

EVENT_LABEL_DIR_NAMES = ("auto_labels", "reviewed_labels", "labels", "annotations")
VIDEO_CLASSIFICATION_DIR_NAMES = ("video_classifications",)


@dataclass
class EventLabelImportResult:
    loaded: int = 0
    inserted: int = 0
    skipped: int = 0
    not_matched: int = 0
    imported_asset_ids: set[str] = field(default_factory=set)
    label_key_by_asset_id: dict[str, str] = field(default_factory=dict)


def iter_label_files(label_dir: Path) -> list[Path]:
    return sorted(path for path in label_dir.rglob("*.json") if path.is_file())


def build_event_label_dirs(incoming_dir: Path) -> list[Path]:
    return [Path(incoming_dir) / dir_name for dir_name in EVENT_LABEL_DIR_NAMES]


def collect_event_label_json_paths(incoming_dir: Path) -> list[Path]:
    json_paths: list[Path] = []
    for label_dir in build_event_label_dirs(incoming_dir):
        if label_dir.exists():
            json_paths.extend(iter_label_files(label_dir))
    return json_paths


def summarize_event_label_import(result: EventLabelImportResult) -> dict[str, int]:
    return {
        "loaded": result.loaded,
        "inserted": result.inserted,
        "skipped": result.skipped,
        "not_matched": result.not_matched,
    }


def detect_label_source(json_path: Path) -> tuple[str, str]:
    lowered_parts = [part.lower() for part in json_path.parts]
    if any("review" in part for part in lowered_parts):
        return "manual_review", "reviewed"
    if any("auto" in part for part in lowered_parts):
        return "auto", "auto_generated"
    return "manual", "pending"


def build_label_key(
    raw_key: str,
    label_source: str,
    review_status: str,
    *,
    label_format: str | None = None,
) -> str:
    base_key = str(raw_key).rsplit(".", 1)[0]
    if label_format == "video_classification_json":
        parent, _, stem = base_key.rpartition("/")
        if parent:
            return f"{parent}/video_classifications/{stem}.json"
        return f"video_classifications/{stem}.json"
    if label_source == "manual" and review_status == "pending":
        return f"{base_key}.json"
    suffix = "reviewed" if review_status == "reviewed" else label_source
    return f"{base_key}.{suffix}.json"


def resolve_matching_asset(db, json_path: Path, payload: dict | list, *, source_unit_name: str | None = None) -> dict | None:
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
        matched = db.find_by_raw_key_stem(normalized, source_unit_name=source_unit_name)
        if matched:
            return matched
    return None


def extract_label_events(data: dict | list) -> list[dict]:
    if isinstance(data, list):
        return [normalize_event_payload(item, index) for index, item in enumerate(data)]

    if not isinstance(data, dict):
        return []

    candidates = data.get("events") or data.get("segments") or data.get("predictions")
    if isinstance(candidates, list) and candidates:
        return [normalize_event_payload(item, index) for index, item in enumerate(candidates)]

    return [normalize_event_payload(data, 0)]


def normalize_event_payload(item: object, index: int) -> dict:
    payload = item if isinstance(item, dict) else {}
    start_sec = coerce_float(
        payload.get("timestamp_start_sec")
        or payload.get("start_sec")
        or payload.get("start")
        or payload.get("begin_sec")
    )
    end_sec = coerce_float(
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
    predicted_classes = payload.get("predicted_classes") if isinstance(payload.get("predicted_classes"), list) else []
    if object_count == 0 and predicted_classes:
        object_count = len(predicted_classes)
    if object_count == 0 and str(payload.get("predicted_class") or "").strip():
        object_count = 1
    caption_text = (
        str(
            payload.get("caption_text")
            or payload.get("predicted_class")
            or payload.get("classification")
            or payload.get("class_label")
            or payload.get("caption")
            or payload.get("description")
            or payload.get("text")
            or ""
        ).strip()
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


def coerce_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def detect_label_format(data: dict | list) -> str:
    if isinstance(data, dict):
        has_image_identity = any(str(data.get(key) or "").strip() for key in ("image_key", "image_id"))
        has_image_classification = (
            isinstance(data.get("predicted_classes"), list)
            or isinstance(data.get("class_counts"), dict)
            or bool(str(data.get("bbox_labels_key") or "").strip())
        )
        if has_image_identity and has_image_classification:
            return "image_classification_json"
        if (
            any(str(data.get(key) or "").strip() for key in ("predicted_class", "classification", "class_label"))
            and not has_image_identity
        ):
            return "video_classification_json"
        if "images" in data and "annotations" in data:
            return "coco"
        if "events" in data or "segments" in data or "predictions" in data:
            return "auto_event_json"
        if "shapes" in data:
            return "labelme"
    if isinstance(data, list):
        return "yolo"
    return "custom"


def detect_label_tool(data: dict | list, label_source: str) -> str:
    if isinstance(data, dict):
        explicit_tool = str(data.get("label_tool") or data.get("tool") or "").strip()
        if explicit_tool:
            return explicit_tool
        label_format = detect_label_format(data)
        if label_format == "video_classification_json":
            return "gemini"
        if label_format == "image_classification_json":
            return "yolo-world-classification"
    if label_source == "auto":
        return "auto-model"
    if label_source == "manual_review":
        return "review-tool"
    return "pre-built"


def stable_label_id(
    asset_id: str,
    label_source: str,
    review_status: str,
    event_index: int,
    event: dict,
) -> str:
    token = "|".join(
        [
            str(asset_id),
            str(label_source),
            str(review_status),
            str(event_index),
            str(event.get("timestamp_start_sec")),
            str(event.get("timestamp_end_sec")),
            str(event.get("caption_text") or ""),
            str(event.get("object_count") or 0),
        ]
    )
    return sha1(token.encode("utf-8")).hexdigest()


def import_event_label_files(
    context,
    db,
    minio,
    json_paths: list[Path],
    *,
    source_unit_name: str | None = None,
    update_timestamp_status: bool = False,
) -> EventLabelImportResult:
    result = EventLabelImportResult()

    for json_path in sorted(json_paths):
        try:
            label_data = json.loads(json_path.read_text(encoding="utf-8"))
            matching_asset = resolve_matching_asset(
                db,
                json_path,
                label_data,
                source_unit_name=source_unit_name,
            )
            if not matching_asset:
                context.log.warning(f"매칭 raw_key 없음: {json_path}")
                result.not_matched += 1
                continue

            raw_key = str(matching_asset["raw_key"])
            asset_id = str(matching_asset["asset_id"])
            label_source, review_status = detect_label_source(json_path)
            events = extract_label_events(label_data)
            if not events:
                events = [{}]

            event_count = len(events)
            label_format = detect_label_format(label_data)
            label_key = build_label_key(
                raw_key,
                label_source,
                review_status,
                label_format=label_format,
            )
            label_bytes = json.dumps(label_data, ensure_ascii=False).encode("utf-8")
            minio.upload("vlm-labels", label_key, label_bytes, "application/json")
            label_tool = detect_label_tool(label_data, label_source)
            for event_index, event in enumerate(events):
                db.insert_label(
                    {
                        "label_id": stable_label_id(
                            asset_id,
                            label_source,
                            review_status,
                            event_index,
                            event,
                        ),
                        "asset_id": asset_id,
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
                result.inserted += 1

            if update_timestamp_status:
                db.update_timestamp_status(
                    asset_id,
                    "completed",
                    label_key=label_key,
                )
            result.loaded += 1
            result.imported_asset_ids.add(asset_id)
            result.label_key_by_asset_id[asset_id] = label_key
        except Exception as exc:  # noqa: BLE001
            context.log.error(f"라벨 로딩 실패: {json_path}: {exc}")
            result.skipped += 1

    return result
