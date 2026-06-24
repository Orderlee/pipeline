"""URL/annotation 변환 헬퍼 — stateless, DB 의존 없음."""

from __future__ import annotations

import base64
import re
from datetime import datetime, timezone
from pathlib import PurePosixPath
from urllib.parse import urlparse, parse_qs

# 클립 파일명 패턴: {base}_{8자리ms}_{8자리ms}
_CLIP_PATTERN = re.compile(r"^(.+)_(\d{8})_(\d{8})$")


def _decode_fileuri(video_url: str) -> str | None:
    try:
        qs = parse_qs(urlparse(video_url).query)
        encoded = qs.get("fileuri", [None])[0]
        if encoded:
            return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        pass
    return None


def _extract_raw_key_from_video_url(video_url: str, raw_bucket: str = "vlm-raw") -> str | None:
    """LS task data.video presigned URL -> raw_files.raw_key."""
    fileuri = _decode_fileuri(video_url)
    if fileuri:
        if fileuri.startswith(f"{raw_bucket}/"):
            return fileuri[len(raw_bucket) + 1 :]
        return fileuri or None
    try:
        path = (urlparse(video_url).path or "").lstrip("/")
        if "/" not in path:
            return None
        bucket, key = path.split("/", 1)
        if bucket != raw_bucket:
            return None
        return key or None
    except Exception:
        return None


def _resolve_labels_key_for_video(raw_key: str, stem: str) -> str:
    """raw_key -> events/<stem>.json 형태."""
    p = PurePosixPath(raw_key)
    return str(p.parent / "events" / f"{stem}.json")


def annotation_to_events(annotation: dict, fps: int) -> list[dict]:
    """LS annotation result -> Gemini 이벤트 포맷 변환."""
    events = []
    for item in annotation.get("result", []):
        if item.get("type") != "timelinelabels":
            continue
        value = item.get("value", {})
        for r in value.get("ranges", []):
            start_sec = r["start"] / fps
            end_sec = r["end"] / fps
            labels = value.get("timelinelabels", [])
            category = labels[0] if labels else "unknown"
            events.append(
                {
                    "category": category,
                    "duration": round(end_sec - start_sec, 3),
                    "timestamp": [round(start_sec, 3), round(end_sec, 3)],
                }
            )
    return events


def _parse_image_url_to_key(image_url: str) -> str | None:
    """presigned URL -> MinIO object key."""
    try:
        parsed = urlparse(image_url)
        path = (parsed.path or "").lstrip("/")
        if "/" not in path:
            return None
        return path.split("/", 1)[1] or None
    except Exception:
        return None


def _sam3_key_from_image_key(image_key: str) -> str:
    """build_sam3_detection_key()와 동일 규약 — cross-package import 회피 위해 인라인."""
    p = PurePosixPath(str(image_key or ""))
    stem = p.stem or "image"
    parent = p.parent
    raw_parent = parent.parent if parent.name == "image" else parent
    if str(raw_parent) and str(raw_parent) != ".":
        return f"{raw_parent}/sam3_segmentations/{stem}.json"
    return f"sam3_segmentations/{stem}.json"


def annotation_to_rectangles(annotation: dict) -> list[dict]:
    """LS RectangleLabels annotation -> bbox 리스트."""
    rects: list[dict] = []
    for item in annotation.get("result", []):
        if item.get("type") != "rectanglelabels":
            continue
        v = item.get("value", {}) or {}
        W = int(item.get("original_width") or 0)
        H = int(item.get("original_height") or 0)
        if W <= 0 or H <= 0:
            continue
        x = float(v.get("x", 0)) * W / 100.0
        y = float(v.get("y", 0)) * H / 100.0
        w = float(v.get("width", 0)) * W / 100.0
        h = float(v.get("height", 0)) * H / 100.0
        labels = v.get("rectanglelabels") or []
        label_name = str(labels[0]).strip() if labels else "unknown"
        rects.append(
            {
                "label": label_name,
                "bbox": [round(x, 2), round(y, 2), round(w, 2), round(h, 2)],
                "score": item.get("score"),
                "rotation": float(v.get("rotation", 0) or 0),
                "image_width": W,
                "image_height": H,
            }
        )
    return rects


def _merge_categories(existing: list[dict], new_names: list[str]) -> tuple[list[dict], dict[str, int]]:
    """기존 categories 보존 + 신규 label 확장. (updated_list, name_to_id) 반환."""
    name_to_id = {str(c.get("name", "")).strip(): int(c["id"]) for c in existing if "id" in c and "name" in c}
    max_id = max(name_to_id.values(), default=0)
    merged = list(existing)
    for name in new_names:
        name_s = name.strip()
        if not name_s or name_s in name_to_id:
            continue
        max_id += 1
        merged.append({"id": max_id, "name": name_s, "supercategory": "object"})
        name_to_id[name_s] = max_id
    return merged, name_to_id


def build_reviewed_coco_json(
    existing: dict,
    rectangles: list[dict],
    image_key: str,
) -> dict:
    """기존 SAM3 COCO JSON에 검수 결과를 반영해 새 JSON 생성."""
    out = dict(existing) if isinstance(existing, dict) else {}

    images = out.get("images") or []
    if not images and rectangles:
        r0 = rectangles[0]
        images = [
            {
                "id": 1,
                "file_name": image_key,
                "width": r0.get("image_width"),
                "height": r0.get("image_height"),
            }
        ]
    image_id = int(images[0]["id"]) if images else 1

    categories_in = out.get("categories") or []
    label_names = [r["label"] for r in rectangles]
    categories_out, name_to_id = _merge_categories(categories_in, label_names)

    new_annotations: list[dict] = []
    for i, r in enumerate(rectangles, start=1):
        bbox = r["bbox"]
        ann = {
            "id": i,
            "image_id": image_id,
            "category_id": name_to_id[r["label"]],
            "bbox": bbox,
            "area": round(float(bbox[2]) * float(bbox[3]), 2),
            "iscrowd": 0,
            "segmentation": [],
        }
        if r.get("score") is not None:
            ann["score"] = float(r["score"])
        if r.get("rotation"):
            ann["rotation"] = r["rotation"]
        new_annotations.append(ann)

    meta = dict(out.get("meta") or {})
    meta["review_source"] = "manual_review"
    meta["reviewed_at"] = datetime.now(timezone.utc).isoformat()
    meta["reviewed_object_count"] = len(new_annotations)

    out["info"] = out.get("info") or {}
    out["licenses"] = out.get("licenses") or []
    out["images"] = images
    out["annotations"] = new_annotations
    out["categories"] = categories_out
    out["meta"] = meta
    return out
