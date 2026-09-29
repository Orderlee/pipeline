"""Helpers for dispatch JSON payload normalization."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from vlm_pipeline.lib.env_utils import (
    _RUN_MODE_TO_OUTPUTS,
    VALID_LABELING_METHODS,
    VALID_OUTPUTS,
    YOLO_OUTPUTS,
    derive_classes_from_categories,
    normalize_output_name,
    resolve_outputs,
)

_OUTPUT_PRIORITY = {
    "timestamp_video": 0,
    "captioning_video": 1,
    "captioning_image": 2,
    "bbox": 2,
    "classification_image": 3,
    "classification_video": 4,
    "skip": 999,
}

# video 소스에서만 의미가 있는 라벨링 방법.
#
# 2026-09-21 batch b4fe9a6f-339(comfy_local, 이미지 1장): 요청은 `[captioning_image, bbox]`
# 였는데 `_OUTPUT_DEPENDENCIES["captioning_image"] = ["timestamp_video", "captioning_video"]`
# (env_utils.py) 가 의존성으로 두 video 메서드를 붙여 4개로 확장됐고, LS 생성 단계가
# 이미지뿐인 배치에 빈 video 프로젝트(LS id 821)를 만들었다.
#
# 그 의존성은 "비디오에서 프레임을 뽑으려면 먼저 이벤트 구간(timestamp)을 알아야 한다"는
# video 전제라, 원본이 이미지면 성립하지 않는다. 따라서 payload 가 이미지 전용임을
# 선언하면(`output_media='image'`) video 전용 메서드는 의존성으로도 자동 추가하지 않는다.
VIDEO_ONLY_LABELING_METHODS = frozenset(
    {
        "timestamp_video",
        "captioning_video",
        "classification_video",
    }
)

_NO_LABELING_MARKERS = frozenset(
    {
        "필요없음",
        "라벨링필요없음",
        "라벨링_필요없음",
        "라벨링없음",
        "skip",
        "no_labeling",
        "labeling_not_required",
        "not_required",
        "ff",  # ingest-only (DB/MinIO 적재만, 라벨링 skip)
    }
)


def format_dispatch_storage_list(values: list[str] | None) -> str:
    """DB 저장용 dispatch list를 사람이 읽기 쉬운 쉼표 문자열로 변환."""
    if not values:
        return ""

    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        rendered = str(item or "").strip()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return ", ".join(normalized)


def _normalize_string_list(value: Any, *, lowercase: bool = True) -> list[str]:
    if not isinstance(value, list):
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        rendered = str(item or "").strip()
        if not rendered:
            continue
        if lowercase:
            rendered = rendered.lower()
        if rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def _normalize_output_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        rendered = normalize_output_name(item)
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def _collect_invalid_output_values(
    raw_values: list[str],
    *,
    valid_values: set[str] | frozenset[str],
) -> list[str]:
    invalid: list[str] = []
    seen: set[str] = set()
    for item in raw_values:
        normalized = normalize_output_name(item)
        if not normalized or normalized in _NO_LABELING_MARKERS or normalized in valid_values:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        invalid.append(normalized)
    return invalid


def _has_no_labeling_marker(values: list[str]) -> bool:
    for item in values:
        if normalize_output_name(item) in _NO_LABELING_MARKERS:
            return True
    return False


def normalize_media_kind(value: Any) -> str:
    """dispatch payload 의 매체 선언을 'image' / 'video' / '' 로 정규화.

    선언이 없으면 빈 문자열 — 기존 dispatch 요청(선언 없음)은 media 필터를 타지 않는다.
    """
    rendered = str(value or "").strip().lower()
    if rendered in {"image", "images", "img"}:
        return "image"
    if rendered in {"video", "videos"}:
        return "video"
    return ""


def _finalize_outputs(values: list[str], *, media_kind: str = "") -> list[str]:
    resolved = resolve_outputs(run_mode=None, outputs_raw=",".join(values))
    deduped: list[str] = []
    for item in resolved:
        rendered = str(item or "").strip().lower()
        if not rendered or rendered not in VALID_OUTPUTS or rendered in deduped:
            continue
        if media_kind == "image" and rendered in VIDEO_ONLY_LABELING_METHODS:
            # 의존성 자동 추가분만 여기서 걸러진다 — 명시 요청은 parse 단계에서 이미 reject.
            continue
        deduped.append(rendered)
    if deduped and all(item in YOLO_OUTPUTS for item in deduped):
        deduped = [item for item in deduped if item in YOLO_OUTPUTS]
    deduped.sort(key=lambda item: (_OUTPUT_PRIORITY.get(item, 999), item))
    return deduped


def _reject_video_methods_on_image_media(values: list[str], media_kind: str) -> None:
    """이미지 전용 배치에 video 전용 메서드가 명시 요청되면 fail-loud.

    조용히 버리면 "요청한 것과 다른 라벨링이 돌았다"가 로그에만 남으므로 거부한다.
    GenAI promote 는 같은 계약을 HTTP 400 으로 먼저 막는다(jobs/promote.py).
    """
    if media_kind != "image":
        return
    rejected = [item for item in values if item in VIDEO_ONLY_LABELING_METHODS]
    if rejected:
        raise ValueError(f"video_labeling_method_on_image_media:{','.join(rejected)}")


def parse_dispatch_request_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize dispatch JSON payload into routing-friendly values.

    Priority (older agents may still send outputs/run_mode instead of labeling_method):
    1. labeling_method
    2. outputs (backward compat)
    3. run_mode (backward compat)
    """
    raw_labeling_method_items = _normalize_string_list(payload.get("labeling_method"), lowercase=True)
    raw_outputs_items = _normalize_string_list(payload.get("outputs"), lowercase=True)
    raw_categories = _normalize_string_list(payload.get("categories"), lowercase=True)
    raw_classes = _normalize_string_list(payload.get("classes"), lowercase=True)
    invalid_labeling_method = _collect_invalid_output_values(
        raw_labeling_method_items,
        valid_values=VALID_LABELING_METHODS,
    )
    invalid_outputs = _collect_invalid_output_values(
        raw_outputs_items,
        valid_values=VALID_OUTPUTS,
    )
    archive_only = any(
        (
            _has_no_labeling_marker(raw_labeling_method_items),
            _has_no_labeling_marker(raw_outputs_items),
            _has_no_labeling_marker(raw_categories),
        )
    )

    if archive_only:
        non_marker_values = [
            normalize_output_name(item)
            for item in [*raw_labeling_method_items, *raw_outputs_items]
            if normalize_output_name(item) and normalize_output_name(item) not in _NO_LABELING_MARKERS
        ]
        if non_marker_values:
            raise ValueError("skip_must_be_standalone")
        return {
            "categories": raw_categories,
            "classes": raw_classes,
            "labeling_method": ["skip"],
            "outputs_str": "skip",
            "run_mode": "",
            "archive_only": True,
        }

    raw_labeling_method = _normalize_output_list(payload.get("labeling_method"))
    raw_outputs = _normalize_output_list(payload.get("outputs"))
    run_mode = str(payload.get("run_mode") or "").strip().lower()
    media_kind = normalize_media_kind(payload.get("output_media") or payload.get("media_kind"))

    if raw_labeling_method:
        if invalid_labeling_method:
            raise ValueError("invalid_labeling_method")
        valid_outputs = [item for item in raw_labeling_method if item in VALID_LABELING_METHODS]
        if not valid_outputs:
            raise ValueError("invalid_labeling_method")
        _reject_video_methods_on_image_media(valid_outputs, media_kind)
        labeling_method = _finalize_outputs(valid_outputs, media_kind=media_kind)
    elif raw_outputs:
        if invalid_outputs:
            raise ValueError("invalid_outputs")
        valid_outputs = [item for item in raw_outputs if item in VALID_OUTPUTS]
        if not valid_outputs:
            raise ValueError("invalid_outputs")
        _reject_video_methods_on_image_media(valid_outputs, media_kind)
        labeling_method = _finalize_outputs(valid_outputs, media_kind=media_kind)
    elif run_mode:
        if run_mode not in _RUN_MODE_TO_OUTPUTS:
            raise ValueError(f"invalid_run_mode:{run_mode}")
        labeling_method = [
            item
            for item in _RUN_MODE_TO_OUTPUTS[run_mode]
            if not (media_kind == "image" and item in VIDEO_ONLY_LABELING_METHODS)
        ]
    else:
        raise ValueError("missing_labeling_method_or_outputs_or_run_mode")

    if not labeling_method:
        # media 필터가 전부 걷어낸 경우 — 조용히 빈 dispatch 를 만들지 않는다.
        raise ValueError("no_labeling_method_for_image_media")

    categories = raw_categories
    classes = raw_classes
    if not classes and categories:
        classes = derive_classes_from_categories(categories)
    if "classification_video" in labeling_method and not (categories or classes):
        raise ValueError("classification_video_requires_categories_or_classes")

    return {
        "categories": categories,
        "classes": classes,
        "labeling_method": labeling_method,
        "outputs_str": ",".join(labeling_method),
        "run_mode": run_mode,
        "archive_only": False,
    }
