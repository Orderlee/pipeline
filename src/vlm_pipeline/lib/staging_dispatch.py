"""Helpers for staging dispatch JSON payload normalization."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from vlm_pipeline.lib.env_utils import (
    VALID_OUTPUTS,
    YOLO_OUTPUTS,
    derive_classes_from_categories,
    normalize_output_name,
    resolve_outputs,
)

_RUN_MODE_TO_OUTPUTS = {
    "gemini": ["timestamp", "captioning"],
    "yolo": ["bbox"],
    "both": ["timestamp", "captioning", "bbox"],
}

_OUTPUT_PRIORITY = {
    "timestamp": 0,
    "captioning": 1,
    "bbox": 2,
    "image_classification": 3,
    "video_classification": 4,
}

_NO_LABELING_MARKERS = frozenset(
    {
        "필요없음",
        "라벨링필요없음",
        "라벨링_필요없음",
        "라벨링없음",
        "no_labeling",
        "labeling_not_required",
        "not_required",
    }
)


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


def _has_no_labeling_marker(values: list[str]) -> bool:
    for item in values:
        if normalize_output_name(item) in _NO_LABELING_MARKERS:
            return True
    return False


def _finalize_outputs(values: list[str]) -> list[str]:
    resolved = resolve_outputs(run_mode=None, outputs_raw=",".join(values))
    deduped: list[str] = []
    for item in resolved:
        rendered = str(item or "").strip().lower()
        if not rendered or rendered not in VALID_OUTPUTS or rendered in deduped:
            continue
        deduped.append(rendered)
    if deduped and all(item in YOLO_OUTPUTS for item in deduped):
        deduped = [item for item in deduped if item in YOLO_OUTPUTS]
    deduped.sort(key=lambda item: (_OUTPUT_PRIORITY.get(item, 999), item))
    return deduped


def parse_dispatch_request_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize staging dispatch JSON into routing-friendly values.

    Priority:
    1. labeling_method
    2. outputs (legacy)
    3. run_mode (legacy)
    """
    raw_labeling_method_items = _normalize_string_list(payload.get("labeling_method"), lowercase=True)
    raw_outputs_items = _normalize_string_list(payload.get("outputs"), lowercase=True)
    raw_categories = _normalize_string_list(payload.get("categories"), lowercase=True)
    raw_classes = _normalize_string_list(payload.get("classes"), lowercase=True)

    archive_only = any(
        (
            _has_no_labeling_marker(raw_labeling_method_items),
            _has_no_labeling_marker(raw_outputs_items),
            _has_no_labeling_marker(raw_categories),
        )
    )

    if archive_only:
        return {
            "categories": raw_categories,
            "classes": raw_classes,
            "labeling_method": [],
            "outputs_str": "",
            "run_mode": "",
            "archive_only": True,
        }

    raw_labeling_method = _normalize_output_list(payload.get("labeling_method"))
    raw_outputs = _normalize_output_list(payload.get("outputs"))
    run_mode = str(payload.get("run_mode") or "").strip().lower()

    if raw_labeling_method:
        valid_outputs = [item for item in raw_labeling_method if item in VALID_OUTPUTS]
        if not valid_outputs:
            raise ValueError("invalid_labeling_method")
        labeling_method = _finalize_outputs(valid_outputs)
    elif raw_outputs:
        valid_outputs = [item for item in raw_outputs if item in VALID_OUTPUTS]
        if not valid_outputs:
            raise ValueError("invalid_outputs")
        labeling_method = _finalize_outputs(valid_outputs)
    elif run_mode:
        if run_mode not in _RUN_MODE_TO_OUTPUTS:
            raise ValueError(f"invalid_run_mode:{run_mode}")
        labeling_method = list(_RUN_MODE_TO_OUTPUTS[run_mode])
    else:
        raise ValueError("missing_labeling_method_or_outputs_or_run_mode")

    categories = raw_categories
    classes = raw_classes
    if not classes and categories:
        classes = derive_classes_from_categories(categories)

    return {
        "categories": categories,
        "classes": classes,
        "labeling_method": labeling_method,
        "outputs_str": ",".join(labeling_method),
        "run_mode": run_mode,
        "archive_only": False,
    }
