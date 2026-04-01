"""Hard-coded YOLO per-class confidence threshold helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

YOLO_SERVER_DEFAULT_CLASSES: tuple[str, ...] = (
    "person",
    "car",
    "truck",
    "bus",
    "motorcycle",
    "bicycle",
    "fire",
    "smoke",
    "flame",
    "knife",
    "gun",
    "bag",
    "backpack",
    "suitcase",
    "helmet",
    "safety vest",
    "hard hat",
    "traffic cone",
    "barricade",
    "dog",
    "cat",
)

# Keep this explicit map in sync with docker/yolo/app.py defaults and the
# category-derived aliases used by the pipeline.
YOLO_CLASS_CONFIDENCE_THRESHOLDS: dict[str, float] = {
    "person": 0.25,
    "car": 0.25,
    "truck": 0.25,
    "bus": 0.25,
    "motorcycle": 0.25,
    "bicycle": 0.25,
    "fire": 0.25,
    "smoke": 0.25,
    "flame": 0.25,
    "knife": 0.25,
    "gun": 0.25,
    "bag": 0.25,
    "backpack": 0.25,
    "suitcase": 0.25,
    "helmet": 0.25,
    "safety vest": 0.25,
    "hard hat": 0.25,
    "traffic cone": 0.25,
    "barricade": 0.25,
    "dog": 0.25,
    "cat": 0.25,
    "person_fallen": 0.30,
    "weapon": 0.25,
    "violence": 0.25,
    "fight": 0.25,
}


def _normalize_class_names(values: Iterable[object] | None) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values or []:
        rendered = str(value or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def _coerce_confidence(value: object, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(fallback)


def get_explicit_class_confidence_thresholds() -> dict[str, float]:
    """Return a copy of the hard-coded class threshold map."""
    return dict(YOLO_CLASS_CONFIDENCE_THRESHOLDS)


def resolve_active_class_confidence_thresholds(
    requested_classes: Iterable[object] | None,
    global_confidence_threshold: float,
) -> dict[str, float]:
    """Resolve the current run's active per-class thresholds."""
    base_threshold = _coerce_confidence(global_confidence_threshold, 0.25)
    active_classes = _normalize_class_names(requested_classes) or list(YOLO_SERVER_DEFAULT_CLASSES)
    return {
        class_name: _coerce_confidence(YOLO_CLASS_CONFIDENCE_THRESHOLDS.get(class_name), base_threshold)
        for class_name in active_classes
    }


def resolve_effective_request_confidence_threshold(
    global_confidence_threshold: float,
    class_confidence_thresholds: Mapping[str, object] | None,
) -> float:
    """Compute the confidence sent to the YOLO server."""
    base_threshold = _coerce_confidence(global_confidence_threshold, 0.25)
    thresholds = [
        _coerce_confidence(threshold, base_threshold)
        for threshold in (class_confidence_thresholds or {}).values()
    ]
    return min([base_threshold, *thresholds])


def filter_detections_by_class_confidence(
    detections: Iterable[Mapping[str, Any]] | None,
    *,
    global_confidence_threshold: float,
    class_confidence_thresholds: Mapping[str, object] | None,
) -> list[dict[str, Any]]:
    """Filter detections using global + per-class confidence thresholds."""
    base_threshold = _coerce_confidence(global_confidence_threshold, 0.25)
    accepted: list[dict[str, Any]] = []

    for detection in detections or []:
        class_name = str(detection.get("class") or detection.get("class_name") or "").strip().lower()
        try:
            confidence = float(detection.get("confidence"))
        except (TypeError, ValueError):
            continue

        class_threshold = _coerce_confidence((class_confidence_thresholds or {}).get(class_name), base_threshold)
        acceptance_threshold = max(base_threshold, class_threshold)
        if confidence < acceptance_threshold:
            continue
        accepted.append(dict(detection))

    return accepted
