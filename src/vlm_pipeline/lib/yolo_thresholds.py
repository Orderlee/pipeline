"""Hard-coded per-class confidence threshold helpers.

NOTE: 파일명에 "yolo" 가 들어가지만 SAM3 도 같은 dict 를 공유한다
(src/vlm_pipeline/defs/sam/detection_assets.py:227 의 prompt_score_thresholds).
class 이름이 YOLO/SAM3 모델에 따라 다를 수 있어 둘 다의 prompt 변형을 dict 에 등록한다.
예: YOLO 는 "person_fallen", SAM3 는 자연어 "fallen person" — 별도 키로 관리.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from vlm_pipeline.lib.detection_common import normalize_classes

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
    "cigarette",
    "smoking",
    "knife",
    "gun",
    "bat",
    "baseball bat",
    "sword",
    "dagger",
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
    # 2026-06-04: Kling/Veo 생성 영상 기준 사용자 검증 후 조정.
    # fire 그룹 (fire, flame, open flame) — preset Fire 클릭 시 SAM3 prompt 로 보냄.
    # smoke 그룹 (smoke, smoke cloud) — preset Smoke 클릭 시 SAM3 prompt 로 보냄.
    # cigarette/smoking 은 Gemini event 카테고리만 (SAM3 prompt 안 쓰임) — base 0.25 유지.
    "fire": 0.74,
    "smoke": 0.56,
    "flame": 0.74,
    "cigarette": 0.25,
    "smoking": 0.25,
    "knife": 0.25,
    "gun": 0.25,
    "bat": 0.15,
    "baseball bat": 0.15,
    "sword": 0.25,
    "dagger": 0.25,
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
    "person_fallen": 0.79,
    # SAM3 자연어 prompt 변형 — 2026-06-02 + 2026-06-04 확장.
    # hybrid preset Falldown 의 모든 SAM3 phrase 가 동일 threshold 공유.
    # 이전엔 "fallen person" 만 등록되어서 다른 phrase 는 base 0.25 적용 → 노이즈 박스 폭주.
    # 사용자 batch e00879ff-b04 검증: "person on the ground" 만 30+ boxes (0.51-0.90).
    "fallen person": 0.81,
    "person lying down": 0.81,
    "person on the ground": 0.81,
    "weapon": 0.25,
    "violence": 0.25,
    "fight": 0.25,
    # hybrid preset 의 SAM3 phrase — fire/smoke 그룹은 위에서 조정된 값과 sync.
    "open flame": 0.74,
    "smoke cloud": 0.56,
    "fighting people": 0.25,
    "punching person": 0.25,
    "person hitting person": 0.25,
}


def _normalize_class_names(values: Iterable[object] | None) -> list[str]:
    return normalize_classes(values)


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
        _coerce_confidence(threshold, base_threshold) for threshold in (class_confidence_thresholds or {}).values()
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
