"""공용 클래스 임계값 모듈 단위 테스트.

이름은 yolo_ 지만 실제 소비자는 SAM3(defs/sam/*), dispatch builders,
lib/detection_coco, gemini/ls_tasks_create 까지 6곳이다 — YOLO 전용이 아니다.

임계값은 운영 중 계속 튜닝되므로(2026-09 실측: fire 0.74, person_fallen 0.79)
구체 수치를 얼려두지 않고 **불변식**만 검증한다. 수치를 박으면 튜닝마다 깨진다.
"""

from __future__ import annotations

from vlm_pipeline.lib.yolo_thresholds import (
    YOLO_CLASS_CONFIDENCE_THRESHOLDS,
    filter_detections_by_class_confidence,
    get_explicit_class_confidence_thresholds,
    resolve_active_class_confidence_thresholds,
    resolve_effective_request_confidence_threshold,
)


def test_explicit_thresholds_are_valid_probabilities_from_the_table() -> None:
    thresholds = get_explicit_class_confidence_thresholds()

    assert thresholds, "명시 임계값 테이블이 비면 전 클래스가 전역값으로 떨어진다"
    # 오타 클래스가 조용히 무시되지 않도록 — 정본 테이블 밖의 키는 없어야 한다.
    assert set(thresholds) <= set(YOLO_CLASS_CONFIDENCE_THRESHOLDS)
    assert all(0.0 < v <= 1.0 for v in thresholds.values())


def test_filter_detections_applies_per_class_threshold_over_global() -> None:
    active = resolve_active_class_confidence_thresholds(["person_fallen", "fire"], 0.25)
    pf, fire = active["person_fallen"], active["fire"]
    assert pf > 0.25, "person_fallen 은 전역값보다 엄격해야 한다"

    filtered = filter_detections_by_class_confidence(
        [
            {"class": "person_fallen", "confidence": pf - 0.01, "bbox": [1, 2, 3, 4]},
            {"class": "person_fallen", "confidence": pf, "bbox": [1, 2, 3, 4]},
            {"class": "fire", "confidence": fire, "bbox": [1, 2, 3, 4]},
        ],
        global_confidence_threshold=0.25,
        class_confidence_thresholds=active,
    )

    assert filtered == [
        {"class": "person_fallen", "confidence": pf, "bbox": [1, 2, 3, 4]},
        {"class": "fire", "confidence": fire, "bbox": [1, 2, 3, 4]},
    ]


def test_resolve_effective_request_confidence_uses_lowest_active_threshold() -> None:
    active = resolve_active_class_confidence_thresholds(["person_fallen", "fire"], 0.35)

    assert active == {
        "person_fallen": YOLO_CLASS_CONFIDENCE_THRESHOLDS["person_fallen"],
        "fire": YOLO_CLASS_CONFIDENCE_THRESHOLDS["fire"],
    }
    # 서버에는 가장 낮은 값으로 요청해야 클래스별 재필터 전에 후보가 유실되지 않는다.
    # 요청 전역값이 더 낮으면 그것이, 클래스 임계값이 더 낮으면 그것이 유효값이 된다.
    assert resolve_effective_request_confidence_threshold(0.35, active) == 0.35
    assert resolve_effective_request_confidence_threshold(0.90, active) == min(active.values())


def test_unknown_class_falls_back_to_global_threshold() -> None:
    filtered = filter_detections_by_class_confidence(
        [
            {"class": "unknown_class", "confidence": 0.24, "bbox": [1, 2, 3, 4]},
            {"class": "unknown_class", "confidence": 0.25, "bbox": [1, 2, 3, 4]},
        ],
        global_confidence_threshold=0.25,
        class_confidence_thresholds={"person_fallen": 0.30},
    )

    assert filtered == [
        {"class": "unknown_class", "confidence": 0.25, "bbox": [1, 2, 3, 4]},
    ]
