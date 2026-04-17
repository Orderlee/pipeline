"""Detection 모델(YOLO, SAM3 등) 공통 헬퍼.

YOLO/SAM3 양쪽에서 동일하게 복제되어 있던 로직을 하나로 통합한다:
- 클래스 정규화, 태그 파싱
- image_label_id 생성
- DB flush
- run tag → target class 해석
- spec config → bbox class 해석
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from hashlib import sha1

from vlm_pipeline.lib.env_utils import derive_classes_from_categories


# ---------------------------------------------------------------------------
# 클래스 정규화 / 태그 파싱
# ---------------------------------------------------------------------------

def normalize_classes(values: Iterable[object] | None) -> list[str]:
    """중복 제거 + lowercase + strip. 순서 보존."""
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values or []:
        rendered = str(value or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def parse_tag_list(raw_value: object) -> list[str]:
    """Dagster run tag 값(str, list, JSON array 문자열)을 클래스 리스트로 변환."""
    if isinstance(raw_value, list):
        return normalize_classes(raw_value)

    rendered = str(raw_value or "").strip()
    if not rendered:
        return []

    try:
        if rendered.startswith("["):
            parsed = json.loads(rendered)
            if isinstance(parsed, list):
                return normalize_classes(parsed)
    except Exception:
        pass

    return normalize_classes(rendered.split(","))


# ---------------------------------------------------------------------------
# image_label_id 생성
# ---------------------------------------------------------------------------

def stable_image_label_id(image_id: str, labels_key: str) -> str:
    """Deterministic label ID — 재실행 시 동일 행을 덮어쓸 수 있도록 한다."""
    return sha1(f"{image_id}|{labels_key}".encode()).hexdigest()


# ---------------------------------------------------------------------------
# DB flush
# ---------------------------------------------------------------------------

def flush_image_labels(db, rows: list[dict], context, *, tool_name: str = "detection") -> None:
    """image_labels 버퍼를 DB에 일괄 삽입한다."""
    try:
        count = db.batch_insert_image_labels(rows)
        context.log.debug(f"{tool_name} image_labels flush: {count}건")
    except Exception as exc:
        context.log.error(f"{tool_name} image_labels flush 실패: {exc}")


# ---------------------------------------------------------------------------
# Target class resolution (run tags → 클래스 목록)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TargetClassResolution:
    """run tag로부터 해석된 detection 대상 클래스 정보."""
    classes: list[str]
    class_source: str
    spec_id: str | None = None
    resolved_config_id: str | None = None


def resolve_target_classes(
    tags: Mapping[str, str] | None,
    db,
) -> TargetClassResolution:
    """run tag에서 spec_id / classes / categories 순으로 detection 대상 클래스를 해석한다.

    YOLO와 SAM3 양쪽에서 동일한 로직이므로 공통화한다.
    """
    if not tags:
        return TargetClassResolution(classes=[], class_source="server_default")

    spec_id = str(tags.get("spec_id") or "").strip()
    if spec_id:
        # Lazy import: defs/ 계층 의존을 함수 본문으로 미뤄 L1-2 layering 유지.
        from vlm_pipeline.defs.spec.config_resolver import load_persisted_spec_config

        config_bundle = load_persisted_spec_config(db, spec_id)
        resolved_config_id = str(config_bundle["resolved_config_id"] or "").strip() or None
        spec_classes = normalize_classes(config_bundle["spec"].get("classes") or [])
        bbox_config = config_bundle["config_json"].get("bbox", {})
        config_target_classes = normalize_classes(bbox_config.get("target_classes") or [])

        if config_target_classes:
            allowed = set(config_target_classes)
            target_classes = [v for v in spec_classes if v in allowed]
            if target_classes:
                return TargetClassResolution(
                    classes=target_classes,
                    class_source="spec_config_intersection",
                    spec_id=spec_id,
                    resolved_config_id=resolved_config_id,
                )
        if spec_classes:
            return TargetClassResolution(
                classes=spec_classes,
                class_source="spec_classes",
                spec_id=spec_id,
                resolved_config_id=resolved_config_id,
            )
        return TargetClassResolution(
            classes=[],
            class_source="server_default",
            spec_id=spec_id,
            resolved_config_id=resolved_config_id,
        )

    tag_classes = parse_tag_list(tags.get("classes"))
    if tag_classes:
        return TargetClassResolution(classes=tag_classes, class_source="dispatch_tags")

    categories = parse_tag_list(tags.get("categories"))
    derived_classes = normalize_classes(derive_classes_from_categories(categories))
    if derived_classes:
        return TargetClassResolution(
            classes=derived_classes,
            class_source="dispatch_categories_derived",
        )

    return TargetClassResolution(classes=[], class_source="server_default")
