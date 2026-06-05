"""run tag → detection 대상 클래스 해석 (L4 — defs/spec 계층).

lib/detection_common.py 에서 분리: lazy import 없이 config_resolver 를 형제 import.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from vlm_pipeline.defs.spec.config_resolver import load_persisted_spec_config
from vlm_pipeline.lib.detection_common import normalize_classes, parse_tag_list
from vlm_pipeline.lib.env_utils import derive_classes_from_categories


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
