"""Run tag 파싱 유틸 — 순수 함수(L1-2 계층, DB 의존 없음).

DB 의존 헬퍼(`resolve_and_persist_spec_config`, `load_persisted_spec_config`)는
L3+ 계층으로 이동: `vlm_pipeline.defs.spec.config_resolver`.
"""

from __future__ import annotations

from collections.abc import Mapping

from vlm_pipeline.lib.env_utils import (
    is_dispatch_yolo_only_requested,
    parse_outputs_raw,
)


def is_unscoped_mvp_autolabel_run(tags: Mapping[str, str] | None) -> bool:
    """spec·dispatch(folder/outputs/run_mode) 태그 없이 MVP 일직선 job으로 실행되는 경우."""
    if not tags:
        return True
    if str(tags.get("spec_id") or "").strip():
        return False
    if str(tags.get("dispatch_archive_only") or "").lower() in ("1", "true", "yes"):
        return False
    if is_dispatch_yolo_only_requested(tags):
        return False
    if str(tags.get("folder_name") or "").strip():
        return False
    outputs_raw = (
        tags.get("requested_outputs")
        or tags.get("outputs")
        or tags.get("labeling_method")
        or ""
    )
    if parse_outputs_raw(outputs_raw):
        return False
    if str(tags.get("run_mode") or "").strip():
        return False
    return True


def parse_requested_outputs(tags: Mapping[str, str] | None) -> list[str]:
    """Parse requested outputs from run tags for routed staging jobs."""
    if not tags:
        return []
    outputs_raw = (
        tags.get("requested_outputs")
        or tags.get("outputs")
        or tags.get("labeling_method")
        or ""
    )
    return parse_outputs_raw(outputs_raw)


def is_standard_spec_run(tags: Mapping[str, str] | None) -> bool:
    """True when the routed run was triggered for a labeling spec."""
    if not tags:
        return False
    return bool(str(tags.get("spec_id") or "").strip())
