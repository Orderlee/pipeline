"""Backward-compatibility re-exports — canonical location is ``vlm_pipeline.lib.spec_config``."""

from __future__ import annotations

from vlm_pipeline.lib.spec_config import (  # noqa: F401
    load_persisted_spec_config,
    resolve_and_persist_spec_config,
)
