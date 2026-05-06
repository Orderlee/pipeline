"""Shared private primitives for label/* helpers.

Used by ``helpers_video`` (segment extraction) and ``helpers_gemini`` (preview
rendering). Also re-exports ``int_env`` so the ``label_helpers`` facade can
preserve the historical ``from .label_helpers import int_env`` path used by
``timestamp.py``.
"""

from __future__ import annotations

from pathlib import Path

from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.lib.file_loader import build_nonexistent_temp_path

__all__ = ["_build_nonexistent_temp_path", "int_env"]


def _build_nonexistent_temp_path(suffix: str) -> Path:
    return build_nonexistent_temp_path(suffix, prefix="vlm_gemini_")
