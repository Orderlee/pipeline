"""Label key builders — thin wrappers delegating to ``lib.key_builders``."""

from __future__ import annotations


def build_gemini_label_key(raw_key: str) -> str:
    """`<raw_parent>/events/<video_stem>.json` 규칙으로 label key 생성."""
    from vlm_pipeline.lib.key_builders import build_gemini_label_key as _impl
    return _impl(raw_key)


def build_video_classification_key(raw_key: str) -> str:
    from vlm_pipeline.lib.key_builders import build_video_classification_key as _impl
    return _impl(raw_key)
