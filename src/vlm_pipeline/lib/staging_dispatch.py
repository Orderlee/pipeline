"""Compatibility wrapper for legacy test dispatch payload helpers."""

from __future__ import annotations

from vlm_pipeline.lib.dispatch_payload import (
    format_dispatch_storage_list,
    parse_dispatch_request_payload,
)

__all__ = [
    "format_dispatch_storage_list",
    "parse_dispatch_request_payload",
]
