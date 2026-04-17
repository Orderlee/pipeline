"""INGEST @op 정의 — register, normalize, upload, summary (facade).

Layer 3: Dagster @op, lib/ + resources/ import.

실제 구현은 3개 submodule로 분리됨:
- ``ops_common``   — 상수 / transient 판정 / rejection append
- ``ops_register`` — register_incoming
- ``ops_normalize`` — normalize_and_archive + ingest_summary
"""

from __future__ import annotations

from .ops_common import (
    DEFAULT_IMAGE_CODEC,
    DEFAULT_META_WORKERS,
    DEFAULT_RAW_BUCKET,
    DEFAULT_REENCODE_THREADS,
    DEFAULT_REENCODE_WORKERS,
    DEFAULT_UPLOAD_WORKERS,
    DEFAULT_VIDEO_CONTENT_TYPE,
    MAX_META_WORKERS,
    MAX_UPLOAD_WORKERS,
    TRANSIENT_ERROR_MARKERS,
    _append_ingest_rejection,
    _is_retryable_failed_record,
    _is_transient_error,
)
from .ops_normalize import ingest_summary, normalize_and_archive
from .ops_register import register_incoming

__all__ = [
    "DEFAULT_IMAGE_CODEC",
    "DEFAULT_META_WORKERS",
    "DEFAULT_RAW_BUCKET",
    "DEFAULT_REENCODE_THREADS",
    "DEFAULT_REENCODE_WORKERS",
    "DEFAULT_UPLOAD_WORKERS",
    "DEFAULT_VIDEO_CONTENT_TYPE",
    "MAX_META_WORKERS",
    "MAX_UPLOAD_WORKERS",
    "TRANSIENT_ERROR_MARKERS",
    "_append_ingest_rejection",
    "_is_retryable_failed_record",
    "_is_transient_error",
    "ingest_summary",
    "normalize_and_archive",
    "register_incoming",
]
