"""INGEST @op 공통 상수 및 유틸 — transient 판정, rejection append.

Layer 3: 다른 ingest submodule에서 import 가능. 외부 직접 import는 지양.
"""

from __future__ import annotations

from .duplicate import error_code_from_message as _derive_error_code

# ── 업로드 / 재인코딩 / 메타 추출 워커 상수 ──────────────────────
DEFAULT_UPLOAD_WORKERS: int = 4
MAX_UPLOAD_WORKERS: int = 16
DEFAULT_REENCODE_WORKERS: int = 3
DEFAULT_REENCODE_THREADS: int = 4
DEFAULT_META_WORKERS: int = 4
MAX_META_WORKERS: int = 8

# ── MinIO content-type 기본값 ────────────────────────────────────
DEFAULT_VIDEO_CONTENT_TYPE: str = "video/mp4"
DEFAULT_IMAGE_CODEC: str = "jpeg"
DEFAULT_RAW_BUCKET: str = "vlm-raw"

TRANSIENT_ERROR_MARKERS = (
    "could not set lock on file",
    "conflicting lock is held",
    "duckdb lock",
    "ffprobe_timeout",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "connection reset",
)


def _is_transient_error(exc: Exception) -> bool:
    message = str(exc or "").strip().lower()
    return any(marker in message for marker in TRANSIENT_ERROR_MARKERS)


def _is_retryable_failed_record(stale_record: dict) -> bool:
    """failed 레코드 중 자동 재처리 정리 가능한 경우만 선별."""
    error_message = str(stale_record.get("error_message", "") or "").strip().lower()
    if not error_message:
        return False

    non_retryable_prefixes = (
        "duplicate_of:",
        "archive_move_failed",
        "archive_source_missing",
        "duplicate_skipped_in_manifest:",
    )
    if any(error_message.startswith(prefix) for prefix in non_retryable_prefixes):
        return False

    return any(marker in error_message for marker in TRANSIENT_ERROR_MARKERS)


def _append_ingest_rejection(
    ingest_rejections: list[dict] | None,
    *,
    source_path: str,
    rel_path: str | None,
    media_type: str,
    stage: str,
    error_message: str,
    retryable: bool,
    error_code: str | None = None,
) -> None:
    if ingest_rejections is None:
        return
    normalized_message = str(error_message or "").strip()
    ingest_rejections.append(
        {
            "source_path": str(source_path or ""),
            "rel_path": str(rel_path or ""),
            "media_type": str(media_type or "unknown"),
            "stage": stage,
            "error_code": error_code or _derive_error_code(normalized_message),
            "error_message": normalized_message,
            "retryable": bool(retryable),
        }
    )
