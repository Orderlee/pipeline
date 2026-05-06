"""Shared helpers for importing prebuilt local label artifacts — thin facade.

도메인별 분할:
- ``artifact_scan``    — 디렉토리/파일 스캔 (DB·MinIO 의존 없음)
- ``artifact_resolve`` — DB + 파일시스템 lookup
- ``artifact_import``  — ``_ArtifactImportSummary``, ``_write_failure_log``,
  공개 entry point ``import_local_label_artifacts``

기존 caller(``ingest_post.py``, sibling ``artifact_*``,
``tests/unit/test_prelabeled_import_local_artifacts.py``)는 이 facade에서
모든 심볼을 그대로 import 가능.
"""

from __future__ import annotations

from .artifact_import import (
    _ArtifactImportSummary,
    _write_failure_log,
    import_local_label_artifacts,
)
from .artifact_import_utils import (
    now as _now,
    stable_id as _stable_id,
)
from .artifact_resolve import (
    _ensure_processed_media_rows,
    _find_existing_image_row,
)
from .artifact_scan import (
    _coerce_source_unit_dirs,
    resolve_local_artifact_source_dirs,
)

__all__ = [
    "_ArtifactImportSummary",
    "_coerce_source_unit_dirs",
    "_ensure_processed_media_rows",
    "_find_existing_image_row",
    "_now",
    "_stable_id",
    "_write_failure_log",
    "import_local_label_artifacts",
    "resolve_local_artifact_source_dirs",
]
