"""INGEST archive 유틸리티 (facade).

실제 구현은 5개 submodule로 분리됨:
- ``archive_move``     — timeout-wrapped shutil.move, __N suffix 경로 해결
- ``archive_cleanup``  — 빈 디렉토리/잔존 source 정리
- ``archive_policy``   — should_archive_manifest 등 정책 판정
- ``archive_prepare``  — pre-move manifest rewriting
- ``archive_finalize`` — archive 이동 실행 + completion handler
"""

from __future__ import annotations

from .archive_cleanup import (
    ARCHIVE_CLEANUP_MAX_DEPTH,
    ARCHIVE_CLEANUP_MAX_PARENT_LEVELS,
    cleanup_empty_parent_chain,
    cleanup_empty_tree,
    cleanup_residual_source_file,
)
from .archive_finalize import (
    _move_single_file,
    _move_single_file_fallback,
    archive_uploaded_assets,
    complete_uploaded_assets_in_archive,
    complete_uploaded_assets_without_archive,
)
from .archive_move import (
    ARCHIVE_FIND_MAX_SUFFIX,
    ARCHIVE_MAX_SUFFIX_ATTEMPTS,
    DEFAULT_ARCHIVE_MOVE_TIMEOUT_SEC,
    _move_with_timeout,
    find_existing_archive_candidate,
    find_existing_archive_directory,
    find_existing_in_archive_unit_dirs,
    is_source_missing_error,
    resolve_archive_source_unit_name,
    resolve_unique_directory,
    resolve_unique_file,
)
from .archive_policy import (
    FALSY_STRINGS,
    TRUTHY_STRINGS,
    _staging_transfer_allows_archive,
    manifest_allows_auto_bootstrap_without_dispatch,
    manifest_source_under_gcp,
    should_archive_manifest,
)
from .archive_prepare import prepare_manifest_for_archive_upload

__all__ = [
    "ARCHIVE_CLEANUP_MAX_DEPTH",
    "ARCHIVE_CLEANUP_MAX_PARENT_LEVELS",
    "ARCHIVE_FIND_MAX_SUFFIX",
    "ARCHIVE_MAX_SUFFIX_ATTEMPTS",
    "DEFAULT_ARCHIVE_MOVE_TIMEOUT_SEC",
    "FALSY_STRINGS",
    "TRUTHY_STRINGS",
    "_move_single_file",
    "_move_single_file_fallback",
    "_move_with_timeout",
    "_staging_transfer_allows_archive",
    "archive_uploaded_assets",
    "cleanup_empty_parent_chain",
    "cleanup_empty_tree",
    "cleanup_residual_source_file",
    "complete_uploaded_assets_in_archive",
    "complete_uploaded_assets_without_archive",
    "find_existing_archive_candidate",
    "find_existing_archive_directory",
    "find_existing_in_archive_unit_dirs",
    "is_source_missing_error",
    "manifest_allows_auto_bootstrap_without_dispatch",
    "manifest_source_under_gcp",
    "prepare_manifest_for_archive_upload",
    "resolve_archive_source_unit_name",
    "resolve_unique_directory",
    "resolve_unique_file",
    "should_archive_manifest",
]
