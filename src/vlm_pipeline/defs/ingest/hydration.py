"""INGEST manifest hydration helpers.

Dagster/PipelineConfig 의존 없이 stale manifest 재수화만 담당한다.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from vlm_pipeline.lib.validator import ALLOWED_EXTENSIONS

STALE_MANIFEST_ALL_MISSING_REASON = "manifest_stale_all_missing"
ALL_ALREADY_COMPLETED_REASON = "manifest_all_already_completed"
GCP_AUTO_BOOTSTRAP_TRANSFER_TOOLS: frozenset[str] = frozenset({
    "auto_bootstrap_sensor",
    "ingest_retry_manifest",
})
STALE_REHYDRATE_TRANSFER_TOOLS = GCP_AUTO_BOOTSTRAP_TRANSFER_TOOLS


def is_gcp_auto_bootstrap_directory_manifest(manifest: dict, *, source_unit_path: Path | None) -> bool:
    transfer_tool = str(manifest.get("transfer_tool", "")).strip().lower()
    if transfer_tool not in STALE_REHYDRATE_TRANSFER_TOOLS:
        return False
    if str(manifest.get("source_unit_type", "")).strip().lower() != "directory":
        return False
    if source_unit_path is None or not source_unit_path.is_dir():
        return False

    source_unit_name = str(manifest.get("source_unit_name", "")).strip().lower()
    if source_unit_name.startswith("gcp/"):
        return True

    normalized_path = str(source_unit_path).replace("\\", "/").rstrip("/")
    return "/incoming/gcp/" in normalized_path or normalized_path.endswith("/incoming/gcp")


def _normalize_manifest_rel_path(entry: dict, *, source_unit_path: Path) -> str:
    rel_path = str(entry.get("rel_path", "")).strip()
    if rel_path:
        return rel_path
    source_path = str(entry.get("path", "")).strip()
    if not source_path:
        return ""
    try:
        return str(Path(source_path).relative_to(source_unit_path))
    except Exception:
        return Path(source_path).name


def scan_manifest_source_unit_files(source_unit_path: Path) -> list[dict]:
    allowed_exts = {ext.lower() for ext in ALLOWED_EXTENSIONS}
    files: list[dict] = []

    def _scan_recursive(base: Path, rel_prefix: str = "") -> None:
        try:
            for entry in os.scandir(base):
                if entry.is_dir(follow_symlinks=False):
                    sub_rel = f"{rel_prefix}{entry.name}/" if rel_prefix else f"{entry.name}/"
                    _scan_recursive(Path(entry.path), sub_rel)
                elif entry.is_file(follow_symlinks=False):
                    if Path(entry.name).suffix.lower() in allowed_exts:
                        rel = f"{rel_prefix}{entry.name}" if rel_prefix else entry.name
                        try:
                            size = int(entry.stat(follow_symlinks=False).st_size)
                        except OSError:
                            size = 0
                        files.append({"path": entry.path, "size": size, "rel_path": rel})
        except OSError:
            return

    _scan_recursive(source_unit_path)
    return files


def reconcile_manifest_files_against_disk(
    context,
    manifest: dict,
    *,
    source_unit_path: Path,
    db=None,
) -> dict:
    original_entries = [entry for entry in manifest.get("files", []) if isinstance(entry, dict)]
    if not original_entries:
        return manifest

    surviving_entries: list[dict] = []
    missing_rel_paths: list[str] = []
    already_completed_rel_paths: list[str] = []

    candidate_source_paths: list[str] = []
    for entry in original_entries:
        sp = str(entry.get("path", "")).strip()
        if sp:
            candidate_source_paths.append(sp)

    completed_set: set[str] = set()
    if db is not None and candidate_source_paths:
        try:
            completed_set = db.find_completed_source_paths(candidate_source_paths)
        except Exception as exc:  # noqa: BLE001
            context.log.warning(
                f"find_completed_source_paths 조회 실패(필터 스킵): {exc}"
            )
            completed_set = set()

    for entry in original_entries:
        rel_path = _normalize_manifest_rel_path(entry, source_unit_path=source_unit_path)
        source_path = str(entry.get("path", "")).strip()
        candidate_path = Path(source_path) if source_path else None
        if candidate_path is None or not candidate_path.is_file():
            missing_rel_paths.append(rel_path or source_path or "<unknown>")
            continue

        if source_path in completed_set:
            already_completed_rel_paths.append(rel_path or source_path)
            continue

        updated_entry = dict(entry)
        updated_entry["path"] = str(candidate_path)
        updated_entry["rel_path"] = rel_path
        try:
            updated_entry["size"] = int(candidate_path.stat().st_size)
        except OSError:
            updated_entry["size"] = int(entry.get("size") or 0)
        surviving_entries.append(updated_entry)

    missing_count = len(missing_rel_paths)
    already_completed_count = len(already_completed_rel_paths)
    if missing_count <= 0 and already_completed_count <= 0:
        return manifest

    original_file_count = len(original_entries)
    manifest["files"] = surviving_entries
    manifest["file_count"] = len(surviving_entries)
    manifest["stale_manifest_detected"] = True
    manifest["original_file_count"] = original_file_count
    manifest["existing_file_count"] = len(surviving_entries)
    manifest["missing_entry_count"] = missing_count
    manifest["stale_missing_samples"] = missing_rel_paths[:10]
    manifest["rehydrated_at"] = datetime.now().isoformat()
    if already_completed_count > 0:
        manifest["already_completed_file_count"] = already_completed_count
        manifest["already_completed_samples"] = already_completed_rel_paths[:10]

    if not surviving_entries and missing_count > 0 and already_completed_count == 0:
        manifest["stale_manifest_failure_reason"] = STALE_MANIFEST_ALL_MISSING_REASON
    elif not surviving_entries and missing_count > 0:
        # 일부 파일은 이미 completed, 일부는 사라짐 → 실패로 보지 않고 idempotent 종료 허용
        # 단, 추적을 위해 부분 상태는 이미 manifest에 기록됨
        pass

    context.log.warning(
        "manifest reconciliation: "
        f"source_unit_path={source_unit_path}, "
        f"original_file_count={original_file_count}, "
        f"existing_file_count={len(surviving_entries)}, "
        f"missing_count={missing_count}, "
        f"already_completed_count={already_completed_count}, "
        f"missing_sample={missing_rel_paths[:5]}, "
        f"already_completed_sample={already_completed_rel_paths[:5]}"
    )
    return manifest


def hydrate_manifest_files(context, manifest: dict, *, db=None) -> dict:
    """files가 비어 있으면 source_unit_path를 기준으로 실제 파일 목록을 지연 생성한다."""
    source_unit_path_raw = str(manifest.get("source_unit_path", "")).strip()
    source_unit_path = Path(source_unit_path_raw) if source_unit_path_raw else None
    if source_unit_path is None or not source_unit_path.exists():
        return manifest

    if manifest.get("files"):
        if is_gcp_auto_bootstrap_directory_manifest(manifest, source_unit_path=source_unit_path):
            return reconcile_manifest_files_against_disk(
                context,
                manifest,
                source_unit_path=source_unit_path,
                db=db,
            )
        return manifest

    files = scan_manifest_source_unit_files(source_unit_path)
    manifest["files"] = files
    manifest["file_count"] = len(files)
    manifest["source_unit_total_file_count"] = len(files)
    context.log.info(
        "manifest 파일 인덱싱 완료: "
        f"source_unit_path={source_unit_path}, file_count={len(files)}"
    )
    return manifest


def manifest_hydration_failure_reason(manifest: dict | None) -> str | None:
    if not manifest:
        return None
    failure_reason = str(manifest.get("stale_manifest_failure_reason", "")).strip()
    return failure_reason or None


def raise_if_manifest_hydration_failed(context, manifest: dict | None, ingest_rejections: list[dict]) -> None:
    failure_reason = manifest_hydration_failure_reason(manifest)
    if not failure_reason or manifest is None:
        return

    ingest_rejections.append(
        {
            "source_path": str(manifest.get("source_unit_path", "")).strip(),
            "rel_path": "",
            "media_type": "unknown",
            "stage": "manifest_hydrate",
            "error_code": failure_reason,
            "error_message": failure_reason,
            "retryable": False,
        }
    )
    context.log.error(
        "manifest hydrate failed before register: "
        f"reason={failure_reason}, "
        f"source_unit_path={manifest.get('source_unit_path', '')}"
    )
    raise RuntimeError(failure_reason)
