"""raw_ingest — manifest 로드 / hydrate / 이동 흐름."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from .hydration import (
    hydrate_manifest_files,
    raise_if_manifest_hydration_failed as check_manifest_hydration_failure,
)
from .ingest_state import RawIngestState

if TYPE_CHECKING:
    from vlm_pipeline.resources.config import PipelineConfig
    from vlm_pipeline.resources.duckdb import DuckDBResource


def _persist_manifest(manifest_path: str | None, manifest: dict | None) -> None:
    if not manifest_path or manifest is None:
        return
    Path(manifest_path).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_manifest_or_summary(
    context,
    db: "DuckDBResource",
    *,
    manifest_path: str | None,
    request_id: str | None,
) -> tuple[dict | None, dict | None]:
    if manifest_path and Path(manifest_path).exists():
        return json.loads(Path(manifest_path).read_text(encoding="utf-8")), None

    message = "manifest_path가 없거나 파일이 존재하지 않습니다."
    if request_id:
        raise FileNotFoundError(message)

    context.log.warning(f"{message} 상태 요약만 반환합니다.")
    with db.connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM raw_files").fetchone()[0]
        completed = conn.execute(
            "SELECT COUNT(*) FROM raw_files WHERE ingest_status = 'completed'"
        ).fetchone()[0]
    return None, {"total": int(total), "success": int(completed), "failed": 0, "skipped": 0}


def _hydrate_manifest(context, state: RawIngestState, *, db=None) -> None:
    if state.manifest is None:
        return
    context.log.info("manifest_hydrate:start")
    state.manifest = hydrate_manifest_files(context, state.manifest, db=db)
    _persist_manifest(state.manifest_path, state.manifest)
    file_count = int(state.manifest.get("file_count") or len(state.manifest.get("files", [])) or 0)
    already_completed = int(state.manifest.get("already_completed_file_count") or 0)
    log_suffix = f" already_completed={already_completed}" if already_completed else ""
    context.log.info(
        f"manifest_hydrate:done file_count={file_count}{log_suffix}"
    )


def _raise_if_manifest_hydration_failed(context, state: RawIngestState) -> None:
    check_manifest_hydration_failure(context, state.manifest, state.ingest_rejections)


def _move_manifest(
    context,
    config: "PipelineConfig",
    manifest_path: str | None,
    *,
    target_subdir: str,
    unique_name: bool = False,
) -> None:
    """manifest 파일을 target_subdir 하위로 이동한다."""
    if not manifest_path:
        return
    manifest_file = Path(manifest_path)
    if not manifest_file.exists():
        return
    target_dir = Path(config.manifest_dir) / target_subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    destination = target_dir / manifest_file.name
    if unique_name:
        suffix = 2
        while destination.exists():
            destination = target_dir / f"{manifest_file.stem}__{suffix}{manifest_file.suffix}"
            suffix += 1
    try:
        shutil.move(str(manifest_file), str(destination))
        context.log.info(f"manifest 이동 완료: {manifest_file.name} -> {target_subdir}/{destination.name}")
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"manifest 이동 실패: {exc}")


def _move_manifest_to_processed(context, config: "PipelineConfig", manifest_path: str | None) -> None:
    _move_manifest(context, config, manifest_path, target_subdir="processed")


def _move_manifest_to_failed(context, config: "PipelineConfig", manifest_path: str | None) -> None:
    _move_manifest(context, config, manifest_path, target_subdir="failed", unique_name=True)
