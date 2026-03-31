"""INGEST manifest 유틸리티 — manifest 진행/실패/재시도 관련 함수들.

assets.py에서 분리된 manifest 관련 헬퍼.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from vlm_pipeline.lib.env_utils import as_int
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.defs.ingest.archive import resolve_archive_source_unit_name

DONE_MARKER_FILENAME = "_DONE"


def sanitize_manifest_identifier(raw: str, fallback: str = "unknown_manifest") -> str:
    value = str(raw or "").strip()
    if not value:
        return fallback
    normalized = [ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in value]
    collapsed = "".join(normalized).strip("_")
    return collapsed or fallback


def resolve_failure_log_dir(config: PipelineConfig) -> Path:
    configured = str(os.getenv("INGEST_FAILURE_LOG_DIR", "")).strip()
    if configured:
        return Path(configured)
    return Path(config.manifest_dir) / "failed"


def load_manifest_summary(manifest_path: Path) -> dict | None:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    return {
        "manifest_id": str(payload.get("manifest_id") or manifest_path.stem).strip(),
        "source_unit_path": str(payload.get("source_unit_path", "")).strip(),
        "stable_signature": str(payload.get("stable_signature", "")).strip(),
        "chunk_index": max(1, as_int(payload.get("source_unit_chunk_index"), 1)),
        "chunk_count": max(1, as_int(payload.get("source_unit_chunk_count"), 1)),
    }


def matches_source_unit_manifest(
    manifest_summary: dict,
    *,
    source_unit_path: str,
    stable_signature: str,
) -> bool:
    if str(manifest_summary.get("source_unit_path", "")).strip() != source_unit_path:
        return False
    manifest_signature = str(manifest_summary.get("stable_signature", "")).strip()
    if stable_signature:
        return manifest_signature == stable_signature
    return True


def collect_source_unit_manifest_progress(
    *,
    manifest_dir: Path,
    source_unit_path: str,
    stable_signature: str,
    current_manifest_path: Path | None,
    current_manifest_id: str,
    current_chunk_index: int,
    current_chunk_count: int,
) -> tuple[set[int], bool, set[str], int]:
    processed_chunks = {max(1, current_chunk_index)}
    manifest_ids = {current_manifest_id} if current_manifest_id else set()
    expected_chunk_count = max(1, current_chunk_count)
    has_pending_manifest = False

    current_manifest_abs = current_manifest_path.resolve() if current_manifest_path else None
    for state_name in ("processed", "pending"):
        state_dir = manifest_dir / state_name
        if not state_dir.exists():
            continue
        for candidate in sorted(state_dir.glob("*.json")):
            try:
                candidate_abs = candidate.resolve()
            except OSError:
                candidate_abs = candidate
            if current_manifest_abs is not None and candidate_abs == current_manifest_abs:
                continue

            summary = load_manifest_summary(candidate)
            if summary is None:
                continue
            if not matches_source_unit_manifest(
                summary,
                source_unit_path=source_unit_path,
                stable_signature=stable_signature,
            ):
                continue

            expected_chunk_count = max(expected_chunk_count, int(summary["chunk_count"]))
            manifest_id = str(summary.get("manifest_id", "")).strip()
            if manifest_id:
                manifest_ids.add(manifest_id)

            if state_name == "processed":
                processed_chunks.add(int(summary["chunk_index"]))
            else:
                has_pending_manifest = True

    return processed_chunks, has_pending_manifest, manifest_ids, expected_chunk_count


def has_manifest_failure_logs(manifest_dir: Path, manifest_ids: set[str]) -> bool:
    failed_dir = manifest_dir / "failed"
    if not failed_dir.exists():
        return False

    for manifest_id in manifest_ids:
        if not manifest_id:
            continue
        log_path = failed_dir / f"{manifest_id}.jsonl"
        try:
            if log_path.exists() and log_path.stat().st_size > 0:
                return True
        except OSError:
            continue
    return False


def archive_dir_has_payload_files(archive_unit_dir: Path) -> bool:
    try:
        return any(
            path.is_file() and path.name != DONE_MARKER_FILENAME
            for path in archive_unit_dir.rglob("*")
        )
    except OSError:
        return False


def write_done_marker(marker_path: Path) -> None:
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(
        f"completed_at={datetime.now(timezone.utc).isoformat()}\n",
        encoding="utf-8",
    )


def write_ingest_failure_logs(
    context,
    config: PipelineConfig,
    manifest: dict,
    ingest_rejections: list[dict],
) -> Path | None:
    if not ingest_rejections:
        return None

    manifest_id = sanitize_manifest_identifier(str(manifest.get("manifest_id", "")))
    try:
        failure_dir = resolve_failure_log_dir(config)
        failure_dir.mkdir(parents=True, exist_ok=True)
        log_path = failure_dir / f"{manifest_id}.jsonl"

        with log_path.open("a", encoding="utf-8") as fh:
            for row in ingest_rejections:
                payload = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "manifest_id": str(manifest.get("manifest_id", "")),
                    "source_path": str(row.get("source_path", "")),
                    "rel_path": str(row.get("rel_path", "")),
                    "media_type": str(row.get("media_type", "unknown")),
                    "stage": str(row.get("stage", "unknown")),
                    "error_code": str(row.get("error_code", "unknown_error")),
                    "error_message": str(row.get("error_message", "")),
                    "retryable": bool(row.get("retryable", False)),
                }
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

        context.log.warning(
            f"ingest 실패 로그 기록 완료: {log_path} (count={len(ingest_rejections)})"
        )
        return log_path
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"ingest 실패 로그 기록 실패: {exc}")
        return None


def build_retry_manifest(
    context,
    config: PipelineConfig,
    manifest: dict,
    retry_candidates: list[dict],
) -> Path | None:
    if not retry_candidates:
        return None

    max_attempt = max(1, int(os.getenv("INGEST_TRANSIENT_RETRY_MAX_ATTEMPTS", "3")))
    try:
        current_attempt = int(manifest.get("retry_attempt") or 0)
    except (TypeError, ValueError):
        current_attempt = 0
    if current_attempt >= max_attempt:
        context.log.warning(
            "retry manifest 생성 중단: retry 한도 초과 "
            f"(manifest_id={manifest.get('manifest_id')}, current={current_attempt}, max={max_attempt})"
        )
        return None

    dedup: dict[str, dict] = {}
    for row in retry_candidates:
        source_path = str(row.get("source_path", "")).strip()
        if not source_path:
            continue
        rel_path = str(row.get("rel_path", "")).strip() or Path(source_path).name
        media_type = str(row.get("media_type", "unknown")).strip() or "unknown"
        file_size = None
        try:
            if Path(source_path).exists():
                file_size = int(Path(source_path).stat().st_size)
        except OSError:
            file_size = None
        dedup[source_path] = {
            "path": source_path,
            "size": file_size,
            "rel_path": rel_path,
            "media_type": media_type,
        }

    files = [dedup[key] for key in sorted(dedup)]
    if not files:
        return None

    root_manifest_id = str(
        manifest.get("retry_of_manifest_id")
        or manifest.get("manifest_id")
        or "unknown_manifest"
    )
    root_manifest_slug = sanitize_manifest_identifier(root_manifest_id)
    next_attempt = current_attempt + 1
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    retry_manifest_id = f"retry_{root_manifest_slug}_{timestamp}"

    pending_dir = Path(config.manifest_dir) / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    retry_manifest_path = pending_dir / f"{retry_manifest_id}.json"
    suffix = 2
    while retry_manifest_path.exists():
        retry_manifest_path = pending_dir / f"{retry_manifest_id}__{suffix}.json"
        suffix += 1

    source_unit_dispatch_key = str(
        manifest.get("source_unit_dispatch_key")
        or manifest.get("source_unit_path")
        or ""
    ).strip()
    if source_unit_dispatch_key:
        source_unit_dispatch_key = f"{source_unit_dispatch_key}#retry:{next_attempt:02d}"

    payload = {
        "manifest_id": retry_manifest_path.stem,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_dir": manifest.get("source_dir", config.incoming_dir),
        "source_unit_type": manifest.get("source_unit_type", "directory"),
        "source_unit_path": manifest.get("source_unit_path", ""),
        "source_unit_name": manifest.get("source_unit_name", ""),
        "source_unit_dispatch_key": source_unit_dispatch_key,
        "source_unit_total_file_count": len(files),
        "source_unit_chunk_index": 1,
        "source_unit_chunk_count": 1,
        "stable_signature": manifest.get("stable_signature", ""),
        "transfer_tool": "ingest_retry_manifest",
        "file_count": len(files),
        "files": files,
        "retry_of_manifest_id": root_manifest_id,
        "retry_attempt": next_attempt,
        "retry_reason": "transient_db_lock",
    }
    if "archive_requested" in manifest:
        payload["archive_requested"] = manifest.get("archive_requested")

    retry_manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    context.log.warning(
        "transient 오류 재시도 manifest 생성: "
        f"{retry_manifest_path.name} (files={len(files)}, retry_attempt={next_attempt})"
    )
    return retry_manifest_path


def maybe_write_archive_done_marker(
    context,
    db: DuckDBResource,
    manifest: dict,
    *,
    manifest_dir: str,
    manifest_path: str | None,
    archive_dir: str,
    archive_unit_dir_hint: Path | None,
) -> Path | None:
    source_unit_type = str(manifest.get("source_unit_type", "")).strip().lower()
    source_unit_name = str(manifest.get("source_unit_name", "")).strip()
    archive_unit_name = resolve_archive_source_unit_name(source_unit_name) or source_unit_name
    source_unit_path = str(manifest.get("source_unit_path", "")).strip()
    stable_signature = str(manifest.get("stable_signature", "")).strip()
    current_manifest_id = str(manifest.get("manifest_id", "")).strip()
    current_chunk_index = max(1, as_int(manifest.get("source_unit_chunk_index"), 1))
    current_chunk_count = max(1, as_int(manifest.get("source_unit_chunk_count"), 1))

    if source_unit_type != "directory" or not source_unit_name:
        return None

    archive_root_dir = Path(archive_dir)
    archive_unit_dir = archive_unit_dir_hint
    default_archive_unit_dir = archive_root_dir / archive_unit_name
    if archive_unit_dir is None or not archive_unit_dir.exists():
        if default_archive_unit_dir.exists():
            archive_unit_dir = default_archive_unit_dir
    if archive_unit_dir is None or not archive_unit_dir.exists():
        return None

    manifest_root_dir = Path(manifest_dir)
    current_manifest_file = Path(manifest_path) if manifest_path else None
    processed_chunks, has_pending_manifest, manifest_ids, expected_chunk_count = (
        collect_source_unit_manifest_progress(
            manifest_dir=manifest_root_dir,
            source_unit_path=source_unit_path,
            stable_signature=stable_signature,
            current_manifest_path=current_manifest_file,
            current_manifest_id=current_manifest_id,
            current_chunk_index=current_chunk_index,
            current_chunk_count=current_chunk_count,
        )
    )

    if has_pending_manifest:
        context.log.info(
            f"archive _DONE 보류: 동일 source unit의 pending manifest가 남아 있습니다 (unit={source_unit_name})"
        )
        return None

    if len(processed_chunks) < expected_chunk_count:
        context.log.info(
            f"archive _DONE 보류: 아직 처리되지 않은 청크가 있습니다 "
            f"(unit={source_unit_name}, processed={len(processed_chunks)}/{expected_chunk_count})"
        )
        return None

    unresolved_count = db.count_unresolved_rows_for_source_unit(source_unit_path)
    if unresolved_count > 0:
        context.log.info(
            f"archive _DONE 보류: source unit 하위에 미해결 raw_files row가 남아 있습니다 "
            f"(unit={source_unit_name}, unresolved={unresolved_count})"
        )
        return None

    if has_manifest_failure_logs(manifest_root_dir, manifest_ids):
        context.log.info(
            f"archive _DONE 보류: 동일 source unit manifest에 실패 로그가 남아 있습니다 (unit={source_unit_name})"
        )
        return None

    if not archive_dir_has_payload_files(archive_unit_dir):
        context.log.info(
            f"archive _DONE 보류: archive unit 디렉토리에 payload 파일이 없습니다 "
            f"(unit={source_unit_name}, dir={archive_unit_dir})"
        )
        return None

    marker_path = archive_unit_dir / DONE_MARKER_FILENAME
    if marker_path.exists():
        return marker_path

    try:
        write_done_marker(marker_path)
        context.log.info(f"archive _DONE marker 생성 완료: {marker_path}")
        return marker_path
    except OSError as exc:
        context.log.warning(f"archive _DONE marker 생성 실패: {marker_path}: {exc}")
        return None
