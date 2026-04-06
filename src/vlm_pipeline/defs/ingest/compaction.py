"""INGEST manifest compaction helpers for completed GCP auto-bootstrap units."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from .archive import find_existing_archive_directory, resolve_archive_source_unit_name
from .hydration import GCP_AUTO_BOOTSTRAP_TRANSFER_TOOLS
from .manifest import DONE_MARKER_FILENAME, sanitize_manifest_identifier

GCP_COMPACTION_TRANSFER_TOOLS = GCP_AUTO_BOOTSTRAP_TRANSFER_TOOLS


def is_gcp_compaction_candidate(payload: dict) -> bool:
    transfer_tool = str(payload.get("transfer_tool", "")).strip().lower()
    if transfer_tool not in GCP_COMPACTION_TRANSFER_TOOLS:
        return False
    if str(payload.get("source_unit_type", "")).strip().lower() != "directory":
        return False

    source_unit_name = str(payload.get("source_unit_name", "")).strip().lower()
    if source_unit_name.startswith("gcp/"):
        return True

    source_unit_path = str(payload.get("source_unit_path", "")).replace("\\", "/").rstrip("/")
    return "/incoming/gcp/" in source_unit_path or source_unit_path.endswith("/incoming/gcp")


def load_manifest_payload(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def iter_processed_manifest_records(processed_dir: Path) -> list[dict]:
    records: list[dict] = []
    if not processed_dir.exists():
        return records
    for path in sorted(processed_dir.glob("*.json")):
        payload = load_manifest_payload(path)
        if payload is None or not is_gcp_compaction_candidate(payload):
            continue
        records.append(
            {
                "path": path,
                "payload": payload,
                "source_unit_path": str(payload.get("source_unit_path", "")).strip(),
                "stable_signature": str(payload.get("stable_signature", "")).strip(),
                "source_unit_name": str(payload.get("source_unit_name", "")).strip(),
            }
        )
    return records


def collect_processed_manifest_groups(processed_dir: Path) -> dict[tuple[str, str], list[dict]]:
    groups: dict[tuple[str, str], list[dict]] = {}
    for record in iter_processed_manifest_records(processed_dir):
        key = (record["source_unit_path"], record["stable_signature"])
        groups.setdefault(key, []).append(record)
    return groups


def build_pending_manifest_index(manifest_dir: Path) -> dict[tuple[str, str], bool]:
    """pending 디렉토리를 한 번 스캔하여 (source_unit_path, stable_signature) 인덱스를 구축한다."""
    pending_dir = manifest_dir / "pending"
    index: dict[tuple[str, str], bool] = {}
    if not pending_dir.exists():
        return index
    for path in pending_dir.glob("*.json"):
        payload = load_manifest_payload(path)
        if payload is None or not is_gcp_compaction_candidate(payload):
            continue
        key = (
            str(payload.get("source_unit_path", "")).strip(),
            str(payload.get("stable_signature", "")).strip(),
        )
        index[key] = True
    return index


def has_pending_manifest_for_group(
    manifest_dir: Path,
    *,
    source_unit_path: str,
    stable_signature: str,
    pending_index: dict[tuple[str, str], bool] | None = None,
) -> bool:
    if pending_index is not None:
        return pending_index.get((source_unit_path, stable_signature), False)
    pending_dir = manifest_dir / "pending"
    if not pending_dir.exists():
        return False
    for path in pending_dir.glob("*.json"):
        payload = load_manifest_payload(path)
        if payload is None or not is_gcp_compaction_candidate(payload):
            continue
        if str(payload.get("source_unit_path", "")).strip() != source_unit_path:
            continue
        payload_signature = str(payload.get("stable_signature", "")).strip()
        if stable_signature and payload_signature != stable_signature:
            continue
        return True
    return False


def resolve_completed_summary_path(manifest_dir: Path, *, source_unit_name: str, stable_signature: str) -> Path:
    completed_dir = manifest_dir / "completed"
    completed_dir.mkdir(parents=True, exist_ok=True)
    source_slug = sanitize_manifest_identifier(source_unit_name, fallback="unknown_source_unit")
    signature_hash = hashlib.sha1(str(stable_signature or "").encode("utf-8")).hexdigest()[:12]
    return completed_dir / f"{source_slug}__{signature_hash}.json"


def resolve_archive_done_marker_path(archive_dir: Path, *, source_unit_name: str) -> Path | None:
    archive_unit_name = resolve_archive_source_unit_name(source_unit_name) or source_unit_name
    if not archive_unit_name:
        return None
    base_dir = archive_dir / archive_unit_name
    archive_unit_dir = find_existing_archive_directory(base_dir)
    if archive_unit_dir is None:
        return None
    marker_path = archive_unit_dir / DONE_MARKER_FILENAME
    return marker_path if marker_path.exists() else None


def build_completed_summary_payload(
    records: list[dict],
    *,
    archive_done_marker_path: Path,
) -> dict:
    payloads = [record["payload"] for record in records]
    source_unit_name = str(payloads[0].get("source_unit_name", "")).strip()
    source_unit_path = str(payloads[0].get("source_unit_path", "")).strip()
    stable_signature = str(payloads[0].get("stable_signature", "")).strip()
    transfer_tools = sorted(
        {
            str(payload.get("transfer_tool", "")).strip()
            for payload in payloads
            if str(payload.get("transfer_tool", "")).strip()
        }
    )
    chunk_count = max(
        1,
        max(int(payload.get("source_unit_chunk_count") or 1) for payload in payloads),
    )
    total_file_count = max(
        int(payload.get("source_unit_total_file_count") or payload.get("file_count") or 0)
        for payload in payloads
    )
    compacted_manifest_ids = sorted(
        {
            str(payload.get("manifest_id") or record["path"].stem).strip()
            for record, payload in zip(records, payloads)
        }
    )

    return {
        "summary_type": "gcp_completed_manifest_compaction",
        "source_unit_name": source_unit_name,
        "source_unit_path": source_unit_path,
        "stable_signature": stable_signature,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "transfer_tools": transfer_tools,
        "chunk_count": chunk_count,
        "processed_manifest_count": len(records),
        "total_file_count": total_file_count,
        "archive_done_marker_path": str(archive_done_marker_path),
        "compacted_manifest_ids": compacted_manifest_ids,
    }


def compact_completed_manifest_group(
    *,
    manifest_dir: Path,
    archive_dir: Path,
    source_unit_path: str,
    stable_signature: str,
    archive_done_marker_path: Path | None = None,
    apply: bool,
) -> dict:
    processed_groups = collect_processed_manifest_groups(manifest_dir / "processed")
    records = processed_groups.get((source_unit_path, stable_signature), [])
    if not records and not stable_signature:
        for (candidate_path, _candidate_signature), candidate_records in processed_groups.items():
            if candidate_path == source_unit_path:
                records = candidate_records
                break
    return compact_completed_manifest_records(
        manifest_dir=manifest_dir,
        archive_dir=archive_dir,
        records=records,
        source_unit_path=source_unit_path,
        stable_signature=stable_signature,
        archive_done_marker_path=archive_done_marker_path,
        apply=apply,
    )


def compact_completed_manifest_records(
    *,
    manifest_dir: Path,
    archive_dir: Path,
    records: list[dict],
    source_unit_path: str,
    stable_signature: str,
    archive_done_marker_path: Path | None = None,
    apply: bool,
    pending_index: dict[tuple[str, str], bool] | None = None,
) -> dict:
    if not records:
        return {
            "status": "noop",
            "reason": "no_processed_manifests",
            "processed_manifest_count": 0,
            "deleted_manifest_count": 0,
            "summary_written": False,
        }

    source_unit_name = records[0]["source_unit_name"]
    if has_pending_manifest_for_group(
        manifest_dir,
        source_unit_path=source_unit_path,
        stable_signature=stable_signature,
        pending_index=pending_index,
    ):
        return {
            "status": "skipped",
            "reason": "pending_manifest_exists",
            "source_unit_name": source_unit_name,
            "source_unit_path": source_unit_path,
            "stable_signature": stable_signature,
            "processed_manifest_count": len(records),
            "deleted_manifest_count": 0,
            "summary_written": False,
        }

    marker_path = archive_done_marker_path
    if marker_path is None:
        marker_path = resolve_archive_done_marker_path(
            archive_dir,
            source_unit_name=source_unit_name,
        )
    if marker_path is None or not marker_path.exists():
        return {
            "status": "skipped",
            "reason": "done_marker_missing",
            "source_unit_name": source_unit_name,
            "source_unit_path": source_unit_path,
            "stable_signature": stable_signature,
            "processed_manifest_count": len(records),
            "deleted_manifest_count": 0,
            "summary_written": False,
        }

    summary_path = resolve_completed_summary_path(
        manifest_dir,
        source_unit_name=source_unit_name,
        stable_signature=stable_signature,
    )
    summary_payload = build_completed_summary_payload(
        records,
        archive_done_marker_path=marker_path,
    )

    deleted_manifest_count = 0
    if apply:
        summary_path.write_text(
            json.dumps(summary_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        for record in records:
            record["path"].unlink(missing_ok=True)
            deleted_manifest_count += 1

    return {
        "status": "compacted",
        "reason": "ok",
        "source_unit_name": source_unit_name,
        "source_unit_path": source_unit_path,
        "stable_signature": stable_signature,
        "processed_manifest_count": len(records),
        "deleted_manifest_count": deleted_manifest_count if apply else len(records),
        "summary_written": True,
        "summary_path": str(summary_path),
    }


def discover_compactable_manifest_groups(*, manifest_dir: Path, archive_dir: Path) -> list[dict]:
    groups = collect_processed_manifest_groups(manifest_dir / "processed")
    pending_index = build_pending_manifest_index(manifest_dir)

    reports: list[dict] = []
    for (_source_unit_path, _stable_signature), records in sorted(
        groups.items(),
        key=lambda item: (item[1][0]["source_unit_name"], item[1][0]["stable_signature"]),
    ):
        first = records[0]
        reports.append(
            compact_completed_manifest_records(
                manifest_dir=manifest_dir,
                archive_dir=archive_dir,
                records=records,
                source_unit_path=first["source_unit_path"],
                stable_signature=first["stable_signature"],
                apply=False,
                pending_index=pending_index,
            )
        )
    return reports
