from __future__ import annotations

import json
from pathlib import Path

from vlm_pipeline.defs.ingest.compaction import (
    compact_completed_manifest_group,
    discover_compactable_manifest_groups,
    resolve_completed_summary_path,
)


def _write_manifest(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _gcp_payload(
    *,
    manifest_id: str,
    source_unit_name: str = "gcp/source-c-event-bucket/20260402",
    source_unit_path: str = "/nas/incoming/gcp/source-c-event-bucket/20260402",
    stable_signature: str = "337:755754529:1775151249000000000",
    chunk_index: int = 1,
    chunk_count: int = 4,
    transfer_tool: str = "auto_bootstrap_sensor",
) -> dict:
    return {
        "manifest_id": manifest_id,
        "source_unit_type": "directory",
        "source_unit_name": source_unit_name,
        "source_unit_path": source_unit_path,
        "stable_signature": stable_signature,
        "source_unit_chunk_index": chunk_index,
        "source_unit_chunk_count": chunk_count,
        "source_unit_total_file_count": 337,
        "file_count": 100,
        "transfer_tool": transfer_tool,
        "files": [],
    }


def test_compact_completed_manifest_group_writes_summary_and_deletes_processed(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "manifests"
    processed_dir = manifest_dir / "processed"
    archive_dir = tmp_path / "archive"
    archive_unit_dir = archive_dir / "source-c-event-bucket" / "20260402"
    archive_unit_dir.mkdir(parents=True)
    (archive_unit_dir / "_DONE").write_text("done", encoding="utf-8")

    payload1 = _gcp_payload(manifest_id="m1", chunk_index=1)
    payload2 = _gcp_payload(manifest_id="m2", chunk_index=2)
    _write_manifest(processed_dir / "m1.json", payload1)
    _write_manifest(processed_dir / "m2.superseded.json", payload2)

    report = compact_completed_manifest_group(
        manifest_dir=manifest_dir,
        archive_dir=archive_dir,
        source_unit_path=payload1["source_unit_path"],
        stable_signature=payload1["stable_signature"],
        apply=True,
    )

    summary_path = resolve_completed_summary_path(
        manifest_dir,
        source_unit_name=payload1["source_unit_name"],
        stable_signature=payload1["stable_signature"],
    )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert report["status"] == "compacted"
    assert report["deleted_manifest_count"] == 2
    assert summary["source_unit_name"] == payload1["source_unit_name"]
    assert summary["processed_manifest_count"] == 2
    assert summary["chunk_count"] == 4
    assert summary["archive_done_marker_path"].endswith("_DONE")
    assert sorted(summary["compacted_manifest_ids"]) == ["m1", "m2"]
    assert list(processed_dir.glob("*.json")) == []


def test_compact_completed_manifest_group_skips_when_pending_exists(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "manifests"
    processed_dir = manifest_dir / "processed"
    pending_dir = manifest_dir / "pending"
    archive_dir = tmp_path / "archive"
    archive_unit_dir = archive_dir / "source-c-event-bucket" / "20260402"
    archive_unit_dir.mkdir(parents=True)
    (archive_unit_dir / "_DONE").write_text("done", encoding="utf-8")

    payload = _gcp_payload(manifest_id="m1")
    _write_manifest(processed_dir / "m1.json", payload)
    _write_manifest(pending_dir / "m2.json", _gcp_payload(manifest_id="m2"))

    report = compact_completed_manifest_group(
        manifest_dir=manifest_dir,
        archive_dir=archive_dir,
        source_unit_path=payload["source_unit_path"],
        stable_signature=payload["stable_signature"],
        apply=True,
    )

    assert report["status"] == "skipped"
    assert report["reason"] == "pending_manifest_exists"
    assert (processed_dir / "m1.json").exists()


def test_compact_completed_manifest_group_skips_when_done_marker_missing(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "manifests"
    processed_dir = manifest_dir / "processed"
    archive_dir = tmp_path / "archive"
    payload = _gcp_payload(manifest_id="m1")
    _write_manifest(processed_dir / "m1.json", payload)

    report = compact_completed_manifest_group(
        manifest_dir=manifest_dir,
        archive_dir=archive_dir,
        source_unit_path=payload["source_unit_path"],
        stable_signature=payload["stable_signature"],
        apply=True,
    )

    assert report["status"] == "skipped"
    assert report["reason"] == "done_marker_missing"


def test_discover_compactable_manifest_groups_ignores_non_gcp(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "manifests"
    processed_dir = manifest_dir / "processed"
    archive_dir = tmp_path / "archive"
    archive_unit_dir = archive_dir / "source-c-event-bucket" / "20260402"
    archive_unit_dir.mkdir(parents=True)
    (archive_unit_dir / "_DONE").write_text("done", encoding="utf-8")

    _write_manifest(processed_dir / "gcp.json", _gcp_payload(manifest_id="gcp"))
    _write_manifest(
        processed_dir / "manual.json",
        {
            "manifest_id": "manual",
            "source_unit_type": "directory",
            "source_unit_name": "tmp_data_2",
            "source_unit_path": "/nas/incoming/tmp_data_2",
            "stable_signature": "sig",
            "transfer_tool": "dispatch_sensor",
        },
    )

    reports = discover_compactable_manifest_groups(
        manifest_dir=manifest_dir,
        archive_dir=archive_dir,
    )

    assert len(reports) == 1
    assert reports[0]["source_unit_name"] == "gcp/source-c-event-bucket/20260402"


def test_compact_completed_manifest_group_is_idempotent_after_apply(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "manifests"
    processed_dir = manifest_dir / "processed"
    archive_dir = tmp_path / "archive"
    archive_unit_dir = archive_dir / "source-c-event-bucket" / "20260402"
    archive_unit_dir.mkdir(parents=True)
    (archive_unit_dir / "_DONE").write_text("done", encoding="utf-8")

    payload = _gcp_payload(manifest_id="m1")
    _write_manifest(processed_dir / "m1.json", payload)

    first = compact_completed_manifest_group(
        manifest_dir=manifest_dir,
        archive_dir=archive_dir,
        source_unit_path=payload["source_unit_path"],
        stable_signature=payload["stable_signature"],
        apply=True,
    )
    second = compact_completed_manifest_group(
        manifest_dir=manifest_dir,
        archive_dir=archive_dir,
        source_unit_path=payload["source_unit_path"],
        stable_signature=payload["stable_signature"],
        apply=True,
    )

    assert first["status"] == "compacted"
    assert second["status"] == "noop"
    assert second["reason"] == "no_processed_manifests"
