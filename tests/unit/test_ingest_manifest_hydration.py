from __future__ import annotations

import json
from pathlib import Path

import pytest

from vlm_pipeline.defs.ingest.hydration import (
    STALE_MANIFEST_ALL_MISSING_REASON,
    hydrate_manifest_files,
    raise_if_manifest_hydration_failed,
)


class _DummyLog:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def info(self, message: str, *args) -> None:
        rendered = message % args if args else message
        self.infos.append(rendered)

    def warning(self, message: str, *args) -> None:
        rendered = message % args if args else message
        self.warnings.append(rendered)

    def error(self, message: str, *args) -> None:
        rendered = message % args if args else message
        self.errors.append(rendered)


class _DummyState:
    def __init__(self, manifest: dict) -> None:
        self.manifest = manifest
        self.ingest_rejections: list[dict] = []


class _DummyContext:
    def __init__(self) -> None:
        self.log = _DummyLog()


def test_hydrate_manifest_files_reconciles_existing_subset_for_stale_gcp_manifest(tmp_path: Path) -> None:
    source_unit = tmp_path / "incoming" / "gcp" / "source-c-event-bucket" / "20260402"
    source_unit.mkdir(parents=True)
    existing_file = source_unit / "violence_22_5.mp4"
    existing_file.write_bytes(b"video")

    missing_file = source_unit / "violence_22_503.mp4"
    context = _DummyContext()
    manifest = {
        "transfer_tool": "auto_bootstrap_sensor",
        "source_unit_type": "directory",
        "source_unit_name": "gcp/source-c-event-bucket/20260402",
        "source_unit_path": str(source_unit),
        "file_count": 2,
        "source_unit_total_file_count": 337,
        "files": [
            {"path": str(existing_file), "size": 0, "rel_path": "violence_22_5.mp4"},
            {"path": str(missing_file), "size": 0, "rel_path": "violence_22_503.mp4"},
        ],
    }

    hydrated = hydrate_manifest_files(context, manifest)

    assert hydrated["file_count"] == 1
    assert hydrated["source_unit_total_file_count"] == 337
    assert hydrated["stale_manifest_detected"] is True
    assert hydrated["original_file_count"] == 2
    assert hydrated["existing_file_count"] == 1
    assert hydrated["missing_entry_count"] == 1
    assert hydrated["stale_missing_samples"] == ["violence_22_503.mp4"]
    assert hydrated["files"] == [
        {
            "path": str(existing_file),
            "size": len(b"video"),
            "rel_path": "violence_22_5.mp4",
        }
    ]
    assert any("missing_count=1" in message for message in context.log.warnings)


def test_hydrate_manifest_files_marks_all_missing_for_stale_gcp_manifest(tmp_path: Path) -> None:
    source_unit = tmp_path / "incoming" / "gcp" / "source-c-event-bucket" / "20260402"
    source_unit.mkdir(parents=True)

    context = _DummyContext()
    manifest = {
        "transfer_tool": "auto_bootstrap_sensor",
        "source_unit_type": "directory",
        "source_unit_name": "gcp/source-c-event-bucket/20260402",
        "source_unit_path": str(source_unit),
        "file_count": 1,
        "files": [
            {
                "path": str(source_unit / "violence_22_503.mp4"),
                "size": 0,
                "rel_path": "violence_22_503.mp4",
            }
        ],
    }

    hydrated = hydrate_manifest_files(context, manifest)

    assert hydrated["file_count"] == 0
    assert hydrated["files"] == []
    assert hydrated["stale_manifest_failure_reason"] == STALE_MANIFEST_ALL_MISSING_REASON
    assert hydrated["missing_entry_count"] == 1


def test_raise_if_manifest_hydration_failed_stops_before_register() -> None:
    context = _DummyContext()
    state = _DummyState(
        {
            "source_unit_path": "/nas/incoming/gcp/source-c-event-bucket/20260402",
            "stale_manifest_failure_reason": STALE_MANIFEST_ALL_MISSING_REASON,
        }
    )

    with pytest.raises(RuntimeError, match=STALE_MANIFEST_ALL_MISSING_REASON):
        raise_if_manifest_hydration_failed(context, state.manifest, state.ingest_rejections)

    assert state.ingest_rejections == [
        {
            "source_path": "/nas/incoming/gcp/source-c-event-bucket/20260402",
            "rel_path": "",
            "media_type": "unknown",
            "stage": "manifest_hydrate",
            "error_code": STALE_MANIFEST_ALL_MISSING_REASON,
            "error_message": STALE_MANIFEST_ALL_MISSING_REASON,
            "retryable": False,
        }
    ]
    assert any(STALE_MANIFEST_ALL_MISSING_REASON in message for message in context.log.errors)


def test_hydrate_manifest_files_scans_directory_when_files_empty(tmp_path: Path) -> None:
    source_unit = tmp_path / "incoming" / "gcp" / "source-c-event-bucket" / "20260402"
    source_unit.mkdir(parents=True)
    (source_unit / "violence_22_5.mp4").write_bytes(b"video")
    (source_unit / "violence_22_6.mov").write_bytes(b"video-2")
    (source_unit / "ignore.txt").write_text("skip", encoding="utf-8")

    context = _DummyContext()
    manifest = {
        "transfer_tool": "auto_bootstrap_sensor",
        "source_unit_type": "directory",
        "source_unit_name": "gcp/source-c-event-bucket/20260402",
        "source_unit_path": str(source_unit),
        "files": [],
    }

    hydrated = hydrate_manifest_files(context, manifest)

    assert hydrated["file_count"] == 2
    assert sorted(entry["rel_path"] for entry in hydrated["files"]) == [
        "violence_22_5.mp4",
        "violence_22_6.mov",
    ]
    assert "stale_manifest_detected" not in hydrated


def test_hydrate_manifest_files_filters_macos_metadata(tmp_path: Path) -> None:
    source_unit = tmp_path / "incoming" / "sourcej(VHC)"
    source_unit.mkdir(parents=True)
    (source_unit / "CAM1.mp4").write_bytes(b"video")
    # AppleDouble 메타: 원본과 동일 확장자 → 기존 확장자 필터를 통과
    (source_unit / "._CAM1.mp4").write_bytes(b"apple-double")
    (source_unit / ".DS_Store").write_bytes(b"finder")
    (source_unit / "._.DS_Store").write_bytes(b"apple-double-for-ds-store")

    context = _DummyContext()
    manifest = {
        "transfer_tool": "dispatch_sensor",
        "source_unit_type": "directory",
        "source_unit_name": "sourcej(VHC)",
        "source_unit_path": str(source_unit),
        "files": [],
    }

    hydrated = hydrate_manifest_files(context, manifest)

    assert hydrated["file_count"] == 1
    assert [entry["rel_path"] for entry in hydrated["files"]] == ["CAM1.mp4"]


class _DummyDB:
    def __init__(self, completed_paths: set[str]) -> None:
        self.completed_paths = set(completed_paths)
        self.queries: list[list[str]] = []

    def find_completed_source_paths(self, source_paths: list[str]) -> set[str]:
        self.queries.append(list(source_paths))
        return {p for p in source_paths if p in self.completed_paths}


def test_hydrate_manifest_files_drops_already_completed_sources(tmp_path: Path) -> None:
    source_unit = tmp_path / "incoming" / "gcp" / "source-c-event-bucket" / "20260409"
    source_unit.mkdir(parents=True)
    completed_file = source_unit / "smoke_28_693.mp4"
    completed_file.write_bytes(b"video-1")
    pending_file = source_unit / "smoke_28_999.mp4"
    pending_file.write_bytes(b"video-2")

    context = _DummyContext()
    db = _DummyDB(completed_paths={str(completed_file)})
    manifest = {
        "transfer_tool": "auto_bootstrap_sensor",
        "source_unit_type": "directory",
        "source_unit_name": "gcp/source-c-event-bucket/20260409",
        "source_unit_path": str(source_unit),
        "file_count": 2,
        "files": [
            {"path": str(completed_file), "size": 0, "rel_path": "smoke_28_693.mp4"},
            {"path": str(pending_file), "size": 0, "rel_path": "smoke_28_999.mp4"},
        ],
    }

    hydrated = hydrate_manifest_files(context, manifest, db=db)

    assert hydrated["file_count"] == 1
    assert hydrated["files"][0]["rel_path"] == "smoke_28_999.mp4"
    assert hydrated["already_completed_file_count"] == 1
    assert hydrated["already_completed_samples"] == ["smoke_28_693.mp4"]
    assert hydrated["stale_manifest_detected"] is True
    assert hydrated.get("stale_manifest_failure_reason") is None
    assert any("already_completed_count=1" in w for w in context.log.warnings)


def test_hydrate_manifest_files_all_completed_is_idempotent_success(tmp_path: Path) -> None:
    source_unit = tmp_path / "incoming" / "gcp" / "source-c-event-bucket" / "20260409"
    source_unit.mkdir(parents=True)
    f1 = source_unit / "a.mp4"
    f1.write_bytes(b"x")
    f2 = source_unit / "b.mp4"
    f2.write_bytes(b"y")

    context = _DummyContext()
    db = _DummyDB(completed_paths={str(f1), str(f2)})
    manifest = {
        "transfer_tool": "auto_bootstrap_sensor",
        "source_unit_type": "directory",
        "source_unit_name": "gcp/source-c-event-bucket/20260409",
        "source_unit_path": str(source_unit),
        "file_count": 2,
        "files": [
            {"path": str(f1), "size": 0, "rel_path": "a.mp4"},
            {"path": str(f2), "size": 0, "rel_path": "b.mp4"},
        ],
    }

    hydrated = hydrate_manifest_files(context, manifest, db=db)

    assert hydrated["file_count"] == 0
    assert hydrated["files"] == []
    assert hydrated["already_completed_file_count"] == 2
    assert "stale_manifest_failure_reason" not in hydrated


def test_hydrate_manifest_files_without_db_preserves_legacy_behavior(tmp_path: Path) -> None:
    source_unit = tmp_path / "incoming" / "gcp" / "source-c-event-bucket" / "20260409"
    source_unit.mkdir(parents=True)
    existing = source_unit / "a.mp4"
    existing.write_bytes(b"x")

    context = _DummyContext()
    manifest = {
        "transfer_tool": "auto_bootstrap_sensor",
        "source_unit_type": "directory",
        "source_unit_name": "gcp/source-c-event-bucket/20260409",
        "source_unit_path": str(source_unit),
        "file_count": 1,
        "files": [
            {"path": str(existing), "size": 0, "rel_path": "a.mp4"},
        ],
    }

    hydrated = hydrate_manifest_files(context, manifest)

    assert hydrated["file_count"] == 1
    assert "already_completed_file_count" not in hydrated


def test_hydrate_manifest_files_db_failure_falls_back_gracefully(tmp_path: Path) -> None:
    source_unit = tmp_path / "incoming" / "gcp" / "source-c-event-bucket" / "20260409"
    source_unit.mkdir(parents=True)
    existing = source_unit / "a.mp4"
    existing.write_bytes(b"x")

    class _BrokenDB:
        def find_completed_source_paths(self, _paths: list[str]) -> set[str]:
            raise RuntimeError("boom")

    context = _DummyContext()
    manifest = {
        "transfer_tool": "auto_bootstrap_sensor",
        "source_unit_type": "directory",
        "source_unit_name": "gcp/source-c-event-bucket/20260409",
        "source_unit_path": str(source_unit),
        "file_count": 1,
        "files": [
            {"path": str(existing), "size": 0, "rel_path": "a.mp4"},
        ],
    }

    hydrated = hydrate_manifest_files(context, manifest, db=_BrokenDB())

    assert hydrated["file_count"] == 1
    assert any("find_completed_source_paths 조회 실패" in w for w in context.log.warnings)


def test_hydrate_manifest_files_keeps_non_auto_bootstrap_manifest_strict(tmp_path: Path) -> None:
    source_unit = tmp_path / "incoming" / "manual" / "sample"
    source_unit.mkdir(parents=True)

    missing_file = source_unit / "missing.mp4"
    context = _DummyContext()
    manifest = {
        "transfer_tool": "dispatch_sensor",
        "source_unit_type": "directory",
        "source_unit_name": "tmp_data_2",
        "source_unit_path": str(source_unit),
        "file_count": 1,
        "files": [
            {
                "path": str(missing_file),
                "size": 0,
                "rel_path": "missing.mp4",
            }
        ],
    }

    hydrated = hydrate_manifest_files(context, manifest)

    assert hydrated["file_count"] == 1
    assert hydrated["files"] == [
        {
            "path": str(missing_file),
            "size": 0,
            "rel_path": "missing.mp4",
        }
    ]
    assert "stale_manifest_detected" not in hydrated
    assert context.log.warnings == []


def test_persist_manifest_is_atomic_and_leaves_no_temp(tmp_path: Path) -> None:
    """제자리 write_text 면 쓰는 동안 파일이 빈/부분 상태로 보여 센서가 정상 manifest 를
    손상으로 오인한다(그 다음이 격리 = 유실). 임시파일 + os.replace 여야 한다."""
    from vlm_pipeline.defs.ingest.ingest_manifest_flow import _persist_manifest

    target = tmp_path / "pending" / "unit.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text('{"source_unit_path": "/old", "file_count": 1}', encoding="utf-8")

    ino_before = target.stat().st_ino

    _persist_manifest(str(target), {"source_unit_path": "/new", "file_count": 2})

    assert json.loads(target.read_text(encoding="utf-8")) == {"source_unit_path": "/new", "file_count": 2}
    # ⚠️ 내용 단언만으로는 옛 구현(Path.write_text)도 통과한다 — 가짜 안전이었다.
    # 제자리 truncate 쓰기는 **같은 inode** 를 유지하고, tmp+os.replace 는 inode 를 갈아끼운다.
    # 이 한 줄이 원자성의 유일한 회귀 감지기다.
    assert target.stat().st_ino != ino_before, "inode 가 그대로 = 제자리 truncate 쓰기(비원자적)"
    # 임시파일이 남으면 *.json glob 과 runbook 의 pending 카운트 양쪽에서 안 보이는 채로 잔존한다.
    assert sorted(x.name for x in target.parent.iterdir()) == ["unit.json"]


def test_persist_manifest_noop_on_missing_args(tmp_path: Path) -> None:
    from vlm_pipeline.defs.ingest.ingest_manifest_flow import _persist_manifest

    absent = tmp_path / "nope.json"
    _persist_manifest(None, {"a": 1})
    _persist_manifest(str(absent), None)
    assert not absent.exists()
