from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from vlm_pipeline.defs.ingest import archive as archive_lib
from vlm_pipeline.defs.ingest import manifest as manifest_lib
from vlm_pipeline.lib.runtime_profile import RuntimeProfile


def _runtime_profile(is_test: bool) -> RuntimeProfile:
    return RuntimeProfile(name="staging" if is_test else "production", is_staging=is_test)


class _FakeDB:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def update_raw_file_status(
        self,
        asset_id: str,
        status: str,
        error_message: str | None = None,
        archive_path: str | None = None,
        raw_bucket: str | None = None,
    ) -> None:
        self.calls.append(
            {
                "asset_id": asset_id,
                "status": status,
                "error_message": error_message,
                "archive_path": archive_path,
                "raw_bucket": raw_bucket,
            }
        )

    def count_unresolved_rows_for_source_unit(self, source_unit_path: str) -> int:
        return 0


class _FakeLog:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str) -> None:
        self.messages.append(message)

    def warning(self, message: str) -> None:
        self.messages.append(message)


class _FakeContext:
    def __init__(self) -> None:
        self.log = _FakeLog()


def test_should_archive_manifest_for_test_dispatch_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    archive_pending_dir = tmp_path / "archive_pending"
    source_unit_path = archive_pending_dir / "request_1"
    source_unit_path.mkdir(parents=True)

    monkeypatch.setenv("ARCHIVE_PENDING_DIR", str(archive_pending_dir))

    assert (
        archive_lib.should_archive_manifest(
            {
                "transfer_tool": "ingest_retry_manifest",
                "source_unit_path": str(source_unit_path),
            },
            runtime_profile=_runtime_profile(True),
        )
        is True
    )


def test_should_archive_manifest_for_test_auto_bootstrap_non_gcp_allowed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """test + auto_bootstrap 은 incoming/gcp 밖이어도 archive 허용.

    commit dffaf97 "ingest 방식 변경" 이후
    ``_staging_transfer_allows_archive`` (archive_policy.py:44-51) 가
    auto_bootstrap_sensor 를 gcp 여부와 무관하게 전면 허용하도록 바뀌었다.
    """
    archive_pending_dir = tmp_path / "archive_pending"
    incoming_dir = tmp_path / "incoming"
    source_unit_path = incoming_dir / "tmp_data_2" / "smoking"
    source_unit_path.mkdir(parents=True)

    monkeypatch.setenv("ARCHIVE_PENDING_DIR", str(archive_pending_dir))
    cfg = MagicMock()
    cfg.incoming_dir = str(incoming_dir)

    assert (
        archive_lib.should_archive_manifest(
            {
                "transfer_tool": "auto_bootstrap_sensor",
                "source_unit_path": str(source_unit_path),
            },
            config=cfg,
            runtime_profile=_runtime_profile(True),
        )
        is True
    )


def test_should_archive_manifest_for_test_auto_bootstrap_gcp_path_allowed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """test + auto_bootstrap + source under incoming/gcp -> dispatch JSON 없이 archive 허용."""
    incoming_dir = tmp_path / "incoming"
    source_unit_path = incoming_dir / "gcp" / "bucket-a" / "20250101"
    source_unit_path.mkdir(parents=True)

    cfg = MagicMock()
    cfg.incoming_dir = str(incoming_dir)
    cfg.archive_pending_dir = str(tmp_path / "archive_pending")

    assert (
        archive_lib.should_archive_manifest(
            {
                "transfer_tool": "auto_bootstrap_sensor",
                "source_unit_path": str(source_unit_path),
                "archive_requested": True,
            },
            config=cfg,
            runtime_profile=_runtime_profile(True),
        )
        is True
    )


def test_should_archive_manifest_for_test_auto_bootstrap_archive_pending_allowed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """archive_requested=True + auto_bootstrap_sensor 는 gcp 밖(archive_pending 하위)이어도 허용.

    commit dffaf97 이후 archive_policy.py:51 이 auto_bootstrap_sensor 를
    dispatch_sensor/ingest_retry_manifest 와 동일하게 전면 허용한다.
    """
    archive_pending_dir = tmp_path / "archive_pending"
    source_unit_path = archive_pending_dir / "tmp_data_2" / "smoking"
    source_unit_path.mkdir(parents=True)

    monkeypatch.setenv("ARCHIVE_PENDING_DIR", str(archive_pending_dir))
    cfg = MagicMock()
    cfg.incoming_dir = str(tmp_path / "incoming")
    cfg.archive_pending_dir = str(archive_pending_dir)

    assert (
        archive_lib.should_archive_manifest(
            {
                "transfer_tool": "auto_bootstrap_sensor",
                "source_unit_path": str(source_unit_path),
                "archive_requested": True,
            },
            config=cfg,
            runtime_profile=_runtime_profile(True),
        )
        is True
    )


def test_should_archive_manifest_for_test_dispatch_retry_true_flag(
    monkeypatch,
    tmp_path: Path,
) -> None:
    archive_pending_dir = tmp_path / "archive_pending"
    source_unit_path = archive_pending_dir / "request_1"
    source_unit_path.mkdir(parents=True)

    monkeypatch.setenv("ARCHIVE_PENDING_DIR", str(archive_pending_dir))

    assert (
        archive_lib.should_archive_manifest(
            {
                "transfer_tool": "ingest_retry_manifest",
                "source_unit_path": str(source_unit_path),
                "archive_requested": True,
            },
            runtime_profile=_runtime_profile(True),
        )
        is True
    )


def test_should_archive_manifest_in_production_auto_bootstrap_direct_child_allowed(monkeypatch) -> None:
    """commit dffaf97 이후 production auto_bootstrap 은 gcp 여부와 무관하게 전면 허용된다
    (manifest_allows_auto_bootstrap_without_dispatch, archive_policy.py:35-41)."""
    cfg = MagicMock()
    cfg.incoming_dir = "/nas/incoming"

    assert (
        archive_lib.should_archive_manifest(
            {
                "transfer_tool": "auto_bootstrap_sensor",
                "source_unit_path": "/nas/incoming/sample",
            },
            config=cfg,
            runtime_profile=_runtime_profile(False),
        )
        is True
    )


def test_should_archive_manifest_in_production_auto_bootstrap_nested_allowed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """nested auto_bootstrap 경로도 commit dffaf97 이후 전면 허용 정책 아래 archive 대상이다."""
    inc = tmp_path / "incoming"
    nested = inc / "proj" / "batch"
    nested.mkdir(parents=True)
    cfg = MagicMock()
    cfg.incoming_dir = str(inc)

    assert (
        archive_lib.should_archive_manifest(
            {
                "transfer_tool": "auto_bootstrap_sensor",
                "source_unit_path": str(nested),
                "archive_requested": True,
            },
            config=cfg,
            runtime_profile=_runtime_profile(False),
        )
        is True
    )


def test_should_archive_manifest_in_production_dispatch_nested_still_ok(
    monkeypatch,
    tmp_path: Path,
) -> None:
    inc = tmp_path / "incoming"
    nested = inc / "proj" / "batch"
    nested.mkdir(parents=True)
    cfg = MagicMock()
    cfg.incoming_dir = str(inc)

    assert (
        archive_lib.should_archive_manifest(
            {
                "transfer_tool": "dispatch_sensor",
                "source_unit_path": str(nested),
                "archive_requested": True,
            },
            config=cfg,
            runtime_profile=_runtime_profile(False),
        )
        is True
    )


def test_complete_uploaded_assets_without_archive_marks_completed() -> None:
    fake_db = _FakeDB()
    fake_context = _FakeContext()

    completed = archive_lib.complete_uploaded_assets_without_archive(
        context=fake_context,
        db=fake_db,
        manifest={"manifest_id": "auto_bootstrap_1", "transfer_tool": "auto_bootstrap_sensor"},
        uploaded=[
            {"asset_id": "asset-1", "source_path": "/nas/staging/incoming/a.mp4"},
            {"asset_id": "asset-2", "source_path": "/nas/staging/incoming/b.mp4"},
        ],
    )

    assert [row["asset_id"] for row in completed] == ["asset-1", "asset-2"]
    assert fake_db.calls == [
        {
            "asset_id": "asset-1",
            "status": "completed",
            "error_message": None,
            "archive_path": None,
            "raw_bucket": "vlm-raw",
        },
        {
            "asset_id": "asset-2",
            "status": "completed",
            "error_message": None,
            "archive_path": None,
            "raw_bucket": "vlm-raw",
        },
    ]
    assert fake_context.log.messages


def test_archive_uploaded_assets_removes_empty_incoming_parents_after_directory_move(
    tmp_path: Path,
) -> None:
    fake_db = _FakeDB()
    fake_context = _FakeContext()
    incoming_dir = tmp_path / "incoming"
    source_unit_path = incoming_dir / "proj_a" / "request_1"
    source_unit_path.mkdir(parents=True)
    video_path = source_unit_path / "a.mp4"
    video_path.write_bytes(b"a")
    archive_dir = tmp_path / "archive"

    archived, archive_hint = archive_lib.archive_uploaded_assets(
        context=fake_context,
        db=fake_db,
        manifest={
            "manifest_id": "dispatch_1",
            "source_dir": str(incoming_dir),
            "source_unit_type": "directory",
            "source_unit_name": "request_1",
            "source_unit_path": str(source_unit_path),
            "source_unit_total_file_count": 1,
            "file_count": 1,
            "files": [
                {"path": str(video_path), "rel_path": "a.mp4"},
            ],
        },
        uploaded=[
            {
                "asset_id": "asset-1",
                "source_path": str(video_path),
                "media_type": "video",
            }
        ],
        archive_dir=str(archive_dir),
    )

    assert archive_hint == archive_dir / "request_1"
    assert len(archived) == 1
    assert (incoming_dir / "proj_a").exists() is False
    assert incoming_dir.exists() is True
    assert (archive_dir / "request_1" / "a.mp4").exists() is True


def test_archive_uploaded_assets_dedup_count_enables_fast_path(
    tmp_path: Path,
) -> None:
    """uploaded < total 이라도 duplicate_skip_count 로 채워지면 folder rename fast-path 활성화.

    2026-05-20 appdata 2500 dispatch (uploaded=2496, dedup=4) 케이스 회귀 방지:
    fast-path 미활성 시 per-file ThreadPool 이 NFS dir lock 경합 → 3h 56m 소요.
    fast-path 활성 시 폴더 rename 1 회로 끝 (수 초).
    """
    fake_db = _FakeDB()
    fake_context = _FakeContext()
    incoming_dir = tmp_path / "incoming"
    source_unit_path = incoming_dir / "request_1"
    source_unit_path.mkdir(parents=True)
    uploaded_video_path = source_unit_path / "a.mp4"
    duplicate_video_path = source_unit_path / "b.mp4"
    uploaded_video_path.write_bytes(b"a")
    duplicate_video_path.write_bytes(b"a")  # same content (intra-run dedup)
    archive_dir = tmp_path / "archive"

    archived, archive_hint = archive_lib.archive_uploaded_assets(
        context=fake_context,
        db=fake_db,
        manifest={
            "manifest_id": "dispatch_1",
            "source_dir": str(incoming_dir),
            "source_unit_type": "directory",
            "source_unit_name": "request_1",
            "source_unit_path": str(source_unit_path),
            "source_unit_total_file_count": 2,
            "file_count": 2,
            "files": [
                {"path": str(uploaded_video_path), "rel_path": "a.mp4"},
                {"path": str(duplicate_video_path), "rel_path": "b.mp4"},
            ],
        },
        uploaded=[
            {
                "asset_id": "asset-1",
                "source_path": str(uploaded_video_path),
                "media_type": "video",
            }
        ],
        archive_dir=str(archive_dir),
        duplicate_skip_count=1,
    )

    # fast-path 활성 → 폴더 rename → uploaded + duplicate 둘 다 archive 로 이동
    assert archive_hint == archive_dir / "request_1"
    assert len(archived) == 1
    assert (archive_dir / "request_1" / "a.mp4").exists() is True
    # duplicate 도 archive 로 따라 이동 (orphan, DB 추적 X, harmless)
    assert (archive_dir / "request_1" / "b.mp4").exists() is True
    assert source_unit_path.exists() is False


def test_archive_uploaded_assets_partial_duplicate_moves_uploaded_only(
    tmp_path: Path,
) -> None:
    fake_db = _FakeDB()
    fake_context = _FakeContext()
    incoming_dir = tmp_path / "incoming"
    source_unit_path = incoming_dir / "proj_a" / "request_1"
    source_unit_path.mkdir(parents=True)
    uploaded_video_path = source_unit_path / "a.mp4"
    duplicate_video_path = source_unit_path / "b.mp4"
    uploaded_video_path.write_bytes(b"a")
    duplicate_video_path.write_bytes(b"b")
    archive_dir = tmp_path / "archive"

    archived, archive_hint = archive_lib.archive_uploaded_assets(
        context=fake_context,
        db=fake_db,
        manifest={
            "manifest_id": "dispatch_1",
            "source_dir": str(incoming_dir),
            "source_unit_type": "directory",
            "source_unit_name": "request_1",
            "source_unit_path": str(source_unit_path),
            "source_unit_total_file_count": 2,
            "file_count": 2,
            "files": [
                {"path": str(uploaded_video_path), "rel_path": "a.mp4"},
                {"path": str(duplicate_video_path), "rel_path": "b.mp4"},
            ],
        },
        uploaded=[
            {
                "asset_id": "asset-1",
                "source_path": str(uploaded_video_path),
                "media_type": "video",
            }
        ],
        archive_dir=str(archive_dir),
    )

    assert archive_hint == archive_dir / "request_1"
    assert len(archived) == 1
    assert Path(archived[0]["archive_path"]) == archive_dir / "request_1" / "a.mp4"
    assert (archive_dir / "request_1" / "a.mp4").exists() is True
    assert uploaded_video_path.exists() is False
    assert duplicate_video_path.exists() is True
    assert source_unit_path.exists() is True
    assert fake_db.calls == [
        {
            "asset_id": "asset-1",
            "status": "completed",
            "error_message": None,
            "archive_path": str(archive_dir / "request_1" / "a.mp4"),
            "raw_bucket": "vlm-raw",
        }
    ]


def test_archive_uploaded_assets_strips_gcp_prefix_in_archive_path(
    tmp_path: Path,
) -> None:
    fake_db = _FakeDB()
    fake_context = _FakeContext()
    incoming_dir = tmp_path / "incoming"
    source_unit_path = incoming_dir / "gcp" / "source-b-event-bucket" / "20260329"
    source_unit_path.mkdir(parents=True)
    video_path = source_unit_path / "a.mp4"
    video_path.write_bytes(b"a")
    archive_dir = tmp_path / "archive"

    archived, archive_hint = archive_lib.archive_uploaded_assets(
        context=fake_context,
        db=fake_db,
        manifest={
            "manifest_id": "auto_bootstrap_1",
            "source_dir": str(incoming_dir),
            "source_unit_type": "directory",
            "source_unit_name": "gcp/source-b-event-bucket/20260329",
            "source_unit_path": str(source_unit_path),
            "source_unit_total_file_count": 1,
            "file_count": 1,
            "files": [
                {"path": str(video_path), "rel_path": "a.mp4"},
            ],
        },
        uploaded=[
            {
                "asset_id": "asset-1",
                "source_path": str(video_path),
                "media_type": "video",
            }
        ],
        archive_dir=str(archive_dir),
    )

    assert archive_hint == archive_dir / "source-b-event-bucket" / "20260329"
    assert len(archived) == 1
    assert (archive_dir / "source-b-event-bucket" / "20260329" / "a.mp4").exists() is True
    assert "/archive/gcp/" not in str(archived[0]["archive_path"])
    assert "/archive/source-b-event-bucket/" in str(archived[0]["archive_path"])


def test_maybe_write_archive_done_marker_uses_gcp_stripped_archive_unit_path(
    tmp_path: Path,
) -> None:
    fake_db = _FakeDB()
    fake_context = _FakeContext()
    manifest_dir = tmp_path / "manifests"
    archive_dir = tmp_path / "archive"
    archive_unit_dir = archive_dir / "source-c-event-bucket" / "20260329"
    archive_unit_dir.mkdir(parents=True)
    (archive_unit_dir / "a.mp4").write_bytes(b"a")

    marker_path = manifest_lib.maybe_write_archive_done_marker(
        context=fake_context,
        db=fake_db,
        manifest={
            "manifest_id": "auto_bootstrap_1",
            "source_unit_type": "directory",
            "source_unit_name": "gcp/source-c-event-bucket/20260329",
            "source_unit_path": "/nas/incoming/gcp/source-c-event-bucket/20260329",
            "source_unit_chunk_index": 1,
            "source_unit_chunk_count": 1,
        },
        manifest_dir=str(manifest_dir),
        manifest_path=None,
        archive_dir=str(archive_dir),
        archive_unit_dir_hint=None,
    )

    assert marker_path == archive_unit_dir / "_DONE"
    assert marker_path.exists() is True
