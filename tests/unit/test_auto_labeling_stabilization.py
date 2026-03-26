from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from vlm_pipeline.resources.duckdb_base import DuckDBBaseMixin
from vlm_pipeline.resources.duckdb_dedup import DuckDBDedupMixin
from vlm_pipeline.resources.duckdb_ingest import DuckDBIngestMixin
from vlm_pipeline.resources.duckdb_labeling import DuckDBLabelingMixin


class _TestDuckDBResource(
    DuckDBBaseMixin,
    DuckDBDedupMixin,
    DuckDBIngestMixin,
    DuckDBLabelingMixin,
):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path


def _resource(tmp_path: Path) -> _TestDuckDBResource:
    return _TestDuckDBResource(str(tmp_path / "pipeline.duckdb"))


def _insert_raw_video(
    resource: _TestDuckDBResource,
    *,
    asset_id: str = "asset-1",
    raw_key: str = "adlib-hotel-202512/20251222/sample.mp4",
) -> str:
    now = datetime(2026, 3, 11, 0, 0, 0)
    with resource.connect() as conn:
        conn.execute(
            """
            INSERT INTO raw_files (
                asset_id, source_path, original_name, media_type, file_size,
                checksum, archive_path, raw_bucket, raw_key, ingest_status,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                asset_id,
                f"/nas/archive/{raw_key}",
                Path(raw_key).name,
                "video",
                1234,
                f"checksum-{asset_id}",
                f"/nas/archive/{raw_key}",
                "vlm-raw",
                raw_key,
                "completed",
                now,
                now,
            ],
        )
        conn.execute(
            """
            INSERT INTO video_metadata (
                asset_id, width, height, duration_sec, fps, codec,
                frame_count, extracted_at, auto_label_status, auto_label_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                asset_id,
                1920,
                1080,
                12.5,
                25.0,
                "h264",
                312,
                now,
                "pending",
                None,
            ],
        )
    return asset_id


def test_ensure_schema_adds_runtime_auto_label_columns_and_image_labels(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    with duckdb.connect(resource.db_path) as conn:
        conn.execute(
            """
            CREATE TABLE raw_files (
                asset_id VARCHAR PRIMARY KEY,
                source_path VARCHAR,
                original_name VARCHAR,
                media_type VARCHAR,
                file_size BIGINT,
                checksum VARCHAR,
                archive_path VARCHAR,
                raw_bucket VARCHAR,
                raw_key VARCHAR,
                ingest_status VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE video_metadata (
                asset_id VARCHAR PRIMARY KEY,
                width INTEGER,
                height INTEGER,
                duration_sec DOUBLE,
                fps DOUBLE,
                codec VARCHAR,
                frame_count INTEGER,
                extracted_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE labels (
                label_id VARCHAR PRIMARY KEY,
                asset_id VARCHAR,
                labels_bucket VARCHAR,
                labels_key VARCHAR,
                label_format VARCHAR,
                label_tool VARCHAR,
                label_status VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE processed_clips (
                clip_id VARCHAR PRIMARY KEY,
                source_asset_id VARCHAR,
                source_label_id VARCHAR,
                event_index INTEGER,
                clip_start_sec DOUBLE,
                clip_end_sec DOUBLE,
                checksum VARCHAR,
                file_size BIGINT,
                processed_bucket VARCHAR,
                clip_key VARCHAR,
                label_key VARCHAR,
                width INTEGER,
                height INTEGER,
                codec VARCHAR,
                process_status VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE image_metadata (
                image_id VARCHAR PRIMARY KEY,
                source_asset_id VARCHAR,
                source_clip_id VARCHAR,
                image_bucket VARCHAR,
                image_key VARCHAR,
                image_role VARCHAR,
                frame_index INTEGER,
                frame_sec DOUBLE,
                checksum VARCHAR,
                file_size BIGINT,
                width INTEGER,
                height INTEGER,
                color_mode VARCHAR,
                bit_depth INTEGER,
                has_alpha BOOLEAN,
                orientation INTEGER,
                extracted_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO video_metadata (asset_id, duration_sec, fps, frame_count, extracted_at)
            VALUES ('legacy-video', 10.0, 25.0, 250, CURRENT_TIMESTAMP)
            """
        )
        conn.execute(
            """
            INSERT INTO processed_clips (
                clip_id, source_asset_id, source_label_id, event_index,
                processed_bucket, clip_key, process_status, created_at
            ) VALUES ('legacy-clip', 'legacy-video', 'legacy-label', 0, 'vlm-processed', 'legacy.mp4', 'pending', CURRENT_TIMESTAMP)
            """
        )

    resource.ensure_schema()

    with resource.connect() as conn:
        video_columns = {
            row[0]
            for row in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'video_metadata'").fetchall()
        }
        image_columns = {
            row[0]
            for row in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'image_metadata'").fetchall()
        }
        processed_columns = {
            row[0]
            for row in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'processed_clips'").fetchall()
        }
        image_label_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'image_labels'"
        ).fetchone()[0]
        video_defaults = conn.execute(
            """
            SELECT auto_label_status
            FROM video_metadata
            WHERE asset_id = 'legacy-video'
            """
        ).fetchone()[0]
        clip_defaults = conn.execute(
            """
            SELECT image_extract_status, image_extract_count
            FROM processed_clips
            WHERE clip_id = 'legacy-clip'
            """
        ).fetchone()

    assert {"auto_label_status", "auto_label_error", "auto_label_key", "auto_labeled_at"} <= video_columns
    assert {
        "image_caption_text",
        "image_caption_score",
        "image_caption_bucket",
        "image_caption_key",
        "image_caption_generated_at",
    } <= image_columns
    assert {
        "duration_sec",
        "fps",
        "frame_count",
        "image_extract_status",
        "image_extract_count",
        "image_extract_error",
        "image_extracted_at",
    } <= processed_columns
    assert image_label_exists == 1
    assert video_defaults == "pending"
    assert clip_defaults == ("pending", 0)


def test_ensure_schema_upgrades_asset_id_style_image_metadata_without_rebuild(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    with duckdb.connect(resource.db_path) as conn:
        conn.execute(
            """
            CREATE TABLE raw_files (
                asset_id VARCHAR PRIMARY KEY,
                source_path VARCHAR,
                original_name VARCHAR,
                media_type VARCHAR,
                file_size BIGINT,
                checksum VARCHAR,
                archive_path VARCHAR,
                raw_bucket VARCHAR,
                raw_key VARCHAR,
                ingest_status VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw_files (
                asset_id, source_path, original_name, media_type, file_size, checksum,
                archive_path, raw_bucket, raw_key, ingest_status, created_at, updated_at
            ) VALUES (
                'legacy-image', '/nas/archive/legacy.jpg', 'legacy.jpg', 'image', 1234, 'sum-1',
                '/nas/archive/legacy.jpg', 'vlm-raw', 'legacy/legacy.jpg', 'completed',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE image_metadata (
                asset_id VARCHAR PRIMARY KEY,
                width INTEGER,
                height INTEGER,
                caption_text TEXT,
                extracted_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO image_metadata (asset_id, width, height, caption_text, extracted_at)
            VALUES ('legacy-image', 640, 480, 'legacy image caption', CURRENT_TIMESTAMP)
            """
        )

    resource.repair_image_metadata_table()

    with resource.connect() as conn:
        columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'image_metadata'"
            ).fetchall()
        }
        row = conn.execute(
            """
            SELECT image_id, source_asset_id, image_bucket, image_key, color_mode, bit_depth, image_caption_text, image_caption_score
            FROM image_metadata
            WHERE image_id = 'legacy-image'
            """
        ).fetchone()
        image_label_columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'image_labels'"
            ).fetchall()
        }

    assert {
        "image_id",
        "source_asset_id",
        "image_caption_text",
        "image_caption_score",
        "image_caption_bucket",
        "image_caption_key",
        "image_caption_generated_at",
    } <= columns
    assert "caption_text" not in columns
    assert row == ("legacy-image", "legacy-image", "vlm-raw", "legacy/legacy.jpg", "RGB", 8, "legacy image caption", None)
    assert {"image_label_id", "image_id", "source_clip_id"} <= image_label_columns


def test_repair_image_metadata_table_rebuilds_constraints_from_legacy_schema(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    with duckdb.connect(resource.db_path) as conn:
        conn.execute(
            """
            CREATE TABLE raw_files (
                asset_id VARCHAR PRIMARY KEY,
                source_path VARCHAR,
                original_name VARCHAR,
                media_type VARCHAR,
                file_size BIGINT,
                checksum VARCHAR,
                archive_path VARCHAR,
                raw_bucket VARCHAR,
                raw_key VARCHAR,
                ingest_status VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw_files (
                asset_id, source_path, original_name, media_type, file_size, checksum,
                archive_path, raw_bucket, raw_key, ingest_status, created_at, updated_at
            ) VALUES (
                'legacy-image', '/nas/archive/legacy.jpg', 'legacy.jpg', 'image', 1234, 'sum-1',
                '/nas/archive/legacy.jpg', 'vlm-raw', 'legacy/legacy.jpg', 'completed',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE image_metadata (
                asset_id VARCHAR PRIMARY KEY,
                image_bucket VARCHAR,
                image_key VARCHAR,
                width INTEGER,
                height INTEGER,
                extracted_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO image_metadata (asset_id, image_bucket, image_key, width, height, extracted_at)
            VALUES ('legacy-image', 'vlm-raw', 'legacy/legacy.jpg', 640, 480, CURRENT_TIMESTAMP)
            """
        )

    resource.repair_image_metadata_table()

    with resource.connect() as conn:
        row = conn.execute(
            """
            SELECT image_id, source_asset_id, image_bucket, image_key
            FROM image_metadata
            WHERE image_id = 'legacy-image'
            """
        ).fetchone()
        pk_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM duckdb_constraints()
            WHERE table_name = 'image_metadata'
              AND constraint_type = 'PRIMARY KEY'
              AND array_length(constraint_column_names) = 1
              AND constraint_column_names[1] = 'image_id'
            """
        ).fetchone()[0]
        migrated_exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = 'image_metadata__migrated'
            """
        ).fetchone()[0]

    assert row == ("legacy-image", "legacy-image", "vlm-raw", "legacy/legacy.jpg")
    assert pk_count == 1
    assert migrated_exists == 0


def test_insert_label_single_row_succeeds(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    resource.ensure_schema()
    asset_id = _insert_raw_video(resource)

    resource.insert_label(
        {
            "label_id": "label-1",
            "asset_id": asset_id,
            "labels_key": "adlib-hotel-202512/20251222/sample.json",
            "label_format": "gemini_event_json",
            "label_tool": "gemini",
            "label_source": "auto",
            "event_index": 0,
            "event_count": 1,
            "timestamp_start_sec": 0.0,
            "timestamp_end_sec": 2.0,
            "caption_text": "smoke",
        }
    )

    with resource.connect() as conn:
        row = conn.execute(
            """
            SELECT label_id, asset_id, labels_key, timestamp_start_sec, timestamp_end_sec
            FROM labels
            WHERE label_id = 'label-1'
            """
        ).fetchone()

    assert row == (
        "label-1",
        asset_id,
        "adlib-hotel-202512/20251222/sample.json",
        0.0,
        2.0,
    )


def test_replace_gemini_labels_rolls_back_on_insert_failure(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    resource.ensure_schema()
    asset_id = _insert_raw_video(resource)
    resource.insert_label(
        {
            "label_id": "existing-label",
            "asset_id": asset_id,
            "labels_key": "adlib-hotel-202512/20251222/sample.json",
            "label_format": "gemini_event_json",
            "label_tool": "gemini",
            "label_source": "auto",
            "event_index": 0,
            "event_count": 1,
            "timestamp_start_sec": 0.0,
            "timestamp_end_sec": 1.0,
            "caption_text": "existing",
        }
    )

    with pytest.raises(Exception):
        resource.replace_gemini_labels(
            asset_id,
            "adlib-hotel-202512/20251222/sample.json",
            [
                {
                    "label_id": "duplicate-id",
                    "event_index": 0,
                    "event_count": 2,
                    "timestamp_start_sec": 0.0,
                    "timestamp_end_sec": 1.0,
                    "caption_text": "first",
                },
                {
                    "label_id": "duplicate-id",
                    "event_index": 1,
                    "event_count": 2,
                    "timestamp_start_sec": 2.0,
                    "timestamp_end_sec": 3.0,
                    "caption_text": "second",
                },
            ],
        )

    with resource.connect() as conn:
        rows = conn.execute(
            """
            SELECT label_id, caption_text
            FROM labels
            WHERE asset_id = ?
              AND labels_key = ?
            ORDER BY label_id
            """,
            [asset_id, "adlib-hotel-202512/20251222/sample.json"],
        ).fetchall()

    assert rows == [("existing-label", "existing")]


def test_find_captioning_pending_videos_only_returns_generated_without_labels(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    resource.ensure_schema()
    asset_id = _insert_raw_video(resource)

    resource.update_auto_label_status(
        asset_id,
        "generated",
        label_key="adlib-hotel-202512/20251222/sample.json",
        labeled_at=datetime(2026, 3, 11, 1, 0, 0),
    )

    pending = resource.find_captioning_pending_videos(limit=10)
    assert [row["asset_id"] for row in pending] == [asset_id]

    resource.replace_gemini_labels(
        asset_id,
        "adlib-hotel-202512/20251222/sample.json",
        [
            {
                "label_id": "gemini-1",
                "event_index": 0,
                "event_count": 1,
                "timestamp_start_sec": 0.0,
                "timestamp_end_sec": 2.0,
                "caption_text": "caption",
            }
        ],
    )

    assert resource.find_captioning_pending_videos(limit=10) == []


def test_find_processable_keeps_failed_clips_reprocessable(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    resource.ensure_schema()
    asset_id = _insert_raw_video(resource)
    resource.insert_label(
        {
            "label_id": "label-1",
            "asset_id": asset_id,
            "labels_key": "adlib-hotel-202512/20251222/sample.json",
            "label_format": "gemini_event_json",
            "label_tool": "gemini",
            "label_source": "auto",
            "event_index": 0,
            "event_count": 1,
            "timestamp_start_sec": 0.0,
            "timestamp_end_sec": 2.0,
            "caption_text": "smoke",
        }
    )
    resource.insert_processed_clip(
        {
            "clip_id": "clip-1",
            "source_asset_id": asset_id,
            "source_label_id": "label-1",
            "event_index": 0,
            "clip_start_sec": 0.0,
            "clip_end_sec": 2.0,
            "clip_key": "adlib-hotel-202512/20251222/clips/sample_e000.mp4",
            "label_key": "adlib-hotel-202512/20251222/sample.json",
            "process_status": "failed",
            "image_extract_status": "failed",
            "image_extract_error": "frame_extract_failed",
        }
    )

    rows = resource.find_processable()
    assert [row["label_id"] for row in rows] == ["label-1"]

    resource.insert_processed_clip(
        {
            "clip_id": "clip-1",
            "source_asset_id": asset_id,
            "source_label_id": "label-1",
            "event_index": 0,
            "clip_start_sec": 0.0,
            "clip_end_sec": 2.0,
            "clip_key": "adlib-hotel-202512/20251222/clips/sample_e000.mp4",
            "label_key": "adlib-hotel-202512/20251222/sample.json",
            "process_status": "completed",
            "image_extract_status": "completed",
        }
    )

    assert resource.find_processable() == []


def test_find_processable_allows_ready_for_labeling_with_spec_id(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    resource.ensure_schema()
    asset_id = _insert_raw_video(resource)

    with resource.connect() as conn:
        conn.execute(
            """
            UPDATE raw_files
            SET ingest_status = 'ready_for_labeling',
                spec_id = 'spec-1',
                source_unit_name = 'unit-a'
            WHERE asset_id = ?
            """,
            [asset_id],
        )

    resource.insert_label(
        {
            "label_id": "label-spec-1",
            "asset_id": asset_id,
            "labels_key": "adlib-hotel-202512/20251222/sample.json",
            "label_format": "gemini_event_json",
            "label_tool": "gemini",
            "label_source": "auto",
            "event_index": 0,
            "event_count": 1,
            "timestamp_start_sec": 0.0,
            "timestamp_end_sec": 2.0,
            "caption_text": "smoke",
        }
    )

    rows = resource.find_processable(spec_id="spec-1")
    assert [row["label_id"] for row in rows] == ["label-spec-1"]
    assert resource.find_processable(spec_id="spec-2") == []


def test_find_ready_for_labeling_caption_backlog_returns_timestamp_completed_rows(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    resource.ensure_schema()
    asset_id = _insert_raw_video(resource)

    with resource.connect() as conn:
        conn.execute(
            """
            UPDATE raw_files
            SET ingest_status = 'ready_for_labeling',
                spec_id = 'spec-1',
                source_unit_name = 'unit-a'
            WHERE asset_id = ?
            """,
            [asset_id],
        )

    resource.update_timestamp_status(
        asset_id,
        "completed",
        label_key="adlib-hotel-202512/20251222/events/sample.json",
        completed_at=datetime(2026, 3, 11, 2, 0, 0),
    )

    rows = resource.find_ready_for_labeling_caption_backlog("spec-1", limit=10)
    assert rows == [
        {
            "asset_id": asset_id,
            "raw_bucket": "vlm-raw",
            "raw_key": "adlib-hotel-202512/20251222/sample.mp4",
            "timestamp_label_key": "adlib-hotel-202512/20251222/events/sample.json",
            "duration_sec": 12.5,
        }
    ]


def test_find_caption_pending_by_folder_returns_dispatch_rows(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    resource.ensure_schema()
    asset_id = _insert_raw_video(resource)

    with resource.connect() as conn:
        conn.execute(
            """
            UPDATE raw_files
            SET ingest_status = 'completed',
                source_unit_name = 'unit-a'
            WHERE asset_id = ?
            """,
            [asset_id],
        )

    resource.update_timestamp_status(
        asset_id,
        "completed",
        label_key="adlib-hotel-202512/20251222/events/sample.json",
        completed_at=datetime(2026, 3, 11, 2, 0, 0),
    )

    rows = resource.find_caption_pending_by_folder("unit-a", limit=10)
    assert rows == [
        {
            "asset_id": asset_id,
            "raw_bucket": "vlm-raw",
            "raw_key": "adlib-hotel-202512/20251222/sample.mp4",
            "timestamp_label_key": "adlib-hotel-202512/20251222/events/sample.json",
            "duration_sec": 12.5,
        }
    ]


def test_replace_processed_clip_frame_metadata_persists_image_caption_text(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    resource.ensure_schema()
    asset_id = _insert_raw_video(resource)
    resource.insert_label(
        {
            "label_id": "label-1",
            "asset_id": asset_id,
            "labels_key": "adlib-hotel-202512/20251222/events/sample.json",
            "label_format": "gemini_event_json",
            "label_tool": "gemini",
            "label_source": "auto",
            "event_index": 0,
            "event_count": 1,
            "timestamp_start_sec": 0.0,
            "timestamp_end_sec": 2.0,
            "caption_text": "smoke",
        }
    )
    resource.insert_processed_clip(
        {
            "clip_id": "clip-1",
            "source_asset_id": asset_id,
            "source_label_id": "label-1",
            "event_index": 0,
            "clip_start_sec": 0.0,
            "clip_end_sec": 2.0,
            "clip_key": "adlib-hotel-202512/20251222/clips/sample_e000.mp4",
            "label_key": "adlib-hotel-202512/20251222/events/sample.json",
            "process_status": "completed",
            "image_extract_status": "completed",
        }
    )

    resource.replace_processed_clip_frame_metadata(
        asset_id,
        "clip-1",
        [
            {
                "image_id": "frame-1",
                "source_clip_id": "clip-1",
                "image_bucket": "vlm-processed",
                "image_key": "adlib-hotel-202512/20251222/image/sample_00000001.jpg",
                "image_role": "processed_clip_frame",
                "frame_index": 1,
                "frame_sec": 0.5,
                "image_caption_text": "연기가 보이는 프레임",
                "image_caption_score": 0.91,
                "image_caption_bucket": "vlm-labels",
                "image_caption_key": "adlib-hotel-202512/20251222/image_captions/sample_00000001.json",
                "image_caption_generated_at": datetime(2026, 3, 25, 15, 0, 0),
            }
        ],
    )

    rows = resource.list_processed_clip_frame_rows("clip-1")
    assert len(rows) == 1
    assert rows[0]["image_caption_text"] == "연기가 보이는 프레임"
    assert rows[0]["image_caption_score"] == pytest.approx(0.91)
    assert rows[0]["image_caption_bucket"] == "vlm-labels"
    assert rows[0]["image_caption_key"] == "adlib-hotel-202512/20251222/image_captions/sample_00000001.json"
    assert "caption_text" not in rows[0]


def test_replace_processed_clip_frame_metadata_absorbs_legacy_caption_text(tmp_path: Path) -> None:
    resource = _resource(tmp_path)
    resource.ensure_schema()
    asset_id = _insert_raw_video(resource)
    resource.insert_processed_clip(
        {
            "clip_id": "clip-legacy-caption",
            "source_asset_id": asset_id,
            "source_label_id": None,
            "event_index": 0,
            "clip_start_sec": 0.0,
            "clip_end_sec": 1.0,
            "clip_key": "unit/test/clips/legacy.mp4",
            "process_status": "completed",
            "image_extract_status": "completed",
        }
    )

    resource.replace_processed_clip_frame_metadata(
        asset_id,
        "clip-legacy-caption",
        [
            {
                "image_id": "frame-legacy-caption",
                "source_clip_id": "clip-legacy-caption",
                "image_bucket": "vlm-processed",
                "image_key": "unit/test/image/legacy_00000001.jpg",
                "image_role": "processed_clip_frame",
                "frame_index": 1,
                "frame_sec": 0.2,
                "caption_text": "legacy image caption",
            }
        ],
    )

    rows = resource.list_processed_clip_frame_rows("clip-legacy-caption")
    assert len(rows) == 1
    assert rows[0]["image_caption_text"] == "legacy image caption"
    assert "caption_text" not in rows[0]


def test_extract_clip_frames_captions_only_highest_relevance_frame(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pytest.importorskip("dagster")
    from vlm_pipeline.defs.process import assets as process_assets

    class _DummyMinIO:
        def __init__(self) -> None:
            self.uploaded: list[tuple[str, str]] = []

        def upload(self, bucket: str, key: str, data: bytes, content_type: str) -> None:
            self.uploaded.append((bucket, key))

    dummy_minio = _DummyMinIO()
    score_by_frame = {
        b"frame-1": 0.2,
        b"frame-2": 0.9,
        b"frame-3": 0.5,
    }
    selected_frames: list[bytes] = []

    monkeypatch.setattr(process_assets, "plan_frame_timestamps", lambda **_: [0.1, 0.5, 0.9])
    monkeypatch.setattr(
        process_assets,
        "extract_frame_jpeg_bytes",
        lambda clip_path, frame_sec, jpeg_quality: f"frame-{int(frame_sec * 10)}".encode("utf-8"),
    )
    monkeypatch.setattr(
        process_assets,
        "describe_frame_bytes",
        lambda frame_bytes: {
            "width": 640,
            "height": 360,
            "color_mode": "RGB",
            "bit_depth": 8,
            "has_alpha": False,
            "orientation": 1,
        },
    )
    monkeypatch.setattr(
        process_assets,
        "_score_event_frame_image_relevance",
        lambda analyzer, frame_bytes, **kwargs: score_by_frame[frame_bytes],
    )

    def _fake_generate_caption(analyzer, frame_bytes, **kwargs):
        selected_frames.append(frame_bytes)
        return "가장 유사한 프레임 캡션"

    monkeypatch.setattr(process_assets, "_generate_event_frame_image_caption", _fake_generate_caption)

    clip_path = tmp_path / "clip.mp4"
    clip_path.write_bytes(b"stub")

    frame_rows, uploaded_keys, uploaded_caption_keys = process_assets._extract_clip_frames(
        dummy_minio,
        clip_id="clip-1",
        source_asset_id="asset-1",
        clip_path=clip_path,
        clip_key="unit/test/clips/sample.mp4",
        duration_sec=1.0,
        fps=30.0,
        frame_count=30,
        max_frames=3,
        jpeg_quality=90,
        image_profile="current",
        frame_interval_sec=0.4,
        image_caption_analyzer=object(),
        image_caption_event_category="smoke",
        image_caption_event_caption_text="연기가 퍼지는 이벤트",
        image_caption_parent_label_key="unit/test/events/sample.json",
        store_image_caption_json=True,
    )

    assert len(frame_rows) == 3
    assert len(uploaded_keys) == 3
    assert uploaded_caption_keys == ["unit/test/image_captions/sample_00000002.json"]
    assert [row["image_caption_text"] for row in frame_rows] == [
        None,
        "가장 유사한 프레임 캡션",
        None,
    ]
    assert [row["image_caption_score"] for row in frame_rows] == [
        pytest.approx(0.2),
        pytest.approx(0.9),
        pytest.approx(0.5),
    ]
    assert frame_rows[1]["image_caption_bucket"] == "vlm-labels"
    assert frame_rows[1]["image_caption_key"] == "unit/test/image_captions/sample_00000002.json"
    assert frame_rows[0]["image_caption_key"] is None
    assert selected_frames == [b"frame-2"]
    assert ("vlm-labels", "unit/test/image_captions/sample_00000002.json") in dummy_minio.uploaded


def test_processed_clip_image_key_uses_sibling_image_directory() -> None:
    pytest.importorskip("dagster")
    from vlm_pipeline.defs.process.assets import _build_processed_clip_image_key

    image_key = _build_processed_clip_image_key(
        "adlib-hotel-202512/20251222/clips/sample_e001_00000000_00002000.mp4",
        1,
    )

    assert image_key == "adlib-hotel-202512/20251222/image/sample_e001_00000000_00002000_00000001.jpg"


def test_image_caption_key_uses_sibling_image_captions_directory() -> None:
    pytest.importorskip("dagster")
    from vlm_pipeline.defs.process.assets import _build_image_caption_key

    caption_key = _build_image_caption_key(
        "adlib-hotel-202512/20251222/image/sample_e001_00000000_00002000_00000001.jpg"
    )

    assert caption_key == "adlib-hotel-202512/20251222/image_captions/sample_e001_00000000_00002000_00000001.json"


def test_score_event_frame_image_relevance_with_retry_skips_after_vertex_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("dagster")
    from vlm_pipeline.defs.process import assets as process_assets

    calls: list[int] = []
    sleeps: list[float] = []
    logs: list[str] = []

    class _DummyLog:
        def warning(self, message, *args):
            rendered = message % args if args else message
            logs.append(rendered)

    def _raise_rate_limit(*args, **kwargs):
        calls.append(1)
        raise RuntimeError("429 Resource exhausted")

    monkeypatch.setattr(process_assets, "_score_event_frame_image_relevance", _raise_rate_limit)
    monkeypatch.setattr(process_assets.time, "sleep", lambda sec: sleeps.append(sec))

    got = process_assets._score_event_frame_image_relevance_with_retry(
        analyzer=object(),
        frame_bytes=b"frame",
        clip_id="clip-1",
        frame_index=3,
        event_category="smoke",
        event_caption_text="연기가 번지는 장면",
        image_caption_log=_DummyLog(),
    )

    assert got is None
    assert len(calls) == 2
    assert sleeps == [process_assets._VERTEX_RELEVANCE_RETRY_DELAY_SEC]
    assert any("frame image relevance 429" in line for line in logs)
    assert any("frame image relevance skip" in line for line in logs)


def test_track_empty_output_failure_suppresses_after_three_consecutive_failures() -> None:
    pytest.importorskip("dagster")
    from vlm_pipeline.defs.process import assets as process_assets

    consecutive_failures: dict[str, int] = {}
    suppressed_asset_ids: set[str] = set()

    assert not process_assets._track_empty_output_failure(
        "asset-1",
        "ffmpeg_frame_extract_failed:empty_output",
        consecutive_failures=consecutive_failures,
        suppressed_asset_ids=suppressed_asset_ids,
    )
    assert not process_assets._track_empty_output_failure(
        "asset-1",
        "ffmpeg_frame_extract_failed:empty_output",
        consecutive_failures=consecutive_failures,
        suppressed_asset_ids=suppressed_asset_ids,
    )
    assert process_assets._track_empty_output_failure(
        "asset-1",
        "ffmpeg_frame_extract_failed:empty_output",
        consecutive_failures=consecutive_failures,
        suppressed_asset_ids=suppressed_asset_ids,
    )

    assert consecutive_failures["asset-1"] == 3
    assert "asset-1" in suppressed_asset_ids


def test_track_empty_output_failure_resets_on_other_errors() -> None:
    pytest.importorskip("dagster")
    from vlm_pipeline.defs.process import assets as process_assets

    consecutive_failures: dict[str, int] = {"asset-1": 2}
    suppressed_asset_ids: set[str] = set()

    suppressed = process_assets._track_empty_output_failure(
        "asset-1",
        "vertex_timeout",
        consecutive_failures=consecutive_failures,
        suppressed_asset_ids=suppressed_asset_ids,
    )

    assert not suppressed
    assert consecutive_failures["asset-1"] == 0
    assert not suppressed_asset_ids
