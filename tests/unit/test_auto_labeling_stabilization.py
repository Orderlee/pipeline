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
    assert {"caption_text"} <= image_columns
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
                "caption_text": "연기가 보이는 프레임",
            }
        ],
    )

    rows = resource.list_processed_clip_frame_rows("clip-1")
    assert len(rows) == 1
    assert rows[0]["caption_text"] == "연기가 보이는 프레임"


def test_processed_clip_image_key_uses_sibling_image_directory() -> None:
    pytest.importorskip("dagster")
    from vlm_pipeline.defs.process.assets import _build_processed_clip_image_key

    image_key = _build_processed_clip_image_key(
        "adlib-hotel-202512/20251222/clips/sample_e001_00000000_00002000.mp4",
        1,
    )

    assert image_key == "adlib-hotel-202512/20251222/image/sample_e001_00000000_00002000_00000001.jpg"
