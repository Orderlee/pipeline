"""PG PROCESS 도메인 — labels insert, processable 조회, processed_clips CRUD.

``insert_label`` / ``find_processable`` / ``insert_processed_clip`` /
``find_processed_clip_by_checksum`` 은 defs/process/ 와 defs/label/ 양쪽에서
호출되는 공유 쿼리 모음이다.  dedup/phash 와 무관하므로 분리.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from vlm_pipeline.resources.postgres_base import PostgresBaseMixin


_LABELS_INSERT_SQL = """
INSERT INTO labels (
    label_id, asset_id, labels_bucket, labels_key,
    label_format, label_tool, label_source, review_status,
    event_index, event_count, timestamp_start_sec, timestamp_end_sec,
    caption_text, object_count, label_status, created_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (label_id) DO UPDATE SET
    asset_id = EXCLUDED.asset_id,
    labels_bucket = EXCLUDED.labels_bucket,
    labels_key = EXCLUDED.labels_key,
    label_format = EXCLUDED.label_format,
    label_tool = EXCLUDED.label_tool,
    label_source = EXCLUDED.label_source,
    review_status = EXCLUDED.review_status,
    event_index = EXCLUDED.event_index,
    event_count = EXCLUDED.event_count,
    timestamp_start_sec = EXCLUDED.timestamp_start_sec,
    timestamp_end_sec = EXCLUDED.timestamp_end_sec,
    caption_text = EXCLUDED.caption_text,
    object_count = EXCLUDED.object_count,
    label_status = EXCLUDED.label_status,
    created_at = EXCLUDED.created_at
"""


class PostgresProcessMixin:
    """PROCESS / LABELS 관련 PostgreSQL 메서드 mixin."""

    def insert_label(self, label: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _LABELS_INSERT_SQL,
                    (
                        label.get("label_id") or str(uuid4()),
                        label.get("asset_id"),
                        label.get("labels_bucket", "vlm-labels"),
                        label.get("labels_key"),
                        label.get("label_format"),
                        label.get("label_tool", "pre-built"),
                        label.get("label_source", "manual"),
                        label.get("review_status", "pending"),
                        label.get("event_index", 0),
                        label.get("event_count"),
                        label.get("timestamp_start_sec"),
                        label.get("timestamp_end_sec"),
                        label.get("caption_text"),
                        label.get("object_count", 0),
                        label.get("label_status", "completed"),
                        label.get("created_at", datetime.now()),
                    ),
                )

    def find_processable(
        self,
        folder_name: str | None = None,
        *,
        spec_id: str | None = None,
    ) -> list[dict]:
        with self.connect() as conn:
            where_clauses = [
                "r.ingest_status IN ('completed', 'ready_for_labeling')",
                "l.label_status = 'completed'",
                """
                  NOT EXISTS (
                      SELECT 1
                      FROM processed_clips pc
                      WHERE pc.source_label_id = l.label_id
                        AND pc.process_status = 'completed'
                  )
                """.strip(),
            ]
            params: list[Any] = []
            folder_clause, folder_params = PostgresBaseMixin._folder_filter(folder_name)
            if folder_clause:
                where_clauses.append(folder_clause.removeprefix("AND ").strip())
                params.extend(folder_params)
            if spec_id:
                where_clauses.append("r.spec_id = %s")
                params.append(str(spec_id))

            where_sql = "\n                  AND ".join(where_clauses)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        r.media_type,
                        r.archive_path,
                        r.source_path,
                        l.label_id,
                        l.labels_bucket,
                        l.labels_key,
                        l.label_source,
                        l.review_status,
                        l.event_index,
                        l.timestamp_start_sec,
                        l.timestamp_end_sec,
                        l.caption_text,
                        l.object_count,
                        vm.duration_sec AS video_duration_sec,
                        vm.width AS video_width,
                        vm.height AS video_height,
                        vm.codec AS video_codec
                    FROM raw_files r
                    JOIN labels l ON r.asset_id = l.asset_id
                    LEFT JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE {where_sql}
                    """,
                    params,
                )
                rows = cur.fetchall()
            columns = [
                "asset_id",
                "raw_bucket",
                "raw_key",
                "media_type",
                "archive_path",
                "source_path",
                "label_id",
                "labels_bucket",
                "labels_key",
                "label_source",
                "review_status",
                "event_index",
                "timestamp_start_sec",
                "timestamp_end_sec",
                "caption_text",
                "object_count",
                "video_duration_sec",
                "video_width",
                "video_height",
                "video_codec",
            ]
            return self._rows_to_dicts(rows, columns)

    def insert_processed_clip(self, clip: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO processed_clips (
                        clip_id, source_asset_id, source_label_id, event_index,
                        clip_start_sec, clip_end_sec, checksum, file_size,
                        processed_bucket, clip_key, label_key, data_source, caption_text,
                        width, height, codec, duration_sec, fps, frame_count,
                        image_extract_status, image_extract_count, image_extract_error, image_extracted_at,
                        process_status, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (clip_id) DO UPDATE SET
                        source_asset_id = EXCLUDED.source_asset_id,
                        source_label_id = EXCLUDED.source_label_id,
                        event_index = EXCLUDED.event_index,
                        clip_start_sec = EXCLUDED.clip_start_sec,
                        clip_end_sec = EXCLUDED.clip_end_sec,
                        checksum = EXCLUDED.checksum,
                        file_size = EXCLUDED.file_size,
                        processed_bucket = EXCLUDED.processed_bucket,
                        clip_key = EXCLUDED.clip_key,
                        label_key = EXCLUDED.label_key,
                        data_source = EXCLUDED.data_source,
                        caption_text = EXCLUDED.caption_text,
                        width = EXCLUDED.width,
                        height = EXCLUDED.height,
                        codec = EXCLUDED.codec,
                        duration_sec = EXCLUDED.duration_sec,
                        fps = EXCLUDED.fps,
                        frame_count = EXCLUDED.frame_count,
                        image_extract_status = EXCLUDED.image_extract_status,
                        image_extract_count = EXCLUDED.image_extract_count,
                        image_extract_error = EXCLUDED.image_extract_error,
                        image_extracted_at = EXCLUDED.image_extracted_at,
                        process_status = EXCLUDED.process_status,
                        created_at = EXCLUDED.created_at
                    """,
                    (
                        clip.get("clip_id") or str(uuid4()),
                        clip.get("source_asset_id"),
                        clip.get("source_label_id"),
                        clip.get("event_index", 0),
                        clip.get("clip_start_sec"),
                        clip.get("clip_end_sec"),
                        clip.get("checksum"),
                        clip.get("file_size"),
                        clip.get("processed_bucket", "vlm-processed"),
                        clip.get("clip_key"),
                        clip.get("label_key"),
                        clip.get("data_source", "manual"),
                        clip.get("caption_text"),
                        clip.get("width"),
                        clip.get("height"),
                        clip.get("codec"),
                        clip.get("duration_sec"),
                        clip.get("fps"),
                        clip.get("frame_count"),
                        clip.get("image_extract_status", "pending"),
                        clip.get("image_extract_count", 0),
                        clip.get("image_extract_error"),
                        clip.get("image_extracted_at"),
                        clip.get("process_status", "completed"),
                        clip.get("created_at", datetime.now()),
                    ),
                )

    def find_processed_clip_by_checksum(
        self,
        checksum: str,
        *,
        source_asset_id: str | None = None,
        exclude_clip_id: str | None = None,
    ) -> dict[str, Any] | None:
        normalized_checksum = str(checksum or "").strip()
        if not normalized_checksum:
            return None

        with self.connect() as conn:
            where_clauses = ["checksum = %s"]
            params: list[Any] = [normalized_checksum]
            if source_asset_id:
                where_clauses.append("source_asset_id = %s")
                params.append(str(source_asset_id))
            if exclude_clip_id:
                where_clauses.append("clip_id != %s")
                params.append(str(exclude_clip_id))

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        clip_id,
                        source_asset_id,
                        source_label_id,
                        event_index,
                        clip_start_sec,
                        clip_end_sec,
                        checksum,
                        clip_key,
                        process_status
                    FROM processed_clips
                    WHERE {" AND ".join(where_clauses)}
                    ORDER BY created_at
                    LIMIT 1
                    """,
                    params,
                )
                row = cur.fetchone()
            if row is None:
                return None
            columns = [
                "clip_id",
                "source_asset_id",
                "source_label_id",
                "event_index",
                "clip_start_sec",
                "clip_end_sec",
                "checksum",
                "clip_key",
                "process_status",
            ]
            return self._row_to_dict(row, columns)
