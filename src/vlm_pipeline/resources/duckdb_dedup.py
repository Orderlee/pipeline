"""DuckDB DEDUP 도메인 — phash, dup_group, similar 검색 등."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


class DuckDBDedupMixin:
    """DEDUP 관련 DuckDB 메서드 mixin."""

    def find_phash_null(self, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                FROM raw_files
                WHERE phash IS NULL
                  AND media_type = 'image'
                  AND ingest_status = 'completed'
                ORDER BY created_at
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "archive_path", "source_path"]
            return [dict(zip(columns, row)) for row in rows]

    def update_phash(self, asset_id: str, phash: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE raw_files SET phash = ?, updated_at = ? WHERE asset_id = ?",
                [phash, datetime.now(), asset_id],
            )

    def clear_error_message(self, asset_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE raw_files
                SET error_message = NULL, updated_at = ?
                WHERE asset_id = ?
                  AND error_message LIKE 'phash_%'
                """,
                [datetime.now(), asset_id],
            )

    def update_dup_group(self, asset_id: str, group_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE raw_files SET dup_group_id = ?, updated_at = ? WHERE asset_id = ?",
                [group_id, datetime.now(), asset_id],
            )

    def find_similar_phash(
        self,
        phash_hex: str,
        threshold: int = 5,
        exclude_asset_id: str | None = None,
        prefix_hex_len: int = 2,
    ) -> list[dict]:
        if not phash_hex:
            return []

        normalized = str(phash_hex).strip().lower()
        try:
            target_int = int(normalized, 16)
        except ValueError:
            return []

        candidates = self._phash_prefix_candidates(
            phash_hex=normalized,
            threshold=threshold,
            prefix_hex_len=prefix_hex_len,
        )
        normalized_prefix_len = max(1, min(4, int(prefix_hex_len)))
        all_prefix_count = 1 << (normalized_prefix_len * 4)

        with self.connect() as conn:
            where_parts = ["phash IS NOT NULL"]
            params: list[Any] = []

            if exclude_asset_id:
                where_parts.append("asset_id != ?")
                params.append(exclude_asset_id)

            if candidates and len(candidates) < all_prefix_count:
                placeholders = ", ".join(["?"] * len(candidates))
                where_parts.append(
                    f"lower(substr(phash, 1, {normalized_prefix_len})) IN ({placeholders})"
                )
                params.extend(candidates)

            query = (
                "SELECT asset_id, phash FROM raw_files "
                f"WHERE {' AND '.join(where_parts)}"
            )
            rows = conn.execute(query, params).fetchall()

        result: list[dict[str, Any]] = []
        for asset_id, other_phash in rows:
            if not other_phash:
                continue
            try:
                distance = (target_int ^ int(str(other_phash), 16)).bit_count()
            except ValueError:
                continue
            if distance <= threshold:
                result.append(
                    {"asset_id": asset_id, "phash": other_phash, "distance": distance}
                )
        result.sort(key=lambda row: (row["distance"], str(row["asset_id"])))
        return result

    def insert_label(self, label: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO labels (
                    label_id, asset_id, labels_bucket, labels_key,
                    label_format, label_tool, label_source, review_status,
                    event_index, event_count, timestamp_start_sec, timestamp_end_sec,
                    caption_text, object_count, label_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
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
                ],
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
            if folder_name:
                where_clauses.append("r.raw_key LIKE ?")
                params.append(f"{folder_name}/%")
            if spec_id:
                columns = self._table_columns(conn, "raw_files")
                if "spec_id" not in columns:
                    return []
                where_clauses.append("r.spec_id = ?")
                params.append(str(spec_id))

            where_sql = "\n                  AND ".join(where_clauses)
            rows = conn.execute(
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
                    vm.width AS video_width,
                    vm.height AS video_height,
                    vm.codec AS video_codec
                FROM raw_files r
                JOIN labels l ON r.asset_id = l.asset_id
                LEFT JOIN video_metadata vm ON vm.asset_id = r.asset_id
                WHERE {where_sql}
                """,
                params,
            ).fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key", "media_type",
                "archive_path", "source_path",
                "label_id", "labels_bucket", "labels_key",
                "label_source", "review_status", "event_index",
                "timestamp_start_sec", "timestamp_end_sec",
                "caption_text", "object_count",
                "video_width", "video_height", "video_codec",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def insert_processed_clip(self, clip: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO processed_clips (
                    clip_id, source_asset_id, source_label_id, event_index,
                    clip_start_sec, clip_end_sec, checksum, file_size,
                    processed_bucket, clip_key, label_key, data_source, caption_text,
                    width, height, codec, duration_sec, fps, frame_count,
                    image_extract_status, image_extract_count, image_extract_error, image_extracted_at,
                    process_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                [
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
                ],
            )

    def find_dataset_candidates(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    pc.clip_id,
                    pc.source_asset_id,
                    pc.source_label_id,
                    pc.processed_bucket,
                    pc.clip_key,
                    pc.label_key,
                    pc.data_source,
                    pc.caption_text,
                    pc.clip_start_sec,
                    pc.clip_end_sec,
                    pc.width,
                    pc.height,
                    pc.codec,
                    l.label_source,
                    l.review_status,
                    l.label_format,
                    l.object_count
                FROM processed_clips pc
                LEFT JOIN labels l ON l.label_id = pc.source_label_id
                WHERE pc.process_status = 'completed'
                """
            ).fetchall()
            columns = [
                "clip_id", "source_asset_id", "source_label_id", "processed_bucket", "clip_key",
                "label_key", "data_source", "caption_text", "clip_start_sec", "clip_end_sec",
                "width", "height", "codec", "label_source", "review_status", "label_format", "object_count",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def insert_dataset(self, dataset: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO datasets (
                    dataset_id, name, version, config, split_ratio,
                    dataset_bucket, dataset_prefix, build_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    dataset.get("dataset_id") or str(uuid4()),
                    dataset.get("name"),
                    dataset.get("version"),
                    dataset.get("config"),
                    dataset.get("split_ratio"),
                    dataset.get("dataset_bucket", "vlm-dataset"),
                    dataset.get("dataset_prefix"),
                    dataset.get("build_status", "pending"),
                    dataset.get("created_at", datetime.now()),
                ],
            )

    def update_dataset_status(self, dataset_id: str, status: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE datasets SET build_status = ? WHERE dataset_id = ?",
                [status, dataset_id],
            )

    def insert_dataset_clips_batch(self, clips: list[dict]) -> None:
        if not clips:
            return
        rows = [
            [
                clip["dataset_id"],
                clip["clip_id"],
                clip["split"],
                clip.get("dataset_key"),
            ]
            for clip in clips
        ]
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO dataset_clips (dataset_id, clip_id, split, dataset_key)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
