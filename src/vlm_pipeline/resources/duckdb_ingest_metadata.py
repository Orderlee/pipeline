"""DuckDB INGEST 도메인 — image/video metadata CRUD 및 process 관련 쿼리."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from vlm_pipeline.resources.duckdb_video_metadata import DuckDBVideoMetadataMixin


class DuckDBIngestMetadataMixin(DuckDBVideoMetadataMixin):
    """image_metadata, video_metadata CRUD 및 프레임/캡션/프로세스 쿼리 mixin."""

    @staticmethod
    def _normalized_image_caption_text(payload: dict[str, Any]) -> Any:
        image_caption_text = payload.get("image_caption_text")
        if image_caption_text not in (None, ""):
            return image_caption_text
        return payload.get("caption_text")

    # ------------------------------------------------------------------
    # image_metadata
    # ------------------------------------------------------------------

    def insert_image_metadata(self, asset_id: str, meta: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO image_metadata (
                    image_id, source_asset_id, source_clip_id, image_bucket, image_key,
                    image_role, frame_index, frame_sec, checksum, file_size,
                    width, height, color_mode, bit_depth,
                    has_alpha, orientation, image_caption_text, image_caption_score,
                    image_caption_bucket, image_caption_key, image_caption_generated_at, extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    meta.get("image_id") or asset_id,
                    asset_id,
                    meta.get("source_clip_id"),
                    meta.get("image_bucket", "vlm-raw"),
                    meta.get("image_key"),
                    meta.get("image_role", "source_image"),
                    meta.get("frame_index"),
                    meta.get("frame_sec"),
                    meta.get("checksum"),
                    meta.get("file_size"),
                    meta.get("width"),
                    meta.get("height"),
                    meta.get("color_mode", "RGB"),
                    meta.get("bit_depth", 8),
                    meta.get("has_alpha", False),
                    meta.get("orientation", 1),
                    self._normalized_image_caption_text(meta),
                    meta.get("image_caption_score"),
                    meta.get("image_caption_bucket"),
                    meta.get("image_caption_key"),
                    meta.get("image_caption_generated_at"),
                    meta.get("extracted_at", datetime.now()),
                ],
            )

    def find_image_metadata_by_image_key(self, image_key: str) -> dict[str, Any] | None:
        normalized_key = str(image_key or "").strip()
        if not normalized_key:
            return None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM image_metadata
                WHERE image_key = ?
                LIMIT 1
                """,
                [normalized_key],
            ).fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, row))

    def find_image_metadata_by_image_id(self, image_id: str) -> dict[str, Any] | None:
        normalized_id = str(image_id or "").strip()
        if not normalized_id:
            return None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM image_metadata
                WHERE image_id = ?
                LIMIT 1
                """,
                [normalized_id],
            ).fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, row))

    def find_image_metadata_by_stem(
        self,
        stem: str,
        *,
        source_unit_name: str | None = None,
    ) -> dict[str, Any] | None:
        normalized_stem = str(stem or "").strip()
        if not normalized_stem:
            return None
        with self.connect() as conn:
            query = """
                SELECT im.*
                FROM image_metadata im
                JOIN raw_files r ON r.asset_id = im.source_asset_id
                WHERE regexp_extract(im.image_key, '[^/]+$') LIKE ? || '.%'
            """
            params: list[Any] = [normalized_stem]
            if source_unit_name:
                columns = self._table_columns(conn, "raw_files")
                if "source_unit_name" in columns:
                    query += " AND r.source_unit_name = ?"
                    params.append(str(source_unit_name))
            query += " ORDER BY im.extracted_at DESC LIMIT 1"
            row = conn.execute(query, params).fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, row))

    def upsert_image_metadata_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        payload_rows = []
        for row in rows:
            payload_rows.append(
                [
                    row.get("image_id") or str(uuid4()),
                    row.get("source_asset_id"),
                    row.get("source_clip_id"),
                    row.get("image_bucket", "vlm-processed"),
                    row.get("image_key"),
                    row.get("image_role", "processed_clip_frame"),
                    row.get("frame_index"),
                    row.get("frame_sec"),
                    row.get("checksum"),
                    row.get("file_size"),
                    row.get("width"),
                    row.get("height"),
                    row.get("color_mode", "RGB"),
                    row.get("bit_depth", 8),
                    row.get("has_alpha", False),
                    row.get("orientation", 1),
                    self._normalized_image_caption_text(row),
                    row.get("image_caption_score"),
                    row.get("image_caption_bucket"),
                    row.get("image_caption_key"),
                    row.get("image_caption_generated_at"),
                    row.get("extracted_at", datetime.now()),
                ]
            )
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO image_metadata (
                    image_id, source_asset_id, source_clip_id, image_bucket, image_key,
                    image_role, frame_index, frame_sec, checksum, file_size,
                    width, height, color_mode, bit_depth,
                    has_alpha, orientation, image_caption_text, image_caption_score,
                    image_caption_bucket, image_caption_key, image_caption_generated_at, extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload_rows,
            )
        return len(payload_rows)

    def update_image_caption_metadata(
        self,
        image_id: str,
        *,
        image_caption_text: str,
        caption_score: float | None,
        caption_bucket: str,
        caption_key: str,
        generated_at: datetime | None,
    ) -> None:
        normalized_id = str(image_id or "").strip()
        if not normalized_id:
            return
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE image_metadata
                SET image_caption_text = ?,
                    image_caption_score = ?,
                    image_caption_bucket = ?,
                    image_caption_key = ?,
                    image_caption_generated_at = ?,
                    extracted_at = COALESCE(extracted_at, CURRENT_TIMESTAMP)
                WHERE image_id = ?
                """,
                [
                    image_caption_text,
                    caption_score,
                    caption_bucket,
                    caption_key,
                    generated_at,
                    normalized_id,
                ],
            )

    def list_video_frame_rows(self, source_asset_id: str) -> list[dict[str, Any]]:
        normalized_id = str(source_asset_id or "").strip()
        if not normalized_id:
            return []
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    image_id, source_asset_id, source_clip_id, image_bucket, image_key, image_role,
                    frame_index, frame_sec, checksum, file_size,
                    width, height, color_mode, bit_depth,
                    has_alpha, orientation, image_caption_text, image_caption_score,
                    image_caption_bucket, image_caption_key, image_caption_generated_at, extracted_at
                FROM image_metadata
                WHERE source_asset_id = ?
                  AND image_role IN ('video_frame', 'video_event_frame')
                  AND source_clip_id IS NULL
                ORDER BY frame_index, image_key, image_id
                """,
                [normalized_id],
            ).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in rows]

    def list_processed_clip_frame_rows(self, source_clip_id: str) -> list[dict[str, Any]]:
        normalized_clip_id = str(source_clip_id or "").strip()
        if not normalized_clip_id:
            return []
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    image_id, source_asset_id, source_clip_id, image_bucket, image_key, image_role,
                    frame_index, frame_sec, checksum, file_size,
                    width, height, color_mode, bit_depth,
                    has_alpha, orientation, image_caption_text, image_caption_score,
                    image_caption_bucket, image_caption_key, image_caption_generated_at, extracted_at
                FROM image_metadata
                WHERE source_clip_id = ?
                  AND image_role = 'processed_clip_frame'
                ORDER BY frame_index, image_key, image_id
                """,
                [normalized_clip_id],
            ).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in rows]

    def replace_video_frame_metadata(
        self,
        source_asset_id: str,
        frames: list[dict[str, Any]],
        *,
        image_role: str = "video_frame",
    ) -> int:
        return self._replace_frame_metadata(
            source_asset_id=source_asset_id,
            source_clip_id=None,
            image_role=image_role,
            frames=frames,
        )

    def replace_processed_clip_frame_metadata(
        self,
        source_asset_id: str,
        source_clip_id: str,
        frames: list[dict[str, Any]],
    ) -> int:
        return self._replace_frame_metadata(
            source_asset_id=source_asset_id,
            source_clip_id=source_clip_id,
            image_role="processed_clip_frame",
            frames=frames,
        )

    def _replace_frame_metadata(
        self,
        *,
        source_asset_id: str,
        source_clip_id: str | None,
        image_role: str,
        frames: list[dict[str, Any]],
    ) -> int:
        normalized_id = str(source_asset_id or "").strip()
        if not normalized_id:
            return 0
        normalized_clip_id = str(source_clip_id or "").strip() or None

        rows = []
        for frame in frames:
            rows.append(
                [
                    frame.get("image_id") or str(uuid4()),
                    normalized_id,
                    frame.get("source_clip_id", normalized_clip_id),
                    frame.get("image_bucket", "vlm-processed"),
                    frame.get("image_key"),
                    frame.get("image_role", image_role),
                    frame.get("frame_index"),
                    frame.get("frame_sec"),
                    frame.get("checksum"),
                    frame.get("file_size"),
                    frame.get("width"),
                    frame.get("height"),
                    frame.get("color_mode", "RGB"),
                    frame.get("bit_depth", 8),
                    frame.get("has_alpha", False),
                    frame.get("orientation", 1),
                    self._normalized_image_caption_text(frame),
                    frame.get("image_caption_score"),
                    frame.get("image_caption_bucket"),
                    frame.get("image_caption_key"),
                    frame.get("image_caption_generated_at"),
                    frame.get("extracted_at", datetime.now()),
                ]
            )

        with self.connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    (
                        """
                        DELETE FROM image_metadata
                        WHERE source_asset_id = ?
                          AND image_role = ?
                          AND source_clip_id = ?
                        """
                        if normalized_clip_id is not None
                        else
                        """
                        DELETE FROM image_metadata
                        WHERE source_asset_id = ?
                          AND image_role = ?
                          AND source_clip_id IS NULL
                        """
                    ),
                    [normalized_id, image_role, normalized_clip_id]
                    if normalized_clip_id is not None
                    else [normalized_id, image_role],
                )
                if rows:
                    conn.executemany(
                        """
                        INSERT INTO image_metadata (
                            image_id, source_asset_id, source_clip_id, image_bucket, image_key,
                            image_role, frame_index, frame_sec, checksum, file_size,
                            width, height, color_mode, bit_depth,
                            has_alpha, orientation, image_caption_text, image_caption_score,
                            image_caption_bucket, image_caption_key, image_caption_generated_at, extracted_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return len(rows)

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # process-related queries (frame candidates)
    # ------------------------------------------------------------------

    def find_video_frame_candidates(
        self,
        *,
        limit: int = 200,
        overwrite_existing: bool = False,
    ) -> list[dict[str, Any]]:
        normalized_limit = max(1, int(limit))
        params: list[Any] = [normalized_limit]
        backlog_assets_cte = ""
        backlog_join_sql = ""
        if not overwrite_existing:
            backlog_assets_cte = """
                , backlog_assets AS (
                    SELECT DISTINCT
                        r.asset_id
                    FROM raw_files r
                    JOIN labels l ON l.asset_id = r.asset_id
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    LEFT JOIN existing_frames
                      ON existing_frames.source_asset_id = r.asset_id
                     AND existing_frames.frame_index = COALESCE(l.event_index, 0) + 1
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND l.label_status = 'completed'
                      AND (l.timestamp_start_sec IS NOT NULL OR l.timestamp_end_sec IS NOT NULL)
                      AND COALESCE(existing_frames.frame_row_count, 0) = 0
                    ORDER BY r.asset_id
                    LIMIT ?
                )
            """
            params = [normalized_limit]
            backlog_join_sql = "JOIN backlog_assets ba ON ba.asset_id = r.asset_id"

        with self.connect() as conn:
            rows = conn.execute(
                f"""
                WITH existing_frames AS (
                    SELECT
                        source_asset_id,
                        frame_index,
                        COUNT(*) AS frame_row_count
                    FROM image_metadata
                    WHERE image_role = 'video_event_frame'
                      AND source_clip_id IS NULL
                    GROUP BY source_asset_id, frame_index
                )
                {backlog_assets_cte}
                SELECT
                    r.asset_id,
                    r.archive_path,
                    COALESCE(NULLIF(r.raw_bucket, ''), 'vlm-raw') AS raw_bucket,
                    r.raw_key,
                    r.original_name,
                    l.label_id,
                    COALESCE(l.event_index, 0) AS event_index,
                    l.timestamp_start_sec,
                    l.timestamp_end_sec,
                    vm.duration_sec,
                    vm.fps,
                    vm.frame_count,
                    COALESCE(vm.frame_extract_status, 'pending') AS frame_extract_status,
                    COALESCE(vm.frame_extract_count, 0) AS frame_extract_count,
                    COALESCE(existing_frames.frame_row_count, 0) AS extracted_frame_count
                FROM raw_files r
                {backlog_join_sql}
                JOIN labels l ON l.asset_id = r.asset_id
                JOIN video_metadata vm ON vm.asset_id = r.asset_id
                LEFT JOIN existing_frames
                  ON existing_frames.source_asset_id = r.asset_id
                 AND existing_frames.frame_index = COALESCE(l.event_index, 0) + 1
                WHERE r.media_type = 'video'
                  AND r.ingest_status = 'completed'
                  AND l.label_status = 'completed'
                  AND (l.timestamp_start_sec IS NOT NULL OR l.timestamp_end_sec IS NOT NULL)
                ORDER BY
                  r.asset_id,
                  l.event_index,
                  l.label_id
                {"" if not overwrite_existing else "LIMIT ?"}
                """,
                params if not overwrite_existing else [normalized_limit],
            ).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in rows]

    def find_processed_clip_frame_candidates(
        self,
        *,
        limit: int = 200,
        overwrite_existing: bool = False,
    ) -> list[dict[str, Any]]:
        normalized_limit = max(1, int(limit))
        params: list[Any] = [normalized_limit]
        overwrite_sql = ""
        if not overwrite_existing:
            overwrite_sql = "AND COALESCE(existing_frames.frame_row_count, 0) = 0"

        with self.connect() as conn:
            rows = conn.execute(
                f"""
                WITH existing_frames AS (
                    SELECT
                        source_clip_id,
                        COUNT(*) AS frame_row_count
                    FROM image_metadata
                    WHERE image_role = 'processed_clip_frame'
                      AND source_clip_id IS NOT NULL
                    GROUP BY source_clip_id
                )
                SELECT
                    pc.clip_id,
                    pc.source_asset_id,
                    COALESCE(NULLIF(pc.processed_bucket, ''), 'vlm-processed') AS processed_bucket,
                    pc.clip_key,
                    r.original_name,
                    vm.duration_sec,
                    vm.fps,
                    vm.frame_count,
                    COALESCE(existing_frames.frame_row_count, 0) AS extracted_frame_count
                FROM processed_clips pc
                JOIN raw_files r ON r.asset_id = pc.source_asset_id
                JOIN video_metadata vm ON vm.asset_id = pc.source_asset_id
                LEFT JOIN existing_frames ON existing_frames.source_clip_id = pc.clip_id
                WHERE pc.process_status = 'completed'
                  AND r.media_type = 'video'
                  AND r.ingest_status = 'completed'
                  AND pc.clip_key IS NOT NULL
                  AND pc.clip_key <> ''
                  {overwrite_sql}
                ORDER BY pc.created_at, pc.clip_id
                LIMIT ?
                """,
                params,
            ).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in rows]

