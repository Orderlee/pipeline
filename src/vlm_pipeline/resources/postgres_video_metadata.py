"""PG video_metadata CRUD 및 프레임 추출 쿼리 mixin (DuckDBVideoMetadataMixin 1:1 포팅).

변환 규칙:
  - placeholder ``?`` → ``%s``
  - ``conn.execute(sql, [...]).fetchall()`` → cursor 패턴
  - DuckDB BOOLEAN 비교는 그대로 (PG strict bool — Python ``True/False`` 그대로 전달)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


class PostgresVideoMetadataMixin:
    """video_metadata insert/update 및 프레임 후보 쿼리."""

    def insert_video_metadata(self, asset_id: str, meta: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO video_metadata (
                        asset_id, width, height, duration_sec, fps,
                        codec, bitrate, frame_count, has_audio,
                        environment_type, daynight_type, outdoor_score,
                        avg_brightness, env_method, extracted_at,
                        frame_extract_status, frame_extract_count,
                        frame_extract_error, frame_extracted_at,
                        original_codec, original_profile, original_has_b_frames,
                        original_level_int, reencode_required, reencode_reason,
                        reencode_applied, reencode_preset
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        asset_id,
                        meta.get("width"),
                        meta.get("height"),
                        meta.get("duration_sec"),
                        meta.get("fps"),
                        meta.get("codec"),
                        meta.get("bitrate"),
                        meta.get("frame_count"),
                        meta.get("has_audio", False),
                        meta.get("environment_type"),
                        meta.get("daynight_type"),
                        meta.get("outdoor_score"),
                        meta.get("avg_brightness"),
                        meta.get("env_method"),
                        meta.get("extracted_at", datetime.now()),
                        meta.get("frame_extract_status", "pending"),
                        meta.get("frame_extract_count", 0),
                        meta.get("frame_extract_error"),
                        meta.get("frame_extracted_at"),
                        meta.get("original_codec"),
                        meta.get("original_profile"),
                        meta.get("original_has_b_frames", False),
                        meta.get("original_level_int"),
                        meta.get("reencode_required", False),
                        meta.get("reencode_reason"),
                        meta.get("reencode_applied", False),
                        meta.get("reencode_preset"),
                    ),
                )
            columns = self._table_columns(conn, "video_metadata")
            reencode_cols = {
                "original_codec", "original_profile", "original_has_b_frames",
                "original_level_int", "reencode_required", "reencode_reason",
            }
            if reencode_cols.issubset(columns):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE video_metadata SET
                            original_codec        = %s,
                            original_profile      = %s,
                            original_has_b_frames = %s,
                            original_level_int    = %s,
                            reencode_required     = %s,
                            reencode_reason       = %s
                        WHERE asset_id = %s
                        """,
                        (
                            meta.get("original_codec"),
                            meta.get("original_profile"),
                            meta.get("original_has_b_frames", False),
                            meta.get("original_level_int"),
                            meta.get("reencode_required", False),
                            meta.get("reencode_reason"),
                            asset_id,
                        ),
                    )

    def update_video_reencode_applied(
        self,
        asset_id: str,
        *,
        codec: str = "h264",
        reencode_preset: str = "standard",
    ) -> None:
        normalized_id = str(asset_id or "").strip()
        if not normalized_id:
            return
        with self.connect() as conn:
            columns = self._table_columns(conn, "video_metadata")
            with conn.cursor() as cur:
                if {"reencode_applied", "reencode_preset"}.issubset(columns):
                    cur.execute(
                        """
                        UPDATE video_metadata
                        SET codec            = %s,
                            reencode_applied = TRUE,
                            reencode_preset  = %s
                        WHERE asset_id = %s
                        """,
                        (codec, reencode_preset, normalized_id),
                    )
                else:
                    cur.execute(
                        "UPDATE video_metadata SET codec = %s WHERE asset_id = %s",
                        (codec, normalized_id),
                    )

    def update_video_frame_extract_status(
        self,
        asset_id: str,
        status: str,
        *,
        frame_count: int | None = None,
        error_message: str | None = None,
        extracted_at: datetime | None = None,
    ) -> None:
        normalized_id = str(asset_id or "").strip()
        if not normalized_id:
            return

        frame_count_value = frame_count if frame_count is not None else 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET frame_extract_status = %s,
                        frame_extract_count = %s,
                        frame_extract_error = %s,
                        frame_extracted_at = %s,
                        extracted_at = COALESCE(extracted_at, %s)
                    WHERE asset_id = %s
                    """,
                    (
                        status,
                        frame_count_value,
                        error_message,
                        extracted_at,
                        datetime.now(),
                        normalized_id,
                    ),
                )

    def find_raw_video_extract_pending(self, limit: int = 500, folder_name: str | None = None) -> list[dict[str, Any]]:
        """라벨(event) 없이 처리할 raw video 후보를 반환."""
        with self.connect() as conn:
            query_cond = "AND r.raw_key LIKE %s" if folder_name else ""
            params: list[Any] = [max(1, int(limit))]
            if folder_name:
                params.insert(0, f"{folder_name}/%")

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        r.archive_path,
                        vm.duration_sec,
                        vm.fps,
                        vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND COALESCE(vm.frame_extract_status, 'pending') = 'pending'
                      {query_cond}
                    ORDER BY r.created_at
                    LIMIT %s
                    """,
                    params,
                )
                rows = cur.fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key", "archive_path",
                "duration_sec", "fps", "frame_count",
            ]
            return [dict(zip(columns, row)) for row in rows]
