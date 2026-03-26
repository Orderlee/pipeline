"""DuckDB INGEST 도메인 — raw_files CRUD, metadata INSERT 등."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


class DuckDBIngestMixin:
    """INGEST 관련 DuckDB 메서드 mixin."""

    @staticmethod
    def _normalized_image_caption_text(payload: dict[str, Any]) -> Any:
        image_caption_text = payload.get("image_caption_text")
        if image_caption_text not in (None, ""):
            return image_caption_text
        return payload.get("caption_text")

    def ensure_dispatch_tracking_tables(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS staging_dispatch_requests (
                    request_id           VARCHAR PRIMARY KEY,
                    folder_name          VARCHAR,
                    run_mode             VARCHAR,
                    outputs              VARCHAR,
                    labeling_method      VARCHAR,
                    categories           VARCHAR,
                    classes              VARCHAR,
                    image_profile        VARCHAR,
                    status               VARCHAR DEFAULT 'pending',
                    archive_pending_path VARCHAR,
                    archive_path         VARCHAR,
                    max_frames_per_video INTEGER,
                    jpeg_quality         INTEGER,
                    confidence_threshold DOUBLE,
                    iou_threshold        DOUBLE,
                    requested_by         VARCHAR,
                    requested_at         TIMESTAMP,
                    processed_at         TIMESTAMP,
                    completed_at         TIMESTAMP,
                    error_message        TEXT,
                    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS staging_pipeline_runs (
                    run_id               VARCHAR PRIMARY KEY,
                    request_id           VARCHAR,
                    folder_name          VARCHAR,
                    step_name            VARCHAR NOT NULL,
                    step_order           INTEGER DEFAULT 0,
                    step_status          VARCHAR DEFAULT 'pending',
                    model_name           VARCHAR,
                    model_version        VARCHAR,
                    applied_params       VARCHAR,
                    input_count          INTEGER DEFAULT 0,
                    output_count         INTEGER DEFAULT 0,
                    error_count          INTEGER DEFAULT 0,
                    started_at           TIMESTAMP,
                    completed_at         TIMESTAMP,
                    error_message        TEXT,
                    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def get_in_flight_dispatch_requests(self, folder_name: str) -> list[dict[str, Any]]:
        normalized_folder = str(folder_name or "").strip()
        if not normalized_folder:
            return []
        with self.connect() as conn:
            if not self._table_exists(conn, "staging_dispatch_requests"):
                return []
            rows = conn.execute(
                """
                SELECT request_id, status
                FROM staging_dispatch_requests
                WHERE folder_name = ?
                  AND status IN ('running', 'archive_moved')
                ORDER BY processed_at DESC, created_at DESC
                """,
                [normalized_folder],
            ).fetchall()
        return [
            {"request_id": str(row[0]), "status": str(row[1]) if row[1] is not None else ""}
            for row in rows
        ]

    def get_dispatch_request_status(self, request_id: str) -> str | None:
        normalized_request_id = str(request_id or "").strip()
        if not normalized_request_id:
            return None
        with self.connect() as conn:
            if not self._table_exists(conn, "staging_dispatch_requests"):
                return None
            row = conn.execute(
                "SELECT status FROM staging_dispatch_requests WHERE request_id = ?",
                [normalized_request_id],
            ).fetchone()
        return str(row[0]) if row and row[0] is not None else None

    def get_active_staging_model_configs(self, output_types: list[str]) -> dict[str, dict[str, Any]]:
        normalized_types = [str(item or "").strip() for item in output_types if str(item or "").strip()]
        if not normalized_types:
            return {}
        placeholders = ", ".join("?" * len(normalized_types))
        with self.connect() as conn:
            if not self._table_exists(conn, "staging_model_configs"):
                return {}
            rows = conn.execute(
                f"""
                SELECT
                    output_type,
                    model_name,
                    model_version,
                    default_max_frames,
                    default_jpeg_quality,
                    default_confidence,
                    default_iou
                FROM staging_model_configs
                WHERE output_type IN ({placeholders})
                  AND is_active = TRUE
                """,
                normalized_types,
            ).fetchall()
        return {
            str(row[0]): {
                "model_name": row[1],
                "model_version": row[2],
                "default_max_frames": row[3],
                "default_jpeg_quality": row[4],
                "default_confidence": row[5],
                "default_iou": row[6],
            }
            for row in rows
        }

    def insert_dispatch_request(self, record: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO staging_dispatch_requests (
                    request_id, folder_name, run_mode, outputs, labeling_method,
                    categories, classes, image_profile,
                    status, archive_pending_path, archive_path,
                    max_frames_per_video, jpeg_quality, confidence_threshold, iou_threshold,
                    requested_by, requested_at, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    record.get("request_id"),
                    record.get("folder_name"),
                    record.get("run_mode"),
                    record.get("outputs"),
                    record.get("labeling_method"),
                    record.get("categories"),
                    record.get("classes"),
                    record.get("image_profile"),
                    record.get("status", "running"),
                    record.get("archive_pending_path"),
                    record.get("archive_path"),
                    record.get("max_frames_per_video"),
                    record.get("jpeg_quality"),
                    record.get("confidence_threshold"),
                    record.get("iou_threshold"),
                    record.get("requested_by"),
                    record.get("requested_at"),
                    record.get("processed_at"),
                ],
            )

    def insert_dispatch_pipeline_runs(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        payloads = [
            (
                row.get("run_id") or str(uuid4()),
                row.get("request_id"),
                row.get("folder_name"),
                row.get("step_name"),
                row.get("step_order", 0),
                row.get("step_status", "pending"),
                row.get("model_name"),
                row.get("model_version"),
                row.get("applied_params"),
            )
            for row in rows
        ]
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO staging_pipeline_runs (
                    run_id, request_id, folder_name,
                    step_name, step_order, step_status,
                    model_name, model_version, applied_params
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payloads,
            )
        return len(payloads)

    def upsert_failed_dispatch_request(self, record: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO staging_dispatch_requests (
                    request_id, folder_name, run_mode, outputs, labeling_method,
                    categories, classes, image_profile,
                    max_frames_per_video, jpeg_quality,
                    confidence_threshold, iou_threshold,
                    status, error_message, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'failed', ?, ?)
                ON CONFLICT (request_id) DO UPDATE SET
                    status = 'failed',
                    error_message = excluded.error_message,
                    processed_at = excluded.processed_at
                """,
                [
                    record.get("request_id"),
                    record.get("folder_name"),
                    record.get("run_mode"),
                    record.get("outputs"),
                    record.get("labeling_method"),
                    record.get("categories"),
                    record.get("classes"),
                    record.get("image_profile"),
                    record.get("max_frames_per_video"),
                    record.get("jpeg_quality"),
                    record.get("confidence_threshold"),
                    record.get("iou_threshold"),
                    record.get("error_message"),
                    record.get("processed_at"),
                ],
            )

    def insert_raw_files_batch(self, records: list[dict]) -> int:
        if not records:
            return 0
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            has_source_unit = "source_unit_name" in columns
            has_spec_id = "spec_id" in columns

            base_cols = [
                "asset_id", "source_path", "original_name", "media_type",
                "file_size", "checksum", "archive_path", "raw_bucket", "raw_key",
                "ingest_batch_id", "transfer_tool", "ingest_status", "error_message",
                "created_at", "updated_at",
            ]
            if has_source_unit:
                base_cols.append("source_unit_name")
            if has_spec_id:
                base_cols.append("spec_id")

            placeholders = ", ".join("?" * len(base_cols))
            insert_cols = ", ".join(base_cols)
            sql = f"INSERT INTO raw_files ({insert_cols}) VALUES ({placeholders})"

            rows = []
            for rec in records:
                now = datetime.now()
                row = [
                    rec.get("asset_id") or str(uuid4()),
                    rec.get("source_path"),
                    rec.get("original_name"),
                    rec.get("media_type", "image"),
                    rec.get("file_size"),
                    rec.get("checksum"),
                    rec.get("archive_path"),
                    rec.get("raw_bucket", "vlm-raw"),
                    rec.get("raw_key"),
                    rec.get("ingest_batch_id"),
                    rec.get("transfer_tool", "manual"),
                    rec.get("ingest_status", "pending"),
                    rec.get("error_message"),
                    rec.get("created_at", now),
                    rec.get("updated_at", now),
                ]
                if has_source_unit:
                    row.append(rec.get("source_unit_name"))
                if has_spec_id:
                    row.append(rec.get("spec_id"))
                rows.append(row)

            conn.executemany(sql, rows)
            return len(rows)

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

    def insert_video_metadata(self, asset_id: str, meta: dict) -> None:
        with self.connect() as conn:
            conn.execute(
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
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
                ],
            )

    def find_by_checksum(self, checksum: str, completed_only: bool = True) -> dict[str, Any] | None:
        query = "SELECT * FROM raw_files WHERE checksum = ?"
        params: list[Any] = [checksum]
        if completed_only:
            query += " AND ingest_status = 'completed'"

        with self.connect() as conn:
            result = conn.execute(query, params).fetchone()
            if result is None:
                return None
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result))

    def find_any_by_checksum(self, checksum: str) -> dict[str, Any] | None:
        return self.find_by_checksum(checksum, completed_only=False)

    def has_raw_file(self, asset_id: str) -> bool:
        normalized_id = str(asset_id or "").strip()
        if not normalized_id:
            return False
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM raw_files WHERE asset_id = ? LIMIT 1",
                [normalized_id],
            ).fetchone()
        return row is not None

    def has_failed_duplicate_for_source(self, source_path: str, duplicate_asset_id: str) -> bool:
        normalized_source = str(source_path or "").strip()
        normalized_target = str(duplicate_asset_id or "").strip()
        if not normalized_source or not normalized_target:
            return False
        duplicate_marker = f"duplicate_of:{normalized_target}"
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM raw_files
                WHERE source_path = ?
                  AND ingest_status = 'failed'
                  AND error_message = ?
                LIMIT 1
                """,
                [normalized_source, duplicate_marker],
            ).fetchone()
        return row is not None

    def update_raw_file_status(
        self,
        asset_id: str,
        status: str,
        error_message: str | None = None,
        archive_path: str | None = None,
        raw_bucket: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE raw_files
                SET ingest_status = ?,
                    error_message = ?,
                    archive_path = COALESCE(?, archive_path),
                    raw_bucket = COALESCE(?, raw_bucket),
                    updated_at = ?
                WHERE asset_id = ?
                """,
                [status, error_message, archive_path, raw_bucket, datetime.now(), asset_id],
            )

    def batch_update_spec_and_status(
        self, updates: list[dict]
    ) -> int:
        """raw_files.ingest_status( 및 spec_id) 배치 업데이트. updates: [{"asset_id", "ingest_status", "spec_id"?}]. spec_id 컬럼 있을 때만 spec_id 반영."""
        if not updates:
            return 0
        now = datetime.now()
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            has_spec_id = "spec_id" in columns
            for u in updates:
                aid = u.get("asset_id")
                if not aid:
                    continue
                status = u.get("ingest_status", "pending")
                if has_spec_id and "spec_id" in u:
                    conn.execute(
                        "UPDATE raw_files SET ingest_status = ?, spec_id = ?, updated_at = ? WHERE asset_id = ?",
                        [status, u.get("spec_id"), now, aid],
                    )
                else:
                    conn.execute(
                        "UPDATE raw_files SET ingest_status = ?, updated_at = ? WHERE asset_id = ?",
                        [status, now, aid],
                    )
            return len([x for x in updates if x.get("asset_id")])

    def batch_update_status(self, updates: list[dict]) -> int:
        """배치 상태 업데이트. updates: [{"asset_id", "status", "archive_path"?, "error_message"?}]"""
        if not updates:
            return 0
        now = datetime.now()
        rows = []
        for u in updates:
            rows.append((
                u["status"],
                u.get("error_message"),
                u.get("archive_path"),
                u.get("raw_bucket"),
                now,
                u["asset_id"],
            ))
        with self.connect() as conn:
            conn.executemany(
                """
                UPDATE raw_files
                SET ingest_status = ?,
                    error_message = ?,
                    archive_path = COALESCE(?, archive_path),
                    raw_bucket = COALESCE(?, raw_bucket),
                    updated_at = ?
                WHERE asset_id = ?
                """,
                rows,
            )
        return len(rows)

    def update_dispatch_request_status(
        self,
        request_id: str,
        status: str,
        *,
        error_message: str | None = None,
        archive_path: str | None = None,
        completed_at: datetime | None = None,
        processed_at: datetime | None = None,
    ) -> None:
        normalized_request_id = str(request_id or "").strip()
        if not normalized_request_id:
            return
        now = datetime.now()
        with self.connect() as conn:
            if not self._table_exists(conn, "staging_dispatch_requests"):
                return
            conn.execute(
                """
                UPDATE staging_dispatch_requests
                SET status = ?,
                    error_message = ?,
                    archive_path = COALESCE(?, archive_path),
                    processed_at = COALESCE(?, processed_at),
                    completed_at = COALESCE(?, completed_at)
                WHERE request_id = ?
                """,
                [
                    status,
                    error_message,
                    archive_path,
                    processed_at or now,
                    completed_at,
                    normalized_request_id,
                ],
            )

    def update_dispatch_pipeline_step(
        self,
        request_id: str,
        step_name: str,
        step_status: str,
        *,
        error_message: str | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        normalized_request_id = str(request_id or "").strip()
        normalized_step_name = str(step_name or "").strip()
        if not normalized_request_id or not normalized_step_name:
            return
        with self.connect() as conn:
            if not self._table_exists(conn, "staging_pipeline_runs"):
                return
            conn.execute(
                """
                UPDATE staging_pipeline_runs
                SET step_status = ?,
                    error_message = ?,
                    started_at = COALESCE(?, started_at),
                    completed_at = COALESCE(?, completed_at)
                WHERE request_id = ?
                  AND step_name = ?
                """,
                [
                    step_status,
                    error_message,
                    started_at,
                    completed_at,
                    normalized_request_id,
                    normalized_step_name,
                ],
            )

    def close_dispatch_request(
        self,
        request_id: str,
        *,
        status: str,
        error_message: str | None = None,
    ) -> None:
        normalized_request_id = str(request_id or "").strip()
        if not normalized_request_id:
            return

        completed_at = datetime.now()
        self.update_dispatch_request_status(
            normalized_request_id,
            status,
            error_message=error_message,
            completed_at=completed_at,
            processed_at=completed_at,
        )

        with self.connect() as conn:
            if not self._table_exists(conn, "staging_pipeline_runs"):
                return
            conn.execute(
                """
                UPDATE staging_pipeline_runs
                SET step_status = CASE
                        WHEN step_status IN ('completed', 'failed', 'canceled', 'skipped') THEN step_status
                        ELSE ?
                    END,
                    error_message = CASE
                        WHEN step_status IN ('completed', 'failed', 'canceled', 'skipped') THEN error_message
                        ELSE COALESCE(?, error_message)
                    END,
                    completed_at = CASE
                        WHEN step_status IN ('completed', 'failed', 'canceled', 'skipped') THEN completed_at
                        ELSE COALESCE(completed_at, ?)
                    END
                WHERE request_id = ?
                """,
                [status, error_message, completed_at, normalized_request_id],
            )

    def count_unresolved_rows_for_source_unit(self, source_unit_path: str) -> int:
        normalized_path = str(source_unit_path or "").strip()
        if not normalized_path:
            return 0

        like_pattern = f"{normalized_path.rstrip('/')}/%"
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM raw_files
                WHERE (source_path = ? OR source_path LIKE ?)
                  AND NOT (
                      ingest_status IN ('completed', 'skipped')
                      OR (
                          ingest_status = 'failed'
                          AND error_message LIKE 'duplicate_of:%'
                      )
                  )
                """,
                [normalized_path, like_pattern],
            ).fetchone()
        return int(row[0]) if row else 0

    def count_raw_files_for_source_unit_name(self, source_unit_name: str) -> int:
        normalized_name = str(source_unit_name or "").strip()
        if not normalized_name:
            return 0
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            if "source_unit_name" not in columns:
                return 0
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM raw_files
                WHERE source_unit_name = ?
                """,
                [normalized_name],
            ).fetchone()
        return int(row[0]) if row else 0

    def mark_duplicate_skipped_assets(self, duplicate_asset_files: dict[str, list[str]]) -> int:
        normalized_items: dict[str, list[str]] = {}

        def _parse_file_names(payload: str) -> list[str]:
            names = [name.strip() for name in str(payload or "").split(",") if name.strip()]
            return names or ["unknown_file"]

        for asset_id, file_names in (duplicate_asset_files or {}).items():
            normalized_id = str(asset_id or "").strip()
            if not normalized_id:
                continue
            merged = normalized_items.setdefault(normalized_id, [])
            if isinstance(file_names, list):
                merged.extend(
                    str(file_name).strip() or "unknown_file"
                    for file_name in file_names
                )
            else:
                merged.extend(_parse_file_names(str(file_names)))

        if not normalized_items:
            return 0

        now = datetime.now()
        with self.connect() as conn:
            placeholders = ", ".join("?" * len(normalized_items))
            rows = conn.execute(
                f"SELECT asset_id, error_message FROM raw_files WHERE asset_id IN ({placeholders})",
                list(normalized_items.keys()),
            ).fetchall()

            updates: list[tuple] = []
            for asset_id, current_error_raw in rows:
                current_error = str(current_error_raw or "").strip()
                file_names = [
                    file_name
                    for file_name in normalized_items[asset_id]
                    if str(file_name).strip()
                ] or ["unknown_file"]
                if not current_error:
                    merged_files = list(file_names)
                elif current_error.startswith("duplicate_skipped_in_manifest:"):
                    existing_payload = current_error.replace("duplicate_skipped_in_manifest:", "", 1)
                    merged_files = _parse_file_names(existing_payload) + list(file_names)
                else:
                    continue

                marker = f"duplicate_skipped_in_manifest:{','.join(sorted(merged_files))}"
                if marker != current_error:
                    updates.append((marker, now, asset_id))

            if updates:
                conn.executemany(
                    "UPDATE raw_files SET error_message = ?, updated_at = ? WHERE asset_id = ?",
                    updates,
                )

        return len(updates)

    def delete_asset_for_reingest(self, asset_id: str) -> None:
        """실패/중단된 ingest 자산을 재처리 가능 상태로 정리 (트랜잭션 래핑)."""
        with self.connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    """
                    DELETE FROM dataset_clips
                    WHERE clip_id IN (
                        SELECT clip_id FROM processed_clips WHERE source_asset_id = ?
                    )
                    """,
                    [asset_id],
                )
                conn.execute("DELETE FROM image_metadata WHERE source_asset_id = ?", [asset_id])
                conn.execute("DELETE FROM processed_clips WHERE source_asset_id = ?", [asset_id])
                conn.execute("DELETE FROM labels WHERE asset_id = ?", [asset_id])
                conn.execute("DELETE FROM video_metadata WHERE asset_id = ?", [asset_id])
                conn.execute("DELETE FROM raw_files WHERE asset_id = ?", [asset_id])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

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

    def list_raw_files_by_source_unit_name(self, source_unit_name: str) -> list[dict[str, Any]]:
        normalized_name = str(source_unit_name or "").strip()
        if not normalized_name:
            return []
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            if "source_unit_name" not in columns:
                return []
            rows = conn.execute(
                """
                SELECT *
                FROM raw_files
                WHERE source_unit_name = ?
                ORDER BY created_at, raw_key, asset_id
                """,
                [normalized_name],
            ).fetchall()
            result_columns = [desc[0] for desc in conn.description]
            return [dict(zip(result_columns, row)) for row in rows]

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
            if {"reencode_applied", "reencode_preset"}.issubset(columns):
                conn.execute(
                    """
                    UPDATE video_metadata
                    SET codec = ?,
                        reencode_applied = TRUE,
                        reencode_preset = ?
                    WHERE asset_id = ?
                    """,
                    [codec, reencode_preset, normalized_id],
                )
            else:
                conn.execute(
                    "UPDATE video_metadata SET codec = ? WHERE asset_id = ?",
                    [codec, normalized_id],
                )

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
            conn.execute(
                """
                UPDATE video_metadata
                SET frame_extract_status = ?,
                    frame_extract_count = ?,
                    frame_extract_error = ?,
                    frame_extracted_at = ?,
                    extracted_at = COALESCE(extracted_at, ?)
                WHERE asset_id = ?
                """,
                [
                    status,
                    frame_count_value,
                    error_message,
                    extracted_at,
                    datetime.now(),
                    normalized_id,
                ],
            )

    def find_raw_video_extract_pending(self, limit: int = 500, folder_name: str | None = None) -> list[dict[str, Any]]:
        """yolo_run_mode 등에서 라벨(event) 없이 처리할 raw video 후보를 반환합니다."""
        with self.connect() as conn:
            query_cond = "AND r.raw_key LIKE ?" if folder_name else ""
            params = [max(1, int(limit))]
            if folder_name:
                params.insert(0, f"{folder_name}/%")

            rows = conn.execute(
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
                LIMIT ?
                """,
                params,
            ).fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key", "archive_path",
                "duration_sec", "fps", "frame_count",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def delete_failed_rows_by_error_filters(
        self,
        *,
        exact_errors: list[str] | None = None,
        like_patterns: list[str] | None = None,
    ) -> int:
        exact_values = [str(v).strip() for v in (exact_errors or []) if str(v).strip()]
        like_values = [str(v).strip() for v in (like_patterns or []) if str(v).strip()]
        if not exact_values and not like_values:
            return 0

        clauses: list[str] = []
        params: list[Any] = []
        if exact_values:
            placeholders = ", ".join(["?"] * len(exact_values))
            clauses.append(f"error_message IN ({placeholders})")
            params.extend(exact_values)
        for pattern in like_values:
            clauses.append("error_message LIKE ?")
            params.append(pattern)

        where_filter = " OR ".join(clauses)
        with self.connect() as conn:
            target_count = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM raw_files
                WHERE ingest_status = 'failed'
                  AND ({where_filter})
                """,
                params,
            ).fetchone()[0]
            if int(target_count) <= 0:
                return 0
            conn.execute(
                f"""
                DELETE FROM raw_files
                WHERE ingest_status = 'failed'
                  AND ({where_filter})
                """,
                params,
            )
            return int(target_count)

    def recover_archive_move_failed_asset(self, asset_id: str, archive_path: str) -> bool:
        normalized_asset_id = str(asset_id or "").strip()
        normalized_archive_path = str(archive_path or "").strip()
        if not normalized_asset_id or not normalized_archive_path:
            return False
        with self.connect() as conn:
            row = conn.execute(
                """
                UPDATE raw_files
                SET ingest_status = 'completed',
                    error_message = NULL,
                    archive_path = ?,
                    raw_bucket = COALESCE(raw_bucket, 'vlm-raw'),
                    updated_at = ?
                WHERE asset_id = ?
                  AND ingest_status = 'failed'
                RETURNING asset_id
                """,
                [normalized_archive_path, datetime.now(), normalized_asset_id],
            ).fetchone()
        return row is not None

    def list_completed_videos_for_spec_router(
        self, limit: int = 500
    ) -> list[dict[str, Any]]:
        """ingest_router용: ingest_status=completed, spec_id 미설정 비디오. source_unit_name 컬럼 있을 때만."""
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            if "source_unit_name" not in columns or "spec_id" not in columns:
                return []
            rows = conn.execute(
                """
                SELECT asset_id, COALESCE(source_unit_name, '') AS source_unit_name, raw_key
                FROM raw_files
                WHERE media_type = 'video'
                  AND ingest_status = 'completed'
                  AND (spec_id IS NULL OR spec_id = '')
                ORDER BY created_at
                LIMIT ?
                """,
                [max(1, int(limit))],
            ).fetchall()
            return [
                {"asset_id": r[0], "source_unit_name": r[1] or "", "raw_key": r[2]}
                for r in rows
            ]

    def find_by_raw_key_stem(self, stem: str, source_unit_name: str | None = None) -> dict[str, Any] | None:
        with self.connect() as conn:
            query = """
                SELECT * FROM raw_files
                WHERE regexp_extract(raw_key, '[^/]+$')
                      LIKE ? || '.%'
                  AND ingest_status = 'completed'
                """
            params: list[Any] = [stem]
            if source_unit_name:
                columns = self._table_columns(conn, "raw_files")
                if "source_unit_name" in columns:
                    query += " AND source_unit_name = ?"
                    params.append(str(source_unit_name))
            query += " LIMIT 1"
            result = conn.execute(query, params).fetchone()
            if result is None:
                return None
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result))
