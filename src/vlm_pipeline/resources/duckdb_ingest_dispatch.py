"""DuckDB INGEST 도메인 — dispatch tracking 관련 메서드."""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar
from uuid import uuid4

DISPATCH_REQUESTS_TABLE = "dispatch_requests"
LEGACY_DISPATCH_REQUESTS_TABLE = "staging_dispatch_requests"
DISPATCH_MODEL_CONFIGS_TABLE = "dispatch_model_configs"
LEGACY_DISPATCH_MODEL_CONFIGS_TABLE = "staging_model_configs"
DISPATCH_PIPELINE_RUNS_TABLE = "dispatch_pipeline_runs"
LEGACY_DISPATCH_PIPELINE_RUNS_TABLE = "staging_pipeline_runs"


class DuckDBIngestDispatchMixin:
    """Dispatch tracking tables CRUD mixin."""

    _dispatch_tables_ensured: ClassVar[bool] = False

    def ensure_dispatch_tracking_tables(self) -> None:
        if DuckDBIngestDispatchMixin._dispatch_tables_ensured:
            return
        with self.connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {DISPATCH_REQUESTS_TABLE} (
                    request_id           VARCHAR PRIMARY KEY,
                    folder_name          VARCHAR,
                    run_mode             VARCHAR,
                    outputs              VARCHAR,
                    labeling_method      VARCHAR,
                    dispatch_mode        VARCHAR DEFAULT 'standard',
                    requested_labeling_method_raw VARCHAR,
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
                    ls_task_status       VARCHAR DEFAULT 'pending',
                    bucket               VARCHAR DEFAULT 'vlm-processed',
                    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            request_columns = self._table_columns(conn, DISPATCH_REQUESTS_TABLE)
            request_alter_specs = {
                "labeling_method": "VARCHAR",
                "dispatch_mode": "VARCHAR DEFAULT 'standard'",
                "requested_labeling_method_raw": "VARCHAR",
                "categories": "VARCHAR",
                "classes": "VARCHAR",
                "max_frames_per_video": "INTEGER",
                "jpeg_quality": "INTEGER",
                "confidence_threshold": "DOUBLE",
                "iou_threshold": "DOUBLE",
                "completed_at": "TIMESTAMP",
                "ls_task_status": "VARCHAR DEFAULT 'pending'",
                "bucket": "VARCHAR DEFAULT 'vlm-processed'",
            }
            for column_name, column_type in request_alter_specs.items():
                if column_name in request_columns:
                    continue
                conn.execute(
                    f"ALTER TABLE {DISPATCH_REQUESTS_TABLE} ADD COLUMN {column_name} {column_type}"
                )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {DISPATCH_MODEL_CONFIGS_TABLE} (
                    config_id            VARCHAR PRIMARY KEY,
                    output_type          VARCHAR NOT NULL UNIQUE,
                    model_name           VARCHAR NOT NULL,
                    model_version        VARCHAR,
                    default_max_frames   INTEGER DEFAULT 24,
                    default_jpeg_quality INTEGER DEFAULT 90,
                    default_confidence   DOUBLE DEFAULT 0.25,
                    default_iou          DOUBLE DEFAULT 0.45,
                    extra_params         VARCHAR,
                    is_active            BOOLEAN DEFAULT TRUE,
                    description          TEXT,
                    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {DISPATCH_PIPELINE_RUNS_TABLE} (
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
            self._seed_dispatch_model_configs(conn)
            self._backfill_dispatch_requests_from_legacy(conn)
            self._backfill_dispatch_model_configs_from_legacy(conn)
            self._backfill_dispatch_pipeline_runs_from_legacy(conn)
        DuckDBIngestDispatchMixin._dispatch_tables_ensured = True

    def _seed_dispatch_model_configs(self, conn) -> None:
        seed_rows = [
            (
                "cfg_bbox",
                "bbox",
                "yolov8l-worldv2",
                "v2",
                24,
                90,
                0.25,
                0.45,
                None,
                True,
                "YOLO-World-L bbox detection — GPU 서버 호출",
            ),
            (
                "cfg_timestamp",
                "timestamp",
                "gemini-2.0-flash",
                "2.0",
                12,
                90,
                None,
                None,
                None,
                True,
                "Gemini 이벤트 구간(timestamp) 추출",
            ),
            (
                "cfg_captioning",
                "captioning",
                "gemini-2.0-flash",
                "2.0",
                12,
                90,
                None,
                None,
                None,
                True,
                "Gemini 캡셔닝 + clip 절단 + 프레임 추출",
            ),
        ]
        for row in seed_rows:
            conn.execute(
                f"""
                INSERT INTO {DISPATCH_MODEL_CONFIGS_TABLE} (
                    config_id, output_type, model_name, model_version,
                    default_max_frames, default_jpeg_quality,
                    default_confidence, default_iou,
                    extra_params, is_active, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (config_id) DO NOTHING
                """,
                list(row),
            )

    def _legacy_select_expr(self, legacy_columns: set[str], column_name: str, fallback_sql: str = "NULL") -> str:
        if column_name in legacy_columns:
            return column_name
        return fallback_sql

    def _backfill_dispatch_requests_from_legacy(self, conn) -> None:
        if not self._table_exists(conn, LEGACY_DISPATCH_REQUESTS_TABLE):
            return
        legacy_columns = self._table_columns(conn, LEGACY_DISPATCH_REQUESTS_TABLE)
        if not legacy_columns:
            return
        conn.execute(
            f"""
            INSERT INTO {DISPATCH_REQUESTS_TABLE} (
                request_id, folder_name, run_mode, outputs, labeling_method,
                dispatch_mode, requested_labeling_method_raw,
                categories, classes, image_profile, status,
                archive_pending_path, archive_path,
                max_frames_per_video, jpeg_quality, confidence_threshold, iou_threshold,
                requested_by, requested_at, processed_at, completed_at,
                error_message, ls_task_status, bucket, created_at
            )
            SELECT
                request_id,
                folder_name,
                run_mode,
                outputs,
                {self._legacy_select_expr(legacy_columns, 'labeling_method')},
                COALESCE({self._legacy_select_expr(legacy_columns, 'dispatch_mode', "'standard'")}, 'standard'),
                {self._legacy_select_expr(legacy_columns, 'requested_labeling_method_raw')},
                {self._legacy_select_expr(legacy_columns, 'categories')},
                {self._legacy_select_expr(legacy_columns, 'classes')},
                {self._legacy_select_expr(legacy_columns, 'image_profile')},
                COALESCE(status, 'pending'),
                {self._legacy_select_expr(legacy_columns, 'archive_pending_path')},
                {self._legacy_select_expr(legacy_columns, 'archive_path')},
                {self._legacy_select_expr(legacy_columns, 'max_frames_per_video')},
                {self._legacy_select_expr(legacy_columns, 'jpeg_quality')},
                {self._legacy_select_expr(legacy_columns, 'confidence_threshold')},
                {self._legacy_select_expr(legacy_columns, 'iou_threshold')},
                {self._legacy_select_expr(legacy_columns, 'requested_by')},
                {self._legacy_select_expr(legacy_columns, 'requested_at')},
                {self._legacy_select_expr(legacy_columns, 'processed_at')},
                {self._legacy_select_expr(legacy_columns, 'completed_at')},
                {self._legacy_select_expr(legacy_columns, 'error_message')},
                COALESCE({self._legacy_select_expr(legacy_columns, 'ls_task_status')}, 'pending'),
                COALESCE({self._legacy_select_expr(legacy_columns, 'bucket', "'vlm-processed'")}, 'vlm-processed'),
                COALESCE({self._legacy_select_expr(legacy_columns, 'created_at')}, CURRENT_TIMESTAMP)
            FROM {LEGACY_DISPATCH_REQUESTS_TABLE}
            ON CONFLICT (request_id) DO NOTHING
            """
        )

    def _backfill_dispatch_model_configs_from_legacy(self, conn) -> None:
        if not self._table_exists(conn, LEGACY_DISPATCH_MODEL_CONFIGS_TABLE):
            return
        legacy_columns = self._table_columns(conn, LEGACY_DISPATCH_MODEL_CONFIGS_TABLE)
        if not legacy_columns:
            return
        conn.execute(
            f"""
            INSERT INTO {DISPATCH_MODEL_CONFIGS_TABLE} (
                config_id, output_type, model_name, model_version,
                default_max_frames, default_jpeg_quality,
                default_confidence, default_iou,
                extra_params, is_active, description,
                updated_at, created_at
            )
            SELECT
                config_id,
                output_type,
                model_name,
                {self._legacy_select_expr(legacy_columns, 'model_version')},
                COALESCE({self._legacy_select_expr(legacy_columns, 'default_max_frames')}, 24),
                COALESCE({self._legacy_select_expr(legacy_columns, 'default_jpeg_quality')}, 90),
                {self._legacy_select_expr(legacy_columns, 'default_confidence')},
                {self._legacy_select_expr(legacy_columns, 'default_iou')},
                {self._legacy_select_expr(legacy_columns, 'extra_params')},
                COALESCE({self._legacy_select_expr(legacy_columns, 'is_active')}, TRUE),
                {self._legacy_select_expr(legacy_columns, 'description')},
                COALESCE({self._legacy_select_expr(legacy_columns, 'updated_at')}, CURRENT_TIMESTAMP),
                COALESCE({self._legacy_select_expr(legacy_columns, 'created_at')}, CURRENT_TIMESTAMP)
            FROM {LEGACY_DISPATCH_MODEL_CONFIGS_TABLE}
            ON CONFLICT (config_id) DO NOTHING
            """
        )

    def _backfill_dispatch_pipeline_runs_from_legacy(self, conn) -> None:
        if not self._table_exists(conn, LEGACY_DISPATCH_PIPELINE_RUNS_TABLE):
            return
        legacy_columns = self._table_columns(conn, LEGACY_DISPATCH_PIPELINE_RUNS_TABLE)
        if not legacy_columns:
            return
        conn.execute(
            f"""
            INSERT INTO {DISPATCH_PIPELINE_RUNS_TABLE} (
                run_id, request_id, folder_name,
                step_name, step_order, step_status,
                model_name, model_version, applied_params,
                input_count, output_count, error_count,
                started_at, completed_at, error_message, created_at
            )
            SELECT
                run_id,
                request_id,
                folder_name,
                step_name,
                COALESCE({self._legacy_select_expr(legacy_columns, 'step_order')}, 0),
                COALESCE({self._legacy_select_expr(legacy_columns, 'step_status')}, 'pending'),
                {self._legacy_select_expr(legacy_columns, 'model_name')},
                {self._legacy_select_expr(legacy_columns, 'model_version')},
                {self._legacy_select_expr(legacy_columns, 'applied_params')},
                COALESCE({self._legacy_select_expr(legacy_columns, 'input_count')}, 0),
                COALESCE({self._legacy_select_expr(legacy_columns, 'output_count')}, 0),
                COALESCE({self._legacy_select_expr(legacy_columns, 'error_count')}, 0),
                {self._legacy_select_expr(legacy_columns, 'started_at')},
                {self._legacy_select_expr(legacy_columns, 'completed_at')},
                {self._legacy_select_expr(legacy_columns, 'error_message')},
                COALESCE({self._legacy_select_expr(legacy_columns, 'created_at')}, CURRENT_TIMESTAMP)
            FROM {LEGACY_DISPATCH_PIPELINE_RUNS_TABLE}
            ON CONFLICT (run_id) DO NOTHING
            """
        )

    def get_in_flight_dispatch_requests(self, folder_name: str) -> list[dict[str, Any]]:
        normalized_folder = str(folder_name or "").strip()
        if not normalized_folder:
            return []
        with self.connect() as conn:
            if not self._table_exists(conn, DISPATCH_REQUESTS_TABLE):
                return []
            rows = conn.execute(
                f"""
                SELECT request_id, status
                FROM {DISPATCH_REQUESTS_TABLE}
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
            if not self._table_exists(conn, DISPATCH_REQUESTS_TABLE):
                return None
            row = conn.execute(
                f"SELECT status FROM {DISPATCH_REQUESTS_TABLE} WHERE request_id = ?",
                [normalized_request_id],
            ).fetchone()
        return str(row[0]) if row and row[0] is not None else None

    def get_active_dispatch_model_configs(self, output_types: list[str]) -> dict[str, dict[str, Any]]:
        normalized_types = [str(item or "").strip() for item in output_types if str(item or "").strip()]
        if not normalized_types:
            return {}
        placeholders = ", ".join("?" * len(normalized_types))
        with self.connect() as conn:
            if not self._table_exists(conn, DISPATCH_MODEL_CONFIGS_TABLE):
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
                FROM {DISPATCH_MODEL_CONFIGS_TABLE}
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
                f"""
                INSERT INTO {DISPATCH_REQUESTS_TABLE} (
                    request_id, folder_name, run_mode, outputs, labeling_method,
                    dispatch_mode, requested_labeling_method_raw,
                    categories, classes, image_profile,
                    status, archive_pending_path, archive_path,
                    max_frames_per_video, jpeg_quality, confidence_threshold, iou_threshold,
                    requested_by, requested_at, processed_at, bucket
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    record.get("request_id"),
                    record.get("folder_name"),
                    record.get("run_mode"),
                    record.get("outputs"),
                    record.get("labeling_method"),
                    record.get("dispatch_mode", "standard"),
                    record.get("requested_labeling_method_raw"),
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
                    record.get("bucket", "vlm-processed"),
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
                f"""
                INSERT INTO {DISPATCH_PIPELINE_RUNS_TABLE} (
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
                f"""
                INSERT INTO {DISPATCH_REQUESTS_TABLE} (
                    request_id, folder_name, run_mode, outputs, labeling_method,
                    dispatch_mode, requested_labeling_method_raw,
                    categories, classes, image_profile,
                    max_frames_per_video, jpeg_quality,
                    confidence_threshold, iou_threshold,
                    status, error_message, processed_at, bucket
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'failed', ?, ?, ?)
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
                    record.get("dispatch_mode", "standard"),
                    record.get("requested_labeling_method_raw"),
                    record.get("categories"),
                    record.get("classes"),
                    record.get("image_profile"),
                    record.get("max_frames_per_video"),
                    record.get("jpeg_quality"),
                    record.get("confidence_threshold"),
                    record.get("iou_threshold"),
                    record.get("error_message"),
                    record.get("processed_at"),
                    record.get("bucket", "vlm-processed"),
                ],
            )

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
            if not self._table_exists(conn, DISPATCH_REQUESTS_TABLE):
                return
            conn.execute(
                f"""
                UPDATE {DISPATCH_REQUESTS_TABLE}
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
            if not self._table_exists(conn, DISPATCH_PIPELINE_RUNS_TABLE):
                return
            conn.execute(
                f"""
                UPDATE {DISPATCH_PIPELINE_RUNS_TABLE}
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
            if not self._table_exists(conn, DISPATCH_PIPELINE_RUNS_TABLE):
                return
            conn.execute(
                f"""
                UPDATE {DISPATCH_PIPELINE_RUNS_TABLE}
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
