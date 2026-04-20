"""DuckDB INGEST 도메인 — dispatch tracking 관련 메서드."""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar
from uuid import uuid4


class DuckDBIngestDispatchMixin:
    """Dispatch tracking tables CRUD mixin.

    dispatch_requests / dispatch_pipeline_runs DDL은 schema.sql + duckdb_migration 쪽에서
    일원화하여 보장한다 (CLAUDE.md "schema.sql = 최종 상태" 원칙).
    """

    # 같은 request_id 에 대해 두 run_status sensor 가 순차 fire 될 때
    # 앞선 terminal 마감을 뒷 sensor 가 덮어쓰지 않도록 하는 가드.
    _TERMINAL_DISPATCH_STATUSES: ClassVar[frozenset[str]] = frozenset(
        {"completed", "failed", "canceled"}
    )

    def get_in_flight_dispatch_requests(self, folder_name: str) -> list[dict[str, Any]]:
        normalized_folder = str(folder_name or "").strip()
        if not normalized_folder:
            return []
        with self.connect() as conn:
            if not self._table_exists(conn, "dispatch_requests"):
                return []
            rows = conn.execute(
                """
                SELECT request_id, status
                FROM dispatch_requests
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
            if not self._table_exists(conn, "dispatch_requests"):
                return None
            row = conn.execute(
                "SELECT status FROM dispatch_requests WHERE request_id = ?",
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
                INSERT INTO dispatch_requests (
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
                INSERT INTO dispatch_pipeline_runs (
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
                INSERT INTO dispatch_requests (
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
            if not self._table_exists(conn, "dispatch_requests"):
                return

            current_row = conn.execute(
                "SELECT status FROM dispatch_requests WHERE request_id = ?",
                [normalized_request_id],
            ).fetchone()
            if current_row:
                current_status = str(current_row[0] or "")
                if (
                    current_status in self._TERMINAL_DISPATCH_STATUSES
                    and current_status != status
                ):
                    # race: 이미 마감된 terminal status 는 덮어쓰지 않음
                    return

            conn.execute(
                """
                UPDATE dispatch_requests
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
            if not self._table_exists(conn, "dispatch_pipeline_runs"):
                return
            conn.execute(
                """
                UPDATE dispatch_pipeline_runs
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
            if not self._table_exists(conn, "dispatch_pipeline_runs"):
                return
            conn.execute(
                """
                UPDATE dispatch_pipeline_runs
                SET step_status = CASE
                        WHEN step_status IN ('completed', 'failed', 'canceled', 'skipped') THEN step_status
                        WHEN started_at IS NULL THEN 'skipped'
                        ELSE ?
                    END,
                    error_message = CASE
                        WHEN step_status IN ('completed', 'failed', 'canceled', 'skipped') THEN error_message
                        WHEN started_at IS NULL THEN COALESCE(error_message, 'never_started')
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
