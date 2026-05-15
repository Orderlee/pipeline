"""PG GENAI 도메인 — genai_batches / genai_jobs CRUD + status rollup.

GenAI Studio (FastAPI 컨테이너 + Dagster polling sensor) 가 호출.
Phase 1 에서 schema/메서드 정의, Phase 3+ 에서 실제 호출자가 추가됨.

DuckDB backend 에서는 이 mixin 의 메서드가 호출되지 않는다 (GenAI 데이터는
PG 전용으로 저장). DuckDB 는 postgres_scanner 로 read-only 조회만 한다.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


_BATCH_INSERT_SQL = """
INSERT INTO genai_batches (
    batch_id, engine, output_media, prompt, options_json, requested_by,
    status, n_total, n_succeeded, n_failed, submitted_at, completed_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (batch_id) DO NOTHING
"""

_JOB_INSERT_SQL = """
INSERT INTO genai_jobs (
    job_id, batch_id, seq_in_batch, input_asset_id, output_asset_id,
    provider_job_id, status, error_message, cost_units,
    submitted_at, completed_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (job_id) DO NOTHING
"""


class PostgresGenAIMixin:
    """genai_batches / genai_jobs 테이블 CRUD 및 batch status rollup."""

    # ------------------------------------------------------------------
    # Write — submit (UI POST /genai/batches 가 호출)
    # ------------------------------------------------------------------
    def insert_genai_batch(self, batch: dict) -> None:
        now = datetime.now()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _BATCH_INSERT_SQL,
                    (
                        batch["batch_id"],
                        batch["engine"],
                        batch["output_media"],
                        batch["prompt"],
                        batch.get("options_json"),
                        batch.get("requested_by"),
                        batch.get("status", "pending"),
                        int(batch["n_total"]),
                        int(batch.get("n_succeeded", 0)),
                        int(batch.get("n_failed", 0)),
                        batch.get("submitted_at", now),
                        batch.get("completed_at"),
                    ),
                )

    def insert_genai_jobs_batch(self, jobs: list[dict]) -> int:
        if not jobs:
            return 0
        rows = []
        for job in jobs:
            rows.append((
                job["job_id"],
                job["batch_id"],
                int(job["seq_in_batch"]),
                job.get("input_asset_id"),
                job.get("output_asset_id"),
                job.get("provider_job_id"),
                job.get("status", "pending"),
                job.get("error_message"),
                job.get("cost_units"),
                job.get("submitted_at"),
                job.get("completed_at"),
            ))
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(_JOB_INSERT_SQL, rows)
        return len(rows)

    # ------------------------------------------------------------------
    # Update — adapter / sensor 가 호출
    # ------------------------------------------------------------------
    def update_genai_job_submitted(
        self,
        job_id: str,
        provider_job_id: str | None,
        submitted_at: datetime | None = None,
    ) -> None:
        ts = submitted_at or datetime.now()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE genai_jobs
                       SET provider_job_id = %s,
                           status = 'submitted',
                           submitted_at = COALESCE(submitted_at, %s)
                     WHERE job_id = %s
                    """,
                    (provider_job_id, ts, job_id),
                )

    def update_genai_job_status(
        self,
        job_id: str,
        status: str,
        error_message: str | None = None,
        cost_units: float | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        ts = completed_at or (datetime.now() if status in ("done", "failed") else None)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE genai_jobs
                       SET status = %s,
                           error_message = COALESCE(%s, error_message),
                           cost_units = COALESCE(%s, cost_units),
                           completed_at = COALESCE(%s, completed_at)
                     WHERE job_id = %s
                    """,
                    (status, error_message, cost_units, ts, job_id),
                )

    def update_genai_job_assets(
        self,
        batch_id: str,
        seq_in_batch: int,
        input_asset_id: str | None = None,
        output_asset_id: str | None = None,
    ) -> None:
        # INGEST sensor 가 originals/outputs raw_files row INSERT 직후 호출.
        # 둘 중 하나만 채우거나 둘 다 채울 수 있음 — COALESCE 로 idempotent.
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE genai_jobs
                       SET input_asset_id  = COALESCE(%s, input_asset_id),
                           output_asset_id = COALESCE(%s, output_asset_id)
                     WHERE batch_id = %s AND seq_in_batch = %s
                    """,
                    (input_asset_id, output_asset_id, batch_id, int(seq_in_batch)),
                )

    def recompute_batch_status(self, batch_id: str) -> str:
        # n_succeeded / n_failed rollup + status 자동 결정.
        # done = 'succeeded', failed = 'failed', mixed = 'partial_success', else 'running'.
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE status='done')          AS n_done,
                        COUNT(*) FILTER (WHERE status='failed')        AS n_failed,
                        COUNT(*)                                       AS n_total
                      FROM genai_jobs
                     WHERE batch_id = %s
                    """,
                    (batch_id,),
                )
                n_done, n_failed, n_total = cur.fetchone()
                if n_total == 0:
                    return "pending"
                if n_done + n_failed < n_total:
                    new_status = "running"
                    completed_at = None
                elif n_failed == 0:
                    new_status = "succeeded"
                    completed_at = datetime.now()
                elif n_done == 0:
                    new_status = "failed"
                    completed_at = datetime.now()
                else:
                    new_status = "partial_success"
                    completed_at = datetime.now()
                cur.execute(
                    """
                    UPDATE genai_batches
                       SET status = %s,
                           n_succeeded = %s,
                           n_failed = %s,
                           completed_at = COALESCE(%s, completed_at)
                     WHERE batch_id = %s
                    """,
                    (new_status, int(n_done), int(n_failed), completed_at, batch_id),
                )
                return new_status

    # ------------------------------------------------------------------
    # Read — UI / sensor 가 호출
    # ------------------------------------------------------------------
    def get_genai_batch(self, batch_id: str) -> dict | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT batch_id, engine, output_media, prompt, options_json,
                           requested_by, status, n_total, n_succeeded, n_failed,
                           submitted_at, completed_at
                      FROM genai_batches
                     WHERE batch_id = %s
                    """,
                    (batch_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [
                    "batch_id", "engine", "output_media", "prompt", "options_json",
                    "requested_by", "status", "n_total", "n_succeeded", "n_failed",
                    "submitted_at", "completed_at",
                ]
                return dict(zip(cols, row))

    def list_genai_batches(
        self,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        sql = """
            SELECT batch_id, engine, output_media, status,
                   n_total, n_succeeded, n_failed,
                   submitted_at, completed_at
              FROM genai_batches
        """
        params: list[Any] = []
        if status:
            sql += " WHERE status = %s"
            params.append(status)
        sql += " ORDER BY submitted_at DESC LIMIT %s"
        params.append(int(limit))
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
            cols = [
                "batch_id", "engine", "output_media", "status",
                "n_total", "n_succeeded", "n_failed",
                "submitted_at", "completed_at",
            ]
            return [dict(zip(cols, row)) for row in rows]

    def list_genai_jobs(self, batch_id: str) -> list[dict]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT job_id, batch_id, seq_in_batch,
                           input_asset_id, output_asset_id, provider_job_id,
                           status, error_message, cost_units,
                           submitted_at, completed_at
                      FROM genai_jobs
                     WHERE batch_id = %s
                     ORDER BY seq_in_batch
                    """,
                    (batch_id,),
                )
                rows = cur.fetchall()
            cols = [
                "job_id", "batch_id", "seq_in_batch",
                "input_asset_id", "output_asset_id", "provider_job_id",
                "status", "error_message", "cost_units",
                "submitted_at", "completed_at",
            ]
            return [dict(zip(cols, row)) for row in rows]

    def find_pending_genai_jobs(
        self,
        engine: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        # Dagster polling sensor 가 호출. status IN ('submitted','running') 의 비동기 job.
        sql = """
            SELECT j.job_id, j.batch_id, j.seq_in_batch,
                   j.provider_job_id, j.status, j.submitted_at,
                   b.engine, b.output_media
              FROM genai_jobs j
              JOIN genai_batches b ON b.batch_id = j.batch_id
             WHERE j.status IN ('submitted','running')
        """
        params: list[Any] = []
        if engine:
            sql += " AND b.engine = %s"
            params.append(engine)
        sql += " ORDER BY j.submitted_at NULLS LAST LIMIT %s"
        params.append(int(limit))
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
            cols = [
                "job_id", "batch_id", "seq_in_batch",
                "provider_job_id", "status", "submitted_at",
                "engine", "output_media",
            ]
            return [dict(zip(cols, row)) for row in rows]
