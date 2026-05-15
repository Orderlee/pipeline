"""GenAI 컨테이너의 Postgres 클라이언트.

Dagster Resource 와 별도로, FastAPI orchestrator 가 직접 사용하는 thin layer.
PostgresGenAIMixin 의 SQL 과 동일한 동작을 하지만, dagster 의존을 제거해
Pure psycopg2 로 호출.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.pool


_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        dsn = os.getenv("DATAOPS_POSTGRES_DSN") or os.getenv("PIPELINE_DB_DSN")
        if not dsn:
            raise RuntimeError("DATAOPS_POSTGRES_DSN env required")
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=int(os.getenv("GENAI_PG_POOL_MIN", "1")),
            maxconn=int(os.getenv("GENAI_PG_POOL_MAX", "5")),
            dsn=dsn,
        )
    return _pool


@contextmanager
def connect():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ----------------------------------------------------------------------
# genai_batches / genai_jobs CRUD (PostgresGenAIMixin SQL 과 1:1 미러)
# ----------------------------------------------------------------------


def insert_genai_batch(batch: dict) -> None:
    now = datetime.now()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO genai_batches (
                    batch_id, engine, output_media, prompt, options_json, requested_by,
                    status, n_total, n_succeeded, n_failed, submitted_at, completed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id) DO NOTHING
                """,
                (
                    batch["batch_id"], batch["engine"], batch["output_media"],
                    batch["prompt"], batch.get("options_json"), batch.get("requested_by"),
                    batch.get("status", "pending"), int(batch["n_total"]),
                    int(batch.get("n_succeeded", 0)), int(batch.get("n_failed", 0)),
                    batch.get("submitted_at", now), batch.get("completed_at"),
                ),
            )


def insert_genai_jobs_batch(jobs: list[dict]) -> int:
    if not jobs:
        return 0
    rows = [
        (
            j["job_id"], j["batch_id"], int(j["seq_in_batch"]),
            j.get("input_asset_id"), j.get("output_asset_id"),
            j.get("provider_job_id"), j.get("status", "pending"),
            j.get("error_message"), j.get("cost_units"),
            j.get("submitted_at"), j.get("completed_at"),
        )
        for j in jobs
    ]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO genai_jobs (
                    job_id, batch_id, seq_in_batch, input_asset_id, output_asset_id,
                    provider_job_id, status, error_message, cost_units,
                    submitted_at, completed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO NOTHING
                """,
                rows,
            )
    return len(rows)


def update_job_submitted(job_id: str, provider_job_id: str | None) -> None:
    now = datetime.now()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE genai_jobs
                   SET provider_job_id = %s,
                       status = 'submitted',
                       submitted_at = COALESCE(submitted_at, %s)
                 WHERE job_id = %s
                """,
                (provider_job_id, now, job_id),
            )


def update_job_status(
    job_id: str,
    status: str,
    error_message: str | None = None,
    cost_units: float | None = None,
) -> None:
    completed_at = datetime.now() if status in ("done", "failed") else None
    with connect() as conn:
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
                (status, error_message, cost_units, completed_at, job_id),
            )


def recompute_batch_status(batch_id: str) -> str:
    """genai_jobs status 집계 → genai_batches.status / n_succeeded / n_failed 갱신.

    derived:
      - 모든 job done → 'succeeded'
      - 모든 job 끝났는데 failed 0 → 'succeeded'
      - 모든 job 끝났는데 done 0 → 'failed'
      - 모든 job 끝났는데 done > 0 and failed > 0 → 'partial_success'
      - 아직 끝나지 않은 job 있으면 → 'running'
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status='done')   AS n_done,
                    COUNT(*) FILTER (WHERE status='failed') AS n_failed,
                    COUNT(*)                                 AS n_total
                  FROM genai_jobs WHERE batch_id = %s
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


_LIST_BATCHES_MAX_SCAN = 10_000  # bulk_group filter 시 PG row 스캔 상한


def list_batches(
    status: str | None = None,
    engine: str | None = None,
    bulk_group_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """status/engine 은 SQL WHERE, bulk_group_id 는 options_json TEXT 안에 있어
    Python 측에서 json.loads 후 매칭 (malformed JSON 에 강건).

    bulk_group_id 필터 시 LIMIT/OFFSET 으로 페이지네이션 하면서 매칭이 `limit` 만큼
    채워질 때까지 (또는 _LIST_BATCHES_MAX_SCAN 행 스캔까지) 반복.
    """
    base_sql = """
        SELECT batch_id, engine, output_media, status,
               n_total, n_succeeded, n_failed,
               submitted_at, completed_at, prompt, options_json
          FROM genai_batches
    """
    where_params: list[Any] = []
    where: list[str] = []
    if status:
        where.append("status = %s")
        where_params.append(status)
    if engine:
        where.append("engine = %s")
        where_params.append(engine)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    cols = ["batch_id", "engine", "output_media", "status",
            "n_total", "n_succeeded", "n_failed",
            "submitted_at", "completed_at", "prompt", "options_json"]
    out: list[dict] = []
    target = int(limit)
    page = max(target * 4, 100) if bulk_group_id else target
    offset = 0
    scanned = 0
    while True:
        sql = base_sql + where_sql + " ORDER BY submitted_at DESC LIMIT %s OFFSET %s"
        params = tuple(where_params) + (page, offset)
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        if not rows:
            break
        for r in rows:
            scanned += 1
            d = dict(zip(cols, r))
            bgi = _extract_bulk_group_id(d.get("options_json"))
            d["bulk_group_id"] = bgi
            if bulk_group_id and bgi != bulk_group_id:
                continue
            out.append(d)
            if len(out) >= target:
                return out
        if not bulk_group_id:
            break  # 필터 없으면 한 페이지로 충분
        offset += page
        if scanned >= _LIST_BATCHES_MAX_SCAN:
            break
    return out


def _extract_bulk_group_id(options_json_text: str | None) -> str | None:
    if not options_json_text:
        return None
    try:
        import json as _json
        d = _json.loads(options_json_text)
    except (TypeError, ValueError):
        return None
    bgi = d.get("bulk_group_id") if isinstance(d, dict) else None
    return bgi if isinstance(bgi, str) and bgi else None


def distinct_bulk_groups(limit_rows: int = 500) -> list[str]:
    """최근 batch 들에서 발견되는 bulk_group_id 의 distinct 목록 (최신순).

    options_json TEXT 안에서 Python 측 파싱 — 사용량 적은 staging 에서 충분.
    더 큰 스케일 가면 options_json 을 JSONB 컬럼으로 마이그레이션 + 인덱스 권장.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT options_json FROM genai_batches
                 WHERE options_json IS NOT NULL
                 ORDER BY submitted_at DESC LIMIT %s
                """,
                (int(limit_rows),),
            )
            rows = cur.fetchall()
    seen: list[str] = []
    seen_set: set[str] = set()
    for (txt,) in rows:
        bgi = _extract_bulk_group_id(txt)
        if bgi and bgi not in seen_set:
            seen.append(bgi)
            seen_set.add(bgi)
    return seen


def get_batch_with_jobs(batch_id: str) -> dict | None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT batch_id, engine, output_media, prompt, options_json,
                       requested_by, status, n_total, n_succeeded, n_failed,
                       submitted_at, completed_at
                  FROM genai_batches WHERE batch_id = %s
                """,
                (batch_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = ["batch_id", "engine", "output_media", "prompt", "options_json",
                    "requested_by", "status", "n_total", "n_succeeded", "n_failed",
                    "submitted_at", "completed_at"]
            batch = dict(zip(cols, row))

            cur.execute(
                """
                SELECT job_id, seq_in_batch, input_asset_id, output_asset_id,
                       provider_job_id, status, error_message, cost_units,
                       submitted_at, completed_at
                  FROM genai_jobs WHERE batch_id = %s ORDER BY seq_in_batch
                """,
                (batch_id,),
            )
            job_rows = cur.fetchall()
        job_cols = ["job_id", "seq_in_batch", "input_asset_id", "output_asset_id",
                    "provider_job_id", "status", "error_message", "cost_units",
                    "submitted_at", "completed_at"]
        batch["jobs"] = [dict(zip(job_cols, r)) for r in job_rows]
    return batch
