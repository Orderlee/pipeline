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
                       error_message = NULL,   -- deferred 등 이전 메시지 제거
                       submitted_at = COALESCE(submitted_at, %s)
                 WHERE job_id = %s
                   AND status NOT IN ('done','failed')  -- terminal 불변: cancel 직후 drain 이 failed→submitted 로 못 뒤집음
                """,
                (provider_job_id, now, job_id),
            )


def mark_job_deferred(job_id: str, reason: str) -> None:
    """Kling 동시성 한도로 즉시 submit 못 한 job 을 'pending' 유지 + 사유 기록.
    실패(failed) 아님 — sensor drain 이 슬롯 빌 때 재시도."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE genai_jobs
                   SET status = 'pending',
                       error_message = %s
                 WHERE job_id = %s
                   AND status NOT IN ('done','failed')  -- terminal 불변: deferred 가 cancel(failed) 을 pending 으로 못 되살림
                """,
                (f"[deferred] {reason}"[:500], job_id),
            )


def count_inflight_jobs(engine: str) -> int:
    """해당 engine 의 in-flight(submitted/running) job 수 (전체 batch 합산).
    Kling 동시 작업 한도 게이트용 (account-level)."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                  FROM genai_jobs j JOIN genai_batches b ON b.batch_id=j.batch_id
                 WHERE b.engine = %s AND j.status IN ('submitted','running')
                """,
                (engine,),
            )
            (n,) = cur.fetchone()
    return int(n or 0)


def list_pending_async_jobs(engine: str, limit: int = 50) -> list[dict]:
    """engine 의 pending job (batch 가 아직 running/pending) FIFO. drain 대상."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT j.job_id, j.batch_id, j.seq_in_batch, b.prompt, b.options_json
                  FROM genai_jobs j JOIN genai_batches b ON b.batch_id=j.batch_id
                 WHERE b.engine = %s AND j.status = 'pending'
                   AND b.status IN ('running','pending')
                 ORDER BY j.batch_id, j.seq_in_batch
                 LIMIT %s
                """,
                (engine, int(limit)),
            )
            rows = cur.fetchall()
    cols = ["job_id", "batch_id", "seq_in_batch", "prompt", "options_json"]
    return [dict(zip(cols, r)) for r in rows]


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
                   AND status NOT IN ('done','failed')  -- terminal 불변: 늦은 provider 완료가 cancel 을 못 되살림
                """,
                (status, error_message, cost_units, completed_at, job_id),
            )


def cancel_batch(batch_id: str, reason: str) -> int:
    """미완료(pending/submitted/running) job 을 failed 로 마킹. pending 정지 = drain 이
    더 이상 제출 안 함(과금 차단). 반환: 취소된 job 수.
    ⚠️ 이미 submitted 된 건 Kling 에서 계속 생성될 수 있음(provider-cancel 없음).
    batch.status 는 하드코딩 대신 job 집계에서 파생(recompute) — 이미 종료된 batch 를
    'failed' 로 오염시키지 않고, done+cancelled 혼재는 partial_success 로 반영."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE genai_jobs SET status='failed', error_message=%s, completed_at=now()
                     WHERE batch_id=%s AND status IN ('pending','submitted','running')""",
                (reason, batch_id),
            )
            n = cur.rowcount
    if recompute_batch_status(batch_id) == "pending":
        # 0-job batch (submit 이 jobs insert 전에 죽은 잔재): 집계 파생 불가 + reconcile 도
        # EXISTS(jobs) 라 안 줍는다 — 취소는 직접 종료. job 행 없인 다른 writer 가 이
        # batch 를 안 건드리므로 raw UPDATE 안전.
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE genai_batches SET status='failed', completed_at=COALESCE(completed_at, now())
                         WHERE batch_id=%s AND status IN ('pending','running')""",
                    (batch_id,),
                )
    return n


def recompute_batch_status(batch_id: str) -> str:
    """genai_jobs status 집계 → genai_batches.status / n_succeeded / n_failed 갱신.

    derived:
      - 모든 job 끝났는데 failed 0 → 'succeeded'
      - 모든 job 끝났는데 done 0 → 'failed'
      - 모든 job 끝났는데 done > 0 and failed > 0 → 'partial_success'
      - 아직 끝나지 않은 job 있으면 → 'running'
    completed_at 은 최초 terminal 전이 시각만 보존(반복 recompute 로 안 밀림),
    running 복귀 시 NULL 로 초기화.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            # 동시 recompute(cancel/drain/finalize/reconcile) 직렬화 — 무락 SELECT→UPDATE 로
            # stale 집계가 최신 상태를 덮어쓰는 역전 방지. 락은 트랜잭션 commit 시 해제.
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (batch_id,))
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
            elif n_failed == 0:
                new_status = "succeeded"
            elif n_done == 0:
                new_status = "failed"
            else:
                new_status = "partial_success"
            cur.execute(
                """
                UPDATE genai_batches
                   SET status = %s,
                       n_succeeded = %s,
                       n_failed = %s,
                       completed_at = CASE WHEN %s THEN COALESCE(completed_at, now()) END
                 WHERE batch_id = %s
                """,
                (new_status, int(n_done), int(n_failed), new_status != "running", batch_id),
            )
            return new_status


def reconcile_stale_batches(limit: int = 200) -> list[tuple[str, str]]:
    """모든 job 이 terminal(done/failed) 인데 batch.status 가 running/pending 으로
    남은 stale batch 를 찾아 recompute_batch_status 로 보정.

    submit 부분실패 후 crash, finalize 중단, sensor 누락 등으로 발생 가능. sensor 가
    매 tick 호출하는 안전망. 반환: [(batch_id, new_status), ...].
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT b.batch_id
                  FROM genai_batches b
                 WHERE b.status IN ('running','pending')
                   AND EXISTS (SELECT 1 FROM genai_jobs j WHERE j.batch_id=b.batch_id)
                   AND NOT EXISTS (
                       SELECT 1 FROM genai_jobs j
                        WHERE j.batch_id=b.batch_id
                          AND j.status IN ('pending','submitted','running')
                   )
                 ORDER BY b.submitted_at
                 LIMIT %s
                """,
                (int(limit),),
            )
            stale = [r[0] for r in cur.fetchall()]
    out: list[tuple[str, str]] = []
    for bid in stale:
        out.append((bid, recompute_batch_status(bid)))
    return out


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


def release_batch_promote_claim(batch_id: str) -> bool:
    """promote 실행 중 실패한 경우 claim 을 되돌린다 (보상 UPDATE).

    options_json.promoted_to_labeling 키를 제거. claim 후 파일 복사/JSON write 가
    실패했을 때 호출 — 같은 batch 가 재시도 가능해진다.

    이 함수는 무조건 실행 (이미 false 거나 키 없어도 idempotent). 반환은 informational.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE genai_batches
                   SET options_json = (
                         COALESCE(options_json::jsonb, '{}'::jsonb) - 'promoted_to_labeling'
                       )::text
                 WHERE batch_id = %s
                """,
                (batch_id,),
            )
            return cur.rowcount > 0


def claim_batch_for_promote(batch_id: str) -> bool:
    """Atomically mark batch.options_json.promoted_to_labeling=true if not already set.

    promote-to-labeling 의 race-safe guard. options_json 안 boolean flag 로 두고
    JSONB merge + WHERE not-exists 조건을 하나의 UPDATE 로 직렬화한다. 두 번째
    요청은 0 rows affected → False 반환 → endpoint 가 409 Conflict 응답.

    이 SQL 은 options_json 이 NULL 이거나 promoted_to_labeling 키가 없거나 false
    인 경우에만 true 로 set. promoted_to_labeling=true 인 row 는 못 잡고 False.

    Note: options_json 컬럼은 TEXT 타입(002_genai.sql:58). jsonb 연산 후 ::text 로
    명시 캐스팅해 assignment 호환 보장.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE genai_batches
                   SET options_json = (
                         COALESCE(options_json::jsonb, '{}'::jsonb)
                         || jsonb_build_object('promoted_to_labeling', true)
                       )::text
                 WHERE batch_id = %s
                   AND (options_json IS NULL
                        OR NOT (COALESCE(options_json::jsonb -> 'promoted_to_labeling',
                                          'false'::jsonb) = 'true'::jsonb))
                """,
                (batch_id,),
            )
            return cur.rowcount == 1


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
