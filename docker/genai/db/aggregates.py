"""Cost / activity 집계 쿼리.

genai_jobs 의 cost_units / status 를 engine 단위로 전체기간(all-time) 누적 집계.
"""

from __future__ import annotations

from . import pg


def cost_summary() -> dict:
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT b.engine,
                       COUNT(j.*) FILTER (WHERE j.status='done')   AS n_done,
                       COUNT(j.*) FILTER (WHERE j.status='failed') AS n_failed,
                       COALESCE(SUM(j.cost_units), 0)              AS total_cost
                  FROM genai_jobs j
                  JOIN genai_batches b ON b.batch_id = j.batch_id
                 GROUP BY b.engine
                 ORDER BY b.engine
                """
            )
            rows = cur.fetchall()

            cur.execute(
                """
                SELECT COUNT(DISTINCT b.batch_id),
                       COUNT(DISTINCT b.requested_by),
                       COUNT(j.*) FILTER (WHERE j.status='done'),
                       COUNT(j.*) FILTER (WHERE j.status='failed'),
                       COALESCE(SUM(j.cost_units), 0)
                  FROM genai_batches b
                  JOIN genai_jobs j ON j.batch_id = b.batch_id
                """
            )
            n_batches, n_users, total_done, total_failed, total_cost = cur.fetchone()
    by_engine = [
        {"engine": r[0], "n_done": int(r[1]), "n_failed": int(r[2]),
         "total_cost": float(r[3] or 0.0)}
        for r in rows
    ]
    return {
        "totals": {
            "n_batches": int(n_batches or 0),
            "n_users": int(n_users or 0),
            "n_done": int(total_done or 0),
            "n_failed": int(total_failed or 0),
            "total_cost": float(total_cost or 0.0),
        },
        "by_engine": by_engine,
    }
