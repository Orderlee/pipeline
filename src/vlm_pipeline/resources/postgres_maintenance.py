"""GPU 정비락 PG read/write mixin (gpu_maintenance_lock, migration 014)."""
from __future__ import annotations

from typing import Any


class PostgresMaintenanceMixin:
    """gpu_maintenance_lock 단일행 upsert/read. guard 센서·trainer op 가 사용."""

    def set_gpu_maintenance(
        self,
        target: str,
        *,
        active: bool,
        owner_run_id: str | None = None,
        ttl_seconds: int = 1800,
        note: str | None = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gpu_maintenance_lock
                        (target, active, owner_run_id, entered_at, heartbeat_at, ttl_seconds, note, updated_at)
                    VALUES
                        (%(target)s, %(active)s, %(owner_run_id)s,
                         CASE WHEN %(active)s THEN now() ELSE NULL END,
                         CASE WHEN %(active)s THEN now() ELSE NULL END,
                         %(ttl_seconds)s, %(note)s, now())
                    ON CONFLICT (target) DO UPDATE SET
                        active       = EXCLUDED.active,
                        owner_run_id = CASE WHEN EXCLUDED.active THEN EXCLUDED.owner_run_id ELSE NULL END,
                        entered_at   = CASE WHEN EXCLUDED.active THEN now() ELSE NULL END,
                        heartbeat_at = CASE WHEN EXCLUDED.active THEN now() ELSE NULL END,
                        ttl_seconds  = EXCLUDED.ttl_seconds,
                        note         = CASE WHEN EXCLUDED.active THEN EXCLUDED.note ELSE NULL END,
                        updated_at   = now()
                    """,
                    {
                        "target": target,
                        "active": bool(active),
                        "owner_run_id": owner_run_id,
                        "ttl_seconds": int(ttl_seconds),
                        "note": note,
                    },
                )

    def bump_gpu_maintenance_heartbeat(self, target: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE gpu_maintenance_lock SET heartbeat_at = now(), updated_at = now() "
                    "WHERE target = %(target)s AND active = TRUE",
                    {"target": target},
                )

    def get_gpu_maintenance(self, target: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT active, owner_run_id, entered_at, heartbeat_at, ttl_seconds, note "
                    "FROM gpu_maintenance_lock WHERE target = %(target)s",
                    {"target": target},
                )
                row = cur.fetchone()
                if row is None:
                    return None
                cols = ("active", "owner_run_id", "entered_at", "heartbeat_at", "ttl_seconds", "note")
                if isinstance(row, dict):
                    return dict(row)
                return dict(zip(cols, row))
