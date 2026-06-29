"""GPU 정비락 — 순수 판단 로직 (L1-2, no PG/dagster/torch import).

서버사이드 게이트(docker/*/app.py)와 guard 센서(defs/train/sensor_maintenance_guard.py)가
공유하는 flag 표현 + stale 판정. 저장/네트워크 부수효과 없음.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

VALID_TARGETS = ("sam3", "pe_core")


@dataclass(frozen=True)
class MaintenanceFlag:
    active: bool
    target: str
    owner_run_id: str | None = None
    entered_at: float | None = None
    heartbeat_at: float | None = None
    ttl_seconds: int = 1800
    note: str | None = None


CLEAR_FLAG = MaintenanceFlag(active=False, target="")


def _to_epoch(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def flag_from_pg_row(row: dict[str, Any] | None, *, target: str) -> MaintenanceFlag:
    """gpu_maintenance_lock 행(dict) → MaintenanceFlag. 행 없으면 active=False."""
    if not row:
        return MaintenanceFlag(active=False, target=target)
    return MaintenanceFlag(
        active=bool(row.get("active", False)),
        target=target,
        owner_run_id=(str(row["owner_run_id"]) if row.get("owner_run_id") else None),
        entered_at=_to_epoch(row.get("entered_at")),
        heartbeat_at=_to_epoch(row.get("heartbeat_at")),
        ttl_seconds=int(row.get("ttl_seconds") or 1800),
        note=(str(row["note"]) if row.get("note") else None),
    )


def is_heartbeat_stale(flag: MaintenanceFlag, *, now_ts: float) -> bool:
    """active 락의 heartbeat 가 TTL 을 초과했으면 True."""
    if not flag.active:
        return False
    if flag.heartbeat_at is None:
        return True
    return (now_ts - flag.heartbeat_at) > float(flag.ttl_seconds)


def decide_auto_release(
    flag: MaintenanceFlag,
    *,
    owner_run_is_running: bool,
    now_ts: float,
) -> tuple[bool, str | None]:
    """guard 센서가 자동해제해야 하는지 판정.

    해제 조건(둘 중 하나):
      - heartbeat 가 TTL 초과 (dead heartbeat)
      - owner run 이 RUNNING/STARTED 아님 (프로세스 죽음/재배포 고아화)
    inactive 락은 해제 대상 아님.
    """
    if not flag.active:
        return (False, None)
    if is_heartbeat_stale(flag, now_ts=now_ts):
        return (True, "heartbeat_ttl_expired")
    if not owner_run_is_running:
        return (True, "owner_run_not_running")
    return (False, None)
