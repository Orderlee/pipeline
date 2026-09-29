"""ComfyUI GPU0 생성 lease — 순수 판단 로직 (L1-2, no PG/dagster/requests import).

`generation_gpu_leases`(migration 030) 한 행을 읽어 **"지금 GPU0 를 남이 들고 있는가"** 만
판정한다. `lib/maintenance_flag.py` 와 같은 자리·같은 모양이다 — 저장/네트워크 부수효과 없음.

읽기 전용 계약
--------------
이 모듈도, 이 모듈을 쓰는 어떤 Dagster 경로도 **lease 를 획득하지 않는다.** acquire /
heartbeat / release / TTL steal 은 전부 `docker/genai/db/pg.py` 소유다. Dagster 가 획득까지
하면 comfy 가 영원히 lease 를 못 잡는 역방향 교착이 생기고, 2026-09-18 의 3일 503
(정비 플래그가 안 풀려 embedding 이 계속 거부) 이 반대 방향으로 재현된다.

TTL 이 진실의 기준
------------------
`expires_at` 이 지난 lease 는 `state='active'` 여도 blocking 이 아니다. 그래서 owner
프로세스가 죽어 release 를 못 해도 이 판정은 **TTL 경과만으로 자연 회복**한다 —
누구의 heartbeat 도, 어떤 정리 작업도 기다리지 않는다.

적용 범위 (측정 근거는 docs/exec-plans/active/comfyui-remaining-work-plan.md P3-2)
----------------------------------------------------------------------------------
GPU0 를 실제로 다투는 Dagster 작업은 **embedding-service 경유 경로뿐**이다. comfy 가
lease 를 잡으면 embedding-service 를 `/maintenance/enter` + `/unload` 로 내리므로,
그 사이 임베딩 asset 의 `wait_until_ready()` 는 120s 를 헛돌다 실패한다.
ffmpeg NVENC 재인코딩은 CUDA core 가 아니라 **별개 NVENC 하드웨어 유닛**을 쓰므로
(CLAUDE.md §GPU 할당 정책의 경합 분석과 같은 논거) 이 lease 로 막지 않는다 —
안 다투는 자원 때문에 인제스트 본류를 세우는 쪽이 더 해롭다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

# migration 030 의 CHECK 제약이 허용하는 유일한 resource 값.
GPU0_COMFY = "gpu0_comfy"

_ACTIVE_STATE = "active"


@dataclass(frozen=True)
class GpuLease:
    """`generation_gpu_leases` 한 행의 판정용 표현."""

    resource: str
    state: str
    owner_job_id: str | None = None
    acquired_at: float | None = None
    expires_at: float | None = None


def _to_epoch(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def lease_from_pg_row(row: dict[str, Any] | None, *, resource: str = GPU0_COMFY) -> GpuLease | None:
    """`generation_gpu_leases` 행(dict) → GpuLease. 행이 없으면 None(=아무도 안 들고 있음)."""
    if not row:
        return None
    return GpuLease(
        resource=str(row.get("resource") or resource),
        state=str(row.get("state") or ""),
        owner_job_id=(str(row["owner_job_id"]) if row.get("owner_job_id") else None),
        acquired_at=_to_epoch(row.get("acquired_at")),
        expires_at=_to_epoch(row.get("expires_at")),
    )


def is_lease_blocking(lease: GpuLease | None, *, now_ts: float) -> bool:
    """이 lease 때문에 GPU0 작업을 미뤄야 하는가.

    blocking 조건은 둘 다 충족일 때뿐:
      - `state='active'` (released/expired 행은 이미 남의 것이 아니다)
      - `expires_at` 이 아직 안 지남 (TTL 경과 = 고아 lease = 안 막는다)

    `expires_at` 이 없는 행은 **막지 않는다**. 030 스키마가 NOT NULL 로 강제하므로
    None 은 파싱 실패거나 남의 스키마라는 뜻이고, 그 불확실성으로 GPU0 를 잠그는 것은
    이 모듈이 막으려는 바로 그 교착이다.
    """
    if lease is None:
        return False
    if lease.state != _ACTIVE_STATE:
        return False
    if lease.expires_at is None:
        return False
    return now_ts < lease.expires_at


def seconds_until_free(lease: GpuLease | None, *, now_ts: float) -> float:
    """blocking lease 가 늦어도 언제 풀리는지(초). 안 막는 lease 면 0.0.

    TTL 상한이라 실제 해제는 보통 이보다 빠르다 (정상 comfy job 은 12~60s).
    운영자가 "얼마나 기다리면 되나"를 로그 한 줄로 알 수 있게 하는 용도.
    """
    if not is_lease_blocking(lease, now_ts=now_ts):
        return 0.0
    assert lease is not None and lease.expires_at is not None  # is_lease_blocking 이 보장
    return max(0.0, lease.expires_at - now_ts)


def describe_lease(lease: GpuLease | None) -> str:
    """로그용 한 줄 요약."""
    if lease is None:
        return "none"
    return f"resource={lease.resource} state={lease.state} owner={lease.owner_job_id or '?'}"
