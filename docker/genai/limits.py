"""GenAI Studio 운영 제한 — quota + rate-limit.

quota — 사용자별 일별 배치 수 + 누적 입력 bytes (PG 집계, soft limit).
rate-limit — sliding window per user (in-memory, 단일 컨테이너 가정).

값 미설정 시 비활성. 사내망 신뢰 모델 보완용 (악의적 사용 방어보다는 운영자 실수
방어가 1차 목적).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta

from db import pg


_log = logging.getLogger(__name__)


_RATE_LIMIT_PER_MIN = int(os.getenv("GENAI_RATE_LIMIT_PER_MIN", "0"))   # 0 = off
_DAILY_BATCH_LIMIT = int(os.getenv("GENAI_DAILY_BATCH_LIMIT", "0"))     # 0 = off
_DAILY_BYTES_LIMIT = int(os.getenv("GENAI_DAILY_BYTES_LIMIT", "0"))     # 0 = off


_rate_lock = threading.Lock()
_rate_state: dict[str, deque[float]] = {}


class LimitExceeded(Exception):
    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind


def check_rate_limit(user: str) -> None:
    """sliding 60s window. limit=0 이면 항상 통과."""
    if _RATE_LIMIT_PER_MIN <= 0:
        return
    now = time.time()
    cutoff = now - 60.0
    with _rate_lock:
        q = _rate_state.setdefault(user, deque())
        while q and q[0] < cutoff:
            q.popleft()
        if len(q) >= _RATE_LIMIT_PER_MIN:
            raise LimitExceeded(
                "rate_limit",
                f"rate limit {_RATE_LIMIT_PER_MIN}/min 초과 (user={user})",
            )
        q.append(now)


def _daily_usage(user: str, since: datetime) -> tuple[int, int]:
    """user 의 since 이후 batch 수 + 누적 input_total_bytes (Python 측 JSON 파싱).

    SQL 측 ::jsonb 캐스팅은 단 한 row 라도 malformed 면 전체 query 가 ABORT 된다.
    그래서 TEXT 그대로 가져와 Python 에서 json.loads + 실패시 warn + skip.

    이 함수는 PG 가 살아있다는 전제로 호출자가 자체 try/except 결정 — 일시 장애 시
    의도적으로 raise 해서 advisory 라도 false-OK 가 안 나가도록 한다 (CLI bulk
    pre-check 는 이 정직성에 의존).
    """
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*), COALESCE(
                  ARRAY_AGG(options_json) FILTER (WHERE options_json IS NOT NULL),
                  ARRAY[]::text[]
                )
                  FROM genai_batches
                 WHERE requested_by = %s AND submitted_at > %s
                """,
                (user, since),
            )
            n_batches, opt_texts = cur.fetchone()
    total_bytes = 0
    for txt in opt_texts or []:
        try:
            d = json.loads(txt)
        except (TypeError, ValueError):
            # malformed 옵션 — 운영자 디버깅 신호. usage 집계만 skip.
            _log.warning("malformed options_json skipped (user=%r)", user)
            continue
        try:
            total_bytes += int(d.get("input_total_bytes") or 0)
        except (TypeError, ValueError):
            continue
    return int(n_batches or 0), total_bytes


def check_daily_quota(user: str | None, files_total_bytes: int) -> None:
    """일별 batch 수 + 누적 입력 bytes 검사. PG 집계."""
    if _DAILY_BATCH_LIMIT <= 0 and _DAILY_BYTES_LIMIT <= 0:
        return
    if not user:
        # auth 우회 모드 — 사용자 식별 불가 → quota 미적용
        return
    since = datetime.now() - timedelta(days=1)
    n_batches, total = _daily_usage(user, since)
    if _DAILY_BATCH_LIMIT > 0 and n_batches >= _DAILY_BATCH_LIMIT:
        raise LimitExceeded(
            "daily_batch",
            f"일별 배치 수 한도 {_DAILY_BATCH_LIMIT} 초과 (user={user})",
        )
    if _DAILY_BYTES_LIMIT > 0 and total + files_total_bytes > _DAILY_BYTES_LIMIT:
        raise LimitExceeded(
            "daily_bytes",
            f"일별 입력 bytes 한도 {_DAILY_BYTES_LIMIT} 초과 "
            f"(user={user}, used={total}, request={files_total_bytes})",
        )


def status() -> dict:
    return {
        "rate_limit_per_min": _RATE_LIMIT_PER_MIN,
        "daily_batch_limit": _DAILY_BATCH_LIMIT,
        "daily_bytes_limit": _DAILY_BYTES_LIMIT,
    }


def usage(user: str | None) -> dict:
    """주어진 user 의 최근 24h 사용량 + headroom 반환. CLI bulk pre-check 용.

    limit 이 비활성(0)인 항목은 remaining=None (unlimited).
    rate_limit 의 현재 60s window 사용량은 in-memory 라 단일 컨테이너 가정.
    """
    now = time.time()
    cutoff = now - 60.0
    rate_used = 0
    with _rate_lock:
        if user and user in _rate_state:
            q = _rate_state[user]
            rate_used = sum(1 for ts in q if ts >= cutoff)

    # PG 일시 장애 시 silent fallback 금지 — 호출자(/genai/limits)가 503 으로 노출.
    # CLI bulk pre-check 는 이 정직성에 의존 (false-OK 응답으로 1000-batch 폭주 차단).
    if user:
        since = datetime.now() - timedelta(days=1)
        daily_batches, daily_bytes = _daily_usage(user, since)
    else:
        daily_batches, daily_bytes = 0, 0

    def _remaining(used: int, limit_: int) -> int | None:
        if limit_ <= 0:
            return None  # unlimited
        return max(0, limit_ - int(used or 0))

    return {
        "user": user,
        "rate_limit": {
            "per_min": _RATE_LIMIT_PER_MIN,
            "used_60s": rate_used,
            "remaining": _remaining(rate_used, _RATE_LIMIT_PER_MIN),
        },
        "daily_batches": {
            "limit": _DAILY_BATCH_LIMIT,
            "used_24h": int(daily_batches or 0),
            "remaining": _remaining(int(daily_batches or 0), _DAILY_BATCH_LIMIT),
        },
        "daily_bytes": {
            "limit": _DAILY_BYTES_LIMIT,
            "used_24h": int(daily_bytes or 0),
            "remaining": _remaining(int(daily_bytes or 0), _DAILY_BYTES_LIMIT),
        },
    }
