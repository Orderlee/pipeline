"""cross_table_consistency_sensor — Phase 3-B inline DQ #5.

stack-candidates §13 #5: 파이프라인 중간 탈락을 운영자가 감지 못하는 가장 흔한
케이스. SELECT 2개로 다음 두 inconsistency 를 주기적 모니터링.

  1) raw_files.ingest_status='completed' (video) 인데 video_metadata 행 없음
  2) video_metadata.timestamp_status='completed' 인데 timestamp_label_key IS NULL

둘 다 정상 운영에서 0 이어야 함. >0 이면 운영자 인지 필요.

WARNING 로그 + (선택) Slack 알림 (cooldown 적용). 실제 데이터를 안 건드림 — 운영 안전.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

from dagster import DefaultSensorStatus, SkipReason, sensor

from vlm_pipeline.lib.env_utils import int_env


# 5분 기본 간격 — Postgres COUNT(*) 2개는 ms 단위라 빈번해도 부담 없음.
_DEFAULT_INTERVAL_SEC = 300

# Slack 알림 트리거 임계값 — N개 이상이면 알림. 0 = 0 도 알림 (잡음 위험).
_DEFAULT_ALERT_THRESHOLD = 1

# Slack 알림 쿨다운 — 운영 중 같은 inconsistency 가 반복 감지돼도 N초 안엔 한번만.
_DEFAULT_ALERT_COOLDOWN_SEC = 3600  # 1h

# 모듈 단위 state. sensor instance 단일 프로세스 (Dagster daemon) 라 thread-safe.
_last_alert_ts: float = 0.0


def _send_slack_alert(message: str) -> bool:
    """Slack webhook 으로 메시지 전송. SLACK_WEBHOOK_URL 미설정 시 silent skip."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return False
    try:
        payload = json.dumps({"text": message}).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
        return True
    except Exception:  # noqa: BLE001
        return False


@sensor(
    minimum_interval_seconds=int_env("CROSS_TABLE_CONSISTENCY_INTERVAL_SEC", _DEFAULT_INTERVAL_SEC, 60),
    default_status=DefaultSensorStatus.RUNNING,
    description="cross-table DQ — ingest_status↔video_metadata, timestamp_status↔label_key",
    required_resource_keys={"db"},
)
def cross_table_consistency_sensor(context):
    """주기적 cross-table inconsistency 카운트 + WARNING + 선택 Slack 알림."""
    global _last_alert_ts  # noqa: PLW0603

    db = getattr(context.resources, "db", None)
    if db is None:
        return SkipReason("cross_table_consistency: db resource 없음")

    try:
        counts = db.count_cross_table_inconsistencies()
    except Exception as exc:  # noqa: BLE001
        # PG 일시 장애는 다음 tick 에서 재시도 — 여기서 fail 시 sensor 전체 정지.
        context.log.warning("cross_table_consistency: 쿼리 실패 — %s. 다음 tick 재시도.", exc)
        return SkipReason(f"cross_table_consistency: query failed ({exc})")

    ingest_no_meta = counts.get("ingest_completed_no_metadata", 0)
    ts_no_label = counts.get("timestamp_completed_no_label_key", 0)
    total = ingest_no_meta + ts_no_label

    if total == 0:
        # 정상 — 로그도 안 남김 (sensor 가 매 tick 로그하면 noise).
        return SkipReason("cross_table_consistency: 정상 (0 inconsistencies)")

    # 비정상 — WARNING 로 항상 가시화.
    detail = f"ingest_completed_no_metadata={ingest_no_meta}, timestamp_completed_no_label_key={ts_no_label}"
    context.log.warning("cross_table_consistency: 불일치 감지 — %s", detail)

    # Slack 알림 (cooldown 적용) — threshold 이상이면 발송.
    threshold = int_env("CROSS_TABLE_CONSISTENCY_ALERT_THRESHOLD", _DEFAULT_ALERT_THRESHOLD, 1)
    cooldown = int_env(
        "CROSS_TABLE_CONSISTENCY_ALERT_COOLDOWN_SEC",
        _DEFAULT_ALERT_COOLDOWN_SEC,
        60,
    )
    now = time.time()
    if total >= threshold and (now - _last_alert_ts) > cooldown:
        message = (
            f"[VLM Pipeline DQ] cross-table inconsistency 감지\n"
            f"- ingest_status='completed' 이지만 video_metadata 누락: "
            f"{ingest_no_meta}건\n"
            f"- timestamp_status='completed' 이지만 timestamp_label_key NULL: "
            f"{ts_no_label}건\n"
            f"조치 후보:\n"
            f"  1) scripts/backfill_video_metadata.py 실행 (#1 케이스)\n"
            f"  2) clip_timestamp 재실행 또는 timestamp_label_key 수동 backfill (#2 케이스)"
        )
        if _send_slack_alert(message):
            _last_alert_ts = now
            context.log.info("cross_table_consistency: Slack 알림 발송 완료")

    return SkipReason(f"cross_table_consistency: {detail}")
