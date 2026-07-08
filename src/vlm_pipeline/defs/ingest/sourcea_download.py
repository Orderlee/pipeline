"""SourceA 사이트 일일 수집 asset (L4).

Tailscale 경유 MinIO(100.x) → /nas/data/sourcea 미러 + DB 기반 by_category 분류.
env·NAS·연결 preflight 실패는 모두 graceful skip — NAS 미복구/tailscale off 상태에서
스케줄이 돌아도 에러가 나지 않는다.
설계: docs/superpowers/specs/2026-07-06-sourcea-daily-download-design.md
"""

from __future__ import annotations

import socket
from datetime import datetime
from zoneinfo import ZoneInfo

from dagster import AssetKey, MaterializeResult, asset

from vlm_pipeline.lib import sourcea_site as kk

_TZ = ZoneInfo("Asia/Seoul")
_CONNECT_TIMEOUT_SEC = 5


def _skip(context, reason: str) -> MaterializeResult:
    context.log.warning(f"sourcea_site_download skip: {reason}")
    return MaterializeResult(metadata={"skipped": True, "reason": reason})


def _is_scheduled_run(context) -> bool:
    """이 run이 스케줄(`dagster/schedule_name` tag)로 시작됐는지.

    direct-invoke(`build_asset_context()`) 컨텍스트는 `context.run` 접근 시
    DagsterInvalidPropertyError 를 던지므로 try/except 로 False 처리 — 수동 run은
    태그가 없어 어차피 False 이지만, 이 가드 자체가 "수동 run은 deadline 없이
    끝까지 돈다"는 기존 트레이드오프를 건드리지 않는다.
    """
    try:
        tags = context.run.tags
    except Exception:
        try:
            tags = context.run_tags  # deprecated fallback — 일부 direct-invoke 헬퍼 경로용
        except Exception:
            return False
    return "dagster/schedule_name" in tags


@asset(
    key=AssetKey(["pipeline", "sourcea_site"]),
    description="SourceA MinIO → /nas/data/sourcea 미러 + by_category 분류 (tailscale 필요)",
    group_name="sites",
)
def sourcea_site_download(context) -> MaterializeResult:
    cfg = kk.SourceAConfig.from_env()
    if cfg is None:
        return _skip(context, "SOURCEA_* env 미설정")

    try:
        if not cfg.dest.parent.is_dir():
            return _skip(context, f"NAS mount 없음: {cfg.dest.parent}")
    except OSError as e:  # NFS stale handle 등
        return _skip(context, f"NAS 접근 불가: {e}")

    try:
        socket.create_connection((cfg.minio_host, cfg.minio_port), timeout=_CONNECT_TIMEOUT_SEC).close()
    except OSError as e:
        return _skip(context, f"MinIO {cfg.minio_host}:{cfg.minio_port} 연결 실패(tailscale off?): {e}")

    now = datetime.now(_TZ)
    deadline = kk.compute_deadline(cfg.deadline, now)
    if deadline is None and cfg.deadline and _is_scheduled_run(context):
        # UI Re-execute 는 부모 run 의 schedule 태그를 상속해 여기서 skip 된다 —
        # 마감 이후 수동 수집은 Re-execute 가 아니라 asset 직접 Materialize 로 (태그 없음 → 무제한).
        return _skip(context, f"스케줄 run이 마감({cfg.deadline}) 이후 시작 — 수동 수집은 직접 Materialize")
    dates = kk.recent_dates(cfg.lookback_days, now.date())
    if not dates:
        return _skip(context, f"lookback_days={cfg.lookback_days} — 수집 대상 날짜 없음")
    context.log.info(f"수집 대상 {dates[0]}..{dates[-1]}, deadline={deadline}")

    stats = kk.download_dates(cfg, dates, deadline=deadline)
    context.log.info(f"download: {stats}")

    meta = {"skipped": False, **stats}
    try:
        cls = kk.classify(cfg, set(dates))
        meta.update({f"classify_{k}": v for k, v in cls.items()})
    except Exception as e:  # noqa: BLE001 — DB 순단이어도 다운로드 결과는 보존
        context.log.warning(f"classify 실패(다운로드는 완료): {e}")
        meta["classify_error"] = str(e)
    return MaterializeResult(metadata=meta)
