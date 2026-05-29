"""Phase 3-C @asset_check definitions — stack-candidates #2.

Dagster declarative checks 를 ingest + label asset 에 묶어 run 시점 무결성 검증.

cross_table_consistency_sensor (Phase 3-B) 는 *주기적* (5분 tick) 검증인 반면,
@asset_check 는 *run-bound* — asset 머터리얼라이즈 직후 한 번. 두 가지가 보완 관계.

Severity 정책 (codex 권고):
  - ERROR: 데이터 손실 / dataset 오염 위험 시. run 을 fail 로 처리.
  - WARN: 카운트 이상치만, 운영자 인지 충분. run 통과.

본 PR 는 모두 WARN — Phase 3 도입기엔 false positive 가능성 ↑, 일단 가시화만 우선.
운영 안정화 후 ERROR 로 승격 가능.
"""

from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.label.assets import clip_timestamp


@asset_check(
    asset=raw_ingest,
    blocking=False,
    description=(
        "raw_files.ingest_status='completed' 행 중 archive_path NULL 카운트. "
        "정상 0. >0 이면 archive 이동 누락 (orchestrator 가 archive 전에 status 토글)."
    ),
    required_resource_keys={"db"},
)
def raw_ingest_archive_consistency(context) -> AssetCheckResult:
    """이번 run 직후 raw_files completed↔archive_path 불일치 카운트."""
    db = context.resources.db
    count = db.count_completed_raw_files_without_archive()
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.WARN,
        description=(
            f"completed_no_archive={count} "
            f"(>0 이면 'docker exec docker-postgres-1 psql -d vlm_pipeline -c "
            f'\\"UPDATE raw_files SET ingest_status=NULL WHERE archive_path IS NULL\\"\' '
            f"같은 backfill 검토)"
        ),
        metadata={"completed_no_archive": count},
    )


@asset_check(
    asset=raw_ingest,
    blocking=False,
    description=(
        "raw_files.ingest_status='completed' (video) 인데 video_metadata 행 없음. "
        "정상 0. cross_table_consistency_sensor 와 같은 SELECT 를 asset 시점에서도 한 번."
    ),
    required_resource_keys={"db"},
)
def raw_ingest_video_metadata_consistency(context) -> AssetCheckResult:
    """raw_ingest 직후 video raw_files ↔ video_metadata 일치 확인."""
    db = context.resources.db
    counts = db.count_cross_table_inconsistencies()
    bad = counts["ingest_completed_no_metadata"]
    return AssetCheckResult(
        passed=bad == 0,
        severity=AssetCheckSeverity.WARN,
        description=(f"ingest_completed_no_metadata={bad} (>0 이면 scripts/backfill_video_metadata.py 실행 검토)"),
        metadata={"ingest_completed_no_metadata": bad},
    )


@asset_check(
    asset=clip_timestamp,
    blocking=False,
    description=(
        "video_metadata.timestamp_status='completed' 인데 timestamp_label_key NULL. "
        "정상 0. clip_timestamp 가 status 만 업데이트하고 label_key 누락한 케이스."
    ),
    required_resource_keys={"db"},
)
def clip_timestamp_label_key_consistency(context) -> AssetCheckResult:
    """clip_timestamp 직후 timestamp_status↔label_key 일치 확인."""
    db = context.resources.db
    counts = db.count_cross_table_inconsistencies()
    bad = counts["timestamp_completed_no_label_key"]
    return AssetCheckResult(
        passed=bad == 0,
        severity=AssetCheckSeverity.WARN,
        description=(
            f"timestamp_completed_no_label_key={bad} "
            f"(>0 이면 clip_timestamp 재실행 또는 timestamp_label_key 수동 backfill)"
        ),
        metadata={"timestamp_completed_no_label_key": bad},
    )


# Definitions 에서 import 할 단일 list.
PHASE_3C_ASSET_CHECKS = [
    raw_ingest_archive_consistency,
    raw_ingest_video_metadata_consistency,
    clip_timestamp_label_key_consistency,
]
