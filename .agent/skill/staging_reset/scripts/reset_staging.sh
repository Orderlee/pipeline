#!/usr/bin/env bash
# ============================================================
# Staging 환경 전체 초기화 스크립트
# 사용법: ./reset_staging.sh [--db-only|--minio-only|--files-only|--all]
# ============================================================
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
DOCKER_COMPOSE="docker compose -f $PROJECT_ROOT/docker/docker-compose.yaml"
STAGING_NAS="/home/user/mou/staging"
DUCKDB_HOST="$PROJECT_ROOT/docker/data/staging.duckdb"

# 색상
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[RESET]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err() { echo -e "${RED}[ERROR]${NC} $1"; }

MODE="${1:---all}"

# ── 실행 중인 Dagster run 확인 ──
check_running_runs() {
    log "실행 중인 Dagster staging run 확인..."
    RUNNING=$($DOCKER_COMPOSE exec -T dagster-staging bash -c '
DAGSTER_HOME=/app/dagster_home_staging python3 << PYEOF
from dagster._core.instance import DagsterInstance
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.storage.runs.base import RunsFilter
import os
os.environ["DAGSTER_HOME"] = "/app/dagster_home_staging"
instance = DagsterInstance.get()
started = instance.get_runs(filters=RunsFilter(statuses=[DagsterRunStatus.STARTED]))
print(len(started))
PYEOF
' 2>/dev/null || echo "0")
    
    if [ "$RUNNING" != "0" ] && [ "$RUNNING" != "" ]; then
        warn "실행 중인 run이 ${RUNNING}개 있습니다. 취소하시겠습니까? (y/N)"
        read -r answer
        if [ "$answer" != "y" ] && [ "$answer" != "Y" ]; then
            err "초기화 중단"
            exit 1
        fi
        # 실행 중인 run 강제 취소
        $DOCKER_COMPOSE exec -T dagster-staging bash -c '
DAGSTER_HOME=/app/dagster_home_staging python3 << PYEOF
from dagster._core.instance import DagsterInstance
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.storage.runs.base import RunsFilter
import os
os.environ["DAGSTER_HOME"] = "/app/dagster_home_staging"
instance = DagsterInstance.get()
started = instance.get_runs(filters=RunsFilter(statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED]))
for r in started:
    instance.report_run_canceled(r)
    print(f"  Canceled: {r.run_id[:12]}")
PYEOF
' 2>/dev/null
        log "실행 중인 run 취소 완료"
        sleep 3
    else
        log "실행 중인 run 없음 ✅"
    fi
}

# ── 1. DuckDB 초기화 ──
reset_db() {
    log "DuckDB staging 초기화..."
    
    if [ -f "$DUCKDB_HOST" ]; then
        rm -f "$DUCKDB_HOST" "$DUCKDB_HOST.wal"
        log "  삭제: $DUCKDB_HOST"
    fi
    
    # Docker 내부에서도 삭제
    $DOCKER_COMPOSE exec -T dagster-staging bash -c '
        rm -f /data/staging.duckdb /data/staging.duckdb.wal 2>/dev/null
        echo "  Docker 내부 DuckDB 삭제 완료"
    ' 2>/dev/null || true
    
    log "DuckDB 초기화 완료 ✅ (다음 Dagster run에서 스키마 자동 생성)"
}

# ── 2. MinIO 초기화 ──
reset_minio() {
    log "MinIO staging 버킷 초기화..."
    
    # staging MinIO alias 설정
    $DOCKER_COMPOSE exec -T minio mc alias set staging http://10.0.0.36:9002 minioadmin minioadmin 2>/dev/null
    
    for bucket in vlm-raw vlm-processed vlm-labels vlm-dataset; do
        log "  버킷 비우기: $bucket"
        $DOCKER_COMPOSE exec -T minio mc rm --recursive --force staging/$bucket/ 2>/dev/null || true
    done
    
    log "MinIO 초기화 완료 ✅"
}

# ── 3. NAS 파일 초기화 ──
reset_files() {
    log "NAS staging 파일 초기화..."
    
    # archive 하위 삭제 (원본 데이터는 보존)
    if [ -d "$STAGING_NAS/archive" ]; then
        rm -rf "$STAGING_NAS/archive/"*
        log "  삭제: $STAGING_NAS/archive/*"
    fi
    
    # archive_pending 하위 삭제
    if [ -d "$STAGING_NAS/archive_pending" ]; then
        rm -rf "$STAGING_NAS/archive_pending/"*
        log "  삭제: $STAGING_NAS/archive_pending/*"
    fi
    
    # incoming manifests 삭제
    if [ -d "$STAGING_NAS/incoming/.manifests" ]; then
        rm -rf "$STAGING_NAS/incoming/.manifests/"*
        log "  삭제: $STAGING_NAS/incoming/.manifests/*"
    fi
    
    # 원본 데이터 확인
    if [ -d "$STAGING_NAS/tmp_data_2" ]; then
        log "  원본 보존: tmp_data_2 ($(du -sh "$STAGING_NAS/tmp_data_2" 2>/dev/null | cut -f1))"
    fi
    if [ -d "$STAGING_NAS/GS건설" ]; then
        log "  원본 보존: GS건설 ($(du -sh "$STAGING_NAS/GS건설" 2>/dev/null | cut -f1))"
    fi
    
    log "NAS 파일 초기화 완료 ✅"
}

# ── 4. Dagster run 기록 초기화 ──
reset_dagster_storage() {
    log "Dagster staging storage 초기화..."
    
    $DOCKER_COMPOSE exec -T dagster-staging bash -c '
        rm -rf /data/dagster_home_staging/storage/* 2>/dev/null
        echo "  Dagster storage 삭제 완료"
    ' 2>/dev/null || true
    
    log "Dagster storage 초기화 완료 ✅"
}

# ── 메인 ──
echo ""
echo "=========================================="
echo "  Staging 환경 초기화"
echo "  모드: $MODE"
echo "=========================================="
echo ""

check_running_runs

case "$MODE" in
    --db-only)
        reset_db
        ;;
    --minio-only)
        reset_minio
        ;;
    --files-only)
        reset_files
        ;;
    --all)
        reset_db
        reset_minio
        reset_files
        reset_dagster_storage
        ;;
    *)
        echo "사용법: $0 [--db-only|--minio-only|--files-only|--all]"
        exit 1
        ;;
esac

echo ""
log "🎉 초기화 완료! Dagster staging을 재시작하세요."
echo ""
