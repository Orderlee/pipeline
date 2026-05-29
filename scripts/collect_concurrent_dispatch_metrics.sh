#!/bin/bash
# N4 — 동시 dispatch 측정. stack-candidates §13 N4 의 측정 도구 사전 준비물.
#
# 사용법:
#   ./scripts/collect_concurrent_dispatch_metrics.sh --output /tmp/n4 --interval 5 --duration 3600
#
# 동작:
#   1. interval 초마다 다음 metric snapshot 을 timestamped 파일로 저장:
#      - nvidia-smi (GPU util / VRAM / NVENC)
#      - docker stats (CPU/MEM/IO per container)
#      - postgres pg_stat_activity (active connection / lock 대기)
#      - Dagster runs (status / step latency)
#   2. duration 초 후 종료. 모든 snapshot 은 output/<timestamp>/*.txt|json 으로.
#   3. 종료 시 summary report 출력.
#
# 사용 시점:
#   - 캠페인 시작 직전에 launch (background &)
#   - 캠페인 종료 후 출력 디렉토리 검토
#
# 측정 대상 dispatch 안 (사용자가 별도 트리거):
#   - (a) 2 source 동시 dispatch (작은 video 100개씩) — duckdb_writer lock 영향
#   - (b) 3 source 동시 (image + video 혼합) — GPU contention 영향
#   - (c) 5 source 점진 추가 — saturation point
#
# 외부 의존성:
#   - nvidia-smi (호스트)
#   - docker / psql (호스트)
#   - prod postgres 접속: PG_DSN env (기본: postgresql://airflow:airflow@127.0.0.1:15433/vlm_pipeline)

set -euo pipefail

OUTPUT_DIR=""
INTERVAL_SEC=5
DURATION_SEC=3600
PG_DSN="${PG_DSN:-postgresql://airflow:airflow@127.0.0.1:15433/vlm_pipeline}"
DAGSTER_URL="${DAGSTER_URL:-http://127.0.0.1:3030}"
CONTAINER_FILTER="${CONTAINER_FILTER:-docker-dagster-1|docker-sam3-1|docker-postgres-1|docker-minio-1}"

usage() {
    cat <<EOF >&2
Usage: $0 --output DIR [--interval SEC] [--duration SEC]
  --output DIR     measurement snapshot 저장 디렉토리 (필수)
  --interval SEC   snapshot 간격, default 5
  --duration SEC   총 측정 시간, default 3600 (1h)

env:
  PG_DSN              prod postgres DSN (default: postgresql://airflow:airflow@127.0.0.1:15433/vlm_pipeline)
  DAGSTER_URL         Dagster webserver URL (default: http://127.0.0.1:3030)
  CONTAINER_FILTER    docker stats 대상 grep regex
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output) OUTPUT_DIR="$2"; shift 2 ;;
        --interval) INTERVAL_SEC="$2"; shift 2 ;;
        --duration) DURATION_SEC="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "unknown arg: $1" >&2; usage ;;
    esac
done

[[ -z "${OUTPUT_DIR}" ]] && usage

mkdir -p "${OUTPUT_DIR}"
RUN_ID=$(date +%Y%m%dT%H%M%S)
RUN_DIR="${OUTPUT_DIR}/${RUN_ID}"
mkdir -p "${RUN_DIR}/gpu" "${RUN_DIR}/docker" "${RUN_DIR}/pg" "${RUN_DIR}/dagster"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${RUN_DIR}/run.log"; }

log "N4 metrics 시작. interval=${INTERVAL_SEC}s duration=${DURATION_SEC}s output=${RUN_DIR}"
log "PG_DSN=${PG_DSN}"
log "DAGSTER_URL=${DAGSTER_URL}"

# 호스트 메타 1회 capture
{
    echo "=== uname -a ==="; uname -a
    echo "=== nvidia-smi -L ==="; nvidia-smi -L 2>&1 || echo "(nvidia-smi 미설치)"
    echo "=== docker ps ==="; docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' 2>&1
    echo "=== free -h ==="; free -h
    echo "=== df -h ==="; df -h | head -20
} > "${RUN_DIR}/host_meta.txt"

# 1회 snapshot 함수
snapshot() {
    local ts=$1
    # GPU — nvidia-smi 가 없으면 skip
    if command -v nvidia-smi >/dev/null 2>&1; then
        nvidia-smi \
            --query-gpu=index,utilization.gpu,utilization.memory,memory.used,memory.total,encoder.stats.sessionCount \
            --format=csv,noheader,nounits \
            > "${RUN_DIR}/gpu/${ts}.csv" 2>/dev/null || true
    fi

    # docker stats — non-blocking, 1회 sample
    docker stats --no-stream --format \
        '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}}' 2>/dev/null \
        | grep -E "${CONTAINER_FILTER}" \
        > "${RUN_DIR}/docker/${ts}.csv" || true

    # PG — active connections + locks
    PGPASSWORD="$(printf '%s' "$PG_DSN" | sed -nE 's|.*://[^:]+:([^@]+)@.*|\1|p')" \
    psql "${PG_DSN}" -At -F'|' -c "
        SELECT 'connections', count(*) FROM pg_stat_activity
        UNION ALL
        SELECT 'active', count(*) FROM pg_stat_activity WHERE state = 'active'
        UNION ALL
        SELECT 'lock_wait', count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'
        UNION ALL
        SELECT 'idle_in_tx', count(*) FROM pg_stat_activity WHERE state = 'idle in transaction'
    " > "${RUN_DIR}/pg/${ts}.tsv" 2>&1 || true

    # Dagster — 최근 진행 중 run + step 통계 (GraphQL light)
    curl -sf "${DAGSTER_URL}/graphql" \
        -H "Content-Type: application/json" \
        -d '{"query":"{runsOrError(filter:{statuses:[STARTED,QUEUED]}){...on Runs{results{runId jobName status startTime}}}}"}' \
        > "${RUN_DIR}/dagster/${ts}.json" 2>/dev/null || true
}

start_epoch=$(date +%s)
deadline=$((start_epoch + DURATION_SEC))
i=0
while [[ $(date +%s) -lt $deadline ]]; do
    i=$((i + 1))
    ts=$(date +%Y%m%dT%H%M%S)
    snapshot "${ts}"
    if (( i % 12 == 0 )); then
        log "  snapshot #${i} (elapsed $(( $(date +%s) - start_epoch ))s / ${DURATION_SEC}s)"
    fi
    sleep "${INTERVAL_SEC}"
done

log "측정 종료. 총 ${i} snapshots → ${RUN_DIR}/"

# 간단 summary
{
    echo "=== N4 측정 요약 (${RUN_ID}) ==="
    echo "interval: ${INTERVAL_SEC}s  duration: ${DURATION_SEC}s  snapshots: ${i}"
    echo ""
    echo "=== GPU peak util (gpu/*.csv index,gpu_util,mem_util,mem_used,mem_total,enc_sessions) ==="
    cat "${RUN_DIR}"/gpu/*.csv 2>/dev/null | sort -t, -k2 -nr | head -10 || echo "(no gpu samples)"
    echo ""
    echo "=== docker stats CPU peak (docker/*.csv name,cpu%,mem,net,block) ==="
    cat "${RUN_DIR}"/docker/*.csv 2>/dev/null | sort -t, -k2 -nr | head -10 || echo "(no docker samples)"
    echo ""
    echo "=== PG connection peak (pg/*.tsv) ==="
    grep -h "active" "${RUN_DIR}"/pg/*.tsv 2>/dev/null | sort -t'|' -k2 -nr | head -5 || echo "(no pg samples)"
    echo ""
    echo "=== Dagster active run count (sample) ==="
    for f in $(ls -t "${RUN_DIR}"/dagster/*.json 2>/dev/null | head -3); do
        python3 -c "
import json, sys
try:
    d = json.load(open('$f'))
    runs = (d.get('data',{}).get('runsOrError',{}) or {}).get('results',[]) or []
    print('$f:', len(runs), 'active')
except Exception:
    pass
" || true
    done
} > "${RUN_DIR}/SUMMARY.txt"

log "Summary → ${RUN_DIR}/SUMMARY.txt"
cat "${RUN_DIR}/SUMMARY.txt"
