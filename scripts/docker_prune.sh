#!/bin/bash
# Phase 5-D (#7 GHCR 대안) — docker image / builder cache 주기 정리.
#
# GHCR 도입 비권장 결론 후 대안: self-hosted runner 가 build cache 누적시켜
# 호스트 디스크 96% 도달. GHCR 대신 cache 정리로 해소.
#
# 사용:
#   ./scripts/docker_prune.sh                    # default: keep_storage=20GB, threshold=85%
#   ./scripts/docker_prune.sh --keep-storage 30  # builder cache 30GB 까지 보존
#   ./scripts/docker_prune.sh --dry-run          # 실제 prune 안 하고 대상만 보고
#
# 권장 cron (운영자 crontab 등록):
#   0 3 * * 0 /home/user/work_p/Datapipeline-Data-data_pipeline/scripts/docker_prune.sh >> /var/log/docker_prune.log 2>&1
#   ↑ 매주 일요일 03:00 KST. pg-backup (02:00) 와 1시간 간격.

set -euo pipefail

KEEP_STORAGE_GB=20
THRESHOLD_PERCENT=85
AGE_FILTER="168h"  # 1 week
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep-storage) KEEP_STORAGE_GB="$2"; shift 2 ;;
        --threshold) THRESHOLD_PERCENT="$2"; shift 2 ;;
        --age) AGE_FILTER="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help)
            cat <<EOF >&2
Usage: $0 [--keep-storage GB] [--threshold PCT] [--age DURATION] [--dry-run]
  --keep-storage GB    builder cache 보존량, default 20
  --threshold PCT      prune 후 디스크 % 가 이 값 초과 시 WARNING+exit 2, default 85
  --age DURATION       이 시간보다 오래된 객체 prune, default 168h (1주)
  --dry-run            실제 prune 안 함 — 대상 list 만 출력
EOF
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

before_pct=$(df / | tail -1 | awk '{print int($5)}')
before_avail=$(df -h / | tail -1 | awk '{print $4}')
log "before: root disk ${before_pct}% used (${before_avail} avail)"

log "docker system df:"
docker system df 2>&1 | sed 's/^/  /' || true

if [[ "${DRY_RUN}" == "1" ]]; then
    log "DRY RUN — dangling images that would be pruned:"
    docker images -f dangling=true -q 2>&1 | head -20 | sed 's/^/  /' || true
    log "DRY RUN — builder cache breakdown (top 10):"
    docker builder du 2>&1 | head -12 | sed 's/^/  /' || true
    log "DRY RUN: 실제 prune skip. 위 list 가 prune 대상."
    exit 0
fi

# 1) untagged dangling images (가장 안전)
log "docker image prune (dangling only)..."
docker image prune --force 2>&1 | tail -3 | sed 's/^/  /'

# 2) age-based image prune (오래된 unreferenced image)
log "docker image prune --filter until=${AGE_FILTER}..."
docker image prune --filter "until=${AGE_FILTER}" --force 2>&1 | tail -3 | sed 's/^/  /'

# 3) builder cache — 가장 큰 source (~31GB on 2026-05-29 측정).
#    --keep-storage 로 layer cache 효과 일부 보존 (재빌드 시간 단축).
log "docker builder prune --keep-storage ${KEEP_STORAGE_GB}GB --filter until=${AGE_FILTER}..."
docker builder prune --keep-storage "${KEEP_STORAGE_GB}GB" --filter "until=${AGE_FILTER}" --force 2>&1 | tail -3 | sed 's/^/  /'

# 4) volumes — *제외* (실수 시 운영 데이터 손실 위험). 별도 명령으로만 실행.
# 5) containers — *제외* (운영자가 디버깅용 stop 시킨 케이스 보호).

after_pct=$(df / | tail -1 | awk '{print int($5)}')
after_avail=$(df -h / | tail -1 | awk '{print $4}')
log "after:  root disk ${after_pct}% used (${after_avail} avail)"

if [[ ${after_pct} -gt ${THRESHOLD_PERCENT} ]]; then
    log "⚠️  disk ${after_pct}% > threshold ${THRESHOLD_PERCENT}% — 추가 조치 검토 필요:"
    log "   - docker volume ls (수동 검토 후 prune)"
    log "   - 큰 로그 파일 확인 (/var/log/, dagster_home/storage/, 컨테이너 stdout)"
    log "   - GHCR 도입 재검토 (디스크 정리만으로 부족하다면)"
    exit 2
fi

log "✅ disk 정리 완료. ${before_pct}% → ${after_pct}%"
