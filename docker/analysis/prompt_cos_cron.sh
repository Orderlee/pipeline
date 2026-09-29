#!/usr/bin/env bash
# prompt_cos_db.py 주기 갱신 — 호스트 crontab 에서 호출.
#
# flock -n 은 타협이 아니라 필수다: 2026-07-06 에 refresh_frames_labels 2h cron 이
# 실행시간이 주기를 넘겨 3중 중첩되면서 스왑 쓰래싱으로 load 165 + SSH 끊김이 났다.
# 겹치면 도는 게 아니라 **건너뛴다**.
#
# 자원 정책: BLAS 6스레드(16코어 중) + nice 10. 컨테이너 안에서 도므로 호스트
# 라벨링/FiftyOne 과 CPU 만 공유하고 GPU 는 건드리지 않는다.
set -uo pipefail

C="${ANALYSIS_CONTAINER:-docker-analysis-1}"
LOCK="/tmp/prompt_cos_cron.lock"
LOG_DIR="${LOG_DIR:-/home/user/work_p/Datapipeline-Data-data_pipeline/docker/analysis/.cron_logs}"
LOG="$LOG_DIR/prompt_cos_$(date +%Y-%m-%d).log"
mkdir -p "$LOG_DIR"

exec 9>"$LOCK"
if ! flock -n 9; then
  echo "[$(date '+%F %T')] 이전 실행이 아직 도는 중 — 건너뜀" >>"$LOG"
  exit 0
fi

say() { echo "[$(date '+%F %T')] $*" >>"$LOG"; }

if ! docker inspect -f '{{.State.Running}}' "$C" 2>/dev/null | grep -qx true; then
  say "컨테이너 $C 미기동 — 중단 (restart:unless-stopped 라 곧 복귀할 것)"
  exit 0
fi

# 루트 디스크가 98% 다. 집계만 쓰므로 증가분은 수십 MB 지만 여유가 없으면 아예 안 돈다.
AVAIL_GB=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
if [[ -z "$AVAIL_GB" || "$AVAIL_GB" -lt 5 ]]; then
  say "루트 여유 ${AVAIL_GB:-?}GB < 5GB — 적재 중단 (ENOSPC 방지)"
  exit 1
fi

run() { docker exec -e COS_THREADS="${COS_THREADS:-6}" "$C" \
          nice -n 10 python3 /workspace/prompt_cos_db.py "$@" >>"$LOG" 2>&1; }

say "=== 시작 (여유 ${AVAIL_GB}GB) ==="
run selftest || { say "selftest 실패 — 중단"; exit 1; }
run score    || { say "score 실패 (rc=$?)"; exit 1; }
run report   || say "report 실패 — score 는 이미 반영됨"
run notion   || say "notion 본문 생성 실패"
say "=== 완료 ==="

# 로그는 14일치만 남긴다 (루트 디스크 98%)
find "$LOG_DIR" -name 'prompt_cos_*.log' -mtime +14 -delete 2>/dev/null || true
