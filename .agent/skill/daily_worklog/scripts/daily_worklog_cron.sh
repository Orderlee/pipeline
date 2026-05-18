#!/usr/bin/env bash
# daily_worklog_cron.sh — crontab에서 호출되는 래퍼 스크립트
# 월~금 오후 5:30에 실행되어 당일 작업 내용을 WORKLOG.md, CLAUDE2.md에 기록
#
# crontab 등록:
#   30 17 * * 1-5 /home/user/work_p/Datapipeline-Data-data_pipeline/.agent/skill/daily_worklog/scripts/daily_worklog_cron.sh
#
# 수동 실행:
#   bash .agent/skill/daily_worklog/scripts/daily_worklog_cron.sh
#   bash .agent/skill/daily_worklog/scripts/daily_worklog_cron.sh --dry-run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# .agent/skill/daily_worklog/scripts/ → 리포지토리 루트 (4단계 위)
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
LOG_DIR="$REPO_ROOT/.worklog_logs"
LOG_FILE="$LOG_DIR/daily_worklog_$(date +%Y%m%d_%H%M%S).log"

# 로그 디렉토리 생성
mkdir -p "$LOG_DIR"

# PATH 설정 (cron 환경에서 git, docker 등을 찾을 수 있도록)
export PATH="/usr/local/bin:/usr/bin:/bin:/snap/bin:$HOME/.local/bin:$PATH"
export HOME="${HOME:-/home/user}"

# 실행
cd "$REPO_ROOT"

echo "=== daily_worklog_cron.sh 시작: $(date) ===" | tee -a "$LOG_FILE"
echo "REPO_ROOT: $REPO_ROOT" | tee -a "$LOG_FILE"
echo "인자: $*" | tee -a "$LOG_FILE"

python3 "$SCRIPT_DIR/daily_worklog.py" "$@" 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

echo "=== 종료: $(date), exit=$EXIT_CODE ===" | tee -a "$LOG_FILE"

# 30일 이상 된 로그 정리
find "$LOG_DIR" -name "daily_worklog_*.log" -mtime +30 -delete 2>/dev/null || true

exit $EXIT_CODE
