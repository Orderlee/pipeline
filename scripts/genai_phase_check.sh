#!/bin/bash
# GenAI rollout phase resume notifier.
# 사용 시나리오:
#   1) crontab 에 등록 (예: 30분마다)
#        */30 * * * * /home/user/work_p/Datapipeline-Data-data_pipeline_test/scripts/genai_phase_check.sh
#   2) 스크립트는 docs/genai_rollout/phase_status.md 의 last_qa_passed_at 부터
#      GENAI_PHASE_INTERVAL_SECONDS (기본 14400=4h) 경과 시 desktop notification 발송.
#   3) 사용자가 알림 받고 Claude Code 열어 "Phase X 시작" 한 마디 → 새 세션이
#      phase_status.md 기반으로 즉시 이어감.
#
# CronCreate (Claude Code in-process) 가 session-only 라 신뢰성 낮은 문제를
# OS-level cron + 알림으로 우회.
set -euo pipefail

ROLLOUT_FILE="${ROLLOUT_FILE:-/home/user/work_p/Datapipeline-Data-data_pipeline_test/docs/genai_rollout/phase_status.md}"
INTERVAL="${GENAI_PHASE_INTERVAL_SECONDS:-14400}"      # 4h 기본
LOCK_FILE="${LOCK_FILE:-/tmp/genai_phase_check.lock}"

if [[ ! -f "$ROLLOUT_FILE" ]]; then
    echo "phase_status.md not found: $ROLLOUT_FILE" >&2
    exit 1
fi

RENOTIFY_AFTER="${GENAI_PHASE_RENOTIFY_SECONDS:-21600}"   # 6h 기본

# lock 형식: "<phase>:<epoch_of_last_notify>"
last_notified_phase=""
last_notified_epoch=0
if [[ -f "$LOCK_FILE" ]]; then
    raw="$(cat "$LOCK_FILE" || true)"
    last_notified_phase="${raw%%:*}"
    last_notified_epoch="${raw##*:}"
    [[ "$last_notified_epoch" =~ ^[0-9]+$ ]] || last_notified_epoch=0
fi

current_phase="$(grep -E '^- \*\*current_phase\*\*:' "$ROLLOUT_FILE" | sed -E 's/^- \*\*current_phase\*\*:\s*//;s/[[:space:]].*//')"
# last_qa_passed_at: "YYYY-MM-DD HH:MM (Phase N)" 형식. ` (` 앞까지가 timestamp.
last_qa="$(grep -E '^- \*\*last_qa_passed_at\*\*:' "$ROLLOUT_FILE" | sed -E 's/^- \*\*last_qa_passed_at\*\*:\s*//;s/[[:space:]]+\(.*//')"

if [[ -z "$current_phase" || -z "$last_qa" ]]; then
    echo "phase_status.md parse 실패 (current_phase=$current_phase last_qa=$last_qa)" >&2
    exit 1
fi

# Phase 5+ (모두 완료) 면 종료
if [[ "$current_phase" =~ ^[0-9]+$ ]] && (( current_phase > 5 )); then
    echo "rollout 완료. 알림 중단."
    exit 0
fi

# last_qa 형식: YYYY-MM-DD HH:MM 또는 YYYY-MM-DD
qa_epoch="$(date -d "$last_qa" +%s 2>/dev/null || echo 0)"
now_epoch="$(date +%s)"
elapsed=$((now_epoch - qa_epoch))

if (( elapsed < INTERVAL )); then
    remain=$(( INTERVAL - elapsed ))
    printf "Phase %s 대기 중. 다음 알림까지 %d 분.\n" "$current_phase" $((remain/60))
    exit 0
fi

# 같은 phase 로 직전 알림 보낸 시각이 RENOTIFY_AFTER 안이면 skip
if [[ "$last_notified_phase" == "$current_phase" ]]; then
    since_notify=$(( now_epoch - last_notified_epoch ))
    if (( since_notify < RENOTIFY_AFTER )); then
        remain=$(( RENOTIFY_AFTER - since_notify ))
        echo "Phase $current_phase 알림 발송 대기 중 (재알림까지 $((remain/60))m)"
        exit 0
    fi
    msg_prefix="(재알림) "
else
    msg_prefix=""
fi

msg="${msg_prefix}GenAI rollout: Phase $current_phase 진행 가능 (last QA: $last_qa, 경과 $((elapsed/3600))h). Claude Code 에서 'Phase $current_phase 시작' 한 마디로 이어가세요."

# 1) desktop notification
if command -v notify-send >/dev/null 2>&1; then
    notify-send -t 0 "Claude Code · GenAI Phase" "$msg" || true
fi

# 2) stdout (cron MAILTO 또는 logger)
echo "[$(date -Iseconds)] $msg"
if command -v logger >/dev/null 2>&1; then
    logger -t genai-phase-check "$msg" || true
fi

# 3) lock 업데이트 — phase 전환 시점 OR RENOTIFY_AFTER 경과 시점까지 재발송 안함
echo "${current_phase}:${now_epoch}" > "$LOCK_FILE"
