#!/usr/bin/env bash
# sourcei GT 변경 전파 전체 체인 — 폴더 재라벨·삭제를 FiftyOne·원장·문장 지표까지 밀어넣는다.
#
# 왜 스크립트인가: App 의 삭제 오퍼레이터는 **동기**라 여기서 하는 계산(31뱅크 × 60만 문장,
# 수 분)을 그 안에 넣으면 앱이 그만큼 멈춘다. 그래서 값싼 것만 App 안에서 즉시 하고
# (좌표 정리 = `user-embeddings._prune_brain_results`), 비싼 것은 이 체인이 밖에서 몰아 한다.
#
# 안전장치
#   · flock  — 2026-07-06 에 2h cron 이 3중 중첩돼 스왑 쓰래싱으로 호스트가 죽은 이력이 있다.
#   · 지문   — 원장 sha256 이 그대로면 비싼 단계(3~5)를 통째로 건너뛴다. 변화가 없을 때
#              몇 초 만에 끝나므로 자주 돌려도 공짜다.
#   · 단계 1~2 는 항상 돈다(값싸고, 이게 변화를 만들어 지문을 바꾼다).
set -euo pipefail
DS="${1:-sourcei}"
WORK="/data/fiftyone/${DS,,}/work"
STAMP_HOST="/home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/fiftyone/${DS,,}/work/.refresh_stamp"
LOCK="/tmp/refresh_${DS}.lock"
X() { docker exec -i docker-analysis-1 python3 "$@"; }
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

exec 9>"$LOCK"
flock -n 9 || { log "다른 실행이 진행 중 — 종료"; exit 0; }

log "1) FiftyOne ground_truth ← 폴더"
X /workspace/gt_folder_resync.py "$DS" --apply 2>&1 | grep -vE '^\s+·' | tail -6

log "2) 원장 + embed.npz"
X /workspace/ledger_resync.py "$DS" --apply 2>&1 | tail -6

FP=$(docker exec -i docker-analysis-1 sha256sum "$WORK/ledger.jsonl" | cut -c1-16)
OLD=$(docker exec -i docker-analysis-1 cat "$WORK/.refresh_stamp" 2>/dev/null || echo none)
if [ "$FP" = "$OLD" ]; then
  log "원장 지문 $FP 변화 없음 — 비싼 단계(3~5) 건너뜀"
  exit 0
fi
log "원장 지문 $OLD → $FP · 비싼 단계 진행"

log "3) 문장 top-k 축 (wins/purity/n_cameras/adopted)"
X /workspace/refresh_sentence_metrics.py --apply 2>&1 | grep -E "sum\(wins\)|반영 완료|⚠️" | tail -4

log "4) wave npz 재계산 (31뱅크)"
X /workspace/_run_wave.py 2>&1 | grep -E "stage_wave 완료|Traceback|MemoryError" | tail -3

log "5) 최근접 프레임 연결 + wave 축 (filepath/nearest_*/match/wave_gain)"
X /workspace/refresh_prompt_links.py --apply 2>&1 | grep -E "갱신 대상|반영 완료|⚠️|set filepath" | tail -6

docker exec -i docker-analysis-1 sh -c "printf '%s' '$FP' > '$WORK/.refresh_stamp'"
log "완료 · 지문 $FP 기록"
