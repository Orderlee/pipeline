#!/usr/bin/env bash
# 뱅크 태그 계약 정기 점검 — 생산자↔소비자 드리프트 조기 탐지.
#
# 왜 필요한가 (2026-08-14 사고):
#   `prompt_scores_export.suffixes()` 가 생산자의 `vtag()` 전환(2026-08-11)을 따라가지
#   못해 `winner_gidx_*` 를 전 버전에서 해석하지 못한 채 몇 달을 갔다. 거버넌스 export 의
#   문장 귀속 층이 전 행 null 이었다.
#   ⚠️ 배선 문제가 아니었다 — `cmd_export` 는 이미 `validate_dir()` 을 자체 실행하고
#   위반 시 exit 1 을 돌려준다. 실패한 것은 **아무도 export 를 돌리지 않았다**는 것이다.
#   즉 없던 것은 검사기가 아니라 **트리거**다. 이 스크립트가 그 트리거다.
#
# 무엇을 검사하나:
#   `bank_tags_contract.py` — 리졸버 층. 초 단위로 끝나고 위 사고를 정확히 잡는다
#   (pre-fix 상태에서 C4 가 "도달 불가 29/176건" 으로 실패함을 실측 확인).
#   산출물 층(prompt_frame_pred 등)은 `prompt_scores_export export` 가 자체 검증하므로
#   여기서 중복하지 않는다 — 29버전 export 는 1시간 규모라 정기 실행에 맞지 않는다.
#
# ⚠️ flock 필수: 2026-07-06 에 2시간 주기 `refresh_frames_labels` cron 이 실행 시간을
#    넘겨 3중 중첩되며 호스트를 스왑 쓰래싱(load 165, SSH 끊김)으로 몰아넣은 이력이 있다.
#    이 점검은 초 단위지만 컨테이너가 멈춰 있으면 docker exec 가 길게 매달릴 수 있다.
#
# 사용:
#   scripts 로 등록하지 않고 호스트 cron 에서 직접 부른다 (analysis 는 Dagster 관리 밖).
#   crontab -e:
#     17 7 * * * /home/user/work_p/Datapipeline-Data-data_pipeline/docker/analysis/bank_health.sh
#
# 정본: docker/analysis/bank_health.sh
# 설계 근거: docs/superpowers/specs/2026-08-14-fiftyone-bank-filter-schema-design.md §5-3
set -uo pipefail

CONTAINER="${BANK_HEALTH_CONTAINER:-docker-analysis-1}"
# source-h 은 2026-08-18 사용자 요청으로 데이터셋 삭제됨 (GT 정본은 sourceh_v2/work/ledger.jsonl 에 잔존).
# 검사기는 없는 데이터셋을 FAIL 로 치므로(저하 실행 방지) 목록에서도 함께 내렸다.
# `frames` 는 2026-08-18 -prompts 동반 데이터셋 개통(prompt DB 연결)과 함께 편입.
# (2026-08-19 개명: frames_captions → frames, frames_captions-prompts → frames-prompts)
DATASETS="${BANK_HEALTH_DATASETS:-sourcei,frames}"
LOG="${BANK_HEALTH_LOG:-/home/user/logs/bank_health.log}"
LOCK="${BANK_HEALTH_LOCK:-/tmp/bank_health.lock}"
TIMEOUT="${BANK_HEALTH_TIMEOUT:-600}"

mkdir -p "$(dirname "$LOG")"

# flock -n: 이전 실행이 아직 돌고 있으면 **겹치지 않고 그냥 빠진다**.
exec 9>"$LOCK"
if ! flock -n 9; then
    echo "[$(date '+%F %T')] SKIP 이전 실행이 진행 중 (중첩 방지)" >>"$LOG"
    exit 0
fi

ts() { date '+%F %T'; }

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "[$(ts)] SKIP 컨테이너 $CONTAINER 미기동" >>"$LOG"
    exit 0
fi

# ⚠️ `--datasets "${DATASETS//,/ }"` 로 쓰면 "sourcei source-h" 이 **한 인자**로 넘어가
#    데이터셋 로드가 실패하고, 경계 케이스만 검사한 채 **초록으로 끝난다**(2026-08-14 실측).
#    배열로 분리해 넘긴다.
IFS=',' read -ra DS <<<"$DATASETS"
out=$(timeout "$TIMEOUT" docker exec "$CONTAINER" \
        python /workspace/bank_tags_contract.py --datasets "${DS[@]}" 2>&1)
rc=$?

# 저하 실행(라이브 데이터셋 미검사)은 `bank_tags_contract.py` 가 스스로 FAIL 로 처리한다 —
# 가드는 검사기 안에 있어야지 래퍼에 있으면 다음 래퍼가 또 빠뜨린다.

if [ $rc -eq 0 ]; then
    echo "[$(ts)] OK  $(echo "$out" | tail -1)" >>"$LOG"
    exit 0
fi

# 실패는 조용히 넘기지 않는다 — 이 사고가 몇 달 간 무증상이었던 이유가 그것이다.
{
    echo "[$(ts)] FAIL rc=$rc — 뱅크 태그 계약 위반"
    echo "$out" | sed 's/^/    /'
    echo "    ↳ 조치: docs/superpowers/specs/2026-08-14-fiftyone-bank-filter-schema-design.md §3 D7"
} >>"$LOG"
echo "$out" >&2
exit "$rc"
