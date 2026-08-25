#!/usr/bin/env bash
# prompt_cos_db.py 배치 러너 — cron 이 부를 때마다 **pending 스텝 하나만** 처리한다.
#
# 왜 한 스텝씩인가 (실측 근거):
#   · 군집 affinity 를 한 번에 돌리다 `sentence_affinity` 가 3GB→6GB 로 늘어
#     루트 디스크가 98%→99%(가용 12GB)까지 내려갔다. PG 데이터가 이 디스크에 있어
#     채우면 프로덕션이 멈춘다. 그래서 스텝을 쪼개고 매 스텝 앞에 디스크 가드를 둔다.
#   · 벡터 전용 뱅크 JSON 은 파일당 0.5~1.6GB 라 로딩만 10분 넘게 걸린다. 한 뱅크가
#     한 스텝이다.
#
# 왜 컨테이너가 아니라 호스트에서 도는가:
#   벡터 전용 뱅크 원본(/home/user/mou/userwatch/prompts)이 analysis 컨테이너에
#   마운트돼 있지 않다. 호스트 anaconda 에 numpy/psycopg2/sklearn/scipy 가 다 있어
#   한 환경으로 통일하는 쪽이 낫다 (컨테이너/호스트 혼용은 경로 사고를 부른다).
#   ⚠️ 맨 `python` 은 깨진 macOS venv 가 PATH 최선두라 절대 쓰지 말 것.
#
# 상태는 DB 원장(analysis.batch_step)에 있다. 로그 파일이 아니라 그게 진실이다.
set -uo pipefail

REPO=/home/user/work_p/Datapipeline-Data-data_pipeline
PY=/home/user/anaconda3/bin/python
SCRIPT="$REPO/docker/analysis/prompt_cos_db.py"
LOCK=/tmp/prompt_cos_batch.lock
LOG_DIR="$REPO/docker/analysis/.cron_logs"
LOG="$LOG_DIR/batch_$(date +%Y-%m-%d).log"
MIN_FREE_GB="${MIN_FREE_GB:-8}"

mkdir -p "$LOG_DIR"
say() { echo "[$(date '+%F %T')] $*" >>"$LOG"; }

exec 9>"$LOCK"
if ! flock -n 9; then
  say "이전 실행이 아직 도는 중 — 건너뜀"
  exit 0
fi

# ── 시간 가드: 02:40 일일 cron(prompt_cos_cron.sh)과 겹치지 않게 비운다 ──
#    둘은 서로 다른 lock 이라 동시 실행이 가능하고, 겹치면 CPU/RAM 을 두 배로 먹는다.
#    쓰는 bank_version 은 서로 disjoint 라 데이터 충돌은 아니다 — 자원 충돌만 막는다.
H=$(date +%-H)
if [[ "$H" -ge 2 && "$H" -lt 4 ]]; then
  say "02~04시는 일일 cron 구간 — 건너뜀"
  exit 0
fi

# ── 디스크 가드: 이게 이 스크립트의 존재 이유다 ──
AVAIL=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
if [[ -z "$AVAIL" || "$AVAIL" -lt "$MIN_FREE_GB" ]]; then
  say "루트 여유 ${AVAIL:-?}GB < ${MIN_FREE_GB}GB — 중단 (ENOSPC 방지)"
  exit 1
fi

export DATAOPS_POSTGRES_DSN="${DATAOPS_POSTGRES_DSN:-postgresql://airflow:airflow@localhost:15433/vlm_pipeline}"
export EXT_BANK_DIR="${EXT_BANK_DIR:-/home/user/mou/userwatch/prompts}"
export COS_REPORT_DIR="${COS_REPORT_DIR:-$REPO/docker/data/fiftyone/frames_bank/report}"
export COS_THREADS="${COS_THREADS:-6}"

# ── 스텝 종류에 따라 실행기를 고른다 ──
#   score-ext / topk-ext : 원본 JSON 이 호스트에만 있다 (컨테이너 미마운트). sklearn 불필요.
#   그 외      : sklearn 필요 → 컨테이너 (호스트 anaconda 의 sklearn 은 numpy ABI 불일치로 깨짐)
KIND=$(nice -n 10 "$PY" "$SCRIPT" batch-peek 2>/dev/null | cut -f1)
say "다음 스텝 종류: ${KIND:-?} (여유 ${AVAIL}GB)"

if [[ "$KIND" == "none" || -z "$KIND" ]]; then
  say "pending 스텝 없음 — 배치 완료"
  exit 0
fi

if [[ "$KIND" == "score-ext" || "$KIND" == "topk-ext" ]]; then
  nice -n 10 "$PY" "$SCRIPT" batch-next >>"$LOG" 2>&1
  rc=$?
else
  if ! docker inspect -f '{{.State.Running}}' docker-analysis-1 2>/dev/null | grep -qx true; then
    say "docker-analysis-1 미기동 — 이 스텝은 다음 tick 에 재시도 (원장 변경 없음)"
    exit 0
  fi
  docker exec -e COS_THREADS="$COS_THREADS" docker-analysis-1 nice -n 10 \
    python3 /workspace/prompt_cos_db.py batch-next \
    --top-per-cell "${TOP_PER_CELL:-50}" >>"$LOG" 2>&1
  rc=$?
fi
say "=== batch-next($KIND) 종료 rc=$rc ==="

# 남은 스텝 요약을 로그 꼬리에 남긴다 (사람이 tail 만 봐도 진행률이 보이게)
nice -n 10 "$PY" - <<'PYEOF' >>"$LOG" 2>&1
import os, psycopg2
cur = psycopg2.connect(os.environ["DATAOPS_POSTGRES_DSN"]).cursor()
cur.execute("SELECT status, COUNT(*) FROM analysis.batch_step GROUP BY 1 ORDER BY 1")
print("  진행률:", ", ".join(f"{s}={n}" for s, n in cur))
cur.execute("""SELECT kind, arg, note FROM analysis.batch_step
               WHERE status='failed' ORDER BY step_id LIMIT 5""")
for k, a, nt in cur:
    print(f"  실패: {k} {a} — {nt}")
PYEOF

find "$LOG_DIR" -name 'batch_*.log' -mtime +14 -delete 2>/dev/null || true
exit $rc
