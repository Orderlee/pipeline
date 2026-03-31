#!/usr/bin/env bash
# 한 사이클(레거시 파일 ingress QA): safe_drop → archive_pending 대기 → 트리거
# 사용: STAGING_ROOT=/home/pia/mou/staging bash scripts/staging_qa_run_cycle.sh tmp_data_2 qa_c1_ts timestamp
#       STAGING_ROOT=... bash scripts/staging_qa_run_cycle.sh GS건설 qa_x bbox captioning
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
STAGING_ROOT="${STAGING_ROOT:-/home/pia/mou/staging}"
export STAGING_ROOT
FOLDER="${1:?folder e.g. tmp_data_2}"
REQ="${2:?request_id}"
shift 2
OUTPUTS=("$@")
if [[ ${#OUTPUTS[@]} -eq 0 ]]; then
  echo "outputs 필요 (예: timestamp)" >&2
  exit 1
fi
echo "=== [QA] safe_drop → incoming: $FOLDER ==="
# 선택: QA_DROP_SOURCE=/abs/path/to/small/tmp_data_2 (대용량 원본 대신)
bash scripts/staging_qa_safe_drop_incoming.sh "$FOLDER"
echo "=== [QA] archive_pending 대기 후 트리거: ${OUTPUTS[*]} ==="
python3 scripts/staging_test_dispatch.py \
  --folder "$FOLDER" \
  --skip-copy \
  --wait-archive-pending \
  --request-id "$REQ" \
  --round 1 \
  --test-idx 1 \
  --outputs "${OUTPUTS[@]}"
