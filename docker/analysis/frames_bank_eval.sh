#!/usr/bin/env bash
# frames_captions 프롬프트 뱅크 평가 — 원커맨드 (스펙 §5-3).
#
#   ./docker/analysis/frames_bank_eval.sh                           # 전체 사이클
#   ./docker/analysis/frames_bank_eval.sh --bank v1.0.9.0 /path/text_features_v1.0.9.0.csv
#
# 매핑(bank_domain_map.yaml)이 비어 있으면 채점은 hard-skip 되고 스탬프만 찍힌다 = 0단계 정상.
# GT 가 늘었을 때 재채점 없이 GT 만 갱신하려면: frames_bank_ledger.py → gtsync → report
# 세 스테이지만 재실행 (재채점 불필요). GT 스냅샷은 frames_bank_ledger.py 가 만들므로
# ledger 없이 gtsync 만 돌리면 stale snapshot 을 재동기화하게 된다.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
C="${ANALYSIS_CONTAINER:-docker-analysis-1}"

# ambient /workspace 의존 금지 — 필요 파일 전부 명시 반입 (drift 차단, 스펙 §10 불채택 항목의 해소)
for f in prompt_geometry.py frames_bank_ledger.py bank_domain_map.yaml fiftyone_presentation.py; do
  docker cp "$REPO/docker/analysis/$f" "$C:/workspace/" >/dev/null
done

run() { docker exec -e OMP_NUM_THREADS=4 -e OPENBLAS_NUM_THREADS=4 \
        -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml "$C" python3 "$@"; }

if [[ "${1:-}" == "--bank" ]]; then
  VER="${2:?사용법: --bank <버전> <CSV경로>}"
  CSV="${3:?CSV 경로 필요}"
  docker cp "$CSV" "$C:/tmp/bank_new.csv"
  run /workspace/prompt_geometry.py bank --profile frames --csv /tmp/bank_new.csv --version "$VER"
fi

run /workspace/prompt_geometry.py selftest --profile frames
run /workspace/frames_bank_ledger.py
for st in score gap viz gtsync report; do
  run /workspace/prompt_geometry.py "$st" --profile frames
done

echo
echo "완료 — http://10.0.0.10:5153/datasets/frames_captions"
echo "  · 워크스페이스 bank-eval / 뷰 'bank: <도메인> …' / 사이드바 ⑥ 프롬프트뱅크"
echo "  · 리포트: docker exec $C cat /data/fiftyone/frames_bank/report/bank_eval_report.md"
