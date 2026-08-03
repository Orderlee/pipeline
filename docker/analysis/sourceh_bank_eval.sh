#!/usr/bin/env bash
# 프롬프트 뱅크 버전 비교 — 표준 절차 원커맨드 (요구 #4: 표준화·자동화).
#
#   ./docker/analysis/sourceh_bank_eval.sh v1.0.8.4 v1.0.9.0 [/path/to/text_features_v1.0.9.0.csv]
#
# 하는 일 (전부 멱등):
#   0. 새 버전 CSV 가 주어지면 /embed_text 로 뱅크 임베딩 생성 (bank 스테이지, ~2분)
#   1. analyze — 동일예산/matched-min/한계곡선 (참고용) + cache
#   2. gap     — 신버전 미검출 프레임 군집 + 후보 문장 프로브 → "다음 타깃"
#                ⚠️ 이게 빠지면 사이드바 "다음 타깃"이 **옛 버전 군집**을 계속 표시한다
#   3. flips   — 오탐→정탐/정탐→오탐 프레임 식별 + 이유 분해          (요구 #1·#2)
#   4. prune   — 문장별 선언클래스 순도 / LOO 제거이득 / 탐욕 그룹제거 → 삭제 랭킹 CSV
#                + Embeddings 패널 Color-by 용 winner_* 필드
#   5. viz     — margin_viz(사분면) 산점도
#   6. guide   — 장면어×이벤트절 후보의 FN구조율/유발FP/선택도 자동 측정  (요구 #3)
#                (도입부 숫자는 flips.json/prune.json 에서 읽는다 — 하드코딩 아님)
#   7. slim    — 분석 표면 큐레이션 (필드/brain/워크스페이스/사이드바)
#   8. report  — markdown 종합
#
# 판정 기준 (docs/source-h-prompt-geometry-2026-07-31.md §11):
#   · H1/H2 구분이 필요하면 매칭 카운트 팩토리얼을 별도 실행 (동일예산 검정은 참고용)
#   · 새 문장 채택: 유발 FP ≤ 0.10% 중 FN 구조율 최대 (guide 표의 ✅)
#   · 뱅크는 공동 조율돼 있다 — 클래스 단위 문장 수입은 전체 재평가 통과 시에만
#
# 전제: source-h 프레임 데이터셋이 준비돼 있어야 한다 (sourceh_frames_sync.sh --build).
set -euo pipefail

VER_A="${1:?사용법: sourceh_bank_eval.sh <기준버전> <신버전> [신버전CSV]}"
VER_B="${2:?신버전 이름 필요}"
CSV_B="${3:-}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
C="${ANALYSIS_CONTAINER:-docker-analysis-1}"

docker cp "$REPO/docker/analysis/sourceh_prompt_geometry.py" "$C:/workspace/" >/dev/null

run() { docker exec -e BANK_A="$VER_A" -e BANK_B="$VER_B" "$C" \
        python3 /workspace/sourceh_prompt_geometry.py "$@"; }

# 새 버전 npz 가 없고 CSV 가 주어졌으면 임베딩 생성
if [[ -n "$CSV_B" ]]; then
  docker cp "$CSV_B" "$C:/tmp/bank_b.csv"
  run bank --csv /tmp/bank_b.csv --version "$VER_B"
fi

# 순서 고정: gap → flips → prune → viz → guide → slim.
#   guide 도입부가 flips.json/prune.json 을 읽고, slim 이 prune 의 winner_* 필드를
#   사이드바에 편입한다. slim 은 반드시 마지막 직전.
for st in analyze gap flips prune viz guide slim report; do
  run "$st"
done

echo
echo "완료 — FiftyOne: http://10.0.0.10:5153/datasets/source-h"
echo "  · 뷰 30_fixed(오탐→정탐)/31_broken, 05~07_gap_*"
echo "  · 워크스페이스 flips / margin / prompt / gap / explore"
echo "  · 프롬프트 차이는 워크스페이스 'prompt' → Color by winner_purity_* / winner_loo_* /"
echo "    winner_pair_cos. **먼저 Color by 를 camera 로 바꿔 널 모델을 확인**할 것"
echo "    (승자문장→카메라 예측력 82~87% — 카메라 지도와 닮으면 그 그림은 무의미)"
echo "  · 삭제 랭킹: /data/fiftyone/sourceh_v2/report/prune_<version>.csv"
echo "  · 작성 가이드: /data/fiftyone/sourceh_v2/report/prompt_authoring_guide.md"
