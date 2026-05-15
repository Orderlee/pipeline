#!/usr/bin/env bash
# GenAI Studio — 미리 정의된 옵션으로 batch 1회 실행.
#
# 사용:
#   1) 아래 ▼▼▼ "사용자 수정 영역" ▼▼▼ 안의 값들을 본인 시나리오에 맞춰 수정
#   2) 저장 후 ./scripts/run-batch.sh 실행
#
# 시나리오별로 분리하려면 이 파일 복사해 새 .sh 만들면 됨:
#   cp scripts/run-batch.sh scripts/run-veo-landscape.sh
set -euo pipefail


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼ 사용자 수정 영역 ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

# 1. 엔진 — 5종 중 하나 선택
#    kling      (image2video, Kling Pro 플랜)
#    higgsfield (image2video, fal.ai)
#    veo        (image2video, Vertex AI)
#    nanobanana (image2image, Gemini)
#    gpt_image  (image2image, OpenAI)
ENGINE="nanobanana"

# 2. 이미지 경로 — 파일 / 디렉토리 / 빈 문자열
#    파일      : 1장 batch
#    디렉토리  : 안의 모든 png/jpg/webp 를 alphabetical 순서로 한 batch 에 일괄
#                (최대 20장, ≤50MB/장)
#    빈 문자열 : 이미지 없음. ENGINE='veo' 인 경우에만 가능 (txt2video).
IMAGE="./test.png"
# IMAGE="./images/"     # ← 디렉토리 예시
# IMAGE=""              # ← veo txt2video (텍스트만)

# 3. 프롬프트 — 한 줄 또는 여러 줄
PROMPT="A cinematic slow camera pan over a sun-drenched landscape"

# ────────────────────────────────────────────────────────────
# 엔진별 세부 옵션 — ENGINE 이 kling/veo 일 때만 의미 있음

# Kling 옵션
KLING_MODEL="kling-video-o1"      # 모델 ID
KLING_MODE="pro"                   # std | pro | master
KLING_DURATION="5"                 # 5 | 10 (초)
KLING_ASPECT="16:9"                # 16:9 | 9:16 | 1:1

# Veo 옵션
VEO_MODEL="veo-3.1-generate-001"
VEO_DURATION="8"                   # 4 | 6 | 8 (초)
VEO_ASPECT="16:9"                  # 16:9 | 9:16

# ────────────────────────────────────────────────────────────
# 공통

WAIT="true"        # true 면 결과 도착까지 대기 / false 면 batch_id 받고 즉시 종료
TIMEOUT="900"      # WAIT="true" 시 대기 한도 (초)

# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲ 사용자 수정 영역 끝 ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI="$SCRIPT_DIR/genai-cli.sh"

# IMAGE 가 비었으면 --text-only (veo txt2video), 디렉토리면 --images-from-dir,
# 파일이면 --image 자동 분기.
if [[ -z "$IMAGE" ]]; then
    if [[ "$ENGINE" != "veo" ]]; then
        echo "error: IMAGE='' 는 veo (txt2video) 에서만 가능합니다. ENGINE=$ENGINE" >&2
        exit 1
    fi
    opts=( submit "$ENGINE" --text-only --prompt "$PROMPT" )
    IMAGE_LABEL="(none — txt2video)"
elif [[ -d "$IMAGE" ]]; then
    opts=( submit "$ENGINE" --images-from-dir "$IMAGE" --prompt "$PROMPT" )
    IMAGE_LABEL="$IMAGE/  (dir, alphabetical)"
elif [[ -f "$IMAGE" ]]; then
    opts=( submit "$ENGINE" --image "$IMAGE" --prompt "$PROMPT" )
    IMAGE_LABEL="$IMAGE  (single file)"
else
    echo "error: IMAGE='$IMAGE' 가 파일/디렉토리로 존재하지 않습니다." >&2
    exit 1
fi
case "$ENGINE" in
    kling)
        opts+=( --model "$KLING_MODEL" --mode "$KLING_MODE"
                --duration "$KLING_DURATION" --aspect-ratio "$KLING_ASPECT" )
        ;;
    veo)
        opts+=( --model "$VEO_MODEL" --duration "$VEO_DURATION"
                --aspect-ratio "$VEO_ASPECT" )
        ;;
    higgsfield|nanobanana|gpt_image)
        # 추가 옵션 없음 — 기본값 사용
        ;;
    *)
        echo "error: 알 수 없는 ENGINE=$ENGINE" >&2
        exit 1
        ;;
esac

if [[ "$WAIT" == "true" ]]; then
    opts+=( --wait --timeout "$TIMEOUT" )
fi

echo "▶ ENGINE=$ENGINE"
echo "▶ IMAGE : $IMAGE_LABEL"
echo "▶ PROMPT: $PROMPT"
echo "▶ WAIT=$WAIT TIMEOUT=${TIMEOUT}s"
echo

exec "$CLI" "${opts[@]}"
