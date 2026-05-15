#!/usr/bin/env bash
# GenAI Studio — bulk submit (이미지 × 프롬프트 대량 batch).
#
# 사용 패턴 2가지 중 선택:
#
#   (A) manifest JSON 하나로 명시:
#         BULK_FILE="./my-bulk.json" 로 설정 후 실행. 다른 옵션은 무시됨.
#         manifest 형식:
#           {
#             "engine": "veo",
#             "options": {"duration": "8", "aspect_ratio": "9:16"},
#             "pairs": [
#               {"image": "rel/path.png", "prompt": "..."},
#               {"image": null, "prompt": "txt2video prompt"}
#             ]
#           }
#
#   (B) 편의 입력:
#         PROMPTS_FILE + IMAGES_DIR + ENGINE 조합. PAIR_MODE 로 매핑 규칙 결정.
#         TEXT_ONLY=true 이면 IMAGES_DIR 무시 (veo txt2video 전용).
#
# 실행 흐름:
#   1) --dry-run 으로 plan 출력 (default — 비용/한도 확인)
#   2) 확인 후 CONFIRM=true 로 다시 실행하면 실제 batch 들 제출
#
# 시나리오별로 분리하려면 이 파일을 복사:
#   cp scripts/run-bulk.sh scripts/run-bulk-veo-nightly.sh
set -euo pipefail


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼ 사용자 수정 영역 ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

# ───── 모드 선택 ─────
# A) manifest 한 파일로 모두 명시할 거면 BULK_FILE 만 채우고 나머지 무시:
BULK_FILE=""                  # 예: "./bulk-veo-2026-05-13.json"

# B) 편의 입력 (BULK_FILE 빈 경우 사용):
ENGINE="veo"                   # kling | higgsfield | veo | nanobanana | gpt_image
PROMPTS_FILE="./prompts.json"  # list[str] 또는 {"prompts": [...]}
IMAGES_DIR=""                  # 디렉토리 (PNG/JPG/WEBP). TEXT_ONLY=true 면 무시
TEXT_ONLY="false"              # true 면 이미지 없이 prompt 만 (veo 전용 txt2video)

# 매핑 규칙 — paired (1:1, N==M 필수) | cartesian (N×M 모든 조합)
PAIR_MODE="paired"

# ───── 엔진 옵션 (선택) ─────
# Veo:
VEO_MODEL=""                   # 예: "veo-3.1-generate-001" (비어두면 서버 default)
VEO_DURATION="8"               # 4 | 6 | 8
VEO_ASPECT="16:9"              # 16:9 | 9:16
# Kling (필요 시):
KLING_MODE=""                  # std | pro | master
KLING_DURATION=""              # 5 | 10
KLING_ASPECT=""                # 16:9 | 9:16 | 1:1

# ───── 그룹 식별자 ─────
# 비워두면 자동으로 bgi-<random12> 생성. 명시하면 UI 의 group chip 에 노출.
# 영숫자 / _ / - / . 1~64자.
GROUP_ID=""                    # 예: "nightly.2026-05-13"

# ───── 안전장치 ─────
# 첫 실행은 무조건 dry-run. 결과 확인 후 CONFIRM=true 로 재실행:
CONFIRM="false"
SLEEP_SEC="0.5"                # batch 간 대기 (Vertex/Kling rate-limit 회피)
CONTINUE_ON_ERROR="false"      # true 면 한 batch 실패 시에도 계속 진행

# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲ 사용자 수정 영역 끝 ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI="$SCRIPT_DIR/genai-cli.sh"

opts=( bulk-submit )

# 모드 A vs B 분기
if [[ -n "$BULK_FILE" ]]; then
    if [[ ! -f "$BULK_FILE" ]]; then
        echo "error: BULK_FILE='$BULK_FILE' 파일이 없습니다." >&2
        exit 1
    fi
    opts+=( --file "$BULK_FILE" )
    INPUT_LABEL="manifest=$BULK_FILE"
else
    if [[ ! -f "$PROMPTS_FILE" ]]; then
        echo "error: PROMPTS_FILE='$PROMPTS_FILE' 파일이 없습니다." >&2
        exit 1
    fi
    if [[ -z "$ENGINE" ]]; then
        echo "error: ENGINE 미설정." >&2
        exit 1
    fi
    opts+=( --prompts-file "$PROMPTS_FILE" --engine "$ENGINE" --pair-mode "$PAIR_MODE" )
    if [[ "$TEXT_ONLY" == "true" ]]; then
        if [[ "$ENGINE" != "veo" ]]; then
            echo "error: TEXT_ONLY=true 는 ENGINE=veo 전용 (got $ENGINE)" >&2
            exit 1
        fi
        opts+=( --text-only )
        INPUT_LABEL="prompts=$PROMPTS_FILE  (txt2video, no images)"
    else
        if [[ -z "$IMAGES_DIR" ]]; then
            echo "error: IMAGES_DIR 비어있음 (TEXT_ONLY=true 가 아니면 필수)" >&2
            exit 1
        fi
        if [[ ! -d "$IMAGES_DIR" ]]; then
            echo "error: IMAGES_DIR='$IMAGES_DIR' 디렉토리 아님" >&2
            exit 1
        fi
        opts+=( --images-from-dir "$IMAGES_DIR" )
        INPUT_LABEL="prompts=$PROMPTS_FILE  images=$IMAGES_DIR  pair=$PAIR_MODE"
    fi
fi

# 엔진 옵션 — manifest 모드여도 CLI override 로 추가 가능
case "$ENGINE" in
    veo)
        [[ -n "$VEO_MODEL" ]]    && opts+=( --model "$VEO_MODEL" )
        [[ -n "$VEO_DURATION" ]] && opts+=( --duration "$VEO_DURATION" )
        [[ -n "$VEO_ASPECT" ]]   && opts+=( --aspect-ratio "$VEO_ASPECT" )
        ;;
    kling)
        [[ -n "$KLING_MODE" ]]     && opts+=( --mode "$KLING_MODE" )
        [[ -n "$KLING_DURATION" ]] && opts+=( --duration "$KLING_DURATION" )
        [[ -n "$KLING_ASPECT" ]]   && opts+=( --aspect-ratio "$KLING_ASPECT" )
        ;;
    higgsfield|nanobanana|gpt_image|"")
        # 추가 옵션 없음 — 기본값 사용
        ;;
    *)
        echo "error: 알 수 없는 ENGINE=$ENGINE" >&2
        exit 1
        ;;
esac

[[ -n "$GROUP_ID" ]] && opts+=( --group-id "$GROUP_ID" )
opts+=( --sleep "$SLEEP_SEC" )
[[ "$CONTINUE_ON_ERROR" == "true" ]] && opts+=( --continue-on-error )

if [[ "$CONFIRM" == "true" ]]; then
    opts+=( --confirm )
    MODE_LABEL="LIVE  (--confirm)"
else
    opts+=( --dry-run )
    MODE_LABEL="dry-run  (CONFIRM=true 로 재실행하면 실제 제출)"
fi

echo "▶ MODE   : $MODE_LABEL"
echo "▶ ENGINE : ${ENGINE:-(manifest 안)}"
echo "▶ INPUT  : $INPUT_LABEL"
echo "▶ GROUP  : ${GROUP_ID:-<auto bgi-...>}"
echo "▶ SLEEP  : ${SLEEP_SEC}s between batches"
echo

exec "$CLI" "${opts[@]}"
