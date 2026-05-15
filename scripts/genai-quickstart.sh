#!/usr/bin/env bash
# GenAI Studio CLI 빠른 시작 도우미.
# - 환경 점검 (python, requests, 컨테이너 상태)
# - 자격증명 안내
# - 자주 쓰는 명령 예시 표시
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI="$SCRIPT_DIR/genai-cli.sh"
BASE="${GENAI_CLI_BASE:-http://10.0.0.10:8088}"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
red() { printf '\033[31m%s\033[0m\n' "$*"; }

bold "=== GenAI Studio CLI 빠른 시작 ==="
echo

# 1) python 점검
bold "[1/4] Python 환경"
if command -v python3 >/dev/null 2>&1; then
    green "  ✓ python3: $(python3 --version)"
else
    red "  ✗ python3 미설치"
    exit 1
fi
if python3 -c "import requests" >/dev/null 2>&1; then
    green "  ✓ requests"
else
    yellow "  ! requests 미설치 — pip install --user requests"
fi
echo

# 2) 컨테이너 점검
bold "[2/4] GenAI 컨테이너"
if curl -fs -o /dev/null --connect-timeout 3 "$BASE/healthz" 2>/dev/null; then
    green "  ✓ $BASE/healthz 응답 OK"
else
    yellow "  ! $BASE 접근 불가 (사내망 외부 또는 컨테이너 다운)"
fi
echo

# 3) 자격증명 점검
bold "[3/4] 자격증명"
if [[ -n "${GENAI_CLI_USER:-}" && -n "${GENAI_CLI_PASS:-}" ]]; then
    green "  ✓ env GENAI_CLI_USER/PASS 설정됨"
elif [[ -f "$HOME/.config/genai-cli/config.toml" ]]; then
    mode=$(stat -c '%a' "$HOME/.config/genai-cli/config.toml")
    if [[ "$mode" == "600" ]]; then
        green "  ✓ ~/.config/genai-cli/config.toml (mode 600)"
    else
        yellow "  ! ~/.config/genai-cli/config.toml mode=$mode (0600 권장)"
    fi
else
    yellow "  ! 자격증명 없음"
    echo "    아래 중 하나로 설정하세요:"
    echo "    (a) env  : export GENAI_CLI_USER=user GENAI_CLI_PASS='...'"
    echo "    (b) config: $CLI config init"
fi
echo

# 4) 자주 쓰는 명령
bold "[4/4] 자주 쓰는 명령"
cat <<'EOF'
  # 엔진 목록
  ./scripts/genai-cli.sh engines

  # 단일 이미지 + 프롬프트 (비동기, 즉시 반환)
  ./scripts/genai-cli.sh submit kling --image cat.png --prompt "slow pan"

  # 결과 대기까지 (--wait)
  ./scripts/genai-cli.sh submit veo --image scene.png \
      --prompt "cinematic" --duration 8 --wait --timeout 600

  # Veo txt2video — 이미지 없이 프롬프트만 (veo 전용)
  ./scripts/genai-cli.sh submit veo --text-only \
      --prompt "drone shot over neon city at night" --duration 8 --wait

  # 디렉토리 일괄
  ./scripts/genai-cli.sh submit nanobanana \
      --images-from-dir ./images/ --prompt "$(cat prompt.txt)" --wait

  # batch 목록 / 상세
  ./scripts/genai-cli.sh batches list
  ./scripts/genai-cli.sh batches list --status running --json
  ./scripts/genai-cli.sh batches show <batch_id>

  # 별도 wait (CI resume)
  ./scripts/genai-cli.sh wait <batch_id> --timeout 900

  # 실패 job 재시도
  ./scripts/genai-cli.sh jobs retry <job_id>

  # 비용 / 활동
  ./scripts/genai-cli.sh costs --range week

  # 도움말
  ./scripts/genai-cli.sh --help
  ./scripts/genai-cli.sh submit --help
EOF
echo
bold "Tips"
echo "  - TTY 에서는 table, pipe 에서는 JSON 자동 (--json/--table 으로 강제 가능)"
echo "  - 모든 명령은 종료 코드 반환 (0=성공, 2=실패, 3=partial, 4=timeout)"
echo "  - 자세한 가이드: docs/genai_rollout/cli_plan.md"
