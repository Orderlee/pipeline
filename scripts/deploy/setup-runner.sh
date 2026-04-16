#!/usr/bin/env bash
# =====================================================
# GitHub Self-Hosted Runner 설치 스크립트
# 운영 서버에서 실행: bash scripts/deploy/setup-runner.sh
# =====================================================
set -euo pipefail

RUNNER_DIR="${HOME}/actions-runner"
RUNNER_VERSION="2.322.0"
RUNNER_ARCH="linux-x64"
RUNNER_TAR="actions-runner-${RUNNER_ARCH}-${RUNNER_VERSION}.tar.gz"
RUNNER_URL="https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${RUNNER_TAR}"

REPO_URL="https://github.com/Orderlee/Datapipeline-Data-data_pipeline"
DEFAULT_RUNNER_NAME="prod-runner"
DEFAULT_RUNNER_LABELS="self-hosted,production,linux"

usage() {
    cat <<'EOF'
GitHub Self-Hosted Runner 설치 스크립트

사용법:
  bash scripts/deploy/setup-runner.sh
  bash scripts/deploy/setup-runner.sh --token <registration-token>
  RUNNER_TOKEN=<registration-token> bash scripts/deploy/setup-runner.sh

옵션:
  -t, --token TOKEN   GitHub self-hosted runner 등록 토큰
  -h, --help          도움말 출력

참고:
  - 보안상 shell history 노출을 줄이려면 `RUNNER_TOKEN=... bash ...` 방식을 권장합니다.
  - token이 없으면 실행 중에 안전하게 입력받습니다.
EOF
}

RUNNER_TOKEN="${RUNNER_TOKEN:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--token)
            if [[ $# -lt 2 ]]; then
                echo "[!] --token 옵션에는 값이 필요합니다."
                exit 1
            fi
            RUNNER_TOKEN="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "[!] 알 수 없는 옵션: $1"
            echo ""
            usage
            exit 1
            ;;
    esac
done

echo "============================================="
echo " GitHub Self-Hosted Runner 설치"
echo "============================================="
echo ""

# 1) Runner 디렉토리 생성
if [ -d "${RUNNER_DIR}" ]; then
    echo "[!] ${RUNNER_DIR} 이미 존재합니다. 기존 설치를 사용합니다."
else
    echo "[1/4] Runner 디렉토리 생성: ${RUNNER_DIR}"
    mkdir -p "${RUNNER_DIR}"
fi

cd "${RUNNER_DIR}"

# 2) Runner 바이너리 다운로드
if [ ! -f "./config.sh" ]; then
    echo "[2/4] Runner 다운로드: v${RUNNER_VERSION}"
    curl -o "${RUNNER_TAR}" -L "${RUNNER_URL}"
    tar xzf "${RUNNER_TAR}"
    rm -f "${RUNNER_TAR}"
else
    echo "[2/4] Runner 바이너리 이미 존재 — 스킵"
fi

# 3) Runner 등록 (토큰 필요)
echo ""
echo "[3/4] Runner 등록"
echo "  GitHub에서 등록 토큰을 발급받아야 합니다:"
echo "  1. ${REPO_URL}/settings/actions/runners/new 접속"
echo "  2. 'New self-hosted runner' 클릭"
echo "  3. 토큰 복사"
echo ""

if [ -n "${RUNNER_TOKEN}" ]; then
    echo "  등록 토큰 입력 소스: 인자/환경변수"
else
    read -rsp "  등록 토큰을 입력하세요: " RUNNER_TOKEN
    echo ""
fi

if [ -z "${RUNNER_TOKEN}" ]; then
    echo "[!] 토큰이 비어있습니다. 중단합니다."
    exit 1
fi

./config.sh \
    --url "${REPO_URL}" \
    --token "${RUNNER_TOKEN}" \
    --name "${DEFAULT_RUNNER_NAME}" \
    --labels "${DEFAULT_RUNNER_LABELS}" \
    --work "_work" \
    --unattended \
    --replace

# 4) systemd 서비스 등록
echo ""
echo "[4/4] systemd 서비스 등록"
sudo ./svc.sh install
sudo ./svc.sh start

echo ""
echo "============================================="
echo " 설치 완료!"
echo " 상태 확인: sudo ./svc.sh status"
echo " 로그 확인: journalctl -u actions.runner.*.service -f"
echo "============================================="
