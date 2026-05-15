#!/usr/bin/env bash
# GenAI Studio CLI — shell shim.
# 사용:
#   ./scripts/genai-cli submit nanobanana --image foo.png --prompt "test"
#   ./scripts/genai-cli batches list --status running
#   ./scripts/genai-cli wait <batch_id>
#   ./scripts/genai-cli config init
#
# PYTHONPATH 에 scripts/ 를 추가하여 genai_cli 패키지를 import 가능하게 한다.
set -euo pipefail

# scripts/ 디렉토리 = 이 파일의 부모
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# python 명령 선택 (python3 우선)
if command -v python3 >/dev/null 2>&1; then
    PY=python3
elif command -v python >/dev/null 2>&1; then
    PY=python
else
    echo "error: python3 이 PATH 에 없습니다." >&2
    exit 127
fi

# requests 의존성 확인 — 없으면 명확한 가이드
if ! "$PY" -c "import requests" >/dev/null 2>&1; then
    echo "error: 'requests' 패키지가 필요합니다." >&2
    echo "  설치: pip install --user requests" >&2
    exit 127
fi

# py 3.10 의 경우 tomli 도 필요 (3.11+ 는 stdlib tomllib)
PYVER=$("$PY" -c "import sys; print(sys.version_info.minor)")
if [[ "$PYVER" -lt 11 ]]; then
    if ! "$PY" -c "import tomli" >/dev/null 2>&1; then
        echo "error: Python 3.${PYVER} 에는 'tomli' 가 필요합니다." >&2
        echo "  설치: pip install --user tomli" >&2
        exit 127
    fi
fi

# PYTHONPATH 에 scripts/ 추가하고 genai_cli 모듈 실행
PYTHONPATH="${SCRIPT_DIR}${PYTHONPATH:+:$PYTHONPATH}" exec "$PY" -m genai_cli "$@"
