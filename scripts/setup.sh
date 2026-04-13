#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo ">>> Setting up Data Pipeline workspace..."

# 1) Python 패키지 설치 (현재 표준: pyproject)
echo ">>> Installing Python dependencies (editable mode)..."
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install -e "${ROOT_DIR}"

# 2) 설정 파일 템플릿 복사
echo ">>> Initializing config file..."
if [ ! -f "${ROOT_DIR}/configs/global.yaml" ]; then
    if [ -f "${ROOT_DIR}/configs/config.sample.yaml" ]; then
        cp "${ROOT_DIR}/configs/config.sample.yaml" "${ROOT_DIR}/configs/global.yaml"
        echo ">>> Created configs/global.yaml"
    else
        echo "[WARN] configs/config.sample.yaml not found!"
    fi
else
    echo ">>> configs/global.yaml already exists. Skipping."
fi

echo ""
echo "Setup complete! Next steps:"
echo "  1. Review '${ROOT_DIR}/docker/.env' paths (INCOMING/ARCHIVE/PROJECTS)."
echo "  2. Run 'docker compose -f ${ROOT_DIR}/docker/docker-compose.yaml up -d app dagster minio postgres'."
echo "  3. Run '${ROOT_DIR}/scripts/verify_mvp.sh'."
