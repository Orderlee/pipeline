#!/bin/bash
# YOLO-World-L v2 모델 다운로드 스크립트
#
# 사용법:
#   sudo bash scripts/download_yolo_model.sh
#
# 모델 저장 위치: docker/data/models/yolo/yolov8l-worldv2.pt
# Docker 컨테이너 내부 경로: /data/models/yolo/yolov8l-worldv2.pt

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
MODEL_DIR="${REPO_ROOT}/docker/data/models/yolo"
MODEL_FILE="${MODEL_DIR}/yolov8l-worldv2.pt"
MODEL_URL="https://github.com/ultralytics/assets/releases/download/v8.3.0/yolov8l-worldv2.pt"

echo "=== YOLO-World-L v2 모델 다운로드 ==="
echo "저장 위치: ${MODEL_FILE}"
echo ""

mkdir -p "${MODEL_DIR}"

if [ -f "${MODEL_FILE}" ]; then
    FILE_SIZE=$(stat -c%s "${MODEL_FILE}" 2>/dev/null || stat -f%z "${MODEL_FILE}" 2>/dev/null || echo "0")
    if [ "${FILE_SIZE}" -gt 50000000 ]; then
        echo "모델 파일이 이미 존재합니다 (${FILE_SIZE} bytes). 건너뜁니다."
        echo "재다운로드하려면 파일을 삭제 후 다시 실행하세요:"
        echo "  rm ${MODEL_FILE}"
        exit 0
    fi
    echo "기존 파일이 불완전합니다. 재다운로드합니다."
fi

echo "다운로드 중: ${MODEL_URL}"
echo "예상 크기: ~91MB"
echo ""

if command -v wget &>/dev/null; then
    wget -O "${MODEL_FILE}" "${MODEL_URL}"
elif command -v curl &>/dev/null; then
    curl -L -o "${MODEL_FILE}" "${MODEL_URL}"
else
    echo "ERROR: wget 또는 curl이 필요합니다."
    exit 1
fi

FILE_SIZE=$(stat -c%s "${MODEL_FILE}" 2>/dev/null || stat -f%z "${MODEL_FILE}" 2>/dev/null || echo "0")
echo ""
echo "=== 다운로드 완료 ==="
echo "파일: ${MODEL_FILE}"
echo "크기: ${FILE_SIZE} bytes"
echo ""
echo "Docker 컨테이너 내부 경로: /data/models/yolo/yolov8l-worldv2.pt"
echo "환경변수: YOLO_MODEL_PATH=/data/models/yolo/yolov8l-worldv2.pt"
