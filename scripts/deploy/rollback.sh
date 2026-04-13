#!/usr/bin/env bash
# =====================================================
# 운영 배포 롤백 스크립트
# 사용법: bash scripts/deploy/rollback.sh [IMAGE_TAG]
# 예시:   bash scripts/deploy/rollback.sh datapipeline:abc12345
# =====================================================
set -euo pipefail

COMPOSE_DIR="$(cd "$(dirname "$0")/../../docker" && pwd)"
IMAGE_TAG="${1:-}"

if [ -z "${IMAGE_TAG}" ]; then
    echo "사용 가능한 이미지 태그:"
    docker images --format '{{.Repository}}:{{.Tag}}  {{.CreatedAt}}' \
        | grep "^datapipeline:" \
        | head -10
    echo ""
    echo "사용법: $0 <IMAGE_TAG>"
    echo "예시:   $0 datapipeline:abc12345"
    exit 1
fi

echo "============================================="
echo " 롤백: ${IMAGE_TAG}"
echo "============================================="

cd "${COMPOSE_DIR}"

docker tag "${IMAGE_TAG}" datapipeline:gpu-cu124

echo "[1/3] code-server 재시작..."
docker compose up -d --no-deps dagster-code-server
sleep 15

echo "[2/3] daemon + webserver 재시작..."
docker compose up -d --no-deps dagster-daemon
docker compose up -d --no-deps dagster

echo "[3/3] Health check..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:3030/server_info > /dev/null 2>&1; then
        echo "롤백 완료 — Dagster 정상 응답 (${i}초)"
        exit 0
    fi
    sleep 2
done

echo "[!] Dagster가 60초 내 응답하지 않음 — 수동 확인 필요"
exit 1
