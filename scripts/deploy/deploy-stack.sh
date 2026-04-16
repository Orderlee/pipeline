#!/usr/bin/env bash

set -euo pipefail

require_var() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        echo "::error::Missing required env: ${name}"
        exit 1
    fi
}

require_var WORKSPACE_ROOT
require_var DEPLOY_ROOT
require_var ENV_TARGET_NAME
require_var HEALTHCHECK_URL
require_var DEPLOY_LABEL

COMPOSE_FILE_NAME="${COMPOSE_FILE_NAME:-docker-compose.yaml}"
IMAGE_NAME="${IMAGE_NAME:-datapipeline:gpu-cu124}"
BUILD_REQUIRED="${BUILD_REQUIRED:-true}"
IMAGE_TAG="${IMAGE_TAG:-}"
ENV_SOURCE="${ENV_SOURCE:-}"
ENV_TEMPLATE_SOURCE="${ENV_TEMPLATE_SOURCE:-}"
REQUIRED_ENV_KEYS="${REQUIRED_ENV_KEYS:-DATAOPS_DUCKDB_PATH MINIO_ENDPOINT}"

mkdir -p "${DEPLOY_ROOT}"
DEPLOY_ROOT="$(cd "${DEPLOY_ROOT}" && pwd)"
WORKSPACE_ROOT="$(cd "${WORKSPACE_ROOT}" && pwd)"
DEPLOY_REPO_ROOT="$(cd "${DEPLOY_ROOT}/.." && pwd)"
ENV_TARGET_PATH="${DEPLOY_ROOT}/${ENV_TARGET_NAME}"

mkdir -p "${DEPLOY_REPO_ROOT}/src" "${DEPLOY_REPO_ROOT}/configs" "${DEPLOY_REPO_ROOT}/gcp" \
    "${DEPLOY_REPO_ROOT}/scripts" "${DEPLOY_REPO_ROOT}/split_dataset" "${DEPLOY_ROOT}/app"

echo "Deploy target: ${DEPLOY_LABEL}"
echo "Deploy root: ${DEPLOY_ROOT}"
echo "Compose file: ${COMPOSE_FILE_NAME}"
echo "Env target: ${ENV_TARGET_PATH}"

sync_dir_if_exists() {
    local source_dir="$1"
    local target_dir="$2"
    if [[ -d "${source_dir}" ]]; then
        mkdir -p "${target_dir}"
        rsync -a --delete "${source_dir}/" "${target_dir}/"
    fi
}

sync_dir_if_exists "${WORKSPACE_ROOT}/src" "${DEPLOY_REPO_ROOT}/src"
sync_dir_if_exists "${WORKSPACE_ROOT}/configs" "${DEPLOY_REPO_ROOT}/configs"
sync_dir_if_exists "${WORKSPACE_ROOT}/gcp" "${DEPLOY_REPO_ROOT}/gcp"
sync_dir_if_exists "${WORKSPACE_ROOT}/scripts" "${DEPLOY_REPO_ROOT}/scripts"
sync_dir_if_exists "${WORKSPACE_ROOT}/split_dataset" "${DEPLOY_REPO_ROOT}/split_dataset"
rsync -a --delete --exclude='dagster_home/' --exclude='dagster_home_staging/' --exclude='credentials/' \
    "${WORKSPACE_ROOT}/docker/app/" "${DEPLOY_ROOT}/app/"
rsync -a "${WORKSPACE_ROOT}/docker/Dockerfile" "${DEPLOY_ROOT}/Dockerfile"
rsync -a "${WORKSPACE_ROOT}/docker/docker-compose.yaml" "${DEPLOY_ROOT}/docker-compose.yaml"
if [[ -f "${WORKSPACE_ROOT}/docker/.env.test" ]]; then
    rsync -a "${WORKSPACE_ROOT}/docker/.env.test" "${DEPLOY_ROOT}/.env.test.template"
fi

if [[ -n "${ENV_SOURCE}" && -f "${ENV_SOURCE}" ]]; then
    if [[ "$(cd "$(dirname "${ENV_SOURCE}")" && pwd)/$(basename "${ENV_SOURCE}")" != "${ENV_TARGET_PATH}" ]]; then
        cp "${ENV_SOURCE}" "${ENV_TARGET_PATH}"
    fi
elif [[ -n "${ENV_TEMPLATE_SOURCE}" && -f "${ENV_TEMPLATE_SOURCE}" ]]; then
    cp "${ENV_TEMPLATE_SOURCE}" "${ENV_TARGET_PATH}"
    echo "Seeded env file from template: ${ENV_TEMPLATE_SOURCE}"
else
    echo "::error::No env source available for ${ENV_TARGET_PATH}"
    exit 1
fi

for key in ${REQUIRED_ENV_KEYS}; do
    if ! grep -q "^${key}=" "${ENV_TARGET_PATH}"; then
        echo "::error::Missing required env key in ${ENV_TARGET_NAME}: ${key}"
        exit 1
    fi
done

if ! grep -q '^MINIO_ACCESS_KEY=' "${ENV_TARGET_PATH}" && grep -q '^MINIO_ROOT_USER=' "${ENV_TARGET_PATH}"; then
    printf '\nMINIO_ACCESS_KEY=%s\n' "$(grep '^MINIO_ROOT_USER=' "${ENV_TARGET_PATH}" | tail -n1 | cut -d= -f2-)" >> "${ENV_TARGET_PATH}"
fi
if ! grep -q '^MINIO_SECRET_KEY=' "${ENV_TARGET_PATH}" && grep -q '^MINIO_ROOT_PASSWORD=' "${ENV_TARGET_PATH}"; then
    printf 'MINIO_SECRET_KEY=%s\n' "$(grep '^MINIO_ROOT_PASSWORD=' "${ENV_TARGET_PATH}" | tail -n1 | cut -d= -f2-)" >> "${ENV_TARGET_PATH}"
fi

export PIPELINE_ENV_FILE="${ENV_TARGET_NAME}"

echo "Disk usage before deploy"
df -h
docker system df || true

pushd "${DEPLOY_ROOT}" >/dev/null

compose() {
    docker compose --env-file "${ENV_TARGET_NAME}" -f "${COMPOSE_FILE_NAME}" "$@"
}

if [[ "${BUILD_REQUIRED}" == "true" ]]; then
    compose build --progress plain app
else
    echo "Skipping image build for ${DEPLOY_LABEL}"
fi

if [[ -n "${IMAGE_TAG}" ]]; then
    docker tag "${IMAGE_NAME}" "${IMAGE_TAG}"
fi

# 기존 컨테이너 정지 후 재시작 (포트 충돌 방지)
compose stop dagster dagster-daemon dagster-code-server 2>/dev/null || true
compose rm -f dagster dagster-daemon dagster-code-server 2>/dev/null || true

compose up -d --no-deps dagster-code-server
echo "code-server restart complete; waiting for gRPC stability..."
sleep 15
compose up -d --no-deps dagster-daemon
compose up -d --no-deps dagster
echo "Dagster services restarted"

for i in $(seq 1 30); do
    if curl -sf "${HEALTHCHECK_URL}" > /dev/null 2>&1; then
        echo "Health check passed (${i})"
        popd >/dev/null
        exit 0
    fi
    sleep 2
done

popd >/dev/null
echo "::error::Health check failed: ${HEALTHCHECK_URL}"
exit 1
