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
if [[ -f "${WORKSPACE_ROOT}/docker/.env.test.example" ]]; then
    rsync -a "${WORKSPACE_ROOT}/docker/.env.test.example" "${DEPLOY_ROOT}/.env.test.template"
elif [[ -f "${WORKSPACE_ROOT}/docker/.env.test" ]]; then
    # legacy 호환 — .env.test 가 git tracked 였던 시절의 fallback (점진 제거).
    rsync -a "${WORKSPACE_ROOT}/docker/.env.test" "${DEPLOY_ROOT}/.env.test.template"
fi

# DEPLOY_REPO_ROOT 의 git tree 도 GIT_TARGET_REF 로 hard-reset 하여
# rsync 결과와 git HEAD 가 항상 일치하게 만든다. 이렇게 하지 않으면 호스트의
# `git status` / `git log` 가 영원히 stale 로 보여 운영자가 혼동한다.
# GIT_TARGET_REF 가 설정 안 되었거나 .git 이 없으면 silent skip (legacy 호환).
GIT_TARGET_REF="${GIT_TARGET_REF:-}"
if [[ -n "${GIT_TARGET_REF}" && -d "${DEPLOY_REPO_ROOT}/.git" ]]; then
    if git -C "${DEPLOY_REPO_ROOT}" fetch --quiet origin 2>/dev/null; then
        git -C "${DEPLOY_REPO_ROOT}" reset --hard --quiet "${GIT_TARGET_REF}"
        echo "Deploy repo synced to ${GIT_TARGET_REF}: $(git -C "${DEPLOY_REPO_ROOT}" rev-parse --short HEAD)"
    else
        echo "::warning::git fetch failed in ${DEPLOY_REPO_ROOT}; skipping git reset"
    fi
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
    # 2026-05-21: sam3 도 build target 에 포함 — docker/sam3/ 변경 시 자동 rebuild.
    # 이전에는 app 만 빌드해서 docker/sam3/app.py 변경이 컨테이너에 반영 안 됐음.
    # layer cache 로 sam3 변경 없으면 빠름.
    compose build --progress plain app sam3
else
    echo "Skipping image build for ${DEPLOY_LABEL}"
fi

if [[ -n "${IMAGE_TAG}" ]]; then
    docker tag "${IMAGE_NAME}" "${IMAGE_TAG}"
fi

# postgres: compose 변경 (image swap 등) 자동 감지하여 recreate.
# 변화 없으면 no-op (compose 가 same-state 면 skip). prod (.env) 는 POSTGRES_IMAGE
# 미설정 → default postgres:15 그대로 유지 → 영향 0. staging (.env.test) 에서
# POSTGRES_IMAGE=pgduckdb/pgduckdb:15-v1.1.1 같이 override 시 자동 swap.
# postgres_data volume 보존 → schema/데이터 손실 없음.
echo "Ensuring postgres up-to-date with compose..."
compose up -d postgres
# postgres healthy 까지 대기 (최대 60s) — dagster 가 PG 접속 시점 race 방지.
for i in $(seq 1 30); do
    state=$(compose ps -a postgres --format json 2>/dev/null | python3 -c "import json,sys; data=sys.stdin.read().strip(); print('') if not data else print(json.loads(data.split(chr(10))[0]).get('Health',''))" 2>/dev/null || echo "")
    if [[ "${state}" == "healthy" ]]; then
        echo "postgres healthy (${i})"
        break
    fi
    sleep 2
done

# 기존 dagster 컨테이너 정지 후 재시작 (포트 충돌 방지)
compose stop dagster dagster-daemon dagster-code-server 2>/dev/null || true
compose rm -f dagster dagster-daemon dagster-code-server 2>/dev/null || true

compose up -d --no-deps dagster-code-server
echo "code-server restart complete; waiting for gRPC stability..."
sleep 15
compose up -d --no-deps dagster-daemon
compose up -d --no-deps dagster
echo "Dagster services restarted"

# 2026-05-21: sam3 도 image 변경 시 명시 force-recreate.
# `docker compose up -d` 가 image SHA 변경을 항상 감지하진 않음 (같은 tag 라 unchanged 로 판단).
# BUILD_REQUIRED=true 면 image 갱신됐을 가능성 → sam3 재기동.
if [[ "${BUILD_REQUIRED}" == "true" ]]; then
    compose up -d --no-deps --force-recreate sam3
    echo "sam3 force-recreated to pick up new image"
fi

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
