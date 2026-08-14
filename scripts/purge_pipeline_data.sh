#!/usr/bin/env bash
set -euo pipefail

# Wrapper: run purge script inside running pipeline container.
# Default target container: docker-dagster-1 (prod; fallback: pipeline-test-dagster-1 staging).

CONTAINER="${PIPELINE_CONTAINER:-}"
SCRIPT_PATH="/scripts/purge_pipeline_data.py"

if [[ -z "${CONTAINER}" ]]; then
  if docker ps --format '{{.Names}}' | grep -Fxq 'docker-dagster-1'; then
    CONTAINER="docker-dagster-1"
  elif docker ps --format '{{.Names}}' | grep -Fxq 'pipeline-test-dagster-1'; then
    CONTAINER="pipeline-test-dagster-1"
  else
    echo "[ERROR] No running container found (docker-dagster-1 / pipeline-test-dagster-1)." >&2
    exit 1
  fi
fi

echo "[INFO] container=${CONTAINER}"
echo "[INFO] command=python3 ${SCRIPT_PATH} $*"

docker exec -i "${CONTAINER}" python3 "${SCRIPT_PATH}" "$@"

