#!/usr/bin/env bash
set -euo pipefail

# Wrapper: run purge script inside running pipeline container.
# Default target container: pipeline-dagster-1 (fallback: pipeline-app-1).

CONTAINER="${PIPELINE_CONTAINER:-}"
SCRIPT_PATH="/scripts/purge_pipeline_data.py"

if [[ -z "${CONTAINER}" ]]; then
  if docker ps --format '{{.Names}}' | grep -Fxq 'pipeline-dagster-1'; then
    CONTAINER="pipeline-dagster-1"
  elif docker ps --format '{{.Names}}' | grep -Fxq 'pipeline-app-1'; then
    CONTAINER="pipeline-app-1"
  else
    echo "[ERROR] No running container found (pipeline-dagster-1 / pipeline-app-1)." >&2
    exit 1
  fi
fi

echo "[INFO] container=${CONTAINER}"
echo "[INFO] command=python3 ${SCRIPT_PATH} $*"

docker exec -i "${CONTAINER}" python3 "${SCRIPT_PATH}" "$@"

