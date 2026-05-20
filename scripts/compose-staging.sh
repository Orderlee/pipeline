#!/usr/bin/env bash
# Staging docker compose wrapper.
#
# Sets compose project name to `pipeline-test` (container prefix) AND loads
# `.env.test` so staging-specific paths (NAS_200tb/staging/), ports (:3031,
# :19002, :15432), and POSTGRES_IMAGE override are correctly applied.
#
# Without `-p pipeline-test`, the compose CLI defaults the project name to the
# host directory name `docker`, which then targets the PROD containers —
# observed 2026-05-19 during QA E2E. Always use this wrapper for staging.
#
# Usage:
#   ./scripts/compose-staging.sh up -d
#   ./scripts/compose-staging.sh build app
#   ./scripts/compose-staging.sh restart dagster
#
# Must be invoked from the staging clone:
#   /home/user/work_p/Datapipeline-Data-data_pipeline_test
set -euo pipefail
COMPOSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../docker" && pwd)"
# Force the in-YAML `env_file: ${PIPELINE_ENV_FILE:-.env}` to resolve to `.env.test`
# so dagster containers receive DATAOPS_POSTGRES_DSN (which lives only in .env.test).
# Without this, the YAML default `.env` was loaded — prod-shape env without staging DSN.
export PIPELINE_ENV_FILE=.env.test
exec docker compose -p pipeline-test --env-file "${COMPOSE_DIR}/.env.test" --project-directory "${COMPOSE_DIR}" -f "${COMPOSE_DIR}/docker-compose.yaml" "$@"
