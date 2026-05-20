#!/usr/bin/env bash
# Production docker compose wrapper.
#
# Ensures `.env` is explicitly loaded so that bind-mount paths (INCOMING_HOST_PATH,
# ARCHIVE_HOST_PATH) and DAGSTER_PORT are correctly resolved at compose-parse time.
#
# Manual invocation of `docker compose ...` without --env-file may silently fall back
# to compose YAML defaults (legacy /home/user/mou/incoming, port 3030) — observed
# 2026-05-19 during QA E2E. Always use this wrapper for prod compose ops.
#
# Usage:
#   ./scripts/compose-prod.sh up -d
#   ./scripts/compose-prod.sh up -d --force-recreate dagster
#   ./scripts/compose-prod.sh build app
#   ./scripts/compose-prod.sh logs -f dagster
set -euo pipefail
COMPOSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../docker" && pwd)"
exec docker compose -p docker --env-file "${COMPOSE_DIR}/.env" --project-directory "${COMPOSE_DIR}" -f "${COMPOSE_DIR}/docker-compose.yaml" "$@"
