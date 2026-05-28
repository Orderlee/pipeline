#!/bin/bash
# pg_dump → restic backup → forget+prune.
# env (compose 에서 주입):
#   PGHOST, PGUSER, PGPASSWORD, PGDATABASE  — Postgres 접속
#   RESTIC_REPOSITORY, RESTIC_PASSWORD       — restic repo
#   BACKUP_RETENTION_{DAILY,WEEKLY,MONTHLY}  — restic forget 정책
set -euo pipefail

TS=$(date '+%Y%m%d_%H%M%S')
DB_NAME="${PGDATABASE:-vlm_pipeline}"
HOST_TAG="${BACKUP_HOST_TAG:-docker-postgres-1}"
SNAPSHOT_NAME="${DB_NAME}_${TS}.dump"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] $*"; }

log "pg_dump ${DB_NAME} → restic backup (stdin=${SNAPSHOT_NAME})"

# pg_dump custom format (-F c) + max compression (-Z 9). 71MB DB → ~10MB 예상.
# stdin streaming 으로 디스크 임시파일 안 만들고 restic 으로 바로 전달.
pg_dump -h "${PGHOST}" -U "${PGUSER}" -d "${DB_NAME}" -F c -Z 9 \
  | restic backup --stdin --stdin-filename "${SNAPSHOT_NAME}" \
      --tag daily --tag "db:${DB_NAME}" --host "${HOST_TAG}"

log "restic forget --prune (daily=${BACKUP_RETENTION_DAILY:-7} weekly=${BACKUP_RETENTION_WEEKLY:-4} monthly=${BACKUP_RETENTION_MONTHLY:-3})"
restic forget --tag daily \
  --keep-daily "${BACKUP_RETENTION_DAILY:-7}" \
  --keep-weekly "${BACKUP_RETENTION_WEEKLY:-4}" \
  --keep-monthly "${BACKUP_RETENTION_MONTHLY:-3}" \
  --prune

log "backup done"
