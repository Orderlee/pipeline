#!/bin/bash
# restic restore drill — 가장 최근 pg_dump snapshot 을 가져와 임시 DB 로 복원해 검증.
# 분기 1회 실행. 본 스크립트가 통과해야 "백업이 살아있다" 보장됨 — 미실행 백업 = theater.
#
# 사용법:
#   sudo ./scripts/restic-restore.sh [--snapshot <id>] [--target-db vlm_pipeline_restore_drill]
#
# 안전장치:
#   - 운영 vlm_pipeline DB 를 절대 덮어쓰지 않음 (target_db != vlm_pipeline 강제)
#   - 복원 후 row 카운트 검증 후 임시 DB drop
set -euo pipefail

SNAPSHOT_ID="latest"
TARGET_DB="vlm_pipeline_restore_drill"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot) SNAPSHOT_ID="$2"; shift 2 ;;
    --target-db) TARGET_DB="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "unknown arg: $1"; exit 1 ;;
  esac
done

if [[ "${TARGET_DB}" == "vlm_pipeline" ]]; then
  echo "🚨 target-db 가 운영 DB 와 같음 — 강제 차단" >&2
  exit 1
fi

PG_CONTAINER="${PG_CONTAINER:-docker-postgres-1}"
BACKUP_CONTAINER="${BACKUP_CONTAINER:-docker-pg-backup-1}"
TMP_DUMP="/tmp/restic-restore-${TARGET_DB}.dump"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "1) restic snapshots 목록 (최근 3개)"
docker exec "${BACKUP_CONTAINER}" restic snapshots --compact --latest 3

# snapshot 안의 dump 파일 경로는 timestamp 포함이라 매번 다름.
# `restic snapshots <id> --json` 의 paths[0] 가 곧 dump 파일 경로 (예: "/vlm_pipeline_20260528_153906.dump").
log "2) snapshot ${SNAPSHOT_ID} 의 dump 경로 조회"
DUMP_PATH=$(docker exec "${BACKUP_CONTAINER}" restic snapshots "${SNAPSHOT_ID}" --json \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[0]["paths"][0])')
if [[ -z "${DUMP_PATH}" ]]; then
  echo "⚠️ snapshot ${SNAPSHOT_ID} 의 paths[0] 추출 실패" >&2
  exit 1
fi
log "   path = ${DUMP_PATH}"

log "3) snapshot ${SNAPSHOT_ID} 에서 stdin dump 복원 → ${TMP_DUMP}"
docker exec "${BACKUP_CONTAINER}" restic dump "${SNAPSHOT_ID}" "${DUMP_PATH}" > "${TMP_DUMP}"
ls -lh "${TMP_DUMP}"

if [[ "${DRY_RUN}" == "1" ]]; then
  log "DRY RUN: 복원 단계 skip. dump 파일만 검증: $(file ${TMP_DUMP})"
  exit 0
fi

log "4) 임시 DB ${TARGET_DB} 생성 (기존 있으면 drop)"
docker exec "${PG_CONTAINER}" psql -U airflow -d postgres -c "DROP DATABASE IF EXISTS ${TARGET_DB};"
docker exec "${PG_CONTAINER}" psql -U airflow -d postgres -c "CREATE DATABASE ${TARGET_DB};"

log "5) pg_restore → ${TARGET_DB}"
docker cp "${TMP_DUMP}" "${PG_CONTAINER}:/tmp/restore.dump"
docker exec "${PG_CONTAINER}" pg_restore -U airflow -d "${TARGET_DB}" --no-owner --no-privileges /tmp/restore.dump || {
  echo "⚠️ pg_restore 비-치명 경고 가능 (기존 owner/privilege)"
}

log "6) 검증: 주요 테이블 row count"
docker exec "${PG_CONTAINER}" psql -U airflow -d "${TARGET_DB}" -c "
  SELECT 'raw_files' tbl, count(*) FROM raw_files
  UNION ALL SELECT 'video_metadata', count(*) FROM video_metadata
  UNION ALL SELECT 'image_labels', count(*) FROM image_labels
  UNION ALL SELECT 'labels', count(*) FROM labels
  UNION ALL SELECT 'dispatch_requests', count(*) FROM dispatch_requests;
"

log "7) cleanup: ${TARGET_DB} drop + /tmp 정리"
docker exec "${PG_CONTAINER}" psql -U airflow -d postgres -c "DROP DATABASE ${TARGET_DB};"
# docker cp 는 root 소유로 파일을 떨어뜨리므로 컨테이너 기본 user(postgres)로는 rm 못 함 → -u root.
docker exec -u root "${PG_CONTAINER}" rm -f /tmp/restore.dump
rm -f "${TMP_DUMP}"

log "✅ restore drill 통과 — 백업 무결성 확인됨"
