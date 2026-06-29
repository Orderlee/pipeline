#!/usr/bin/env bash
# clear_maintenance.sh — GPU 정비락 수동 해제 런북 (fail-safe escape hatch).
#
# 사용: scripts/clear_maintenance.sh [sam3|pe_core|all]   (기본 all)
# 동작: (1) PG gpu_maintenance_lock.active=FALSE (psql 있으면 best-effort)
#       (2) 서빙 /maintenance/exit + /warmup POST
#       (3) /maintenance/status 출력
# 센서(maintenance_guard_sensor)가 죽었거나 PG 불통일 때 운영자가 직접 실행.
set -euo pipefail

TARGET="${1:-all}"
SAM3_API_URL="${SAM3_API_URL:-http://10.0.0.10:8002}"
EMBEDDING_API_URL="${EMBEDDING_API_URL:-http://10.0.0.10:8000}"
DSN="${PIPELINE_DSN:-${DATAOPS_POSTGRES_DSN:-}}"

clear_pg() {
  local tgt="$1"
  if [[ -z "${DSN}" ]] || ! command -v psql >/dev/null 2>&1; then
    echo "[pg] skip (DSN 없음 또는 psql 미설치) target=${tgt}"
    return 0
  fi
  psql "${DSN}" -v ON_ERROR_STOP=on -c \
    "UPDATE gpu_maintenance_lock SET active=FALSE, owner_run_id=NULL, updated_at=now() WHERE target='${tgt}';" \
    && echo "[pg] cleared target=${tgt}" || echo "[pg] WARN clear 실패 target=${tgt}"
}

clear_serving() {
  local name="$1" base="$2"
  echo "[serving] ${name} (${base}) exit+warmup"
  curl -sf -X POST "${base%/}/maintenance/exit"  >/dev/null && echo "  exit ok"   || echo "  exit WARN"
  curl -sf -X POST "${base%/}/warmup"            >/dev/null && echo "  warmup ok" || echo "  warmup WARN"
  echo "  status: $(curl -sf "${base%/}/maintenance/status" || echo '<unreachable>')"
}

do_target() {
  case "$1" in
    sam3)    clear_pg sam3;    clear_serving sam3    "${SAM3_API_URL}" ;;
    pe_core) clear_pg pe_core; clear_serving pe_core "${EMBEDDING_API_URL}" ;;
    *) echo "unknown target: $1" >&2; exit 2 ;;
  esac
}

if [[ "${TARGET}" == "all" ]]; then
  do_target sam3
  do_target pe_core
else
  do_target "${TARGET}"
fi
echo "done."
