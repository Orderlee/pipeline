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
# 호스트에서 실행하는 운영자 스크립트라 기본값은 localhost + **호스트** 포트.
# (컨테이너 포트와 다름: embedding-service 는 컨테이너 8003 → 호스트 8004)
# 이전 기본값은 2026-07-06 IP 개편 전 대역 + 잘못된 포트라 도달 불가였고,
# curl -sf 가 타임아웃을 WARN 으로 삼켜 "아무것도 안 했는데 성공처럼" 보였다.
SAM3_API_URL="${SAM3_API_URL:-http://localhost:8002}"
EMBEDDING_API_URL="${EMBEDDING_API_URL:-http://localhost:8004}"
CURL_MAX_TIME="${CURL_MAX_TIME:-10}"
DSN="${PIPELINE_DSN:-${DATAOPS_POSTGRES_DSN:-}}"
FAILURES=0

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
  if curl -sf --max-time "${CURL_MAX_TIME}" -X POST "${base%/}/maintenance/exit" >/dev/null; then
    echo "  exit ok"
  else
    echo "  exit FAIL — ${base} 도달 불가/거부. 정비락이 서버측에 남아있다."
    FAILURES=$((FAILURES + 1))
  fi
  if curl -sf --max-time "${CURL_MAX_TIME}" -X POST "${base%/}/warmup" >/dev/null; then
    echo "  warmup ok"
  else
    echo "  warmup FAIL — 모델 재로딩 안 됨 (첫 요청이 느리거나 503 일 수 있음)."
    FAILURES=$((FAILURES + 1))
  fi
  echo "  status: $(curl -sf --max-time "${CURL_MAX_TIME}" "${base%/}/maintenance/status" || echo '<unreachable>')"
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

# 서빙 호출이 하나라도 실패했으면 non-zero — "조용히 아무것도 안 함" 을 성공으로 오인하지 않도록.
if (( FAILURES > 0 )); then
  echo "done with ${FAILURES} serving failure(s) — 정비락이 실제로 해제되지 않았을 수 있다." >&2
  echo "SAM3_API_URL / EMBEDDING_API_URL 을 확인하고 재실행하라. 예:" >&2
  echo "  SAM3_API_URL=http://localhost:8002 EMBEDDING_API_URL=http://localhost:8004 $0 ${TARGET}" >&2
  exit 1
fi
echo "done."
