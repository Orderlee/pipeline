#!/usr/bin/env bash
# restore_pipeline_postgres_port.sh
# pipeline-postgres-1 (LS + nas_tree 보유, 수동 docker run 컨테이너)을 같은 데이터 볼륨/
# 네트워크/alias/env 로 재생성하되 호스트 포트 15434 를 복원한다.
# 2026-06-11 재생성 때 -p 15434:5432 누락 → KPI 샘플러(localhost:15434) 중단된 것 복구.
# ⚠️ LS(pipeline-labelstudio-1)가 이 PG 의존 → 짧은 순단. dagster prod(docker-postgres-1)와는 무관.
# ⚠️ 코디네이트 재가동 창에서만 실행. 데이터는 named volume(pipeline_postgres_data)이라 보존됨.
# Codex 리뷰 반영: 실패 시 자동 롤백 trap, health 타임아웃 하드페일, verify 실패 하드페일,
# 포트/네트워크 사전점검, create→connect→start(DNS 갭 제거), 127.0.0.1 바인드, stop -t 120.
set -euo pipefail

OLD=pipeline-postgres-1
BAK="${OLD}-noport-bak"
IMG="pgduckdb/pgduckdb:15-v1.1.1"
VOL="pipeline_postgres_data"

echo "[0/6] 사전 점검"
docker inspect "$OLD" >/dev/null || { echo "ERROR: $OLD 없음"; exit 1; }
if docker inspect "$BAK" >/dev/null 2>&1; then
  echo "ERROR: $BAK 이미 존재 — 이전 복구 잔재. 수동 정리 후 재실행."; exit 1
fi
echo "  현재 포트바인딩: $(docker inspect "$OLD" --format '{{json .HostConfig.PortBindings}}')  (비어있으면 정상)"
# 포트/네트워크가 준비됐는지 — 파괴적 단계 전에 (RISK-4)
if ss -H -ltn 'sport = :15434' 2>/dev/null | grep -q .; then
  echo "ERROR: 호스트 포트 15434 이미 사용중 — 리스너 정리 후 재실행 (socat 포워더 등)"
  ss -H -ltn 'sport = :15434' || true; exit 1
fi
docker network inspect pipeline-network >/dev/null || { echo "ERROR: pipeline-network 없음"; exit 1; }
docker network inspect pipeline_default >/dev/null || { echo "ERROR: pipeline_default 없음"; exit 1; }

echo "[1/6] env 캡처 (POSTGRES_*/PGDATA/LANG 만; 비밀번호 미출력)"
mapfile -t ENVS < <(docker inspect "$OLD" --format '{{range .Config.Env}}{{println .}}{{end}}' \
                    | grep -E '^(POSTGRES_|PGDATA=|LANG=)')
[ "${#ENVS[@]}" -ge 3 ] || { echo "ERROR: env 캡처 실패(${#ENVS[@]}개) — 중단"; exit 1; }
PGUSER=$(printf '%s\n' "${ENVS[@]}" | sed -n 's/^POSTGRES_USER=//p')
[ -n "$PGUSER" ] || { echo "ERROR: POSTGRES_USER 캡처 실패"; exit 1; }
ENVARGS=(); for e in "${ENVS[@]}"; do ENVARGS+=(-e "$e"); done
echo "  캡처된 env 키: $(printf '%s\n' "${ENVS[@]}" | cut -d= -f1 | tr '\n' ' ')"

# --- 여기부터 파괴적: 실패하면 자동 롤백 (BUG-1) ---
_ROLLBACK_NEEDED=0
_auto_rollback() {
  [ "$_ROLLBACK_NEEDED" -eq 1 ] || return 0
  echo "AUTO-ROLLBACK: $BAK → $OLD 복원 시도"
  docker rm -f "$OLD" >/dev/null 2>&1 || true
  docker rename "$BAK" "$OLD" >/dev/null 2>&1 || true
  docker start "$OLD" >/dev/null 2>&1 || true
  echo "ROLLBACK 완료 — 원래 컨테이너 복원됨 (포트 없는 상태로). 수동 점검 요망."
}
trap _auto_rollback EXIT

echo "[2/6] stop(-t 120) + rename old → $BAK (삭제 아님, 롤백 가능)"
docker stop -t 120 "$OLD"
docker rename "$OLD" "$BAK"
_ROLLBACK_NEEDED=1

echo "[3/6] create (-p 127.0.0.1:15434:5432 + 같은 볼륨/이미지/alias) → 두 네트워크 → start"
docker create --name "$OLD" --restart unless-stopped \
  -v "$VOL":/var/lib/postgresql/data \
  -p 127.0.0.1:15434:5432 \
  --network pipeline-network --network-alias pipeline-postgres --network-alias postgres \
  "${ENVARGS[@]}" \
  "$IMG" postgres
docker network connect --alias pipeline-postgres --alias postgres pipeline_default "$OLD"
docker start "$OLD"

echo "[4/6] healthy 대기 (최대 60s)"
_READY=0
for i in $(seq 1 30); do
  if docker exec "$OLD" pg_isready -U "$PGUSER" >/dev/null 2>&1; then
    echo "  pg_isready OK (iter $i)"; _READY=1; break
  fi; sleep 2
done
[ "$_READY" -eq 1 ] || { echo "ERROR: Postgres 60s 내 미기동"; exit 1; }

echo "[5/6] 검증 (포트 게시 + TCP 도달 + nas_tree 쿼리)"
docker port "$OLD" | grep -q '15434' || { echo "ERROR: 15434 미게시"; exit 1; }
timeout 3 bash -c '</dev/tcp/127.0.0.1/15434' 2>/dev/null || { echo "ERROR: localhost:15434 미도달"; exit 1; }
if ! VOUT=$(docker exec "$OLD" psql -U "$PGUSER" -d nas_tree -tAc \
    "SELECT 'nas_tree OK, kpi_sync rows='||count(*) FROM kpi_sync_status;" 2>&1); then
  printf '%s\n' "$VOUT" | grep -v collation || true
  echo "ERROR: nas_tree 검증 실패"; exit 1
fi
printf '%s\n' "$VOUT" | grep -v collation || true

echo "[6/6] 성공 — 롤백 trap 해제"
_ROLLBACK_NEEDED=0
trap - EXIT
echo
echo "완료. KPI 샘플러(cron 5분)가 localhost:15434 재연결 → 5분 내 Grafana 갱신."
echo "확신 서면 백업 제거: docker rm $BAK"
echo "수동 롤백(원하면): docker rm -f $OLD && docker rename $BAK $OLD && docker start $OLD"
