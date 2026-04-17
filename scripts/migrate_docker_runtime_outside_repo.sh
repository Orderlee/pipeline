#!/usr/bin/env bash
#
# Docker runtime (dagster_home, data)을 git 작업트리 밖으로 이동시키고
# 컨테이너를 호스트 pia UID(1000:1000)로 전환하는 1회성 migration.
#
# 실행 전:
#   - production 트래픽이 없는 유지보수 시점인지 확인
#   - DuckDB에 쓰기 중인 job 없어야 함
#   - 이 스크립트는 `sudo`를 사용한다 (root 소유 파일 이동/삭제)
#
# 실행 후:
#   - docker/.env 의 DOCKER_USER / DAGSTER_HOME_HOST_PATH / DOCKER_DATA_HOST_PATH 주석 해제
#   - docker compose down && up -d 로 새 구성 적용
#
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

# ---- 기본값 (원하면 export로 덮어쓰기) ----
TARGET_ROOT="${TARGET_ROOT:-/home/pia/vlm-pipeline}"
ENV_PROFILE="${ENV_PROFILE:-prod}"   # prod | dev
PIA_UID="${PIA_UID:-1000}"
PIA_GID="${PIA_GID:-1000}"

case "$ENV_PROFILE" in
  prod)
    SRC_DAGSTER_HOME="$REPO_ROOT/docker/app/dagster_home"
    SRC_DATA="$REPO_ROOT/docker/data"
    DST_DAGSTER_HOME="$TARGET_ROOT/prod/dagster_home"
    DST_DATA="$TARGET_ROOT/prod/data"
    COMPOSE_FILE="docker/docker-compose.yaml"
    ;;
  dev)
    SRC_DAGSTER_HOME="$REPO_ROOT/docker/app/dagster_home_dev"
    SRC_DATA="$REPO_ROOT/docker/data_dev"
    DST_DAGSTER_HOME="$TARGET_ROOT/dev/dagster_home"
    DST_DATA="$TARGET_ROOT/dev/data"
    COMPOSE_FILE="docker/docker-compose.dev.yaml"
    ;;
  *)
    echo "ENV_PROFILE must be 'prod' or 'dev'" >&2
    exit 1
    ;;
esac

echo "== migration plan =="
echo "  profile           : $ENV_PROFILE"
echo "  compose           : $COMPOSE_FILE"
echo "  dagster_home  src : $SRC_DAGSTER_HOME"
echo "  dagster_home  dst : $DST_DAGSTER_HOME"
echo "  data          src : $SRC_DATA"
echo "  data          dst : $DST_DATA"
echo "  target UID:GID    : $PIA_UID:$PIA_GID"
read -r -p "proceed? (y/N): " ans
[[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "abort"; exit 0; }

echo "[1/5] stop containers"
docker compose -f "$COMPOSE_FILE" down

echo "[2/5] 기존 runtime 파일 소유권 복구 (pia:pia)"
sudo chown -R "$PIA_UID:$PIA_GID" "$SRC_DAGSTER_HOME" "$SRC_DATA" 2>/dev/null || true

echo "[3/5] 대상 경로 생성 + rsync"
mkdir -p "$DST_DAGSTER_HOME" "$DST_DATA"
rsync -a --delete "$SRC_DAGSTER_HOME/" "$DST_DAGSTER_HOME/"
rsync -a --delete "$SRC_DATA/" "$DST_DATA/"

echo "[4/5] 대상 경로 소유권 확정"
sudo chown -R "$PIA_UID:$PIA_GID" "$DST_DAGSTER_HOME" "$DST_DATA"

echo "[5/5] 완료. 다음 단계:"
cat <<EOF

  1) docker/.env 또는 docker/.env.test 에서 아래 주석 해제:
       DOCKER_USER=${PIA_UID}:${PIA_GID}
     + prod: DAGSTER_HOME_HOST_PATH=$DST_DAGSTER_HOME
             DOCKER_DATA_HOST_PATH=$DST_DATA
     + dev : DEV_DAGSTER_HOME_HOST_PATH=$DST_DAGSTER_HOME
             DEV_DATA_HOST_PATH=$DST_DATA

  2) docker compose -f $COMPOSE_FILE up -d

  3) 이전 경로 백업/정리 (동작 확인 후):
       sudo rm -rf "$SRC_DAGSTER_HOME" "$SRC_DATA"
       (places365 모델 캐시는 따로 백업했다가 새 경로로 이동)

EOF
