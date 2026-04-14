#!/usr/bin/env bash
# ============================================================
# cleanup_test_env.sh
# Test 데이터 plane 초기화 스크립트
# ============================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker/docker-compose.yaml"
ENV_FILE="$REPO_ROOT/docker/.env.test"
PROJECT_NAME="${TEST_COMPOSE_PROJECT_NAME:-pipeline-test}"
TEST_ROOT="${TEST_ROOT:-${STAGING_ROOT:-/home/pia/mou/staging}}"
TEST_DAGSTER_SERVICE="${TEST_DAGSTER_SERVICE:-dagster-code-server}"
TEST_DUCKDB_PATH="${TEST_DUCKDB_PATH:-/data/staging.duckdb}"
TEST_MINIO_ENDPOINT="${TEST_MINIO_ENDPOINT:-http://172.168.47.36:9002}"

info()  { echo -e "${CYAN}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }

compose_python() {
  local code="$1"
  docker compose \
    --env-file "$ENV_FILE" \
    -p "$PROJECT_NAME" \
    -f "$COMPOSE_FILE" \
    exec -T "$TEST_DAGSTER_SERVICE" \
    python3 -c "$code"
}

echo -e "${CYAN}============================================${NC}"
echo -e "${CYAN}  Test 데이터 plane 초기화${NC}"
echo -e "${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo -e "${CYAN}============================================${NC}"
echo ""

info "1/4 DuckDB 테스트 row 정리..."
CLEANUP_SCRIPT=$(cat <<PYEOF
import duckdb, sys
db_path = ${TEST_DUCKDB_PATH@Q}
tables = [
    "image_labels",
    "image_metadata",
    "processed_clips",
    "labels",
    "video_metadata",
    "dispatch_pipeline_runs",
    "dispatch_requests",
    "staging_pipeline_runs",
    "staging_dispatch_requests",
    "raw_files",
]
try:
    conn = duckdb.connect(db_path)
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            conn.execute(f"DELETE FROM {table}")
            print(f"  {table}: {count} rows 삭제")
        except Exception as exc:
            print(f"  {table}: 스킵 ({exc})")
    conn.close()
    print("DB 정리 완료")
except Exception as exc:
    print(f"DB 연결 실패: {exc}", file=sys.stderr)
    sys.exit(1)
PYEOF
)

if compose_python "$CLEANUP_SCRIPT" 2>&1; then
  ok "DuckDB 정리 완료"
else
  warn "DuckDB 정리 중 일부 오류 발생 (계속 진행)"
fi
echo ""

info "2/4 MinIO 테스트 버킷 정리..."
MINIO_SCRIPT=$(cat <<PYEOF
import boto3
from botocore.config import Config as BotoConfig
try:
    client = boto3.client(
        "s3",
        endpoint_url=${TEST_MINIO_ENDPOINT@Q},
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    buckets = ["vlm-raw", "vlm-processed", "vlm-labels", "raw-data"]
    for bucket in buckets:
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            print(f"  {bucket}: 없음 또는 접근 불가 — 스킵")
            continue
        paginator = client.get_paginator("list_objects_v2")
        total = 0
        for page in paginator.paginate(Bucket=bucket):
            objects = page.get("Contents", [])
            if not objects:
                continue
            delete_keys = [{"Key": obj["Key"]} for obj in objects]
            client.delete_objects(Bucket=bucket, Delete={"Objects": delete_keys})
            total += len(delete_keys)
        print(f"  {bucket}: {total}개 객체 삭제")
    print("MinIO 정리 완료")
except Exception as exc:
    print(f"MinIO 정리 실패: {exc}")
PYEOF
)

if compose_python "$MINIO_SCRIPT" 2>&1; then
  ok "MinIO 정리 완료"
else
  warn "MinIO 정리 중 오류 발생 (계속 진행)"
fi
echo ""

info "3/4 파일시스템 정리..."
for subdir in archive archive_pending; do
  target="$TEST_ROOT/$subdir"
  if [[ -d "$target" ]]; then
    rm -rf "$target/"*
    ok "$subdir 디렉토리 비움"
  else
    warn "$subdir 디렉토리 없음"
  fi
done

if [[ -d "$TEST_ROOT/incoming" ]]; then
  find "$TEST_ROOT/incoming" -mindepth 1 -maxdepth 1 ! -name '.DS_Store' ! -name '.dispatch' ! -name '.manifests' -exec rm -rf {} +
  ok "incoming 디렉토리 비움 (.dispatch/.manifests 유지)"
else
  warn "incoming 디렉토리 없음"
fi
echo ""

info "4/4 dispatch & manifests 정리..."
for subdir in pending processed failed; do
  target="$TEST_ROOT/incoming/.dispatch/$subdir"
  if [[ -d "$target" ]]; then
    rm -rf "$target/"*
    ok ".dispatch/$subdir 비움"
  fi
done

manifest_dir="$TEST_ROOT/incoming/.manifests"
if [[ -d "$manifest_dir" ]]; then
  find "$manifest_dir" -mindepth 1 -exec rm -rf {} +
  ok ".manifests 비움"
fi

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  ✅ Test 데이터 plane 초기화 완료${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
info "현재 상태:"
echo "  archive:         $(find "$TEST_ROOT/archive" -type f 2>/dev/null | wc -l) files"
echo "  archive_pending: $(find "$TEST_ROOT/archive_pending" -type f 2>/dev/null | wc -l) files"
echo "  incoming:        $(find "$TEST_ROOT/incoming" -mindepth 1 -type f 2>/dev/null | wc -l) files"
