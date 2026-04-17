#!/usr/bin/env bash
# ============================================================
# cleanup_staging_test.sh
# Staging 파이프라인 테스트 환경 초기화 스크립트
# - DB 테이블 비우기 (DuckDB)
# - MinIO 버킷 비우기
# - archive / archive_pending / incoming 파일 삭제
# ============================================================
set -euo pipefail

# ── 색상 ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

COMPOSE_FILE="/home/pia/work_p/Datapipeline-Data-data_pipeline/docker/docker-compose.yaml"
STAGING_ROOT="/home/pia/mou/staging"

info()  { echo -e "${CYAN}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail()  { echo -e "${RED}[FAIL]${NC} $*"; }

echo -e "${CYAN}============================================${NC}"
echo -e "${CYAN}  Staging 테스트 환경 초기화${NC}"
echo -e "${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo -e "${CYAN}============================================${NC}"
echo ""

# ── 1. DuckDB 테이블 비우기 ──
info "1/4 DuckDB 테이블 비우기..."
CLEANUP_SCRIPT=$(cat <<'PYEOF'
import duckdb, sys
db_path = "/data/staging.duckdb"
tables = [
    # FK 자식 테이블 먼저 삭제
    "image_labels",
    "image_metadata",
    "processed_clips",
    "labels",
    "video_metadata",
    "dispatch_pipeline_runs",
    "dispatch_requests",
    "raw_files",
]
try:
    conn = duckdb.connect(db_path)
    for t in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            conn.execute(f"DELETE FROM {t}")
            print(f"  {t}: {count} rows 삭제")
        except Exception as e:
            print(f"  {t}: 스킵 ({e})")
    conn.close()
    print("DB 정리 완료")
except Exception as e:
    print(f"DB 연결 실패: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF
)

if docker compose -f "$COMPOSE_FILE" --profile staging exec -T dagster-staging python3 -c "$CLEANUP_SCRIPT" 2>&1; then
    ok "DuckDB 테이블 비우기 완료"
else
    warn "DuckDB 정리 중 일부 오류 발생 (계속 진행)"
fi
echo ""

# ── 2. MinIO 버킷 비우기 ──
info "2/4 MinIO 버킷 비우기 (vlm-raw, vlm-processed, vlm-labels, raw-data)..."
MINIO_SCRIPT=$(cat <<'PYEOF'
import boto3
from botocore.config import Config as BotoConfig
try:
    client = boto3.client(
        "s3",
        endpoint_url="http://172.168.47.36:9002",
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
            if objects:
                delete_keys = [{"Key": obj["Key"]} for obj in objects]
                client.delete_objects(Bucket=bucket, Delete={"Objects": delete_keys})
                total += len(delete_keys)
        print(f"  {bucket}: {total}개 객체 삭제")
    print("MinIO 정리 완료")
except Exception as e:
    print(f"MinIO 정리 실패: {e}")
PYEOF
)

if docker compose -f "$COMPOSE_FILE" --profile staging exec -T dagster-staging python3 -c "$MINIO_SCRIPT" 2>&1; then
    ok "MinIO 버킷 비우기 완료"
else
    warn "MinIO 정리 중 오류 발생 (계속 진행)"
fi
echo ""

# ── 3. 파일시스템 정리 ──
info "3/4 파일시스템 정리..."

# archive
if [ -d "$STAGING_ROOT/archive" ]; then
    rm -rf "$STAGING_ROOT/archive/"*
    ok "archive 디렉토리 비움"
else
    warn "archive 디렉토리 없음"
fi

# archive_pending
if [ -d "$STAGING_ROOT/archive_pending" ]; then
    rm -rf "$STAGING_ROOT/archive_pending/"*
    ok "archive_pending 디렉토리 비움"
else
    warn "archive_pending 디렉토리 없음"
fi

# incoming (keep .DS_Store)
if [ -d "$STAGING_ROOT/incoming" ]; then
    find "$STAGING_ROOT/incoming" -mindepth 1 -maxdepth 1 ! -name '.DS_Store' -exec rm -rf {} +
    ok "incoming 디렉토리 비움 (.DS_Store 유지)"
else
    warn "incoming 디렉토리 없음"
fi
echo ""

# ── 4. dispatch / manifests 정리 ──
info "4/4 dispatch & manifests 정리..."

for subdir in pending processed failed; do
    target="$STAGING_ROOT/incoming/.dispatch/$subdir"
    if [ -d "$target" ]; then
        rm -rf "$target/"*
        ok ".dispatch/$subdir 비움"
    fi
done

manifest_dir="$STAGING_ROOT/incoming/.manifests"
if [ -d "$manifest_dir" ]; then
    rm -rf "$manifest_dir/"*
    ok ".manifests 비움"
fi

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  ✅ Staging 테스트 환경 초기화 완료!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""

# 상태 확인
info "현재 상태:"
echo "  archive:         $(find "$STAGING_ROOT/archive" -type f 2>/dev/null | wc -l) files"
echo "  archive_pending:  $(find "$STAGING_ROOT/archive_pending" -type f 2>/dev/null | wc -l) files"
echo "  incoming:         $(find "$STAGING_ROOT/incoming" -mindepth 1 -type f 2>/dev/null | wc -l) files"
