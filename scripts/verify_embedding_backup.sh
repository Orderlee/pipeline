#!/bin/bash
# verify_embedding_backup.sh — image_embeddings 백업 전/후 대조용 read-only 검증.
#
# 출력: row count + entity_type 분포 + HNSW 인덱스 존재 + vector 확장 버전.
# 멱등, read-only. 운영 DB 를 변경하지 않는다.
#
# 사용법:
#   # env DSN 사용 (권장)
#   PGDSN="postgresql://airflow:airflow@localhost:15433/vlm_pipeline" \
#     ./scripts/verify_embedding_backup.sh
#
#   # 개별 PG env 사용 (psql 표준)
#   PGHOST=localhost PGPORT=15433 PGUSER=airflow PGPASSWORD=airflow PGDATABASE=vlm_pipeline \
#     ./scripts/verify_embedding_backup.sh
#
#   # 컨테이너 내부에서 직접 실행 (compose network 내 hostname)
#   PGDSN="postgresql://airflow:airflow@postgres:5432/vlm_pipeline" \
#     ./scripts/verify_embedding_backup.sh
set -euo pipefail

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S %Z')

# PGDSN 이 설정되어 있으면 psql -d 로 전달, 없으면 PGHOST/PGPORT/... env 에 맡긴다.
if [[ -n "${PGDSN:-}" ]]; then
  PSQL_TARGET=("-d" "${PGDSN}")
else
  PSQL_TARGET=()
fi

run_sql() {
  psql "${PSQL_TARGET[@]}" -t -A -c "$1"
}

run_sql_pretty() {
  psql "${PSQL_TARGET[@]}" -c "$1"
}

echo "========================================"
echo "  image_embeddings backup verification"
echo "  ${TIMESTAMP}"
echo "========================================"

# 1) vector 확장 버전
echo ""
echo "--- vector extension ---"
EXT_VERSION=$(run_sql "SELECT extversion FROM pg_extension WHERE extname = 'vector';" 2>/dev/null || true)
if [[ -z "${EXT_VERSION}" ]]; then
  echo "WARN: vector extension NOT installed (pgvector not present in this PG image)"
  echo "      image_embeddings restore WILL fail without pgvector-enabled image"
else
  echo "pgvector version: ${EXT_VERSION}"
fi

# 2) 테이블 존재 여부
echo ""
echo "--- table existence ---"
TABLE_EXISTS=$(run_sql "SELECT 1 FROM information_schema.tables WHERE table_name = 'image_embeddings';" 2>/dev/null || true)
if [[ -z "${TABLE_EXISTS}" ]]; then
  echo "WARN: image_embeddings table does NOT exist"
  echo "  Migration 006/007 not applied, or pgvector restore failed."
  echo "========================================"
  echo "  RESULT: NOT VERIFIABLE (table missing)"
  echo "========================================"
  exit 1
fi
echo "image_embeddings: EXISTS"

# 3) 전체 row count
echo ""
echo "--- row counts ---"
TOTAL=$(run_sql "SELECT COUNT(*) FROM image_embeddings;")
echo "total rows: ${TOTAL}"

# 4) entity_type 분포
echo ""
echo "--- entity_type distribution ---"
run_sql_pretty "
  SELECT
    entity_type,
    COUNT(*) AS count,
    MIN(created_at) AS oldest,
    MAX(created_at) AS newest
  FROM image_embeddings
  GROUP BY entity_type
  ORDER BY entity_type;
"

# 5) model_name 분포
echo ""
echo "--- model_name distribution ---"
run_sql_pretty "
  SELECT model_name, entity_type, COUNT(*) AS count
  FROM image_embeddings
  GROUP BY model_name, entity_type
  ORDER BY model_name, entity_type;
"

# 6) HNSW 인덱스 존재 확인
echo ""
echo "--- HNSW index ---"
HNSW_EXISTS=$(run_sql "SELECT 1 FROM pg_indexes WHERE indexname = 'image_embeddings_hnsw';")
if [[ -z "${HNSW_EXISTS}" ]]; then
  echo "WARN: image_embeddings_hnsw index MISSING"
  echo "  Run: CREATE INDEX image_embeddings_hnsw ON image_embeddings USING hnsw (embedding vector_cosine_ops);"
else
  echo "image_embeddings_hnsw: EXISTS"
  HNSW_DEF=$(run_sql "SELECT indexdef FROM pg_indexes WHERE indexname = 'image_embeddings_hnsw';")
  echo "  ${HNSW_DEF}"
fi

# 7) 모든 인덱스 목록
echo ""
echo "--- all image_embeddings indexes ---"
run_sql_pretty "
  SELECT indexname, indexdef
  FROM pg_indexes
  WHERE tablename = 'image_embeddings'
  ORDER BY indexname;
"

# 8) 테이블/인덱스 크기
echo ""
echo "--- table and index sizes ---"
run_sql_pretty "
  SELECT
    relname AS name,
    relkind AS kind,
    pg_size_pretty(pg_relation_size(oid)) AS size
  FROM pg_class
  WHERE relname LIKE 'image_embeddings%'
  ORDER BY relname;
"

echo ""
echo "========================================"
echo "  RESULT: VERIFIED (rows=${TOTAL})"
echo "========================================"
