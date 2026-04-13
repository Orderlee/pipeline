#!/bin/bash
set -euo pipefail

# reset_test_docker.sh
# 컨테이너 내부에서 테스트 데이터 초기화

DB_PATH="${DB_PATH:-/data/pipeline.duckdb}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MANIFEST_PENDING_DIR="${MANIFEST_PENDING_DIR:-/nas/incoming/.manifests/pending}"
MANIFEST_PROCESSED_DIR="${MANIFEST_PROCESSED_DIR:-/nas/incoming/.manifests/processed}"

echo "=== 1. DuckDB 테이블 비우기 ==="
python3 - <<PY
import duckdb

db = duckdb.connect("${DB_PATH}")
tables = [
    "dataset_clips",
    "datasets",
    "processed_clips",
    "labels",
    "image_metadata",
    "video_metadata",
    "raw_files",
]
for table in tables:
    db.execute(f"DELETE FROM {table}")
    count = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {count}")
db.close()
PY

echo "=== 2. MinIO vlm-* 버킷 비우기 ==="
python3 - <<PY
import boto3
from botocore.exceptions import ClientError

endpoint = "${MINIO_ENDPOINT}"
if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
    endpoint = f"http://{endpoint}"

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id="${MINIO_ACCESS_KEY}",
    aws_secret_access_key="${MINIO_SECRET_KEY}",
)

for bucket in ["vlm-raw", "vlm-labels", "vlm-processed", "vlm-dataset"]:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "unknown")
        print(f"{bucket}: skipped ({code})")
        continue

    removed = 0
    paginator = s3.get_paginator("list_objects_v2")
    batch = []

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})
            if len(batch) >= 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
                removed += len(batch)
                batch = []

    if batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
        removed += len(batch)

    print(f"{bucket}: {removed} objects removed")
PY

echo "=== 3. manifest 초기화 ==="
if timeout 10 bash -c "mkdir -p \"${MANIFEST_PENDING_DIR}\" \"${MANIFEST_PROCESSED_DIR}\" && rm -f \"${MANIFEST_PENDING_DIR}\"/*.json \"${MANIFEST_PROCESSED_DIR}\"/*.json"; then
  echo "manifest: cleared"
else
  echo "manifest: skipped (path timeout or unavailable)"
fi

echo "=== 초기화 완료 ==="
