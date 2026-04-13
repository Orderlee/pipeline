#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yaml"
CTN="pipeline-dagster-1"

db_scalar() {
  local sql="$1"
  docker exec "$CTN" duckdb -readonly -csv /data/pipeline.duckdb "$sql" | tr -d '\r' | tail -n 1
}

echo "[1/6] docker compose services"
docker compose -f "$COMPOSE_FILE" ps

echo "[2/6] minio health"
curl -fsS http://localhost:9000/minio/health/live >/dev/null
echo "  - minio: OK"

echo "[3/6] dagster container checks"
docker exec "$CTN" python3 -c "import duckdb,imagehash,PIL,korean_romanizer,pydantic_settings; print('python deps: OK')"
docker exec "$CTN" ffprobe -version >/dev/null
echo "  - ffprobe: OK"

echo "[4/6] duckdb schema checks (inside container)"
docker exec "$CTN" duckdb -readonly /data/pipeline.duckdb ".tables"

required_tables=(raw_files image_metadata video_metadata labels processed_clips datasets dataset_clips)
for t in "${required_tables[@]}"; do
  exists="$(db_scalar "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='$t';")"
  if [[ "${exists:-0}" -lt 1 ]]; then
    echo "  - missing required table: $t" >&2
    exit 1
  fi
done

clip_exists="$(db_scalar "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='clip_metadata';")"
if [[ "${clip_exists:-0}" -ne 0 ]]; then
  echo "  - forbidden table still exists: clip_metadata" >&2
  exit 1
fi

bak_required=(_bak_raw_files _bak_processed_clips _bak_labels)
for t in "${bak_required[@]}"; do
  exists="$(db_scalar "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='$t';")"
  if [[ "${exists:-0}" -lt 1 ]]; then
    echo "  - missing backup table: $t" >&2
    exit 1
  fi
done

docker exec "$CTN" duckdb -readonly /data/pipeline.duckdb "SELECT COUNT(*) AS raw_files_count FROM raw_files;"
docker exec "$CTN" duckdb -readonly /data/pipeline.duckdb "SELECT COUNT(*) AS image_meta_count FROM image_metadata;"
docker exec "$CTN" duckdb -readonly /data/pipeline.duckdb "SELECT COUNT(*) AS video_meta_count FROM video_metadata;"

image_source_col="$(db_scalar "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='main' AND table_name='image_metadata' AND column_name='source_asset_id';")"
if [[ "${image_source_col:-0}" -lt 1 ]]; then
  echo "  - image_metadata.source_asset_id missing" >&2
  exit 1
fi

image_source_clip_col="$(db_scalar "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='main' AND table_name='image_metadata' AND column_name='source_clip_id';")"
if [[ "${image_source_clip_col:-0}" -lt 1 ]]; then
  echo "  - image_metadata.source_clip_id missing" >&2
  exit 1
fi

video_frame_status_col="$(db_scalar "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='main' AND table_name='video_metadata' AND column_name='frame_extract_status';")"
if [[ "${video_frame_status_col:-0}" -lt 1 ]]; then
  echo "  - video_metadata.frame_extract_status missing" >&2
  exit 1
fi

echo "[5/6] nas bind check (container path)"
docker exec "$CTN" sh -lc 'timeout 5 ls -ld /nas/datasets/projects >/dev/null'
docker exec "$CTN" sh -lc 'timeout 5 ls -ld /nas/incoming >/dev/null'
echo "  - /nas/datasets/projects: OK"
echo "  - /nas/incoming: OK"

echo "[6/6] done"
echo "MVP verify completed"
