#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <host_input_dir> [--container-prefix /nas/incoming]"
  exit 1
fi

HOST_INPUT_DIR=""
CONTAINER_PREFIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container-prefix)
      CONTAINER_PREFIX="${2:-}"
      shift 2
      ;;
    *)
      if [[ -z "$HOST_INPUT_DIR" ]]; then
        HOST_INPUT_DIR="$1"
      fi
      shift
      ;;
  esac
done

if [[ -z "$HOST_INPUT_DIR" ]]; then
  echo "host input dir is required"
  exit 1
fi

if [[ ! -d "$HOST_INPUT_DIR" ]]; then
  echo "input dir not found: $HOST_INPUT_DIR"
  exit 1
fi

MANIFEST_ROOT="$HOST_INPUT_DIR/.manifests"
PENDING_DIR="$MANIFEST_ROOT/pending"
mkdir -p "$PENDING_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
MANIFEST_FILE="$PENDING_DIR/bootstrap_migration_${TS}.json"

HOST_INPUT_DIR_ABS="$(cd "$HOST_INPUT_DIR" && pwd)"

to_container_path() {
  local src="$1"
  if [[ -n "$CONTAINER_PREFIX" ]]; then
    local rel="${src#$HOST_INPUT_DIR_ABS}"
    if [[ "$rel" == "$src" ]]; then
      rel=""
    fi
    echo "${CONTAINER_PREFIX%/}${rel}"
    return
  fi

  # fallback: keep source path
  echo "$src"
}

TMP_FILES="$(mktemp)"
find "$HOST_INPUT_DIR_ABS" -type f \
  ! -path "*/.manifests/*" \
  -print | sort > "$TMP_FILES"

FILE_COUNT="$(wc -l < "$TMP_FILES" | tr -d ' ')"

{
  echo "{"
  echo "  \"manifest_id\": \"bootstrap_migration_${TS}\"," 
  echo "  \"generated_at\": \"$(date -Iseconds)\"," 
  echo "  \"source_host_path\": \"$HOST_INPUT_DIR_ABS\"," 
  echo "  \"container_prefix\": \"$CONTAINER_PREFIX\"," 
  echo "  \"file_count\": $FILE_COUNT," 
  echo "  \"files\": ["

  idx=0
  while IFS= read -r file; do
    idx=$((idx + 1))
    cpath="$(to_container_path "$file")"
    size="$(stat -c %s "$file")"
    comma=","
    if [[ "$idx" -eq "$FILE_COUNT" ]]; then
      comma=""
    fi
    printf '    {"path":"%s","size":%s}%s\n' "$cpath" "$size" "$comma"
  done < "$TMP_FILES"

  echo "  ]"
  echo "}"
} > "$MANIFEST_FILE"

rm -f "$TMP_FILES"

echo "Manifest created: $MANIFEST_FILE"
