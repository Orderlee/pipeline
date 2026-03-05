#!/bin/bash
#
# Unified GCS downloader wrapper.
# - Primary engine: download_from_gcs_rclone.py
# - Modes: date-folders | legacy-range
#
# Example:
#   ./download_from_gcs.sh --mode date-folders --download-dir /nas/incoming/gcp
#   ./download_from_gcs.sh --mode legacy-range --download
#   ./download_from_gcs.sh --mode date-folders --list-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_SCRIPT="$SCRIPT_DIR/download_from_gcs_rclone.py"

# Default envs (can be overridden externally)
: "${GCS_MODE:=legacy-range}"
: "${BUCKET_NAME:=khon-kaen-rtsp-bucket}"
: "${GCS_BUCKETS:=khon-kaen-rtsp-bucket adlib-hotel-202512}"
: "${RCLONE_REMOTE:=gcs}"
: "${DOWNLOAD_DIR:=/nas/datasets/projects/khon_kaen_data}"

print_header() {
    echo "=========================================="
    echo " Unified GCS 다운로드 스크립트"
    echo "=========================================="
    echo "mode(default): ${GCS_MODE}"
    echo "bucket       : ${BUCKET_NAME}"
    echo "buckets      : ${GCS_BUCKETS}"
    echo "remote       : ${RCLONE_REMOTE}"
    echo "download_dir : ${DOWNLOAD_DIR}"
    echo ""
}

check_python() {
    if ! command -v python3 &> /dev/null; then
        echo "[ERROR] python3가 설치되어 있지 않습니다."
        exit 1
    fi
}

has_mode_arg() {
    for arg in "$@"; do
        if [[ "$arg" == "--mode" || "$arg" == --mode=* ]]; then
            return 0
        fi
    done
    return 1
}

main() {
    print_header
    check_python

    export BUCKET_NAME
    export GCS_BUCKETS
    export RCLONE_REMOTE
    export DOWNLOAD_DIR

    if has_mode_arg "$@"; then
        python3 "$PY_SCRIPT" "$@"
    else
        python3 "$PY_SCRIPT" --mode "$GCS_MODE" "$@"
    fi
}

main "$@"
