#!/usr/bin/env bash
set -euo pipefail

# Manifest bootstrap shim (legacy scanner replacement)
#
# Usage:
#   ./scripts/run_scanner.sh
#   ./scripts/run_scanner.sh --host-input-dir incoming
#   ./scripts/run_scanner.sh --container-prefix /nas/incoming

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BOOTSTRAP_SCRIPT="${REPO_ROOT}/scripts/bootstrap_manifest.sh"

HOST_INPUT_DIR="${DATAOPS_INCOMING_HOST_DIR:-incoming}"
CONTAINER_PREFIX="${DATAOPS_INCOMING_CONTAINER_PREFIX:-/nas/incoming}"

print_usage() {
  cat <<USAGE
Usage: ./scripts/run_scanner.sh [options]

Options:
  --host-input-dir <path>    Host incoming directory (default: ${HOST_INPUT_DIR})
  --container-prefix <path>  Container incoming prefix (default: ${CONTAINER_PREFIX})
  -h, --help                 Show help

Notes:
  - This script no longer runs the legacy scanner module.
  - It creates a manifest JSON under <incoming>/.manifests/pending/ so Dagster sensor can pick it up.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host-input-dir)
      HOST_INPUT_DIR="${2:-}"
      shift 2
      ;;
    --container-prefix)
      CONTAINER_PREFIX="${2:-}"
      shift 2
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "[WARN] Ignoring legacy scanner option: $1"
      shift
      ;;
  esac
done

if [[ ! -x "${BOOTSTRAP_SCRIPT}" ]]; then
  echo "[ERROR] bootstrap script not executable: ${BOOTSTRAP_SCRIPT}"
  exit 1
fi

if [[ ! -d "${HOST_INPUT_DIR}" ]]; then
  echo "[ERROR] host input dir not found: ${HOST_INPUT_DIR}"
  exit 1
fi

echo "=============================================="
echo "Manifest Bootstrap Shim"
echo "=============================================="
echo "Host incoming dir : ${HOST_INPUT_DIR}"
echo "Container prefix  : ${CONTAINER_PREFIX}"
echo "=============================================="

"${BOOTSTRAP_SCRIPT}" "${HOST_INPUT_DIR}" --container-prefix "${CONTAINER_PREFIX}"

echo "[OK] manifest bootstrap completed"
