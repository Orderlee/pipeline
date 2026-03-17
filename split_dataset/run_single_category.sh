#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# Single category runner
#
# What it does:
# - Read JSONs from <CATEGORY_PREFIX>/<BBOXES_SUBDIR>
# - Scan images under <CATEGORY_PREFIX>
# - Split train/test (default 9:1)
# - Write to <DEST_PREFIX_BASE>/<DATASET_NAME>/
#
# Usage:
#   ./split_dataset/run_single_category.sh <category_prefix> [dataset_name]
#
# Example:
#   ./split_dataset/run_single_category.sh \
#     vietnam_data/processed_data/bat_as_weapon/20250812_AM_IS_CD_BW_1 weapon
#
# Dry run:
#   DRY_RUN=1 ./split_dataset/run_single_category.sh <category_prefix> [dataset_name]
# ------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CATEGORY_PREFIX="${1:-${CATEGORY_PREFIX:-}}"
if [[ -z "${CATEGORY_PREFIX}" ]]; then
  echo "Usage: $0 <category_prefix> [dataset_name]"
  exit 1
fi

DATASET_NAME="${2:-${DATASET_NAME:-$(basename "${CATEGORY_PREFIX}")}}"

# Defaults (override via env var)
CONFIG_PATH="${CONFIG_PATH:-${REPO_ROOT}/configs/global.yaml}"
SOURCE_BUCKET="${SOURCE_BUCKET:-image-data}"
BBOXES_SUBDIR="${BBOXES_SUBDIR:-bboxes}"
ASSETS_DIR_PREFIX="${ASSETS_DIR_PREFIX:-}"
DEST_BUCKET="${DEST_BUCKET:-data-pipeline}"
DEST_PREFIX_BASE="${DEST_PREFIX_BASE:-yolo_dataset2}"
TRAIN_RATIO="${TRAIN_RATIO:-0.9}"
SEED="${SEED:-42}"
BUCKET_PATH="${BUCKET_PATH:-s3://image-data}"

JSON_PREFIX="${CATEGORY_PREFIX%/}/${BBOXES_SUBDIR}"
DEST_PREFIX="${DEST_PREFIX_BASE%/}/${DATASET_NAME}"

DRY_FLAG=""
if [[ "${DRY_RUN:-0}" != "0" ]]; then
  DRY_FLAG="--dry-run"
fi

echo "[split_dataset] CONFIG_PATH=${CONFIG_PATH}"
echo "[split_dataset] SOURCE_BUCKET=${SOURCE_BUCKET}"
echo "[split_dataset] CATEGORY_PREFIX=${CATEGORY_PREFIX}"
echo "[split_dataset] JSON_PREFIX=${JSON_PREFIX}"
echo "[split_dataset] BBOXES_SUBDIR=${BBOXES_SUBDIR}"
echo "[split_dataset] ASSETS_DIR_PREFIX=${ASSETS_DIR_PREFIX}"
echo "[split_dataset] DEST_BUCKET=${DEST_BUCKET}"
echo "[split_dataset] DEST_PREFIX=${DEST_PREFIX}"
echo "[split_dataset] DATASET_NAME=${DATASET_NAME}"
echo "[split_dataset] TRAIN_RATIO=${TRAIN_RATIO}"
echo "[split_dataset] SEED=${SEED}"
echo "[split_dataset] BUCKET_PATH=${BUCKET_PATH}"
if [[ -n "${DRY_FLAG}" ]]; then
  echo "[split_dataset] DRY_RUN=1"
fi

python "${REPO_ROOT}/split_dataset/split_dataset.py" \
  --config "${CONFIG_PATH}" \
  --source-bucket "${SOURCE_BUCKET}" \
  --json-prefix "${JSON_PREFIX}" \
  --images-prefix "${CATEGORY_PREFIX}" \
  --assets-dir-prefix "${ASSETS_DIR_PREFIX}" \
  --dest-bucket "${DEST_BUCKET}" \
  --dest-prefix "${DEST_PREFIX}" \
  --dataset-name "${DATASET_NAME}" \
  --train-ratio "${TRAIN_RATIO}" \
  --seed "${SEED}" \
  --bucket-path "${BUCKET_PATH}" \
  ${DRY_FLAG}
