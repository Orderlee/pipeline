#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# Batch split (category merge) runner
#
# What it does:
# - Iterate categories under BATCH_ROOT_PREFIX (e.g. vietnam_data/processed_data/<category>/...)
# - Collect all JSONs under each category's */bboxes
# - Split train/test (default 9:1)
# - Merge categories ending with MERGE_SUFFIX into one dataset folder (MERGE_NAME)
#
# Quick run (use defaults):
#   ./split_dataset/run_batch_dataset.sh
#
# Dry run:
#   DRY_RUN=1 ./split_dataset/run_batch_dataset.sh
#
# Override example:
#   BATCH_ROOT_PREFIX=vietnam_data/processed_data \
#   DEST_PREFIX=yolo_dataset2 \
#   MERGE_SUFFIX=_as_weapon \
#   MERGE_NAME=weapon \
#   ./split_dataset/run_batch_dataset.sh
# ------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults (override via env var)
CONFIG_PATH="${CONFIG_PATH:-${REPO_ROOT}/configs/global.yaml}"
SOURCE_BUCKET="${SOURCE_BUCKET:-image-data}"
BATCH_ROOT_PREFIX="${BATCH_ROOT_PREFIX:-vietnam_data/processed_data}"
BBOXES_SUBDIR="${BBOXES_SUBDIR:-bboxes}"
ASSETS_DIR_PREFIX="${ASSETS_DIR_PREFIX:-}"
DEST_BUCKET="${DEST_BUCKET:-data-pipeline}"
DEST_PREFIX="${DEST_PREFIX:-yolo_dataset2}"
MERGE_SUFFIX="${MERGE_SUFFIX:-_as_weapon}"
MERGE_NAME="${MERGE_NAME:-weapon}"
TRAIN_RATIO="${TRAIN_RATIO:-0.9}"
SEED="${SEED:-42}"
BUCKET_PATH="${BUCKET_PATH:-s3://image-data}"

DRY_FLAG=""
if [[ "${DRY_RUN:-0}" != "0" ]]; then
  DRY_FLAG="--dry-run"
fi

echo "[split_dataset] CONFIG_PATH=${CONFIG_PATH}"
echo "[split_dataset] SOURCE_BUCKET=${SOURCE_BUCKET}"
echo "[split_dataset] BATCH_ROOT_PREFIX=${BATCH_ROOT_PREFIX}"
echo "[split_dataset] BBOXES_SUBDIR=${BBOXES_SUBDIR}"
echo "[split_dataset] ASSETS_DIR_PREFIX=${ASSETS_DIR_PREFIX}"
echo "[split_dataset] DEST_BUCKET=${DEST_BUCKET}"
echo "[split_dataset] DEST_PREFIX=${DEST_PREFIX}"
echo "[split_dataset] MERGE_SUFFIX=${MERGE_SUFFIX}"
echo "[split_dataset] MERGE_NAME=${MERGE_NAME}"
echo "[split_dataset] TRAIN_RATIO=${TRAIN_RATIO}"
echo "[split_dataset] SEED=${SEED}"
echo "[split_dataset] BUCKET_PATH=${BUCKET_PATH}"
if [[ -n "${DRY_FLAG}" ]]; then
  echo "[split_dataset] DRY_RUN=1"
fi

python "${REPO_ROOT}/split_dataset/split_dataset.py" \
  --config "${CONFIG_PATH}" \
  --source-bucket "${SOURCE_BUCKET}" \
  --batch-root-prefix "${BATCH_ROOT_PREFIX}" \
  --bboxes-subdir "${BBOXES_SUBDIR}" \
  --assets-dir-prefix "${ASSETS_DIR_PREFIX}" \
  --dest-bucket "${DEST_BUCKET}" \
  --dest-prefix "${DEST_PREFIX}" \
  --merge-category-suffix "${MERGE_SUFFIX}" \
  --merge-category-name "${MERGE_NAME}" \
  --train-ratio "${TRAIN_RATIO}" \
  --seed "${SEED}" \
  --bucket-path "${BUCKET_PATH}" \
  ${DRY_FLAG}
