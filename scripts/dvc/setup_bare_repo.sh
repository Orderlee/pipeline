#!/usr/bin/env bash
# DVC 데이터 전용 BARE git repo + post-receive 훅 설치 (운영자 1회 실행 — CI 자동실행 X).
# 앱 배포 경로(rsync --delete + git reset --hard)와 격리하기 위해 /srv/data-repos 아래에 둔다.
# 사용: sudo DVC_INGEST_WEBHOOK_URL=http://10.0.0.10:3030/... bash scripts/dvc/setup_bare_repo.sh
set -euo pipefail

REPO_DIR="${DVC_DATA_REPO_PATH:-/srv/data-repos/dvc-datasets.git}"
HOOK_SRC="$(cd "$(dirname "$0")" && pwd)/post-receive"

mkdir -p "$(dirname "$REPO_DIR")"
if [ ! -d "$REPO_DIR" ]; then
    git init --bare "$REPO_DIR"
    echo "[dvc-setup] created bare repo: $REPO_DIR"
else
    echo "[dvc-setup] bare repo already exists: $REPO_DIR"
fi

install -m 0755 "$HOOK_SRC" "$REPO_DIR/hooks/post-receive"
echo "[dvc-setup] installed post-receive hook -> $REPO_DIR/hooks/post-receive"
echo "[dvc-setup] DONE. Curators: git remote add origin $REPO_DIR  (then dvc add/push + git push)"
