#!/usr/bin/env bash
# scripts/sync_from_prod.sh — pull source code from the prod host into this
# working tree, excluding everything that either holds real secrets or is
# runtime / build state. After running, invoke the sanitize_repo skill to
# scrub the remaining real values before you commit.
#
# Configuration (no real values committed — defaults are placeholders):
#   PROD_USER  (default: user)
#   PROD_HOST  (default: 10.0.0.10)
#   PROD_REPO  (default: /home/user/work_p/Datapipeline-Data-data_pipeline)
#
# Set them either as env vars OR put them in scripts/sync_from_prod.env
# (GITIGNORED — that file is the only place the real host/user live).
#
# Usage:
#   scripts/sync_from_prod.sh              # do the sync
#   scripts/sync_from_prod.sh --dry-run    # show what would change, write nothing
#   scripts/sync_from_prod.sh --help       # this help text
#
# Note: --delete is intentionally NOT passed to rsync. That protects local-only
# files (the gitignored rule map, local env files, this script's own .env) from
# being silently wiped when prod doesn't have them.

set -euo pipefail

usage() {
  sed -n '2,/^set -euo/p' "$0" | sed -n '1,/^set -euo/{/^set -euo/!p;}' | sed 's/^# \{0,1\}//'
}

DRY=""
for arg in "$@"; do
  case "$arg" in
    --dry-run|-n) DRY="--dry-run" ;;
    --help|-h)    usage; exit 0 ;;
    *) echo "unknown arg: $arg" >&2; usage >&2; exit 2 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCAL_ENV="$SCRIPT_DIR/sync_from_prod.env"

if [[ -f "$LOCAL_ENV" ]]; then
  # shellcheck disable=SC1090
  source "$LOCAL_ENV"
fi

PROD_USER="${PROD_USER:-user}"
PROD_HOST="${PROD_HOST:-10.0.0.10}"
PROD_REPO="${PROD_REPO:-/home/user/work_p/Datapipeline-Data-data_pipeline}"

echo "source : ${PROD_USER}@${PROD_HOST}:${PROD_REPO}/"
echo "dest   : ${REPO_ROOT}/"
[[ -n "$DRY" ]] && echo "(dry-run — nothing will be written)"
[[ -f "$LOCAL_ENV" ]] || echo "(no $LOCAL_ENV found — using placeholder defaults; create that file with real PROD_* values)"
echo

RSYNC_OPTS=(-av)
[[ -n "$DRY" ]] && RSYNC_OPTS+=("$DRY")

rsync "${RSYNC_OPTS[@]}" \
  --exclude='.git' \
  --exclude='.env' \
  --exclude='.env.*' \
  --exclude='*.env' \
  --exclude='*.env.*' \
  --exclude='docker/.env*' \
  --exclude='node_modules' \
  --exclude='__pycache__' \
  --exclude='docker/data/' \
  --exclude='docker/app/dagster_home/' \
  --exclude='credentials/' \
  --exclude='.github/workflows/' \
  --exclude='.pre-commit-config.yaml' \
  --exclude='/README.md' \
  --exclude='/.mcp.json' \
  --exclude='/site_reports' \
  --exclude='.bkit' \
  "${PROD_USER}@${PROD_HOST}:${PROD_REPO}/" \
  "${REPO_ROOT}/"

echo
if [[ -n "$DRY" ]]; then
  echo "✓ dry-run complete. Re-run without --dry-run to actually sync."
else
  echo "✓ sync complete. Next: invoke the sanitize_repo skill before committing"
  echo "   (e.g. tell the agent: 'sanitize_repo 돌려줘')."
fi
