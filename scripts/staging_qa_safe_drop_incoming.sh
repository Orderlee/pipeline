#!/usr/bin/env bash
# Backward-compatible wrapper for the canonical test QA helper.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
exec bash scripts/test_qa_safe_drop_incoming.sh "$@"
