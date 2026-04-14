#!/usr/bin/env bash
# Backward-compatible wrapper for `scripts/cleanup_test_env.sh`.
set -euo pipefail

exec bash "$(cd "$(dirname "$0")" && pwd)/cleanup_test_env.sh" "$@"
