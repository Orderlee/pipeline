#!/bin/bash
# restic repo idempotent init → busybox crond foreground.
set -euo pipefail

: "${RESTIC_REPOSITORY:?RESTIC_REPOSITORY env required}"
: "${RESTIC_PASSWORD:?RESTIC_PASSWORD env required}"
: "${PGHOST:?PGHOST env required}"
: "${PGPASSWORD:?PGPASSWORD env required}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] entrypoint: $*"; }

# 처음 기동 시 repo 가 없으면 init (idempotent: cat config 로 존재 확인).
if ! restic cat config >/dev/null 2>&1; then
  log "initializing restic repository at ${RESTIC_REPOSITORY}"
  restic init
else
  log "restic repository already initialized at ${RESTIC_REPOSITORY}"
fi

log "starting busybox crond — schedule per /etc/crontabs/root"
log "next backup: $(crontab -l 2>/dev/null | grep -v '^$\|^#' | head -1)"
# busybox crond (-f foreground, -L stdout). tini 가 PID1 이라 setpgid 경고도 없음.
# -l 8 : 로그 레벨(INFO). 명시적으로 root crontab 사용.
exec /usr/sbin/crond -f -L /dev/stdout -l 8 -c /etc/crontabs
