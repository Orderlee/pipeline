"""NAS/NFS reachability probe — sensor가 NAS flap 시 무한 대기 회피 (#3).

2026-05-27 인시던트: archive_dispatch_sensor / auto_bootstrap_manifest_sensor 가 NAS 10.0.0.51
NFS 데몬 크래시 중 `pending_dir.glob()` 단계에서 300s 까지 hang → gRPC DEADLINE_EXCEEDED.
glob 호출 전 stat() 을 짧은 timeout 으로 호출해 NAS 도달 불가 시 빠른 skip 한다.

env:
- NETWORK_MOUNT_PROBE_TIMEOUT_SEC (default 8)
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from vlm_pipeline.lib.env_utils import int_env

DEFAULT_PROBE_TIMEOUT_SEC = 8


def probe_path_reachable(path: str | os.PathLike, *, timeout_sec: int | None = None) -> tuple[bool, str]:
    """경로가 timeout 안에 stat 가능한지 확인.

    Returns:
        (reachable, reason)
        - (True, "ok") : 정상 응답
        - (False, "timeout:<n>s") : timeout 초과
        - (False, "oserror:<msg>") : OSError (ENOENT, EIO 등)
    """
    if timeout_sec is None:
        timeout_sec = int_env("NETWORK_MOUNT_PROBE_TIMEOUT_SEC", DEFAULT_PROBE_TIMEOUT_SEC, 1)
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(os.stat, str(path))
            future.result(timeout=timeout_sec)
        return True, "ok"
    except FuturesTimeoutError:
        return False, f"timeout:{timeout_sec}s"
    except OSError as exc:
        return False, f"oserror:{type(exc).__name__}:{exc}"
