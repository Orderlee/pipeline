"""Phase 3-D dataset lineage helpers — stack-candidates #11 1단계.

datasets row 에 build 시점의 spec_hash / git_sha / build_started_at 을 같이 기록해
"어떤 spec/코드 시점의 빌드였는지" 사후 추적 가능하게.

이 단계는 *기록만* — dataset_prefix (MinIO path) 는 그대로 유지. 후속 PR 에서
spec_hash 기반 path 로 옮길 때까지 consumer 호환 깨지지 않는다.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime
from typing import Any


def compute_spec_hash(config: dict[str, Any]) -> str:
    """결정적 hash — 같은 config dict ↔ 같은 hash.

    JSON 직렬화 + sort_keys=True 로 key 순서 영향 제거. 16자 truncated SHA256
    (full 64자 너무 김, prefix 충돌 확률 무시 가능: 16자 = 64bit hex = 1.8e19 공간).
    """
    payload = json.dumps(config, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def capture_git_sha(*, fallback: str | None = None) -> str | None:
    """현재 build 시점의 git commit SHA.

    우선순위:
      1) ``DATAOPS_DEPLOYED_GIT_SHA`` env (CI 의 deploy-stack.sh 가 set 한다고 가정 —
         미설정 시 다음 단계로 폴백)
      2) ``GITHUB_SHA`` (Actions runner)
      3) ``git rev-parse HEAD`` (workspace .git 가 있을 때)
      4) ``fallback`` (보통 None — DB 에 NULL 저장)

    컨테이너 안에선 .git 없을 가능성 ↑ 라 env 가 1순위. fail-soft.
    """
    for env_key in ("DATAOPS_DEPLOYED_GIT_SHA", "GITHUB_SHA"):
        v = os.getenv(env_key, "").strip()
        if v:
            return v[:40]  # full SHA 길이 cap

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2.0,
            check=False,
        )
        sha = result.stdout.strip()
        if result.returncode == 0 and sha:
            return sha[:40]
    except Exception:  # noqa: BLE001
        pass

    return fallback


def make_build_lineage(config: dict[str, Any]) -> dict[str, Any]:
    """build_dataset asset 등에서 호출 — insert_dataset 에 mix-in 할 dict 반환."""
    return {
        "spec_hash": compute_spec_hash(config),
        "git_sha": capture_git_sha(),
        "build_started_at": datetime.now(),
    }
