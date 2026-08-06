"""docker/postgres/Dockerfile 이 pgvector 를 corruption-fix 이상 버전으로 고정하는지 검증.

이 Dockerfile 이 존재하는 이유 자체가 "버전 미고정 + git 미추적" 이었으므로,
핀이 풀리거나(=버전 없는 apt install) 취약 버전으로 내려가는 회귀를 여기서 막는다.

pgvector upstream CHANGELOG:
  0.8.3 — HNSW vacuum 중 인덱스 corruption 수정
  0.8.4 — "hnsw graph not repaired" + vacuum 중 insert 오류 수정
표준 lib(pathlib/re) 만 의존 — dagster import 없음.
"""

from __future__ import annotations

import pathlib
import re

DOCKERFILE = pathlib.Path("docker/postgres/Dockerfile")

# HNSW vacuum corruption 이 처음 수정된 버전. 이 아래로는 내려갈 수 없다.
MIN_PGVECTOR = (0, 8, 3)


def _pinned_version() -> str:
    text = DOCKERFILE.read_text(encoding="utf-8")
    match = re.search(r"^ARG\s+PGVECTOR_APT_VERSION=(\S+)", text, re.MULTILINE)
    assert match, "PGVECTOR_APT_VERSION ARG 가 없다 — 버전 핀이 사라졌다"
    return match.group(1)


def test_dockerfile_exists():
    assert DOCKERFILE.is_file(), "prod PG 이미지는 추적되는 빌드 정의가 있어야 한다"


def test_apt_install_uses_the_pinned_arg():
    """apt install 이 ARG 를 실제로 참조해야 핀이 의미를 갖는다."""
    text = DOCKERFILE.read_text(encoding="utf-8")
    assert "postgresql-15-pgvector=${PGVECTOR_APT_VERSION}" in text, (
        "apt install 이 버전 없이 호출되면 빌드 시점마다 다른 pgvector 가 들어간다 "
        "(staging 의 datapipeline-pg-pgvector 가 그렇게 만들어졌다)"
    )


def test_pinned_version_is_at_or_above_corruption_fix():
    pinned = _pinned_version()
    # apt 버전 문자열 예: 0.8.5-1.pgdg12+1 → 앞의 semver 만 비교.
    semver = tuple(int(p) for p in pinned.split("-", 1)[0].split("."))
    assert semver >= MIN_PGVECTOR, f"pgvector {pinned} 은 HNSW vacuum corruption 에 노출된다 (>= 0.8.3 필요)"
