"""lib.dvc_pull — pure helpers for the API-pull wrapper (L1)."""

from __future__ import annotations

import hashlib
import json
import os

_CHUNK = 1024 * 1024


def _file_md5(path: str) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compute_dvc_md5(path: str) -> str | None:
    """`dvc get` 로 받은 경로의 md5 를 DVC 와 동일한 규칙으로 계산한다.

    - 파일: 내용의 md5 (hex)
    - 디렉토리: DVC 의 dir-hash — 각 파일의 ``{"md5", "relpath"}`` 를 relpath 로 정렬해
      ``json.dumps(..., sort_keys=True)`` 로 직렬화한 뒤 md5 를 내고 ``.dir`` 접미사를 붙인다.
      (`dataset_catalog.dvc_md5` 에 저장된 값이 이 형식이다.)

    경로가 없으면 None → 호출자의 `verify_pulled_md5` 가 mismatch 로 처리한다.

    기존 구현은 항상 None 을 돌려주는 스텁이라, 카탈로그에 `dvc_md5` 가 있는 정상 케이스에서
    실 pull 이 바이트가 맞아도 **항상 exit 3** 이었다 — "검증 없음" 보다 나쁜 "가짜 실패".
    """
    if os.path.isfile(path):
        return _file_md5(path)
    if not os.path.isdir(path):
        return None

    entries = []
    for root, _dirs, files in os.walk(path):
        for name in files:
            abs_path = os.path.join(root, name)
            rel = os.path.relpath(abs_path, path).replace(os.sep, "/")
            entries.append({"md5": _file_md5(abs_path), "relpath": rel})
    entries.sort(key=lambda e: e["relpath"])
    blob = json.dumps(entries, sort_keys=True).encode("utf-8")
    return hashlib.md5(blob).hexdigest() + ".dir"


def build_dvc_get_argv(repo_path: str, out_path: str, git_rev: str, dest: str) -> list[str]:
    """`dvc get <repo> <out> --rev <rev> -o <dest>` — fetches a versioned out from MinIO."""
    return ["dvc", "get", repo_path, out_path, "--rev", git_rev, "-o", dest]


def verify_pulled_md5(expected_md5: str | None, computed_md5: str | None) -> bool:
    """True if nothing to verify (expected None) or the two md5s match."""
    if expected_md5 is None:
        return True
    return expected_md5 == computed_md5
