"""lib.dvc_pull — pure helpers for the API-pull wrapper (L1)."""
from __future__ import annotations


def build_dvc_get_argv(repo_path: str, out_path: str, git_rev: str, dest: str) -> list[str]:
    """`dvc get <repo> <out> --rev <rev> -o <dest>` — fetches a versioned out from MinIO."""
    return ["dvc", "get", repo_path, out_path, "--rev", git_rev, "-o", dest]


def verify_pulled_md5(expected_md5: str | None, computed_md5: str | None) -> bool:
    """True if nothing to verify (expected None) or the two md5s match."""
    if expected_md5 is None:
        return True
    return expected_md5 == computed_md5
