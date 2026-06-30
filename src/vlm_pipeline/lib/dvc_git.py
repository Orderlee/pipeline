"""lib.dvc_git — pure subprocess git wrappers for the DVC bare data repo (L1).

Only `git` is invoked here (read-only: log, ls-tree, show). The `dvc` binary and
MinIO bytes are NOT touched — those live in the L4 ingestion op / the pull wrapper.
No dagster / resources / ops import.
"""
from __future__ import annotations

import subprocess

from vlm_pipeline.lib.dvc_catalog import GIT_LOG_FORMAT, GitCommitMeta, parse_git_log_format


def _git(repo_path: str, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", repo_path, *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout


def git_log_meta(repo_path: str, rev: str) -> GitCommitMeta:
    """`git log -1 --format=GIT_LOG_FORMAT <rev>` → GitCommitMeta."""
    out = _git(repo_path, "log", "-1", f"--format={GIT_LOG_FORMAT}", rev)
    return parse_git_log_format(out)


def list_dvc_files_at_rev(repo_path: str, rev: str) -> list[str]:
    """All `*.dvc` pointer paths tracked at <rev>."""
    out = _git(repo_path, "ls-tree", "-r", "--name-only", rev)
    return sorted(line for line in out.splitlines() if line.endswith(".dvc"))


def read_file_at_rev(repo_path: str, rev: str, path: str) -> str:
    """`git show <rev>:<path>` — content of a tracked file at a rev."""
    return _git(repo_path, "show", f"{rev}:{path}")


def iter_recent_revs(repo_path: str, limit: int = 50) -> list[str]:
    """Most-recent <limit> commit revs (newest first) — reconciliation backstop."""
    out = _git(repo_path, "log", f"-{int(limit)}", "--format=%H")
    return [line.strip() for line in out.splitlines() if line.strip()]
