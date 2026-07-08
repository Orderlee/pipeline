"""lib.dvc_catalog — pure parsers for the DVC curation layer (L1).

.dvc YAML pointer + `git log -1 --format=...` output → dataclasses. No dagster /
resources / ops import (enforced by scripts/check_lib_layer_imports.py). Only stdlib
+ PyYAML (already a dep via dagster). Used by the ingestion op/sensor (defs/train, L4).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import yaml

# The git pretty-format used by ingestion (J4) — 6 fields, newline-separated.
# %H=full rev, %s=subject, %b=body, %an=author name, %ae=author email, %cI=committer ISO date.
GIT_LOG_FORMAT = "%H%n%s%n%b%n%an%n%ae%n%cI"

_DVC_BUCKET = "vlm-dataset"
_DVC_PREFIX = "_dvc"


@dataclass(frozen=True)
class DvcOut:
    path: str
    md5: str | None = None
    size: int | None = None
    nfiles: int | None = None


@dataclass(frozen=True)
class DvcPointer:
    dvc_file_path: str
    outs: list[DvcOut] = field(default_factory=list)


@dataclass(frozen=True)
class GitCommitMeta:
    git_rev: str
    commit_subject: str
    commit_message: str
    commit_author_name: str
    commit_author_email: str
    committed_at: str

    @property
    def git_short_rev(self) -> str:
        return self.git_rev[:12]


def parse_dvc_pointer(dvc_file_path: str, yaml_text: str) -> DvcPointer:
    """Parse a .dvc YAML pointer into a DvcPointer. Raises ValueError if no `outs`."""
    doc = yaml.safe_load(yaml_text) or {}
    raw_outs = doc.get("outs")
    if not raw_outs or not isinstance(raw_outs, list):
        raise ValueError(f"{dvc_file_path}: .dvc file has no 'outs' list")
    outs: list[DvcOut] = []
    for entry in raw_outs:
        if not isinstance(entry, dict):
            continue
        size = entry.get("size")
        nfiles = entry.get("nfiles")
        outs.append(
            DvcOut(
                path=str(entry.get("path", "")),
                md5=(str(entry["md5"]) if entry.get("md5") is not None else None),
                size=(int(size) if size is not None else None),
                nfiles=(int(nfiles) if nfiles is not None else None),
            )
        )
    if not outs:
        raise ValueError(f"{dvc_file_path}: .dvc 'outs' is empty")
    return DvcPointer(dvc_file_path=dvc_file_path, outs=outs)


def parse_git_log_format(stdout: str) -> GitCommitMeta:
    """Parse `git log -1 --format=GIT_LOG_FORMAT` stdout.

    Layout: line0=rev, line1=subject, lines[2:-3]=body (may be multi-line, may be empty),
    line[-3]=author name, line[-2]=author email, line[-1]=committed_at (ISO).
    """
    lines = stdout.rstrip("\n").split("\n")
    if len(lines) < 5:
        raise ValueError(f"git log output too short ({len(lines)} lines): {stdout!r}")
    git_rev = lines[0].strip()
    commit_subject = lines[1]
    committed_at = lines[-1].strip()
    commit_author_email = lines[-2].strip()
    commit_author_name = lines[-3].strip()
    body_lines = lines[2:-3]
    commit_message = "\n".join(body_lines).strip()
    return GitCommitMeta(
        git_rev=git_rev,
        commit_subject=commit_subject,
        commit_message=commit_message,
        commit_author_name=commit_author_name,
        commit_author_email=commit_author_email,
        committed_at=committed_at,
    )


def dvc_remote_url_for(out_path: str, *, bucket: str = _DVC_BUCKET, prefix: str = _DVC_PREFIX) -> str:
    """s3 URL of an out under the fixed vlm-dataset/_dvc/ prefix (5-bucket policy)."""
    clean = out_path.strip("/")
    return f"s3://{bucket}/{prefix}/{clean}"
