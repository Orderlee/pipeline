"""lib.dvc_catalog — pure .dvc YAML + git-log parsers (L1, no dagster/DB/MinIO)."""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.lib.dvc_catalog import (  # noqa: E402
    GIT_LOG_FORMAT,
    DvcOut,
    DvcPointer,
    GitCommitMeta,
    dvc_remote_url_for,
    parse_dvc_pointer,
    parse_git_log_format,
)

# A real `dvc add data/fire_v3` produces data/fire_v3.dvc with this shape:
_DVC_YAML = """\
outs:
- md5: a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6.dir
  size: 104857600
  nfiles: 1280
  hash: md5
  path: fire_v3
"""

_GIT_STDOUT = (
    "9f3c2a1b7e4d5c6a8b0f1e2d3c4b5a6978d0e1f2\n"
    "curate: fire/smoke v3 — drop blurry night frames\n"
    "Added 320 SourceA night clips, removed 44 mislabeled.\n"
    "Class balance: fire=540 smoke=410.\n"
    "Seohee Jin\n"
    "shjin@example.com\n"
    "2026-06-29T11:18:04+09:00\n"
)


def test_git_log_format_constant_is_six_fields() -> None:
    assert GIT_LOG_FORMAT == "%H%n%s%n%b%n%an%n%ae%n%cI"


def test_parse_dvc_pointer_single_out() -> None:
    ptr = parse_dvc_pointer("data/fire_v3.dvc", _DVC_YAML)
    assert isinstance(ptr, DvcPointer)
    assert ptr.dvc_file_path == "data/fire_v3.dvc"
    assert len(ptr.outs) == 1
    out = ptr.outs[0]
    assert isinstance(out, DvcOut)
    assert out.path == "fire_v3"
    assert out.md5 == "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6.dir"
    assert out.size == 104857600
    assert out.nfiles == 1280


def test_parse_dvc_pointer_missing_optional_fields() -> None:
    ptr = parse_dvc_pointer("x.dvc", "outs:\n- path: lone\n  md5: deadbeef\n")
    out = ptr.outs[0]
    assert out.path == "lone"
    assert out.md5 == "deadbeef"
    assert out.size is None
    assert out.nfiles is None


def test_parse_dvc_pointer_rejects_no_outs() -> None:
    try:
        parse_dvc_pointer("bad.dvc", "meta: {}\n")
    except ValueError as exc:
        assert "outs" in str(exc)
    else:
        raise AssertionError("expected ValueError on .dvc with no outs")


def test_parse_git_log_format_multiline_body() -> None:
    meta = parse_git_log_format(_GIT_STDOUT)
    assert isinstance(meta, GitCommitMeta)
    assert meta.git_rev == "9f3c2a1b7e4d5c6a8b0f1e2d3c4b5a6978d0e1f2"
    assert meta.commit_subject == "curate: fire/smoke v3 — drop blurry night frames"
    # body = everything between subject and the trailing 3 fields (may be multi-line):
    assert "Added 320 SourceA night clips" in meta.commit_message
    assert "Class balance: fire=540 smoke=410." in meta.commit_message
    assert meta.commit_author_name == "Seohee Jin"
    assert meta.commit_author_email == "shjin@example.com"
    assert meta.committed_at == "2026-06-29T11:18:04+09:00"


def test_parse_git_log_format_empty_body() -> None:
    stdout = "abc123\nsubject only\n\nName\nn@e.x\n2026-01-01T00:00:00Z\n"
    meta = parse_git_log_format(stdout)
    assert meta.commit_subject == "subject only"
    assert meta.commit_message == ""
    assert meta.committed_at == "2026-01-01T00:00:00Z"


def test_dvc_remote_url_for() -> None:
    assert dvc_remote_url_for("fire_v3") == "s3://vlm-dataset/_dvc/fire_v3"
    assert dvc_remote_url_for("a/b") == "s3://vlm-dataset/_dvc/a/b"
