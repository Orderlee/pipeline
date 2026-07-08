"""DVC setup artifacts — presence + shape (NOT executed). Pure file checks."""

from __future__ import annotations

import pathlib
import shutil
import subprocess

import pytest

_REPO = pathlib.Path(__file__).resolve().parents[2]
_DVC_DIR = _REPO / "scripts" / "dvc"


def _read(p: pathlib.Path) -> str:
    assert p.exists(), f"missing setup artifact: {p}"
    return p.read_text(encoding="utf-8")


def test_setup_bare_repo_script_shape():
    text = _read(_DVC_DIR / "setup_bare_repo.sh")
    assert text.startswith("#!/usr/bin/env bash") or text.startswith("#!/bin/bash")
    assert "set -euo pipefail" in text
    assert "git init --bare" in text
    # isolated from the deploy path (bare repo lives under /srv/data-repos, not the app repo):
    assert "/srv/data-repos/dvc-datasets.git" in text
    assert "post-receive" in text  # installs the hook


def test_post_receive_hook_ingests_pushed_rev():
    text = _read(_DVC_DIR / "post-receive")
    assert text.startswith("#!/usr/bin/env bash") or text.startswith("#!/bin/bash")
    # reads stdin (oldrev newrev refname) — that's how git feeds a post-receive hook:
    assert "while read" in text
    # 직접 ingest 경로(webhook 수신부 대신): dvc-ingest.env source + 변경 .dvc 만 ingest 구동.
    assert "dvc-ingest.env" in text
    assert "--only" in text  # 변경된 .dvc 만 인덱싱
    assert "DVC_INGEST_SCRIPT" in text  # ingest_to_catalog.py 호출


def test_dvc_config_template_is_minio_s3_remote():
    text = _read(_DVC_DIR / "dvc_config.template")
    assert "[core]" in text
    assert "s3://vlm-dataset/_dvc" in text, "remote must be the fixed vlm-dataset/_dvc prefix"
    assert "endpointurl" in text  # MinIO is S3-compatible behind an endpoint URL
    assert "MINIO_ENDPOINT" in text and "MINIO_ACCESS_KEY" in text


def test_dvc_pinned_in_curation_and_trainer_not_serving():
    curation = _read(_REPO / "docker" / "curation" / "requirements.txt")
    trainer = _read(_REPO / "docker" / "trainer" / "requirements.txt")
    assert "dvc[s3]" in curation, "curation image must pin dvc[s3]"
    assert "dvc[s3]" in trainer, "trainer image must pin dvc[s3]"
    # serving images must NOT carry dvc:
    for serving in ("embedding", "sam3"):
        req = _REPO / "docker" / serving / "requirements.txt"
        if req.exists():
            assert "dvc" not in req.read_text(encoding="utf-8"), f"{serving} must not depend on dvc"


def test_shell_artifacts_parse_with_bash_n():
    for name in ("setup_bare_repo.sh", "post-receive"):
        p = _DVC_DIR / name
        rc = subprocess.run(["bash", "-n", str(p)], capture_output=True, text=True)
        assert rc.returncode == 0, f"{name} bash -n failed: {rc.stderr}"


@pytest.mark.skipif(shutil.which("shellcheck") is None, reason="shellcheck not installed")
def test_shellcheck_clean():
    for name in ("setup_bare_repo.sh", "post-receive"):
        rc = subprocess.run(["shellcheck", "-S", "error", str(_DVC_DIR / name)], capture_output=True, text=True)
        assert rc.returncode == 0, f"shellcheck {name}: {rc.stdout}{rc.stderr}"
