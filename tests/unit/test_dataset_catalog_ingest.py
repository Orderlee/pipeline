"""dataset_catalog ingestion — real tmp git repo + moto MinIO + _DummyDB.

git ops run for real (subprocess on a tmp repo). The DVC byte transfer is NEVER run;
object presence is checked against moto. Missing object -> pending_missing_dvc_objects.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.defs.train import catalog_ingest as _ci  # noqa: E402
from vlm_pipeline.lib import dvc_git  # noqa: E402


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


class _NullLog:
    def info(self, *a, **k): ...
    def warning(self, *a, **k): ...


@pytest.fixture
def tmp_data_repo(tmp_path):
    repo = tmp_path / "dvc-datasets"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "shjin@example.com")
    _git(repo, "config", "user.name", "Seohee Jin")
    data = repo / "data"
    data.mkdir()
    (data / "fire_v3.dvc").write_text(
        "outs:\n- md5: deadbeef.dir\n  size: 1024\n  nfiles: 7\n  path: fire_v3\n",
        encoding="utf-8",
    )
    _git(repo, "add", "data/fire_v3.dvc")
    _git(repo, "commit", "-q", "-m", "curate: fire v3\n\nremoved blurry frames")
    rev = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    return repo, rev


class _DummyDB:
    def __init__(self):
        self.rows = []

    def insert_catalog_row(self, row):
        self.rows.append(row)
        return row["dataset_catalog_id"]


def test_dvc_git_wrappers_read_real_repo(tmp_data_repo):
    repo, rev = tmp_data_repo
    meta = dvc_git.git_log_meta(str(repo), rev)
    assert meta.commit_subject == "curate: fire v3"
    assert "removed blurry frames" in meta.commit_message
    assert meta.commit_author_email == "shjin@example.com"
    files = dvc_git.list_dvc_files_at_rev(str(repo), rev)
    assert files == ["data/fire_v3.dvc"]
    text = dvc_git.read_file_at_rev(str(repo), rev, "data/fire_v3.dvc")
    assert "fire_v3" in text


def test_ingest_available_when_object_present(tmp_data_repo, mock_minio):
    repo, rev = tmp_data_repo
    mock_minio.upload("vlm-dataset", "_dvc/fire_v3", b"x")  # simulate dvc push done
    db = _DummyDB()
    out = _ci._run_catalog_ingest(
        db,
        mock_minio,
        repo_path=str(repo),
        data_repo_id="dvc-datasets",
        rev=rev,
        dataset_name="fire",
        task="sam3_detection",
        log=_NullLog(),
    )
    assert out["status"] == "available"
    assert out["missing_objects"] is False
    assert len(db.rows) == 1
    assert db.rows[0]["status"] == "available"
    assert db.rows[0]["commit_subject"] == "curate: fire v3"
    assert db.rows[0]["dvc_remote_url"] == "s3://vlm-dataset/_dvc/fire_v3"


def test_ingest_pending_when_object_missing(tmp_data_repo, mock_minio):
    repo, rev = tmp_data_repo  # no upload → object absent
    db = _DummyDB()
    out = _ci._run_catalog_ingest(
        db,
        mock_minio,
        repo_path=str(repo),
        data_repo_id="dvc-datasets",
        rev=rev,
        dataset_name="fire",
        task="sam3_detection",
        log=_NullLog(),
    )
    assert out["status"] == "pending_missing_dvc_objects"
    assert out["missing_objects"] is True
    assert db.rows[0]["status"] == "pending_missing_dvc_objects"
