"""assemble_multi_project_coco (Tier 2) — pinned DVC 소스 N개 → merge. 전부 mock.

dvc_get 은 주입형(실 dvc 미실행): -o dest 에 synthetic coco.json 을 써서 'pull' 흉내.
_DB.get_catalog_by_alias 가 프로젝트별 pin 행 반환.
"""
from __future__ import annotations

import json
import os

from vlm_pipeline.defs.train.dataset import (
    _run_build_trainset_from_dvc,
    assemble_multi_project_coco,
    freeze_multi_project_trainset,
)


class _DB:
    def __init__(self, rows):
        self._rows = rows  # {task: catalog_row or None}

    def get_catalog_by_alias(self, task, alias="current"):
        return self._rows.get(task)


def _coco(cls, fn):
    return {
        "images": [{"id": 1, "file_name": fn}],
        "annotations": [{"id": 1, "image_id": 1, "category_id": 1, "bbox": [0, 0, 1, 1]}],
        "categories": [{"id": 1, "name": cls}],
    }


def _row(cid, rev, out):
    return {"dataset_catalog_id": cid, "git_rev": rev, "dvc_out_path": out, "dvc_md5": "m" + cid}


def _make_dvc_get(coco_by_out):
    # build_dvc_get_argv → [dvc, get, repo, out, --rev, rev, -o, dest]
    def dvc_get(argv):
        out, dest = argv[3], argv[-1]
        os.makedirs(dest, exist_ok=True)
        with open(os.path.join(dest, "coco.json"), "w", encoding="utf-8") as fh:
            json.dump(coco_by_out[out], fh)
    return dvc_get


def test_assemble_two_projects(tmp_path):
    db = _DB({"projA": _row("ca", "r1", "fireA"), "projB": _row("cb", "r2", "fireB")})
    dvc_get = _make_dvc_get({"fireA": _coco("fire", "a.jpg"), "fireB": _coco("smoke", "b.jpg")})
    merged, spec = assemble_multi_project_coco(
        db, sources=[{"task": "projA"}, {"task": "projB"}], dest_root=str(tmp_path), dvc_get=dvc_get
    )
    assert len(merged["images"]) == 2
    assert {c["name"] for c in merged["categories"]} == {"fire", "smoke"}
    assert [s["task"] for s in spec["sources"]] == ["projA", "projB"]
    assert spec["sources"][0]["git_rev"] == "r1" and spec["sources"][0]["dataset_catalog_id"] == "ca"
    assert spec["missing"] == []


def test_assemble_allowlist(tmp_path):
    db = _DB({"projA": _row("ca", "r1", "fireA"), "projB": _row("cb", "r2", "fireB")})
    dvc_get = _make_dvc_get({"fireA": _coco("fire", "a.jpg"), "fireB": _coco("smoke", "b.jpg")})
    merged, _ = assemble_multi_project_coco(
        db, sources=[{"task": "projA"}, {"task": "projB"}], dest_root=str(tmp_path),
        dvc_get=dvc_get, class_allowlist=["fire"],
    )
    assert {c["name"] for c in merged["categories"]} == {"fire"}      # smoke dropped
    assert len(merged["annotations"]) == 1


def test_assemble_skips_unpinned_project(tmp_path):
    db = _DB({"projA": _row("ca", "r1", "fireA"), "projB": None})       # projB not pinned
    dvc_get = _make_dvc_get({"fireA": _coco("fire", "a.jpg")})
    merged, spec = assemble_multi_project_coco(
        db, sources=[{"task": "projA"}, {"task": "projB"}], dest_root=str(tmp_path), dvc_get=dvc_get
    )
    assert [s["task"] for s in spec["sources"]] == ["projA"]
    assert spec["missing"] == [{"task": "projB", "alias": "current"}]   # recorded, not silent
    assert len(merged["images"]) == 1


class _FreezeDB:
    """train_dataset_version_exists + insert_train_dataset_version 캡처 + 멱등 시뮬."""
    def __init__(self):
        self.inserted = []
        self._checksums = set()

    def train_dataset_version_exists(self, task, checksum):
        return checksum in self._checksums

    def insert_train_dataset_version(self, row):
        self.inserted.append(row)
        self._checksums.add(row["content_checksum"])


class _MinIO:
    def __init__(self):
        self.uploaded = []

    def upload(self, bucket, key, data, content_type="application/octet-stream"):
        self.uploaded.append((bucket, key, len(data)))


_MERGED = {
    "images": [{"id": 1, "file_name": "a.jpg", "source": "projA"},
               {"id": 2, "file_name": "b.jpg", "source": "projB"}],
    "annotations": [{"id": 1, "image_id": 1, "category_id": 1, "bbox": [0, 0, 1, 1]},
                    {"id": 2, "image_id": 2, "category_id": 2, "bbox": [1, 1, 2, 2]}],
    "categories": [{"id": 1, "name": "fire"}, {"id": 2, "name": "smoke"}],
}
_SPEC = {"sources": [{"task": "projA", "dataset_catalog_id": "ca", "git_rev": "r1"},
                     {"task": "projB", "dataset_catalog_id": "cb", "git_rev": "r2"}]}


def test_freeze_inserts_version_with_source_spec():
    db, minio = _FreezeDB(), _MinIO()
    out = freeze_multi_project_trainset(db, minio, merged_coco=_MERGED, source_spec=_SPEC, task="sam3_detection")
    assert out["skipped_duplicate"] is False
    assert out["total_count"] == 2 and out["per_class_counts"] == {"fire": 1, "smoke": 1}
    assert len(db.inserted) == 1
    row = db.inserted[0]
    assert row["source_spec"] == _SPEC                 # 멀티프로젝트 lineage 기록
    assert row["content_checksum"] == out["content_checksum"]
    assert row["manifest_key"].endswith("/coco.json")
    assert row["split_assignment_key"].endswith("/split_assignment.json")   # MinIO 키 (리터럴 아님)
    assert len(minio.uploaded) == 2                    # merged COCO + split_assignment 동결


def test_freeze_idempotent_on_content_checksum():
    db, minio = _FreezeDB(), _MinIO()
    a = freeze_multi_project_trainset(db, minio, merged_coco=_MERGED, source_spec=_SPEC, task="sam3_detection")
    b = freeze_multi_project_trainset(db, minio, merged_coco=_MERGED, source_spec=_SPEC, task="sam3_detection")
    assert a["skipped_duplicate"] is False and b["skipped_duplicate"] is True
    assert a["content_checksum"] == b["content_checksum"]   # 결정적
    assert len(db.inserted) == 1                            # 두 번째는 no-op


class _FullDB:
    """assemble(get_catalog_by_alias) + freeze(exists/insert) 둘 다 — asset DVC 경로 end-to-end."""
    def __init__(self, rows):
        self._rows = rows
        self.inserted = []
        self._cks = set()

    def get_catalog_by_alias(self, task, alias="current"):
        return self._rows.get(task)

    def train_dataset_version_exists(self, task, checksum):
        return checksum in self._cks

    def insert_train_dataset_version(self, row):
        self.inserted.append(row)
        self._cks.add(row["content_checksum"])


def test_run_build_trainset_from_dvc_end_to_end(tmp_path):
    db = _FullDB({"projA": _row("ca", "r1", "fireA"), "projB": _row("cb", "r2", "fireB")})
    minio = _MinIO()
    dvc_get = _make_dvc_get({"fireA": _coco("fire", "a.jpg"), "fireB": _coco("smoke", "b.jpg")})
    out = _run_build_trainset_from_dvc(
        db, minio, sources=[{"task": "projA"}, {"task": "projB"}],
        task="sam3_detection", dest_root=str(tmp_path), dvc_get=dvc_get,
    )
    assert out["skipped_duplicate"] is False and out["train_dataset_version_id"]
    assert out["sources"] == 2 and out["missing_sources"] == 0 and out["total_count"] == 2
    assert len(db.inserted) == 1
    assert [s["task"] for s in db.inserted[0]["source_spec"]["sources"]] == ["projA", "projB"]
