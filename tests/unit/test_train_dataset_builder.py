"""Frozen snapshot builder tests — determinism / idempotency / AL honesty / stratify-fail."""

from __future__ import annotations

import pytest

pytest.importorskip("dagster")

from tests.helpers.dagster_dummies import DummyContext, DummyLogPermissive  # noqa: E402
from vlm_pipeline.defs.train import dataset as train_dataset  # noqa: E402


class _DummyDB:
    def __init__(self, candidates, confirmed=None):
        self._candidates = candidates
        self._confirmed = set(confirmed or set())
        self.inserted: list[dict] = []
        self.existing_checksums: set[tuple[str, str]] = set()

    def find_sam3_finalized_bbox_candidates(self, folder_name=None):
        return list(self._candidates)

    def find_al_confirmed_image_ids(self, image_ids):
        return {i for i in image_ids if i in self._confirmed}

    def train_dataset_version_exists(self, task, content_checksum):
        return (task, content_checksum) in self.existing_checksums

    def insert_train_dataset_version(self, row):
        self.existing_checksums.add((row["task"], row["content_checksum"]))
        self.inserted.append(row)


class _DummyMinIO:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def upload(self, bucket, key, data, content_type="application/octet-stream"):
        self.objects[(bucket, key)] = data if isinstance(data, (bytes, bytearray)) else bytes(data)

    def upload_json(self, bucket, key, payload, **kw):
        import json as _j

        self.objects[(bucket, key)] = _j.dumps(payload).encode("utf-8")


def _cands(n_groups=6, per_group=3):
    out = []
    for g in range(n_groups):
        for i in range(per_group):
            out.append(
                {
                    "image_id": f"vid{g:02d}_f{i:02d}",
                    "image_bucket": "vlm-processed",
                    "image_key": f"proj/vid{g:02d}/image/f{i:02d}.jpg",
                    "source_asset_id": f"vid{g:02d}",
                    "source_unit_name": "proj",
                    "category": "fire",
                    "box_index": 0,
                    "bbox_x": 10.0,
                    "bbox_y": 20.0,
                    "bbox_w": 50.0,
                    "bbox_h": 70.0,
                }
            )
    return out


def _ctx():
    return DummyContext(op_config={}, log=DummyLogPermissive())


def test_build_is_idempotent_same_checksum_noop():
    db = _DummyDB(_cands())
    minio = _DummyMinIO()
    common = dict(
        task="sam3_detection",
        folder_name="proj",
        ratios={"train": 0.6, "val": 0.2, "test": 0.2},
        seed=42,
        group_key_field="source_asset_id",
        min_per_split=1,
        force_new=False,
        log=_ctx().log,
    )
    r1 = train_dataset._run_build_trainset(db, minio, **common)
    assert r1["skipped_duplicate"] is False
    assert len(db.inserted) == 1
    r2 = train_dataset._run_build_trainset(db, minio, **common)
    assert r2["skipped_duplicate"] is True
    assert r2["content_checksum"] == r1["content_checksum"]
    assert len(db.inserted) == 1  # no second row


def test_al_confirmed_count_honest_zero():
    db = _DummyDB(_cands(), confirmed=set())  # AL∩annotations empty today
    minio = _DummyMinIO()
    r = train_dataset._run_build_trainset(
        db,
        minio,
        task="sam3_detection",
        folder_name="proj",
        ratios={"train": 0.6, "val": 0.2, "test": 0.2},
        seed=1,
        group_key_field="source_asset_id",
        min_per_split=1,
        force_new=False,
        log=_ctx().log,
    )
    assert r["al_confirmed_count"] == 0
    assert r["ls_count"] == len(_cands())


def test_no_group_spans_splits_in_split_file():
    db = _DummyDB(_cands())
    minio = _DummyMinIO()
    train_dataset._run_build_trainset(
        db,
        minio,
        task="sam3_detection",
        folder_name="proj",
        ratios={"train": 0.6, "val": 0.2, "test": 0.2},
        seed=3,
        group_key_field="source_asset_id",
        min_per_split=1,
        force_new=False,
        log=_ctx().log,
    )
    import json as _j

    split_obj = next(v for (b, k), v in minio.objects.items() if k.endswith("/splits/split_assignment.json"))
    assignment = _j.loads(split_obj.decode("utf-8"))
    # map group -> set of splits; each group must map to exactly one split
    by_group: dict[str, set[str]] = {}
    for image_id, split_name in assignment.items():
        grp = image_id.split("_f")[0]
        by_group.setdefault(grp, set()).add(split_name)
    assert all(len(s) == 1 for s in by_group.values())


def test_stratify_floor_fails_when_rare_class_starved():
    # 2 groups only -> val/test cannot both get a 'smoke' group if only one group has smoke
    cands = []
    for g in range(2):
        for i in range(2):
            cands.append(
                {
                    "image_id": f"vid{g}_f{i}",
                    "image_bucket": "vlm-processed",
                    "image_key": f"proj/vid{g}/image/f{i}.jpg",
                    "source_asset_id": f"vid{g}",
                    "source_unit_name": "proj",
                    "category": "fire" if g == 0 else "smoke",
                    "box_index": 0,
                    "bbox_x": 1.0,
                    "bbox_y": 1.0,
                    "bbox_w": 5.0,
                    "bbox_h": 5.0,
                }
            )
    db = _DummyDB(cands)
    minio = _DummyMinIO()
    with pytest.raises(ValueError, match="stratify"):
        train_dataset._run_build_trainset(
            db,
            minio,
            task="sam3_detection",
            folder_name="proj",
            ratios={"train": 0.5, "val": 0.25, "test": 0.25},
            seed=1,
            group_key_field="source_asset_id",
            min_per_split=1,
            force_new=False,
            log=_ctx().log,
        )
