"""Unit tests for PostgresTrainMixin candidate queries (mock cursor, no live PG)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from vlm_pipeline.resources.postgres_train import PostgresTrainMixin


class _Mixin(PostgresTrainMixin):
    """PostgresTrainMixin standalone with a stubbed connect() + _rows_to_dicts."""

    def __init__(self, fetch_rows):
        self._fetch_rows = fetch_rows
        self.executed: list[tuple] = []

    @staticmethod
    def _rows_to_dicts(rows, columns):
        return [dict(zip(columns, r)) for r in rows]

    def connect(self):
        mixin = self

        class _Ctx:
            def __enter__(self_):
                cur = MagicMock()
                cur.__enter__ = MagicMock(return_value=cur)
                cur.__exit__ = MagicMock(return_value=False)
                cur.fetchall.return_value = mixin._fetch_rows
                conn = MagicMock()
                conn.cursor.return_value = cur

                def _exec(sql, params=None):
                    mixin.executed.append((sql, params))

                cur.execute.side_effect = _exec
                self_._conn = conn
                return conn

            def __exit__(self_, *a):
                return False

        return _Ctx()


def test_finalized_bbox_candidates_maps_columns():
    rows = [
        ("img-1", "vlm-processed", "proj/image/f1.jpg", "asset-1", "proj", "fire", 0, 1.0, 2.0, 3.0, 4.0),
    ]
    m = _Mixin(rows)
    out = m.find_sam3_finalized_bbox_candidates(folder_name="proj")
    assert out[0]["image_id"] == "img-1"
    assert out[0]["category"] == "fire"
    assert out[0]["source_unit_name"] == "proj"
    sql = m.executed[0][0]
    assert "image_label_annotations" in sql
    assert "review_status = 'finalized'" in sql


def test_al_confirmed_empty_when_no_annotations():
    m = _Mixin([])  # no annotation rows -> honest empty set
    confirmed = m.find_al_confirmed_image_ids(["img-1", "img-2"])
    assert confirmed == set()


def test_al_confirmed_returns_subset():
    m = _Mixin([("img-2",)])
    confirmed = m.find_al_confirmed_image_ids(["img-1", "img-2"])
    assert confirmed == {"img-2"}


def test_insert_train_dataset_version_uses_on_conflict():
    m = _Mixin([])
    m.insert_train_dataset_version(
        {
            "train_dataset_version_id": "tdv-1",
            "task": "sam3_detection",
            "content_checksum": "abc",
            "ls_count": 5,
            "al_confirmed_count": 0,
            "total_count": 5,
            "seed": 42,
        }
    )
    sql = m.executed[0][0]
    assert "INSERT INTO train_dataset_versions" in sql
    assert "ON CONFLICT (task, content_checksum) DO NOTHING" in sql


@pytest.mark.usefixtures("postgres_resource")
def test_insert_then_conflict_noop_real_pg(postgres_resource):
    db = postgres_resource
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('train_dataset_versions')")
            if cur.fetchone()[0] is None:
                pytest.skip("train_dataset_versions not present (migration 013 not merged)")
    payload = {
        "train_dataset_version_id": "tdv-int-1",
        "task": "sam3_detection",
        "content_checksum": "deadbeef",
        "ls_count": 3,
        "al_confirmed_count": 0,
        "total_count": 3,
        "seed": 42,
        "class_map": {"fire": 0},
        "split_ratios": {"train": 0.8, "val": 0.1, "test": 0.1},
        "per_class_counts": {"fire": {"train": 2, "val": 1, "test": 0, "total": 3}},
    }
    db.insert_train_dataset_version(payload)
    db.insert_train_dataset_version({**payload, "train_dataset_version_id": "tdv-int-2"})  # same checksum
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM train_dataset_versions WHERE content_checksum = 'deadbeef'")
            assert cur.fetchone()[0] == 1  # ON CONFLICT no-op
