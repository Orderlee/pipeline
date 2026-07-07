"""PostgresTrainMixin DVC-catalog helpers — insert / get-by-alias / transactional pin.

No real PG. _DummyDB captures executed SQL and simulates UNIQUE-on-catalog + the
single alias-per-task invariant so pin_alias' transaction logic is fully exercised.
"""
from __future__ import annotations

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.resources.postgres_train import PostgresTrainMixin  # noqa: E402


class _Cursor:
    # get_catalog_by_alias uses PostgresBaseMixin._cursor_to_dicts(cur): description=None → [].
    description = None

    def __init__(self, store):
        self.store = store
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.store["log"].append((sql, params))
        s = " ".join(sql.split()).lower()
        if "insert into dataset_catalog (" in s:
            # ON CONFLICT DO UPDATE ... WHERE status='pending_missing_dvc_objects' — simulate
            # dedup by UNIQUE key, self-heal only when the pre-existing row is still pending.
            key = (params["data_repo_id"], params["git_rev"], params["dvc_file_path"], params["dvc_out_path"])
            existing = self.store["catalog_by_key"].get(key)
            if existing is None:
                self.store["catalog_by_key"][key] = params["dataset_catalog_id"]
                self.store["task_by_id"][params["dataset_catalog_id"]] = params["task"]
                self.store["status_by_id"][params["dataset_catalog_id"]] = params.get("status", "available")
                self._result = (params["dataset_catalog_id"],)
            else:
                prev_status = self.store["status_by_id"].get(existing)
                if prev_status == "pending_missing_dvc_objects":
                    self.store["status_by_id"][existing] = params.get("status", "available")
                    self._result = (existing,)  # DO UPDATE matched WHERE -> RETURNING fires
                else:
                    self._result = None  # WHERE guard blocked the update -> no RETURNING row
        elif "select task, status from dataset_catalog where dataset_catalog_id" in s:
            tsk = self.store["task_by_id"].get(params["dataset_catalog_id"])
            status = self.store["status_by_id"].get(params["dataset_catalog_id"])
            self._result = (tsk, status) if tsk is not None else None
        elif "select dataset_catalog_id from dataset_catalog where" in s:
            key = (params["data_repo_id"], params["git_rev"], params["dvc_file_path"], params["dvc_out_path"])
            cid = self.store["catalog_by_key"].get(key)
            self._result = (cid,) if cid else None
        elif "from dataset_catalog_aliases a" in s:
            row = self.store["aliases"].get((params["task"], params["alias"]))
            self._result = row
        elif "select dataset_catalog_id from dataset_catalog_aliases" in s:
            row = self.store["aliases"].get((params["task"], params["alias"]))
            self._result = (row["dataset_catalog_id"],) if row else None
        elif "insert into dataset_catalog_aliases" in s or "update dataset_catalog_aliases" in s:
            self.store["aliases"][(params["task"], params["alias"])] = {
                "dataset_catalog_id": params["dataset_catalog_id"], "task": params["task"], "alias": params["alias"],
            }
        elif "insert into dataset_catalog_pin_events" in s:
            self.store["pin_events"].append(params)
        elif "update dataset_catalog set status" in s:
            self.store["status_updates"].append(params)
            self.store["status_by_id"][params["dataset_catalog_id"]] = "pinned"

    def fetchone(self):
        return self._result

    def fetchall(self):
        return []  # description is None → _cursor_to_dicts returns [] before this is hit

    def cursor(self):
        return _Cursor(self.store)


class _Conn:
    def __init__(self, store):
        self.store = store
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return _Cursor(self.store)

    def commit(self):
        self.committed = True


class _DummyDB(PostgresTrainMixin):
    def __init__(self):
        self.store = {
            "log": [], "catalog_by_key": {}, "task_by_id": {}, "status_by_id": {}, "aliases": {},
            "pin_events": [], "status_updates": [],
        }

    def connect(self):
        return _Conn(self.store)


def _row(cid, rev="rev1", status="available"):
    return {
        "dataset_catalog_id": cid, "task": "sam3_detection", "dataset_name": "fire",
        "status": status, "data_repo_id": "dvc-datasets", "git_rev": rev,
        "dvc_file_path": "data/fire.dvc", "dvc_out_path": "fire",
    }


def test_insert_catalog_row_returns_id_and_is_idempotent():
    db = _DummyDB()
    cid1 = db.insert_catalog_row(_row("11111111-1111-1111-1111-111111111111"))
    assert cid1 == "11111111-1111-1111-1111-111111111111"
    # same UNIQUE key, different generated id → ON CONFLICT → returns the FIRST id:
    cid2 = db.insert_catalog_row(_row("22222222-2222-2222-2222-222222222222"))
    assert cid2 == cid1, "idempotent insert must return the pre-existing row id"


def test_pin_alias_is_transactional_and_appends_event():
    db = _DummyDB()
    a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    db.insert_catalog_row(_row(a, rev="rev1"))
    db.insert_catalog_row(_row(b, rev="rev2"))

    out1 = db.pin_alias(task="sam3_detection", alias="current", dataset_catalog_id=a, pinned_by="eng")
    assert out1["dataset_catalog_id"] == a
    assert out1["previous_dataset_catalog_id"] is None
    assert db.store["aliases"][("sam3_detection", "current")]["dataset_catalog_id"] == a
    assert len(db.store["pin_events"]) == 1
    assert db.store["status_updates"][-1]["dataset_catalog_id"] == a  # status->pinned

    out2 = db.pin_alias(task="sam3_detection", alias="current", dataset_catalog_id=b, pinned_by="eng", pin_reason="better")
    assert out2["dataset_catalog_id"] == b
    assert out2["previous_dataset_catalog_id"] == a, "must record the displaced catalog id"
    assert db.store["aliases"][("sam3_detection", "current")]["dataset_catalog_id"] == b
    assert len(db.store["pin_events"]) == 2
    assert db.store["pin_events"][-1]["pin_reason"] == "better"


def test_get_catalog_by_alias_none_when_unpinned():
    db = _DummyDB()
    assert db.get_catalog_by_alias("sam3_detection", "current") is None


def test_pin_alias_rejects_cross_task_catalog():
    # Codex BUG3: pinning a task's alias to another task's catalog row must be rejected.
    db = _DummyDB()
    cid = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    db.insert_catalog_row(_row(cid, rev="rev9"))  # task='sam3_detection'
    with pytest.raises(ValueError):
        db.pin_alias(task="person_detection", alias="current", dataset_catalog_id=cid, pinned_by="eng")


def test_insert_catalog_row_self_heals_pending_to_available():
    # H-4: a re-ingest of the same DVC UNIQUE key must flip a pending row to the newly
    # observed status (e.g. once the MinIO objects show up), not stay stuck forever.
    db = _DummyDB()
    cid = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    db.insert_catalog_row(_row(cid, rev="rev-pending", status="pending_missing_dvc_objects"))
    assert db.store["status_by_id"][cid] == "pending_missing_dvc_objects"

    healed_id = db.insert_catalog_row(_row("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee", rev="rev-pending", status="available"))
    assert healed_id == cid, "self-heal must still return the original row's id"
    assert db.store["status_by_id"][cid] == "available"


def test_insert_catalog_row_does_not_demote_available_row():
    # WHERE guard: an 'available'/'pinned' row must never be overwritten by a re-ingest.
    db = _DummyDB()
    cid = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    db.insert_catalog_row(_row(cid, rev="rev-avail", status="available"))
    assert db.store["status_by_id"][cid] == "available"

    same_id = db.insert_catalog_row(
        _row("11111111-2222-3333-4444-555555555555", rev="rev-avail", status="pending_missing_dvc_objects")
    )
    assert same_id == cid
    assert db.store["status_by_id"][cid] == "available", "available row must not be demoted back to pending"


def test_pin_alias_rejects_pending_status():
    # M-5: fast-fail instead of letting a pending_missing_dvc_objects row get pinned (which
    # would only fail later, opaquely, at `dvc get` time).
    db = _DummyDB()
    cid = "12121212-1212-1212-1212-121212121212"
    db.insert_catalog_row(_row(cid, rev="rev-p", status="pending_missing_dvc_objects"))
    with pytest.raises(ValueError):
        db.pin_alias(task="sam3_detection", alias="current", dataset_catalog_id=cid, pinned_by="eng")


def test_pin_alias_rejects_invalid_status():
    db = _DummyDB()
    cid = "13131313-1313-1313-1313-131313131313"
    db.insert_catalog_row(_row(cid, rev="rev-i", status="invalid"))
    with pytest.raises(ValueError):
        db.pin_alias(task="sam3_detection", alias="current", dataset_catalog_id=cid, pinned_by="eng")


def test_pin_alias_accepts_available_and_pinned_status():
    db = _DummyDB()
    cid = "14141414-1414-1414-1414-141414141414"
    db.insert_catalog_row(_row(cid, rev="rev-ok", status="available"))
    out = db.pin_alias(task="sam3_detection", alias="current", dataset_catalog_id=cid, pinned_by="eng")
    assert out["dataset_catalog_id"] == cid
    # re-pinning an already-pinned row (status now 'pinned') must also succeed.
    out2 = db.pin_alias(task="sam3_detection", alias="current", dataset_catalog_id=cid, pinned_by="eng2")
    assert out2["dataset_catalog_id"] == cid
