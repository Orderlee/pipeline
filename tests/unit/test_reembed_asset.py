"""reembed_under_version asset — _DummyDB + mocked embedding client (no GPU, no PG, no HTTP).

design §8.1-B: 새 versioned model_name 으로 incumbent 커버 집합 재임베딩. resumable + 올바른
대상 + 새 model_name write 를 단언. 실행 게이트(ENABLE_TRAINING)는 launcher 레벨.
"""

from __future__ import annotations

import importlib

import pytest

reembed = importlib.import_module("vlm_pipeline.defs.embed.reembed")


class _Log:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass


class _Ctx:
    def __init__(self, config):
        self.op_config = config
        self.log = _Log()


class _Client:
    def wait_until_ready(self):
        return True

    def embed(self, data):
        return [0.1] * 1024

    def embed_text(self, text):
        return [0.2] * 1024


class _DummyMinIO:
    def download(self, bucket, key):
        return b"img-bytes"


class _DummyDB:
    def __init__(self, frame_targets, caption_targets):
        self._frame = frame_targets
        self._caption = caption_targets
        self.inserted_rows = []
        self.queried = []

    def ensure_runtime_schema(self):
        pass

    def get_active_embedding_model(self, scope="frame_search"):
        return "facebook/PE-Core-L14-336"

    def find_reembed_targets(self, *, incumbent_model_name, new_model_name, entity_type, limit):
        self.queried.append((incumbent_model_name, new_model_name, entity_type))
        page = self._frame if entity_type == "frame" else self._caption
        # one page then empty (resumable loop terminates)
        if page and not getattr(self, f"_done_{entity_type}", False):
            setattr(self, f"_done_{entity_type}", True)
            return page
        return []

    def batch_insert_embeddings(self, rows):
        self.inserted_rows.extend(rows)
        return len(rows)


@pytest.fixture(autouse=True)
def _mock_embed_client(monkeypatch):
    monkeypatch.setattr(reembed, "get_embedding_client", lambda: _Client())


def test_reembed_targets_incumbent_and_writes_new_model_name(monkeypatch):
    monkeypatch.setenv("ENABLE_TRAINING", "1")
    db = _DummyDB(
        frame_targets=[{"image_id": "f1", "image_bucket": "b", "image_key": "k", "image_role": "source_image"}],
        caption_targets=[{"label_id": "c1", "asset_id": "a1", "caption_text": "fire"}],
    )
    ctx = _Ctx({"new_version": "ft-2026.06.29-lora-001", "entity_types": ["frame", "caption"], "limit": 100})
    out = reembed._run_reembed(ctx, db, _DummyMinIO())

    new_name = "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"
    # incumbent = active stock pointer; new model_name = versioned
    assert ("facebook/PE-Core-L14-336", new_name, "frame") in db.queried
    assert out["new_model_name"] == new_name
    assert out["embedded"] == 2
    # every written row carries the NEW versioned model_name
    assert {r["model_name"] for r in db.inserted_rows} == {new_name}
    assert {r["entity_type"] for r in db.inserted_rows} == {"frame", "caption"}


def test_reembed_rejects_blank_version():
    db = _DummyDB([], [])
    ctx = _Ctx({"new_version": "", "entity_types": ["frame"]})
    with pytest.raises(Exception):
        reembed._run_reembed(ctx, db, _DummyMinIO())
