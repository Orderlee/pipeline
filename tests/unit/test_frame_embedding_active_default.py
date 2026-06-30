"""frame_embedding 은 op_config 에 model_name 이 없으면 활성 포인터(db.get_active_embedding_model)
를 기본값으로 써야 한다 (design §8.1-A). decorated @asset 라 build_op_context 로 구동
(Dagster 1.13 는 임의 dummy context 직접 호출을 거부 → build_op_context 가 지원 경로).
"""

from __future__ import annotations

from dagster import build_op_context

from vlm_pipeline.defs.embed.assets import frame_embedding


class _DummyDB:
    def __init__(self):
        self.asked_model = None

    def ensure_runtime_schema(self):
        pass

    def get_active_embedding_model(self, scope="frame_search"):
        return "facebook/PE-Core-L14-336@ft-active"

    def find_pending_frame_embeddings(self, model_name, limit, image_roles):
        self.asked_model = model_name
        return []  # empty backlog → asset returns early, no embedding-service call


class _DummyMinIO:
    pass


def test_frame_embedding_uses_active_pointer_when_model_unset():
    db = _DummyDB()
    with build_op_context(op_config={"limit": 10}) as ctx:  # no model_name in config
        out = frame_embedding(ctx, db, _DummyMinIO())
    assert out == {"embedded": 0, "failed": 0, "pending": 0}
    # the active pointer (not the hardcoded stock constant) was queried for pending rows
    assert db.asked_model == "facebook/PE-Core-L14-336@ft-active"


def test_frame_embedding_respects_explicit_model_name():
    db = _DummyDB()
    with build_op_context(op_config={"limit": 10, "model_name": "facebook/PE-Core-L14-336"}) as ctx:
        frame_embedding(ctx, db, _DummyMinIO())
    assert db.asked_model == "facebook/PE-Core-L14-336"  # explicit config wins
