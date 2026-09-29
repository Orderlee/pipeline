"""GPU0 comfy lease 게이트 — 순수 판단(lib) + 임베딩 asset 의 defer 경로.

계약 세 가지를 못 박는다:
  1. TTL 경과 lease 는 **안 막는다** — owner 가 죽어도 자연 회복 (2026-09-18 3일 503 재발 방지).
  2. Dagster 쪽 PG 면은 **SELECT 전용** — 획득 메서드가 생기면 역방향 교착이 된다.
  3. lease 가 잡혀 있으면 임베딩 asset 은 Failure 가 아니라 **skip** 하고 다음 tick 에 재시도.
"""

from __future__ import annotations

import datetime as dt
import importlib
import inspect
import time

import pytest
from dagster import build_op_context

from vlm_pipeline.defs.embed.helpers import await_embedding_gpu, read_blocking_gpu0_lease
from vlm_pipeline.resources.postgres_genai import PostgresGenAIMixin
from vlm_pipeline.lib.gpu_lease import (
    GPU0_COMFY,
    GpuLease,
    describe_lease,
    is_lease_blocking,
    lease_from_pg_row,
    seconds_until_free,
)

NOW = 1_000_000.0


def _row(**kw) -> dict:
    base = {
        "resource": GPU0_COMFY,
        "owner_job_id": "batch-001",
        "state": "active",
        "acquired_at": NOW - 10.0,
        "expires_at": NOW + 1190.0,
    }
    base.update(kw)
    return base


# ----------------------------------------------------------------------
# 1. 순수 판정 (lib/gpu_lease.py)
# ----------------------------------------------------------------------
def test_missing_row_is_not_blocking():
    assert lease_from_pg_row(None) is None
    assert is_lease_blocking(None, now_ts=NOW) is False
    assert seconds_until_free(None, now_ts=NOW) == 0.0


def test_active_unexpired_lease_blocks():
    lease = lease_from_pg_row(_row())
    assert lease is not None
    assert lease.owner_job_id == "batch-001"
    assert is_lease_blocking(lease, now_ts=NOW) is True
    assert seconds_until_free(lease, now_ts=NOW) == pytest.approx(1190.0)


@pytest.mark.parametrize("state", ["released", "expired", ""])
def test_non_active_state_does_not_block(state):
    lease = lease_from_pg_row(_row(state=state))
    assert is_lease_blocking(lease, now_ts=NOW) is False


def test_ttl_expired_lease_does_not_block_even_when_active():
    """owner 가 죽어 release 를 못 해도 TTL 만으로 풀린다 — 이 파일의 존재 이유."""
    lease = lease_from_pg_row(_row(expires_at=NOW - 1.0))
    assert lease.state == "active"
    assert is_lease_blocking(lease, now_ts=NOW) is False
    assert seconds_until_free(lease, now_ts=NOW) == 0.0


def test_expires_at_exactly_now_does_not_block():
    """경계는 '안 막는 쪽'으로 연다 — 고착보다 조기 진입이 낫다."""
    assert is_lease_blocking(lease_from_pg_row(_row(expires_at=NOW)), now_ts=NOW) is False


def test_null_expires_at_does_not_block():
    """030 은 NOT NULL 이므로 None = 파싱 실패/남의 스키마. 불확실성으로 GPU0 를 잠그지 않는다."""
    assert is_lease_blocking(lease_from_pg_row(_row(expires_at=None)), now_ts=NOW) is False


def test_datetime_columns_are_parsed():
    """psycopg2 는 TIMESTAMP 를 datetime 으로 준다 — epoch 변환이 깨지면 게이트가 통째로 무력화된다."""
    start = dt.datetime(2026, 9, 21, 0, 0, 0, tzinfo=dt.timezone.utc)
    lease = lease_from_pg_row(_row(acquired_at=start, expires_at=start + dt.timedelta(seconds=1200)))
    assert is_lease_blocking(lease, now_ts=start.timestamp() + 600) is True
    assert is_lease_blocking(lease, now_ts=start.timestamp() + 1201) is False


def test_describe_lease_is_log_safe():
    assert describe_lease(None) == "none"
    assert "batch-001" in describe_lease(lease_from_pg_row(_row()))
    assert "?" in describe_lease(GpuLease(resource=GPU0_COMFY, state="active"))


# ----------------------------------------------------------------------
# 2. Dagster 쪽 PG 면은 읽기 전용이어야 한다
# ----------------------------------------------------------------------
def test_lib_gpu_lease_has_no_acquire_or_release_api():
    """획득 API 가 lib 에 생기면 Dagster 가 comfy 를 굶길 수 있다."""
    mod = importlib.import_module("vlm_pipeline.lib.gpu_lease")
    names = [n for n in dir(mod) if not n.startswith("_")]
    forbidden = [n for n in names if any(k in n.lower() for k in ("acquire", "release", "heartbeat", "steal"))]
    assert forbidden == [], f"lib/gpu_lease.py must stay read-only; found {forbidden}"


def test_postgres_genai_mixin_never_writes_the_lease_table():
    """mixin 안의 generation_gpu_leases SQL 은 SELECT 뿐이어야 한다."""
    from vlm_pipeline.resources import postgres_genai

    src = inspect.getsource(postgres_genai)
    assert "generation_gpu_leases" in src  # 게이트가 사라지면 이 테스트도 같이 죽어야 한다
    for verb in (
        "INSERT INTO generation_gpu_leases",
        "UPDATE generation_gpu_leases",
        "DELETE FROM generation_gpu_leases",
    ):
        assert verb not in src, f"Dagster must not {verb.split()[0].lower()} the comfy lease"


def test_get_generation_gpu_lease_defaults_to_gpu0_comfy():
    from vlm_pipeline.resources.postgres_genai import PostgresGenAIMixin

    sig = inspect.signature(PostgresGenAIMixin.get_generation_gpu_lease)
    assert sig.parameters["resource"].default == GPU0_COMFY


# ----------------------------------------------------------------------
# 3. await_embedding_gpu — 3-way 판정
# ----------------------------------------------------------------------
class _DB:
    def __init__(self, row=None, exc=None):
        self._row = row
        self._exc = exc
        self.calls = 0

    def get_generation_gpu_lease(self, resource=GPU0_COMFY):
        self.calls += 1
        if self._exc is not None:
            raise self._exc
        return self._row


class _Client:
    def __init__(self, ready: bool):
        self._ready = ready
        self.waits = 0

    def wait_until_ready(self):
        self.waits += 1
        return self._ready


def test_ready_service_without_lease_proceeds():
    client = _Client(True)
    assert await_embedding_gpu(_DB(None), client, now_ts=NOW) == (True, None)
    assert client.waits == 1


def test_blocking_lease_defers_without_polling_the_service():
    """120s 헛돌기 방지 — lease 가 확실하면 wait_until_ready 를 아예 부르지 않는다."""
    client = _Client(True)
    ready, reason = await_embedding_gpu(_DB(_row()), client, now_ts=NOW)
    assert ready is False
    assert reason is not None and "gpu0_comfy" in reason
    assert client.waits == 0


def test_service_down_without_lease_is_systemic():
    """lease 가 없는데 서비스가 안 뜨면 진짜 장애 → 호출부가 Failure 를 던지도록 (False, None)."""
    assert await_embedding_gpu(_DB(None), _Client(False), now_ts=NOW) == (False, None)


def test_lease_acquired_during_the_wait_is_a_defer_not_a_failure():
    """대기 120s 사이에 comfy 가 잡아간 경우 — 재확인이 없으면 systemic 으로 오진한다."""

    class _LateDB(_DB):
        def get_generation_gpu_lease(self, resource=GPU0_COMFY):
            self.calls += 1
            return None if self.calls == 1 else _row()

    db = _LateDB()
    ready, reason = await_embedding_gpu(db, _Client(False), now_ts=NOW)
    assert ready is False
    assert reason is not None
    assert db.calls == 2


def test_expired_lease_with_dead_service_stays_systemic():
    """TTL 지난 lease 를 defer 근거로 쓰면 3일 503 이 재현된다."""
    assert await_embedding_gpu(_DB(_row(expires_at=NOW - 1.0)), _Client(False), now_ts=NOW) == (False, None)


def test_lease_lookup_failure_is_fail_open():
    """030 미적용 staging / DB 순단에서 임베딩이 통째로 멈추면 안 된다."""
    db = _DB(exc=RuntimeError('relation "generation_gpu_leases" does not exist'))
    assert read_blocking_gpu0_lease(db, now_ts=NOW) is None
    assert await_embedding_gpu(db, _Client(True), now_ts=NOW) == (True, None)


# ----------------------------------------------------------------------
# 4. asset 배선 — Failure 가 아니라 skip 으로 나간다
# ----------------------------------------------------------------------
class _AssetDB(_DB):
    def ensure_runtime_schema(self):
        pass

    def get_active_embedding_model(self, scope="frame_search"):
        return "facebook/PE-Core-L14-336"

    def find_pending_frame_embeddings(self, *, model_name, limit, image_roles):
        return [{"image_id": "img-1", "image_bucket": "b", "image_key": "k"}]

    def find_pending_caption_embeddings(self, *, model_name, limit):
        return [{"label_id": "lab-1", "caption_text": "t"}]

    def batch_insert_embeddings(self, rows):  # pragma: no cover — defer 경로에선 안 불린다
        raise AssertionError("must not embed while comfy holds GPU0")


@pytest.fixture
def _held_lease(monkeypatch):
    """embedding client 는 준비돼 있다고 보고 — 오직 lease 때문에 미뤄지는지만 본다."""
    monkeypatch.setattr(
        "vlm_pipeline.defs.embed.assets.get_embedding_client",
        lambda: _Client(True),
    )
    return _AssetDB(_row(expires_at=time.time() + 600))


def test_frame_embedding_defers_instead_of_failing(_held_lease):
    from vlm_pipeline.defs.embed.assets import frame_embedding

    with build_op_context(op_config={"limit": 10}) as ctx:
        out = frame_embedding(ctx, _held_lease, None)
    assert out["deferred"] == "gpu0_comfy_lease"
    assert out["embedded"] == 0
    assert out["pending"] == 1  # backlog 는 그대로 남는다 → 다음 tick 이 다시 집어간다


def test_caption_embedding_defers_instead_of_failing(_held_lease):
    from vlm_pipeline.defs.embed.assets import caption_embedding

    with build_op_context(op_config={"limit": 10}) as ctx:
        out = caption_embedding(ctx, _held_lease)
    assert out["deferred"] == "gpu0_comfy_lease"
    assert out["embedded"] == 0


def test_reembed_defers_instead_of_failing(monkeypatch, _held_lease):
    """gpu_trainer 슬롯(concurrency=1)을 붙잡고 헛돌지 않는지."""
    reembed = importlib.import_module("vlm_pipeline.defs.embed.reembed")
    monkeypatch.setenv("ENABLE_TRAINING", "1")
    monkeypatch.setattr(reembed, "get_embedding_client", lambda: _Client(True))
    monkeypatch.setattr(
        "vlm_pipeline.defs.embed.helpers.read_blocking_gpu0_lease",
        lambda db, now_ts=None: lease_from_pg_row(_row(expires_at=time.time() + 600)),
    )
    ctx = build_op_context(op_config={"new_version": "ft-2026.09.21-lora-001"})
    out = reembed._run_reembed(ctx, _held_lease, None)
    assert out["deferred"] == "gpu0_comfy_lease"
    assert out["embedded"] == 0


# ----------------------------------------------------------------------
# 5. mixin 의 행 매핑 — psycopg2 기본 커서는 **튜플**을 준다
# ----------------------------------------------------------------------
class _Cur:
    def __init__(self, row):
        self._row = row
        self.sql = None
        self.params = None

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params

    def fetchone(self):
        return self._row

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Mixin(PostgresGenAIMixin):
    def __init__(self, row):
        self.cur = _Cur(row)

    def connect(self):
        return _Conn(self.cur)


def test_tuple_row_columns_map_in_select_order():
    """SELECT 순서와 cols 튜플이 어긋나면 state 자리에 owner 가 들어가 게이트가 조용히 뒤집힌다."""
    mixin = _Mixin(("gpu0_comfy", "batch-001", "active", NOW - 10.0, NOW + 1190.0))
    out = mixin.get_generation_gpu_lease()
    assert out == {
        "resource": "gpu0_comfy",
        "owner_job_id": "batch-001",
        "state": "active",
        "acquired_at": NOW - 10.0,
        "expires_at": NOW + 1190.0,
    }
    assert is_lease_blocking(lease_from_pg_row(out), now_ts=NOW) is True
    # SELECT 절의 컬럼 순서가 매핑과 같은지 직접 확인 (한쪽만 바뀌는 드리프트 차단)
    select_cols = [c.strip() for c in mixin.cur.sql.split("SELECT", 1)[1].split("FROM", 1)[0].split(",")]
    assert select_cols == ["resource", "owner_job_id", "state", "acquired_at", "expires_at"]
    assert mixin.cur.params == {"resource": GPU0_COMFY}


def test_missing_lease_row_returns_none():
    assert _Mixin(None).get_generation_gpu_lease() is None
