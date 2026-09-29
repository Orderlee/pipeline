"""embedding 서비스 정비 게이트: maintenance 활성 시 503 + lazy-reload 거부."""

from __future__ import annotations

import importlib
import pathlib
import sys

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("multipart")

_SVC_DIR = str(pathlib.Path("docker/embedding").resolve())


@pytest.fixture
def client(monkeypatch):
    from fastapi.testclient import TestClient

    if _SVC_DIR not in sys.path:
        sys.path.insert(0, _SVC_DIR)
    monkeypatch.setenv("EMBEDDING_BACKEND", "open_clip")
    import backends.open_clip_be as be

    state = {"loaded": False, "load_calls": 0}

    def _load(self):
        state["load_calls"] += 1
        state["loaded"] = True

    monkeypatch.setattr(be.OpenClipBackend, "load", _load)
    monkeypatch.setattr(be.OpenClipBackend, "unload", lambda self: state.__setitem__("loaded", False))
    monkeypatch.setattr(be.OpenClipBackend, "is_loaded", lambda self: state["loaded"])
    monkeypatch.setattr(be.OpenClipBackend, "embed_image", lambda self, b: [0.1] * 1024)
    monkeypatch.setattr(be.OpenClipBackend, "embed_text", lambda self, t: [0.2] * 1024)

    sys.modules.pop("app", None)
    app_mod = importlib.import_module("app")
    with TestClient(app_mod.app) as c:
        c.__dict__["_test_state"] = state  # expose for assertions (avoid TestClient._state clash)
        yield c


def test_embed_503_under_maintenance(client):
    assert client.post("/maintenance/enter", data={"owner_run_id": "run-1"}).status_code == 200
    r = client.post("/embed_text", data={"text": "x"})
    assert r.status_code == 503
    assert "maintenance" in r.json()["detail"]


def test_warmup_503_under_maintenance(client):
    client.post("/maintenance/enter", data={"owner_run_id": "run-1"})
    assert client.post("/warmup").status_code == 503


def test_lazy_reload_refused_under_maintenance(client):
    # unload, then enter maintenance, then /embed must NOT reload (load_calls frozen)
    client.post("/unload")
    calls_before = client.__dict__["_test_state"]["load_calls"]
    client.post("/maintenance/enter", data={"owner_run_id": "run-1"})
    r = client.post("/embed_text", data={"text": "x"})
    assert r.status_code == 503
    assert client.__dict__["_test_state"]["load_calls"] == calls_before  # no lazy reload happened


def test_exit_restores_normal_operation(client):
    client.post("/maintenance/enter", data={"owner_run_id": "run-1"})
    assert client.post("/embed_text", data={"text": "x"}).status_code == 503
    assert client.post("/maintenance/exit").status_code == 200
    r = client.post("/embed_text", data={"text": "x"})
    assert r.status_code == 200
    assert len(r.json()["vector"]) == 1024


def test_status_reflects_flag(client):
    assert client.get("/maintenance/status").json()["active"] is False
    client.post("/maintenance/enter", data={"owner_run_id": "run-7", "note": "ft"})
    s = client.get("/maintenance/status").json()
    assert s["active"] is True and s["owner_run_id"] == "run-7"


def test_normal_operation_unaffected_when_clear(client):
    r = client.post("/embed", files={"file": ("i.jpg", b"x", "application/octet-stream")})
    assert r.status_code == 200 and len(r.json()["vector"]) == 1024


# ─── 정비창 소유권 (P1-3) ────────────────────────────────────────────────────
# 정비창은 GPU 한 장을 독점하는 전역 상태다. owner 검증이 없으면 comfy_local job
# 하나가 trainer/SAM3 의 drain 을 해제할 수 있다. 동시에 이 게이트가 닫힌 채 남으면
# 서비스가 통째로 503 이 되므로(2026-09-18), 해제 경로는 반드시 살아 있어야 한다.


def test_exit_rejects_foreign_owner(client):
    """남의 정비창은 닫지 못한다 — 이 테스트가 P1-3 의 본체."""
    client.post("/maintenance/enter", data={"owner_run_id": "trainer-run"})
    r = client.post("/maintenance/exit", data={"owner_run_id": "comfy-job-1"})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "maintenance_owner_mismatch"
    assert r.json()["detail"]["current_owner_run_id"] == "trainer-run"
    # 거부됐으면 창은 그대로 닫혀 있어야 한다 (게이트가 풀렸는지로 확인).
    assert client.get("/maintenance/status").json()["active"] is True
    assert client.post("/embed_text", data={"text": "x"}).status_code == 503


def test_exit_accepts_matching_owner(client):
    client.post("/maintenance/enter", data={"owner_run_id": "trainer-run"})
    r = client.post("/maintenance/exit", data={"owner_run_id": "trainer-run"})
    assert r.status_code == 200 and r.json()["released_by"] == "owner"
    assert client.post("/embed_text", data={"text": "x"}).status_code == 200


def test_exit_force_releases_any_owner(client):
    """운영자 escape hatch (scripts/clear_maintenance.sh) 는 무조건 통해야 한다."""
    client.post("/maintenance/enter", data={"owner_run_id": "trainer-run"})
    r = client.post("/maintenance/exit", data={"force": "true"})
    assert r.status_code == 200 and r.json()["released_by"] == "force"
    assert client.get("/maintenance/status").json()["active"] is False


def test_exit_without_owner_stays_backward_compatible(client):
    """owner 미지정 = 옛 호출자. 통과시키되 force 와 구분해 기록한다."""
    client.post("/maintenance/enter", data={"owner_run_id": "trainer-run"})
    r = client.post("/maintenance/exit")
    assert r.status_code == 200 and r.json()["released_by"] == "unverified"
    assert client.get("/maintenance/status").json()["active"] is False


def test_exit_when_inactive_is_idempotent_noop(client):
    """정비 중이 아니면 누가 불러도 200 — 해제 경로를 막으면 503 이 굳는다."""
    r = client.post("/maintenance/exit", data={"owner_run_id": "whoever"})
    assert r.status_code == 200 and r.json()["released_by"] == "unowned"


def test_enter_rejects_takeover_by_other_owner(client):
    """exit 만 막고 enter 를 열어두면 '가로채서 진입 → 내 것처럼 해제' 로 우회된다."""
    client.post("/maintenance/enter", data={"owner_run_id": "trainer-run"})
    r = client.post("/maintenance/enter", data={"owner_run_id": "comfy-job-1"})
    assert r.status_code == 409
    assert client.get("/maintenance/status").json()["owner_run_id"] == "trainer-run"


def test_enter_by_same_owner_is_idempotent(client):
    client.post("/maintenance/enter", data={"owner_run_id": "trainer-run", "ttl_seconds": "60"})
    r = client.post("/maintenance/enter", data={"owner_run_id": "trainer-run", "ttl_seconds": "120"})
    assert r.status_code == 200 and r.json()["granted_by"] == "owner"
    assert client.get("/maintenance/status").json()["ttl_seconds"] == 120


def test_enter_force_takes_over(client):
    client.post("/maintenance/enter", data={"owner_run_id": "trainer-run"})
    r = client.post("/maintenance/enter", data={"owner_run_id": "operator", "force": "true"})
    assert r.status_code == 200 and r.json()["granted_by"] == "force"
    assert client.get("/maintenance/status").json()["owner_run_id"] == "operator"


def test_heartbeat_rejects_foreign_owner(client):
    """남의 창을 무한 연장하는 heartbeat 가 2026-09-18 3일 503 의 기전이었다."""
    client.post("/maintenance/enter", data={"owner_run_id": "trainer-run", "ttl_seconds": "60"})
    assert client.post("/maintenance/heartbeat", data={"owner_run_id": "comfy-job-1"}).status_code == 409
    assert client.post("/maintenance/heartbeat", data={"owner_run_id": "trainer-run"}).status_code == 200


def test_comfy_local_exit_calls_carry_owner_run_id():
    """comfy_local 이 owner 를 안 실으면 서버측 검증이 무의미해진다 (호출부 계약).

    소스 정적 검증인 이유: 어댑터 전체를 띄우려면 ComfyUI·PG 스텁이 필요한데,
    여기서 지키고 싶은 건 '모든 maintenance 호출에 owner_run_id 가 붙는가' 하나다.
    """
    import pathlib as _pathlib
    import re

    src = _pathlib.Path("docker/genai/adapters/comfy_local.py").read_text()
    calls = re.findall(r"_embedding_post\(\s*\n?\s*\"(/maintenance/[a-z]+)\"([^)]*)\)", src)
    assert calls, "comfy_local 의 maintenance 호출을 찾지 못했다 (정규식/파일 구조 변경?)"
    for path, args in calls:
        assert "owner_run_id" in args, f"{path} 호출에 owner_run_id 가 없다: {args!r}"
