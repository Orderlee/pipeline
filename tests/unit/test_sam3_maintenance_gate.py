"""SAM3 서비스 정비 게이트: maintenance 활성 시 503 + lazy-reload 거부."""

from __future__ import annotations

import importlib
import json
import pathlib
import sys
import time

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("multipart")

_SVC_DIR = str(pathlib.Path("docker/sam3").resolve())

_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01"
    b"\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
)


@pytest.fixture
def client(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    if _SVC_DIR not in sys.path:
        sys.path.insert(0, _SVC_DIR)
    sys.modules.pop("app", None)
    app_mod = importlib.import_module("app")

    # 정비 상태는 이제 워커 간 공유 파일이다 (모듈 전역 dict 아님).
    # 테스트마다 격리하지 않으면 앞 테스트가 남긴 active 상태가 다음 테스트로 샌다.
    monkeypatch.setattr(app_mod, "_MAINTENANCE_STATE_PATH", str(tmp_path / "maintenance.json"))

    state = {"load_calls": 0}

    def _fake_load():
        state["load_calls"] += 1
        app_mod._processor = object()
        app_mod._model = object()
        app_mod._model_loaded_at = 1.0
        app_mod._load_error = None

    monkeypatch.setattr(app_mod, "_load_model", _fake_load)
    monkeypatch.setattr(app_mod, "_run_segmentation", lambda *a, **k: ([], {}))
    monkeypatch.setattr(app_mod, "_reset_gpu_peak_memory", lambda: None)
    monkeypatch.setattr(app_mod, "_gpu_peak_memory_gb", lambda: None)

    with TestClient(app_mod.app) as c:
        c.__dict__["_test_mod"] = app_mod
        c.__dict__["_test_state"] = state
        yield c


def _segment(client):
    return client.post(
        "/segment",
        files={"file": ("i.png", _PNG, "image/png")},
        data={"prompts_json": '["fire"]'},
    )


def test_segment_503_under_maintenance(client):
    assert client.post("/maintenance/enter", data={"owner_run_id": "r1"}).status_code == 200
    r = _segment(client)
    assert r.status_code == 503
    assert r.json()["detail"] == "gpu_under_maintenance"


def test_warmup_503_under_maintenance(client):
    client.post("/maintenance/enter", data={"owner_run_id": "r1"})
    assert client.post("/warmup").status_code == 503


def test_lazy_reload_refused_under_maintenance(client):
    client.post("/unload")
    calls_before = client.__dict__["_test_state"]["load_calls"]
    client.post("/maintenance/enter", data={"owner_run_id": "r1"})
    assert _segment(client).status_code == 503
    assert client.__dict__["_test_state"]["load_calls"] == calls_before


def test_exit_restores_normal_operation(client):
    client.post("/maintenance/enter", data={"owner_run_id": "r1"})
    assert _segment(client).status_code == 503
    assert client.post("/maintenance/exit").status_code == 200
    assert client.post("/warmup").status_code == 200
    assert _segment(client).status_code == 200


def test_normal_operation_unaffected_when_clear(client):
    assert _segment(client).status_code == 200


def test_gate_honors_state_written_by_another_worker(client):
    """uvicorn --workers N 은 fork 된 독립 프로세스다.

    `/maintenance/enter` 는 그중 한 워커에만 도달하므로, 상태가 모듈 전역 dict 이면
    나머지 워커는 계속 /segment 를 서빙한다 (drain 불능). 여기서는 '다른 워커가 쓴'
    상황을 파일에 직접 기록해 재현하고, 이 프로세스의 게이트가 그것을 존중하는지 본다.
    """
    app_mod = client.__dict__["_test_mod"]
    assert _segment(client).status_code == 200  # 아직 정비 아님

    with open(app_mod._MAINTENANCE_STATE_PATH, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "active": True,
                "owner_run_id": "other-worker",
                "entered_at": time.time(),
                "heartbeat_at": time.time(),
                "ttl_seconds": 1800,
                "note": None,
            },
            fh,
        )

    assert _segment(client).status_code == 503
    assert client.post("/warmup").status_code == 503


def test_expired_ttl_auto_releases_the_gate(client):
    """TTL 은 저장만 되고 검사되지 않아 장식이었다 — `/maintenance/exit` 를 잊으면 무기한 503.

    heartbeat 가 TTL 을 넘기면 게이트가 스스로 풀려야 한다.
    """
    assert client.post("/maintenance/enter", data={"owner_run_id": "r1", "ttl_seconds": "60"}).status_code == 200
    assert _segment(client).status_code == 503

    app_mod = client.__dict__["_test_mod"]
    with open(app_mod._MAINTENANCE_STATE_PATH, encoding="utf-8") as fh:
        state = json.load(fh)
    state["heartbeat_at"] = time.time() - 61  # TTL 60s 를 막 넘김
    with open(app_mod._MAINTENANCE_STATE_PATH, "w", encoding="utf-8") as fh:
        json.dump(state, fh)

    assert client.get("/maintenance/status").json()["active"] is False
    assert _segment(client).status_code == 200


def test_heartbeat_extends_ttl_but_does_not_revive_a_released_gate(client):
    assert client.post("/maintenance/enter", data={"owner_run_id": "r1", "ttl_seconds": "60"}).status_code == 200
    assert client.post("/maintenance/heartbeat").json()["active"] is True

    client.post("/maintenance/exit")
    assert client.post("/maintenance/heartbeat").json()["active"] is False
    assert _segment(client).status_code == 200


def test_corrupt_state_file_fails_open(client):
    """게이트가 닫히면 SAM3 전면 503 이다. 파일 손상으로 서빙을 죽이면 안 된다."""
    app_mod = client.__dict__["_test_mod"]
    with open(app_mod._MAINTENANCE_STATE_PATH, "w", encoding="utf-8") as fh:
        fh.write("{not json")
    assert _segment(client).status_code == 200
