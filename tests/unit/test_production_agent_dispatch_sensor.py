from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("dagster")

from dagster import SkipReason, build_sensor_context  # noqa: E402

import vlm_pipeline.defs.dispatch.production_agent_sensor as sensor_mod  # noqa: E402
from vlm_pipeline.defs.dispatch.service import prepare_dispatch_request  # noqa: E402
from vlm_pipeline.defs.dispatch.production_agent_sensor import (  # noqa: E402
    _build_agent_ack_payload as build_agent_ack_payload,
    _normalize_agent_dispatch_request as normalize_agent_dispatch_request,
    _should_wait_for_dispatch_params as should_wait_for_dispatch_params,
)
from vlm_pipeline.lib.env_utils import CATEGORY_TO_CLASSES  # noqa: E402
from vlm_pipeline.resources.runtime_settings import (  # noqa: E402
    ProductionAgentPollingSettings,
    load_production_agent_polling_settings,
)


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _FetchSession:
    def __init__(self) -> None:
        self.last_url = ""
        self.last_params: dict[str, Any] | None = None
        self.last_timeout: tuple[int, int] | None = None

    def get(self, url: str, *, params: dict[str, Any], timeout: tuple[int, int]) -> _FakeResponse:
        self.last_url = url
        self.last_params = params
        self.last_timeout = timeout
        return _FakeResponse({"items": []})


class _AckSession:
    def __init__(self) -> None:
        self.last_url = ""
        self.last_json: dict[str, Any] | None = None
        self.last_timeout: tuple[int, int] | None = None

    def post(self, url: str, *, json: dict[str, Any], timeout: tuple[int, int]) -> _FakeResponse:
        self.last_url = url
        self.last_json = json
        self.last_timeout = timeout
        return _FakeResponse({})


class _FakeLog:
    def warning(self, message: str) -> None:
        _ = message


class _FakeContext:
    def __init__(self) -> None:
        self.log = _FakeLog()


class _FakeDB:
    def ensure_runtime_schema(self) -> None:
        return None

    def ensure_dispatch_tracking_tables(self) -> None:
        return None

    def get_dispatch_request_status(self, request_id: str) -> str | None:
        return None


def test_normalize_agent_dispatch_request_maps_source_unit_name_and_defaults() -> None:
    normalized = normalize_agent_dispatch_request(
        {
            "request_id": "req_prod_1",
            "source_unit_name": "tmp_data_2",
            "labeling_method": ["bbox"],
        },
        delivery_id="dlv_1",
    )

    assert normalized["request_id"] == "req_prod_1"
    assert normalized["folder_name"] == "tmp_data_2"
    assert normalized["image_profile"] == "current"
    assert normalized["requested_by"] == "dispatch-agent"


def test_normalize_agent_dispatch_request_generates_request_id_from_delivery() -> None:
    normalized = normalize_agent_dispatch_request(
        {
            "source_unit_name": "tmp_data_2",
            "labeling_method": ["bbox"],
            "categories": ["weapon"],
        },
        delivery_id="dlv 2026/03/26",
    )

    # sanitize_path_component() strips disallowed chars (e.g. "/") rather than
    # substituting them with "_" — see tests/unit/test_lib_sanitizer.py::test_special_chars_removed.
    assert normalized["request_id"] == "agent_dlv_20260326"


def test_normalize_agent_dispatch_request_rejects_missing_source_unit_name() -> None:
    with pytest.raises(ValueError, match="missing_source_unit_name"):
        normalize_agent_dispatch_request(
            {"labeling_method": ["bbox"]},
            delivery_id="dlv_1",
        )


def test_agent_payload_flows_into_existing_dispatch_preparation() -> None:
    normalized = normalize_agent_dispatch_request(
        {
            "source_unit_name": "tmp_data_2",
            "labeling_method": ["bbox"],
            "categories": ["smoke", "falldown"],
        },
        delivery_id="dlv_1",
    )

    prepared = prepare_dispatch_request(normalized, incoming_dir=Path("/nas/incoming"))

    expected_classes: list[str] = []
    for category in ("smoke", "falldown"):
        for phrase in CATEGORY_TO_CLASSES[category]:
            if phrase not in expected_classes:
                expected_classes.append(phrase)

    assert prepared.folder_name == "tmp_data_2"
    assert prepared.labeling_method == ["bbox"]
    assert prepared.classes == expected_classes


def test_build_agent_ack_payload_uses_expected_shape() -> None:
    payload = build_agent_ack_payload(
        delivery_id="dlv_1",
        status="accepted",
        request_id="req_1",
        message="ok",
    )

    assert payload == {
        "delivery_id": "dlv_1",
        "status": "accepted",
        "request_id": "req_1",
        "message": "ok",
    }


def test_should_wait_for_dispatch_params_when_all_dispatch_fields_are_empty() -> None:
    assert should_wait_for_dispatch_params(
        {
            "source_unit_name": "tmp_data_2",
            "request_id": "req_waiting",
            "labeling_method": [],
            "outputs": [],
            "run_mode": "",
            "categories": [],
            "classes": [],
        }
    )


def test_should_not_wait_when_any_dispatch_field_has_value() -> None:
    assert not should_wait_for_dispatch_params(
        {
            "source_unit_name": "tmp_data_2",
            "request_id": "req_ready",
            "labeling_method": ["bbox"],
            "categories": [],
            "classes": [],
        }
    )


def test_should_not_wait_for_skip_labeling_method() -> None:
    assert not should_wait_for_dispatch_params(
        {
            "source_unit_name": "tmp_data_2",
            "request_id": "req_skip",
            "labeling_method": ["skip"],
            "categories": [],
            "classes": [],
        }
    )


def test_fetch_pending_dispatch_items_uses_fixed_production_path() -> None:
    session = _FetchSession()
    items = sensor_mod._fetch_pending_dispatch_items(
        session,
        base_url="http://host.docker.internal:8080",
        limit=7,
        timeout=(3, 10),
    )

    assert items == []
    assert session.last_url == "http://host.docker.internal:8080/api/production/dispatch/pending"
    assert session.last_params == {"limit": 7}
    assert session.last_timeout == (3, 10)


def test_post_dispatch_ack_uses_fixed_production_path() -> None:
    session = _AckSession()
    sensor_mod._post_dispatch_ack(
        _FakeContext(),
        session,
        base_url="http://host.docker.internal:8080",
        timeout=(3, 10),
        delivery_id="dlv_1",
        status="accepted",
        request_id="req_1",
        message="ok",
    )

    assert session.last_url == "http://host.docker.internal:8080/api/production/dispatch/ack"
    assert session.last_json == {
        "delivery_id": "dlv_1",
        "status": "accepted",
        "request_id": "req_1",
        "message": "ok",
    }
    assert session.last_timeout == (3, 10)


def test_load_production_agent_polling_settings_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROD_AGENT_POLLING_ENABLED", "true")
    monkeypatch.setenv("PROD_AGENT_BASE_URL", "http://host.docker.internal:8080/")
    monkeypatch.setenv("PROD_AGENT_POLL_LIMIT", "11")
    monkeypatch.setenv("PROD_AGENT_CONNECT_TIMEOUT_SEC", "4")
    monkeypatch.setenv("PROD_AGENT_READ_TIMEOUT_SEC", "12")
    monkeypatch.setenv("PROD_AGENT_SENSOR_INTERVAL_SEC", "40")

    settings = load_production_agent_polling_settings()

    assert settings.enabled is True
    assert settings.base_url == "http://host.docker.internal:8080"
    assert settings.poll_limit == 11
    assert settings.connect_timeout_sec == 4
    assert settings.read_timeout_sec == 12
    assert settings.interval_sec == 40


def test_load_production_agent_polling_settings_uses_default_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PROD_AGENT_POLLING_ENABLED", raising=False)
    monkeypatch.delenv("PROD_AGENT_BASE_URL", raising=False)
    monkeypatch.delenv("PROD_AGENT_POLL_LIMIT", raising=False)
    monkeypatch.delenv("PROD_AGENT_CONNECT_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("PROD_AGENT_READ_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("PROD_AGENT_SENSOR_INTERVAL_SEC", raising=False)

    settings = load_production_agent_polling_settings()

    assert settings.enabled is False
    assert settings.base_url == "http://host.docker.internal:8080"
    assert settings.poll_limit == 20
    assert settings.connect_timeout_sec == 3
    assert settings.read_timeout_sec == 10
    assert settings.interval_sec == 30


def test_production_agent_dispatch_sensor_skips_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sensor_mod,
        "load_production_agent_polling_settings",
        lambda: ProductionAgentPollingSettings(
            enabled=False,
            base_url="http://host.docker.internal:8080",
            poll_limit=20,
            connect_timeout_sec=3,
            read_timeout_sec=10,
            interval_sec=30,
        ),
    )

    context = build_sensor_context(resources={"db": _FakeDB()})
    results = list(sensor_mod._production_agent_dispatch_sensor_fn(context))

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)


class _FakeSession:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_production_agent_dispatch_sensor_accepts_waiting_request_without_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    incoming_dir = tmp_path / "incoming"
    (incoming_dir / "tmp_data_2").mkdir(parents=True)

    monkeypatch.setenv("INCOMING_DIR", str(incoming_dir))
    # PipelineConfig() is constructed unconditionally inside the sensor fn and requires
    # non-default MinIO credentials (config.py reject_default_minio_credentials validator).
    monkeypatch.setenv("MINIO_ACCESS_KEY", "test-access-key")
    monkeypatch.setenv("MINIO_SECRET_KEY", "test-secret-key")

    monkeypatch.setattr(
        sensor_mod,
        "load_production_agent_polling_settings",
        lambda: ProductionAgentPollingSettings(
            enabled=True,
            base_url="http://host.docker.internal:8080",
            poll_limit=20,
            connect_timeout_sec=3,
            read_timeout_sec=10,
            interval_sec=30,
        ),
    )
    monkeypatch.setattr(
        sensor_mod,
        "_fetch_pending_dispatch_items",
        lambda session, **kwargs: [
            {
                "delivery_id": "dlv_wait_1",
                "request": {
                    "request_id": "req_waiting_1",
                    "source_unit_name": "tmp_data_2",
                    "labeling_method": [],
                    "outputs": [],
                    "run_mode": "",
                    "categories": [],
                    "classes": [],
                },
            }
        ],
    )

    ack_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        sensor_mod,
        "_post_dispatch_ack",
        lambda context, session, **kwargs: ack_calls.append(kwargs),
    )
    monkeypatch.setattr(
        sensor_mod.requests,
        "Session",
        _FakeSession,
    )
    monkeypatch.setattr(
        sensor_mod,
        "process_dispatch_ingress_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("process_dispatch_ingress_request should not be called")
        ),
    )

    context = build_sensor_context(resources={"db": _FakeDB()})

    results = list(sensor_mod._production_agent_dispatch_sensor_fn(context))

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    assert ack_calls == [
        {
            "base_url": "http://host.docker.internal:8080",
            "timeout": (3, 10),
            "delivery_id": "dlv_wait_1",
            "status": "accepted",
            "request_id": "req_waiting_1",
            "message": "waiting_for_dispatch_params",
        }
    ]
