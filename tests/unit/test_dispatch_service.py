from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("dagster", exc_type=ImportError)

from vlm_pipeline.defs.dispatch.service import (  # noqa: E402
    DispatchIngressRequest,
    PreparedDispatchRequest,
    build_dispatch_pipeline_rows,
    build_dispatch_run_request,
    process_dispatch_ingress_request,
    resolve_dispatch_applied_params,
)
from vlm_pipeline.lib.yolo_thresholds import YOLO_CLASS_CONFIDENCE_THRESHOLDS  # noqa: E402


def _prepared_request() -> PreparedDispatchRequest:
    return PreparedDispatchRequest(
        request_id="req-1",
        folder_name="tmp_data_2",
        incoming_folder_path=Path("/nas/incoming/tmp_data_2"),
        run_mode="both",
        outputs_str="timestamp_video,captioning_video,bbox",
        labeling_method=["timestamp_video", "captioning_video", "bbox"],
        categories=["smoke", "weapon"],
        classes=["smoke", "knife", "gun"],
        image_profile="current",
        requested_by="tester",
        requested_at="2026-03-26T12:00:00",
        archive_only=False,
        storage_outputs="timestamp_video, captioning_video, bbox",
        storage_labeling_method="timestamp_video, captioning_video, bbox",
        storage_categories="smoke, weapon",
        storage_classes="smoke, knife, gun",
    )


def test_resolve_dispatch_applied_params_ignores_max_frames() -> None:
    applied = resolve_dispatch_applied_params(
        {
            "max_frames_per_video": 99,
            "jpeg_quality": 88,
            "confidence_threshold": 0.33,
            "iou_threshold": 0.55,
        },
        {
            "bbox": {
                "default_max_frames": 24,
                "default_jpeg_quality": 90,
                "default_confidence": 0.25,
                "default_iou": 0.45,
            }
        },
    )

    assert applied.max_frames_per_video is None
    assert applied.jpeg_quality == 88
    assert applied.confidence_threshold == 0.33
    assert applied.iou_threshold == 0.55


def test_build_dispatch_pipeline_rows_omits_max_frames_from_applied_params() -> None:
    prepared = _prepared_request()
    applied = resolve_dispatch_applied_params(
        {"jpeg_quality": 92, "confidence_threshold": 0.3, "iou_threshold": 0.5},
        {"bbox": {}},
    )

    rows = build_dispatch_pipeline_rows(
        prepared,
        model_defaults={"bbox": {"model_name": "yolo", "model_version": "v1"}},
        applied_params=applied,
    )

    frame_row = next(row for row in rows if row["step_name"] == "frame_extract")
    payload = json.loads(frame_row["applied_params"])
    assert "max_frames" not in payload
    assert payload["jpeg_quality"] == 92
    # per-class thresholds derive from the canonical override table (lib/yolo_thresholds.py) —
    # values are tuned over time, so assert against the table rather than a frozen snapshot.
    expected_class_thresholds = {cls: YOLO_CLASS_CONFIDENCE_THRESHOLDS[cls] for cls in prepared.classes}
    assert payload["class_confidence_thresholds"] == expected_class_thresholds
    assert payload["effective_request_confidence_threshold"] == min(
        applied.confidence_threshold, *expected_class_thresholds.values()
    )


def test_build_dispatch_run_request_does_not_emit_max_frames_tag() -> None:
    prepared = _prepared_request()
    applied = resolve_dispatch_applied_params(
        {"max_frames_per_video": 99, "jpeg_quality": 91},
        {"bbox": {"default_jpeg_quality": 90}},
    )

    run_request = build_dispatch_run_request(
        prepared,
        manifest_path=Path("/tmp/dispatch_manifest.json"),
        applied_params=applied,
    )

    assert "max_frames_per_video" not in run_request.tags
    assert run_request.tags["jpeg_quality"] == "91"


class _FakeLog:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str) -> None:
        self.messages.append(message)


class _FakeContext:
    def __init__(self) -> None:
        self.log = _FakeLog()


class _FakeDB:
    def __init__(self) -> None:
        self.dispatch_status: str | None = None
        self.inflight_rows: list[dict] = []
        self.closed_requests: list[str] = []
        self.inserted_request: dict | None = None
        self.inserted_pipeline_rows: list[dict] | None = None

    def get_in_flight_dispatch_requests(self, _folder_name: str) -> list[dict]:
        return list(self.inflight_rows)

    def get_dispatch_request_status(self, _request_id: str) -> str | None:
        return self.dispatch_status

    def close_dispatch_request(self, request_id: str, *, status: str, error_message: str) -> None:
        self.closed_requests.append(f"{request_id}:{status}:{error_message}")

    def get_active_staging_model_configs(self, _outputs: list[str]) -> dict[str, dict]:
        return {"bbox": {}}

    def insert_dispatch_request(self, row: dict) -> None:
        self.inserted_request = row

    def insert_dispatch_pipeline_runs(self, rows: list[dict]) -> None:
        self.inserted_pipeline_rows = rows


def _config_for(tmp_path: Path):
    cfg = type("Cfg", (), {})()
    cfg.incoming_dir = str(tmp_path / "incoming")
    cfg.archive_dir = str(tmp_path / "archive")
    cfg.manifest_dir = str(tmp_path / "manifests")
    return cfg


def test_process_dispatch_ingress_request_rejects_duplicate(tmp_path: Path) -> None:
    (tmp_path / "incoming" / "tmp_data_2").mkdir(parents=True)
    db = _FakeDB()
    db.dispatch_status = "running"

    result = process_dispatch_ingress_request(
        _FakeContext(),
        db_resource=db,
        config=_config_for(tmp_path),
        ingress_request=DispatchIngressRequest(
            payload={
                "request_id": "req_dup",
                "folder_name": "tmp_data_2",
                "labeling_method": ["bbox"],
            },
            fallback_request_id="req_dup",
            duplicate_policy="reject",
            in_flight_policy="reject",
        ),
    )

    assert result.status == "rejected"
    assert result.reason == "duplicate_request_id"


def test_process_dispatch_ingress_request_duplicate_noop(tmp_path: Path) -> None:
    (tmp_path / "incoming" / "tmp_data_2").mkdir(parents=True)
    db = _FakeDB()
    db.dispatch_status = "running"

    result = process_dispatch_ingress_request(
        _FakeContext(),
        db_resource=db,
        config=_config_for(tmp_path),
        ingress_request=DispatchIngressRequest(
            payload={
                "request_id": "req_dup",
                "folder_name": "tmp_data_2",
                "labeling_method": ["bbox"],
            },
            fallback_request_id="req_dup",
            duplicate_policy="accept_noop",
            in_flight_policy="defer",
        ),
    )

    assert result.status == "duplicate_noop"
    assert result.reason == "duplicate_request_id_noop"


def test_process_dispatch_ingress_request_deferred_when_folder_in_flight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "incoming" / "tmp_data_2").mkdir(parents=True)
    db = _FakeDB()
    db.inflight_rows = [{"request_id": "stale"}]
    monkeypatch.setattr(
        "vlm_pipeline.defs.dispatch.service.has_active_dispatch_run",
        lambda context, folder_name: folder_name == "tmp_data_2",
    )

    result = process_dispatch_ingress_request(
        _FakeContext(),
        db_resource=db,
        config=_config_for(tmp_path),
        ingress_request=DispatchIngressRequest(
            payload={
                "request_id": "req_busy",
                "folder_name": "tmp_data_2",
                "labeling_method": ["bbox"],
            },
            fallback_request_id="req_busy",
            duplicate_policy="accept_noop",
            in_flight_policy="defer",
        ),
    )

    assert result.status == "deferred"
    assert result.reason == "folder_dispatch_in_flight"


def test_process_dispatch_ingress_request_success_creates_run_request(tmp_path: Path) -> None:
    (tmp_path / "incoming" / "tmp_data_2").mkdir(parents=True)
    db = _FakeDB()

    result = process_dispatch_ingress_request(
        _FakeContext(),
        db_resource=db,
        config=_config_for(tmp_path),
        ingress_request=DispatchIngressRequest(
            payload={
                "request_id": "req_ok",
                "folder_name": "tmp_data_2",
                "labeling_method": ["bbox"],
            },
            fallback_request_id="req_ok",
            duplicate_policy="reject",
            in_flight_policy="reject",
        ),
    )

    assert result.status == "run_request"
    assert result.run_request is not None
    assert result.run_request.job_name == "dispatch_stage_job"
    assert result.prepared is not None
    assert result.prepared.request_id == "req_ok"
    assert db.inserted_request is not None


def test_process_dispatch_ingress_request_ignores_prompts_in_payload(tmp_path: Path) -> None:
    (tmp_path / "incoming" / "tmp_data_2").mkdir(parents=True)
    db = _FakeDB()

    result = process_dispatch_ingress_request(
        _FakeContext(),
        db_resource=db,
        config=_config_for(tmp_path),
        ingress_request=DispatchIngressRequest(
            payload={
                "request_id": "req_prompt",
                "folder_name": "tmp_data_2",
                "labeling_method": ["timestamp"],
                "prompts": ["  Detect smoke  ", "Detect smoke", "Detect violence"],
            },
            fallback_request_id="req_prompt",
            duplicate_policy="reject",
            in_flight_policy="reject",
        ),
    )

    assert result.status == "run_request"
    assert result.prepared is not None
    assert result.run_request is not None

    manifest_dir = tmp_path / "manifests" / "dispatch"
    manifests = sorted(manifest_dir.glob("*.json"))
    assert len(manifests) == 1
    payload = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert "prompts" not in payload


def test_process_dispatch_ingress_request_allows_timestamp_without_prompts(tmp_path: Path) -> None:
    (tmp_path / "incoming" / "tmp_data_2").mkdir(parents=True)
    db = _FakeDB()

    result = process_dispatch_ingress_request(
        _FakeContext(),
        db_resource=db,
        config=_config_for(tmp_path),
        ingress_request=DispatchIngressRequest(
            payload={
                "request_id": "req_prompt_required",
                "folder_name": "tmp_data_2",
                "labeling_method": ["timestamp"],
            },
            fallback_request_id="req_prompt_required",
            duplicate_policy="reject",
            in_flight_policy="reject",
        ),
    )

    assert result.status == "run_request"
    assert result.reason == "run_request_created"
    assert result.run_request is not None


def test_process_dispatch_ingress_request_ignores_bbox_only_prompts(tmp_path: Path) -> None:
    (tmp_path / "incoming" / "tmp_data_2").mkdir(parents=True)
    db = _FakeDB()

    result = process_dispatch_ingress_request(
        _FakeContext(),
        db_resource=db,
        config=_config_for(tmp_path),
        ingress_request=DispatchIngressRequest(
            payload={
                "request_id": "req_bbox_prompt",
                "folder_name": "tmp_data_2",
                "labeling_method": ["bbox"],
                "prompts": ["Detect smoke"],
            },
            fallback_request_id="req_bbox_prompt",
            duplicate_policy="reject",
            in_flight_policy="reject",
        ),
    )

    assert result.status == "run_request"
    assert result.prepared is not None

    manifest_dir = tmp_path / "manifests" / "dispatch"
    manifests = sorted(manifest_dir.glob("*.json"))
    assert len(manifests) == 1
    payload = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert "prompts" not in payload


def test_process_dispatch_ingress_request_skip_routes_to_ingest_job(tmp_path: Path) -> None:
    (tmp_path / "incoming" / "tmp_data_2").mkdir(parents=True)
    db = _FakeDB()

    result = process_dispatch_ingress_request(
        _FakeContext(),
        db_resource=db,
        config=_config_for(tmp_path),
        ingress_request=DispatchIngressRequest(
            payload={
                "request_id": "req_skip",
                "folder_name": "tmp_data_2",
                "labeling_method": ["SKIP"],
                "categories": ["smoke"],
                "classes": ["smoke"],
            },
            fallback_request_id="req_skip",
            duplicate_policy="reject",
            in_flight_policy="reject",
        ),
    )

    assert result.status == "run_request"
    assert result.prepared is not None
    assert result.prepared.archive_only is True
    assert result.prepared.labeling_method == ["skip"]
    assert result.run_request is not None
    assert result.run_request.job_name == "ingest_job"
    assert result.run_request.tags["dispatch_archive_only"] == "true"
