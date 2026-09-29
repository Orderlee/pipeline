from __future__ import annotations

import copy
import io
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from PIL import Image


ROOT = Path(__file__).resolve().parents[2]
GENAI = ROOT / "docker" / "genai"
if str(GENAI) not in sys.path:
    sys.path.insert(0, str(GENAI))

import app as genai_app  # noqa: E402


def _client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GENAI_AUTH_DISABLED", "true")
    monkeypatch.setenv("GENAI_ENGINES_ENABLED", "comfy_local")
    monkeypatch.setenv("COMFYUI_CONTRACT_ROOT", str(ROOT / "docker" / "comfyui"))
    return TestClient(genai_app.create_app())


def test_comfy_node_workspace_renders_repository_graphs(monkeypatch):
    response = _client(monkeypatch).get("/genai/comfy-nodes", auth=("tester", "unused"))

    assert response.status_code == 200
    assert "Native ComfyUI Canvas" in response.text
    assert "flux2-klein-4b-edit-v1" in response.text
    assert "sdxl-inpaint-cctv-v1" in response.text
    assert "ReferenceLatent" in response.text
    assert "VAEEncodeForInpaint" in response.text
    assert 'src="/genai/comfy-native/"' in response.text
    assert "app.loadApiJson" in response.text
    assert "restoreApiLinks" in response.text
    assert "source.connect" in response.text
    assert "fitToBounds" in response.text
    assert "공식 ComfyUI frontend" in response.text
    assert 'id="nc-bulk-open"' in response.text
    assert 'id="nc-bulk-files" multiple' in response.text
    assert "M images × 1 node prompt" in response.text
    assert "기본 Run = Load Image 1장" in response.text
    assert "attachFirstFileToCanvas" in response.text
    assert "installNativeRunGuard" in response.text


def test_comfy_node_workspace_rejects_unknown_workflow(monkeypatch):
    response = _client(monkeypatch).get(
        "/genai/comfy-nodes?workflow_id=../../arbitrary",
        auth=("tester", "unused"),
    )

    assert response.status_code == 404


def test_comfy_node_contract_loader_is_fixed_allowlist(monkeypatch):
    monkeypatch.setenv("COMFYUI_CONTRACT_ROOT", str(ROOT / "docker" / "comfyui"))

    contracts = genai_app._load_comfy_node_contracts()

    assert set(contracts) == {
        "flux2-klein-4b-edit-v1",
        "sdxl-inpaint-cctv-v1",
    }
    assert all("prompt" in contract for contract in contracts.values())


def test_comfy_node_contract_loader_respects_operator_subset(monkeypatch):
    monkeypatch.setenv("COMFYUI_CONTRACT_ROOT", str(ROOT / "docker" / "comfyui"))
    monkeypatch.setenv("COMFYUI_ALLOWED_WORKFLOWS", "sdxl-inpaint-cctv-v1,../../unsafe")

    contracts = genai_app._load_comfy_node_contracts()

    assert set(contracts) == {"sdxl-inpaint-cctv-v1"}


def test_native_prompt_gate_accepts_scalar_edits_only(monkeypatch):
    monkeypatch.setenv("COMFYUI_CONTRACT_ROOT", str(ROOT / "docker" / "comfyui"))
    contracts = genai_app._load_comfy_node_contracts()
    graph = copy.deepcopy(contracts["flux2-klein-4b-edit-v1"]["prompt"])
    graph["1"]["inputs"]["image"] = "native-upload.png"
    graph["6"]["inputs"]["text"] = "add one fallen person"
    graph["14"]["inputs"]["noise_seed"] = 123
    for node in graph.values():
        node["_meta"] = {"title": node["class_type"]}

    workflow_id, values = genai_app._match_comfy_native_prompt(graph)

    assert workflow_id == "flux2-klein-4b-edit-v1"
    assert values["source_image"] == "native-upload.png"
    assert values["prompt"] == "add one fallen person"
    assert values["seed"] == 123


def test_native_prompt_gate_rejects_model_or_structure_change(monkeypatch):
    monkeypatch.setenv("COMFYUI_CONTRACT_ROOT", str(ROOT / "docker" / "comfyui"))
    contract = genai_app._load_comfy_node_contracts()["flux2-klein-4b-edit-v1"]

    changed_model = copy.deepcopy(contract["prompt"])
    changed_model["2"]["inputs"]["unet_name"] = "unapproved.safetensors"
    with pytest.raises(ValueError, match="does not match"):
        genai_app._match_comfy_native_prompt(changed_model)

    added_node = copy.deepcopy(contract["prompt"])
    added_node["999"] = {"class_type": "SaveImage", "inputs": {}}
    with pytest.raises(ValueError, match="does not match"):
        genai_app._match_comfy_native_prompt(added_node)


def test_missing_native_load_image_has_actionable_error(monkeypatch):
    class MissingResponse:
        status_code = 404

    monkeypatch.setattr(genai_app.requests, "get", lambda *args, **kwargs: MissingResponse())

    with pytest.raises(ValueError, match="M×1 Batch"):
        genai_app._fetch_comfy_input("source.png")


def test_comfy_proxy_drops_non_latin1_optional_header():
    headers = {
        "content-type": "image/jpeg",
        "cache-control": "public, max-age=60",
        "content-disposition": 'inline; filename="site-h.jpg"',
        "x-internal": "not-forwarded",
    }

    assert genai_app._safe_comfy_proxy_headers(headers) == {
        "content-type": "image/jpeg",
        "cache-control": "public, max-age=60",
    }


@pytest.mark.parametrize(
    "path",
    (
        "/genai/comfy-native/api/prompt",
        "/genai/comfy-native/api/prompt/",
        "/genai/comfy-native/prompt",
    ),
)
def test_nested_native_prompt_route_is_gated_before_proxy(monkeypatch, path):
    monkeypatch.setenv("GENAI_AUTH_DISABLED", "true")
    monkeypatch.setenv("GENAI_ENGINES_ENABLED", "kling,veo")
    monkeypatch.setenv("COMFYUI_CONTRACT_ROOT", str(ROOT / "docker" / "comfyui"))

    response = TestClient(genai_app.create_app()).post(
        path,
        auth=("tester", "unused"),
        json={"prompt": {}},
    )

    assert response.status_code == 503
    assert response.json()["error"]["type"] == "comfy_local_disabled"


def _png(*, value: int = 127, size: tuple[int, int] = (4, 4)) -> bytes:
    buffer = io.BytesIO()
    Image.new("L", size, color=value).save(buffer, format="PNG")
    return buffer.getvalue()


def _unlimited_usage(_user: str) -> dict:
    return {
        "daily_batches": {"remaining": None},
        "daily_bytes": {"remaining": None},
    }


def test_comfy_bulk_page_exposes_cartesian_and_mask_controls(monkeypatch):
    response = _client(monkeypatch).get(
        "/genai/bulk?engine=comfy_local",
        auth=("tester", "unused"),
    )

    assert response.status_code == 200
    assert 'id="bulk-eng-comfy_local"' in response.text
    assert 'id="bulk-mode-cartesian"' in response.text
    assert 'id="bulk-mode-cartesian" value="cartesian"\n                 checked' in response.text
    assert "이미지 N장×프롬프트 1개" in response.text
    assert 'name="mask_files"' in response.text
    assert 'id="bulk-comfy-workflow"' in response.text
    assert "seed: 0은 job마다 무작위" in response.text
    assert "프롬프트가 1개면 선택한 이미지 전체에 자동 적용" in response.text


def test_comfy_number_inputs_have_explicit_dark_theme_colors():
    css = (GENAI / "static" / "studio.css").read_text(encoding="utf-8")

    selector = '.gs-engine-opt[data-only-engine="comfy_local"] input[type="number"]'
    assert selector in css
    rule = css.split(selector, maxsplit=1)[1].split("}", maxsplit=1)[0]
    assert "background: var(--gs-card)" in rule
    assert "color: var(--gs-text)" in rule
    assert "-webkit-text-fill-color: var(--gs-text)" in rule


def test_non_comfy_bulk_keeps_safe_paired_default(monkeypatch):
    monkeypatch.setenv("GENAI_AUTH_DISABLED", "true")
    monkeypatch.setenv("GENAI_ENGINES_ENABLED", "kling,comfy_local")
    response = TestClient(genai_app.create_app()).get(
        "/genai/bulk?engine=kling",
        auth=("tester", "unused"),
    )

    assert response.status_code == 200
    assert 'id="bulk-mode-paired" value="paired"\n                 checked' in response.text


def test_comfy_flux_bulk_cartesian_builds_n_by_m_jobs(monkeypatch):
    client = _client(monkeypatch)
    calls: list[dict] = []

    def fake_submit_batch(**kwargs):
        calls.append(kwargs)
        return {"batch_id": f"batch-{len(calls)}"}

    monkeypatch.setattr(genai_app, "submit_batch", fake_submit_batch)
    monkeypatch.setattr(genai_app.limits, "usage", _unlimited_usage)
    source_a = _png(value=40)
    source_b = _png(value=80)

    response = client.post(
        "/genai/bulk-batches",
        auth=("tester", "unused"),
        headers={"accept": "application/json"},
        data={
            "engine": "comfy_local",
            "prompts_text": "fallen person\nsmall fire\nthin smoke",
            "idempotency_token": "flux-cartesian-unique-001",
            "pair_mode": "cartesian",
            "text_only": "false",
            "workflow_id": "flux2-klein-4b-edit-v1",
            "seed": "0",
            "steps": "4",
        },
        files=[
            ("files", ("camera-a.png", source_a, "image/png")),
            ("files", ("camera-b.png", source_b, "image/png")),
        ],
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["total_planned"] == 3
    assert payload["total_jobs"] == 6
    assert payload["pair_mode"] == "cartesian"
    assert len(calls) == 3
    assert [len(call["files"]) for call in calls] == [2, 2, 2]
    assert all(call["control_files"] is None for call in calls)
    assert [call["prompt"] for call in calls] == ["fallen person", "small fire", "thin smoke"]
    assert all(call["options"]["workflow_id"] == "flux2-klein-4b-edit-v1" for call in calls)
    assert len({call["options"]["bulk_group_id"] for call in calls}) == 1


def test_comfy_flux_bulk_broadcasts_one_prompt_to_multiple_images(monkeypatch):
    client = _client(monkeypatch)
    calls: list[dict] = []

    def fake_submit_batch(**kwargs):
        calls.append(kwargs)
        return {"batch_id": f"batch-{len(calls)}"}

    monkeypatch.setattr(genai_app, "submit_batch", fake_submit_batch)
    monkeypatch.setattr(genai_app.limits, "usage", _unlimited_usage)

    response = client.post(
        "/genai/bulk-batches",
        auth=("tester", "unused"),
        headers={"accept": "application/json"},
        data={
            "engine": "comfy_local",
            "prompts_text": "one fallen person on the station floor",
            "idempotency_token": "flux-broadcast-unique-001",
            # A stale browser may retain paired; the server must still broadcast M×1.
            "pair_mode": "paired",
            "text_only": "false",
            "workflow_id": "flux2-klein-4b-edit-v1",
            "seed": "0",
            "steps": "4",
        },
        files=[
            ("files", ("camera-a.png", _png(value=40), "image/png")),
            ("files", ("camera-b.png", _png(value=80), "image/png")),
            ("files", ("camera-c.png", _png(value=120), "image/png")),
        ],
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["pair_mode"] == "cartesian"
    assert payload["total_planned"] == 1
    assert payload["total_jobs"] == 3
    assert len(calls) == 1
    assert calls[0]["prompt"] == "one fallen person on the station floor"
    assert [name for name, _blob in calls[0]["files"]] == [
        "camera-a.png",
        "camera-b.png",
        "camera-c.png",
    ]


def test_comfy_nodes_bulk_uses_values_from_approved_graph(monkeypatch):
    client = _client(monkeypatch)
    calls: list[dict] = []

    def fake_submit_batch(**kwargs):
        calls.append(kwargs)
        return {"batch_id": "node-bulk-001"}

    monkeypatch.setattr(genai_app, "submit_batch", fake_submit_batch)
    monkeypatch.setattr(genai_app.limits, "usage", _unlimited_usage)
    graph = copy.deepcopy(genai_app._load_comfy_node_contracts()["flux2-klein-4b-edit-v1"]["prompt"])
    graph["6"]["inputs"]["text"] = "same fallen-person edit for every camera"
    graph["14"]["inputs"]["noise_seed"] = 2468

    response = client.post(
        "/genai/bulk-batches",
        auth=("tester", "unused"),
        headers={"accept": "application/json"},
        data={
            "engine": "comfy_local",
            "prompts_text": "this spoofed form prompt must be ignored",
            "idempotency_token": "node-graph-bulk-unique-001",
            "pair_mode": "cartesian",
            "text_only": "false",
            "workflow_id": "flux2-klein-4b-edit-v1",
            "prompt_graph": __import__("json").dumps(graph),
        },
        files=[
            ("files", ("camera-a.png", _png(value=40), "image/png")),
            ("files", ("camera-b.png", _png(value=80), "image/png")),
        ],
    )

    assert response.status_code == 200, response.text
    assert response.json()["total_jobs"] == 2
    assert len(calls) == 1
    assert calls[0]["prompt"] == "same fallen-person edit for every camera"
    assert calls[0]["options"]["seed"] == 2468


def test_comfy_nodes_bulk_rejects_modified_graph_structure(monkeypatch):
    client = _client(monkeypatch)
    monkeypatch.setattr(genai_app.limits, "usage", _unlimited_usage)
    graph = copy.deepcopy(genai_app._load_comfy_node_contracts()["flux2-klein-4b-edit-v1"]["prompt"])
    graph["999"] = {"class_type": "SaveImage", "inputs": {}}

    response = client.post(
        "/genai/bulk-batches",
        auth=("tester", "unused"),
        headers={"accept": "application/json"},
        data={
            "engine": "comfy_local",
            "prompts_text": "ignored",
            "idempotency_token": "node-graph-reject-unique-001",
            "pair_mode": "cartesian",
            "workflow_id": "flux2-klein-4b-edit-v1",
            "prompt_graph": __import__("json").dumps(graph),
        },
        files=[("files", ("camera-a.png", _png(value=40), "image/png"))],
    )

    assert response.status_code == 400
    assert "approved Comfy graph required" in response.json()["detail"]


def test_comfy_sdxl_bulk_reuses_ordered_masks_for_each_prompt(monkeypatch):
    client = _client(monkeypatch)
    calls: list[dict] = []

    def fake_submit_batch(**kwargs):
        calls.append(kwargs)
        return {"batch_id": f"batch-{len(calls)}"}

    monkeypatch.setattr(genai_app, "submit_batch", fake_submit_batch)
    monkeypatch.setattr(genai_app.limits, "usage", _unlimited_usage)
    source_a = _png(value=40)
    source_b = _png(value=80)
    mask_a = _png(value=255)
    mask_b = _png(value=255)

    response = client.post(
        "/genai/bulk-batches",
        auth=("tester", "unused"),
        headers={"accept": "application/json"},
        data={
            "engine": "comfy_local",
            "prompts_text": "person lying on floor\nperson collapsed near wall",
            "idempotency_token": "sdxl-cartesian-unique-001",
            "pair_mode": "cartesian",
            "text_only": "false",
            "workflow_id": "sdxl-inpaint-cctv-v1",
            "seed": "0",
            "steps": "30",
            "cfg": "6",
            "denoise": "0.35",
        },
        files=[
            ("files", ("camera-a.png", source_a, "image/png")),
            ("files", ("camera-b.png", source_b, "image/png")),
            ("mask_files", ("mask-a.png", mask_a, "image/png")),
            ("mask_files", ("mask-b.png", mask_b, "image/png")),
        ],
    )

    assert response.status_code == 200, response.text
    assert response.json()["total_jobs"] == 4
    assert len(calls) == 2
    assert all([name for name, _blob in call["control_files"]] == ["mask-a.png", "mask-b.png"] for call in calls)
    assert all(call["options"]["denoise"] == 0.35 for call in calls)


def test_comfy_sdxl_bulk_rejects_missing_per_image_mask(monkeypatch):
    client = _client(monkeypatch)
    monkeypatch.setattr(genai_app.limits, "usage", _unlimited_usage)

    response = client.post(
        "/genai/bulk-batches",
        auth=("tester", "unused"),
        headers={"accept": "application/json"},
        data={
            "engine": "comfy_local",
            "prompts_text": "person lying on floor",
            "idempotency_token": "sdxl-mask-mismatch-001",
            "pair_mode": "cartesian",
            "workflow_id": "sdxl-inpaint-cctv-v1",
        },
        files=[
            ("files", ("camera-a.png", _png(value=40), "image/png")),
            ("files", ("camera-b.png", _png(value=80), "image/png")),
            ("mask_files", ("mask-a.png", _png(value=255), "image/png")),
        ],
    )

    assert response.status_code == 400
    assert "one mask per source image" in response.json()["detail"]


# ----- ComfyUI proxy allowlist -----------------------------------------
# 근거: ComfyUI 고정 커밋 ee71d5c 의 라우트 선언 + comfyui-frontend-package 1.52.7 번들의
# 호출 경로 + prod docker-genai-1 접근로그(ws, api/jobs, api/object_info, api/system_stats,
# api/history, iframe base).


def _proxy_client(monkeypatch) -> tuple[TestClient, list[str]]:
    """Client whose upstream proxy is replaced by a recorder of forwarded paths."""
    forwarded: list[str] = []

    async def fake_proxy(_request, path):
        forwarded.append(path)
        return genai_app.Response(content=b"ok", status_code=200)

    monkeypatch.setenv("GENAI_AUTH_DISABLED", "true")
    monkeypatch.setenv("GENAI_ENGINES_ENABLED", "comfy_local")
    monkeypatch.setenv("COMFYUI_CONTRACT_ROOT", str(ROOT / "docker" / "comfyui"))
    monkeypatch.setattr(genai_app, "_comfy_http_proxy", fake_proxy)
    return TestClient(genai_app.create_app()), forwarded


@pytest.mark.parametrize(
    "path",
    (
        "/genai/comfy-native/",
        "/genai/comfy-native/api/object_info",
        "/genai/comfy-native/api/object_info/LoadImage",
        "/genai/comfy-native/api/system_stats",
        "/genai/comfy-native/api/history",
        "/genai/comfy-native/api/history/abc-123",
        "/genai/comfy-native/api/jobs",
        "/genai/comfy-native/api/features",
        "/genai/comfy-native/api/settings",
        "/genai/comfy-native/api/userdata/user.css",
        "/genai/comfy-native/api/view",
        "/genai/comfy-native/api/workflow_templates",
        "/genai/comfy-native/api/experiment/models/checkpoints",
        "/genai/comfy-native/materialdesignicons.min.css",
        "/genai/comfy-native/assets/index-CENPJz5u.js",
        "/genai/comfy-native/templates/index_logo.json",
        "/genai/comfy-native/scripts/api.js",
        "/genai/comfy-native/extensions/core/colorPalette.js",
        "/genai/comfy-native/internal/folder_paths",
        "/genai/comfy-native/internal/logs/raw",
        "/api/object_info",
        "/api/jobs",
    ),
)
def test_comfy_proxy_allows_paths_the_native_frontend_actually_uses(monkeypatch, path):
    client, forwarded = _proxy_client(monkeypatch)

    response = client.get(path, auth=("tester", "unused"))

    assert response.status_code == 200, response.text
    assert len(forwarded) == 1


def test_comfy_proxy_preserves_asset_case_when_forwarding(monkeypatch):
    """Hashed bundle names are case-sensitive on disk — casefolding them 404s the canvas."""
    client, forwarded = _proxy_client(monkeypatch)

    client.get("/genai/comfy-native/assets/index-CENPJz5u.js", auth=("tester", "unused"))

    assert forwarded == ["assets/index-CENPJz5u.js"]


def test_comfy_proxy_allows_native_image_upload(monkeypatch):
    """comfy_nodes.html posts source frames to /genai/comfy-native/upload/image."""
    client, forwarded = _proxy_client(monkeypatch)

    response = client.post(
        "/genai/comfy-native/upload/image",
        auth=("tester", "unused"),
        files=[("image", ("camera-a.png", _png(), "image/png"))],
    )

    assert response.status_code == 200
    assert forwarded == ["upload/image"]


@pytest.mark.parametrize(
    "path",
    (
        "/genai/comfy-native/manager/reboot",
        "/genai/comfy-native/api/manager/queue/install",
        "/genai/comfy-native/custom-node-route",
        "/genai/comfy-native/internal/files",
        "/genai/comfy-native/api/api/jobs",
        "/api/userdata-export",
    ),
)
def test_comfy_proxy_denies_paths_outside_the_allowlist(monkeypatch, path, caplog):
    client, forwarded = _proxy_client(monkeypatch)

    with caplog.at_level("WARNING", logger="genai.app"):
        response = client.get(path, auth=("tester", "unused"))

    assert response.status_code == 403
    assert response.json()["error"]["type"] == "comfy_path_not_allowlisted"
    assert forwarded == []
    # 조용한 실패 금지: 막힌 경로가 로그에 남아야 UI 회귀를 추적할 수 있다.
    assert any("comfy proxy denied" in record.message for record in caplog.records)
    assert any(path.rsplit("/", 1)[-1] in record.getMessage() for record in caplog.records)


def test_comfy_proxy_denies_write_methods_on_static_paths(monkeypatch):
    client, forwarded = _proxy_client(monkeypatch)

    response = client.post("/genai/comfy-native/assets/index-CENPJz5u.js", auth=("tester", "unused"))

    assert response.status_code == 403
    assert forwarded == []


@pytest.mark.parametrize(
    ("raw_path", "method", "expected"),
    (
        ("./prompt", "POST", "prompt"),
        ("x/../prompt", "POST", "prompt"),
        ("api/./prompt", "POST", "prompt"),
        ("./queue", "GET", "blocked"),
        ("x/../interrupt", "POST", "blocked"),
        ("api/./free", "POST", "blocked"),
        ("PROMPT", "POST", "prompt"),
        ("api/QUEUE", "GET", "blocked"),
        ("jobs/abc-123/cancel", "POST", "blocked"),
        ("api/jobs/cancel", "POST", "blocked"),
        ("assets/../manager/reboot", "GET", "deny"),
        ("api/object_info", "GET", "allow"),
        ("assets/index-CENPJz5u.js", "GET", "allow"),
    ),
)
def test_comfy_proxy_verdict_resolves_dot_segments_and_case(raw_path, method, expected):
    # Production forwards the case-preserving form; the verdict casefolds its own copy.
    normalized = genai_app._normalize_proxy_path(raw_path, casefold=False)

    assert genai_app._comfy_proxy_verdict(normalized, method) == expected


def test_comfy_proxy_routes_prompt_to_the_validation_gate(monkeypatch):
    """`prompt` must reach comfy_native_prompt, never the raw upstream."""
    client, forwarded = _proxy_client(monkeypatch)
    calls: list[dict] = []

    def fake_submit_batch(**kwargs):
        calls.append(kwargs)
        return {"batch_id": "batch-proxy-gate"}

    monkeypatch.setattr(genai_app, "submit_batch", fake_submit_batch)
    monkeypatch.setattr(genai_app.limits, "usage", _unlimited_usage)

    response = client.post(
        "/genai/comfy-native/prompt",
        auth=("tester", "unused"),
        json={"prompt": {"1": {"class_type": "NotAnApprovedGraph", "inputs": {}}}},
    )

    assert response.status_code == 400
    assert response.json()["error"]["type"] == "restricted_workflow_rejected"
    assert forwarded == []
    assert calls == []


@pytest.mark.parametrize(
    "path",
    (
        "/genai/comfy-native/queue",
        "/genai/comfy-native/api/queue",
        "/genai/comfy-native/api/jobs/abc-123/cancel",
        "/api/jobs/cancel",
    ),
)
def test_comfy_proxy_keeps_execution_control_with_genai(monkeypatch, path):
    client, forwarded = _proxy_client(monkeypatch)

    response = client.post(path, auth=("tester", "unused"), json={})

    assert response.status_code == 403
    assert response.json()["error"]["type"] == "managed_by_genai"
    assert forwarded == []


def test_default_run_is_blocked_when_the_media_input_is_a_dead_placeholder():
    """템플릿 플레이스홀더(`source.png`)로 기본 Run 을 누르면 막고 이유를 말한다.

    2026-09-21 실측: Comfy input 볼륨(`/data/input`)에는 업로드본 `genai-*.jpg` 46개만 있고
    `source.png`/`mask.png` 는 없다. 승인 graph 는 그 이름을 들고 있으므로 캔버스가
    미리보기를 404 로 받고 Comfy 가 "Missing Inputs" 를 낸다 — 사용자에게는 "이미지를
    올렸는데 계속 오류" 로 보인다. 가드가 M×1 파일 선택 여부에만 걸려 있어 아무것도 고르지
    않은 상태에서는 원시 오류가 그대로 노출됐다.

    위젯의 알려진 값 목록이 비어 있을 때는 막지 않는다(입력 목록 조회 실패 시 기존 동작 유지).
    """
    html = (ROOT / "docker" / "genai" / "templates" / "comfy_nodes.html").read_text(encoding="utf-8")

    assert "function unresolvedMediaInput(app)" in html
    # 두 미디어 입력 모두 검사해야 SDXL inpaint 의 mask 도 걸린다.
    assert "'source_image', 'mask_image'" in html
    # 알려진 값이 없으면 통과 — fail-open 이 코드에 남아 있는지.
    assert "values.length > 0 && !values.includes(widget.value)" in html
    # 가드가 기본 Run 경로(queuePrompt)에서 실제로 호출되는지.
    guard = html[html.index("app.queuePrompt = async") : html.index("app.__genaiM1RunGuard = true;")]
    assert "unresolvedMediaInput(app)" in guard
    assert "M×1 실행" in guard, "사용자가 무엇을 해야 하는지 알려줘야 한다"
