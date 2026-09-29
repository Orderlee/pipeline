from __future__ import annotations

import hashlib
import json
import sys
from io import BytesIO
from pathlib import Path

import pytest
from PIL import Image


ROOT = Path(__file__).resolve().parents[2]
GENAI = ROOT / "docker" / "genai"
if str(GENAI) not in sys.path:
    sys.path.insert(0, str(GENAI))

from adapters.base import AdapterDeferredError  # noqa: E402
from adapters.comfy_local import ComfyLocalAdapter, ComfyLocalError  # noqa: E402


class _Response:
    def __init__(self, payload=None, *, content=b"", content_type="application/json"):
        self._payload = payload or {}
        self.content = content
        self.headers = {"content-type": content_type}
        self.status_code = 200
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


@pytest.fixture
def adapter(monkeypatch):
    monkeypatch.setenv("COMFYUI_CONTRACT_ROOT", str(ROOT / "docker" / "comfyui"))
    monkeypatch.setenv("COMFYUI_EMBEDDING_COORDINATION_REQUIRED", "false")
    return ComfyLocalAdapter()


def test_flux_submit_renders_allowlisted_graph_and_provenance(adapter, monkeypatch):
    calls = []
    provenance = []
    monkeypatch.setattr("adapters.comfy_local.pg.acquire_generation_gpu_lease", lambda *_: "lease")
    monkeypatch.setattr("adapters.comfy_local.pg.release_generation_gpu_lease", lambda *_: True)
    monkeypatch.setattr("adapters.comfy_local.pg.upsert_genai_job_provenance", provenance.append)

    def fake_request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if url.endswith("/system_stats"):
            return _Response({"devices": [{"vram_free": 15 * 1024**3}]})
        if url.endswith("/queue"):
            return _Response({"queue_running": [], "queue_pending": []})
        if url.endswith("/upload/image"):
            return _Response({"name": "uploaded.png", "subfolder": "", "type": "input"})
        if url.endswith("/prompt"):
            return _Response({"prompt_id": "prompt-123"})
        raise AssertionError(url)

    monkeypatch.setattr("adapters.comfy_local.requests.request", fake_request)
    result = adapter.submit(
        b"source-image",
        "source.png",
        "add a fallen person while preserving the CCTV geometry",
        options={
            "_job_id": "batch-001",
            "workflow_id": "flux2-klein-4b-edit-v1",
            "seed": 42,
            "steps": 4,
        },
    )

    assert result.provider_job_id == "prompt-123"
    prompt_call = next(call for call in calls if call[1].endswith("/prompt"))
    graph = prompt_call[2]["json"]["prompt"]
    assert graph["6"]["inputs"]["text"].startswith("add a fallen person")
    assert graph["14"]["inputs"]["noise_seed"] == 42
    assert graph["16"]["inputs"]["steps"] == 4
    assert graph["19"]["inputs"]["filename_prefix"] == "genai/batch-001"
    assert provenance[0]["workflow_id"] == "flux2-klein-4b-edit-v1"
    assert provenance[0]["seed"] == 42
    assert len(provenance[0]["workflow_sha256"]) == 64
    assert len(provenance[0]["model_manifest_sha256"]) == 64


def test_sdxl_requires_mask_before_gpu_lease(adapter, monkeypatch):
    called = False

    def acquire(*_args):
        nonlocal called
        called = True
        return "lease"

    monkeypatch.setattr("adapters.comfy_local.pg.acquire_generation_gpu_lease", acquire)
    with pytest.raises(ComfyLocalError, match="requires exactly one mask"):
        adapter.submit(
            b"source",
            "source.png",
            "inpaint a smoke plume",
            options={"_job_id": "job-1", "workflow_id": "sdxl-inpaint-cctv-v1"},
        )
    assert called is False


def test_busy_gpu_lease_is_deferred(adapter, monkeypatch):
    monkeypatch.setattr("adapters.comfy_local.pg.acquire_generation_gpu_lease", lambda *_: None)
    with pytest.raises(AdapterDeferredError, match="lease is busy"):
        adapter.submit(
            b"source",
            "source.png",
            "edit",
            options={"_job_id": "job-2", "workflow_id": "flux2-klein-4b-edit-v1"},
        )


def test_poll_rejects_path_traversal_output(adapter, monkeypatch):
    monkeypatch.setattr("adapters.comfy_local.pg.provenance_owner_for_prompt", lambda *_: "job-1")
    monkeypatch.setattr("adapters.comfy_local.pg.heartbeat_generation_gpu_lease", lambda *_: True)
    released = []
    monkeypatch.setattr(
        adapter, "_release_resources", lambda prompt_id, reason, owner_job_id=None: released.append(reason)
    )
    monkeypatch.setattr(
        adapter,
        "_request",
        lambda *_args, **_kwargs: _Response(
            {
                "prompt-1": {
                    "status": {"status_str": "success", "completed": True},
                    "outputs": {"10": {"images": [{"filename": "../escape.png", "subfolder": "", "type": "output"}]}},
                }
            }
        ),
    )
    result = adapter.poll("prompt-1")
    assert result.status == "failed"
    assert released == ["unsafe_output"]


def test_unreachable_comfy_does_not_extend_gpu_lease(adapter, monkeypatch):
    heartbeats = []
    monkeypatch.setattr("adapters.comfy_local.pg.provenance_owner_for_prompt", lambda *_: "job-1")
    monkeypatch.setattr(
        "adapters.comfy_local.pg.heartbeat_generation_gpu_lease",
        lambda *_: heartbeats.append("lease"),
    )
    monkeypatch.setattr(
        adapter,
        "_request",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AdapterDeferredError("offline")),
    )

    with pytest.raises(AdapterDeferredError, match="offline"):
        adapter.poll("prompt-1")

    assert heartbeats == []


def test_orphaned_comfy_queue_is_cleared_before_deferring(adapter, monkeypatch):
    calls = []

    def request(method, path, **kwargs):
        calls.append((method, path, kwargs.get("json")))
        if path == "/system_stats":
            return _Response({"devices": [{"vram_free": 15 * 1024**3}]})
        if path == "/queue" and method == "GET":
            return _Response({"queue_running": [[1, "old-prompt"]], "queue_pending": []})
        return _Response({})

    monkeypatch.setattr(adapter, "_request", request)

    with pytest.raises(AdapterDeferredError, match="orphaned"):
        adapter._prepare_gpu("new-job")

    assert ("POST", "/interrupt", {}) in calls
    assert ("POST", "/queue", {"clear": True}) in calls
    assert (
        "POST",
        "/free",
        {"unload_models": True, "free_memory": True},
    ) in calls


def test_inpaint_pair_requires_same_size_binary_nonempty_mask():
    from app import _validate_inpaint_pair

    def png(mode, size, color):
        output = BytesIO()
        Image.new(mode, size, color).save(output, format="PNG")
        return output.getvalue()

    source = png("RGB", (8, 8), (20, 30, 40))
    _validate_inpaint_pair(source, png("L", (8, 8), 255))

    with pytest.raises(ValueError, match="dimensions differ"):
        _validate_inpaint_pair(source, png("L", (4, 4), 255))
    with pytest.raises(ValueError, match="no selected pixels"):
        _validate_inpaint_pair(source, png("L", (8, 8), 0))
    with pytest.raises(ValueError, match="must be binary"):
        _validate_inpaint_pair(source, png("L", (8, 8), 127))


def test_contract_contains_only_core_allowlisted_workflows():
    contract = ROOT / "docker" / "comfyui"
    manifest = json.loads((contract / "model_manifest.json").read_text(encoding="utf-8"))
    assert {model["path"] for model in manifest["models"]} == {
        "diffusion_models/flux-2-klein-4b-fp8.safetensors",
        "text_encoders/qwen_3_4b.safetensors",
        "vae/flux2-vae.safetensors",
        "checkpoints/sd_xl_base_1.0_0.9vae.safetensors",
    }
    for workflow_id in ("flux2-klein-4b-edit-v1", "sdxl-inpaint-cctv-v1"):
        workflow = json.loads((contract / "workflows" / f"{workflow_id}.json").read_text(encoding="utf-8"))
        assert workflow["workflow_id"] == workflow_id
        assert workflow["output_node_id"] in workflow["prompt"]
        assert all(not node["class_type"].startswith("API") for node in workflow["prompt"].values())


def test_comfy_outputs_cannot_bypass_human_review(monkeypatch):
    from jobs import promote

    monkeypatch.setattr(
        promote.pg,
        "get_batch_with_jobs",
        lambda _batch_id: {
            "batch_id": "batch-1",
            "status": "succeeded",
            "engine": "comfy_local",
            "output_media": "image",
            "jobs": [{"status": "done", "seq_in_batch": 1}],
        },
    )
    with pytest.raises(promote.PromoteValidationError, match="label_policy='required'"):
        promote.promote_batch_to_labeling(
            "batch-1",
            labeling_method=["bbox"],
            label_policy="none",
            categories=[],
            classes=["person"],
        )


def test_amnesiac_comfy_fails_job_and_releases_gpu_after_deadline(adapter, monkeypatch):
    """ComfyUI answering 200 with no history must not hold GPU0 forever.

    Regression for the 3-day outage of 2026-09-18: poll() heartbeat the lease and the
    embedding maintenance flag before looking at the history, so a prompt lost to a
    ComfyUI restart stayed 'running' and kept the embedding service at 503.
    """
    heartbeats: list[str] = []
    released: list[str] = []
    monkeypatch.setattr("adapters.comfy_local.pg.provenance_owner_for_prompt", lambda *_: "job-1")
    monkeypatch.setattr(
        "adapters.comfy_local.pg.heartbeat_generation_gpu_lease",
        lambda *_: heartbeats.append("lease"),
    )
    monkeypatch.setattr(adapter, "_request", lambda *_a, **_k: _Response({}))

    # Still inside the deadline: reported as running, but the lease is NOT extended.
    monkeypatch.setattr("adapters.comfy_local.pg.provenance_age_seconds", lambda *_: 10.0)
    assert adapter.poll("prompt-1").status == "running"
    assert heartbeats == []

    # Past the deadline: terminal failure and GPU0 handed back.
    monkeypatch.setattr(
        "adapters.comfy_local.pg.provenance_age_seconds",
        lambda *_: adapter.job_deadline + 1,
    )
    monkeypatch.setattr(adapter, "_release_resources", lambda _p, reason, **_k: released.append(reason))
    result = adapter.poll("prompt-1")
    assert result.status == "failed"
    assert released == ["deadline_exceeded"]
    assert heartbeats == []


def test_proxy_path_normalization_closes_dot_segment_bypass():
    """`./prompt` reached ComfyUI's real /prompt while dodging the block list."""
    from app import _normalize_proxy_path

    assert _normalize_proxy_path("./prompt") == "prompt"
    assert _normalize_proxy_path("api/../prompt") == "prompt"
    assert _normalize_proxy_path("api/./interrupt") == "api/interrupt"
    assert _normalize_proxy_path("x/y/../../queue") == "queue"
    assert _normalize_proxy_path("//free//") == "free"
    assert _normalize_proxy_path("PROMPT") == "prompt"
    # Ordinary paths are untouched.
    assert _normalize_proxy_path("object_info") == "object_info"
    assert _normalize_proxy_path("view") == "view"


# ----------------------------------------------------------------------
# P2-1 — provenance records only what the workflow contract actually binds.
# ----------------------------------------------------------------------
def _fake_comfy(adapter, monkeypatch):
    """Happy-path ComfyUI + PG fakes. Returns (request calls, provenance records)."""
    calls: list[tuple] = []
    provenance: list[dict] = []
    monkeypatch.setattr("adapters.comfy_local.pg.acquire_generation_gpu_lease", lambda *_: "lease")
    monkeypatch.setattr("adapters.comfy_local.pg.release_generation_gpu_lease", lambda *_: True)
    monkeypatch.setattr("adapters.comfy_local.pg.upsert_genai_job_provenance", provenance.append)

    def fake_request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if url.endswith("/system_stats"):
            return _Response({"devices": [{"vram_free": 15 * 1024**3}]})
        if url.endswith("/queue"):
            return _Response({"queue_running": [], "queue_pending": []})
        if url.endswith("/upload/image"):
            return _Response({"name": "uploaded.png", "subfolder": "", "type": "input"})
        if url.endswith("/prompt"):
            return _Response({"prompt_id": "prompt-123"})
        raise AssertionError(url)

    monkeypatch.setattr("adapters.comfy_local.requests.request", fake_request)
    return calls, provenance


def _submitted_graph(calls):
    return next(call for call in calls if call[1].endswith("/prompt"))[2]["json"]["prompt"]


def test_flux_provenance_records_unbound_params_as_null(adapter, monkeypatch):
    """FLUX.2 Klein binds no negative_prompt/cfg/denoise — the record must say so.

    The first 26 comfy_local rows stamped cfg=6.0 / denoise=0.85 / a negative prompt
    hash onto every FLUX job although the graph never received any of them (its cfg is
    fixed at 1.0 inside CFGGuider): a reproducibility record that was simply false.
    Unbound is now an explicit null — "we had it and did not use it", not silence.
    """
    calls, provenance = _fake_comfy(adapter, monkeypatch)
    adapter.submit(
        b"source-image",
        "source.png",
        "add a fallen person while preserving the CCTV geometry",
        options={
            "_job_id": "batch-001",
            "workflow_id": "flux2-klein-4b-edit-v1",
            "seed": 42,
            "steps": 4,
            # Requested, but this contract has nowhere to put them.
            "cfg": 6.0,
            "denoise": 0.85,
            "negative_prompt": "blurry, watermark",
        },
    )

    record = provenance[0]
    assert record["negative_prompt_sha256"] is None
    assert {"cfg", "denoise", "sampler"} <= set(record["params"])
    assert record["params"]["cfg"] is None
    assert record["params"]["denoise"] is None
    assert record["params"]["sampler"] == "euler"
    assert record["params"]["steps"] == 4

    # ...and none of the ignored values leaked into the submitted graph.
    graph = _submitted_graph(calls)
    assert graph["13"]["inputs"]["cfg"] == 1.0
    assert not any("denoise" in node["inputs"] for node in graph.values())
    assert graph["6"]["inputs"]["text"].startswith("add a fallen person")
    assert all("blurry" not in str(node["inputs"].get("text", "")) for node in graph.values())


def test_sdxl_provenance_records_the_params_it_actually_binds(adapter, monkeypatch):
    """The inpaint contract does bind negative_prompt/cfg/denoise — record the real values."""
    calls, provenance = _fake_comfy(adapter, monkeypatch)
    adapter.submit(
        b"source-image",
        "source.png",
        "inpaint a smoke plume over the loading dock",
        options={
            "_job_id": "batch-002",
            "workflow_id": "sdxl-inpaint-cctv-v1",
            "seed": 7,
            "steps": 30,
            "cfg": 5.5,
            "denoise": 0.6,
            "negative_prompt": "blurry, watermark",
            "_mask_bytes": b"mask-image",
            "_mask_filename": "mask.png",
        },
    )

    record = provenance[0]
    assert record["params"]["cfg"] == 5.5
    assert record["params"]["denoise"] == 0.6
    assert record["params"]["sampler"] == "dpmpp_2m"
    assert record["negative_prompt_sha256"] == hashlib.sha256(b"blurry, watermark").hexdigest()

    graph = _submitted_graph(calls)
    assert graph["7"]["inputs"]["cfg"] == 5.5
    assert graph["7"]["inputs"]["denoise"] == 0.6
    assert graph["5"]["inputs"]["text"] == "blurry, watermark"
    # The recorded sampler is the one the graph will run, not a lookup table entry.
    assert graph["7"]["inputs"]["sampler_name"] == record["params"]["sampler"]


def test_recorded_sampler_comes_from_the_workflow_graph():
    """Anti-hardcoding: each contract must contain exactly one sampler, and it is the one recorded."""
    contract = ROOT / "docker" / "comfyui" / "workflows"
    seen = {}
    for workflow_id in ("flux2-klein-4b-edit-v1", "sdxl-inpaint-cctv-v1"):
        template = json.loads((contract / f"{workflow_id}.json").read_text(encoding="utf-8"))
        literal = {
            node["inputs"]["sampler_name"]
            for node in template["prompt"].values()
            if isinstance((node.get("inputs") or {}).get("sampler_name"), str)
        }
        assert len(literal) == 1, f"{workflow_id}: ambiguous sampler {sorted(literal)}"
        seen[workflow_id] = ComfyLocalAdapter._graph_sampler(template)
        assert seen[workflow_id] == literal.pop()
    # Guards against a copy-paste contract that silently records the wrong engine's sampler.
    assert seen["flux2-klein-4b-edit-v1"] != seen["sdxl-inpaint-cctv-v1"]


def test_graph_sampler_is_none_when_the_contract_has_none():
    assert ComfyLocalAdapter._graph_sampler({"prompt": {"1": {"inputs": {"image": "a.png"}}}}) is None
    # A wired input is a [node, slot] pair — not a sampler name.
    wired = {"prompt": {"1": {"inputs": {"sampler_name": ["2", 0]}}}}
    assert ComfyLocalAdapter._graph_sampler(wired) is None


def test_provenance_json_mirrors_the_row_and_never_claims_an_unused_negative(monkeypatch, tmp_path):
    """provenance.json 과 genai_job_provenance.params_json 은 같은 사실을 말해야 한다."""
    from jobs import finalize

    def write_provenance(negative_sha, params):
        written: dict[str, dict] = {}

        class _Cursor:
            sql = ""

            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return False

            def execute(self, sql, _params=None):
                self.sql = sql

            def fetchone(self):
                if "pg_try_advisory_xact_lock" in self.sql:
                    return (True,)
                return (
                    "comfy_local",
                    "image",
                    "a fallen person",
                    json.dumps({"negative_prompt": "blurry, watermark"}),
                )

            def fetchall(self):
                if "genai_job_provenance" in self.sql:
                    return [
                        (
                            1,
                            "flux2-klein-4b-edit-v1",
                            "w" * 64,
                            "m" * 64,
                            "p" * 64,
                            negative_sha,
                            42,
                            "i" * 64,
                            None,
                            "prompt-1",
                            "o" * 64,
                            params,
                            None,
                            None,
                        )
                    ]
                return [(1, "done", "prompt-1", None)]

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return False

            def cursor(self):
                return _Cursor()

        monkeypatch.setattr(finalize.pg, "connect", lambda: _Conn())
        monkeypatch.setattr(
            finalize, "atomic_write_json", lambda path, payload: written.__setitem__(path.name, payload)
        )
        finalize._maybe_write_outputs_manifest("batch-1", "comfy_local", "image", tmp_path / "outputs")
        return written["provenance.json"]

    unbound = write_provenance(
        None, {"workflow_version": 1, "sampler": "euler", "steps": 4, "cfg": None, "denoise": None}
    )
    assert unbound["schema_version"] == 2
    assert unbound["negative_prompt"] is None  # 안 썼다
    assert unbound["negative_prompt_requested"] == "blurry, watermark"  # 그러나 몰랐던 건 아니다
    assert unbound["jobs"][0]["negative_prompt_sha256"] is None
    assert unbound["jobs"][0]["params"]["sampler"] == "euler"
    assert unbound["jobs"][0]["params"]["cfg"] is None

    bound = write_provenance(
        "n" * 64, {"workflow_version": 1, "sampler": "dpmpp_2m", "steps": 30, "cfg": 5.5, "denoise": 0.6}
    )
    assert bound["negative_prompt"] == "blurry, watermark"
    assert bound["jobs"][0]["params"]["cfg"] == 5.5
    assert bound["jobs"][0]["params"]["sampler"] == "dpmpp_2m"
