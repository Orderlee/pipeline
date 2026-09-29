from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
COMFYUI = ROOT / "docker" / "comfyui"
PINNED_COMMIT = "ee71d5c4993f29086b27fde1629a945ae48425bf"


def _manifest() -> dict:
    return json.loads((COMFYUI / "model_manifest.json").read_text(encoding="utf-8"))


def _workflow(workflow_id: str) -> dict:
    return json.loads((COMFYUI / "workflows" / f"{workflow_id}.json").read_text(encoding="utf-8"))


def _load_validate_models():
    """Load docker/comfyui/validate_models.py as a throwaway module.

    Deliberately not registered in sys.modules: a shared module object would let
    one test's monkeypatched MANIFEST/MODEL_ROOT leak into the next.
    """
    spec = importlib.util.spec_from_file_location("comfyui_validate_models_undertest", COMFYUI / "validate_models.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def model_tree(tmp_path, monkeypatch):
    """A manifest + matching model file, wired into a fresh validate_models module."""
    models = tmp_path / "models"
    (models / "vae").mkdir(parents=True)
    blob = b"pretend-safetensors"
    (models / "vae" / "fake.safetensors").write_bytes(blob)
    manifest = tmp_path / "model_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "models": [
                    {
                        "path": "vae/fake.safetensors",
                        "sha256": hashlib.sha256(blob).hexdigest(),
                        "size_bytes": len(blob),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    module = _load_validate_models()
    monkeypatch.setattr(module, "MANIFEST", manifest)
    monkeypatch.setattr(module, "MODEL_ROOT", models)
    monkeypatch.setattr(module, "STATE", tmp_path / "state.json")
    monkeypatch.delenv("COMFYUI_FAIL_FAST_ON_MODEL_ERROR", raising=False)
    monkeypatch.setenv("COMFYUI_REQUIRE_MODELS", "true")
    monkeypatch.setenv("COMFYUI_VERIFY_MODEL_HASHES", "true")
    return module, models / "vae" / "fake.safetensors"


def test_comfyui_is_internal_gpu0_only_profile():
    compose = yaml.safe_load((ROOT / "docker" / "docker-compose.yaml").read_text(encoding="utf-8"))
    service = compose["services"]["comfyui"]
    assert service["profiles"] == ["comfyui"]
    assert "ports" not in service
    assert service["expose"] == ["8188"]
    assert service["environment"]["CUDA_VISIBLE_DEVICES"] == "0"
    assert service["environment"]["NVIDIA_VISIBLE_DEVICES"] == "0"
    devices = service["deploy"]["resources"]["reservations"]["devices"]
    assert devices == [{"driver": "nvidia", "device_ids": ["0"], "capabilities": ["gpu"]}]
    mounts = service["volumes"]
    assert not any("/nas/" in str(mount) for mount in mounts)


def test_comfyui_revision_is_immutable():
    dockerfile = (COMFYUI / "Dockerfile").read_text(encoding="utf-8")
    assert PINNED_COMMIT in dockerfile
    assert "git pull" not in dockerfile
    assert "ComfyUI-Manager" not in dockerfile


def test_node_revision_is_the_same_in_dockerfile_compose_and_manifest():
    """The manifest's node revision is only useful if it cannot silently lag the image."""
    compose = yaml.safe_load((ROOT / "docker" / "docker-compose.yaml").read_text(encoding="utf-8"))
    compose_arg = compose["services"]["comfyui"]["build"]["args"]["COMFYUI_COMMIT"]
    dockerfile = (COMFYUI / "Dockerfile").read_text(encoding="utf-8")
    revision = _manifest()["comfy_node_revision"]

    assert revision["commit"] == PINNED_COMMIT
    assert str(compose_arg) == PINNED_COMMIT
    assert f"ARG COMFYUI_COMMIT={PINNED_COMMIT}" in dockerfile
    # No custom nodes: every class_type in the workflows must come from this revision.
    assert revision["custom_nodes"] == []


def test_manifest_models_carry_provenance_and_vram_fields():
    manifest = _manifest()
    required = {"path", "sha256", "size_bytes", "license", "source", "comfy_model_names", "staged_vram_mb", "used_by"}
    workflow_ids = set(manifest["workflows"])
    for model in manifest["models"]:
        assert required <= set(model), f"{model.get('path')} is missing {required - set(model)}"
        assert len(model["sha256"]) == 64
        assert isinstance(model["staged_vram_mb"], int) and model["staged_vram_mb"] > 0
        assert model["used_by"] and set(model["used_by"]) <= workflow_ids


def test_peak_vram_is_measured_and_carries_its_source():
    """P2-4 ran on 2026-09-21, so the peak is a number — but only if it says where it came from.

    The invariant this file has always enforced is *not* "the field is null"; it is "never state a
    VRAM figure you did not observe". Before the UAT that meant null plus MEASURED: NO. Now it means
    a real number plus a source that names the method, so a later reader can tell a measurement from
    a guess. A number without `peak_process_vram_gb_source` is the failure this guards against.
    """
    manifest = _manifest()
    vram = manifest["vram"]
    peak = vram["peak_process_vram_gb"]
    assert isinstance(peak, (int, float)) and peak > 0
    assert "MEASURED: YES" in vram["peak_process_vram_note"]
    source = vram["peak_process_vram_gb_source"]
    assert source.startswith("measured")
    assert "nvidia-smi" in source
    assert "measured" in vram["staged_vram_mb_source"]
    # A peak below the staged weights of the heaviest workflow would mean the sampler never caught
    # the run. Guard the obviously-wrong direction without pinning the exact figure.
    heaviest_staged_gb = max(w["staged_vram_mb_total"] for w in manifest["workflows"].values()) / 1024
    assert peak >= heaviest_staged_gb
    for workflow_id, workflow in manifest["workflows"].items():
        wf_peak = workflow["peak_process_vram_gb"]
        assert isinstance(wf_peak, (int, float)) and wf_peak > 0, workflow_id
        assert wf_peak <= peak, f"{workflow_id} cannot exceed the overall peak"
        measured = workflow["peak_process_vram_gb_measured"]
        assert measured["n_runs"] >= 1
        assert measured["min_gb"] <= measured["max_gb"] == wf_peak
        assert measured["samples_mib"] and len(measured["samples_mib"]) == measured["n_runs"]
        assert max(measured["samples_mib"]) / 1024 == pytest.approx(wf_peak, abs=0.01)
        assert measured["source"]


def test_admission_threshold_covers_the_measured_peak_footprint():
    """The VRAM gate must ask for at least what the heaviest workflow actually takes.

    `_prepare_gpu` unloads PE-Core *before* reading free VRAM, so this threshold never guards
    against the embedding service — it guards against other GPU0 tenants. That makes the comparison
    concrete: admission checks free VRAM, and the job then needs its own footprint, which is the
    whole-GPU peak minus the baseline those other tenants already hold. A threshold below that
    difference admits a job that proceeds to OOM, which is exactly what 13 GB did before 2026-09-21
    (it let anything through whenever free VRAM sat in the 13.0-14.25 GB window).

    Rejection is the safe failure here: AdapterDeferredError releases the lease and the next tick
    retries. Admission followed by OOM burns a GPU run and a lease cycle. So this asserts one
    direction only — the threshold may be raised freely, never dropped below the measurement.
    """
    manifest = _manifest()
    vram = manifest["vram"]
    own_footprint_gb = vram["peak_process_vram_gb"] - vram["baseline_other_tenants_gb"]
    assert vram["baseline_other_tenants_gb"] > 0
    assert "MEASURED" in vram["baseline_other_tenants_note"]
    assert vram["preflight_threshold_gb"] >= own_footprint_gb, (
        f"admission threshold {vram['preflight_threshold_gb']} GB is below the measured footprint "
        f"{own_footprint_gb:.2f} GB — the gate would admit jobs that then OOM"
    )

    compose = yaml.safe_load((ROOT / "docker" / "docker-compose.yaml").read_text(encoding="utf-8"))
    declared = compose["services"]["genai"]["environment"]["COMFYUI_MIN_FREE_VRAM_GB"]
    match = re.fullmatch(r"\$\{COMFYUI_MIN_FREE_VRAM_GB:-([0-9.]+)\}", declared)
    assert match, f"threshold must stay overridable via env, got {declared!r}"
    assert float(match.group(1)) == pytest.approx(
        vram["preflight_threshold_gb"]
    ), "compose default and manifest threshold drifted apart"


def test_workflow_staged_vram_totals_match_their_models():
    manifest = _manifest()
    staged = {model["path"]: model["staged_vram_mb"] for model in manifest["models"]}
    for workflow_id, workflow in manifest["workflows"].items():
        assert set(workflow["models"]) <= set(staged), workflow_id
        assert workflow["staged_vram_mb_total"] == sum(staged[path] for path in workflow["models"])
    sdxl = next(m for m in manifest["models"] if m["path"].startswith("checkpoints/"))
    assert sum(sdxl["staged_vram_breakdown_mb"].values()) == sdxl["staged_vram_mb"]


def test_manifest_input_schema_is_derived_from_workflow_bindings():
    """The declared schema must be the graph's own bindings — nothing more, nothing less."""
    manifest = _manifest()
    assert set(manifest["workflows"]) == {"flux2-klein-4b-edit-v1", "sdxl-inpaint-cctv-v1"}
    for workflow_id, declared in manifest["workflows"].items():
        workflow = _workflow(workflow_id)
        bindings = workflow["bindings"]
        assert set(declared["inputs"]) == set(bindings), workflow_id
        for name, spec in declared["inputs"].items():
            node_id, input_key = bindings[name]
            assert spec["node"] == node_id and spec["input"] == input_key, f"{workflow_id}.{name}"
            assert spec["class_type"] == workflow["prompt"][node_id]["class_type"]
            assert input_key in workflow["prompt"][node_id]["inputs"]
        assert set(declared["unbound"]).isdisjoint(bindings), workflow_id
    flux = manifest["workflows"]["flux2-klein-4b-edit-v1"]
    assert flux["inputs"]["steps"]["fixed_value"] == 4
    assert "mask_image" in flux["unbound"]
    assert manifest["workflows"]["sdxl-inpaint-cctv-v1"]["inputs"]["mask_image"]["required"] is True


def test_compose_exposes_both_model_gate_knobs():
    compose = yaml.safe_load((ROOT / "docker" / "docker-compose.yaml").read_text(encoding="utf-8"))
    environment = compose["services"]["comfyui"]["environment"]
    assert environment["COMFYUI_REQUIRE_MODELS"] == "${COMFYUI_REQUIRE_MODELS:-true}"
    assert environment["COMFYUI_FAIL_FAST_ON_MODEL_ERROR"] == "${COMFYUI_FAIL_FAST_ON_MODEL_ERROR:-false}"


def test_missing_model_boots_but_reports_unhealthy(model_tree, monkeypatch):
    """The P2-2 fix: startability and health are separate knobs.

    Before, COMFYUI_REQUIRE_MODELS=true exited here, so `restart: unless-stopped`
    crash-looped and the unhealthy state was unreachable. Now the container comes
    up carrying the reason.
    """
    module, blob = model_tree
    blob.unlink()

    state = module.validate()  # must not raise SystemExit

    assert state["missing"] and not state["mismatched"]
    assert state["healthy"] is False
    assert "not provisioned" in state["health_reason"]
    assert module.health_verdict(state)[0] is False


def test_missing_model_is_healthy_when_models_are_not_required(model_tree, monkeypatch):
    """Design criterion "model 없이도 healthy" — scoped to the bootstrap case."""
    module, blob = model_tree
    blob.unlink()
    monkeypatch.setenv("COMFYUI_REQUIRE_MODELS", "false")

    state = module.validate()

    assert state["healthy"] is True
    assert "degraded" in state["health_reason"]


def test_manifest_mismatch_is_unhealthy_regardless_of_the_flags(model_tree, monkeypatch):
    """Design criterion "manifest 불일치 시 unhealthy" — unconditional, unlike a missing file.

    A file that exists but disagrees with the pinned manifest means reproducibility
    is already broken; no flag may paint that green.
    """
    module, blob = model_tree
    blob.write_bytes(b"tampered-content-of-a-different-length")

    for require in ("true", "false"):
        monkeypatch.setenv("COMFYUI_REQUIRE_MODELS", require)
        state = module.validate()
        assert state["mismatched"], require
        assert state["healthy"] is False, require
        assert module.health_verdict(state)[0] is False, require


def test_unusable_manifest_entry_does_not_take_the_boot_down(model_tree):
    """ "Always starts" has to hold for a malformed manifest too, not just a missing file."""
    module, _ = model_tree
    module.MANIFEST.write_text(json.dumps({"models": [{"sha256": "x"}]}), encoding="utf-8")

    state = module.validate()

    assert state["healthy"] is False
    assert "unusable manifest entry #0" in state["mismatched"][0]


def test_unreadable_manifest_is_a_mismatch_not_a_crash(model_tree):
    module, _ = model_tree
    module.MANIFEST.write_text("{not json", encoding="utf-8")

    state = module.validate()

    assert state["healthy"] is False
    assert "not valid JSON" in state["mismatched"][0]


def test_fail_fast_flag_still_blocks_boot_when_asked(model_tree, monkeypatch):
    module, blob = model_tree
    blob.unlink()
    monkeypatch.setenv("COMFYUI_FAIL_FAST_ON_MODEL_ERROR", "true")

    with pytest.raises(SystemExit):
        module.validate()


def test_prod_configuration_stays_healthy(model_tree):
    """COMFYUI_REQUIRE_MODELS=true with every model in place: unchanged from before."""
    module, _ = model_tree

    state = module.validate()

    assert state["ok"] is True and state["healthy"] is True
    assert state["missing"] == [] and state["mismatched"] == []
    assert len(state["checked"]) == 1


def test_legacy_validation_state_fails_closed(model_tree):
    """A state file from an older image has no severity split; do not read that as healthy."""
    module, _ = model_tree

    assert module.health_verdict({"ok": False, "errors": ["missing model: x"]})[0] is False
    assert module.health_verdict({"ok": True, "errors": []})[0] is True


def _run_healthcheck(tmp_path, state: dict, manifest_text: str, require: str) -> subprocess.CompletedProcess:
    manifest = tmp_path / "model_manifest.json"
    manifest.write_text(manifest_text, encoding="utf-8")
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(COMFYUI / "healthcheck.py")],
        capture_output=True,
        text=True,
        env={
            "PATH": "/usr/bin:/bin",
            "COMFYUI_MODEL_MANIFEST": str(manifest),
            "COMFYUI_MODEL_VALIDATION_STATE": str(state_path),
            "COMFYUI_REQUIRE_MODELS": require,
        },
    )


def test_healthcheck_reports_the_model_reason_before_touching_the_api(tmp_path):
    manifest_text = json.dumps({"schema_version": 2, "models": []})
    sha = hashlib.sha256(manifest_text.encode("utf-8")).hexdigest()
    degraded = {
        "ok": False,
        "missing": ["missing model: vae/fake.safetensors"],
        "mismatched": [],
        "manifest_sha256": sha,
    }

    result = _run_healthcheck(tmp_path, degraded, manifest_text, "true")

    assert result.returncode == 1
    assert "unhealthy" in result.stderr and "not provisioned" in result.stderr


def test_healthcheck_flags_a_state_that_predates_the_current_manifest(tmp_path):
    manifest_text = json.dumps({"schema_version": 2, "models": []})
    stale = {"ok": True, "missing": [], "mismatched": [], "manifest_sha256": "0" * 64}

    result = _run_healthcheck(tmp_path, stale, manifest_text, "true")

    assert result.returncode == 1
    assert "does not describe the current model manifest" in result.stderr


def test_healthcheck_passes_the_model_gate_when_validation_is_clean(tmp_path):
    """Past the gate it goes on to the Comfy API, which is absent here — but not for model reasons."""
    manifest_text = json.dumps({"schema_version": 2, "models": []})
    sha = hashlib.sha256(manifest_text.encode("utf-8")).hexdigest()
    clean = {"ok": True, "missing": [], "mismatched": [], "checked": [], "manifest_sha256": sha}

    result = _run_healthcheck(tmp_path, clean, manifest_text, "true")

    assert "manifest" not in result.stderr and "provisioned" not in result.stderr
