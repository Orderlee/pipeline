"""Validate operator-provisioned model files once before ComfyUI starts.

Two knobs, deliberately not one (plan P2-2).

    COMFYUI_FAIL_FAST_ON_MODEL_ERROR  (default false)  may the container BOOT?
    COMFYUI_REQUIRE_MODELS            (default true)   is a model-less container HEALTHY?

One flag used to govern both, which made the health branch unreachable in either
setting: with the flag on, this script exited before any healthcheck could run
(``restart: unless-stopped`` turned that into a crash-loop, and a crash-looping
container cannot report *why*); with it off, the healthcheck could never fail.
Startability and health are now separate, so "boots, and says what is wrong" is
expressible — that is the state an operator can actually read.

Severity is split too, because "not provisioned yet" and "provisioned wrong" are
different facts:

    missing   file absent. Bootstrap state; COMFYUI_REQUIRE_MODELS decides health.
    mismatch  file present but size / sha256 / path disagrees with the pinned
              manifest, or the manifest itself is unreadable. Reproducibility is
              broken, so this is unhealthy regardless of either flag.

That split is what lets both Phase B completion criteria hold at once: "healthy
without models" applies to the bootstrap case only, "unhealthy on manifest
mismatch" applies unconditionally.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path


MANIFEST = Path(os.getenv("COMFYUI_MODEL_MANIFEST", "/opt/service/model_manifest.json"))
MODEL_ROOT = Path(os.getenv("COMFYUI_MODEL_ROOT", "/models"))
STATE = Path(os.getenv("COMFYUI_MODEL_VALIDATION_STATE", "/tmp/comfyui-model-validation.json"))


def _flag(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifest_sha256() -> str | None:
    """Hash of the manifest as it exists right now, or None if it cannot be read."""
    try:
        return _sha256(MANIFEST)
    except OSError:
        return None


def _read_manifest() -> tuple[dict, str | None, str | None]:
    """Return (payload, sha256, error). A manifest we cannot read is itself a mismatch."""
    try:
        raw = MANIFEST.read_bytes()
    except OSError as exc:
        return {}, None, f"manifest unreadable: {exc}"
    sha = hashlib.sha256(raw).hexdigest()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        return {}, sha, f"manifest is not valid JSON: {exc}"
    if not isinstance(payload, dict) or not isinstance(payload.get("models"), list):
        return {}, sha, "manifest has no models list"
    return payload, sha, None


def health_verdict(state: dict) -> tuple[bool, str]:
    """The health half of the contract. Startability is a separate knob and is not consulted.

    Single implementation on purpose — healthcheck.py imports this rather than
    re-deriving it, so the two cannot drift.
    """
    if "missing" not in state and "mismatched" not in state:
        # State written by an older image: no severity split to read. Fail closed.
        if not state.get("ok"):
            return False, "model validation did not pass (legacy validation state)"
        return True, "model validation passed (legacy validation state)"

    mismatched = list(state.get("mismatched") or [])
    missing = list(state.get("missing") or [])
    if mismatched:
        return False, "manifest mismatch: " + "; ".join(mismatched)
    if missing:
        if _flag("COMFYUI_REQUIRE_MODELS", "true"):
            return False, "models required but not provisioned: " + "; ".join(missing)
        return True, f"degraded: {len(missing)} model(s) not provisioned, COMFYUI_REQUIRE_MODELS is off"
    return True, f"{len(state.get('checked') or [])} model(s) match the pinned manifest"


def validate() -> dict:
    require = _flag("COMFYUI_REQUIRE_MODELS", "true")
    fail_fast = _flag("COMFYUI_FAIL_FAST_ON_MODEL_ERROR", "false")
    verify_hashes = _flag("COMFYUI_VERIFY_MODEL_HASHES", "true")

    payload, manifest_hash, manifest_error = _read_manifest()
    missing: list[str] = []
    mismatched: list[str] = []
    checked: list[dict] = []
    if manifest_error:
        mismatched.append(manifest_error)

    for index, model in enumerate(payload.get("models", [])):
        # Per-entry fail-forward: one unusable row must not take the boot down, or the
        # "container always starts" half of the contract would be a lie.
        try:
            relative = str(model["path"])
            expected_size = int(model.get("size_bytes") or 0)
            expected_hash = str(model.get("sha256") or "").lower()
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            mismatched.append(f"unusable manifest entry #{index}: {exc}")
            continue
        path = (MODEL_ROOT / relative).resolve()
        if MODEL_ROOT.resolve() not in path.parents:
            mismatched.append(f"path escapes model root: {relative}")
            continue
        if not path.is_file():
            missing.append(f"missing model: {relative}")
            continue
        actual_size = path.stat().st_size
        if expected_size and actual_size != expected_size:
            mismatched.append(f"size mismatch: {relative} expected={expected_size} actual={actual_size}")
            continue
        actual_hash = _sha256(path) if verify_hashes else None
        if verify_hashes and actual_hash != expected_hash:
            mismatched.append(f"sha256 mismatch: {relative}")
            continue
        checked.append({"path": relative, "size_bytes": actual_size, "sha256": actual_hash})

    state = {
        "ok": not missing and not mismatched,
        "missing": missing,
        "mismatched": mismatched,
        "errors": mismatched + missing,
        "require_models": require,
        "required": require,
        "fail_fast": fail_fast,
        "hashes_verified": verify_hashes,
        "checked": checked,
        "manifest_sha256": manifest_hash,
    }
    healthy, reason = health_verdict(state)
    state["healthy"] = healthy
    state["health_reason"] = reason
    STATE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "event": "comfyui.model_validation",
        "ok": state["ok"],
        "healthy": healthy,
        "reason": reason,
        "checked": len(checked),
        "missing": missing,
        "mismatched": mismatched,
        "fail_fast": fail_fast,
        "require_models": require,
        "hashes_verified": verify_hashes,
    }
    print(json.dumps(summary, ensure_ascii=False), flush=True)

    if (missing or mismatched) and fail_fast:
        raise SystemExit("ComfyUI model validation failed: " + "; ".join(state["errors"]))
    if missing or mismatched:
        # Boot anyway. The healthcheck carries the verdict so the reason stays visible
        # in `docker ps` / `docker inspect` instead of scrolling past in a restart loop.
        print(f"WARNING: ComfyUI starts degraded: {reason}", flush=True)
    return state


if __name__ == "__main__":
    validate()
