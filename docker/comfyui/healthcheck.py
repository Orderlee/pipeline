"""Container health: cached model validation plus the internal Comfy API.

Health is one of the two model knobs; startability is the other and lives in
validate_models.py (plan P2-2). This script never decides whether the container
may run — it only reports what is true, which is why validate_models.py no
longer exits on a bad model set by default.

The verdict itself is imported rather than re-derived, so the boot-time log line
and the health state can never disagree. /opt/service is sys.path[0] because the
healthcheck is invoked as `python /opt/service/healthcheck.py`.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path

from validate_models import health_verdict, manifest_sha256


STATE = Path(os.getenv("COMFYUI_MODEL_VALIDATION_STATE", "/tmp/comfyui-model-validation.json"))


def check() -> str:
    state = json.loads(STATE.read_text(encoding="utf-8"))
    if state.get("manifest_sha256") != manifest_sha256():
        raise RuntimeError("validation state does not describe the current model manifest")
    healthy, reason = health_verdict(state)
    if not healthy:
        raise RuntimeError(reason)
    with urllib.request.urlopen("http://127.0.0.1:8188/system_stats", timeout=5) as response:
        stats = json.load(response)
    devices = stats.get("devices") or {}
    if isinstance(devices, dict):
        devices = list(devices.values())
    if len(devices) != 1:
        raise RuntimeError(f"expected exactly one visible GPU, got {len(devices)}")
    return reason


if __name__ == "__main__":
    try:
        print(f"ok: {check()}")
    except Exception as exc:
        print(f"unhealthy: {exc}", file=sys.stderr)
        raise SystemExit(1)
