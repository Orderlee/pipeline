"""Restricted ComfyUI adapter for the two repository-owned local image workflows."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from db import pg

from .base import AdapterDeferredError, PollResult, SubmitResult


_LOG = logging.getLogger("genai.comfy_local")


class ComfyLocalError(RuntimeError):
    pass


class ComfyLocalAdapter:
    engine = "comfy_local"
    output_media = "image"
    is_synchronous = False
    output_ext = ".png"

    def __init__(self) -> None:
        self.api_base = os.getenv("COMFYUI_INTERNAL_URL", "http://comfyui:8188").rstrip("/")
        self.contract_root = Path(os.getenv("COMFYUI_CONTRACT_ROOT", "/app/comfy_contract"))
        self.default_workflow = os.getenv("COMFYUI_DEFAULT_WORKFLOW", "flux2-klein-4b-edit-v1")
        configured = os.getenv(
            "COMFYUI_ALLOWED_WORKFLOWS",
            "flux2-klein-4b-edit-v1,sdxl-inpaint-cctv-v1",
        )
        self.allowed_workflows = {item.strip() for item in configured.split(",") if item.strip()}
        self.timeout = int(os.getenv("COMFYUI_HTTP_TIMEOUT_SECONDS", "60"))
        self.download_timeout = int(os.getenv("COMFYUI_DOWNLOAD_TIMEOUT_SECONDS", "300"))
        self.lease_ttl = int(os.getenv("COMFYUI_LEASE_TTL_SECONDS", "1200"))
        # Wall-clock ceiling for one generation.  A job past this is failed and its GPU0
        # lease released, whatever ComfyUI says — reachability is not liveness.
        self.job_deadline = float(os.getenv("COMFYUI_JOB_TIMEOUT_SECONDS", "900"))
        self.min_free_vram_gb = float(os.getenv("COMFYUI_MIN_FREE_VRAM_GB", "13"))
        self.max_output_bytes = int(os.getenv("COMFYUI_MAX_OUTPUT_BYTES", str(100 * 1024 * 1024)))
        self.embedding_url = os.getenv("EMBEDDING_API_URL", "http://embedding-service:8003").rstrip("/")
        self.coordinate_embedding = os.getenv(
            "COMFYUI_EMBEDDING_COORDINATION_REQUIRED", "true"
        ).strip().lower() in {"1", "true", "yes"}
        self.warm_embedding = os.getenv("COMFYUI_EMBEDDING_WARMUP_AFTER", "true").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        self.default_negative_prompt = (
            "illustration, painting, text, watermark, distorted anatomy, duplicate person, "
            "changed camera angle, changed background"
        )

    @staticmethod
    def _sha256_bytes(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _sha256_file(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _template(self, workflow_id: str) -> tuple[dict, str]:
        if workflow_id not in self.allowed_workflows:
            raise ComfyLocalError(
                f"workflow_id={workflow_id!r} not allowed; allowed={sorted(self.allowed_workflows)}"
            )
        if not workflow_id.replace("-", "").isalnum():
            raise ComfyLocalError("invalid workflow_id")
        path = self.contract_root / "workflows" / f"{workflow_id}.json"
        raw = path.read_bytes()
        template = json.loads(raw)
        if template.get("workflow_id") != workflow_id or not isinstance(template.get("prompt"), dict):
            raise ComfyLocalError(f"invalid workflow contract: {workflow_id}")
        return template, self._sha256_bytes(raw)

    @staticmethod
    def _bind(template: dict, name: str, value) -> None:
        try:
            node_id, input_name = template["bindings"][name]
            template["prompt"][str(node_id)]["inputs"][input_name] = value
        except (KeyError, TypeError, ValueError) as exc:
            raise ComfyLocalError(f"workflow binding missing: {name}") from exc

    @staticmethod
    def _graph_sampler(template: dict) -> str | None:
        """Sampler name the contract's graph actually uses, or None when it has none.

        Read out of the graph instead of a per-workflow constant so the record cannot
        drift from the JSON: FLUX.2 carries it on KSamplerSelect, SDXL on KSampler.
        Deterministic — nodes are visited in sorted id order and only a literal
        ``sampler_name`` counts (a wired input is a ``[node, slot]`` list, not a name).
        """
        nodes = template.get("prompt") or {}
        for node_id in sorted(nodes, key=lambda key: (len(str(key)), str(key))):
            value = ((nodes.get(node_id) or {}).get("inputs") or {}).get("sampler_name")
            if isinstance(value, str) and value:
                return value
        return None

    def _request(self, method: str, path: str, **kwargs):
        try:
            response = requests.request(
                method,
                f"{self.api_base}{path}",
                timeout=kwargs.pop("timeout", self.timeout),
                **kwargs,
            )
            response.raise_for_status()
            return response
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if 400 <= status < 500 and status not in {408, 409, 429}:
                detail = exc.response.text[:1000] if exc.response is not None else str(exc)
                raise ComfyLocalError(f"ComfyUI rejected workflow ({status}): {detail}") from exc
            raise AdapterDeferredError(f"ComfyUI transient HTTP error: {exc}") from exc
        except requests.RequestException as exc:
            raise AdapterDeferredError(f"ComfyUI unavailable: {exc}") from exc

    def _embedding_post(self, path: str, *, required: bool = True, **params) -> None:
        try:
            response = requests.post(
                f"{self.embedding_url}{path}", params=params, data=params, timeout=self.timeout
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            if required:
                raise AdapterDeferredError(f"embedding GPU coordination failed: {exc}") from exc

    def _clear_queue_best_effort(self) -> None:
        for path, payload in (
            ("/interrupt", {}),
            ("/queue", {"clear": True}),
            ("/free", {"unload_models": True, "free_memory": True}),
        ):
            try:
                self._request("POST", path, json=payload)
            except (AdapterDeferredError, ComfyLocalError):
                pass

    def _prepare_gpu(self, job_id: str) -> None:
        if self.coordinate_embedding:
            self._embedding_post(
                "/maintenance/enter",
                owner_run_id=job_id,
                ttl_seconds=self.lease_ttl,
                note="comfy_local generation",
            )
            try:
                self._embedding_post("/unload", target="pe_core")
            except Exception:
                self._embedding_post("/maintenance/exit", required=False, owner_run_id=job_id)
                raise

        stats = self._request("GET", "/system_stats").json()
        devices = stats.get("devices") or []
        if isinstance(devices, dict):
            devices = list(devices.values())
        if len(devices) != 1:
            raise ComfyLocalError(f"GPU isolation violation: visible devices={len(devices)}")
        device = devices[0] or {}
        free = device.get("vram_free") or device.get("vram_free_total")
        if free is not None and float(free) / (1024**3) < self.min_free_vram_gb:
            raise AdapterDeferredError(
                f"GPU0 free VRAM below threshold: {float(free)/(1024**3):.1f}GB "
                f"< {self.min_free_vram_gb:.1f}GB"
            )

        queue = self._request("GET", "/queue").json()
        if queue.get("queue_running") or queue.get("queue_pending"):
            # The PG lease was acquired above.  A non-empty queue here can only be
            # orphaned work from an expired/crashed owner; clear it before giving
            # GPU0 back to the embedding service.
            self._clear_queue_best_effort()
            raise AdapterDeferredError("cleared orphaned ComfyUI queue; retry later")

    def _upload(self, data: bytes, filename: str) -> str:
        suffix = Path(filename or "input.png").suffix.lower()
        if suffix not in {".png", ".jpg", ".jpeg", ".webp"}:
            suffix = ".png"
        safe_name = f"genai-{secrets.token_hex(12)}{suffix}"
        response = self._request(
            "POST",
            "/upload/image",
            files={"image": (safe_name, data, "application/octet-stream")},
            data={"type": "input", "overwrite": "false"},
        ).json()
        name = str(response.get("name") or "")
        subfolder = str(response.get("subfolder") or "")
        if not name or Path(name).name != name or ".." in PurePosixPath(subfolder).parts:
            raise ComfyLocalError("unsafe upload response from ComfyUI")
        return f"{subfolder}/{name}" if subfolder else name

    def _release_resources(
        self,
        provider_prompt_id: str,
        reason: str,
        owner_job_id: str | None = None,
        lease_token: str | None = None,
    ) -> None:
        """Hand GPU0 back.  ``lease_token`` fences the release to one acquisition.

        Only submit() holds a token (it just called acquire).  poll()/cancel()/
        download_result() run in a later process off a fresh adapter, so they pass
        none and fall back to the owner match — safe because the owner they release
        is resolved from the *prompt-scoped* provenance row, which a retry of the
        same job overwrites, leaving a stale attempt with no owner to act on.
        """
        owner = pg.complete_genai_job_provenance(provider_prompt_id, None) or owner_job_id
        try:
            requests.post(
                f"{self.api_base}/free",
                json={"unload_models": True, "free_memory": True},
                timeout=self.timeout,
            )
        except requests.RequestException:
            pass
        if owner and not pg.release_generation_gpu_lease(owner, reason, lease_token):
            # Never swallow this: the row is held by a newer acquisition (or already
            # released/expired), so GPU0 is somebody else's now.  Nothing to repair —
            # the TTL steal in acquire_generation_gpu_lease is the recovery path —
            # but an operator chasing a stuck GPU0 must see that we did not free it.
            _LOG.warning(
                "GPU0 lease release refused: owner=%s reason=%s token=%s prompt=%s — "
                "lease belongs to another acquisition; expiry/TTL steal will reclaim it",
                owner,
                reason,
                "presented" if lease_token else "absent",
                provider_prompt_id,
            )
        if self.coordinate_embedding:
            # owner 를 실어야 남의 정비창(trainer/SAM3)을 닫지 않는다 — 어긋나면 409,
            # required=False 라 조용히 지나간다. owner 미상이면 예전처럼 무검증 해제.
            self._embedding_post("/maintenance/exit", required=False, owner_run_id=owner)
            if self.warm_embedding:
                self._embedding_post("/warmup", required=False, target="pe_core")

    def submit(self, image_bytes, image_filename, prompt, options=None):
        opts = dict(options or {})
        job_id = str(opts.pop("_job_id", "")).strip()
        mask_bytes = opts.pop("_mask_bytes", None)
        mask_filename = str(opts.pop("_mask_filename", "mask.png"))
        if not job_id:
            raise ComfyLocalError("internal job id missing")
        workflow_id = str(opts.get("workflow_id") or self.default_workflow)
        native_client_id = str(opts.get("native_client_id") or "").strip()
        if native_client_id and not all(
            char.isalnum() or char in "_.:-" for char in native_client_id
        ):
            raise ComfyLocalError("invalid native Comfy client id")
        if len(native_client_id) > 128:
            raise ComfyLocalError("native Comfy client id too long")
        if workflow_id == "sdxl-inpaint-cctv-v1" and not mask_bytes:
            raise ComfyLocalError("sdxl-inpaint-cctv-v1 requires exactly one mask")
        if workflow_id != "sdxl-inpaint-cctv-v1" and mask_bytes:
            raise ComfyLocalError(f"workflow {workflow_id} does not accept a mask")

        seed = int(opts.get("seed") or secrets.randbelow(2**63 - 1))
        if seed < 0 or seed >= 2**63:
            raise ComfyLocalError("seed must be in [0, 2^63)")
        steps = int(opts.get("steps") or (4 if workflow_id.startswith("flux2-") else 30))
        cfg = float(opts.get("cfg") or 6.0)
        denoise = float(opts.get("denoise") or 0.85)
        negative_prompt = str(opts.get("negative_prompt") or self.default_negative_prompt)
        if workflow_id.startswith("flux2-") and steps != 4:
            raise ComfyLocalError("FLUX.2 Klein distilled workflow requires steps=4")
        if not 1 <= steps <= 60 or not 0.0 <= cfg <= 20.0 or not 0.0 < denoise <= 1.0:
            raise ComfyLocalError("invalid steps/cfg/denoise")

        template, workflow_sha = self._template(workflow_id)
        # A parameter counts as used only if the contract wires it into the graph.
        # FLUX.2 Klein binds no negative_prompt/cfg/denoise (its cfg is fixed at 1.0
        # inside CFGGuider), so applying them would be a no-op and recording them would
        # assert a value the run never used.  Unbound stays None all the way to the
        # provenance row, where it is written as an explicit null.
        bound = set(template.get("bindings") or {})
        sampler = self._graph_sampler(template)
        applied_negative = negative_prompt if "negative_prompt" in bound else None
        applied_cfg = cfg if "cfg" in bound else None
        applied_denoise = denoise if "denoise" in bound else None
        manifest_path = self.contract_root / "model_manifest.json"
        manifest_sha = self._sha256_file(manifest_path)
        # `lease` is this acquisition's fencing token, not a boolean.  Every release
        # below presents it so a hung earlier attempt of the *same* job cannot free
        # GPU0 out from under the retry that now owns it.
        lease = pg.acquire_generation_gpu_lease(job_id, self.lease_ttl)
        if not lease:
            raise AdapterDeferredError("GPU0 Comfy lease is busy")

        prompt_id: str | None = None
        try:
            self._prepare_gpu(job_id)
            source_name = self._upload(image_bytes, image_filename)
            self._bind(template, "source_image", source_name)
            self._bind(template, "prompt", prompt.strip())
            self._bind(template, "seed", seed)
            self._bind(template, "steps", steps)
            self._bind(template, "filename_prefix", f"genai/{job_id}")
            if mask_bytes:
                mask_name = self._upload(mask_bytes, mask_filename)
                self._bind(template, "mask_image", mask_name)
            if applied_negative is not None:
                self._bind(template, "negative_prompt", applied_negative)
            if applied_cfg is not None:
                self._bind(template, "cfg", applied_cfg)
            if applied_denoise is not None:
                self._bind(template, "denoise", applied_denoise)

            result = self._request(
                "POST",
                "/prompt",
                json={
                    "prompt": template["prompt"],
                    "client_id": native_client_id or f"genai-{job_id}",
                },
            ).json()
            prompt_id = str(result.get("prompt_id") or "")
            if not prompt_id:
                raise ComfyLocalError(f"ComfyUI response missing prompt_id: {result}")
            pg.upsert_genai_job_provenance(
                {
                    "job_id": job_id,
                    "workflow_id": workflow_id,
                    "workflow_sha256": workflow_sha,
                    "model_manifest_sha256": manifest_sha,
                    "prompt_sha256": self._sha256_bytes(prompt.strip().encode("utf-8")),
                    "negative_prompt_sha256": (
                        None
                        if applied_negative is None
                        else self._sha256_bytes(applied_negative.encode("utf-8"))
                    ),
                    "seed": seed,
                    "input_sha256": self._sha256_bytes(image_bytes),
                    "mask_sha256": self._sha256_bytes(mask_bytes) if mask_bytes else None,
                    "provider_prompt_id": prompt_id,
                    "params": {
                        "workflow_version": int(template.get("version") or 1),
                        "sampler": sampler,
                        "steps": steps,
                        "cfg": applied_cfg,
                        "denoise": applied_denoise,
                    },
                }
            )
            return SubmitResult(provider_job_id=prompt_id, is_synchronous=False)
        except Exception:
            if prompt_id:
                self._clear_queue_best_effort()
                self._release_resources(
                    prompt_id, "submit_error", owner_job_id=job_id, lease_token=lease
                )
            else:
                if not pg.release_generation_gpu_lease(job_id, "submit_error", lease):
                    _LOG.warning(
                        "GPU0 lease release refused on submit_error: job=%s — this "
                        "attempt's token no longer matches, so a newer acquisition "
                        "holds GPU0 and must keep it",
                        job_id,
                    )
                if self.coordinate_embedding:
                    self._embedding_post("/maintenance/exit", required=False, owner_run_id=job_id)
            raise

    def poll(self, provider_job_id: str) -> PollResult:
        owner = pg.provenance_owner_for_prompt(provider_job_id)
        response = self._request("GET", f"/history/{provider_job_id}").json()
        history = response.get(provider_job_id)
        # Never extend a lease for a job that cannot finish.  ComfyUI answering 200 with
        # an empty history (worker restarted, queue cleared) used to be reported as
        # "running" forever while each tick refreshed the GPU0 lease and the embedding
        # maintenance flag — that pinned the embedding service at 503 for three days.
        # Both leases are now bounded by a wall-clock deadline, not by reachability.
        _status = (history or {}).get("status") or {}
        _terminal = bool(history) and (
            str(_status.get("status_str") or "").lower() in {"error", "failed"}
            or bool(_status.get("completed"))
        )
        if not _terminal:
            age = pg.provenance_age_seconds(provider_job_id)
            if age is not None and age > self.job_deadline:
                self._release_resources(
                    provider_job_id, "deadline_exceeded", owner_job_id=owner
                )
                return PollResult(
                    status="failed",
                    error_message=(
                        f"comfy_local job exceeded {self.job_deadline:.0f}s deadline "
                        f"(age {age:.0f}s, history={'missing' if not history else 'incomplete'})"
                    ),
                )
        # Do not extend either lease while ComfyUI is unreachable.  Their TTLs
        # are the crash-recovery path that prevents GPU0 from remaining blocked.
        if owner and history:
            if not pg.heartbeat_generation_gpu_lease(owner, self.lease_ttl):
                # Losing the lease mid-flight used to be invisible: the bool was
                # dropped and polling carried on as if GPU0 were still ours.
                _LOG.warning(
                    "GPU0 lease heartbeat refused: owner=%s prompt=%s — the lease "
                    "expired or was stolen; this job no longer owns GPU0",
                    owner,
                    provider_job_id,
                )
            if self.coordinate_embedding:
                self._embedding_post("/maintenance/heartbeat", required=False, owner_run_id=owner)
        if not history:
            return PollResult(status="running")
        status = history.get("status") or {}
        status_str = str(status.get("status_str") or "").lower()
        if status_str in {"error", "failed"}:
            messages = status.get("messages") or []
            self._release_resources(provider_job_id, "comfy_failed")
            return PollResult(status="failed", error_message=json.dumps(messages)[-1000:])
        if not status.get("completed"):
            return PollResult(status="running")

        outputs = history.get("outputs") or {}
        images: list[dict] = []
        for node_output in outputs.values():
            images.extend(node_output.get("images") or [])
        if len(images) != 1:
            self._release_resources(provider_job_id, "invalid_output_count")
            return PollResult(
                status="failed",
                error_message=f"expected exactly one image output, got {len(images)}",
            )
        image = images[0]
        filename = str(image.get("filename") or "")
        subfolder = str(image.get("subfolder") or "")
        output_type = str(image.get("type") or "output")
        if (
            not filename
            or Path(filename).name != filename
            or Path(filename).suffix.lower() != ".png"
            or PurePosixPath(subfolder).is_absolute()
            or ".." in PurePosixPath(subfolder).parts
            or output_type != "output"
        ):
            self._release_resources(provider_job_id, "unsafe_output")
            return PollResult(status="failed", error_message="unsafe ComfyUI output metadata")
        query = urlencode(
            {
                "filename": filename,
                "subfolder": subfolder,
                "type": output_type,
                "prompt_id": provider_job_id,
            }
        )
        return PollResult(status="done", result_url=f"comfy://view?{query}", cost_units=0.0)

    def download_result(self, result_url: str) -> bytes:
        parsed = urlparse(result_url)
        params = parse_qs(parsed.query)
        prompt_id = (params.get("prompt_id") or [""])[0]
        try:
            if parsed.scheme != "comfy" or parsed.netloc != "view" or not prompt_id:
                raise ComfyLocalError("invalid internal Comfy result URL")
            view_params = {
                key: (params.get(key) or [""])[0]
                for key in ("filename", "subfolder", "type")
            }
            response = self._request(
                "GET", "/view", params=view_params, timeout=self.download_timeout, stream=True
            )
            content_type = (response.headers.get("content-type") or "").split(";", 1)[0].lower()
            blob = response.content
            if content_type != "image/png" or not blob.startswith(b"\x89PNG\r\n\x1a\n"):
                raise ComfyLocalError(f"unexpected Comfy output content-type={content_type!r}")
            if len(blob) > self.max_output_bytes:
                raise ComfyLocalError(f"Comfy output exceeds {self.max_output_bytes} bytes")
            pg.complete_genai_job_provenance(prompt_id, self._sha256_bytes(blob))
            return blob
        finally:
            if prompt_id:
                self._release_resources(prompt_id, "completed")

    def cancel(self, provider_job_id: str) -> None:
        try:
            self._clear_queue_best_effort()
        finally:
            self._release_resources(provider_job_id, "cancelled")
