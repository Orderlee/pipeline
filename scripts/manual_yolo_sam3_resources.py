from __future__ import annotations

import csv
import json
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import psutil


@dataclass(frozen=True)
class ResourceTarget:
    model_name: str
    container_name: str
    pid: int
    process_label: str


def _run_command(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        raise RuntimeError(stderr or f"command_failed:{' '.join(cmd)}")
    return proc.stdout or ""


def _safe_float(value: object) -> float | None:
    rendered = str(value or "").strip()
    if not rendered or rendered == "-":
        return None
    try:
        return float(rendered)
    except (TypeError, ValueError):
        return None


def _percentile(values: list[float], value: float) -> float | None:
    if not values:
        return None
    return round(float(np.percentile(np.asarray(values, dtype=float), value)), 2)


def _summarize_metric(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"sample_count": 0, "avg": None, "max": None, "p95": None}
    return {
        "sample_count": len(values),
        "avg": round(float(np.mean(np.asarray(values, dtype=float))), 2),
        "max": round(float(np.max(np.asarray(values, dtype=float))), 2),
        "p95": _percentile(values, 95),
    }


def _parse_docker_top(container_name: str) -> ResourceTarget:
    output = _run_command(["docker", "top", container_name, "-eo", "pid,ppid,comm,args"])
    lines = [line.rstrip() for line in output.splitlines() if line.strip()]
    if len(lines) < 2:
        raise RuntimeError(f"docker_top_empty:{container_name}")

    chosen: tuple[int, str] | None = None
    for line in lines[1:]:
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        pid_raw, _ppid, comm, args = parts
        try:
            pid = int(pid_raw)
        except (TypeError, ValueError):
            continue
        comm_lower = comm.lower()
        args_lower = args.lower()
        if "python" in comm_lower and "app.py" in args_lower:
            chosen = (pid, comm)
            break
    if chosen is None:
        raise RuntimeError(f"service_process_not_found:{container_name}")
    return ResourceTarget(
        model_name="",
        container_name=container_name,
        pid=int(chosen[0]),
        process_label=chosen[1],
    )


def _parse_gpu_uuid_map() -> dict[str, int]:
    output = _run_command(
        [
            "nvidia-smi",
            "--query-gpu=index,uuid",
            "--format=csv,noheader,nounits",
        ]
    )
    uuid_map: dict[str, int] = {}
    for line in output.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 2:
            continue
        try:
            uuid_map[parts[1]] = int(parts[0])
        except (TypeError, ValueError):
            continue
    return uuid_map


def _parse_compute_apps(uuid_map: dict[str, int]) -> dict[int, dict[str, float | int | None]]:
    output = _run_command(
        [
            "nvidia-smi",
            "--query-compute-apps=pid,gpu_uuid,used_gpu_memory",
            "--format=csv,noheader,nounits",
        ]
    )
    mapping: dict[int, dict[str, float | int | None]] = {}
    for line in output.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 3:
            continue
        try:
            pid = int(parts[0])
        except (TypeError, ValueError):
            continue
        gpu_uuid = parts[1]
        used_gpu_memory_mb = _safe_float(parts[2])
        mapping[pid] = {
            "gpu_index": uuid_map.get(gpu_uuid),
            "gpu_uuid": gpu_uuid,
            "gpu_mem_used_mb": used_gpu_memory_mb,
        }
    return mapping


def _parse_nvidia_pmon() -> dict[int, dict[str, float | int | None]]:
    output = _run_command(["nvidia-smi", "pmon", "-s", "um", "-c", "1"])
    mapping: dict[int, dict[str, float | int | None]] = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 11)
        if len(parts) < 10:
            continue
        try:
            gpu_index = int(parts[0])
            pid = int(parts[1])
        except (TypeError, ValueError):
            continue
        mapping[pid] = {
            "gpu_index": gpu_index,
            "gpu_util_percent": _safe_float(parts[3]),
            "gpu_mem_util_percent": _safe_float(parts[4]),
            "gpu_mem_used_mb": _safe_float(parts[9]) if len(parts) > 9 else None,
        }
    return mapping


class ModelResourceSampler:
    def __init__(
        self,
        *,
        output_dir: Path,
        sample_interval_sec: float = 1.0,
        yolo_container_name: str = "pipeline-yolo-1",
        sam3_container_name: str = "pipeline-sam3-1",
    ) -> None:
        self.output_dir = output_dir
        self.sample_interval_sec = max(0.2, float(sample_interval_sec))
        self._target_specs = {
            "yolo": yolo_container_name,
            "sam3": sam3_container_name,
        }
        self._targets: dict[str, ResourceTarget] = {}
        self._processes: dict[str, psutil.Process] = {}
        self._samples: dict[str, list[dict[str, Any]]] = {"yolo": [], "sam3": []}
        self._errors: list[str] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_at: datetime | None = None
        self._stopped_at: datetime | None = None

    def start(self) -> None:
        self._started_at = datetime.now()
        for model_name, container_name in self._target_specs.items():
            target = _parse_docker_top(container_name)
            target = ResourceTarget(
                model_name=model_name,
                container_name=container_name,
                pid=target.pid,
                process_label=target.process_label,
            )
            process = psutil.Process(target.pid)
            process.cpu_percent(interval=None)
            self._targets[model_name] = target
            self._processes[model_name] = process
        self._thread = threading.Thread(target=self._run, name="model-resource-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        self._stopped_at = datetime.now()
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(5.0, self.sample_interval_sec * 3.0))
        self._write_csvs()
        return self._build_summary()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            started = time.time()
            try:
                self._sample_once()
            except Exception as exc:  # noqa: BLE001
                self._errors.append(str(exc))
            elapsed = time.time() - started
            remaining = self.sample_interval_sec - elapsed
            if remaining > 0:
                self._stop_event.wait(remaining)

    def _sample_once(self) -> None:
        timestamp = datetime.now().isoformat()
        elapsed_sec = (
            (datetime.now() - self._started_at).total_seconds() if self._started_at is not None else 0.0
        )
        uuid_map = _parse_gpu_uuid_map()
        compute_apps = _parse_compute_apps(uuid_map)
        pmon = _parse_nvidia_pmon()

        for model_name, target in self._targets.items():
            process = self._processes[model_name]
            sample: dict[str, Any] = {
                "timestamp": timestamp,
                "elapsed_sec": round(elapsed_sec, 3),
                "model": model_name,
                "container_name": target.container_name,
                "pid": target.pid,
                "process_label": target.process_label,
                "cpu_percent": None,
                "gpu_index": None,
                "gpu_util_percent": None,
                "gpu_mem_util_percent": None,
                "gpu_mem_used_mb": None,
                "sample_status": "ok",
            }
            try:
                if process.is_running():
                    sample["cpu_percent"] = round(float(process.cpu_percent(interval=None)), 2)
                else:
                    sample["sample_status"] = "process_not_running"
            except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
                sample["sample_status"] = f"cpu_unavailable:{type(exc).__name__}"

            gpu_sample = pmon.get(target.pid) or {}
            compute_sample = compute_apps.get(target.pid) or {}
            sample["gpu_index"] = gpu_sample.get("gpu_index", compute_sample.get("gpu_index"))
            sample["gpu_util_percent"] = gpu_sample.get("gpu_util_percent")
            sample["gpu_mem_util_percent"] = gpu_sample.get("gpu_mem_util_percent")
            sample["gpu_mem_used_mb"] = gpu_sample.get("gpu_mem_used_mb")
            if sample["gpu_mem_used_mb"] is None:
                sample["gpu_mem_used_mb"] = compute_sample.get("gpu_mem_used_mb")
            if sample["gpu_index"] is None and sample["sample_status"] == "ok":
                sample["sample_status"] = "gpu_unavailable"

            self._samples[model_name].append(sample)

    def _write_csvs(self) -> None:
        for model_name, records in self._samples.items():
            path = self.output_dir / f"resource_usage_{model_name}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "timestamp",
                        "elapsed_sec",
                        "model",
                        "container_name",
                        "pid",
                        "process_label",
                        "cpu_percent",
                        "gpu_index",
                        "gpu_util_percent",
                        "gpu_mem_util_percent",
                        "gpu_mem_used_mb",
                        "sample_status",
                    ],
                )
                writer.writeheader()
                for record in records:
                    writer.writerow(record)

    def _build_summary(self) -> dict[str, Any]:
        status = "ok"
        if self._errors:
            status = "degraded"

        models: dict[str, Any] = {}
        for model_name, records in self._samples.items():
            cpu_values = [float(value) for value in [record.get("cpu_percent") for record in records] if value is not None]
            gpu_util_values = [float(value) for value in [record.get("gpu_util_percent") for record in records] if value is not None]
            gpu_mem_util_values = [
                float(value) for value in [record.get("gpu_mem_util_percent") for record in records] if value is not None
            ]
            gpu_mem_used_values = [
                float(value) for value in [record.get("gpu_mem_used_mb") for record in records] if value is not None
            ]
            if not records:
                status = "degraded"
            sample_statuses = sorted({str(record.get("sample_status") or "ok") for record in records})
            if any(state != "ok" for state in sample_statuses):
                status = "degraded"
            target = self._targets.get(model_name)
            models[model_name] = {
                "container_name": target.container_name if target else self._target_specs.get(model_name),
                "pid": target.pid if target else None,
                "process_label": target.process_label if target else None,
                "sample_interval_sec": self.sample_interval_sec,
                "sample_count": len(records),
                "sample_statuses": sample_statuses,
                "csv_path": str(self.output_dir / f"resource_usage_{model_name}.csv"),
                "cpu_percent": _summarize_metric(cpu_values),
                "gpu_util_percent": _summarize_metric(gpu_util_values),
                "gpu_mem_util_percent": _summarize_metric(gpu_mem_util_values),
                "gpu_mem_used_mb": _summarize_metric(gpu_mem_used_values),
                "gpu_indices": sorted(
                    {
                        int(value)
                        for value in [record.get("gpu_index") for record in records]
                        if value is not None and str(value).strip() != ""
                    }
                ),
            }

        summary = {
            "resource_metrics_status": status,
            "sampling_started_at": self._started_at.isoformat() if self._started_at else None,
            "sampling_completed_at": self._stopped_at.isoformat() if self._stopped_at else None,
            "sampling_elapsed_sec": round(
                (self._stopped_at - self._started_at).total_seconds(), 2
            )
            if self._started_at and self._stopped_at
            else None,
            "errors": list(self._errors),
            "models": models,
        }
        summary["yolo"] = models.get("yolo", {})
        summary["sam3"] = models.get("sam3", {})
        return summary
