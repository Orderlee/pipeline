"""auto_bootstrap_manifest_sensor — incoming 미디어 파일 유입 시 안정화 후 pending manifest 자동 생성 (facade).

실제 구현은 5개 submodule로 분리됨:
- ``sensor_bootstrap_cursor``   — cursor v4 serialization (⚠️ 바이트 보존)
- ``sensor_bootstrap_helpers``  — dir 순회, dispatch 트리거, done marker, gcp 판정, chunking
- ``sensor_bootstrap_discover`` — incoming 디렉토리 탐색
- ``sensor_bootstrap_scan``     — unit 파일 스캔, timeout budget, tick 선택
- ``sensor_bootstrap_policy``   — BootstrapPolicy dataclass + decide_unit_ready / decide_eligible_units
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

from dagster import DefaultSensorStatus, SkipReason, sensor

from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.lib.network_probe import probe_path_reachable
from vlm_pipeline.lib.runtime_profile import resolve_runtime_profile
from vlm_pipeline.resources.config import PipelineConfig

from .runtime_policy import (
    auto_bootstrap_manifest_archive_requested,
    auto_bootstrap_unit_allowed,
)
from .sensor_bootstrap_cursor import (
    _build_auto_bootstrap_cursor_payload,
    _parse_auto_bootstrap_cursor,
)
from .sensor_bootstrap_discover import _discover_source_units
from .sensor_bootstrap_helpers import (
    _chunk_files,
    _has_allowed_direct_file,
    _has_done_marker,
    _is_gcp_unit_path,
    _iter_sorted_dir_entries,
    _load_dispatch_requested_folders,
    _source_unit_sort_key,
)
from .sensor_bootstrap_policy import (
    BootstrapPolicy,
    decide_eligible_units,
    decide_unit_ready,
)
from .sensor_bootstrap_scan import (
    _effective_auto_bootstrap_max_units_per_tick,
    _effective_auto_bootstrap_scan_budget_sec,
    _scan_discovered_units,
    _scan_unit_media_files,
    _select_units_for_tick,
)

__all__ = [
    "BootstrapPolicy",
    "_build_auto_bootstrap_cursor_payload",
    "_chunk_files",
    "decide_eligible_units",
    "decide_unit_ready",
    "_discover_source_units",
    "_effective_auto_bootstrap_max_units_per_tick",
    "_effective_auto_bootstrap_scan_budget_sec",
    "_has_allowed_direct_file",
    "_has_done_marker",
    "_is_gcp_unit_path",
    "_iter_sorted_dir_entries",
    "_load_dispatch_requested_folders",
    "_parse_auto_bootstrap_cursor",
    "_scan_discovered_units",
    "_scan_unit_media_files",
    "_select_units_for_tick",
    "_source_unit_sort_key",
    "auto_bootstrap_manifest_sensor",
]


@sensor(
    minimum_interval_seconds=int_env("AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC", 180, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="incoming 미디어 파일 유입 시 안정화 후 pending manifest 자동 생성",
)
def auto_bootstrap_manifest_sensor(context):
    """incoming 파일을 감지해 pending manifest를 자동 생성한다.

    조건:
      1) 동일 signature가 stable_cycles 이상 연속 관측
      2) 마지막 수정 시각이 stable_age_sec 이상 경과

    staging profile: **incoming/gcp** 트리만 스캔 (트리거 JSON 없이 GCS 경로만).

    production: auto_bootstrap도 **incoming/gcp/** 만 스캔한다.
    `incoming/tmp_data_2` 같은 직접 드롭 폴더는 `.dispatch/pending` 트리거 JSON 없이는 처리하지 않는다.
    """
    config = PipelineConfig()
    runtime_profile = resolve_runtime_profile()
    incoming_dir = Path(config.incoming_dir)
    pending_dir = Path(config.manifest_dir) / "pending"

    # NAS 도달성 가드 (#3, 2026-05-28): NFS hard-mount hang 시 mkdir/glob/discover 가
    # 300s 까지 매달려 gRPC DEADLINE_EXCEEDED 유발. 짧은 stat() probe 로 빠른 skip.
    reachable, probe_reason = probe_path_reachable(incoming_dir)
    if not reachable:
        return SkipReason(f"NAS incoming 도달 불가 ({probe_reason}). 다음 tick 재시도.")

    pending_dir.mkdir(parents=True, exist_ok=True)
    policy = BootstrapPolicy.from_env()

    pending_manifests = sorted(pending_dir.glob("*.json"), key=lambda p: str(p))
    pending_count = len(pending_manifests)
    if pending_count >= policy.max_pending_manifests:
        return SkipReason(
            f"pending backlog 보호: auto bootstrap 중단 (pending={pending_count}, limit={policy.max_pending_manifests})"
        )

    previous_units, previous_scan_offset, previous_discovery_offset = _parse_auto_bootstrap_cursor(context.cursor)
    dispatch_requested_folders = _load_dispatch_requested_folders(incoming_dir / ".dispatch" / "pending")

    if not incoming_dir.exists():
        context.update_cursor(
            json.dumps(
                _build_auto_bootstrap_cursor_payload({}, scan_offset=0, discovery_offset=0),
                ensure_ascii=False,
            )
        )
        return SkipReason(f"incoming 디렉토리 없음: {incoming_dir}")

    discovery_elapsed_sec = 0.0
    scan_elapsed_sec = 0.0
    scanned_unit_count = 0
    scan_completed_window = True
    try:
        discovery_started = time.perf_counter()
        # Discovery(디렉토리 스캔) 시간 제한. NAS 지연 시 60초 상한으로 gRPC 타임아웃 여유 확보
        discovery_budget_sec = min(60, max(10, int(policy.scan_budget_sec * 0.5)))
        discovered_units, next_discovery_offset = _discover_source_units(
            incoming_dir,
            policy.allowed_exts,
            discovery_budget_sec,
            discovery_start_index=previous_discovery_offset,
            max_top_entries_per_tick=policy.discovery_max_top_entries,
            excluded_top_level_names=dispatch_requested_folders,
        )
        discovered_units = [
            u
            for u in discovered_units
            if auto_bootstrap_unit_allowed(
                str(u.get("unit_path", "")),
                incoming_dir=incoming_dir,
                config=config,
                runtime_profile=runtime_profile,
            )
        ]
        discovery_elapsed_sec = time.perf_counter() - discovery_started

        # 파일 메타 정보 읽기 스캔에는 남은 예산을 사용합니다
        remaining_budget = max(5, policy.scan_budget_sec - int(discovery_elapsed_sec))

        units_to_scan, _ = _select_units_for_tick(
            discovered_units,
            scan_offset=previous_scan_offset,
            max_units_per_tick=policy.max_units_per_tick,
        )
        units, scanned_unit_keys, scanned_unit_count, scan_elapsed_sec, scan_completed_window = _scan_discovered_units(
            units_to_scan,
            policy.allowed_exts,
            scan_budget_sec=remaining_budget,
        )
        total_discovered_units = len(discovered_units)
        if total_discovered_units > 0:
            next_scan_offset = (previous_scan_offset + scanned_unit_count) % total_discovered_units
        else:
            next_scan_offset = 0
    except Exception as exc:  # noqa: BLE001
        return SkipReason(f"incoming 스캔 실패: {exc}")

    if not discovered_units:
        context.update_cursor(
            json.dumps(
                _build_auto_bootstrap_cursor_payload({}, scan_offset=0, discovery_offset=next_discovery_offset),
                ensure_ascii=False,
            )
        )
        return SkipReason("incoming에 처리 가능한 미디어 unit 없음")

    if scan_elapsed_sec >= 5:
        scanned_file_count = sum(len(unit.get("files", [])) for unit in units.values())
        context.log.info(
            f"auto_bootstrap scan 통계: discovery_elapsed={discovery_elapsed_sec:.2f}s, "
            f"scan_elapsed={scan_elapsed_sec:.2f}s, "
            f"discovered_units={len(discovered_units)}, requested_scan_window={len(units_to_scan)}, "
            f"processed_units={scanned_unit_count}, media_units={len(units)}, "
            f"scanned_files={scanned_file_count}, budget={policy.scan_budget_sec}s, "
            f"partial_scan={'true' if not scan_completed_window else 'false'}"
        )

    now_ns = time.time_ns()
    active_unit_keys = {str(unit["unit_key"]) for unit in discovered_units}
    carry_forward_units: dict[str, dict] = {}
    for unit_key, previous in previous_units.items():
        if unit_key not in active_unit_keys or unit_key in scanned_unit_keys:
            continue
        try:
            stable_cycles = int(previous.get("stable_cycles", 0))
        except (TypeError, ValueError):
            stable_cycles = 0
        carry_forward_units[unit_key] = {
            "signature": str(previous.get("signature", "")),
            "stable_cycles": max(stable_cycles, 0),
            "manifested_signature": str(previous.get("manifested_signature", "")),
        }

    pending_unit_signatures: set[tuple[str, str]] = set()
    pending_unit_paths: set[str] = set()
    for pending_manifest in pending_manifests:
        try:
            payload = json.loads(pending_manifest.read_text(encoding="utf-8"))
        except Exception:
            continue
        pending_unit_path = str(payload.get("source_unit_path", "")).strip()
        pending_signature = str(payload.get("stable_signature", "")).strip()
        if pending_unit_path:
            pending_unit_paths.add(pending_unit_path)
            if pending_signature:
                pending_unit_signatures.add((pending_unit_path, pending_signature))

    ready_units, scanned_next_units, waiting_done_marker_count = decide_eligible_units(
        units,
        previous_units,
        pending_unit_paths,
        pending_unit_signatures,
        now_ns,
        policy,
        incoming_dir,
    )
    next_units = {**carry_forward_units, **scanned_next_units}

    deferred_ready_units_count = max(0, len(ready_units) - policy.max_ready_units_per_tick)
    if deferred_ready_units_count > 0:
        ready_units = ready_units[: policy.max_ready_units_per_tick]

    if not ready_units:
        context.update_cursor(
            json.dumps(
                _build_auto_bootstrap_cursor_payload(
                    next_units,
                    scan_offset=next_scan_offset,
                    discovery_offset=next_discovery_offset,
                ),
                ensure_ascii=False,
            )
        )
        reason = (
            f"복사 안정화 대기 중: units={len(units)} "
            f"(scan_window={scanned_unit_count}/{len(units_to_scan)}/{len(discovered_units)}), "
            f"criteria=cycles>={policy.stable_cycles_required}, age>={policy.stable_age_sec}s"
        )
        if waiting_done_marker_count > 0:
            reason += f", done_marker_waiting={waiting_done_marker_count}, marker={policy.done_marker_name}"
        if deferred_ready_units_count > 0:
            reason += f", deferred_ready={deferred_ready_units_count}"
        if not scan_completed_window:
            reason += f", partial_scan={len(units_to_scan) - scanned_unit_count}, scan_budget={policy.scan_budget_sec}s"
            if policy.skip_on_partial_scan:
                reason += ", cursor_resumes_next_tick=true"
        if scan_elapsed_sec >= 1:
            reason += f", scan_elapsed={scan_elapsed_sec:.1f}s"
        return SkipReason(reason)

    created_manifests: list[str] = []
    deferred_manifest_budget_count = 0
    for index, (unit_key, unit) in enumerate(ready_units, start=1):
        signature = str(unit["signature"])
        unit_files = list(unit["files"])
        manifest_unit_type = str(unit["unit_type"])
        manifest_unit_name = str(unit["unit_name"])
        manifest_unit_path = str(unit["unit_path"])

        unit_file_chunks = _chunk_files(unit_files, policy.max_files_per_manifest)
        chunk_count = len(unit_file_chunks)
        if len(created_manifests) + chunk_count > policy.max_new_manifests_per_tick:
            deferred_manifest_budget_count += 1
            continue
        unit_manifest_failed = False

        for chunk_index, chunk_files in enumerate(unit_file_chunks, start=1):
            now = datetime.now()
            manifest_id = f"auto_bootstrap_{now:%Y%m%d_%H%M%S_%f}_{index:03d}_{chunk_index:03d}"
            manifest_filename = f"{manifest_id}.json"
            source_unit_dispatch_key = f"{manifest_unit_path}#chunk:{chunk_index:04d}/{chunk_count:04d}"

            manifest = {
                "manifest_id": manifest_id,
                "generated_at": now.isoformat(),
                "source_dir": str(incoming_dir),
                "source_unit_type": manifest_unit_type,
                "source_unit_path": manifest_unit_path,
                "source_unit_name": manifest_unit_name,
                "source_unit_dispatch_key": source_unit_dispatch_key,
                "source_unit_total_file_count": len(unit_files),
                "source_unit_chunk_index": chunk_index,
                "source_unit_chunk_count": chunk_count,
                "stable_signature": signature,
                "transfer_tool": "auto_bootstrap_sensor",
                "archive_requested": auto_bootstrap_manifest_archive_requested(
                    {"source_unit_path": manifest_unit_path},
                    config=config,
                    runtime_profile=runtime_profile,
                ),
                # auto_bootstrap 진입 단계에서는 MinIO 업로드를 보류한다.
                # dispatch JSON / dispatch-agent API 가 도착할 때 upload_enabled=True 로
                # 별도 manifest 가 생성돼 실제 MinIO upload + 라벨링이 진행된다.
                "upload_enabled": False,
                "file_count": len(chunk_files),
                "files": [
                    {
                        "path": str(row["path"]),
                        "size": row["size"],
                        "rel_path": row.get("rel_path", Path(row["path"]).name),
                    }
                    for row in chunk_files
                ],
            }

            try:
                (pending_dir / manifest_filename).write_text(
                    json.dumps(manifest, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception as exc:  # noqa: BLE001
                context.log.warning(f"manifest 저장 실패: {manifest_filename}: {exc}")
                unit_manifest_failed = True
                break

            created_manifests.append(manifest_filename)
            context.log.info(
                f"auto_bootstrap: manifest 생성 완료 — {manifest_filename} "
                f"({len(chunk_files)} files, chunk={chunk_index}/{chunk_count}, unit={manifest_unit_path})"
            )

        if not unit_manifest_failed:
            next_units[unit_key]["manifested_signature"] = signature

    context.update_cursor(
        json.dumps(
            _build_auto_bootstrap_cursor_payload(
                next_units,
                scan_offset=next_scan_offset,
                discovery_offset=next_discovery_offset,
            ),
            ensure_ascii=False,
        )
    )

    if not created_manifests:
        if deferred_manifest_budget_count > 0:
            return SkipReason(
                f"manifest 생성 보류: tick당 생성 예산 초과 "
                f"(limit={policy.max_new_manifests_per_tick}, deferred_units={deferred_manifest_budget_count})"
            )
        return SkipReason("manifest 저장 실패: 생성된 파일 없음")

    suffix = f", deferred_ready={deferred_ready_units_count}" if deferred_ready_units_count > 0 else ""
    if deferred_manifest_budget_count > 0:
        suffix += (
            f", deferred_manifest_budget={deferred_manifest_budget_count}, "
            f"max_new_manifests_per_tick={policy.max_new_manifests_per_tick}"
        )
    if not scan_completed_window:
        suffix += f", partial_scan={len(units_to_scan) - scanned_unit_count}, scan_budget={policy.scan_budget_sec}s"
    return SkipReason(
        f"manifest 생성 완료: {len(created_manifests)}개 "
        f"(안정화 기준 cycles>={policy.stable_cycles_required}, age>={policy.stable_age_sec}s, "
        f"scan_window={scanned_unit_count}/{len(units_to_scan)}/{len(discovered_units)}{suffix})"
    )
