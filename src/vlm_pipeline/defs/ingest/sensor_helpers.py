"""INGEST sensor 공통 헬퍼 — cursor 파싱, run_key 생성 등.

sensor_incoming, sensor_stuck_guard, sensor_bootstrap에서 공유.
"""

from __future__ import annotations

import json
import time
from hashlib import sha1
from pathlib import Path

from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.lib.env_utils import int_env

INGEST_MANIFEST_JOB_NAMES = {
    "ingest_job",
    "mvp_stage_job",
}


def parse_cursor(raw_cursor: str | None) -> dict[str, int]:
    """cursor JSON 파싱 — 이전 상태 복원."""
    if not raw_cursor:
        return {}
    try:
        data = json.loads(raw_cursor)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    parsed: dict[str, int] = {}
    for key, value in data.items():
        try:
            parsed[str(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return parsed


def build_source_unit_run_key(
    source_unit_path: str,
    stable_signature: str,
    source_unit_dispatch_key: str = "",
    manifest_id: str = "",
) -> str:
    """manifest 단위 중복 방지용 run_key 생성.

    같은 source unit을 반복 테스트할 때도 manifest_id가 달라지면 새 run이 생성되어야 한다.
    manifest_id가 비어 있는 legacy 상황만 source unit + signature 조합으로 fallback 한다.
    """
    manifest_value = str(manifest_id or "").strip()
    if manifest_value:
        base = manifest_value
    else:
        source_value = source_unit_dispatch_key or source_unit_path or "<unknown_source_unit>"
        signature_value = stable_signature or "<unknown_signature>"
        base = f"{source_value}|{signature_value}"
    source_hash = sha1(base.encode("utf-8")).hexdigest()[:20]
    return f"incoming-unit-{source_hash}"


def read_manifest_payload(manifest_path: Path, context) -> tuple[dict, bool]:
    """manifest JSON 읽기 → (payload, corrupt).

    **corrupt 와 transient 를 반드시 구분한다.**
    - corrupt=True — 내용이 잘못됐다(JSON 파싱 실패 · UTF-8 디코드 실패 · 객체 아님).
      다시 읽어도 같으므로 호출부가 격리한다.
    - corrupt=False + 빈 payload — 읽기 자체가 실패했거나(OSError 계열: CIFS 지연,
      권한, 타임아웃) 내용이 비었다. **절대 격리하지 않는다** — NAS 장애 때 정상
      manifest 를 대량으로 잃는다. 파일은 그대로 두고 다음 tick 에 다시 본다.
    """
    try:
        raw = manifest_path.read_text(encoding="utf-8")
    except OSError as exc:
        context.log.warning(f"manifest 읽기 실패(일시적 — 유지): {manifest_path}: {exc}")
        return {}, False
    except UnicodeDecodeError as exc:
        context.log.warning(f"manifest JSON 파싱 실패: {manifest_path}: {exc}")
        return {}, True

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        context.log.warning(f"manifest JSON 파싱 실패: {manifest_path}: {exc}")
        return {}, True

    if not isinstance(payload, dict):
        context.log.warning(f"manifest 형식 오류(객체 아님): {manifest_path}")
        return {}, True
    return payload, False


def seconds_since_modified(manifest_path: Path) -> float | None:
    """마지막 수정 이후 경과 초. stat 실패 시 None(= 안정 여부 판단 불가)."""
    try:
        return max(0.0, time.time() - manifest_path.stat().st_mtime)
    except OSError:
        return None


def resolve_invalid_manifest_path(processed_dir: Path, manifest_path: Path) -> Path:
    base = processed_dir / f"{manifest_path.stem}.invalid.json"
    if not base.exists():
        return base
    index = 2
    while True:
        candidate = processed_dir / f"{manifest_path.stem}.invalid__{index}.json"
        if not candidate.exists():
            return candidate
        index += 1


def quarantine_invalid_manifest(manifest_path: Path, processed_dir: Path, context) -> bool:
    """손상 manifest 를 processed_dir 로 격리. 이동 실패는 warning 후 계속(fail-forward)."""
    destination = resolve_invalid_manifest_path(processed_dir, manifest_path)
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.rename(destination)
    except OSError as exc:
        context.log.warning(f"invalid manifest 격리 실패: {manifest_path} -> {destination}: {exc}")
        return False
    context.log.warning(f"invalid manifest 격리: {manifest_path.name} -> {destination.name}")
    return True


def load_pending_manifest_entries(
    manifests: list[Path],
    context,
    *,
    processed_dir: Path,
    in_flight_manifest_paths: set[str] | None = None,
) -> list[dict]:
    """pending manifest 를 엔트리로 적재. 손상된 것은 엔트리를 만들지 않고 격리한다.

    격리를 안 하면 빈 source_unit_path 엔트리가 만들어지고, build_source_unit_run_key 가
    빈 문자열에도 예외 없이 키를 돌려주기 때문에 RunRequest 까지 간다. 그 run 은
    ingest_manifest_flow._load_manifest_or_summary 의 무보호 json.loads 에서 죽고,
    manifest 는 이동되지 않아 다음 tick 에 같은 일이 반복된다.

    ## 오격리 방어선 두 겹 — 하나만으로는 부족하다

    1. **쓰기 안정화 대기**(주 방어선). pending/ 에 쓰는 writer 가 전부 원자적이지는 않다 —
       scripts/bootstrap_manifest.sh 는 셸 리다이렉트로 파일에 직접 스트리밍한다. 그 중간
       상태는 부분 JSON 이라 '손상'과 바이트 단위로 구분되지 않는다. 게다가 쓰는 중인 파일을
       rename 해도 열린 fd 는 inode 를 따라가므로, **완전히 유효한 manifest 가 .invalid.json
       이름으로 격리되고 writer 는 성공했다고 보고**한다. 그래서 최근 수정된 파일은 손상으로
       보여도 격리하지 않는다(MANIFEST_QUARANTINE_MIN_AGE_SEC, 기본 600초).
    2. **in-flight 가드**(보조). 진행 중 run 이 재작성하는 manifest 는 나이와 무관하게 보류한다.
       단 이 가드는 run 이 **이미 있는** manifest 만 덮는다 — 신규 생성 중인 파일은 태그가
       없어 구조적으로 못 막는다. 그래서 1번이 주 방어선이다.

    per-file fail-forward: 한 manifest 에서 예상 못 한 예외가 나도 나머지는 계속 처리한다
    (CLAUDE.md 파일 오류 정책). 안 그러면 파일 하나가 센서 tick 전체를 멈춘다.
    """
    entries: list[dict] = []
    in_flight = in_flight_manifest_paths or set()
    min_age_sec = max(0, int_env("MANIFEST_QUARANTINE_MIN_AGE_SEC", 600, 0))
    quarantined = 0
    held = 0

    for manifest_path in manifests:
        try:
            payload, corrupt = read_manifest_payload(manifest_path, context)
            if corrupt:
                age_sec = seconds_since_modified(manifest_path)
                if str(manifest_path) in in_flight:
                    held += 1
                    context.log.warning(f"manifest 파싱 실패했으나 run 진행 중 — 격리 보류: {manifest_path.name}")
                elif age_sec is None or age_sec < min_age_sec:
                    held += 1
                    context.log.warning(
                        f"manifest 파싱 실패했으나 쓰기 중일 수 있음 — 격리 보류: {manifest_path.name} "
                        f"(age={age_sec if age_sec is None else round(age_sec)}s < {min_age_sec}s)"
                    )
                elif quarantine_invalid_manifest(manifest_path, processed_dir, context):
                    quarantined += 1
                continue
            if not payload:
                # 일시적 읽기 실패이거나 내용이 빈 manifest. 파일은 두고 다음 tick 재시도한다.
                # 로그가 없으면 pending 에 영원히 남아도 아무도 모른다.
                context.log.warning(f"manifest 내용 없음 — 건너뜀(pending 잔류): {manifest_path.name}")
                continue

            source_unit_path = str(payload.get("source_unit_path", "")).strip()
            source_unit_dispatch_key = str(payload.get("source_unit_dispatch_key", "")).strip() or source_unit_path
            stable_signature = str(payload.get("stable_signature", "")).strip()
            manifest_id = str(payload.get("manifest_id", "") or manifest_path.stem).strip()
            retry_of_manifest_id = str(payload.get("retry_of_manifest_id", "")).strip()
            retry_reason = str(payload.get("retry_reason", "")).strip()
            try:
                retry_attempt = int(payload.get("retry_attempt", 0) or 0)
            except (TypeError, ValueError):
                retry_attempt = 0
            try:
                mtime_ns = int(manifest_path.stat().st_mtime_ns)
            except OSError:
                mtime_ns = 0
            entries.append(
                {
                    "path": manifest_path,
                    "payload": payload,
                    "source_unit_path": source_unit_path,
                    "source_unit_dispatch_key": source_unit_dispatch_key,
                    "stable_signature": stable_signature,
                    "manifest_id": manifest_id,
                    "retry_of_manifest_id": retry_of_manifest_id,
                    "retry_attempt": retry_attempt,
                    "retry_reason": retry_reason,
                    "mtime_ns": mtime_ns,
                }
            )
        except Exception as exc:  # noqa: BLE001 — per-file fail-forward
            context.log.warning(f"manifest 처리 실패(건너뜀): {manifest_path}: {exc}")

    if quarantined or held:
        context.log.info(f"manifest 판정: invalid_quarantined={quarantined} quarantine_held={held}")
    return entries


def source_unit_group_key(entry: dict) -> str:
    source_unit_dispatch_key = str(entry.get("source_unit_dispatch_key", "")).strip()
    if source_unit_dispatch_key:
        return source_unit_dispatch_key
    return f"manifest_path:{entry['path']}"


def select_latest_per_source_unit(entries: list[dict]) -> tuple[list[dict], list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for entry in entries:
        grouped.setdefault(source_unit_group_key(entry), []).append(entry)

    selected: list[dict] = []
    superseded: list[dict] = []
    for group_entries in grouped.values():
        ordered = sorted(
            group_entries,
            key=lambda row: (int(row.get("mtime_ns", 0)), str(row["path"])),
            reverse=True,
        )
        selected.append(ordered[0])
        superseded.extend(ordered[1:])
    return selected, superseded


def _dispatch_origin_key(entry: dict) -> str:
    """manifest의 출처 그룹 키를 반환.

    같은 source_unit_path를 가리키더라도 chunked(auto_bootstrap)와
    non-chunked(manual 등)는 서로 다른 출처로 구분한다.
    """
    key = str(entry.get("source_unit_dispatch_key", "")).strip()
    idx = key.find("#chunk:")
    if idx >= 0:
        return f"{key[:idx]}#chunked"
    return key


def supersede_by_stable_signature(
    selected: list[dict],
) -> tuple[list[dict], list[dict]]:
    """같은 stable_signature를 가진 manifest 중 서로 다른 출처의 중복 커버리지를 제거.

    1차 dispatch_key 기반 supersede 이후, 같은 폴더(같은 signature)를 가리키는
    서로 다른 출처(auto_bootstrap vs manual_reingest 등)의 manifest가 동시에
    pending일 때 최신 세트만 남기고 나머지를 supersede한다.
    """
    sig_groups: dict[str, list[dict]] = {}
    no_sig: list[dict] = []
    for entry in selected:
        sig = str(entry.get("stable_signature", "")).strip()
        if not sig:
            no_sig.append(entry)
            continue
        sig_groups.setdefault(sig, []).append(entry)

    final_selected: list[dict] = list(no_sig)
    extra_superseded: list[dict] = []

    for _sig, group in sig_groups.items():
        if len(group) <= 1:
            final_selected.extend(group)
            continue

        origin_subgroups: dict[str, list[dict]] = {}
        for e in group:
            origin = _dispatch_origin_key(e)
            origin_subgroups.setdefault(origin, []).append(e)

        if len(origin_subgroups) <= 1:
            final_selected.extend(group)
            continue

        best_origin = max(
            origin_subgroups,
            key=lambda k: max(int(e.get("mtime_ns", 0)) for e in origin_subgroups[k]),
        )
        for origin, subgroup in origin_subgroups.items():
            if origin == best_origin:
                final_selected.extend(subgroup)
            else:
                extra_superseded.extend(subgroup)

    return final_selected, extra_superseded


def resolve_superseded_path(processed_dir: Path, manifest_path: Path) -> Path:
    base = processed_dir / f"{manifest_path.stem}.superseded.json"
    if not base.exists():
        return base
    index = 2
    while True:
        candidate = processed_dir / f"{manifest_path.stem}.superseded__{index}.json"
        if not candidate.exists():
            return candidate
        index += 1


def move_superseded_manifests(entries: list[dict], processed_dir: Path, context) -> int:
    moved = 0
    for entry in entries:
        manifest_path = entry["path"]
        if not manifest_path.exists():
            continue
        destination = resolve_superseded_path(processed_dir, manifest_path)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.rename(destination)
            moved += 1
            context.log.info(f"중복 pending manifest 정리(superseded): {manifest_path.name} -> {destination.name}")
        except OSError as exc:
            context.log.warning(f"superseded manifest 이동 실패: {manifest_path} -> {destination}: {exc}")
    return moved


def collect_in_flight_runs(context) -> list:
    try:
        runs = context.instance.get_runs(
            filters=RunsFilter(
                statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED],
            ),
            limit=200,
        )
        return [run for run in runs if str(getattr(run, "job_name", "") or "") in INGEST_MANIFEST_JOB_NAMES]
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"in-flight run 조회 실패(백프레셔 약화): {exc}")
        return []


def collect_in_flight_source_units(context, runs: list | None = None) -> set[str]:
    if runs is None:
        runs = collect_in_flight_runs(context)
    if not runs:
        return set()

    try:
        source_units: set[str] = set()
        for run in runs:
            tags = getattr(run, "tags", {}) or {}
            source_unit_path = str(tags.get("source_unit_dispatch_key", "") or tags.get("source_unit_path", "")).strip()
            if source_unit_path:
                source_units.add(source_unit_path)
        return source_units
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"in-flight source_unit 수집 실패(중복 방어 약화): {exc}")
        return set()


def collect_in_flight_manifest_paths(context, runs: list | None = None) -> set[str]:
    """진행 중 run 이 붙잡고 있는 manifest 경로 집합.

    load_pending_manifest_entries 의 오격리 **보조** 방어선 입력이다. 주 방어선은 쓰기
    안정화 대기 쪽이다 — 이 집합은 collect_in_flight_runs 가 보는 QUEUED/STARTED run 만
    담으므로 STARTING·CANCELING 이나 **아직 run 이 없는 신규 생성 manifest 는 못 덮는다.**
    완전성을 가정하지 말 것.

    빈 집합을 돌려주면 이 가드가 조용히 꺼지므로(예외를 삼키는 경로 포함) 따로 테스트한다.
    태그 키는 sensor_incoming 이 RunRequest 에 싣는 "manifest_path" 와 같아야 한다.
    """
    if runs is None:
        runs = collect_in_flight_runs(context)
    if not runs:
        return set()

    try:
        paths: set[str] = set()
        for run in runs:
            tags = getattr(run, "tags", {}) or {}
            manifest_path = str(tags.get("manifest_path", "") or "").strip()
            if manifest_path:
                paths.add(manifest_path)
        return paths
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"in-flight manifest 경로 수집 실패(오격리 방어 약화): {exc}")
        return set()


def manifest_retry_state(context, manifest_id: str) -> tuple[bool, int, str]:
    """manifest_id 기준 최근 실행 상태를 바탕으로 재시도 여부를 계산."""
    normalized_id = str(manifest_id or "").strip()
    if not normalized_id:
        return False, 0, "NONE"

    try:
        recent_runs = context.instance.get_runs(
            filters=RunsFilter(tags={"manifest_id": normalized_id}),
            limit=50,
        )
        recent_runs = [
            run for run in recent_runs if str(getattr(run, "job_name", "") or "") in INGEST_MANIFEST_JOB_NAMES
        ]
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"manifest retry state 조회 실패: {normalized_id}: {exc}")
        return False, 0, "LOOKUP_ERROR"

    if not recent_runs:
        return False, 0, "NONE"

    latest_status = getattr(recent_runs[0], "status", None)
    failed_statuses = {DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED}
    failed_run_count = sum(1 for run in recent_runs if getattr(run, "status", None) in failed_statuses)
    should_retry_failed = latest_status in failed_statuses
    # latest_status 가 None 이거나 .name 속성 없을 때 안전 stringify (pyright Optional 가드 명시).
    if latest_status is None:
        latest_status_name = "NONE"
    elif hasattr(latest_status, "name"):
        latest_status_name = str(latest_status.name)
    else:
        latest_status_name = str(latest_status)
    return should_retry_failed, int(failed_run_count), latest_status_name
