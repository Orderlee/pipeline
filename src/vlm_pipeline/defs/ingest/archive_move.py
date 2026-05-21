"""archive 경로 해석 및 timeout-wrapped os.rename / shutil.move.

NAS hang 방지를 위한 timeout wrapper와 __N suffix 충돌 해결 유틸을 포함한다.
이 모듈의 로직은 운영 안전성에 직결되므로 변경 시 주의.

2026-05-21 fix:
  - Python 3.10/3.11/3.12 의 shutil.move(directory) 는 cross-device 와 무관하게
    **항상 copytree+rmtree** 사용 (Python 3.13+ 만 os.rename 우선 시도). 결과적으로
    NFS same-mount 안에서도 폴더 단위 fast-path 가 recursive copy 로 떨어져 매우 느렸음.
  - 그래서 폴더 단위 호출은 os.rename 우선 — cross-device 면 OSError EXDEV 발생,
    caller 가 catch 해서 per-file path 로 fallback.
  - 추가: ThreadPoolExecutor 의 `with` 패턴이 __exit__ 에서 `shutdown(wait=True)` 호출
    → timeout 후에도 inner task 완료 대기 → hang. `try/finally` +
    `shutdown(wait=False, cancel_futures=True)` 로 timeout 즉시 raise.
"""

from __future__ import annotations

import os
import shutil
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path, PurePosixPath
from typing import Callable

from vlm_pipeline.lib.env_utils import int_env

DEFAULT_ARCHIVE_MOVE_TIMEOUT_SEC: int = 300

# ── archive 경로 충돌 해결 상수 ──────────────────────────────────
ARCHIVE_MAX_SUFFIX_ATTEMPTS: int = 100
ARCHIVE_FIND_MAX_SUFFIX: int = 10


def _run_with_timeout(func: Callable, *args, timeout_sec: int, op_label: str) -> None:
    """func(*args) 호출을 timeout 으로 감싼다. timeout 시 즉시 OSError raise.

    `with ThreadPoolExecutor(...)` 의 __exit__ 가 `shutdown(wait=True)` 호출하므로
    timeout 후에도 inner task 완료 대기 → hang. `try/finally` + `cancel_futures=True`
    로 즉시 종료. orphan thread 가 남을 수 있으나 dispatch_stage_job 종료 시 process
    spawn 새로 시작되므로 leak 누적 없음.
    """
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        future = pool.submit(func, *args)
        try:
            future.result(timeout=timeout_sec)
        except FuturesTimeoutError:
            raise OSError(
                f"archive_move_timeout: {timeout_sec}s 초과 — op={op_label}"
            ) from None
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


def _move_with_timeout(src: str, dst: str, timeout_sec: int | None = None) -> None:
    """폴더 또는 파일을 timeout 으로 archive 로 이동.

    동작 분기:
    - **directory**: os.rename 만 시도. Python 3.10/11/12 의 shutil.move(dir) 는 항상
      copytree+rmtree fallback 이라 NFS same-mount 에서도 매우 느림. os.rename 으로
      atomic 한 directory rename 만 시도하고, EXDEV 등 실패 시 OSError raise →
      caller 가 per-file path 로 fallback 결정.
    - **file**: shutil.move 사용. same-fs 면 os.rename, cross-device 면 copy+unlink.
      파일 단위는 fallback 시간 영향 작아서 그대로 사용.
    """
    if timeout_sec is None:
        timeout_sec = int_env("ARCHIVE_MOVE_TIMEOUT_SEC", DEFAULT_ARCHIVE_MOVE_TIMEOUT_SEC, minimum=10)
    op_label = f"src={src} dst={dst}"
    if Path(src).is_dir():
        # 폴더 fast-path: atomic os.rename 만. EXDEV/ENOTEMPTY 등은 caller 에서 per-file path 진입.
        # destination parent dir 가 없으면 os.rename 이 ENOENT — 이전 shutil.move 가 copytree 통해
        # 자동 mkdir 하던 동작을 명시적으로 복원.
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        _run_with_timeout(os.rename, src, dst, timeout_sec=timeout_sec, op_label=f"rename(dir) {op_label}")
    else:
        # 파일: shutil.move 가 same-fs rename + cross-device copy+unlink 자동 처리.
        _run_with_timeout(shutil.move, src, dst, timeout_sec=timeout_sec, op_label=f"move(file) {op_label}")


def resolve_archive_source_unit_name(source_unit_name: str) -> str:
    """archive 디렉토리용 source unit 이름을 정규화한다.

    GCP auto-bootstrap unit은 `gcp/<bucket>/...` 형태로 들어오지만,
    archive 저장은 `archive/<bucket>/...` 규칙을 사용한다.
    """
    raw = str(source_unit_name or "").strip().strip("/")
    if not raw:
        return ""

    parts = [part for part in PurePosixPath(raw).parts if part not in {"", ".", ".."}]
    if len(parts) >= 2 and parts[0].lower() == "gcp":
        parts = parts[1:]

    if not parts:
        return ""
    return str(PurePosixPath(*parts))


def resolve_unique_directory(target_dir: Path, max_attempts: int = ARCHIVE_MAX_SUFFIX_ATTEMPTS) -> Path:
    """경로 충돌 시 __2, __3 suffix를 붙여 유니크 디렉토리 경로 생성."""
    if not target_dir.exists():
        return target_dir
    for idx in range(2, max_attempts + 2):
        candidate = target_dir.with_name(f"{target_dir.name}__{idx}")
        if not candidate.exists():
            return candidate
    raise OSError(f"Cannot find unique directory after {max_attempts} attempts: {target_dir}")


def resolve_unique_file(target_file: Path, max_attempts: int = ARCHIVE_MAX_SUFFIX_ATTEMPTS) -> Path:
    """경로 충돌 시 __2, __3 suffix를 붙여 유니크 파일 경로 생성."""
    if not target_file.exists():
        return target_file
    stem = target_file.stem
    suffix = target_file.suffix
    for idx in range(2, max_attempts + 2):
        candidate = target_file.with_name(f"{stem}__{idx}{suffix}")
        if not candidate.exists():
            return candidate
    raise OSError(f"Cannot find unique file after {max_attempts} attempts: {target_file}")


def find_existing_archive_directory(base_dir: Path, max_suffix: int = ARCHIVE_FIND_MAX_SUFFIX) -> Path | None:
    """archive 대상 디렉토리(base + __N) 중 실제 존재 경로를 탐색."""
    if base_dir.exists() and base_dir.is_dir():
        return base_dir
    for idx in range(2, max_suffix + 1):
        candidate = base_dir.with_name(f"{base_dir.name}__{idx}")
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def find_existing_archive_candidate(base_target: Path, max_suffix: int = ARCHIVE_FIND_MAX_SUFFIX) -> Path | None:
    """archive 대상 경로(base + __N) 중 실제 존재 경로를 탐색.

    NFS 환경에서 exists() 호출은 네트워크 I/O를 발생시키므로 max_suffix를 작게 유지.
    """
    if base_target.exists():
        return base_target

    stem = base_target.stem
    suffix = base_target.suffix
    for idx in range(2, max_suffix + 1):
        candidate = base_target.with_name(f"{stem}__{idx}{suffix}")
        if candidate.exists():
            return candidate
    return None


def find_existing_in_archive_unit_dirs(
    archive_root_dir: Path,
    source_unit_name: str,
    rel_path: str,
    max_suffix: int = ARCHIVE_FIND_MAX_SUFFIX,
) -> Path | None:
    """archive unit 디렉토리에서 파일 존재 여부 탐색.

    NFS 환경에서 exists() 호출은 네트워크 I/O를 발생시키므로 max_suffix를 작게 유지.
    """
    rel = Path(rel_path)
    base_dir = archive_root_dir / source_unit_name
    base_target = base_dir / rel

    found = find_existing_archive_candidate(base_target, max_suffix=max_suffix)
    if found is not None:
        return found

    for idx in range(2, max_suffix + 1):
        candidate_dir = archive_root_dir / f"{source_unit_name}__{idx}"
        if not candidate_dir.exists():
            break
        found = find_existing_archive_candidate(candidate_dir / rel, max_suffix=max_suffix)
        if found is not None:
            return found
    return None


def is_source_missing_error(exc: Exception) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) == 2:
        return True
    return "[Errno 2]" in str(exc)
