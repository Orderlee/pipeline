"""archive 경로 해석 및 timeout-wrapped shutil.move.

NAS hang 방지를 위한 timeout wrapper와 __N suffix 충돌 해결 유틸을 포함한다.
이 모듈의 로직은 운영 안전성에 직결되므로 변경 시 주의.
"""

from __future__ import annotations

import shutil
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path, PurePosixPath

from vlm_pipeline.lib.env_utils import int_env

DEFAULT_ARCHIVE_MOVE_TIMEOUT_SEC: int = 300

# ── archive 경로 충돌 해결 상수 ──────────────────────────────────
ARCHIVE_MAX_SUFFIX_ATTEMPTS: int = 100
ARCHIVE_FIND_MAX_SUFFIX: int = 10


def _move_with_timeout(src: str, dst: str, timeout_sec: int | None = None) -> None:
    """shutil.move를 스레드 기반 타임아웃으로 감싸 NAS hang 방지."""
    if timeout_sec is None:
        timeout_sec = int_env("ARCHIVE_MOVE_TIMEOUT_SEC", DEFAULT_ARCHIVE_MOVE_TIMEOUT_SEC, minimum=10)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(shutil.move, src, dst)
        try:
            future.result(timeout=timeout_sec)
        except FuturesTimeoutError:
            future.cancel()
            raise OSError(
                f"archive_move_timeout: {timeout_sec}s 초과 — src={src} dst={dst}"
            ) from None


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
