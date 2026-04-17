"""archive 완료 후 빈 디렉토리 / 잔존 source 정리."""

from __future__ import annotations

import shutil
from pathlib import Path

ARCHIVE_CLEANUP_MAX_DEPTH: int = 5
ARCHIVE_CLEANUP_MAX_PARENT_LEVELS: int = 8


def cleanup_empty_tree(root: Path, max_depth: int = ARCHIVE_CLEANUP_MAX_DEPTH) -> None:
    """하위가 비어 있으면 상향식으로 빈 디렉토리를 정리.

    NFS 환경에서 os.walk는 느리므로 max_depth를 제한한다.
    """
    if not root.exists() or not root.is_dir():
        return

    def _cleanup_recursive(current: Path, depth: int) -> bool:
        if depth > max_depth:
            return False
        try:
            children = list(current.iterdir())
        except OSError:
            return False

        if not children:
            try:
                current.rmdir()
                return True
            except OSError:
                return False

        all_removed = True
        for child in children:
            if child.is_dir():
                if not _cleanup_recursive(child, depth + 1):
                    all_removed = False
            else:
                all_removed = False

        if all_removed:
            try:
                current.rmdir()
                return True
            except OSError:
                pass
        return False

    _cleanup_recursive(root, 0)


def cleanup_empty_parent_chain(start: Path | None, *, stop_at: Path | None = None, max_levels: int = ARCHIVE_CLEANUP_MAX_PARENT_LEVELS) -> None:
    """비어 있는 부모 디렉토리를 상향식으로 정리한다.

    `stop_at` 디렉토리는 경계로 취급하고 삭제하지 않는다.
    """
    current = start
    boundary: Path | None = None
    if stop_at is not None:
        try:
            boundary = stop_at.resolve()
        except OSError:
            boundary = stop_at

    for _ in range(max_levels):
        if current is None:
            return
        try:
            resolved_current = current.resolve()
        except OSError:
            resolved_current = current

        if boundary is not None and resolved_current == boundary:
            return

        try:
            current.rmdir()
            current = current.parent
            continue
        except FileNotFoundError:
            current = current.parent
            continue
        except OSError:
            return


def cleanup_residual_source_file(context, source_path: str) -> None:
    """archive 완료 후 source 파일이 남아있으면 정리한다."""
    src = Path(source_path)
    try:
        if not src.exists():
            return
    except OSError:
        return

    try:
        if src.is_file() or src.is_symlink():
            src.unlink()
            context.log.warning(f"archive 완료 후 incoming 잔존 파일 정리: {src}")
        elif src.is_dir():
            shutil.rmtree(src)
            context.log.warning(f"archive 완료 후 incoming 잔존 디렉토리 정리: {src}")
    except OSError as exc:
        context.log.error(
            "incoming 잔존 정리 실패(수동 확인 필요, 다음 auto_bootstrap에서 재감지 가능): "
            f"incoming_residual=true path={src} error={exc}"
        )
