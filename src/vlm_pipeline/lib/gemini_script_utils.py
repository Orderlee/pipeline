"""Script-only helpers for batch Gemini video processing.

Used by standalone scripts and CLI entry points to enumerate video files.
Runtime pipeline code should use GeminiAnalyzer directly.
"""

from __future__ import annotations

from pathlib import Path

DEFAULT_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm"}


def list_video_files(root_dir: str, recursive: bool = False) -> list[str]:
    root_path = Path(root_dir).resolve()
    if recursive:
        iterable = root_path.rglob("*")
    else:
        iterable = root_path.iterdir()

    collected: list[str] = []
    for path in iterable:
        if not path.is_file():
            continue
        if path.suffix.lower() not in DEFAULT_VIDEO_EXTENSIONS:
            continue
        collected.append(str(path))
    return sorted(collected)
