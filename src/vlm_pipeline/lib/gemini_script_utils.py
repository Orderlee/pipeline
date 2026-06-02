"""Script-only helpers for batch Gemini video processing.

These utilities are used by standalone scripts and CLI entry points.
Runtime pipeline code should use GeminiAnalyzer directly.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .gemini import GeminiAnalyzer, extract_clean_json_text, load_clean_json

DEFAULT_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm"}


def json_save_path_same_dir_as_video(video_path: str) -> str:
    abs_video = os.path.abspath(video_path)
    video_dir = os.path.dirname(abs_video)
    base_name_no_ext = os.path.splitext(os.path.basename(abs_video))[0]
    return os.path.join(video_dir, base_name_no_ext + ".json")


def save_response_as_json(response_text: str, video_path: str) -> str:
    json_path = json_save_path_same_dir_as_video(video_path)
    try:
        parsed = load_clean_json(response_text)
    except json.JSONDecodeError:
        cleaned = extract_clean_json_text(response_text)
        Path(json_path).write_text(cleaned, encoding="utf-8")
        return json_path

    Path(json_path).write_text(
        json.dumps(parsed, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return json_path


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


def process_video_folder(
    folder: str,
    *,
    analyzer: GeminiAnalyzer,
    recursive: bool = False,
    prompt: str | None = None,
    mime_type: str | None = None,
) -> dict[str, Any]:
    videos = list_video_files(folder, recursive=recursive)
    summary: dict[str, Any] = {
        "folder": str(Path(folder).resolve()),
        "recursive": bool(recursive),
        "total": len(videos),
        "success": 0,
        "failed": 0,
        "outputs": [],
        "errors": [],
    }
    for video_path in videos:
        try:
            response_text = analyzer.analyze_video(video_path, prompt=prompt, mime_type=mime_type)
            json_path = save_response_as_json(response_text, video_path)
            summary["success"] += 1
            summary["outputs"].append({"video_path": video_path, "json_path": json_path})
        except Exception as exc:  # noqa: BLE001
            summary["failed"] += 1
            summary["errors"].append({"video_path": video_path, "error": str(exc)})
    return summary
