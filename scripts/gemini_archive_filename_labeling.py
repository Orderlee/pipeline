#!/usr/bin/env python3
"""Label archive videos with Gemini/Vertex against filename-derived anomaly labels.

This script walks one or more archive bucket roots, derives the expected anomaly
label from each video filename, asks Gemini whether that event is actually
present in the video, and writes per-video JSON plus summary artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import os
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import gettempdir
from typing import Any
from uuid import uuid4

from vlm_pipeline.lib.gemini import GeminiAnalyzer, extract_clean_json_text

VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
REQUEST_SLEEP_SEC = 1.0

EVENT_HINTS: dict[str, dict[str, str]] = {
    "fire": {
        "canonical": "fire",
        "description": "visible flames, burning, or a clear fire incident",
    },
    "smoke": {
        "canonical": "smoke",
        "description": "visible smoke emission, spreading smoke, or haze caused by smoke",
    },
    "falldown": {
        "canonical": "fall",
        "description": "a person falling down, collapsing, or ending up on the ground unexpectedly",
    },
    "fall": {
        "canonical": "fall",
        "description": "a person falling down, collapsing, or ending up on the ground unexpectedly",
    },
    "violence": {
        "canonical": "violence",
        "description": "physical assault, fighting, violent confrontation, or aggressive physical contact",
    },
}


@dataclass(frozen=True)
class VideoTask:
    bucket_name: str
    source_root: Path
    source_path: Path

    @property
    def rel_path(self) -> Path:
        return self.source_path.relative_to(self.source_root)

    @property
    def date_folder(self) -> str:
        parts = self.rel_path.parts
        return parts[0] if parts else ""

    @property
    def stem(self) -> str:
        return self.source_path.stem

    @property
    def expected_label(self) -> str:
        tokens = [tok for tok in re.split(r"[_\-\s]+", self.stem.lower()) if tok]
        return tokens[0] if tokens else "unknown"

    @property
    def expected_hint(self) -> dict[str, str]:
        return EVENT_HINTS.get(
            self.expected_label,
            {
                "canonical": self.expected_label,
                "description": f"the abnormal event labeled '{self.expected_label}'",
            },
        )


def _build_nonexistent_temp_path(suffix: str) -> Path:
    return Path(gettempdir()) / f"gemini_archive_label_{uuid4().hex}{suffix}"


def _cleanup_temp(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw_value = (os.getenv(name) or "").strip()
    try:
        parsed = int(raw_value) if raw_value else int(default)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), parsed)


def _target_preview_bitrate_kbps(*, duration_sec: float | int | None, target_bytes: int) -> int:
    try:
        duration_value = float(duration_sec or 0.0)
    except (TypeError, ValueError):
        duration_value = 0.0

    if duration_value <= 0:
        return 900

    bitrate_kbps = int((max(1, int(target_bytes)) * 8) / duration_value / 1000)
    return max(120, min(2500, bitrate_kbps))


def ffprobe_duration(video_path: Path) -> float | None:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=30)
        if result.returncode != 0:
            return None
        value = (result.stdout or "").strip()
        return float(value) if value else None
    except Exception:
        return None


def prepare_gemini_video_for_request(video_path: Path) -> tuple[Path, Path | None]:
    source_path = Path(video_path)
    source_size = source_path.stat().st_size
    actual_duration = ffprobe_duration(source_path) or 0.0

    safe_bytes = _int_env("GEMINI_SAFE_VIDEO_BYTES", 450 * 1024 * 1024, minimum=1)
    max_duration = _int_env("GEMINI_MAX_DURATION_SEC", 3600, minimum=60)
    request_limit = _int_env("GEMINI_MAX_REQUEST_BYTES", 524_288_000, minimum=safe_bytes)
    target_bytes = min(
        _int_env("GEMINI_PREVIEW_TARGET_BYTES", 120 * 1024 * 1024, minimum=1),
        safe_bytes,
    )

    needs_resize = source_size > safe_bytes
    needs_trim = actual_duration > max_duration if actual_duration > 0 else False
    if not needs_resize and not needs_trim:
        return source_path, None

    effective_duration = min(actual_duration, max_duration) if actual_duration > 0 else max_duration
    attempts = [
        {"target_bytes": target_bytes, "width": 960, "fps": 6},
        {"target_bytes": min(target_bytes, 80 * 1024 * 1024), "width": 640, "fps": 4},
        {"target_bytes": min(target_bytes, 48 * 1024 * 1024), "width": 480, "fps": 3},
    ]

    last_error = "preview_unknown_failure"
    for attempt in attempts:
        preview_path = _build_nonexistent_temp_path(".mp4")
        bitrate_kbps = _target_preview_bitrate_kbps(
            duration_sec=effective_duration,
            target_bytes=int(attempt["target_bytes"]),
        )
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(source_path),
        ]
        if needs_trim:
            cmd.extend(["-t", str(int(max_duration))])
        cmd.extend(
            [
                "-map",
                "0:v:0",
                "-an",
                "-vf",
                f"fps={int(attempt['fps'])},scale=w={int(attempt['width'])}:h=-2:force_original_aspect_ratio=decrease",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-pix_fmt",
                "yuv420p",
                "-b:v",
                f"{bitrate_kbps}k",
                "-maxrate",
                f"{bitrate_kbps}k",
                "-bufsize",
                f"{max(bitrate_kbps * 2, 256)}k",
                "-movflags",
                "+faststart",
                str(preview_path),
            ]
        )
        proc = subprocess.run(cmd, capture_output=True, check=False)
        if proc.returncode == 0 and preview_path.exists() and preview_path.stat().st_size > 0:
            if preview_path.stat().st_size <= request_limit:
                return preview_path, preview_path
            last_error = f"preview_too_large:{preview_path.stat().st_size}"
        else:
            stderr = (proc.stderr or b"").decode("utf-8", errors="ignore").strip()
            last_error = f"preview_ffmpeg_failed:{stderr or 'empty_output'}"
        _cleanup_temp(preview_path)

    raise RuntimeError(
        f"{last_error}; original_size={source_size} exceeds_safe_limit={safe_bytes}"
    )


def build_prompt(task: VideoTask) -> str:
    hint = task.expected_hint
    return (
        "You are validating whether the abnormal event implied by a surveillance video's filename "
        "actually appears in the video.\n\n"
        f"Expected filename label: {task.expected_label}\n"
        f"Canonical interpretation: {hint['canonical']}\n"
        f"Event description: {hint['description']}\n\n"
        "Watch the full video and decide whether the expected event clearly occurs at least once.\n"
        "If a different abnormal event appears instead, mention it.\n"
        "If the evidence is insufficient or ambiguous, return 'uncertain'.\n\n"
        "Return exactly one JSON object with these fields:\n"
        "{\n"
        '  "expected_label": "string",\n'
        '  "canonical_expected_event": "string",\n'
        '  "verdict": "matched | not_matched | uncertain",\n'
        '  "filename_event_present": true | false | null,\n'
        '  "confidence": 0.0,\n'
        '  "detected_events": [\n'
        '    {"label": "string", "timestamp": [start_sec, end_sec], "evidence": "string"}\n'
        "  ],\n"
        '  "summary_ko": "Korean summary",\n'
        '  "reasoning_ko": "Korean explanation"\n'
        "}\n\n"
        "Rules:\n"
        "- Base the answer only on evidence visible in the video.\n"
        "- If the expected event clearly occurs, set verdict='matched' and filename_event_present=true.\n"
        "- If another abnormal event occurs but not the expected one, set verdict='not_matched' and filename_event_present=false.\n"
        "- If nothing is clear enough, set verdict='uncertain' and filename_event_present=null.\n"
        "- Keep detected_events short and timestamped when possible.\n"
        "- Do not wrap the JSON in markdown code fences.\n"
    )


def iter_tasks(source_roots: list[Path], limit: int = 0):
    yielded = 0
    for source_root in source_roots:
        bucket_name = source_root.name
        for root, dirnames, filenames in os.walk(source_root):
            dirnames.sort()
            for filename in sorted(filenames):
                source_path = Path(root) / filename
                if source_path.suffix.lower() not in VIDEO_EXTENSIONS:
                    continue
                yield VideoTask(
                    bucket_name=bucket_name,
                    source_root=source_root,
                    source_path=source_path,
                )
                yielded += 1
                if limit > 0 and yielded >= limit:
                    return


def result_json_path(output_dir: Path, task: VideoTask) -> Path:
    return output_dir / "per_video" / task.bucket_name / task.rel_path.with_suffix(".json")


def analyze_task(
    analyzer: GeminiAnalyzer,
    output_dir: Path,
    task: VideoTask,
    *,
    overwrite: bool,
) -> dict[str, Any]:
    output_path = result_json_path(output_dir, task)
    if output_path.exists() and not overwrite:
        existing = json.loads(output_path.read_text(encoding="utf-8"))
        existing["status"] = existing.get("status", "skipped_existing")
        return existing

    output_path.parent.mkdir(parents=True, exist_ok=True)
    source_size = task.source_path.stat().st_size
    prepared_path: Path | None = None
    preview_path: Path | None = None
    raw_response = None
    started_at = datetime.now(timezone.utc)

    try:
        prepared_path, preview_path = prepare_gemini_video_for_request(task.source_path)
        guessed_mime, _ = mimetypes.guess_type(str(prepared_path))
        raw_response = analyzer.analyze_video(
            str(prepared_path),
            prompt=build_prompt(task),
            mime_type=guessed_mime or "video/mp4",
        )
        cleaned = extract_clean_json_text(raw_response)
        payload = json.loads(cleaned)
        if not isinstance(payload, dict):
            raise ValueError("Gemini response is not a JSON object")

        result = {
            "status": "ok",
            "bucket_name": task.bucket_name,
            "source_root": str(task.source_root),
            "source_path": str(task.source_path),
            "relative_path": str(task.rel_path),
            "date_folder": task.date_folder,
            "file_name": task.source_path.name,
            "file_size_bytes": source_size,
            "expected_label": task.expected_label,
            "canonical_expected_event": task.expected_hint["canonical"],
            "expected_event_description": task.expected_hint["description"],
            "model_expected_event_echo": payload.get("canonical_expected_event"),
            "verdict": payload.get("verdict"),
            "filename_event_present": payload.get("filename_event_present"),
            "confidence": payload.get("confidence"),
            "detected_events": payload.get("detected_events", []),
            "summary_ko": payload.get("summary_ko"),
            "reasoning_ko": payload.get("reasoning_ko"),
            "used_preview": preview_path is not None,
            "analysis_started_at": started_at.isoformat(),
            "analysis_completed_at": datetime.now(timezone.utc).isoformat(),
            "raw_response_text": raw_response,
        }
    except Exception as exc:  # noqa: BLE001
        result = {
            "status": "error",
            "bucket_name": task.bucket_name,
            "source_root": str(task.source_root),
            "source_path": str(task.source_path),
            "relative_path": str(task.rel_path),
            "date_folder": task.date_folder,
            "file_name": task.source_path.name,
            "file_size_bytes": source_size,
            "expected_label": task.expected_label,
            "used_preview": preview_path is not None,
            "analysis_started_at": started_at.isoformat(),
            "analysis_completed_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
            "raw_response_text": raw_response,
        }
    finally:
        _cleanup_temp(preview_path)

    output_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return result


def collect_result_rows(output_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    per_video_dir = output_dir / "per_video"
    if not per_video_dir.exists():
        return rows

    for json_path in sorted(per_video_dir.rglob("*.json")):
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        payload["result_json_path"] = str(json_path)
        rows.append(payload)
    return rows


def write_summary(output_dir: Path) -> None:
    rows = collect_result_rows(output_dir)
    summary_json_path = output_dir / "summary.json"
    summary_csv_path = output_dir / "summary.csv"

    stats = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_results": len(rows),
        "status_counts": {},
        "verdict_counts": {},
        "by_bucket": {},
    }
    for row in rows:
        status = str(row.get("status") or "unknown")
        verdict = str(row.get("verdict") or "")
        bucket_name = str(row.get("bucket_name") or "unknown")
        stats["status_counts"][status] = stats["status_counts"].get(status, 0) + 1
        if verdict:
            stats["verdict_counts"][verdict] = stats["verdict_counts"].get(verdict, 0) + 1
        stats["by_bucket"][bucket_name] = stats["by_bucket"].get(bucket_name, 0) + 1

    summary_json_path.write_text(
        json.dumps(
            {
                "stats": stats,
                "rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    fieldnames = [
        "status",
        "bucket_name",
        "date_folder",
        "file_name",
        "source_path",
        "relative_path",
        "expected_label",
        "canonical_expected_event",
        "expected_event_description",
        "verdict",
        "filename_event_present",
        "confidence",
        "used_preview",
        "error",
        "result_json_path",
    ]
    with summary_csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Gemini/Vertex filename-vs-video anomaly labeling on archive buckets.",
    )
    parser.add_argument(
        "--source-root",
        action="append",
        required=True,
        help="Archive bucket root to scan. May be passed multiple times.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write per-video JSON and summary artifacts.",
    )
    parser.add_argument(
        "--project",
        default=os.getenv("GEMINI_PROJECT", "gmail-361002"),
        help="Vertex AI project id.",
    )
    parser.add_argument(
        "--location",
        default=os.getenv("GEMINI_LOCATION", "us-central1"),
        help="Vertex AI location.",
    )
    parser.add_argument(
        "--credentials-path",
        default=os.getenv("GEMINI_GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Service account JSON path for Vertex authentication.",
    )
    parser.add_argument(
        "--model-name",
        default="gemini-2.5-flash",
        help="Gemini model name.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of videos to process. 0 means no limit.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-run videos even if result JSON already exists.",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=REQUEST_SLEEP_SEC,
        help="Sleep interval between Gemini requests.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    source_roots = [Path(path).resolve() for path in args.source_root]

    run_config = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "source_roots": [str(path) for path in source_roots],
        "output_dir": str(output_dir),
        "project": args.project,
        "location": args.location,
        "credentials_path": args.credentials_path,
        "model_name": args.model_name,
        "limit": args.limit,
        "overwrite": args.overwrite,
        "sleep_sec": args.sleep_sec,
    }
    (output_dir / "run_config.json").write_text(
        json.dumps(run_config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"[INFO] source roots: {len(source_roots)}")
    for source_root in source_roots:
        print(f"[INFO] scanning {source_root}")

    analyzer = GeminiAnalyzer(
        model_name=args.model_name,
        project=args.project,
        location=args.location,
        credentials_path=args.credentials_path,
    )

    processed = 0
    for task in iter_tasks(source_roots, limit=args.limit):
        idx = processed + 1
        result = analyze_task(
            analyzer,
            output_dir,
            task,
            overwrite=args.overwrite,
        )
        processed += 1
        status = str(result.get("status") or "unknown")
        verdict = str(result.get("verdict") or "")
        error = str(result.get("error") or "")
        tail = f" verdict={verdict}" if verdict else ""
        if error:
            tail += f" error={error[:160]}"
        print(f"[{idx}] {task.source_path} status={status}{tail}")
        write_summary(output_dir)
        if args.sleep_sec > 0:
            time.sleep(args.sleep_sec)

    print(f"[DONE] processed={processed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
