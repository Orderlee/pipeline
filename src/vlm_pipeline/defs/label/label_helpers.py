"""LABEL helpers — Gemini 라벨링 공용 유틸리티.

assets.py / timestamp.py 등에서 공유하는 private helper 함수 모음.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import subprocess
from hashlib import sha1
from pathlib import Path
from tempfile import NamedTemporaryFile, gettempdir
from uuid import uuid4

from vlm_pipeline.lib.env_utils import coerce_float, int_env
from vlm_pipeline.lib.file_loader import build_nonexistent_temp_path, cleanup_temp_path
from vlm_pipeline.lib.gemini import GeminiAnalyzer, extract_clean_json_text, load_clean_json
from vlm_pipeline.lib.vertex_chunking import (
    merge_overlapping_events,
    normalize_gemini_events,
    offset_gemini_events,
    plan_overlapping_video_chunks,
)
from vlm_pipeline.resources.minio import MinIOResource


# ── Gemini analyzer ──

def init_gemini_analyzer(context) -> GeminiAnalyzer:
    """GeminiAnalyzer 초기화 (환경변수 기반)."""
    return GeminiAnalyzer()


def clone_gemini_analyzer(analyzer: GeminiAnalyzer) -> GeminiAnalyzer:
    """병렬 worker에서 사용할 GeminiAnalyzer 복제."""
    return GeminiAnalyzer(
        model_name=getattr(analyzer, "model_name", "gemini-2.5-flash"),
        project=getattr(analyzer, "project", None),
        location=getattr(analyzer, "location", None),
        credentials_path=getattr(analyzer, "credentials_path", None),
    )


# ── video materialisation ──

def materialize_video(
    minio: MinIOResource,
    candidate: dict,
) -> tuple[Path, Path | None]:
    """비디오 파일을 로컬에 확보. archive_path 우선, MinIO fallback."""
    from vlm_pipeline.lib.media_utils import materialize_video_path
    return materialize_video_path(minio, candidate)


# ── label key builders (delegate to lib.key_builders) ──

def build_gemini_label_key(raw_key: str) -> str:
    """`<raw_parent>/events/<video_stem>.json` 규칙으로 label key 생성."""
    from vlm_pipeline.lib.key_builders import build_gemini_label_key as _impl
    return _impl(raw_key)


def build_video_classification_key(raw_key: str) -> str:
    from vlm_pipeline.lib.key_builders import build_video_classification_key as _impl
    return _impl(raw_key)


# ── Gemini video analysis (single + chunked) ──

def prepare_gemini_video_for_request(
    video_path: Path,
    *,
    duration_sec: float | int | None,
) -> tuple[Path, Path | None]:
    source_path = Path(video_path)
    source_size = source_path.stat().st_size
    safe_bytes = int_env("GEMINI_SAFE_VIDEO_BYTES", 450 * 1024 * 1024, minimum=1)
    max_duration = int_env("GEMINI_MAX_DURATION_SEC", 3600, minimum=60)

    actual_duration = 0.0
    try:
        actual_duration = float(duration_sec or 0.0)
    except (TypeError, ValueError):
        actual_duration = 0.0

    needs_resize = source_size > safe_bytes
    needs_trim = actual_duration > max_duration
    if not needs_resize and not needs_trim:
        return source_path, None

    effective_duration = min(actual_duration, max_duration) if needs_trim else actual_duration

    request_limit = int_env(
        "GEMINI_MAX_REQUEST_BYTES",
        524_288_000,
        minimum=safe_bytes,
    )
    target_bytes = min(
        int_env("GEMINI_PREVIEW_TARGET_BYTES", 120 * 1024 * 1024, minimum=1),
        safe_bytes,
    )
    attempts = [
        {
            "target_bytes": target_bytes,
            "width": int_env("GEMINI_PREVIEW_MAX_WIDTH", 960, minimum=160),
            "fps": int_env("GEMINI_PREVIEW_FPS", 6, minimum=1),
        },
        {
            "target_bytes": min(target_bytes, 80 * 1024 * 1024),
            "width": 640,
            "fps": 4,
        },
        {
            "target_bytes": min(target_bytes, 48 * 1024 * 1024),
            "width": 480,
            "fps": 3,
        },
    ]

    last_error = "gemini_preview_unknown_failure"
    primary_attempt = attempts[0]
    primary_result = _render_gemini_preview_attempt(
        source_path=source_path,
        duration_sec=effective_duration,
        max_duration=max_duration,
        needs_trim=needs_trim,
        request_limit=request_limit,
        attempt=primary_attempt,
    )
    if primary_result["preview_path"] is not None:
        preview_path = primary_result["preview_path"]
        return preview_path, preview_path
    last_error = str(primary_result["error"] or last_error)

    fallback_attempts = attempts[1:]
    if not fallback_attempts:
        raise RuntimeError(
            f"{last_error}; original_size={source_size}bytes exceeds_safe_limit={safe_bytes}bytes"
        )

    fallback_results: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=min(len(fallback_attempts), 2)) as executor:
        futures = [
            executor.submit(
                _render_gemini_preview_attempt,
                source_path=source_path,
                duration_sec=effective_duration,
                max_duration=max_duration,
                needs_trim=needs_trim,
                request_limit=request_limit,
                attempt=attempt,
            )
            for attempt in fallback_attempts
        ]
        for future in as_completed(futures):
            fallback_results.append(future.result())

    successful = [row for row in fallback_results if row["preview_path"] is not None]
    if successful:
        successful.sort(key=lambda row: int(row["width"]), reverse=True)
        chosen = successful[0]
        for rejected in successful[1:]:
            cleanup_temp_path(rejected["preview_path"])
        preview_path = chosen["preview_path"]
        if isinstance(preview_path, Path):
            return preview_path, preview_path

    fallback_errors = [str(row["error"]) for row in fallback_results if row.get("error")]
    if fallback_errors:
        last_error = "; ".join(fallback_errors)

    raise RuntimeError(
        f"{last_error}; original_size={source_size}bytes exceeds_safe_limit={safe_bytes}bytes"
    )


def analyze_routed_video_events(
    context,
    analyzer: GeminiAnalyzer,
    video_path: Path,
    *,
    duration_sec: float | int | None,
    temp_paths: list[Path],
    video_prompt: str,
) -> list[dict]:
    threshold_sec = int_env("STAGING_GEMINI_CHUNK_THRESHOLD_SEC", 3600, minimum=1)
    duration_value = coerce_float(duration_sec)
    if duration_value < threshold_sec:
        return _analyze_single_video_events(
            analyzer,
            video_path,
            duration_sec=duration_sec,
            temp_paths=temp_paths,
            video_prompt=video_prompt,
        )

    window_sec = int_env("STAGING_GEMINI_CHUNK_WINDOW_SEC", 660, minimum=60)
    stride_sec = int_env("STAGING_GEMINI_CHUNK_STRIDE_SEC", 600, minimum=60)
    chunk_plan = plan_overlapping_video_chunks(
        duration_value,
        window_sec=float(window_sec),
        stride_sec=float(stride_sec),
    )
    if len(chunk_plan) <= 1:
        return _analyze_single_video_events(
            analyzer,
            video_path,
            duration_sec=duration_sec,
            temp_paths=temp_paths,
            video_prompt=video_prompt,
        )

    context.log.info(
        "clip_timestamp: long video chunking 적용 path=%s duration=%.3fs chunks=%d",
        video_path,
        duration_value,
        len(chunk_plan),
    )
    merged_events: list[dict] = []
    chunk_workers = min(
        len(chunk_plan),
        int_env("GEMINI_CHUNK_MAX_WORKERS", 3, minimum=1),
    )

    def _analyze_chunk(chunk) -> dict[str, object]:
        local_temp_paths: list[Path] = []
        try:
            local_analyzer = clone_gemini_analyzer(analyzer)
            chunk_path = extract_video_segment_path(
                video_path,
                start_sec=chunk.start_sec,
                end_sec=chunk.end_sec,
            )
            local_temp_paths.append(chunk_path)
            chunk_events = _analyze_single_video_events(
                local_analyzer,
                chunk_path,
                duration_sec=chunk.duration_sec,
                temp_paths=local_temp_paths,
                video_prompt=video_prompt,
            )
            return {
                "chunk_index": chunk.chunk_index,
                "start_sec": chunk.start_sec,
                "end_sec": chunk.end_sec,
                "events": offset_gemini_events(
                    chunk_events,
                    offset_sec=chunk.start_sec,
                    chunk_end_sec=chunk.end_sec,
                ),
                "event_count": len(chunk_events),
            }
        finally:
            for path in reversed(local_temp_paths):
                cleanup_temp_path(path)

    chunk_results: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=chunk_workers) as executor:
        futures = [executor.submit(_analyze_chunk, chunk) for chunk in chunk_plan]
        for future in as_completed(futures):
            chunk_results.append(future.result())

    chunk_results.sort(key=lambda row: int(row["chunk_index"]))
    for chunk_result in chunk_results:
        merged_events.extend(list(chunk_result["events"]))
        context.log.info(
            "clip_timestamp: chunk %d/%d start=%.3fs end=%.3fs events=%d",
            int(chunk_result["chunk_index"]),
            len(chunk_plan),
            float(chunk_result["start_sec"]),
            float(chunk_result["end_sec"]),
            int(chunk_result["event_count"]),
        )

    return merge_overlapping_events(merged_events)


def _analyze_single_video_events(
    analyzer: GeminiAnalyzer,
    video_path: Path,
    *,
    duration_sec: float | int | None,
    temp_paths: list[Path],
    video_prompt: str,
) -> list[dict]:
    gemini_video_path, gemini_temp_path = prepare_gemini_video_for_request(
        video_path,
        duration_sec=duration_sec,
    )
    if gemini_temp_path is not None:
        temp_paths.append(gemini_temp_path)
    response_text = analyzer.analyze_video(
        str(gemini_video_path),
        prompt=video_prompt,
        mime_type="video/mp4" if gemini_temp_path is not None else None,
    )
    return parse_gemini_events_response(response_text)


# ── Gemini response parsing / serialisation ──

def parse_gemini_events_response(response_text: str) -> list[dict]:
    payload = load_clean_json(response_text)
    return normalize_gemini_events(payload)


def serialize_gemini_events(events: list[dict]) -> bytes:
    normalized = normalize_gemini_events(events)
    rendered = json.dumps(normalized, ensure_ascii=False, indent=2) + "\n"
    return rendered.encode("utf-8")


# ── video segment extraction ──

def extract_video_segment_path(
    video_path: Path,
    *,
    start_sec: float,
    end_sec: float,
) -> Path:
    duration_value = max(0.05, float(end_sec) - float(start_sec))
    output_path = _build_nonexistent_temp_path(".mp4")

    copy_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{float(start_sec):.3f}",
        "-t",
        f"{duration_value:.3f}",
        "-i",
        str(video_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    copy_proc = subprocess.run(copy_cmd, capture_output=True, check=False)
    if copy_proc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
        return output_path

    cleanup_temp_path(output_path)
    output_path = _build_nonexistent_temp_path(".mp4")
    reencode_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{float(start_sec):.3f}",
        "-t",
        f"{duration_value:.3f}",
        "-i",
        str(video_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    reencode_proc = subprocess.run(reencode_cmd, capture_output=True, check=False)
    if reencode_proc.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        stderr = (reencode_proc.stderr or copy_proc.stderr or b"").decode("utf-8", errors="ignore").strip()
        cleanup_temp_path(output_path)
        raise RuntimeError(f"ffmpeg_chunk_extract_failed:{stderr or 'empty_output'}")
    return output_path


# ── classification helpers ──

def stable_video_classification_label_id(asset_id: str, label_key: str, predicted_class: str) -> str:
    token = "|".join([str(asset_id), str(label_key), str(predicted_class or "").strip().lower()])
    return sha1(token.encode("utf-8")).hexdigest()


def build_video_classification_prompt(candidate_classes: list[str]) -> str:
    rendered_candidates = ", ".join(str(value).strip().lower() for value in candidate_classes if str(value).strip())
    return (
        "You are classifying a CCTV video into exactly one class.\n\n"
        f"Allowed classes: [{rendered_candidates}]\n\n"
        "Task:\n"
        "Choose the single best matching class based only on visible evidence in the video.\n\n"
        "Return JSON only in this shape:\n"
        '{"predicted_class":"...", "rationale":"..."}\n\n'
        "Rules:\n"
        "- predicted_class must be exactly one value from Allowed classes.\n"
        "- rationale must be a short English explanation.\n"
        "- Do not include markdown fences or extra explanation."
    )


def parse_video_classification_response(payload_text: str, candidate_classes: list[str]) -> dict[str, str | None]:
    payload = load_clean_json(payload_text)
    if not isinstance(payload, dict):
        raise ValueError("video_classification_response_not_object")

    predicted_class = str(payload.get("predicted_class") or "").strip().lower()
    allowed = {str(value).strip().lower() for value in candidate_classes if str(value).strip()}
    if not predicted_class or predicted_class not in allowed:
        raise ValueError("video_classification_invalid_predicted_class")

    rationale = str(payload.get("rationale") or "").strip() or None
    return {
        "predicted_class": predicted_class,
        "rationale": rationale,
    }


def resolve_dispatch_video_class_candidates(tags) -> list[str]:
    raw_categories = str(tags.get("categories") or "").strip()
    if raw_categories:
        try:
            parsed = json.loads(raw_categories) if raw_categories.startswith("[") else raw_categories.split(",")
        except Exception:
            parsed = raw_categories.split(",")
        normalized = []
        seen = set()
        for value in parsed:
            rendered = str(value or "").strip().lower()
            if not rendered or rendered in seen:
                continue
            seen.add(rendered)
            normalized.append(rendered)
        if normalized:
            return normalized

    raw_classes = str(tags.get("classes") or "").strip()
    if raw_classes:
        try:
            parsed = json.loads(raw_classes) if raw_classes.startswith("[") else raw_classes.split(",")
        except Exception:
            parsed = raw_classes.split(",")
        normalized = []
        seen = set()
        for value in parsed:
            rendered = str(value or "").strip().lower()
            if not rendered or rendered in seen:
                continue
            seen.add(rendered)
            normalized.append(rendered)
        return normalized
    return []


def find_dispatch_video_classification_candidates(
    db,
    *,
    folder_name: str,
    limit: int,
) -> list[dict[str, object]]:
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT
                r.asset_id,
                r.raw_bucket,
                r.raw_key,
                r.archive_path,
                r.source_path,
                vm.duration_sec,
                vm.fps,
                vm.frame_count
            FROM raw_files r
            JOIN video_metadata vm ON vm.asset_id = r.asset_id
            WHERE r.media_type = 'video'
              AND r.ingest_status = 'completed'
              AND r.source_unit_name = ?
              AND NOT EXISTS (
                    SELECT 1
                    FROM labels l
                    WHERE l.asset_id = r.asset_id
                      AND l.label_format = 'video_classification_json'
                )
            ORDER BY r.created_at
            LIMIT ?
            """,
            [folder_name, max(1, int(limit))],
        ).fetchall()
    columns = [
        "asset_id",
        "raw_bucket",
        "raw_key",
        "archive_path",
        "source_path",
        "duration_sec",
        "fps",
        "frame_count",
    ]
    return [dict(zip(columns, row)) for row in rows]


# ── generic utilities ──

def _build_nonexistent_temp_path(suffix: str) -> Path:
    return build_nonexistent_temp_path(suffix, prefix="vlm_gemini_")


def _target_preview_bitrate_kbps(
    *,
    duration_sec: float | int | None,
    target_bytes: int,
) -> int:
    try:
        duration_value = float(duration_sec or 0.0)
    except (TypeError, ValueError):
        duration_value = 0.0

    if duration_value <= 0:
        return 900

    bitrate_kbps = int((max(1, int(target_bytes)) * 8) / duration_value / 1000)
    return max(120, min(2500, bitrate_kbps))


def _render_gemini_preview_attempt(
    *,
    source_path: Path,
    duration_sec: float | int | None,
    max_duration: int,
    needs_trim: bool,
    request_limit: int,
    attempt: dict[str, int],
) -> dict[str, object]:
    preview_path = _build_nonexistent_temp_path(".mp4")
    bitrate_kbps = _target_preview_bitrate_kbps(
        duration_sec=duration_sec,
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
    cmd.extend([
        "-map",
        "0:v:0",
        "-an",
        "-vf",
        (
            f"fps={int(attempt['fps'])},"
            f"scale=w={int(attempt['width'])}:h=-2:force_original_aspect_ratio=decrease"
        ),
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
    ])
    proc = subprocess.run(cmd, capture_output=True, check=False)
    if proc.returncode == 0 and preview_path.exists() and preview_path.stat().st_size > 0:
        size_bytes = preview_path.stat().st_size
        if size_bytes <= request_limit:
            return {
                "preview_path": preview_path,
                "width": int(attempt["width"]),
                "error": None,
            }
        cleanup_temp_path(preview_path)
        return {
            "preview_path": None,
            "width": int(attempt["width"]),
            "error": f"gemini_preview_too_large:{size_bytes}bytes",
        }

    stderr = (proc.stderr or b"").decode("utf-8", errors="ignore").strip()
    cleanup_temp_path(preview_path)
    return {
        "preview_path": None,
        "width": int(attempt["width"]),
        "error": f"gemini_preview_ffmpeg_failed:{stderr or 'empty_output'}",
    }




