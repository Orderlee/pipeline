"""LABEL @asset — Gemini 기반 자동 이벤트 라벨링.

비디오당 1회 Gemini 호출 → 이벤트 JSON 생성 → vlm-labels 업로드 →
video_metadata.auto_label_* 상태 갱신.
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path, PurePosixPath
from tempfile import NamedTemporaryFile, gettempdir
from uuid import uuid4

from dagster import Field, asset

from vlm_pipeline.lib.env_utils import should_run_output
from vlm_pipeline.lib.gemini import GeminiAnalyzer, extract_clean_json_text
from vlm_pipeline.lib.gemini_prompts import VIDEO_EVENT_PROMPT
from vlm_pipeline.lib.staging_vertex import (
    merge_overlapping_events,
    normalize_gemini_events,
    offset_gemini_events,
    plan_overlapping_video_chunks,
)
from vlm_pipeline.lib.spec_config import (
    is_standard_spec_run,
    parse_requested_outputs,
    resolve_and_persist_spec_config,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    name="clip_timestamp",
    deps=["raw_ingest"],
    description="Gemini 1회 호출 → 이벤트 JSON 생성 → vlm-labels 업로드, video_metadata.auto_label_* 갱신",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=50)},
)
def clip_timestamp(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """completed video 중 auto_label_status='pending'인 건에 Gemini 호출."""
    if not should_run_output(context, "timestamp"):
        context.log.info("clip_timestamp 스킵: outputs에 timestamp가 없습니다.")
        return {"processed": 0, "skipped": True}

    folder_name = context.run.tags.get("folder_name")
    limit = int(context.op_config.get("limit", 50))
    candidates = db.find_auto_label_pending_videos(limit=limit, folder_name=folder_name)
    if not candidates:
        context.log.info("AUTO LABEL 대상 없음")
        return {"processed": 0, "failed": 0}

    analyzer = _init_gemini_analyzer(context)
    total_candidates = len(candidates)
    context.log.info(f"clip_timestamp 시작: 총 {total_candidates}건 처리 예정")
    processed = 0
    failed = 0

    for idx, cand in enumerate(candidates, start=1):
        asset_id = cand["asset_id"]
        raw_key = str(cand.get("raw_key") or "")
        temp_paths: list[Path] = []

        try:
            video_path, temp_path = _materialize_video(minio, cand)
            if temp_path is not None:
                temp_paths.append(temp_path)
            duration_val = cand.get("duration_sec")
            gemini_video_path, gemini_temp_path = _prepare_gemini_video_for_request(
                video_path,
                duration_sec=duration_val,
            )
            if gemini_temp_path is not None:
                temp_paths.append(gemini_temp_path)
            label_key = _build_gemini_label_key(raw_key)

            response_text = analyzer.analyze_video(
                str(gemini_video_path),
                prompt=VIDEO_EVENT_PROMPT,
                mime_type="video/mp4" if gemini_temp_path is not None else None,
            )

            cleaned = extract_clean_json_text(response_text)
            label_bytes = cleaned.encode("utf-8")
            minio.ensure_bucket("vlm-labels")
            minio.upload("vlm-labels", label_key, label_bytes, "application/json")

            db.update_auto_label_status(
                asset_id,
                "generated",
                label_key=label_key,
                labeled_at=datetime.now(),
            )
            processed += 1
            if gemini_temp_path is not None:
                max_dur = _int_env("GEMINI_MAX_DURATION_SEC", 3600, minimum=60)
                orig_dur = float(duration_val or 0)
                trimmed = orig_dur > max_dur
                context.log.info(
                    "AUTO LABEL preview 사용: asset_id=%s original_bytes=%s preview_bytes=%s "
                    "original_duration=%.0fs trimmed=%s max_duration=%ds",
                    asset_id,
                    video_path.stat().st_size,
                    gemini_video_path.stat().st_size,
                    orig_dur,
                    trimmed,
                    max_dur,
                )
            context.log.info(
                f"clip_timestamp 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} label_key={label_key} ✅"
            )

        except Exception as exc:
            failed += 1
            db.update_auto_label_status(
                asset_id,
                "failed",
                error=str(exc)[:500],
            )
            context.log.error(
                f"clip_timestamp 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} ❌ {exc}"
            )
        finally:
            for path in reversed(temp_paths):
                _cleanup_temp(path)

    summary = {"processed": processed, "failed": failed}
    context.add_output_metadata(summary)
    context.log.info(f"AUTO LABEL 완료: {summary}")
    return summary


@asset(
    name="clip_timestamp_routed",
    deps=["raw_ingest"],
    description="Staging spec flow: ready_for_labeling + requested_outputs에 timestamp 포함 시 Gemini 이벤트 구간 추출",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=50)},
)
def clip_timestamp_routed(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """run tag: spec_id/requested_outputs (spec flow) 또는 outputs (dispatch flow). timestamp 포함 시 실행."""
    tags = context.run.tags if context.run else {}
    spec_id = tags.get("spec_id")
    requested = parse_requested_outputs(tags)
    standard_spec_run = is_standard_spec_run(tags)
    if "timestamp" not in requested and not standard_spec_run:
        context.log.info("clip_timestamp_routed 스킵: outputs에 timestamp 없음")
        return {"processed": 0, "failed": 0, "skipped": True}

    resolved_config_id = None
    if spec_id:
        config_bundle = resolve_and_persist_spec_config(db, spec_id)
        resolved_config_id = config_bundle["resolved_config_id"]
        context.log.info(
            "clip_timestamp_routed: spec_id=%s resolved_config_id=%s scope=%s",
            spec_id,
            resolved_config_id,
            config_bundle["resolved_config_scope"],
        )

    limit = int(context.op_config.get("limit", 50))
    folder_name = tags.get("folder_name") or tags.get("folder_name_original")
    if spec_id:
        candidates = db.find_ready_for_labeling_timestamp_backlog(spec_id, limit=limit)
    elif folder_name:
        candidates = db.find_timestamp_pending_by_folder(folder_name, limit=limit)
    else:
        candidates = []
    if not candidates:
        context.log.info("clip_timestamp_routed: 대상 없음")
        return {"processed": 0, "failed": 0}

    analyzer = _init_gemini_analyzer(context)
    processed = 0
    failed = 0
    for idx, cand in enumerate(candidates, start=1):
        asset_id = cand["asset_id"]
        raw_key = str(cand.get("raw_key") or "")
        temp_paths: list[Path] = []
        try:
            video_path, temp_path = _materialize_video(minio, cand)
            if temp_path:
                temp_paths.append(temp_path)
            duration_val = cand.get("duration_sec")
            label_key = _build_gemini_label_key(raw_key)
            events = _analyze_routed_video_events(
                context,
                analyzer,
                video_path,
                duration_sec=duration_val,
                temp_paths=temp_paths,
            )
            label_bytes = _serialize_gemini_events(events)
            minio.ensure_bucket("vlm-labels")
            minio.upload("vlm-labels", label_key, label_bytes, "application/json")
            db.update_timestamp_status(
                asset_id,
                "completed",
                label_key=label_key,
                completed_at=datetime.now(),
            )
            processed += 1
        except Exception as exc:
            failed += 1
            db.update_timestamp_status(
                asset_id, "failed", error=str(exc)[:500], completed_at=datetime.now()
            )
            context.log.error(f"clip_timestamp_routed 실패: asset_id={asset_id}: {exc}")
        finally:
            for path in reversed(temp_paths):
                _cleanup_temp(path)
    return {
        "processed": processed,
        "failed": failed,
        "resolved_config_id": resolved_config_id,
    }


# ── helpers ──

def _init_gemini_analyzer(context) -> GeminiAnalyzer:
    """GeminiAnalyzer 초기화 (환경변수 기반)."""
    return GeminiAnalyzer()


def _materialize_video(
    minio: MinIOResource,
    candidate: dict,
) -> tuple[Path, Path | None]:
    """비디오 파일을 로컬에 확보. archive_path 우선, MinIO fallback."""
    archive_path = Path(str(candidate.get("archive_path") or "").strip())
    if archive_path.exists():
        return archive_path, None

    raw_bucket = str(candidate.get("raw_bucket") or "vlm-raw")
    raw_key = str(candidate.get("raw_key") or "")
    suffix = Path(raw_key).suffix or ".mp4"
    tmp = NamedTemporaryFile(delete=False, suffix=suffix)
    try:
        minio.download_fileobj(raw_bucket, raw_key, tmp)
    finally:
        tmp.close()
    return Path(tmp.name), Path(tmp.name)


def _build_gemini_label_key(raw_key: str) -> str:
    """`<raw_parent>/events/<video_stem>.json` 규칙으로 label key 생성."""
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "video"
    parent = key_path.parent
    if str(parent) and str(parent) != ".":
        return str(parent / "events" / f"{stem}.json")
    return str(PurePosixPath("events") / f"{stem}.json")


def _prepare_gemini_video_for_request(
    video_path: Path,
    *,
    duration_sec: float | int | None,
) -> tuple[Path, Path | None]:
    source_path = Path(video_path)
    source_size = source_path.stat().st_size
    safe_bytes = _int_env("GEMINI_SAFE_VIDEO_BYTES", 450 * 1024 * 1024, minimum=1)
    max_duration = _int_env("GEMINI_MAX_DURATION_SEC", 3600, minimum=60)

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

    request_limit = _int_env(
        "GEMINI_MAX_REQUEST_BYTES",
        524_288_000,
        minimum=safe_bytes,
    )
    target_bytes = min(
        _int_env("GEMINI_PREVIEW_TARGET_BYTES", 120 * 1024 * 1024, minimum=1),
        safe_bytes,
    )
    attempts = [
        {
            "target_bytes": target_bytes,
            "width": _int_env("GEMINI_PREVIEW_MAX_WIDTH", 960, minimum=160),
            "fps": _int_env("GEMINI_PREVIEW_FPS", 6, minimum=1),
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
            if preview_path.stat().st_size <= request_limit:
                return preview_path, preview_path
            last_error = (
                f"gemini_preview_too_large:{preview_path.stat().st_size}bytes"
            )
        else:
            stderr = (proc.stderr or b"").decode("utf-8", errors="ignore").strip()
            last_error = f"gemini_preview_ffmpeg_failed:{stderr or 'empty_output'}"
        _cleanup_temp(preview_path)

    raise RuntimeError(
        f"{last_error}; original_size={source_size}bytes exceeds_safe_limit={safe_bytes}bytes"
    )


def _analyze_routed_video_events(
    context,
    analyzer: GeminiAnalyzer,
    video_path: Path,
    *,
    duration_sec: float | int | None,
    temp_paths: list[Path],
) -> list[dict]:
    threshold_sec = _int_env("STAGING_GEMINI_CHUNK_THRESHOLD_SEC", 3600, minimum=1)
    duration_value = _coerce_float(duration_sec)
    if duration_value < threshold_sec:
        return _analyze_single_video_events(
            analyzer,
            video_path,
            duration_sec=duration_sec,
            temp_paths=temp_paths,
        )

    window_sec = _int_env("STAGING_GEMINI_CHUNK_WINDOW_SEC", 660, minimum=60)
    stride_sec = _int_env("STAGING_GEMINI_CHUNK_STRIDE_SEC", 600, minimum=60)
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
        )

    context.log.info(
        "clip_timestamp_routed: long video chunking 적용 path=%s duration=%.3fs chunks=%d",
        video_path,
        duration_value,
        len(chunk_plan),
    )
    merged_events: list[dict] = []
    for chunk in chunk_plan:
        chunk_path = _extract_video_segment_path(
            video_path,
            start_sec=chunk.start_sec,
            end_sec=chunk.end_sec,
        )
        temp_paths.append(chunk_path)
        chunk_events = _analyze_single_video_events(
            analyzer,
            chunk_path,
            duration_sec=chunk.duration_sec,
            temp_paths=temp_paths,
        )
        merged_events.extend(
            offset_gemini_events(
                chunk_events,
                offset_sec=chunk.start_sec,
                chunk_end_sec=chunk.end_sec,
            )
        )
        context.log.info(
            "clip_timestamp_routed: chunk %d/%d start=%.3fs end=%.3fs events=%d",
            chunk.chunk_index,
            len(chunk_plan),
            chunk.start_sec,
            chunk.end_sec,
            len(chunk_events),
        )

    return merge_overlapping_events(merged_events)


def _analyze_single_video_events(
    analyzer: GeminiAnalyzer,
    video_path: Path,
    *,
    duration_sec: float | int | None,
    temp_paths: list[Path],
) -> list[dict]:
    gemini_video_path, gemini_temp_path = _prepare_gemini_video_for_request(
        video_path,
        duration_sec=duration_sec,
    )
    if gemini_temp_path is not None:
        temp_paths.append(gemini_temp_path)
    response_text = analyzer.analyze_video(
        str(gemini_video_path),
        prompt=VIDEO_EVENT_PROMPT,
        mime_type="video/mp4" if gemini_temp_path is not None else None,
    )
    return _parse_gemini_events_response(response_text)


def _parse_gemini_events_response(response_text: str) -> list[dict]:
    cleaned = extract_clean_json_text(response_text)
    payload = json.loads(cleaned)
    return normalize_gemini_events(payload)


def _serialize_gemini_events(events: list[dict]) -> bytes:
    normalized = normalize_gemini_events(events)
    rendered = json.dumps(normalized, ensure_ascii=False, indent=2) + "\n"
    return rendered.encode("utf-8")


def _extract_video_segment_path(
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

    _cleanup_temp(output_path)
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
        _cleanup_temp(output_path)
        raise RuntimeError(f"ffmpeg_chunk_extract_failed:{stderr or 'empty_output'}")
    return output_path


def _build_nonexistent_temp_path(suffix: str) -> Path:
    return Path(gettempdir()) / f"vlm_gemini_{uuid4().hex}{suffix}"


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


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw_value = (os.getenv(name) or "").strip()
    try:
        parsed = int(raw_value) if raw_value else int(default)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), parsed)


def _coerce_float(value: float | int | str | None) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _cleanup_temp(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass
