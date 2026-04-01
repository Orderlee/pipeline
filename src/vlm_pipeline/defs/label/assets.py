"""LABEL @asset — Gemini 기반 자동 이벤트 라벨링.

비디오당 1회 Gemini 호출 → 이벤트 JSON 생성 → vlm-labels 업로드 →
video_metadata.auto_label_* 상태 갱신.
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from hashlib import sha1
from pathlib import Path, PurePosixPath
from tempfile import NamedTemporaryFile, gettempdir
from uuid import uuid4

from dagster import Field, asset

from vlm_pipeline.lib.env_utils import (
    dispatch_folder_for_source_unit,
    dispatch_raw_key_prefix_folder,
    is_dispatch_yolo_only_requested,
    requested_outputs_require_timestamp,
    should_run_output,
)
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
    is_unscoped_mvp_autolabel_run,
    parse_requested_outputs,
    resolve_and_persist_spec_config,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    name="clip_timestamp",
    deps=["raw_ingest"],
    description="Gemini 이벤트 구간(timestamp) → vlm-labels; MVP는 auto_label_*, spec/dispatch는 timestamp_*·ready_for_labeling",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=50)},
)
def clip_timestamp(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    tags = context.run.tags if context.run else {}
    if is_unscoped_mvp_autolabel_run(tags):
        return clip_timestamp_mvp(context, db, minio)
    return clip_timestamp_routed_impl(context, db, minio)


@asset(
    name="classification_video",
    deps=["raw_ingest"],
    description="Dispatch 전용 Gemini 비디오 단일분류 → vlm-labels + labels",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=50)},
)
def classification_video(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    tags = context.run.tags if context.run else {}
    if is_standard_spec_run(tags):
        context.log.info("classification_video 스킵: staging spec 흐름은 이번 범위에서 제외합니다.")
        return {"processed": 0, "failed": 0, "skipped": True}

    requested = parse_requested_outputs(tags)
    if "classification_video" not in requested:
        context.log.info("classification_video 스킵: outputs에 classification_video 없음")
        return {"processed": 0, "failed": 0, "skipped": True}

    folder_name = dispatch_folder_for_source_unit(tags)
    if not folder_name:
        context.log.info("classification_video 스킵: dispatch folder 없음")
        return {"processed": 0, "failed": 0, "skipped": True}

    candidate_classes = _resolve_dispatch_video_class_candidates(tags)
    if not candidate_classes:
        raise RuntimeError("classification_video_requires_categories_or_classes")

    db.ensure_runtime_schema()
    limit = int(context.op_config.get("limit", 50))
    candidates = _find_dispatch_video_classification_candidates(
        db,
        folder_name=folder_name,
        limit=limit,
    )
    if not candidates:
        context.log.info("classification_video: 대상 없음")
        return {"processed": 0, "failed": 0, "predicted": 0}

    analyzer = _init_gemini_analyzer(context)
    processed = 0
    failed = 0

    for idx, cand in enumerate(candidates, start=1):
        asset_id = str(cand.get("asset_id") or "")
        raw_key = str(cand.get("raw_key") or "")
        temp_paths: list[Path] = []
        try:
            video_path, temp_path = _materialize_video(minio, cand)
            if temp_path:
                temp_paths.append(temp_path)
            response_text = analyzer.analyze_video(
                str(video_path),
                prompt=_build_video_classification_prompt(candidate_classes),
            )
            cleaned = extract_clean_json_text(response_text)
            payload = _parse_video_classification_response(cleaned, candidate_classes)
            label_key = _build_video_classification_key(raw_key)
            generated_at = datetime.now()
            artifact_payload = {
                "asset_id": asset_id,
                "raw_key": raw_key,
                "predicted_class": payload["predicted_class"],
                "candidate_classes": candidate_classes,
                "rationale": payload.get("rationale"),
                "model": getattr(analyzer, "model_name", None),
                "generated_at": generated_at.isoformat(),
            }
            minio.ensure_bucket("vlm-labels")
            minio.upload(
                "vlm-labels",
                label_key,
                json.dumps(artifact_payload, ensure_ascii=False).encode("utf-8"),
                "application/json",
            )
            db.insert_label(
                {
                    "label_id": _stable_video_classification_label_id(asset_id, label_key, payload["predicted_class"]),
                    "asset_id": asset_id,
                    "labels_bucket": "vlm-labels",
                    "labels_key": label_key,
                    "label_format": "video_classification_json",
                    "label_tool": "gemini",
                    "label_source": "auto",
                    "review_status": "auto_generated",
                    "event_index": 0,
                    "event_count": 1,
                    "timestamp_start_sec": None,
                    "timestamp_end_sec": None,
                    "caption_text": payload["predicted_class"],
                    "object_count": 1,
                    "label_status": "completed",
                    "created_at": generated_at,
                }
            )
            processed += 1
            context.log.info(
                "classification_video 진행: [%d/%d] asset=%s predicted_class=%s ✅",
                idx,
                len(candidates),
                asset_id,
                payload["predicted_class"],
            )
        except Exception as exc:
            failed += 1
            context.log.error(
                "classification_video 진행: [%d/%d] asset=%s ❌ %s",
                idx,
                len(candidates),
                asset_id,
                exc,
            )
        finally:
            for path in reversed(temp_paths):
                _cleanup_temp(path)

    summary = {"processed": processed, "failed": failed, "predicted": processed}
    context.add_output_metadata(summary)
    return summary


# ── helpers ──

def _stable_video_classification_label_id(asset_id: str, label_key: str, predicted_class: str) -> str:
    token = "|".join([str(asset_id), str(label_key), str(predicted_class or "").strip().lower()])
    return sha1(token.encode("utf-8")).hexdigest()


def _build_video_classification_key(raw_key: str) -> str:
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "video"
    parent = key_path.parent
    if str(parent) and str(parent) != ".":
        return str(parent / "video_classifications" / f"{stem}.json")
    return str(PurePosixPath("video_classifications") / f"{stem}.json")


def _build_video_classification_prompt(candidate_classes: list[str]) -> str:
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


def _parse_video_classification_response(payload_text: str, candidate_classes: list[str]) -> dict[str, str | None]:
    payload = json.loads(str(payload_text or "").strip())
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


def _resolve_dispatch_video_class_candidates(tags) -> list[str]:
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


def _find_dispatch_video_classification_candidates(
    db: DuckDBResource,
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
    video_prompt: str,
) -> list[dict]:
    threshold_sec = _int_env("STAGING_GEMINI_CHUNK_THRESHOLD_SEC", 3600, minimum=1)
    duration_value = _coerce_float(duration_sec)
    if duration_value < threshold_sec:
        return _analyze_single_video_events(
            analyzer,
            video_path,
            duration_sec=duration_sec,
            temp_paths=temp_paths,
            video_prompt=video_prompt,
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
            video_prompt=video_prompt,
        )

    context.log.info(
        "clip_timestamp: long video chunking 적용 path=%s duration=%.3fs chunks=%d",
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
            video_prompt=video_prompt,
        )
        merged_events.extend(
            offset_gemini_events(
                chunk_events,
                offset_sec=chunk.start_sec,
                chunk_end_sec=chunk.end_sec,
            )
        )
        context.log.info(
            "clip_timestamp: chunk %d/%d start=%.3fs end=%.3fs events=%d",
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
    video_prompt: str,
) -> list[dict]:
    gemini_video_path, gemini_temp_path = _prepare_gemini_video_for_request(
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


def clip_timestamp_mvp(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """completed video 중 auto_label_status='pending'인 건에 Gemini 호출 (MVP job)."""
    if not should_run_output(context, "timestamp"):
        context.log.info("clip_timestamp 스킵: outputs에 timestamp가 없습니다.")
        return {"processed": 0, "skipped": True}

    db.ensure_runtime_schema()
    folder_name = dispatch_raw_key_prefix_folder(context.run.tags if context.run else None)
    limit = int(context.op_config.get("limit", 50))
    candidates = db.find_auto_label_pending_videos(limit=limit, folder_name=folder_name)
    if not candidates:
        context.log.info("AUTO LABEL 대상 없음")
        return {"processed": 0, "failed": 0}

    analyzer = _init_gemini_analyzer(context)
    video_prompt = VIDEO_EVENT_PROMPT
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
                prompt=video_prompt,
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


def clip_timestamp_routed_impl(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """spec/dispatch: requested_outputs·spec_id 기준 timestamp 단계."""
    tags = context.run.tags if context.run else {}
    video_prompt = VIDEO_EVENT_PROMPT
    spec_id = tags.get("spec_id")
    requested = parse_requested_outputs(tags)
    standard_spec_run = is_standard_spec_run(tags)
    if is_dispatch_yolo_only_requested(tags):
        context.log.info("clip_timestamp 스킵: dispatch labeling_method가 YOLO 전용입니다.")
        return {"processed": 0, "failed": 0, "skipped": True}
    if not requested_outputs_require_timestamp(requested) and not standard_spec_run:
        context.log.info("clip_timestamp 스킵: outputs에 timestamp 없음")
        return {"processed": 0, "failed": 0, "skipped": True}

    db.ensure_runtime_schema()
    resolved_config_id = None
    if spec_id:
        config_bundle = resolve_and_persist_spec_config(db, spec_id)
        resolved_config_id = config_bundle["resolved_config_id"]
        context.log.info(
            "clip_timestamp: spec_id=%s resolved_config_id=%s scope=%s",
            spec_id,
            resolved_config_id,
            config_bundle["resolved_config_scope"],
        )

    limit = int(context.op_config.get("limit", 50))
    folder_name = dispatch_folder_for_source_unit(tags)
    if spec_id:
        candidates = db.find_ready_for_labeling_timestamp_backlog(spec_id, limit=limit)
    elif folder_name:
        candidates = db.find_timestamp_pending_by_folder(folder_name, limit=limit)
    else:
        candidates = []
    if not candidates:
        context.log.info("clip_timestamp: 대상 없음")
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
                video_prompt=video_prompt,
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
            context.log.error(f"clip_timestamp 실패: asset_id={asset_id}: {exc}")
        finally:
            for path in reversed(temp_paths):
                _cleanup_temp(path)
    return {
        "processed": processed,
        "failed": failed,
        "resolved_config_id": resolved_config_id,
    }
