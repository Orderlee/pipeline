#!/usr/bin/env python3
"""tmp_data_2 수동 YOLO + SAM3 비교 스크립트.

Dagster/DB/MinIO를 거치지 않고 로컬 영상에서 직접 프레임을 추출한 뒤,
YOLO-World와 SAM3.1을 호출해 비교 리포트를 생성한다.

사용 예시:
  PYTHONPATH=src python3 scripts/manual_yolo_sam3_compare.py
  PYTHONPATH=src python3 scripts/manual_yolo_sam3_compare.py --max-videos 1
  PYTHONPATH=src python3 scripts/manual_yolo_sam3_compare.py --resume --skip-yolo
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1
from pathlib import Path
from queue import Queue
import threading
from typing import Any

import numpy as np
from PIL import Image, ImageDraw, ImageFont

# repo root = parent of scripts/
REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

from vlm_pipeline.lib.env_utils import derive_classes_from_categories
from vlm_pipeline.lib.sam3 import SAM3Client
from vlm_pipeline.lib.sam3_compare import (
    compare_prompt_boxes,
    group_sam_detections_by_prompt,
    parse_yolo_coco_payload,
    summarize_benchmark_rows,
)
from vlm_pipeline.lib.video_frames import (
    describe_frame_bytes,
    extract_frame_jpeg_bytes,
    plan_frame_timestamps,
    resolve_frame_sampling_policy,
)
from vlm_pipeline.lib.yolo_coco import build_coco_detection_payload
from vlm_pipeline.lib.yolo_thresholds import (
    filter_detections_by_class_confidence,
    resolve_active_class_confidence_thresholds,
    resolve_effective_request_confidence_threshold,
)
from vlm_pipeline.lib.yolo_world import YOLOWorldClient

LOGGER = logging.getLogger("manual_yolo_sam3_compare")
DEFAULT_INPUT_DIR = Path("incoming/tmp_data_2")
DEFAULT_OUTPUT_ROOT = Path("/tmp/vlm_manual_compare")
DEFAULT_VIDEO_GLOB = "*.mp4,*.MP4,*.mov,*.MOV"
DEFAULT_CATEGORIES = "smoke,fire,smoking,falldown,weapon,violence"
VISUAL_PANEL_MARGIN = 16
VISUAL_HEADER_HEIGHT = 52
VISUAL_FOOTER_HEIGHT = 88
VISUAL_MASK_ALPHA = 88
VISUAL_COLORS: tuple[tuple[int, int, int], ...] = (
    (231, 76, 60),
    (243, 156, 18),
    (46, 204, 113),
    (52, 152, 219),
    (155, 89, 182),
    (241, 196, 15),
    (26, 188, 156),
    (230, 126, 34),
    (149, 165, 166),
)
_JSONL_WRITE_LOCK = threading.Lock()


@dataclass(frozen=True)
class VideoMetadata:
    duration_sec: float
    fps: float
    frame_count: int


@dataclass(frozen=True)
class FrameRecord:
    image_id: str
    video_rel_path: str
    frame_index: int
    frame_sec: float
    frame_path: Path
    frame_rel_path: str
    yolo_path: Path
    yolo_rel_path: str
    sam3_path: Path
    sam3_rel_path: str
    width: int
    height: int


@dataclass
class VideoTask:
    index: int
    total_videos: int
    video_rel_path: Path
    manifest_entry: dict[str, Any]
    frame_records: list[FrameRecord]


def parse_args() -> argparse.Namespace:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_output_dir = DEFAULT_OUTPUT_ROOT / f"tmp_data_2_{timestamp}"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=default_output_dir)
    parser.add_argument("--video-glob", default=DEFAULT_VIDEO_GLOB)
    parser.add_argument("--categories", default=DEFAULT_CATEGORIES)
    parser.add_argument("--classes", default="")
    parser.add_argument("--image-profile", default="current", choices=["current", "dense"])
    parser.add_argument("--confidence-threshold", type=float, default=0.25)
    parser.add_argument("--iou-threshold", type=float, default=0.45)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-videos", type=int, default=0)
    parser.add_argument("--max-masks-per-prompt", type=int, default=50)
    parser.add_argument("--jpeg-quality", type=int, default=90)
    parser.add_argument("--yolo-url", default="http://127.0.0.1:8001")
    parser.add_argument("--sam3-url", default="http://127.0.0.1:8002")
    parser.add_argument("--skip-yolo", action="store_true")
    parser.add_argument("--skip-sam3", action="store_true")
    parser.add_argument("--skip-visuals", action="store_true")
    parser.add_argument("--visual-video-fps", type=float, default=2.0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, str(level or "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _JSONL_WRITE_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_non_negative_int(value: object) -> int:
    rendered = str(value or "").strip()
    return int(rendered) if rendered.isdigit() else 0


def _parse_fps(raw_value: object) -> float:
    rendered = str(raw_value or "").strip()
    if not rendered:
        return 0.0
    if "/" in rendered:
        numerator, denominator = rendered.split("/", 1)
        try:
            denominator_value = float(denominator)
            return float(numerator) / denominator_value if denominator_value else 0.0
        except (TypeError, ValueError):
            return 0.0
    try:
        return float(rendered)
    except (TypeError, ValueError):
        return 0.0


def _probe_frame_count_fallback(video_path: Path) -> int:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-count_frames",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=nb_read_frames",
        "-of",
        "default=nk=1:nw=1",
        str(video_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=False)
    if proc.returncode != 0:
        return 0
    for line in (proc.stdout or "").splitlines():
        frame_count = _parse_non_negative_int(line)
        if frame_count > 0:
            return frame_count
    return 0


def probe_video_metadata(video_path: Path) -> VideoMetadata:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(video_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe_failed:{proc.stderr.strip()}")

    payload = json.loads(proc.stdout or "{}")
    streams = payload.get("streams") or []
    format_info = payload.get("format") or {}
    video_stream = next((stream for stream in streams if stream.get("codec_type") == "video"), {})
    duration_sec = float(format_info.get("duration") or 0.0)
    fps = _parse_fps(video_stream.get("avg_frame_rate") or video_stream.get("r_frame_rate"))
    frame_count = _parse_non_negative_int(video_stream.get("nb_frames"))
    if frame_count <= 0:
        frame_count = _probe_frame_count_fallback(video_path)
    return VideoMetadata(duration_sec=duration_sec, fps=fps, frame_count=frame_count)


def parse_csv_list(raw_value: str) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for chunk in str(raw_value or "").split(","):
        rendered = chunk.strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        values.append(rendered)
    return values


def collect_videos(input_dir: Path, video_glob: str, max_videos: int) -> list[Path]:
    patterns = parse_csv_list(video_glob) or parse_csv_list(DEFAULT_VIDEO_GLOB)
    videos: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for path in input_dir.rglob(pattern):
            if not path.is_file() or path in seen:
                continue
            seen.add(path)
            videos.append(path)
            if max_videos > 0 and len(videos) >= max_videos:
                return sorted(videos)
    return sorted(videos)


def build_image_id(video_rel_path: str, frame_index: int, frame_sec: float) -> str:
    token = f"{video_rel_path}|{frame_index}|{frame_sec:.3f}"
    return sha1(token.encode("utf-8")).hexdigest()


def _resolve_video_output_dir(video_rel_path: Path) -> Path:
    return video_rel_path.with_suffix("")


def resolve_requested_classes(args: argparse.Namespace) -> tuple[list[str], list[str]]:
    explicit_classes = parse_csv_list(args.classes)
    if explicit_classes:
        return explicit_classes, []
    categories = parse_csv_list(args.categories)
    return derive_classes_from_categories(categories), categories


def _get_font(size: int = 16) -> ImageFont.ImageFont:
    try:
        return ImageFont.truetype("DejaVuSans.ttf", size=size)
    except OSError:
        return ImageFont.load_default()


def _class_color(class_name: str) -> tuple[int, int, int]:
    digest = sha1(str(class_name or "").strip().lower().encode("utf-8")).digest()
    return VISUAL_COLORS[digest[0] % len(VISUAL_COLORS)]


def _xywh_to_xyxy(box: list[float]) -> list[float]:
    x, y, w, h = [float(value) for value in box[:4]]
    return [x, y, x + max(0.0, w), y + max(0.0, h)]


def _parse_yolo_annotations(payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    categories = {
        int(category.get("id")): str(category.get("name") or "").strip().lower()
        for category in (payload.get("categories") or [])
        if str(category.get("name") or "").strip()
    }
    boxes_by_class: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for annotation in payload.get("annotations") or []:
        try:
            category_id = int(annotation.get("category_id") or 0)
        except (TypeError, ValueError):
            continue
        class_name = categories.get(category_id)
        bbox = annotation.get("bbox")
        if not class_name or not isinstance(bbox, list) or len(bbox) < 4:
            continue
        try:
            score = float(annotation.get("score") or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        boxes_by_class[class_name].append(
            {
                "bbox": _xywh_to_xyxy([float(value) for value in bbox[:4]]),
                "score": score,
            }
        )
    return dict(boxes_by_class)


def _decode_mask_rle(mask_rle: dict[str, Any] | None) -> np.ndarray | None:
    if not isinstance(mask_rle, dict):
        return None
    size = mask_rle.get("size")
    counts = mask_rle.get("counts")
    if not isinstance(size, list) or len(size) < 2 or not isinstance(counts, list):
        return None
    try:
        height = int(size[0])
        width = int(size[1])
    except (TypeError, ValueError):
        return None
    if height <= 0 or width <= 0:
        return None

    flat = np.zeros(height * width, dtype=np.uint8)
    cursor = 0
    value = 0
    for raw_count in counts:
        try:
            count = int(raw_count)
        except (TypeError, ValueError):
            continue
        if count <= 0:
            value = 1 - value
            continue
        next_cursor = min(flat.size, cursor + count)
        if value == 1:
            flat[cursor:next_cursor] = 1
        cursor = next_cursor
        value = 1 - value
        if cursor >= flat.size:
            break

    return flat.reshape(height, width)


def _format_count_summary(counts: dict[str, int], limit: int = 4) -> str:
    if not counts:
        return "-"
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    shown = [f"{name}x{count}" for name, count in ordered[:limit]]
    if len(ordered) > limit:
        shown.append(f"+{len(ordered) - limit}")
    return ", ".join(shown)


def _image_pair_metrics(rows: list[dict[str, Any]]) -> dict[str, float | int]:
    matched_ious = [float(row.get("matched_iou_mean") or 0.0) for row in rows if float(row.get("matched_iou_mean") or 0.0) > 0.0]
    return {
        "prompt_rows": len(rows),
        "matched_iou_mean": round(sum(matched_ious) / len(matched_ious), 4) if matched_ious else 0.0,
        "matched_pair_count": sum(int(row.get("matched_pair_count") or 0) for row in rows),
        "yolo_box_count": sum(int(row.get("yolo_box_count") or 0) for row in rows),
        "sam_box_count": sum(int(row.get("sam_box_count") or 0) for row in rows),
    }


def _draw_labelled_box(
    draw: ImageDraw.ImageDraw,
    box: list[float],
    *,
    color: tuple[int, int, int],
    label: str,
    width: int = 3,
    font: ImageFont.ImageFont,
) -> None:
    x1, y1, x2, y2 = [float(value) for value in box[:4]]
    draw.rectangle([x1, y1, x2, y2], outline=color, width=width)
    if not label:
        return
    left = max(0.0, x1)
    top = max(0.0, y1 - 20)
    bbox = draw.textbbox((left, top), label, font=font)
    draw.rectangle([bbox[0] - 4, bbox[1] - 2, bbox[2] + 4, bbox[3] + 2], fill=color)
    draw.text((left, top), label, fill=(0, 0, 0), font=font)


def _render_yolo_panel(base_image: Image.Image, payload: dict[str, Any] | None) -> tuple[Image.Image, dict[str, int]]:
    image = base_image.convert("RGBA")
    draw = ImageDraw.Draw(image)
    font = _get_font(15)
    counts: dict[str, int] = {}
    if not isinstance(payload, dict):
        return image.convert("RGB"), counts
    for class_name, items in _parse_yolo_annotations(payload).items():
        counts[class_name] = len(items)
        color = _class_color(class_name)
        for item in items:
            score = float(item.get("score") or 0.0)
            label = f"{class_name} {score:.2f}"
            _draw_labelled_box(draw, list(item["bbox"]), color=color, label=label, font=font)
    return image.convert("RGB"), counts


def _render_sam_panel(base_image: Image.Image, payload: dict[str, Any] | None) -> tuple[Image.Image, dict[str, int]]:
    image = base_image.convert("RGBA")
    draw = ImageDraw.Draw(image)
    font = _get_font(15)
    counts: dict[str, int] = {}
    if not isinstance(payload, dict):
        return image.convert("RGB"), counts

    for detection in payload.get("detections") or []:
        prompt_class = str(detection.get("prompt_class") or "").strip().lower()
        if not prompt_class:
            continue
        counts[prompt_class] = int(counts.get(prompt_class, 0)) + 1
        color = _class_color(prompt_class)

        mask = _decode_mask_rle(detection.get("mask_rle"))
        if mask is not None:
            alpha = Image.fromarray((mask.astype(np.uint8) * VISUAL_MASK_ALPHA), mode="L")
            overlay = Image.new("RGBA", image.size, (*color, 0))
            overlay.putalpha(alpha)
            image = Image.alpha_composite(image, overlay)
            draw = ImageDraw.Draw(image)

        bbox = detection.get("mask_bbox")
        if isinstance(bbox, list) and len(bbox) >= 4:
            try:
                score = float(detection.get("score") or 0.0)
            except (TypeError, ValueError):
                score = 0.0
            _draw_labelled_box(draw, [float(value) for value in bbox[:4]], color=color, label=f"{prompt_class} {score:.2f}", font=font)

        model_box = detection.get("model_box")
        if isinstance(model_box, list) and len(model_box) >= 4:
            draw.rectangle([float(value) for value in model_box[:4]], outline=(*color, 180), width=1)

    return image.convert("RGB"), counts


def _render_frame_visual(
    *,
    frame: FrameRecord,
    visual_frame_path: Path,
    image_rows: list[dict[str, Any]],
    failures_path: Path,
) -> bool:
    try:
        original = Image.open(frame.frame_path).convert("RGB")
        yolo_payload = _read_json(frame.yolo_path) if frame.yolo_path.exists() else None
        sam3_payload = _read_json(frame.sam3_path) if frame.sam3_path.exists() else None
        yolo_panel, yolo_counts = _render_yolo_panel(original, yolo_payload)
        sam_panel, sam_counts = _render_sam_panel(original, sam3_payload)

        width, height = original.size
        canvas_width = (width * 2) + (VISUAL_PANEL_MARGIN * 3)
        canvas_height = VISUAL_HEADER_HEIGHT + height + VISUAL_FOOTER_HEIGHT + VISUAL_PANEL_MARGIN
        canvas = Image.new("RGB", (canvas_width, canvas_height), color=(19, 24, 32))
        draw = ImageDraw.Draw(canvas)
        title_font = _get_font(18)
        body_font = _get_font(15)

        left_x = VISUAL_PANEL_MARGIN
        right_x = (VISUAL_PANEL_MARGIN * 2) + width
        image_y = VISUAL_HEADER_HEIGHT

        canvas.paste(yolo_panel, (left_x, image_y))
        canvas.paste(sam_panel, (right_x, image_y))

        draw.text((left_x, 14), f"{frame.video_rel_path} | frame {frame.frame_index:04d} | t={frame.frame_sec:.3f}s", fill=(245, 247, 250), font=title_font)
        draw.text((left_x, VISUAL_HEADER_HEIGHT - 24), "YOLO bbox", fill=(255, 255, 255), font=body_font)
        draw.text((right_x, VISUAL_HEADER_HEIGHT - 24), "SAM3 mask + bbox", fill=(255, 255, 255), font=body_font)

        metrics = _image_pair_metrics(image_rows)
        footer_y = VISUAL_HEADER_HEIGHT + height + 12
        draw.text(
            (left_x, footer_y),
            f"YOLO total={sum(yolo_counts.values())} | SAM3 total={sum(sam_counts.values())} | prompt_rows={metrics['prompt_rows']} | matched_iou_mean={float(metrics['matched_iou_mean']):.3f}",
            fill=(230, 233, 237),
            font=body_font,
        )
        draw.text(
            (left_x, footer_y + 24),
            f"YOLO: {_format_count_summary(yolo_counts)}",
            fill=(197, 204, 214),
            font=body_font,
        )
        draw.text(
            (left_x, footer_y + 48),
            f"SAM3: {_format_count_summary(sam_counts)}",
            fill=(197, 204, 214),
            font=body_font,
        )

        visual_frame_path.parent.mkdir(parents=True, exist_ok=True)
        canvas.save(visual_frame_path, format="JPEG", quality=90)
        return True
    except Exception as exc:
        _append_jsonl(
            failures_path,
            {
                "stage": "visualize_frame",
                "video_rel_path": frame.video_rel_path,
                "frame_index": frame.frame_index,
                "frame_path": frame.frame_rel_path,
                "error": str(exc),
            },
        )
        return False


def _render_video_preview(frame_dir: Path, output_path: Path, fps: float, failures_path: Path, video_rel_path: str) -> bool:
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-framerate",
            str(max(0.5, float(fps))),
            "-i",
            str((frame_dir / "frame_%04d.jpg").resolve()),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            str(output_path),
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError as exc:
        _append_jsonl(
            failures_path,
            {
                "stage": "visualize_video",
                "video_rel_path": video_rel_path,
                "error": (exc.stderr or b"").decode("utf-8", errors="ignore").strip() or str(exc),
            },
        )
        return False


def render_visualizations(
    *,
    args: argparse.Namespace,
    output_dir: Path,
    frame_records: list[FrameRecord],
    pair_rows: list[dict[str, Any]],
    failures_path: Path,
) -> dict[str, Any]:
    visual_root = output_dir / "compare" / "visuals"
    frames_root = visual_root / "frames"
    videos_root = visual_root / "videos"
    pair_rows_by_image_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in pair_rows:
        image_id = str(row.get("image_id") or "").strip()
        if image_id:
            pair_rows_by_image_id[image_id].append(row)

    rendered_frame_count = 0
    rendered_video_count = 0
    frames_by_video: dict[str, list[Path]] = defaultdict(list)
    video_entries: list[dict[str, Any]] = []

    for frame in sorted(frame_records, key=lambda item: (item.video_rel_path, item.frame_index)):
        frame_dir = frames_root / _resolve_video_output_dir(Path(frame.video_rel_path))
        visual_frame_path = frame_dir / f"frame_{frame.frame_index:04d}.jpg"
        if not (args.resume and visual_frame_path.exists()):
            _render_frame_visual(
                frame=frame,
                visual_frame_path=visual_frame_path,
                image_rows=pair_rows_by_image_id.get(frame.image_id, []),
                failures_path=failures_path,
            )
        if visual_frame_path.exists():
            rendered_frame_count += 1
            frames_by_video[frame.video_rel_path].append(visual_frame_path)

    for video_rel_path, rendered_frames in sorted(frames_by_video.items()):
        if not rendered_frames:
            continue
        video_output_path = videos_root / Path(video_rel_path).with_suffix(".mp4")
        if not (args.resume and video_output_path.exists()):
            _render_video_preview(
                frame_dir=rendered_frames[0].parent,
                output_path=video_output_path,
                fps=args.visual_video_fps,
                failures_path=failures_path,
                video_rel_path=video_rel_path,
            )
        if video_output_path.exists():
            rendered_video_count += 1
        video_entries.append(
            {
                "video_rel_path": video_rel_path,
                "visual_frames_dir": rendered_frames[0].parent.relative_to(output_dir).as_posix(),
                "visual_video_path": video_output_path.relative_to(output_dir).as_posix() if video_output_path.exists() else None,
                "frame_count": len(rendered_frames),
            }
        )

    index_payload = {
        "visualization_root": visual_root.relative_to(output_dir).as_posix(),
        "rendered_frame_count": rendered_frame_count,
        "rendered_video_count": rendered_video_count,
        "visual_video_fps": args.visual_video_fps,
        "videos": video_entries,
    }
    _write_json(visual_root / "index.json", index_payload)
    return {
        "visualization_root": str(visual_root),
        "rendered_visual_frames": rendered_frame_count,
        "rendered_visual_videos": rendered_video_count,
    }


def extract_frames_for_video(
    *,
    args: argparse.Namespace,
    output_dir: Path,
    video_path: Path,
    video_rel_path: Path,
    failures_path: Path,
) -> tuple[VideoMetadata, dict[str, Any], list[FrameRecord]]:
    metadata = probe_video_metadata(video_path)
    sampling = resolve_frame_sampling_policy(
        sampling_mode="raw_video",
        requested_outputs=["bbox"],
        image_profile=args.image_profile,
        duration_sec=metadata.duration_sec,
        fps=metadata.fps,
        frame_count=metadata.frame_count,
    )
    timestamps = plan_frame_timestamps(
        duration_sec=metadata.duration_sec,
        fps=metadata.fps,
        frame_count=metadata.frame_count,
        max_frames_per_video=sampling.effective_max_frames,
        image_profile=args.image_profile,
        frame_interval_sec=sampling.frame_interval_sec,
    )

    video_output_rel_dir = _resolve_video_output_dir(video_rel_path)
    frames_dir = output_dir / "frames" / video_output_rel_dir
    yolo_dir = output_dir / "yolo" / video_output_rel_dir
    sam3_dir = output_dir / "sam3" / video_output_rel_dir
    frames_dir.mkdir(parents=True, exist_ok=True)
    yolo_dir.mkdir(parents=True, exist_ok=True)
    sam3_dir.mkdir(parents=True, exist_ok=True)

    frame_records: list[FrameRecord] = []
    extracted_frames = 0
    failed_frames = 0

    for frame_index, frame_sec in enumerate(timestamps, start=1):
        frame_name = f"frame_{frame_index:04d}.jpg"
        frame_path = frames_dir / frame_name
        frame_rel_path = frame_path.relative_to(output_dir).as_posix()
        yolo_path = yolo_dir / f"frame_{frame_index:04d}.json"
        sam3_path = sam3_dir / f"frame_{frame_index:04d}.json"
        image_id = build_image_id(video_rel_path.as_posix(), frame_index, frame_sec)

        try:
            if args.resume and frame_path.exists():
                frame_bytes = frame_path.read_bytes()
            else:
                frame_bytes = extract_frame_jpeg_bytes(
                    video_path,
                    frame_sec,
                    jpeg_quality=args.jpeg_quality,
                )
                frame_path.write_bytes(frame_bytes)
            frame_meta = describe_frame_bytes(frame_bytes)
            frame_records.append(
                FrameRecord(
                    image_id=image_id,
                    video_rel_path=video_rel_path.as_posix(),
                    frame_index=frame_index,
                    frame_sec=float(frame_sec),
                    frame_path=frame_path,
                    frame_rel_path=frame_rel_path,
                    yolo_path=yolo_path,
                    yolo_rel_path=yolo_path.relative_to(output_dir).as_posix(),
                    sam3_path=sam3_path,
                    sam3_rel_path=sam3_path.relative_to(output_dir).as_posix(),
                    width=int(frame_meta["width"]),
                    height=int(frame_meta["height"]),
                )
            )
            extracted_frames += 1
        except Exception as exc:
            failed_frames += 1
            _append_jsonl(
                failures_path,
                {
                    "stage": "frame_extract",
                    "video_rel_path": video_rel_path.as_posix(),
                    "frame_index": frame_index,
                    "frame_sec": frame_sec,
                    "error": str(exc),
                },
            )

    manifest_entry = {
        "video_rel_path": video_rel_path.as_posix(),
        "video_abs_path": str(video_path),
        "duration_sec": metadata.duration_sec,
        "fps": metadata.fps,
        "frame_count": metadata.frame_count,
        "sampling_mode": sampling.sampling_mode,
        "effective_max_frames": sampling.effective_max_frames,
        "frame_interval_sec": sampling.frame_interval_sec,
        "policy_source": sampling.policy_source,
        "planned_timestamps": timestamps,
        "planned_frames": len(timestamps),
        "extracted_frames": extracted_frames,
        "failed_frame_extracts": failed_frames,
    }
    return metadata, manifest_entry, frame_records


def _extract_elapsed_ms(payload: dict[str, Any]) -> float:
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return 0.0
    try:
        return float(meta.get("elapsed_ms") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _resolve_prompt_classes(payload: dict[str, Any], yolo_boxes_by_class: dict[str, list[list[float]]]) -> list[str]:
    meta = payload.get("meta")
    if isinstance(meta, dict) and isinstance(meta.get("requested_classes"), list):
        resolved: list[str] = []
        seen: set[str] = set()
        for value in meta.get("requested_classes") or []:
            rendered = str(value or "").strip().lower()
            if not rendered or rendered in seen:
                continue
            seen.add(rendered)
            resolved.append(rendered)
        if resolved:
            return resolved
    return sorted(str(key).strip().lower() for key in yolo_boxes_by_class.keys() if str(key).strip())


def _merge_prompt_classes(
    prompts: list[str],
    yolo_boxes_by_class: dict[str, list[list[float]]],
    sam_grouped: dict[str, list[dict[str, Any]]],
) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in [*prompts, *yolo_boxes_by_class.keys(), *sam_grouped.keys()]:
        rendered = str(value or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        merged.append(rendered)
    return merged


def run_yolo_for_video(
    *,
    args: argparse.Namespace,
    video_rel_path: Path,
    frame_records: list[FrameRecord],
    requested_classes: list[str],
    yolo_client: YOLOWorldClient | None,
    failures_path: Path,
) -> tuple[int, int]:
    active_class_confidence_thresholds = resolve_active_class_confidence_thresholds(
        requested_classes,
        args.confidence_threshold,
    )
    effective_request_confidence_threshold = resolve_effective_request_confidence_threshold(
        args.confidence_threshold,
        active_class_confidence_thresholds,
    )

    success_count = 0
    failed_count = 0
    pending_records: list[FrameRecord] = []

    for frame in frame_records:
        if frame.yolo_path.exists() and (args.resume or args.skip_yolo):
            success_count += 1
            continue
        if args.skip_yolo:
            failed_count += 1
            _append_jsonl(
                failures_path,
                {
                    "stage": "yolo_detect",
                    "video_rel_path": video_rel_path.as_posix(),
                    "frame_index": frame.frame_index,
                    "frame_path": frame.frame_rel_path,
                    "error": "skip_yolo_requested_but_output_missing",
                },
            )
            continue
        pending_records.append(frame)

    if not pending_records:
        return success_count, failed_count

    if yolo_client is None:
        raise RuntimeError("yolo_client_required")

    for batch_start in range(0, len(pending_records), max(1, args.batch_size)):
        batch = pending_records[batch_start : batch_start + max(1, args.batch_size)]
        frame_bytes_list = [frame.frame_path.read_bytes() for frame in batch]
        try:
            batch_results = yolo_client.detect_batch(
                frame_bytes_list,
                conf=effective_request_confidence_threshold,
                iou=args.iou_threshold,
                classes=requested_classes or None,
            )
        except Exception as exc:
            for frame in batch:
                failed_count += 1
                _append_jsonl(
                    failures_path,
                    {
                        "stage": "yolo_detect",
                        "video_rel_path": video_rel_path.as_posix(),
                        "frame_index": frame.frame_index,
                        "frame_path": frame.frame_rel_path,
                        "error": str(exc),
                    },
                )
            continue

        for batch_index, frame in enumerate(batch):
            try:
                result = batch_results[batch_index] if batch_index < len(batch_results) else {}
                detections = filter_detections_by_class_confidence(
                    result.get("detections", []),
                    global_confidence_threshold=args.confidence_threshold,
                    class_confidence_thresholds=active_class_confidence_thresholds,
                )
                image_size = result.get("image_size")
                image_width = (
                    result.get("image_width")
                    or (image_size[0] if isinstance(image_size, list) and len(image_size) >= 1 else frame.width)
                )
                image_height = (
                    result.get("image_height")
                    or (image_size[1] if isinstance(image_size, list) and len(image_size) >= 2 else frame.height)
                )
                payload = build_coco_detection_payload(
                    image_id=frame.image_id,
                    source_clip_id=None,
                    image_key=frame.frame_rel_path,
                    image_width=image_width,
                    image_height=image_height,
                    detections=detections,
                    requested_classes=requested_classes,
                    class_source="manual_category_mapping" if requested_classes else "server_default",
                    resolved_config_id=None,
                    confidence_threshold=args.confidence_threshold,
                    iou_threshold=args.iou_threshold,
                    detected_at=datetime.now(),
                    effective_request_confidence_threshold=effective_request_confidence_threshold,
                    class_confidence_thresholds=active_class_confidence_thresholds,
                    elapsed_ms=result.get("elapsed_ms"),
                )
                payload.setdefault("meta", {})
                payload["meta"]["video_rel_path"] = frame.video_rel_path
                payload["meta"]["frame_index"] = frame.frame_index
                payload["meta"]["frame_sec"] = frame.frame_sec
                payload["meta"]["frame_path"] = frame.frame_rel_path
                _write_json(frame.yolo_path, payload)
                success_count += 1
            except Exception as exc:
                failed_count += 1
                _append_jsonl(
                    failures_path,
                    {
                        "stage": "yolo_store",
                        "video_rel_path": video_rel_path.as_posix(),
                        "frame_index": frame.frame_index,
                        "frame_path": frame.frame_rel_path,
                        "error": str(exc),
                    },
                )

    return success_count, failed_count


def run_sam3_for_video(
    *,
    args: argparse.Namespace,
    video_rel_path: Path,
    frame_records: list[FrameRecord],
    sam3_client: SAM3Client | None,
    failures_path: Path,
) -> tuple[int, int]:
    success_count = 0
    failed_count = 0

    for frame in frame_records:
        if frame.sam3_path.exists() and (args.resume or args.skip_sam3):
            success_count += 1
            continue
        if args.skip_sam3:
            failed_count += 1
            _append_jsonl(
                failures_path,
                {
                    "stage": "sam3_segment",
                    "video_rel_path": video_rel_path.as_posix(),
                    "frame_index": frame.frame_index,
                    "frame_path": frame.frame_rel_path,
                    "error": "skip_sam3_requested_but_output_missing",
                },
            )
            continue
        if sam3_client is None:
            raise RuntimeError("sam3_client_required")
        if not frame.yolo_path.exists():
            failed_count += 1
            _append_jsonl(
                failures_path,
                {
                    "stage": "sam3_segment",
                    "video_rel_path": video_rel_path.as_posix(),
                    "frame_index": frame.frame_index,
                    "frame_path": frame.frame_rel_path,
                    "error": "missing_yolo_output",
                },
            )
            continue

        try:
            yolo_payload = _read_json(frame.yolo_path)
            yolo_boxes_by_class = parse_yolo_coco_payload(yolo_payload)
            prompts = _resolve_prompt_classes(yolo_payload, yolo_boxes_by_class)
            if not prompts:
                raise RuntimeError("empty_prompts")
            image_bytes = frame.frame_path.read_bytes()
            prompt_score_thresholds = resolve_active_class_confidence_thresholds(
                prompts,
                args.confidence_threshold,
            )
            sam_result = sam3_client.segment(
                image_bytes,
                prompts=prompts,
                filename=frame.frame_path.name,
                score_threshold=args.confidence_threshold,
                max_masks_per_prompt=max(1, int(args.max_masks_per_prompt)),
                per_prompt_score_thresholds=prompt_score_thresholds,
            )
            payload = {
                "image_id": frame.image_id,
                "video_rel_path": frame.video_rel_path,
                "frame_index": frame.frame_index,
                "frame_sec": frame.frame_sec,
                "frame_path": frame.frame_rel_path,
                "image_width": frame.width,
                "image_height": frame.height,
                "requested_classes": prompts,
                "sam3_prompt_score_thresholds": prompt_score_thresholds,
                "yolo_labels_key": frame.yolo_rel_path,
                "yolo_latency_ms": _extract_elapsed_ms(yolo_payload),
                "sam3_total_latency_ms": float(sam_result.get("elapsed_ms") or 0.0),
                "sam3_per_prompt_latency_ms": sam_result.get("per_prompt_latency_ms") or {},
                "sam3_device": sam_result.get("device"),
                "gpu_memory_peak_gb": sam_result.get("gpu_memory_peak_gb"),
                "detections": list(sam_result.get("detections") or []),
                "generated_at": datetime.now().isoformat(),
            }
            _write_json(frame.sam3_path, payload)
            success_count += 1
        except Exception as exc:
            failed_count += 1
            _append_jsonl(
                failures_path,
                {
                    "stage": "sam3_segment",
                    "video_rel_path": video_rel_path.as_posix(),
                    "frame_index": frame.frame_index,
                    "frame_path": frame.frame_rel_path,
                    "error": str(exc),
                },
            )

    return success_count, failed_count


def _run_yolo_worker(
    *,
    args: argparse.Namespace,
    requested_classes: list[str],
    yolo_client: YOLOWorldClient | None,
    failures_path: Path,
    input_queue: Queue[VideoTask | object],
    output_queue: Queue[VideoTask | object],
    sentinel: object,
) -> None:
    while True:
        item = input_queue.get()
        try:
            if item is sentinel:
                output_queue.put(sentinel)
                return

            task = item
            LOGGER.info(
                "[%d/%d] YOLO 처리: %s frames=%d",
                task.index,
                task.total_videos,
                task.video_rel_path,
                len(task.frame_records),
            )
            try:
                video_yolo_success, video_yolo_failed = run_yolo_for_video(
                    args=args,
                    video_rel_path=task.video_rel_path,
                    frame_records=task.frame_records,
                    requested_classes=requested_classes,
                    yolo_client=yolo_client,
                    failures_path=failures_path,
                )
            except Exception as exc:
                LOGGER.exception("YOLO worker 실패: %s", task.video_rel_path)
                _append_jsonl(
                    failures_path,
                    {
                        "stage": "yolo_worker",
                        "video_rel_path": task.video_rel_path.as_posix(),
                        "error": str(exc),
                    },
                )
                video_yolo_success = 0
                video_yolo_failed = len(task.frame_records)

            task.manifest_entry["yolo_success_frames"] = video_yolo_success
            task.manifest_entry["yolo_failed_frames"] = video_yolo_failed
            output_queue.put(task)
        finally:
            input_queue.task_done()


def _run_sam3_worker(
    *,
    args: argparse.Namespace,
    sam3_client: SAM3Client | None,
    failures_path: Path,
    input_queue: Queue[VideoTask | object],
    sentinel: object,
) -> None:
    while True:
        item = input_queue.get()
        try:
            if item is sentinel:
                return

            task = item
            LOGGER.info(
                "[%d/%d] SAM3 처리: %s frames=%d",
                task.index,
                task.total_videos,
                task.video_rel_path,
                len(task.frame_records),
            )
            try:
                video_sam3_success, video_sam3_failed = run_sam3_for_video(
                    args=args,
                    video_rel_path=task.video_rel_path,
                    frame_records=task.frame_records,
                    sam3_client=sam3_client,
                    failures_path=failures_path,
                )
            except Exception as exc:
                LOGGER.exception("SAM3 worker 실패: %s", task.video_rel_path)
                _append_jsonl(
                    failures_path,
                    {
                        "stage": "sam3_worker",
                        "video_rel_path": task.video_rel_path.as_posix(),
                        "error": str(exc),
                    },
                )
                video_sam3_success = 0
                video_sam3_failed = len(task.frame_records)

            task.manifest_entry["sam3_success_frames"] = video_sam3_success
            task.manifest_entry["sam3_failed_frames"] = video_sam3_failed
        finally:
            input_queue.task_done()


def serialize_pairs_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "benchmark_id",
        "image_id",
        "prompt_class",
        "yolo_box_count",
        "sam_box_count",
        "bbox_count_delta",
        "matched_pair_count",
        "matched_iou_mean",
        "matched_iou_median",
        "yolo_to_sam_coverage",
        "sam_to_yolo_coverage",
        "unmatched_yolo_rate",
        "unmatched_sam_rate",
        "prompt_hit_rate",
        "sam_model_box_vs_mask_bbox_delta",
        "match_count_iou_0_3",
        "match_count_iou_0_5",
        "match_count_iou_0_7",
        "yolo_labels_key",
        "sam3_labels_key",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def build_compare_rows(
    *,
    benchmark_id: str,
    frame_records: list[FrameRecord],
) -> tuple[list[dict[str, Any]], list[float], list[float], float, int]:
    pair_rows: list[dict[str, Any]] = []
    yolo_latency_ms: list[float] = []
    sam3_total_latency_ms: list[float] = []
    gpu_memory_peak_gb = 0.0
    compared_frames = 0

    for frame in frame_records:
        if not frame.yolo_path.exists() or not frame.sam3_path.exists():
            continue
        yolo_payload = _read_json(frame.yolo_path)
        sam3_payload = _read_json(frame.sam3_path)
        yolo_boxes_by_class = parse_yolo_coco_payload(yolo_payload)
        grouped_sam = group_sam_detections_by_prompt(list(sam3_payload.get("detections") or []))
        prompts = _resolve_prompt_classes(yolo_payload, yolo_boxes_by_class)
        for prompt_class in _merge_prompt_classes(prompts, yolo_boxes_by_class, grouped_sam):
            pair_rows.append(
                compare_prompt_boxes(
                    image_id=frame.image_id,
                    prompt_class=prompt_class,
                    yolo_boxes=list(yolo_boxes_by_class.get(prompt_class, [])),
                    sam_detections=list(grouped_sam.get(prompt_class, [])),
                    benchmark_id=benchmark_id,
                    yolo_labels_key=frame.yolo_rel_path,
                    sam3_labels_key=frame.sam3_rel_path,
                )
            )
        compared_frames += 1
        yolo_latency_ms.append(_extract_elapsed_ms(yolo_payload))
        try:
            sam3_total_latency_ms.append(float(sam3_payload.get("sam3_total_latency_ms") or 0.0))
        except (TypeError, ValueError):
            sam3_total_latency_ms.append(0.0)
        try:
            gpu_memory_peak_gb = max(gpu_memory_peak_gb, float(sam3_payload.get("gpu_memory_peak_gb") or 0.0))
        except (TypeError, ValueError):
            pass

    return pair_rows, sam3_total_latency_ms, yolo_latency_ms, gpu_memory_peak_gb, compared_frames


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    start_time = time.perf_counter()
    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()
    compare_dir = output_dir / "compare"
    failures_path = compare_dir / "failures.jsonl"
    pairs_path = compare_dir / "pairs.csv"
    summary_path = compare_dir / "summary.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    compare_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        raise FileNotFoundError(f"input_dir_not_found:{input_dir}")

    requested_classes, categories = resolve_requested_classes(args)
    benchmark_id = output_dir.name
    videos = collect_videos(input_dir, args.video_glob, args.max_videos)
    if not videos:
        raise RuntimeError(f"no_videos_found:{input_dir}")

    LOGGER.info("입력 영상 %d건 발견: %s", len(videos), input_dir)

    yolo_client: YOLOWorldClient | None = None
    sam3_client: SAM3Client | None = None
    yolo_health: dict[str, Any] | None = None
    sam3_health: dict[str, Any] | None = None

    try:
        if not args.skip_yolo:
            yolo_client = YOLOWorldClient(api_url=args.yolo_url, timeout=300)
            if not yolo_client.wait_until_ready(max_wait_sec=120):
                raise RuntimeError("yolo_server_not_ready")
            yolo_health = yolo_client.health()

        if not args.skip_sam3:
            sam3_client = SAM3Client(api_url=args.sam3_url, timeout=600)
            if not sam3_client.wait_until_ready(max_wait_sec=300):
                raise RuntimeError("sam3_server_not_ready")
            sam3_health = sam3_client.health()

        run_config = {
            "benchmark_id": benchmark_id,
            "started_at": datetime.now().isoformat(),
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "video_glob": args.video_glob,
            "image_profile": args.image_profile,
            "confidence_threshold": args.confidence_threshold,
            "iou_threshold": args.iou_threshold,
            "batch_size": args.batch_size,
            "max_videos": args.max_videos,
            "max_masks_per_prompt": args.max_masks_per_prompt,
            "jpeg_quality": args.jpeg_quality,
            "requested_categories": categories,
            "requested_classes": requested_classes,
            "skip_yolo": args.skip_yolo,
            "skip_sam3": args.skip_sam3,
            "skip_visuals": args.skip_visuals,
            "visual_video_fps": args.visual_video_fps,
            "resume": args.resume,
            "yolo_url": args.yolo_url,
            "sam3_url": args.sam3_url,
            "yolo_health": yolo_health,
            "sam3_health": sam3_health,
        }
        _write_json(output_dir / "run_config.json", run_config)

        videos_manifest: list[dict[str, Any]] = []
        all_frames: list[FrameRecord] = []
        total_extract_failures = 0
        yolo_queue: Queue[VideoTask | object] = Queue()
        sam3_queue: Queue[VideoTask | object] = Queue()
        pipeline_sentinel = object()
        yolo_thread = threading.Thread(
            target=_run_yolo_worker,
            kwargs={
                "args": args,
                "requested_classes": requested_classes,
                "yolo_client": yolo_client,
                "failures_path": failures_path,
                "input_queue": yolo_queue,
                "output_queue": sam3_queue,
                "sentinel": pipeline_sentinel,
            },
            name="manual-compare-yolo",
            daemon=True,
        )
        sam3_thread = threading.Thread(
            target=_run_sam3_worker,
            kwargs={
                "args": args,
                "sam3_client": sam3_client,
                "failures_path": failures_path,
                "input_queue": sam3_queue,
                "sentinel": pipeline_sentinel,
            },
            name="manual-compare-sam3",
            daemon=True,
        )
        yolo_thread.start()
        sam3_thread.start()

        pipeline_error: Exception | None = None
        try:
            # Extract the next video while GPU1 (YOLO) and GPU0 (SAM3) process prior work.
            for index, video_path in enumerate(videos, start=1):
                video_rel_path = video_path.relative_to(input_dir)
                LOGGER.info("[%d/%d] 프레임 추출: %s", index, len(videos), video_rel_path)
                _metadata, manifest_entry, frame_records = extract_frames_for_video(
                    args=args,
                    output_dir=output_dir,
                    video_path=video_path,
                    video_rel_path=video_rel_path,
                    failures_path=failures_path,
                )
                manifest_entry["yolo_success_frames"] = 0
                manifest_entry["yolo_failed_frames"] = 0
                manifest_entry["sam3_success_frames"] = 0
                manifest_entry["sam3_failed_frames"] = 0
                videos_manifest.append(manifest_entry)
                all_frames.extend(frame_records)
                total_extract_failures += int(manifest_entry["failed_frame_extracts"])
                yolo_queue.put(
                    VideoTask(
                        index=index,
                        total_videos=len(videos),
                        video_rel_path=video_rel_path,
                        manifest_entry=manifest_entry,
                        frame_records=frame_records,
                    )
                )
        except Exception as exc:
            pipeline_error = exc
        finally:
            yolo_queue.put(pipeline_sentinel)

        yolo_queue.join()
        sam3_queue.join()
        yolo_thread.join()
        sam3_thread.join()

        if pipeline_error is not None:
            raise pipeline_error

        yolo_success_frames = sum(int(entry.get("yolo_success_frames") or 0) for entry in videos_manifest)
        yolo_failed_frames = sum(int(entry.get("yolo_failed_frames") or 0) for entry in videos_manifest)
        sam3_success_frames = sum(int(entry.get("sam3_success_frames") or 0) for entry in videos_manifest)
        sam3_failed_frames = sum(int(entry.get("sam3_failed_frames") or 0) for entry in videos_manifest)

        _write_json(output_dir / "videos_manifest.json", videos_manifest)

        pair_rows, sam3_total_latency_ms, yolo_latency_ms, gpu_memory_peak_gb, compared_frames = build_compare_rows(
            benchmark_id=benchmark_id,
            frame_records=all_frames,
        )
        serialize_pairs_csv(pairs_path, pair_rows)

        summary = summarize_benchmark_rows(
            pair_rows,
            benchmark_id=benchmark_id,
            total_images=compared_frames,
            sam3_total_latency_ms=sam3_total_latency_ms,
            yolo_latency_ms=yolo_latency_ms,
            gpu_memory_peak_gb=gpu_memory_peak_gb,
        )
        summary.update(
            {
                "input_dir": str(input_dir),
                "output_dir": str(output_dir),
                "total_videos": len(videos),
                "total_frames": len(all_frames),
                "frame_extract_failed_frames": total_extract_failures,
                "yolo_success_frames": yolo_success_frames,
                "yolo_failed_frames": yolo_failed_frames,
                "sam3_success_frames": sam3_success_frames,
                "sam3_failed_frames": sam3_failed_frames,
                "compared_frames": compared_frames,
                "pair_row_count": len(pair_rows),
                "requested_categories": categories,
                "requested_classes": requested_classes,
                "completed_at": datetime.now().isoformat(),
                "elapsed_sec": round(time.perf_counter() - start_time, 2),
            }
        )

        if not args.skip_visuals:
            summary.update(
                render_visualizations(
                    args=args,
                    output_dir=output_dir,
                    frame_records=all_frames,
                    pair_rows=pair_rows,
                    failures_path=failures_path,
                )
            )
        _write_json(summary_path, summary)

        LOGGER.info(
            "완료: videos=%d frames=%d compared=%d pair_rows=%d output=%s",
            len(videos),
            len(all_frames),
            compared_frames,
            len(pair_rows),
            output_dir,
        )
        return 0
    finally:
        if yolo_client is not None:
            yolo_client.close()
        if sam3_client is not None:
            sam3_client.close()


if __name__ == "__main__":
    raise SystemExit(main())
