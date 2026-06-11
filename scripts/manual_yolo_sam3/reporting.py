from __future__ import annotations

import csv
import json
import shutil
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw, ImageFont, ImageOps

FONT_PATH = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _dir_size_mb(path: Path) -> float:
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return round(total / 1024 / 1024, 2)


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    return round(float(np.percentile(np.asarray(values, dtype=float), q)), 2)


def _safe_ratio(a: float, b: float) -> float | None:
    if not b:
        return None
    return round(float(a) / float(b), 2)


def _load_font(size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(FONT_PATH, size)


def _draw_wrapped(
    draw: ImageDraw.ImageDraw,
    text: str,
    xy: tuple[int, int],
    *,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int],
    max_width: int,
    line_gap: int = 6,
) -> int:
    x, y = xy
    lines: list[str] = []
    for raw_line in text.split("\n"):
        current = ""
        for ch in raw_line:
            probe = current + ch
            if draw.textlength(probe, font=font) <= max_width or not current:
                current = probe
            else:
                lines.append(current)
                current = ch
        if current or not raw_line:
            lines.append(current)
    for line in lines:
        draw.text((x, y), line, font=font, fill=fill)
        y += font.size + line_gap
    return y


def _group_for_video(rel_path: str) -> str:
    lower = rel_path.lower()
    if lower.startswith("smoking/"):
        return "smoking"
    if lower.startswith("weapon/"):
        return "weapon"
    if lower.startswith("violence/"):
        return "violence"
    if lower.startswith("fall_down/") or lower.startswith("falldown/") or "falldown" in lower:
        return "falldown"
    if lower.startswith("generative_video/") and ("fire" in lower or "smoke" in lower):
        return "fire_smoke"
    if lower.startswith("generative_video/") and "falldown" in lower:
        return "falldown"
    return "other"


def _load_count_maps(run_root: Path) -> tuple[dict[str, int], dict[str, int]]:
    yolo_counts: dict[str, int] = {}
    sam_counts: dict[str, int] = {}

    for path in (run_root / "yolo").rglob("*.json"):
        payload = _read_json(path)
        rel = str(path.relative_to(run_root / "yolo")).replace("\\", "/")
        yolo_counts[rel] = len(payload.get("annotations") or [])

    for path in (run_root / "sam3").rglob("*.json"):
        payload = _read_json(path)
        rel = str(path.relative_to(run_root / "sam3")).replace("\\", "/")
        sam_counts[rel] = len(payload.get("detections") or [])

    return yolo_counts, sam_counts


def _select_visual_candidates(run_root: Path) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    visual_index = _read_json(run_root / "compare" / "visuals" / "index.json")
    yolo_counts, sam_counts = _load_count_maps(run_root)
    entries: list[dict[str, Any]] = []
    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for video in visual_index.get("videos") or []:
        video_rel_path = str(video.get("video_rel_path") or "")
        visual_dir = run_root / str(video.get("visual_frames_dir") or "")
        frame_files = sorted(visual_dir.glob("frame_*.jpg"))
        if not video_rel_path or not frame_files:
            continue
        best: dict[str, Any] | None = None
        for idx, frame_file in enumerate(frame_files):
            stem = frame_file.stem
            json_rel = str(Path(video_rel_path).with_suffix("") / f"{stem}.json").replace("\\", "/")
            yolo_count = int(yolo_counts.get(json_rel, 0))
            sam_count = int(sam_counts.get(json_rel, 0))
            score = (yolo_count * 10) + sam_count
            candidate = {
                "video_rel_path": video_rel_path,
                "group": _group_for_video(video_rel_path),
                "frame_path": frame_file,
                "frame_name": frame_file.name,
                "json_rel": json_rel,
                "yolo_count": yolo_count,
                "sam_count": sam_count,
                "score": score,
                "index": idx,
            }
            if best is None or score > int(best["score"]) or (
                score == int(best["score"])
                and abs(idx - len(frame_files) // 2) < abs(int(best["index"]) - len(frame_files) // 2)
            ):
                best = candidate
        if best is not None:
            entries.append(best)
            by_group[str(best["group"])].append(best)

    entries.sort(key=lambda item: (-int(item["score"]), -int(item["sam_count"]), -int(item["yolo_count"]), str(item["video_rel_path"])))
    for group_items in by_group.values():
        group_items.sort(key=lambda item: (-int(item["score"]), -int(item["sam_count"]), -int(item["yolo_count"]), str(item["video_rel_path"])))
    return entries, by_group


def _render_tile(
    canvas: Image.Image,
    item: dict[str, Any],
    box: tuple[int, int, int, int],
    *,
    title_font: ImageFont.ImageFont,
    body_font: ImageFont.ImageFont,
) -> None:
    x, y, width, height = box
    image = Image.open(Path(item["frame_path"])).convert("RGB")
    tile_image_height = height - 114
    fitted = ImageOps.fit(image, (width, tile_image_height), method=Image.Resampling.LANCZOS)
    canvas.paste(fitted, (x, y))
    overlay_y = y + tile_image_height
    overlay = Image.new("RGBA", (width, height - tile_image_height), (12, 18, 28, 236))
    canvas.paste(overlay, (x, overlay_y), overlay)
    draw = ImageDraw.Draw(canvas)
    draw.rounded_rectangle((x, y, x + width, y + height), radius=16, outline=(68, 89, 118), width=3)
    label = (
        f"{item['group']} | YOLO {item['yolo_count']} | SAM3 {item['sam_count']}\n"
        f"{item['video_rel_path']}\n"
        f"{item['frame_name']}"
    )
    _draw_wrapped(draw, label, (x + 14, overlay_y + 10), font=body_font, fill=(240, 244, 248), max_width=width - 28)


def _render_board(
    *,
    title: str,
    subtitle: str,
    items: list[dict[str, Any]],
    output_path: Path,
    summary_lines: list[str] | None = None,
    cols: int = 3,
    rows: int = 2,
) -> None:
    width, height = 2400, 1600
    canvas = Image.new("RGB", (width, height), (10, 14, 20))
    draw = ImageDraw.Draw(canvas)
    title_font = _load_font(58)
    subtitle_font = _load_font(28)
    body_font = _load_font(24)
    summary_font = _load_font(28)

    draw.rounded_rectangle((45, 35, width - 45, height - 35), radius=28, fill=(17, 24, 34), outline=(45, 60, 82), width=2)
    draw.text((85, 70), title, font=title_font, fill=(245, 247, 250))
    draw.text((88, 148), subtitle, font=subtitle_font, fill=(165, 180, 200))

    top_y = 210
    if summary_lines:
        for line in summary_lines:
            draw.rounded_rectangle((88, top_y - 4, width - 88, top_y + 42), radius=10, fill=(24, 34, 48))
            draw.text((106, top_y), line, font=summary_font, fill=(224, 232, 240))
            top_y += 56
        top_y += 8

    margin_x = 88
    gap_x = 24
    gap_y = 22
    usable_height = height - top_y - 76
    tile_width = int((width - (margin_x * 2) - (gap_x * (cols - 1))) / cols)
    tile_height = int((usable_height - (gap_y * (rows - 1))) / rows)

    for index, item in enumerate(items[: cols * rows]):
        row, col = divmod(index, cols)
        x = margin_x + col * (tile_width + gap_x)
        y = top_y + row * (tile_height + gap_y)
        _render_tile(canvas, item, (x, y, tile_width, tile_height), title_font=subtitle_font, body_font=body_font)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(output_path, quality=92)


def _compute_detection_stats(run_root: Path) -> dict[str, Any]:
    yolo_class_counts = Counter()
    sam_class_counts = Counter()
    yolo_positive_frames = 0
    sam_positive_frames = 0
    yolo_positive_videos: set[str] = set()
    sam_positive_videos: set[str] = set()
    yolo_total_detections = 0
    sam_total_detections = 0

    for path in (run_root / "yolo").rglob("*.json"):
        payload = _read_json(path)
        categories = {int(item["id"]): str(item["name"]) for item in payload.get("categories") or [] if "id" in item and "name" in item}
        annotations = payload.get("annotations") or []
        if annotations:
            yolo_positive_frames += 1
            rel = str(path.relative_to(run_root / "yolo")).replace("\\", "/")
            yolo_positive_videos.add("/".join(rel.split("/")[:-1]) + ".mp4")
        for annotation in annotations:
            yolo_total_detections += 1
            yolo_class_counts[categories.get(int(annotation.get("category_id") or 0), str(annotation.get("category_id")))] += 1

    for path in (run_root / "sam3").rglob("*.json"):
        payload = _read_json(path)
        detections = payload.get("detections") or []
        if detections:
            sam_positive_frames += 1
            rel = str(path.relative_to(run_root / "sam3")).replace("\\", "/")
            sam_positive_videos.add("/".join(rel.split("/")[:-1]) + ".mp4")
        for detection in detections:
            sam_total_detections += 1
            sam_class_counts[str(detection.get("prompt_class") or "unknown")] += 1

    return {
        "yolo_positive_frames": yolo_positive_frames,
        "sam_positive_frames": sam_positive_frames,
        "yolo_positive_videos": len(yolo_positive_videos),
        "sam_positive_videos": len(sam_positive_videos),
        "yolo_total_detections": yolo_total_detections,
        "sam_total_detections": sam_total_detections,
        "yolo_top_classes": yolo_class_counts.most_common(10),
        "sam_top_classes": sam_class_counts.most_common(10),
    }


def generate_report_assets(output_dir: Path) -> dict[str, Any]:
    run_root = output_dir.resolve()
    summary = _read_json(run_root / "compare" / "summary.json")
    report_dir = run_root / "report_assets"
    boards_dir = report_dir / "boards"
    key_frames_dir = report_dir / "key_frames"
    report_dir.mkdir(parents=True, exist_ok=True)
    boards_dir.mkdir(parents=True, exist_ok=True)
    key_frames_dir.mkdir(parents=True, exist_ok=True)

    entries, by_group = _select_visual_candidates(run_root)
    detection_stats = _compute_detection_stats(run_root)
    highlight_entries = entries[:8]
    key_frame_records: list[dict[str, Any]] = []
    for index, item in enumerate(highlight_entries, start=1):
        copied_path = key_frames_dir / f"{index:02d}_{item['group']}_{Path(str(item['video_rel_path'])).stem}_{item['frame_name']}"
        shutil.copy2(Path(item["frame_path"]), copied_path)
        key_frame_records.append(
            {
                "rank": index,
                "group": item["group"],
                "video_rel_path": item["video_rel_path"],
                "frame_name": item["frame_name"],
                "yolo_count": item["yolo_count"],
                "sam_count": item["sam_count"],
                "copied_path": str(copied_path),
            }
        )

    cover_items: list[dict[str, Any]] = []
    for group in ["falldown", "smoking", "weapon", "violence", "fire_smoke"]:
        if by_group.get(group):
            cover_items.append(by_group[group][0])
    for item in highlight_entries:
        if len(cover_items) >= 6:
            break
        if item not in cover_items:
            cover_items.append(item)

    _render_board(
        title="tmp_data_2 YOLO vs SAM3.1 비교 요약",
        subtitle="NotebookLM 업로드용 요약 보드",
        items=cover_items,
        output_path=boards_dir / "00_run_summary.jpg",
        summary_lines=[
            f"입력 경로: {summary.get('input_dir')}",
            f"완료 시각: {summary.get('completed_at')} | 실행 시간: {summary.get('elapsed_sec')}초",
            f"전체 영상 {summary.get('total_videos')}건 | 전체 프레임 {summary.get('total_frames')}건 | pair row {summary.get('pair_row_count')}",
            f"YOLO 성공 {summary.get('yolo_success_frames')} / 실패 {summary.get('yolo_failed_frames')} | SAM3 성공 {summary.get('sam3_success_frames')} / 실패 {summary.get('sam3_failed_frames')}",
            f"YOLO 양성 프레임 {detection_stats['yolo_positive_frames']}건 | SAM3 양성 프레임 {detection_stats['sam_positive_frames']}건 | 시각화 프레임 {summary.get('rendered_visual_frames')}",
            f"요청 카테고리: {', '.join(summary.get('requested_categories') or [])}",
        ],
    )

    _render_board(
        title="검출 하이라이트",
        subtitle="YOLO 또는 SAM3에서 양성으로 잡힌 대표 프레임",
        items=highlight_entries[:6],
        output_path=boards_dir / "01_detection_highlights.jpg",
        summary_lines=[
            "양성 프레임 우선으로 추린 하이라이트입니다.",
            "각 타일에는 그룹, 영상 경로, 프레임 번호, YOLO/SAM3 검출 수를 함께 표시했습니다.",
        ],
    )

    board_specs = [
        ("falldown", "02_falldown_board.jpg", "낙상(falldown) 대표 프레임"),
        ("smoking", "03_smoking_board.jpg", "흡연(smoking) 대표 프레임"),
        ("weapon", "04_weapon_board.jpg", "무기(weapon) 대표 프레임"),
        ("violence", "05_violence_board.jpg", "폭력(violence) 대표 프레임"),
        ("fire_smoke", "06_fire_smoke_board.jpg", "화재/연기(fire, smoke) 대표 프레임"),
    ]
    board_records: list[dict[str, Any]] = []
    for group, filename, title in board_specs:
        items = (by_group.get(group) or [])[:6]
        if not items:
            continue
        board_path = boards_dir / filename
        _render_board(
            title=title,
            subtitle=f"그룹: {group}",
            items=items,
            output_path=board_path,
            summary_lines=[
                f"선정 영상 수: {len(by_group.get(group) or [])} | 보드 타일 수: {len(items)}",
                "양성 대표 프레임 우선, 없으면 중간 프레임을 사용했습니다.",
            ],
        )
        board_records.append({"group": group, "path": str(board_path), "items": len(items)})

    overview_items = [by_group[group][0] for group in ["falldown", "smoking", "weapon", "violence", "fire_smoke"] if by_group.get(group)]
    for item in entries:
        if len(overview_items) >= 6:
            break
        if item not in overview_items:
            overview_items.append(item)
    overview_path = boards_dir / "07_category_overview.jpg"
    _render_board(
        title="카테고리별 한눈 보기",
        subtitle="주요 그룹별 대표 프레임 1장씩 모음",
        items=overview_items[:6],
        output_path=overview_path,
        summary_lines=[f"그룹 수: {len(overview_items[:6])} | 원본 시각화 루트: {summary.get('visualization_root')}"],
    )
    board_records.append({"group": "overview", "path": str(overview_path), "items": len(overview_items[:6])})

    manifest = {
        "run_root": str(run_root),
        "report_dir": str(report_dir),
        "boards": board_records
        + [
            {"group": "summary", "path": str(boards_dir / "00_run_summary.jpg"), "items": len(cover_items)},
            {"group": "highlights", "path": str(boards_dir / "01_detection_highlights.jpg"), "items": min(6, len(highlight_entries))},
        ],
        "key_frames": key_frame_records,
        "summary": {
            "total_videos": summary.get("total_videos"),
            "total_frames": summary.get("total_frames"),
            "pair_row_count": summary.get("pair_row_count"),
            "yolo_positive_frames": detection_stats["yolo_positive_frames"],
            "sam_positive_frames": detection_stats["sam_positive_frames"],
            "completed_at": summary.get("completed_at"),
            "elapsed_sec": summary.get("elapsed_sec"),
        },
    }
    _write_json(report_dir / "index.json", manifest)
    return manifest


def generate_time_resource_report(output_dir: Path) -> dict[str, Any]:
    run_root = output_dir.resolve()
    summary = _read_json(run_root / "compare" / "summary.json")
    report_dir = run_root / "report_assets"
    boards_dir = report_dir / "boards"
    report_dir.mkdir(parents=True, exist_ok=True)
    boards_dir.mkdir(parents=True, exist_ok=True)

    resource_usage = summary.get("resource_usage") or {}
    models = resource_usage.get("models") or {}
    yolo_resource = models.get("yolo") or {}
    sam3_resource = models.get("sam3") or {}
    detection_stats = _compute_detection_stats(run_root)

    metrics = {
        "run_id": run_root.name,
        "input_dir": summary.get("input_dir"),
        "completed_at": summary.get("completed_at"),
        "elapsed_sec": summary.get("elapsed_sec"),
        "total_videos": summary.get("total_videos"),
        "total_frames": summary.get("total_frames"),
        "pair_row_count": summary.get("pair_row_count"),
        "pipeline_images_per_min": summary.get("images_per_min"),
        "gpu_memory_peak_gb": summary.get("gpu_memory_peak_gb"),
        "resource_metrics_status": resource_usage.get("resource_metrics_status"),
        "sampling_elapsed_sec": resource_usage.get("sampling_elapsed_sec"),
        "threshold_profile": summary.get("threshold_profile"),
        "yolo": {
            "total_latency_ms": summary.get("yolo_total_latency_ms"),
            "avg_latency_ms": summary.get("yolo_avg_latency_ms"),
            "p95_latency_ms": summary.get("yolo_p95_latency_ms"),
            "fps_est": round(float(summary.get("total_frames") or 0) / max(float(summary.get("yolo_total_latency_ms") or 1) / 1000.0, 1e-9), 2),
            "compute_share_pct": round(
                (float(summary.get("yolo_total_latency_ms") or 0.0))
                / max(float(summary.get("yolo_total_latency_ms") or 0.0) + float(summary.get("sam3_total_latency_ms") or 0.0), 1e-9)
                * 100.0,
                2,
            ),
            "positive_frames": detection_stats["yolo_positive_frames"],
            "positive_videos": detection_stats["yolo_positive_videos"],
            "total_detections": detection_stats["yolo_total_detections"],
            "top_classes": detection_stats["yolo_top_classes"],
            "cpu_percent": yolo_resource.get("cpu_percent") or {},
            "gpu_util_percent": yolo_resource.get("gpu_util_percent") or {},
            "gpu_mem_util_percent": yolo_resource.get("gpu_mem_util_percent") or {},
            "gpu_mem_used_mb": yolo_resource.get("gpu_mem_used_mb") or {},
        },
        "sam3": {
            "total_latency_ms": summary.get("sam3_total_latency_ms"),
            "avg_latency_ms": summary.get("sam3_avg_latency_ms"),
            "p95_latency_ms": summary.get("sam3_p95_latency_ms"),
            "fps_est": round(float(summary.get("total_frames") or 0) / max(float(summary.get("sam3_total_latency_ms") or 1) / 1000.0, 1e-9), 2),
            "compute_share_pct": round(
                (float(summary.get("sam3_total_latency_ms") or 0.0))
                / max(float(summary.get("yolo_total_latency_ms") or 0.0) + float(summary.get("sam3_total_latency_ms") or 0.0), 1e-9)
                * 100.0,
                2,
            ),
            "positive_frames": detection_stats["sam_positive_frames"],
            "positive_videos": detection_stats["sam_positive_videos"],
            "total_detections": detection_stats["sam_total_detections"],
            "top_classes": detection_stats["sam_top_classes"],
            "cpu_percent": sam3_resource.get("cpu_percent") or {},
            "gpu_util_percent": sam3_resource.get("gpu_util_percent") or {},
            "gpu_mem_util_percent": sam3_resource.get("gpu_mem_util_percent") or {},
            "gpu_mem_used_mb": sam3_resource.get("gpu_mem_used_mb") or {},
        },
        "ratios": {
            "sam3_vs_yolo_avg_latency": _safe_ratio(float(summary.get("sam3_avg_latency_ms") or 0.0), float(summary.get("yolo_avg_latency_ms") or 0.0)),
            "sam3_vs_yolo_p95_latency": _safe_ratio(float(summary.get("sam3_p95_latency_ms") or 0.0), float(summary.get("yolo_p95_latency_ms") or 0.0)),
            "sam3_vs_yolo_total_latency": _safe_ratio(float(summary.get("sam3_total_latency_ms") or 0.0), float(summary.get("yolo_total_latency_ms") or 0.0)),
        },
        "artifact_sizes_mb": {
            "yolo_json": _dir_size_mb(run_root / "yolo"),
            "sam3_json": _dir_size_mb(run_root / "sam3"),
            "compare": _dir_size_mb(run_root / "compare"),
            "visual_frames": _dir_size_mb(run_root / "compare" / "visuals" / "frames"),
            "visual_videos": _dir_size_mb(run_root / "compare" / "visuals" / "videos"),
            "report_assets": _dir_size_mb(run_root / "report_assets"),
        },
        "notes": [
            "GPU/CPU 사용률은 모델 서비스 프로세스 PID 기준으로 수집했습니다.",
            "GPU 사용률과 메모리 util은 nvidia-smi pmon 샘플을 기준으로 집계했습니다.",
            "GPU memory used MB는 compute-app query 결과와 pmon fb 값을 우선 사용해 집계했습니다.",
        ],
    }

    metrics_path = report_dir / "time_resource_comparison_metrics.json"
    _write_json(metrics_path, metrics)

    markdown = f"""# tmp_data_2 시간 및 리소스 사용 비교 보고서

- Run ID: `{metrics['run_id']}`
- 입력 경로: `{metrics['input_dir']}`
- 완료 시각: `{metrics['completed_at']}`
- 총 실행 시간: `{metrics['elapsed_sec']}`초
- 리소스 수집 상태: `{metrics['resource_metrics_status']}`

## 실행 요약

| 항목 | 값 |
|---|---:|
| 전체 영상 수 | {metrics['total_videos']} |
| 전체 프레임 수 | {metrics['total_frames']} |
| pair row 수 | {metrics['pair_row_count']} |
| 파이프라인 처리량 | {metrics['pipeline_images_per_min']} images/min |
| 전체 런 최고 GPU 메모리 | {metrics['gpu_memory_peak_gb']} GB |
| 리소스 샘플링 구간 | {metrics['sampling_elapsed_sec']}초 |

## 모델별 시간 및 리소스 비교

| 항목 | YOLO | SAM3.1 |
|---|---:|---:|
| 총 latency | {metrics['yolo']['total_latency_ms']} ms | {metrics['sam3']['total_latency_ms']} ms |
| 평균 latency / frame | {metrics['yolo']['avg_latency_ms']} ms | {metrics['sam3']['avg_latency_ms']} ms |
| p95 latency / frame | {metrics['yolo']['p95_latency_ms']} ms | {metrics['sam3']['p95_latency_ms']} ms |
| 추정 처리량 | {metrics['yolo']['fps_est']} fps | {metrics['sam3']['fps_est']} fps |
| CPU avg / max / p95 | {metrics['yolo']['cpu_percent'].get('avg')} / {metrics['yolo']['cpu_percent'].get('max')} / {metrics['yolo']['cpu_percent'].get('p95')} | {metrics['sam3']['cpu_percent'].get('avg')} / {metrics['sam3']['cpu_percent'].get('max')} / {metrics['sam3']['cpu_percent'].get('p95')} |
| GPU util avg / max / p95 | {metrics['yolo']['gpu_util_percent'].get('avg')} / {metrics['yolo']['gpu_util_percent'].get('max')} / {metrics['yolo']['gpu_util_percent'].get('p95')} | {metrics['sam3']['gpu_util_percent'].get('avg')} / {metrics['sam3']['gpu_util_percent'].get('max')} / {metrics['sam3']['gpu_util_percent'].get('p95')} |
| GPU mem util avg / max / p95 | {metrics['yolo']['gpu_mem_util_percent'].get('avg')} / {metrics['yolo']['gpu_mem_util_percent'].get('max')} / {metrics['yolo']['gpu_mem_util_percent'].get('p95')} | {metrics['sam3']['gpu_mem_util_percent'].get('avg')} / {metrics['sam3']['gpu_mem_util_percent'].get('max')} / {metrics['sam3']['gpu_mem_util_percent'].get('p95')} |
| GPU memory used MB avg / max / p95 | {metrics['yolo']['gpu_mem_used_mb'].get('avg')} / {metrics['yolo']['gpu_mem_used_mb'].get('max')} / {metrics['yolo']['gpu_mem_used_mb'].get('p95')} | {metrics['sam3']['gpu_mem_used_mb'].get('avg')} / {metrics['sam3']['gpu_mem_used_mb'].get('max')} / {metrics['sam3']['gpu_mem_used_mb'].get('p95')} |

## 검출량 비교

| 항목 | YOLO | SAM3.1 |
|---|---:|---:|
| 양성 프레임 수 | {metrics['yolo']['positive_frames']} | {metrics['sam3']['positive_frames']} |
| 양성 영상 수 | {metrics['yolo']['positive_videos']} | {metrics['sam3']['positive_videos']} |
| 총 검출 수 | {metrics['yolo']['total_detections']} | {metrics['sam3']['total_detections']} |

## 결론

- SAM3.1 평균 latency는 YOLO 대비 `{metrics['ratios']['sam3_vs_yolo_avg_latency']}`배입니다.
- 총 compute share 기준 병목은 `SAM3.1`입니다.
- threshold profile: `{(metrics['threshold_profile'] or {}).get('profile_name')}`
"""
    (report_dir / "time_resource_comparison_report.md").write_text(markdown, encoding="utf-8")

    board_path = boards_dir / "08_time_resource_comparison.jpg"
    canvas = Image.new("RGB", (2400, 1600), (10, 14, 20))
    draw = ImageDraw.Draw(canvas)
    title_font = _load_font(56)
    subtitle_font = _load_font(28)
    body_font = _load_font(24)
    metric_font = _load_font(40)

    draw.rounded_rectangle((45, 35, 2355, 1565), radius=30, fill=(17, 24, 34), outline=(45, 60, 82), width=2)
    draw.text((85, 72), "tmp_data_2 시간 및 리소스 사용 비교", font=title_font, fill=(245, 247, 250))
    draw.text((88, 146), "YOLO vs SAM3.1 실행 시간, 처리량, GPU/CPU 사용량, 산출물 크기 요약", font=subtitle_font, fill=(165, 180, 200))

    cards = [
        ("총 실행 시간", f"{metrics['elapsed_sec']}s"),
        ("전체 프레임", str(metrics["total_frames"])),
        ("처리량", f"{metrics['pipeline_images_per_min']}/min"),
        ("GPU peak", f"{metrics['gpu_memory_peak_gb']} GB"),
    ]
    for index, (label, value) in enumerate(cards):
        x = 85 + index * 560
        draw.rounded_rectangle((x, 210, x + 520, 330), radius=18, fill=(24, 34, 48), outline=(61, 82, 116), width=2)
        draw.text((x + 20, 230), label, font=body_font, fill=(165, 180, 200))
        draw.text((x + 20, 270), value, font=metric_font, fill=(245, 247, 250))

    sections = [
        ("모델별 시간 비교", 85, 380, 1100, 620),
        ("리소스 비교", 1225, 380, 1090, 620),
        ("산출물 및 해석 메모", 85, 1040, 2230, 420),
    ]
    for title, x, y, width, height in sections:
        draw.rounded_rectangle((x, y, x + width, y + height), radius=20, fill=(22, 31, 44), outline=(57, 75, 98), width=2)
        draw.text((x + 24, y + 20), title, font=_load_font(30), fill=(245, 247, 250))

    rows = [
        ("총 latency", metrics["yolo"]["total_latency_ms"], metrics["sam3"]["total_latency_ms"]),
        ("평균 latency/frame", metrics["yolo"]["avg_latency_ms"], metrics["sam3"]["avg_latency_ms"]),
        ("p95 latency/frame", metrics["yolo"]["p95_latency_ms"], metrics["sam3"]["p95_latency_ms"]),
        ("추정 처리량", metrics["yolo"]["fps_est"], metrics["sam3"]["fps_est"]),
        ("양성 프레임", metrics["yolo"]["positive_frames"], metrics["sam3"]["positive_frames"]),
        ("총 검출 수", metrics["yolo"]["total_detections"], metrics["sam3"]["total_detections"]),
    ]
    draw.text((560, 450), "YOLO", font=body_font, fill=(125, 211, 252))
    draw.text((860, 450), "SAM3.1", font=body_font, fill=(251, 191, 36))
    y = 492
    for label, yolo_value, sam3_value in rows:
        draw.line((110, y - 14, 1140, y - 14), fill=(44, 58, 77), width=1)
        draw.text((112, y), str(label), font=body_font, fill=(240, 244, 248))
        draw.text((560, y), str(yolo_value), font=body_font, fill=(196, 236, 255))
        draw.text((860, y), str(sam3_value), font=body_font, fill=(255, 236, 179))
        y += 74

    resource_lines = [
        f"YOLO CPU avg/max/p95: {metrics['yolo']['cpu_percent'].get('avg')} / {metrics['yolo']['cpu_percent'].get('max')} / {metrics['yolo']['cpu_percent'].get('p95')}",
        f"SAM3 CPU avg/max/p95: {metrics['sam3']['cpu_percent'].get('avg')} / {metrics['sam3']['cpu_percent'].get('max')} / {metrics['sam3']['cpu_percent'].get('p95')}",
        f"YOLO GPU util avg/max/p95: {metrics['yolo']['gpu_util_percent'].get('avg')} / {metrics['yolo']['gpu_util_percent'].get('max')} / {metrics['yolo']['gpu_util_percent'].get('p95')}",
        f"SAM3 GPU util avg/max/p95: {metrics['sam3']['gpu_util_percent'].get('avg')} / {metrics['sam3']['gpu_util_percent'].get('max')} / {metrics['sam3']['gpu_util_percent'].get('p95')}",
        f"YOLO GPU mem used avg/max/p95: {metrics['yolo']['gpu_mem_used_mb'].get('avg')} / {metrics['yolo']['gpu_mem_used_mb'].get('max')} / {metrics['yolo']['gpu_mem_used_mb'].get('p95')} MB",
        f"SAM3 GPU mem used avg/max/p95: {metrics['sam3']['gpu_mem_used_mb'].get('avg')} / {metrics['sam3']['gpu_mem_used_mb'].get('max')} / {metrics['sam3']['gpu_mem_used_mb'].get('p95')} MB",
        f"시간 병목: SAM3 평균 latency가 YOLO의 {metrics['ratios']['sam3_vs_yolo_avg_latency']}배",
    ]
    y = 430
    for line in resource_lines:
        draw.text((1250, y), line, font=body_font, fill=(240, 244, 248))
        y += 72

    note_lines = [
        f"yolo/: {metrics['artifact_sizes_mb']['yolo_json']} MB | sam3/: {metrics['artifact_sizes_mb']['sam3_json']} MB | compare/: {metrics['artifact_sizes_mb']['compare']} MB",
        f"visual frames: {metrics['artifact_sizes_mb']['visual_frames']} MB | visual videos: {metrics['artifact_sizes_mb']['visual_videos']} MB | report_assets/: {metrics['artifact_sizes_mb']['report_assets']} MB",
        f"YOLO 상위 클래스: {', '.join(f'{name} {count}' for name, count in metrics['yolo']['top_classes']) or '없음'}",
        f"SAM3 상위 클래스: {', '.join(f'{name} {count}' for name, count in metrics['sam3']['top_classes'][:5]) or '없음'}",
        f"리소스 수집 상태: {metrics['resource_metrics_status']} | threshold profile: {(metrics['threshold_profile'] or {}).get('profile_name')}",
    ]
    y = 1095
    for line in note_lines:
        draw.text((112, y), line, font=body_font, fill=(240, 244, 248))
        y += 62
    canvas.save(board_path, quality=92)

    report_index_path = report_dir / "index.json"
    if report_index_path.exists():
        report_index = _read_json(report_index_path)
    else:
        report_index = {"run_root": str(run_root), "report_dir": str(report_dir), "boards": [], "key_frames": []}
    report_index["time_resource_report"] = {
        "markdown": str(report_dir / "time_resource_comparison_report.md"),
        "metrics_json": str(metrics_path),
        "board_jpg": str(board_path),
    }
    _write_json(report_index_path, report_index)
    return metrics


def generate_sweep_report(sweep_root: Path, run_dirs: list[Path]) -> dict[str, Any]:
    sweep_root = sweep_root.resolve()
    report_dir = sweep_root / "report_assets"
    boards_dir = report_dir / "boards"
    report_dir.mkdir(parents=True, exist_ok=True)
    boards_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    for run_dir in run_dirs:
        row: dict[str, Any] = {"run_name": run_dir.name, "status": "missing"}
        summary_path = run_dir / "compare" / "summary.json"
        metrics_path = run_dir / "report_assets" / "time_resource_comparison_metrics.json"
        if summary_path.exists():
            summary = _read_json(summary_path)
            row.update(
                {
                    "status": "ok",
                    "elapsed_sec": summary.get("elapsed_sec"),
                    "pair_row_count": summary.get("pair_row_count"),
                    "yolo_avg_latency_ms": summary.get("yolo_avg_latency_ms"),
                    "sam3_avg_latency_ms": summary.get("sam3_avg_latency_ms"),
                    "threshold_profile": (summary.get("threshold_profile") or {}).get("profile_name"),
                    "resource_metrics_status": summary.get("resource_metrics_status"),
                }
            )
        if metrics_path.exists():
            metrics = _read_json(metrics_path)
            row.update(
                {
                    "yolo_cpu_avg": ((metrics.get("yolo") or {}).get("cpu_percent") or {}).get("avg"),
                    "sam3_cpu_avg": ((metrics.get("sam3") or {}).get("cpu_percent") or {}).get("avg"),
                    "yolo_gpu_avg": ((metrics.get("yolo") or {}).get("gpu_util_percent") or {}).get("avg"),
                    "sam3_gpu_avg": ((metrics.get("sam3") or {}).get("gpu_util_percent") or {}).get("avg"),
                    "yolo_gpu_mem_max_mb": ((metrics.get("yolo") or {}).get("gpu_mem_used_mb") or {}).get("max"),
                    "sam3_gpu_mem_max_mb": ((metrics.get("sam3") or {}).get("gpu_mem_used_mb") or {}).get("max"),
                    "yolo_positive_frames": (metrics.get("yolo") or {}).get("positive_frames"),
                    "sam_positive_frames": (metrics.get("sam3") or {}).get("positive_frames"),
                    "yolo_total_detections": (metrics.get("yolo") or {}).get("total_detections"),
                    "sam_total_detections": (metrics.get("sam3") or {}).get("total_detections"),
                }
            )
        rows.append(row)

    _write_json(sweep_root / "sweep_manifest.json", {"generated_at": datetime.now().isoformat(), "runs": rows})
    with (sweep_root / "sweep_summary.csv").open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "run_name",
            "status",
            "threshold_profile",
            "resource_metrics_status",
            "elapsed_sec",
            "pair_row_count",
            "yolo_avg_latency_ms",
            "sam3_avg_latency_ms",
            "yolo_cpu_avg",
            "sam3_cpu_avg",
            "yolo_gpu_avg",
            "sam3_gpu_avg",
            "yolo_gpu_mem_max_mb",
            "sam3_gpu_mem_max_mb",
            "yolo_positive_frames",
            "sam_positive_frames",
            "yolo_total_detections",
            "sam_total_detections",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    report_lines = [
        "# tmp_data_2 Threshold Sweep 보고서",
        "",
        f"- 생성 시각: `{datetime.now().isoformat()}`",
        f"- sweep root: `{sweep_root}`",
        "",
        "| Run | Status | Threshold | Elapsed(s) | YOLO avg ms | SAM3 avg ms | YOLO CPU avg | SAM3 CPU avg | YOLO GPU avg | SAM3 GPU avg |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        report_lines.append(
            f"| {row.get('run_name')} | {row.get('status')} | {row.get('threshold_profile')} | {row.get('elapsed_sec')} | {row.get('yolo_avg_latency_ms')} | {row.get('sam3_avg_latency_ms')} | {row.get('yolo_cpu_avg')} | {row.get('sam3_cpu_avg')} | {row.get('yolo_gpu_avg')} | {row.get('sam3_gpu_avg')} |"
        )
    (sweep_root / "sweep_report.md").write_text("\n".join(report_lines), encoding="utf-8")

    canvas = Image.new("RGB", (2400, 1600), (10, 14, 20))
    draw = ImageDraw.Draw(canvas)
    title_font = _load_font(54)
    subtitle_font = _load_font(28)
    body_font = _load_font(24)
    draw.rounded_rectangle((45, 35, 2355, 1565), radius=30, fill=(17, 24, 34), outline=(45, 60, 82), width=2)
    draw.text((85, 72), "tmp_data_2 5회 Threshold Sweep 비교", font=title_font, fill=(245, 247, 250))
    draw.text((88, 146), "각 run의 시간, 리소스, 검출량 비교 요약", font=subtitle_font, fill=(165, 180, 200))

    columns = [
        ("Run", 85),
        ("Threshold", 360),
        ("Elapsed(s)", 760),
        ("YOLO avg", 980),
        ("SAM3 avg", 1180),
        ("YOLO CPU", 1400),
        ("SAM3 CPU", 1600),
        ("YOLO GPU", 1820),
        ("SAM3 GPU", 2020),
        ("SAM det", 2200),
    ]
    header_y = 250
    draw.rounded_rectangle((75, header_y - 12, 2325, header_y + 42), radius=10, fill=(24, 34, 48))
    for label, x in columns:
        draw.text((x, header_y), label, font=body_font, fill=(224, 232, 240))
    y = header_y + 80
    for row in rows:
        draw.line((75, y - 18, 2325, y - 18), fill=(44, 58, 77), width=1)
        values = [
            str(row.get("run_name")),
            str(row.get("threshold_profile")),
            str(row.get("elapsed_sec")),
            str(row.get("yolo_avg_latency_ms")),
            str(row.get("sam3_avg_latency_ms")),
            str(row.get("yolo_cpu_avg")),
            str(row.get("sam3_cpu_avg")),
            str(row.get("yolo_gpu_avg")),
            str(row.get("sam3_gpu_avg")),
            str(row.get("sam_total_detections")),
        ]
        for (label, x), value in zip(columns, values):
            draw.text((x, y), value, font=body_font, fill=(240, 244, 248))
        y += 88

    board_path = boards_dir / "09_threshold_sweep_overview.jpg"
    canvas.save(board_path, quality=92)
    return {
        "generated_at": datetime.now().isoformat(),
        "run_count": len(run_dirs),
        "board_jpg": str(board_path),
        "sweep_report_md": str(sweep_root / "sweep_report.md"),
        "sweep_summary_csv": str(sweep_root / "sweep_summary.csv"),
        "sweep_manifest_json": str(sweep_root / "sweep_manifest.json"),
    }
