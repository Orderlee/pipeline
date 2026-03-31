#!/usr/bin/env python3
"""tmp_data_2 영상으로 이미지 추출 + (선택) YOLO 벤치 — 용량·시간·검출 수 측정.

이미지 추출 및 YOLO-World만 범위. 프리셋별 추출 프레임 수·총 용량·소요 시간을 측정하고,
YOLO 서버가 있으면 샘플 이미지로 검출 수·소요 시간을 추가 측정합니다.

사용:
  PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py
  PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py --video-dir /home/pia/mou/staging/tmp_data_2 --max-videos 5
  PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py --no-yolo
  PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py --preset-names interval_0.5s_q90 interval_1s_q85 --workers 1 2 4
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# repo root = parent of scripts/
REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

from vlm_pipeline.lib.video_frames import (
    extract_frame_jpeg_bytes,
    plan_frame_timestamps,
)

DEFAULT_VIDEO_DIR = Path("/home/pia/mou/staging/tmp_data_2")

# image_profile 숫자: 1=보수적(current), 2=촘촘(dense) — video_frames API는 "current"|"dense" 사용
PROFILE_NUM_TO_STR = {1: "current", 2: "dense"}

# 실험용 프리셋: 1초당 1프레임(interval) 기반 — 많이/적게 추출 비교
# frame_interval_sec: N초마다 1프레임 → 작을수록 많이 추출, 클수록 적게 추출
PRESETS = [
    # 적게 추출 (2초당 1프레임)
    {"name": "interval_2s_q90", "frame_interval_sec": 2.0, "jpeg_quality": 90},
    # 기본 (1초당 1프레임)
    {"name": "interval_1s_q90", "frame_interval_sec": 1.0, "jpeg_quality": 90},
    # 많이 추출 (0.5초당 1프레임 = 2fps)
    {"name": "interval_0.5s_q90", "frame_interval_sec": 0.5, "jpeg_quality": 90},
    # 기본 + 용량 절감
    {"name": "interval_1s_q85", "frame_interval_sec": 1.0, "jpeg_quality": 85},
]


def get_video_meta(video_path: Path) -> tuple[float, float]:
    """ffprobe로 duration_sec, fps 반환. 실패 시 (0.0, 0.0)."""
    try:
        out = subprocess.run(
            [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "format=duration:stream=r_frame_rate",
                "-of", "default=noprint_wrappers=1",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if out.returncode != 0:
            return 0.0, 0.0
        duration = 0.0
        fps = 0.0
        for line in out.stdout.strip().splitlines():
            if line.startswith("duration="):
                duration = float(line.split("=", 1)[1].strip())
            elif line.startswith("r_frame_rate="):
                r = line.split("=", 1)[1].strip()
                if "/" in r:
                    a, b = r.split("/", 1)
                    fps = float(a) / float(b) if float(b) else 0.0
                else:
                    fps = float(r)
        return duration, fps
    except Exception:
        return 0.0, 0.0


def collect_videos(video_dir: Path, max_videos: int) -> list[Path]:
    """영상 파일 목록 (mp4 등)."""
    videos: list[Path] = []
    for ext in ("*.mp4", "*.MP4", "*.mov", "*.MOV"):
        videos.extend(video_dir.rglob(ext))
    videos = sorted(set(videos))[: max(1, max_videos)]
    return videos


def _extract_one_video(
    vpath: Path,
    out_dir: Path,
    max_frames: int,
    jpeg_quality: int,
    profile: str,
    frame_interval_sec: float | None = None,
) -> tuple[int, int, int]:
    """한 영상에 대해 프레임 추출 후 out_dir에 저장. 반환: (성공 프레임 수, 총 바이트, 실패 수)."""
    duration_sec, fps = get_video_meta(vpath)
    if duration_sec <= 0:
        return 0, 0, 1
    timestamps = plan_frame_timestamps(
        duration_sec=duration_sec,
        fps=fps,
        frame_count=None,
        max_frames_per_video=max_frames,
        image_profile=profile,
        frame_interval_sec=frame_interval_sec,
    )
    stem = vpath.stem
    total_bytes = 0
    failed = 0
    for i, sec in enumerate(timestamps, start=1):
        try:
            raw = extract_frame_jpeg_bytes(vpath, sec, jpeg_quality=jpeg_quality)
            fpath = out_dir / f"{stem}_{i:04d}.jpg"
            fpath.write_bytes(raw)
            total_bytes += len(raw)
        except Exception:
            failed += 1
    return len(timestamps) - failed, total_bytes, failed


def _resolve_profile(preset: dict) -> str:
    """preset에서 image_profile_num(숫자) 또는 image_profile(문자) → "current"|"dense"."""
    if "image_profile_num" in preset:
        return PROFILE_NUM_TO_STR.get(int(preset["image_profile_num"]), "current")
    return preset.get("image_profile", "current")


def run_preset(
    preset: dict,
    videos: list[Path],
    out_root: Path,
    run_yolo: bool,
    yolo_url: str,
    workers: int = 1,
) -> dict:
    """한 프리셋으로 전 영상 추출(worker 병렬) → 용량·시간 측정. 선택 시 YOLO 호출."""
    name = preset["name"]
    jpeg_quality = int(preset.get("jpeg_quality", 90))
    frame_interval_sec = preset.get("frame_interval_sec")
    if frame_interval_sec is not None:
        frame_interval_sec = float(frame_interval_sec)
    max_frames = int(preset.get("max_frames", 12))
    profile = _resolve_profile(preset)

    preset_slug = name.replace("(", "_").replace(")", "").replace(",", "_")
    out_dir = out_root / f"{preset_slug}_w{workers}"
    out_dir.mkdir(parents=True, exist_ok=True)

    extract_start = time.perf_counter()
    total_frames = 0
    total_bytes = 0
    failed = 0

    if workers <= 1:
        for vpath in videos:
            f, b, err = _extract_one_video(
                vpath, out_dir, max_frames, jpeg_quality, profile, frame_interval_sec
            )
            total_frames += f
            total_bytes += b
            failed += err
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _extract_one_video,
                    vpath,
                    out_dir,
                    max_frames,
                    jpeg_quality,
                    profile,
                    frame_interval_sec,
                ): vpath
                for vpath in videos
            }
            for fut in as_completed(futures):
                f, b, err = fut.result()
                total_frames += f
                total_bytes += b
                failed += err

    extract_elapsed = time.perf_counter() - extract_start

    result = {
        "preset": name,
        "frame_interval_sec": frame_interval_sec,
        "max_frames_per_video": preset.get("max_frames"),
        "jpeg_quality": jpeg_quality,
        "image_profile_num": preset.get("image_profile_num"),
        "image_profile": profile,
        "workers": workers,
        "video_count": len(videos),
        "total_frames": total_frames,
        "total_extracted_images": total_frames,
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / (1024 * 1024), 2),
        "extract_time_sec": round(extract_elapsed, 1),
        "failed_frames": failed,
    }

    # YOLO는 worker=1일 때만 실행 (동일 프리셋이면 프레임 수·용량 동일, 비교는 추출 시간 위주)
    if run_yolo and total_frames > 0 and workers == 1:
        # 추출된 전체 이미지에 대해 YOLO-World 배치 추론 (서버 MAX_BATCH=8 기준)
        all_paths = sorted(out_dir.glob("*.jpg"))
        batch_size = int(os.environ.get("YOLO_BENCH_BATCH", "8"))
        yolo_start = time.perf_counter()
        total_detections = 0
        yolo_err: str | None = None
        batches = 0
        for i in range(0, len(all_paths), batch_size):
            chunk = all_paths[i : i + batch_size]
            d, err = call_yolo_batch(yolo_url, chunk)
            if err:
                yolo_err = err
                break
            total_detections += d
            batches += 1
        yolo_elapsed = time.perf_counter() - yolo_start
        result["yolo_label_images"] = len(all_paths)
        result["yolo_label_batches"] = batches
        result["yolo_label_time_sec"] = round(yolo_elapsed, 1)
        result["yolo_total_detections"] = total_detections
        if yolo_err:
            result["yolo_error"] = yolo_err

    return result


def call_yolo_batch(base_url: str, image_paths: list[Path]) -> tuple[int, str | None]:
    """YOLO /detect/batch 호출 후 총 검출 수 반환. 오류 시 (0, error_message)."""
    try:
        import requests  # noqa: PLC0415
    except ImportError:
        return 0, "requests not installed"

    url = f"{base_url.rstrip('/')}/detect/batch"
    files = [("files", (p.name, open(p, "rb"), "image/jpeg")) for p in image_paths]
    try:
        r = requests.post(url, files=files, params={"conf": 0.25, "iou": 0.45}, timeout=120)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        total = 0
        for item in results:
            dets = item.get("detections", [])
            total += len(dets)
        return total, None
    except Exception as e:
        return 0, str(e)
    finally:
        for _, (_, fh, _) in files:
            fh.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="영상 이미지 추출 + YOLO 벤치 (용량·시간)")
    ap.add_argument("--video-dir", type=Path, default=DEFAULT_VIDEO_DIR, help="영상 디렉터리")
    ap.add_argument("--max-videos", type=int, default=0, help="최대 영상 수 (0=전체)")
    ap.add_argument("--no-yolo", action="store_true", help="YOLO 호출 생략")
    ap.add_argument("--yolo-url", default=os.environ.get("YOLO_API_URL", "http://localhost:8001"), help="YOLO API URL")
    ap.add_argument("--out-dir", type=Path, default=None, help="추출 이미지 저장 (기본 임시)")
    ap.add_argument("--workers", type=int, nargs="+", default=[1], help="병렬 워커 수 (여러 개 시 비교용, 예: 1 2 4)")
    ap.add_argument(
        "--preset-names",
        nargs="*",
        default=None,
        metavar="NAME",
        help="지정 시 해당 이름의 프리셋만 실행 (예: interval_0.5s_q90 interval_1s_q85). 미지정 시 전체 PRESETS.",
    )
    args = ap.parse_args()

    video_dir = args.video_dir
    if not video_dir.is_dir():
        print(f"영상 디렉터리 없음: {video_dir}", file=sys.stderr)
        sys.exit(1)

    max_videos = args.max_videos or 999
    videos = collect_videos(video_dir, max_videos)
    if not videos:
        print("영상 파일 없음", file=sys.stderr)
        sys.exit(1)

    run_yolo = not args.no_yolo
    worker_list = [max(1, int(w)) for w in args.workers]
    out_root = args.out_dir or Path(tempfile.mkdtemp(prefix="staging_extract_"))
    print(f"영상 수: {len(videos)}, workers: {worker_list}, 추출 출력: {out_root}", file=sys.stderr)

    presets_to_run = PRESETS
    if args.preset_names:
        wanted = set(args.preset_names)
        presets_to_run = [p for p in PRESETS if p["name"] in wanted]
        missing = wanted - {p["name"] for p in presets_to_run}
        if missing:
            print(f"알 수 없는 preset 이름 (무시): {sorted(missing)}", file=sys.stderr)
        if not presets_to_run:
            print("실행할 프리셋이 없습니다. --preset-names 와 PRESETS 이름을 확인하세요.", file=sys.stderr)
            sys.exit(1)

    all_results: list[dict] = []
    for preset in presets_to_run:
        for w in worker_list:
            r = run_preset(preset, videos, out_root, run_yolo, args.yolo_url, workers=w)
            all_results.append(r)
            print(
                f"  {r['preset']} workers={r['workers']}: 추출 이미지 총 개수={r['total_frames']}장 size={r['total_mb']} MB time={r['extract_time_sec']}s"
                + (
                    f" yolo_det={r.get('yolo_total_detections', '')} yolo_label={r.get('yolo_label_time_sec', '')}s"
                    if run_yolo
                    else ""
                ),
                file=sys.stderr,
            )

    print(json.dumps(all_results, ensure_ascii=False, indent=2))
    if not args.out_dir:
        shutil.rmtree(out_root, ignore_errors=True)


if __name__ == "__main__":
    main()
