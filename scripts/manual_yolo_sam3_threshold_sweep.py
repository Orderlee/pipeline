#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
if str(REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO / "scripts"))

from manual_yolo_sam3_reporting import generate_sweep_report

DEFAULT_INPUT_DIR = Path("/home/pia/mou/incoming/tmp_data_2")
DEFAULT_OUTPUT_ROOT = Path("/tmp/vlm_manual_compare")


def parse_args() -> argparse.Namespace:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(description="tmp_data_2 5회 threshold sweep wrapper")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT / f"tmp_data_2_threshold_sweep_{timestamp}",
    )
    parser.add_argument("--categories", default="smoke,smoking,fire,falldown,weapon,violence")
    parser.add_argument("--image-profile", default="current", choices=["current", "dense"])
    parser.add_argument("--confidence-threshold", type=float, default=0.25)
    parser.add_argument("--iou-threshold", type=float, default=0.45)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-videos", type=int, default=0)
    parser.add_argument("--max-masks-per-prompt", type=int, default=50)
    parser.add_argument("--jpeg-quality", type=int, default=90)
    parser.add_argument("--yolo-url", default="http://127.0.0.1:8001")
    parser.add_argument("--sam3-url", default="http://127.0.0.1:8002")
    parser.add_argument("--resource-sample-interval-sec", type=float, default=1.0)
    parser.add_argument("--resource-yolo-container", default="pipeline-yolo-1")
    parser.add_argument("--resource-sam3-container", default="pipeline-sam3-1")
    parser.add_argument("--visual-video-fps", type=float, default=2.0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def build_profiles() -> list[tuple[str, dict]]:
    return [
        (
            "01_baseline_model_defaults",
            {
                "profile_name": "01_baseline_model_defaults",
                "mode": "baseline_model_defaults",
                "category_thresholds": {},
                "notes": [
                    "YOLO uses explicit per-class defaults from current code.",
                    "SAM3 uses service default score threshold 0.25 without per-prompt overrides.",
                ],
            },
        ),
        (
            "02_explore_low",
            {
                "profile_name": "02_explore_low",
                "mode": "category_override",
                "category_thresholds": {
                    "smoke": 0.20,
                    "smoking": 0.20,
                    "fire": 0.20,
                    "falldown": 0.25,
                    "weapon": 0.15,
                    "violence": 0.20,
                },
                "notes": ["Low threshold exploratory run."],
            },
        ),
        (
            "03_explore_balanced",
            {
                "profile_name": "03_explore_balanced",
                "mode": "category_override",
                "category_thresholds": {
                    "smoke": 0.25,
                    "smoking": 0.25,
                    "fire": 0.25,
                    "falldown": 0.40,
                    "weapon": 0.20,
                    "violence": 0.25,
                },
                "notes": ["Balanced threshold exploratory run."],
            },
        ),
        (
            "04_explore_strict",
            {
                "profile_name": "04_explore_strict",
                "mode": "category_override",
                "category_thresholds": {
                    "smoke": 0.30,
                    "smoking": 0.30,
                    "fire": 0.30,
                    "falldown": 0.55,
                    "weapon": 0.25,
                    "violence": 0.30,
                },
                "notes": ["Strict threshold exploratory run."],
            },
        ),
        (
            "05_final_person_fallen_079",
            {
                "profile_name": "05_final_person_fallen_079",
                "mode": "category_override",
                "category_thresholds": {
                    "smoke": 0.25,
                    "smoking": 0.25,
                    "fire": 0.25,
                    "falldown": 0.79,
                    "weapon": 0.15,
                    "violence": 0.25,
                },
                "notes": ["Final run with person_fallen threshold fixed to 0.79."],
            },
        ),
    ]


def main() -> int:
    args = parse_args()
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    compare_script = REPO / "scripts" / "manual_yolo_sam3_compare.py"
    run_dirs: list[Path] = []

    for run_name, profile in build_profiles():
        run_dir = output_root / run_name
        run_dir.mkdir(parents=True, exist_ok=True)
        config_path = run_dir / "threshold_config.json"
        config_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
        run_dirs.append(run_dir)

        cmd = [
            sys.executable,
            str(compare_script),
            "--input-dir",
            str(args.input_dir),
            "--output-dir",
            str(run_dir),
            "--categories",
            args.categories,
            "--image-profile",
            args.image_profile,
            "--confidence-threshold",
            str(args.confidence_threshold),
            "--iou-threshold",
            str(args.iou_threshold),
            "--batch-size",
            str(args.batch_size),
            "--max-videos",
            str(args.max_videos),
            "--max-masks-per-prompt",
            str(args.max_masks_per_prompt),
            "--jpeg-quality",
            str(args.jpeg_quality),
            "--yolo-url",
            args.yolo_url,
            "--sam3-url",
            args.sam3_url,
            "--category-thresholds-json",
            str(config_path),
            "--resource-sample-interval-sec",
            str(args.resource_sample_interval_sec),
            "--resource-yolo-container",
            args.resource_yolo_container,
            "--resource-sam3-container",
            args.resource_sam3_container,
            "--visual-video-fps",
            str(args.visual_video_fps),
            "--log-level",
            args.log_level,
        ]
        if args.resume:
            cmd.append("--resume")

        print(f"[threshold-sweep] start {run_name}: {' '.join(cmd)}", flush=True)
        completed = subprocess.run(cmd, check=False)
        print(f"[threshold-sweep] done {run_name}: exit_code={completed.returncode}", flush=True)

    generate_sweep_report(output_root, run_dirs)
    print(f"[threshold-sweep] sweep output: {output_root}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
