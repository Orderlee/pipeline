from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
for candidate in (REPO, REPO / "scripts", REPO / "src"):
    rendered = str(candidate)
    if rendered not in sys.path:
        sys.path.insert(0, rendered)

import manual_yolo_sam3_compare as compare_script
import manual_yolo_sam3_threshold_sweep as sweep_script


def _build_args(config_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        category_thresholds_json=config_path,
        confidence_threshold=0.25,
    )


def test_build_profiles_match_expected_threshold_sweep() -> None:
    profiles = sweep_script.build_profiles()

    assert [run_name for run_name, _ in profiles] == [
        "01_baseline_model_defaults",
        "02_explore_low",
        "03_explore_balanced",
        "04_explore_strict",
        "05_experiment_final",
    ]

    profile_map = {run_name: payload for run_name, payload in profiles}

    assert profile_map["01_baseline_model_defaults"]["mode"] == "category_override"
    assert profile_map["01_baseline_model_defaults"]["category_thresholds"] == {
        "smoke": 0.20,
        "smoking": 0.20,
        "fire": 0.20,
        "falldown": 0.20,
        "weapon": 0.15,
        "violence": 0.20,
    }
    assert profile_map["05_experiment_final"]["profile_name"] == "05_experiment_final"
    assert profile_map["05_experiment_final"]["category_thresholds"] == {
        "smoke": 0.25,
        "smoking": 0.25,
        "fire": 0.25,
        "falldown": 0.79,
        "weapon": 0.15,
        "violence": 0.25,
    }


def test_load_threshold_profile_maps_falldown_to_person_fallen_for_baseline_and_final(
    tmp_path: Path,
) -> None:
    profiles = {run_name: payload for run_name, payload in sweep_script.build_profiles()}
    requested_categories = ["smoke", "smoking", "fire", "falldown", "weapon", "violence"]
    requested_classes = compare_script.derive_classes_from_categories(requested_categories)

    baseline_config_path = tmp_path / "baseline.json"
    compare_script._write_json(baseline_config_path, profiles["01_baseline_model_defaults"])
    baseline_profile = compare_script.load_threshold_profile(
        args=_build_args(baseline_config_path),
        benchmark_id="bench-baseline",
        output_dir=tmp_path / "baseline",
        requested_categories=requested_categories,
        requested_classes=requested_classes,
    )

    final_config_path = tmp_path / "final.json"
    compare_script._write_json(final_config_path, profiles["05_experiment_final"])
    final_profile = compare_script.load_threshold_profile(
        args=_build_args(final_config_path),
        benchmark_id="bench-final",
        output_dir=tmp_path / "final",
        requested_categories=requested_categories,
        requested_classes=requested_classes,
    )

    assert baseline_profile.mode == "category_override"
    assert baseline_profile.yolo_class_thresholds["person_fallen"] == 0.20
    assert baseline_profile.sam3_prompt_thresholds["person_fallen"] == 0.20
    assert baseline_profile.yolo_effective_request_confidence_threshold == 0.15

    assert final_profile.profile_name == "05_experiment_final"
    assert final_profile.yolo_class_thresholds["person_fallen"] == 0.79
    assert final_profile.sam3_prompt_thresholds["person_fallen"] == 0.79
    assert final_profile.yolo_class_thresholds["fire"] == 0.25
    assert final_profile.yolo_class_thresholds["bat"] == 0.15
    assert final_profile.yolo_effective_request_confidence_threshold == 0.15
