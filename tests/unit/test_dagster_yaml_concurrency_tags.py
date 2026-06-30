"""dagster.yaml run_coordinator enforces gpu_trainer=1 and pg_writer=1."""
from __future__ import annotations
import pathlib
import yaml

YAML_PATH = pathlib.Path("docker/app/dagster_home/dagster.yaml")


def test_gpu_trainer_and_pg_writer_tag_limits_present():
    cfg = yaml.safe_load(YAML_PATH.read_text())
    limits = (
        cfg.get("run_coordinator", {}).get("config", {}).get("tag_concurrency_limits", [])
    )
    by_key = {}
    for entry in limits:
        # entries look like {"key": "gpu_trainer", "limit": 1} or with a value matcher
        by_key[entry.get("key")] = entry.get("limit")
    assert by_key.get("gpu_trainer") == 1, f"gpu_trainer limit missing/!= 1: {limits}"
    assert by_key.get("pg_writer") == 1, f"pg_writer limit missing/!= 1: {limits}"
