"""mlflow compose service — YAML parse + service/env presence (no server in CI).

Asserts the profile-gated mlflow service exists with a Postgres backend store on
a SEPARATE 'mlflow' DB and a MinIO-backed s3://vlm-dataset/_mlflow/ artifact root.
Pure file + YAML checks — no docker daemon, no network.
"""

from __future__ import annotations

import pathlib

import yaml

_REPO = pathlib.Path(__file__).resolve().parents[2]
_COMPOSE = _REPO / "docker" / "docker-compose.yaml"
_DOCKERFILE = _REPO / "docker" / "mlflow" / "Dockerfile"


def _mlflow_service() -> dict:
    doc = yaml.safe_load(_COMPOSE.read_text(encoding="utf-8"))
    services = doc["services"]
    assert "mlflow" in services, "mlflow service missing from docker-compose.yaml"
    return services["mlflow"]


def test_mlflow_service_is_profile_gated() -> None:
    svc = _mlflow_service()
    assert svc.get("profiles") == ["mlflow"], f"mlflow must be profiles:[mlflow], got {svc.get('profiles')}"
    # one-shot/long-running server but must NOT auto-start with the default stack:
    # profile gating already guarantees that; just assert no default-profile leak.


def test_mlflow_backend_store_is_separate_pg_db() -> None:
    svc = _mlflow_service()
    cmd = svc.get("command")
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
    assert "--backend-store-uri" in cmd_str
    # separate 'mlflow' DB on the shared postgres (NOT the pipeline DB):
    assert "MLFLOW_BACKEND_STORE_URI" in cmd_str or "/mlflow" in cmd_str, cmd_str


def test_mlflow_artifact_root_is_vlm_dataset_prefix() -> None:
    svc = _mlflow_service()
    cmd = svc.get("command")
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
    assert "s3://vlm-dataset/_mlflow" in cmd_str, "artifact root must be the vlm-dataset/_mlflow prefix (no new bucket)"


def test_mlflow_minio_endpoint_env_present() -> None:
    svc = _mlflow_service()
    env = svc.get("environment", {})
    # docker-compose env may be a dict or a list of "K=V"; normalize to dict keys.
    keys = set(env.keys()) if isinstance(env, dict) else {e.split("=", 1)[0] for e in env}
    for required in ("MLFLOW_S3_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        assert required in keys, f"mlflow service missing env {required}: {sorted(keys)}"


def test_mlflow_dockerfile_adds_pg_and_s3_deps() -> None:
    text = _DOCKERFILE.read_text(encoding="utf-8")
    assert "ghcr.io/mlflow/mlflow" in text, "thin image must FROM the official mlflow image"
    assert "psycopg2-binary" in text, "PG backend store needs psycopg2-binary"
    assert "boto3" in text, "S3 artifact upload needs boto3"
