"""CI rebuild-glob + YAML parse guards for the MLOps containers (Section G).

These assert that docker/trainer/, docker/mlflow/, docker/curation/ changes trigger an
image rebuild on BOTH the prod and staging deploy workflows, and that the workflow YAML
still parses. (scripts/dvc/ is already covered by the existing ^scripts/ clause.)
No GPU / no runtime — pure file + YAML checks (CI-safe).
"""
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[2]
_PROD = _REPO / ".github" / "workflows" / "deploy-production.yml"
_TEST = _REPO / ".github" / "workflows" / "deploy-test.yml"

_GLOBS = (
    '[[ "${path}" =~ ^docker/trainer/ ]]',
    '[[ "${path}" =~ ^docker/mlflow/ ]]',
    '[[ "${path}" =~ ^docker/curation/ ]]',
)


def test_workflows_exist():
    assert _PROD.is_file(), f"missing {_PROD}"
    assert _TEST.is_file(), f"missing {_TEST}"


def test_prod_workflow_has_mlops_globs():
    text = _PROD.read_text(encoding="utf-8")
    for glob in _GLOBS:
        assert glob in text, f"prod workflow missing rebuild glob: {glob}"


def test_test_workflow_has_mlops_globs():
    text = _TEST.read_text(encoding="utf-8")
    for glob in _GLOBS:
        assert glob in text, f"staging workflow missing rebuild glob: {glob}"


def test_prod_workflow_parses_as_yaml():
    # GitHub maps `on:` to the YAML boolean True key; just assert it loads + has jobs.
    doc = yaml.safe_load(_PROD.read_text(encoding="utf-8"))
    assert "detect_image_rebuild" in doc["jobs"]
    assert "deploy" in doc["jobs"]


def test_test_workflow_parses_as_yaml():
    doc = yaml.safe_load(_TEST.read_text(encoding="utf-8"))
    assert "detect_image_rebuild" in doc["jobs"]
    assert "deploy" in doc["jobs"]
