"""CLAUDE.md MLOps operator-doc presence guard (Section G, spec §14).

Asserts the safety-critical MLOps runbook section exists and names the exact
shared-contract symbols operators need (promote/rollback/maintenance recovery).
"""
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_CLAUDE = _REPO / "CLAUDE.md"


def test_mlops_section_present():
    txt = _CLAUDE.read_text(encoding="utf-8")
    assert "## MLOps — 파인튜닝 트랙" in txt


def test_mlops_section_names_required_symbols():
    txt = _CLAUDE.read_text(encoding="utf-8")
    required = [
        "scripts/promote_model.py",
        "scripts/clear_maintenance.sh",
        "model_registry",
        "train_dataset_versions",
        "ENABLE_TRAINING",
        "/maintenance/enter",
        "/maintenance/exit",
        "gpu_trainer",
        "status='promotable'",
    ]
    for token in required:
        assert token in txt, f"CLAUDE.md MLOps section missing: {token!r}"
