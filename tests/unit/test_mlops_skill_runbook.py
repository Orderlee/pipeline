"""mlops-finetune SKILL.md operator-runbook presence guard (Section G, spec §14)."""
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_SKILL = _REPO / ".agent" / "skill" / "mlops-finetune" / "SKILL.md"


def test_skill_file_exists():
    assert _SKILL.is_file(), f"missing {_SKILL}"


def test_skill_has_required_sections():
    txt = _SKILL.read_text(encoding="utf-8")
    required = [
        "## 9. 정비락 복구",          # maintenance-lock recovery (spec §9)
        "scripts/clear_maintenance.sh",
        "/maintenance/exit",
        "/warmup",
        "## run 이 progressing 인지 hung 인지 판별",
        "train_log.jsonl",
        "## staging vs prod 검증 분리",
        "ENABLE_TRAINING=false",
    ]
    for token in required:
        assert token in txt, f"SKILL.md missing: {token!r}"
