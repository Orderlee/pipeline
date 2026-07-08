"""CLAUDE.md scripts-table MLOps entries guard (Section G, spec §14)."""

from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_CLAUDE = _REPO / "CLAUDE.md"


def test_scripts_table_lists_mlops_scripts():
    lines = _CLAUDE.read_text(encoding="utf-8").splitlines()
    # locate the "자주 쓰는 스크립트" table header
    start = next(i for i, ln in enumerate(lines) if "자주 쓰는 스크립트" in ln)
    # the table rows live within ~30 lines of the header
    block = "\n".join(lines[start : start + 30])
    assert "`scripts/promote_model.py`" in block
    assert "`scripts/clear_maintenance.sh`" in block
