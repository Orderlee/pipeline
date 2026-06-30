"""deploy-stack.sh trainer gating guards (Section G).

The vlm-trainer container is profile-gated AND must never be auto-started by the
deploy script (it is a manual, Dagster-decoupled GPU process, spec §7.3). So:
  - a trainer_active() helper exists (mirrors sam3_active/embedding_active),
  - trainer is added to build_targets only when active,
  - the script NEVER runs `compose up`/recreate on the trainer service.
Pure string checks on the shell source (CI-safe, no docker).
"""
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_DEPLOY = _REPO / "scripts" / "deploy" / "deploy-stack.sh"


def _src() -> str:
    return _DEPLOY.read_text(encoding="utf-8")


def test_deploy_stack_exists():
    assert _DEPLOY.is_file()


def test_trainer_active_helper_defined():
    src = _src()
    assert "trainer_active()" in src
    # mirrors the sam3/embedding gating idiom
    assert "grep -qx trainer" in src


def test_trainer_added_to_build_targets_when_active():
    src = _src()
    assert "build_targets+=(trainer)" in src


def test_trainer_is_never_started_by_deploy():
    # The trainer is launched manually by the operator, never by the deploy script.
    src = _src()
    for forbidden in (
        "compose up -d --no-deps trainer",
        "compose up -d --no-deps --force-recreate trainer",
        "compose up -d trainer",
    ):
        assert forbidden not in src, f"deploy must not start trainer: {forbidden!r}"
