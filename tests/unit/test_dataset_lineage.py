"""Phase 3-D dataset lineage helpers (stack-candidates #11 1단계).

vlm_pipeline.lib.dataset_lineage — datasets row 에 build 시점 spec_hash/git_sha
/build_started_at 을 같이 기록하는 헬퍼 모듈. 본 단계는 *기록만* — dataset_prefix
(MinIO path) 변경 없음.

CI host pydantic skew 무관: 이 helper 는 hashlib/json/os/subprocess 표준 라이브러리
만 의존, dagster 없음.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path


def _load_dataset_lineage_module():
    """Force-load source file (CI runner stale editable-install 회피, Phase 3-C 와 동일 패턴)."""
    workspace_root = Path(__file__).resolve().parents[2]
    source_path = workspace_root / "src" / "vlm_pipeline" / "lib" / "dataset_lineage.py"
    spec = importlib.util.spec_from_file_location(
        "vlm_pipeline_lib_dataset_lineage_fresh",
        source_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to build spec for {source_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_fresh_module = _load_dataset_lineage_module()
capture_git_sha = _fresh_module.capture_git_sha
compute_spec_hash = _fresh_module.compute_spec_hash
make_build_lineage = _fresh_module.make_build_lineage


# ─── compute_spec_hash ─────────────────────────────────────────────────────


def test_spec_hash_is_deterministic() -> None:
    """같은 config dict ↔ 같은 hash, 호출 횟수/시점 무관."""
    cfg = {"a": 1, "b": [1, 2, 3], "c": "hello"}
    assert compute_spec_hash(cfg) == compute_spec_hash(cfg)


def test_spec_hash_key_order_independent() -> None:
    """JSON sort_keys=True 라 key 순서 영향 없어야 함."""
    assert compute_spec_hash({"a": 1, "b": 2}) == compute_spec_hash({"b": 2, "a": 1})


def test_spec_hash_changes_on_value_change() -> None:
    assert compute_spec_hash({"a": 1}) != compute_spec_hash({"a": 2})


def test_spec_hash_returns_16_hex_chars() -> None:
    h = compute_spec_hash({"a": 1})
    assert len(h) == 16
    assert all(c in "0123456789abcdef" for c in h)


def test_spec_hash_handles_nested_structures() -> None:
    """list / dict / 중첩 dict 모두 결정적."""
    a = compute_spec_hash({"k": [{"x": 1}, {"y": 2}], "z": None})
    b = compute_spec_hash({"z": None, "k": [{"x": 1}, {"y": 2}]})
    assert a == b


def test_spec_hash_handles_non_jsonable_with_default_str() -> None:
    """datetime 등 non-JSON 객체는 str() 로 직렬화 (default=str)."""
    h = compute_spec_hash({"t": datetime(2026, 1, 1)})
    assert isinstance(h, str) and len(h) == 16


# ─── capture_git_sha ───────────────────────────────────────────────────────


def test_git_sha_uses_dataops_env_first(monkeypatch) -> None:
    monkeypatch.setenv("DATAOPS_DEPLOYED_GIT_SHA", "abc123def456")
    monkeypatch.setenv("GITHUB_SHA", "wrongvalue")
    assert capture_git_sha() == "abc123def456"


def test_git_sha_falls_back_to_github_sha(monkeypatch) -> None:
    monkeypatch.delenv("DATAOPS_DEPLOYED_GIT_SHA", raising=False)
    monkeypatch.setenv("GITHUB_SHA", "fffeeeddd")
    assert capture_git_sha() == "fffeeeddd"


def test_git_sha_truncates_at_40_chars(monkeypatch) -> None:
    """full SHA 가 40자보다 길 일은 없지만 cap 보장 (env 오용 방어)."""
    monkeypatch.setenv("DATAOPS_DEPLOYED_GIT_SHA", "x" * 60)
    assert capture_git_sha() == "x" * 40


def test_git_sha_returns_fallback_when_no_env_and_no_git(monkeypatch, tmp_path) -> None:
    """env 도 없고 .git 도 없는 환경 (컨테이너 안) → fallback 반환."""
    monkeypatch.delenv("DATAOPS_DEPLOYED_GIT_SHA", raising=False)
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    monkeypatch.chdir(tmp_path)  # .git 없는 디렉토리
    # subprocess git rev-parse 가 fail → fallback 반환.
    assert capture_git_sha(fallback="explicit-fallback") == "explicit-fallback"


# ─── make_build_lineage ────────────────────────────────────────────────────


def test_make_build_lineage_returns_three_keys(monkeypatch) -> None:
    monkeypatch.setenv("DATAOPS_DEPLOYED_GIT_SHA", "deadbeef")
    out = make_build_lineage({"a": 1})
    assert set(out.keys()) == {"spec_hash", "git_sha", "build_started_at"}


def test_make_build_lineage_spec_hash_matches_compute(monkeypatch) -> None:
    cfg = {"foo": "bar", "n": 42}
    monkeypatch.setenv("DATAOPS_DEPLOYED_GIT_SHA", "deadbeef")
    out = make_build_lineage(cfg)
    assert out["spec_hash"] == compute_spec_hash(cfg)


def test_make_build_lineage_build_started_is_recent_datetime(monkeypatch) -> None:
    monkeypatch.setenv("DATAOPS_DEPLOYED_GIT_SHA", "deadbeef")
    before = datetime.now()
    out = make_build_lineage({"a": 1})
    after = datetime.now()
    assert isinstance(out["build_started_at"], datetime)
    assert before <= out["build_started_at"] <= after
