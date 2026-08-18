"""supersede_by_stable_signature / _dispatch_origin_key 단위 테스트.

Dagster import 호환성 문제를 우회하기 위해 대상 함수만 동적 로드한다.
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from unittest import mock


def _load_target_functions():
    """sensor_helpers.py에서 테스트 대상 함수만 추출 (dagster mock)."""
    dagster_mock = types.ModuleType("dagster")
    dagster_core = types.ModuleType("dagster._core")
    dagster_storage = types.ModuleType("dagster._core.storage")
    dagster_run = types.ModuleType("dagster._core.storage.dagster_run")
    dagster_run.DagsterRunStatus = mock.MagicMock()
    dagster_run.RunsFilter = mock.MagicMock()
    dagster_mock._core = dagster_core
    dagster_core.storage = dagster_storage
    dagster_storage.dagster_run = dagster_run

    saved = {}
    stub_modules = {
        "dagster": dagster_mock,
        "dagster._core": dagster_core,
        "dagster._core.storage": dagster_storage,
        "dagster._core.storage.dagster_run": dagster_run,
    }
    for name, mod in stub_modules.items():
        saved[name] = sys.modules.get(name)
        sys.modules[name] = mod

    mod_name = "vlm_pipeline.defs.ingest.sensor_helpers"
    if mod_name in sys.modules:
        del sys.modules[mod_name]

    try:
        module = importlib.import_module(mod_name)
        return module._dispatch_origin_key, module.supersede_by_stable_signature
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
        sys.modules.pop(mod_name, None)


_dispatch_origin_key, supersede_by_stable_signature = _load_target_functions()


def _entry(
    *,
    dispatch_key: str = "",
    sig: str = "",
    mtime_ns: int = 0,
    name: str = "test.json",
) -> dict:
    return {
        "path": Path(f"/tmp/pending/{name}"),
        "source_unit_dispatch_key": dispatch_key,
        "stable_signature": sig,
        "mtime_ns": mtime_ns,
    }


# ---------------------------------------------------------------------------
# _dispatch_origin_key
# ---------------------------------------------------------------------------


class TestDispatchOriginKey:
    def test_chunked_gets_suffix(self):
        entry = _entry(dispatch_key="/nas/incoming/gcp/bucket/20260309#chunk:0001/0002")
        assert _dispatch_origin_key(entry) == "/nas/incoming/gcp/bucket/20260309#chunked"

    def test_non_chunked_returns_as_is(self):
        entry = _entry(dispatch_key="/nas/incoming/gcp/bucket/20260309")
        assert _dispatch_origin_key(entry) == "/nas/incoming/gcp/bucket/20260309"

    def test_empty_key(self):
        entry = _entry(dispatch_key="")
        assert _dispatch_origin_key(entry) == ""


# ---------------------------------------------------------------------------
# supersede_by_stable_signature
# ---------------------------------------------------------------------------

SIG_A = "147:365713965:1773023880000000000"
SIG_B = "50:123456789:9999999999999999999"


class TestSupersedeByStableSignature:
    def test_auto_and_manual_same_sig_manual_superseded(self):
        """auto_bootstrap 청크 2개 + manual 1개 (같은 sig) -> manual이 supersede."""
        auto1 = _entry(
            dispatch_key="/nas/incoming/gcp/bucket/20260309#chunk:0001/0002",
            sig=SIG_A,
            mtime_ns=2000,
            name="auto_001.json",
        )
        auto2 = _entry(
            dispatch_key="/nas/incoming/gcp/bucket/20260309#chunk:0002/0002",
            sig=SIG_A,
            mtime_ns=2100,
            name="auto_002.json",
        )
        manual = _entry(
            dispatch_key="/nas/incoming/gcp/bucket/20260309",
            sig=SIG_A,
            mtime_ns=1000,
            name="manual_003.json",
        )

        selected, superseded = supersede_by_stable_signature([auto1, auto2, manual])

        selected_names = {e["path"].name for e in selected}
        superseded_names = {e["path"].name for e in superseded}

        assert selected_names == {"auto_001.json", "auto_002.json"}
        assert superseded_names == {"manual_003.json"}

    def test_manual_newer_than_auto_auto_superseded(self):
        """manual이 더 최신이면 auto 청크들이 supersede."""
        auto1 = _entry(
            dispatch_key="/nas/incoming/gcp/bucket/20260309#chunk:0001/0002",
            sig=SIG_A,
            mtime_ns=1000,
            name="auto_001.json",
        )
        auto2 = _entry(
            dispatch_key="/nas/incoming/gcp/bucket/20260309#chunk:0002/0002",
            sig=SIG_A,
            mtime_ns=1100,
            name="auto_002.json",
        )
        manual = _entry(
            dispatch_key="/nas/incoming/gcp/bucket/20260309",
            sig=SIG_A,
            mtime_ns=5000,
            name="manual_003.json",
        )

        selected, superseded = supersede_by_stable_signature([auto1, auto2, manual])

        selected_names = {e["path"].name for e in selected}
        superseded_names = {e["path"].name for e in superseded}

        assert selected_names == {"manual_003.json"}
        assert superseded_names == {"auto_001.json", "auto_002.json"}

    def test_different_sig_no_supersede(self):
        """stable_signature가 다르면 서로 영향 없음."""
        e1 = _entry(dispatch_key="key_a", sig=SIG_A, mtime_ns=1000, name="a.json")
        e2 = _entry(dispatch_key="key_b", sig=SIG_B, mtime_ns=2000, name="b.json")

        selected, superseded = supersede_by_stable_signature([e1, e2])

        assert len(selected) == 2
        assert len(superseded) == 0

    def test_empty_sig_skipped(self):
        """stable_signature가 비어있는 manifest는 항상 selected."""
        e1 = _entry(dispatch_key="key_a", sig="", mtime_ns=1000, name="a.json")
        e2 = _entry(dispatch_key="key_b", sig="", mtime_ns=2000, name="b.json")

        selected, superseded = supersede_by_stable_signature([e1, e2])

        assert len(selected) == 2
        assert len(superseded) == 0

    def test_same_sig_same_base_key_no_supersede(self):
        """같은 signature + 같은 dispatch base key(같은 출처) -> supersede 안 함."""
        chunk1 = _entry(
            dispatch_key="/nas/incoming/gcp/bucket/20260309#chunk:0001/0003",
            sig=SIG_A,
            mtime_ns=1000,
            name="auto_001.json",
        )
        chunk2 = _entry(
            dispatch_key="/nas/incoming/gcp/bucket/20260309#chunk:0002/0003",
            sig=SIG_A,
            mtime_ns=1100,
            name="auto_002.json",
        )
        chunk3 = _entry(
            dispatch_key="/nas/incoming/gcp/bucket/20260309#chunk:0003/0003",
            sig=SIG_A,
            mtime_ns=1200,
            name="auto_003.json",
        )

        selected, superseded = supersede_by_stable_signature([chunk1, chunk2, chunk3])

        assert len(selected) == 3
        assert len(superseded) == 0

    def test_single_entry_no_supersede(self):
        """단일 항목은 항상 selected."""
        e = _entry(dispatch_key="key_a", sig=SIG_A, mtime_ns=1000, name="a.json")

        selected, superseded = supersede_by_stable_signature([e])

        assert len(selected) == 1
        assert len(superseded) == 0

    def test_mixed_sigs_independent(self):
        """서로 다른 sig 그룹은 독립적으로 처리."""
        auto_a = _entry(
            dispatch_key="/path_a#chunk:0001/0001",
            sig=SIG_A,
            mtime_ns=2000,
            name="auto_a.json",
        )
        manual_a = _entry(
            dispatch_key="/path_a",
            sig=SIG_A,
            mtime_ns=1000,
            name="manual_a.json",
        )
        auto_b = _entry(
            dispatch_key="/path_b#chunk:0001/0001",
            sig=SIG_B,
            mtime_ns=500,
            name="auto_b.json",
        )
        manual_b = _entry(
            dispatch_key="/path_b",
            sig=SIG_B,
            mtime_ns=3000,
            name="manual_b.json",
        )

        selected, superseded = supersede_by_stable_signature([auto_a, manual_a, auto_b, manual_b])

        selected_names = {e["path"].name for e in selected}
        superseded_names = {e["path"].name for e in superseded}

        assert "auto_a.json" in selected_names
        assert "manual_a.json" in superseded_names
        assert "manual_b.json" in selected_names
        assert "auto_b.json" in superseded_names

    def test_empty_list(self):
        selected, superseded = supersede_by_stable_signature([])
        assert selected == []
        assert superseded == []
