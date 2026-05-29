"""ls_tasks.py CATEGORY_SYNONYMS / build_label_normalizer 회귀 테스트.

2026-05-29 운영 진단으로 ``vanguardhealthcarevhc_v2`` LS 프로젝트 32 tasks 가 모두
prediction 0건이었음. 원인: Gemini events 가 ``fight`` / ``unsafe_climbing_activity``
같은 raw category 로 응답했는데 dispatch 카테고리 ``violence`` / ``climbing up`` 의
동의어 매핑이 ``CATEGORY_SYNONYMS`` 에 없어 normalizer 가 전부 drop.

본 테스트는 회귀 방어 — 새 카테고리 매핑이 깨지면 prediction attach 가 다시 침묵하지
않게 catch.

CI host pydantic skew 면역: ls_tasks.py 는 boto3/requests 의존하지만 본 테스트는
``ls_tasks`` 모듈 일부만 import (``CATEGORY_SYNONYMS``, ``build_label_normalizer``).
fresh import via spec_from_file_location.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_ls_tasks_module():
    """gemini/ls_tasks.py 를 직접 spec_from_file_location 으로 로드 (CI runner stale install 회피)."""
    workspace_root = Path(__file__).resolve().parents[2]
    gemini_dir = workspace_root / "src" / "gemini"
    src_str = str(gemini_dir)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)
    source_path = gemini_dir / "ls_tasks.py"
    spec = importlib.util.spec_from_file_location(
        "ls_tasks_fresh",
        source_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to build spec for {source_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_ls_tasks = _load_ls_tasks_module()
CATEGORY_SYNONYMS = _ls_tasks.CATEGORY_SYNONYMS
build_label_normalizer = _ls_tasks.build_label_normalizer


# ─── violence 매핑 ─────────────────────────────────────────────────────────


def test_violence_synonyms_include_fight():
    """vanguardhealthcarevhc_v2 회귀 — Gemini ``fight`` event 가 violence 로 매핑되어야 함."""
    assert "fight" in CATEGORY_SYNONYMS["violence"]


def test_violence_normalizer_picks_up_fight():
    norm = build_label_normalizer(["violence"])
    assert norm.get("fight") == "violence"
    # identity 도 유지.
    assert norm.get("violence") == "violence"


# ─── climbing up 매핑 ──────────────────────────────────────────────────────


def test_climbing_up_synonyms_include_unsafe_climbing_activity():
    """vanguardhealthcarevhc_v2 회귀 — ``unsafe_climbing_activity`` 가 climbing up 으로 매핑."""
    assert "unsafe_climbing_activity" in CATEGORY_SYNONYMS["climbing up"]


def test_climbing_up_normalizer_picks_up_unsafe_climbing_activity():
    norm = build_label_normalizer(["climbing up"])
    assert norm.get("unsafe_climbing_activity") == "climbing up"
    assert norm.get("climbing up") == "climbing up"
    # underscore variant 도 별도 알리아스로 정의함.
    assert norm.get("climbing_up") == "climbing up"


# ─── 회귀: 기존 매핑 무변경 ───────────────────────────────────────────────


def test_existing_falldown_mapping_intact():
    norm = build_label_normalizer(["falldown"])
    assert norm.get("fall") == "falldown"
    assert norm.get("falldown") == "falldown"
    assert norm.get("simulated_fall") == "falldown"


def test_existing_fire_smoke_person_mappings_intact():
    norm = build_label_normalizer(["fire", "smoke", "person"])
    assert norm.get("flame") == "fire"
    assert norm.get("cigarette") == "smoke"
    assert norm.get("person") == "person"


# ─── drop 동작 보존 (의도된 정책) ─────────────────────────────────────────


def test_unmapped_category_still_drops_when_not_in_target_cats():
    """매핑 안 된 raw category 는 normalizer.get → None → drop 정책 유지.

    Gemini 가 ``unauthorized_activity`` 처럼 등록 안 된 raw 를 반환해도 노이즈로
    LS 에 안 노출되어야 함 (운영 정책).
    """
    norm = build_label_normalizer(["violence", "climbing up"])
    assert norm.get("unauthorized_activity") is None
    assert norm.get("unsafe_act") is None
    assert norm.get("person_movement") is None


def test_identity_mapping_when_canonical_outside_synonyms_table():
    """SYNONYMS 에 없는 canonical 도 자기 자신은 매핑 (legacy 호환)."""
    norm = build_label_normalizer(["custom_label"])
    assert norm.get("custom_label") == "custom_label"
