"""docker/analysis/prompt_data_contract.py — 버전/gidx 해석 계약.

이 모듈이 틀리면 프레임↔문장 조인이 조용히 0건이 되거나(정규화 누락) 엉뚱한 뱅크의
문장에 귀속된다(오프셋 누락). 둘 다 예외를 내지 않고 **틀린 표**를 만든다.
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[2] / "docker" / "analysis" / "prompt_data_contract.py"
_SPEC = importlib.util.spec_from_file_location("prompt_data_contract", str(_PATH))
pdc = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pdc)


@pytest.mark.parametrize(
    "raw,want",
    [
        ("V1.0.10.3", "1.0.10.3"),
        ("v1.0.10.3", "1.0.10.3"),
        ("1.0.13.0", "1.0.13.0"),
        ("  v1.0.8.0  ", "1.0.8.0"),
        (None, ""),
        ("vGEN20260904", "GEN20260904"),
    ],
)
def test_norm_version(raw, want):
    assert pdc.norm_version(raw) == want


def test_local_gidx_strips_generation_block():
    # 실측: sourcei winner_gidx 는 2,600,012 처럼 블록 오프셋이 얹혀 있다.
    assert pdc.local_gidx(2_600_012) == 12
    assert pdc.local_gidx(0) == 0
    assert pdc.local_gidx(None) is None


def test_gidx_offset_is_a_constant_not_env_derived():
    # 오프셋이 런타임 env 에 의존하면 세대마다 조인이 어긋난다 (알려진 사고).
    assert pdc.GIDX_OFFSET == 100_000


def test_build_gidx_class_map_normalizes_and_masks():
    m = pdc.build_gidx_class_map(
        versions=["V1.0.8.0", "v1.0.8.0", None, "v1.0.8.0"],
        gidxs=[2_600_012, 2_600_013, 5, None],
        categories=["fire", "smoke", "normal", "normal"],
    )
    assert m == {("1.0.8.0", 12): "fire", ("1.0.8.0", 13): "smoke"}


def test_exact_duplicate_groups_finds_identical_prediction_vectors():
    groups = pdc.exact_duplicate_groups({"a": b"\x00\x01", "b": b"\x00\x01", "c": b"\x01\x01"})
    assert groups == [["a", "b"]]


def test_exact_duplicate_groups_ignores_singletons():
    assert pdc.exact_duplicate_groups({"a": b"\x00", "b": b"\x01"}) == []


def test_misattribution_suspect_is_same_preds_but_different_texts():
    """실측 v1.0.2.0/v1.0.2.1 — 텍스트는 다른데(자리표시자 vs 실문장) 예측이 같다.

    벡터 귀속이 틀렸다는 서명이다. 텍스트까지 같으면 그냥 양성 중복이라 잡으면 안 된다.
    """
    dup = [["1.0.2.0", "1.0.2.1"], ["1.0.5.1", "1.0.6.0"]]
    th = {"1.0.2.0": "hA", "1.0.2.1": "hB", "1.0.5.1": "hC", "1.0.6.0": "hC"}
    assert pdc.misattribution_suspects(dup, th) == [["1.0.2.0", "1.0.2.1"]]


def test_placeholder_text_is_not_corruption():
    """external_only 뱅크는 텍스트가 전부 자리표시자지만 벡터는 유효하다 — corrupt 아님."""
    dup = [["1.0.13.0", "1.0.13.1"]]
    th = {"1.0.13.0": "hSAME", "1.0.13.1": "hSAME"}
    assert pdc.misattribution_suspects(dup, th) == []


def test_text_set_hash_is_order_independent_and_strips():
    assert pdc.text_set_hash(["b", "a"]) == pdc.text_set_hash([" a ", "b"])
    assert pdc.text_set_hash(["a"]) != pdc.text_set_hash(["a", "b"])
