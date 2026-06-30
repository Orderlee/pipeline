"""lib.active_model — 활성 임베딩 model_name 해석 (순수, no PG)."""

from __future__ import annotations

from vlm_pipeline.lib.active_model import DEFAULT_SCOPE, resolve_active_model_name
from vlm_pipeline.lib.embedding_model_name import STOCK_PE_CORE_MODEL_NAME


def test_default_scope_is_frame_search() -> None:
    assert DEFAULT_SCOPE == "frame_search"


def test_none_row_returns_stock() -> None:
    assert resolve_active_model_name(None) == STOCK_PE_CORE_MODEL_NAME


def test_blank_model_name_returns_stock() -> None:
    assert resolve_active_model_name({"model_name": ""}) == STOCK_PE_CORE_MODEL_NAME
    assert resolve_active_model_name({"model_name": "   "}) == STOCK_PE_CORE_MODEL_NAME
    assert resolve_active_model_name({"model_name": None}) == STOCK_PE_CORE_MODEL_NAME


def test_set_value_returned() -> None:
    row = {"model_name": "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"}
    assert resolve_active_model_name(row) == "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"


def test_custom_default() -> None:
    assert resolve_active_model_name(None, default="x/y") == "x/y"
