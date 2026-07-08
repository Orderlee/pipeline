"""Tests for vlm_pipeline.lib.embedding_model_name — image_embeddings model_name version encoding.

Design §5.3: fine-tune version is encoded INTO the model_name value (no model_version column).
Stock rows keep the bare base name; fine-tuned rows append '@<version>'.
"""

from __future__ import annotations

import pytest

from vlm_pipeline.lib.embedding_model_name import (
    STOCK_PE_CORE_MODEL_NAME,
    build_versioned_model_name,
    is_stock,
    parse_model_name,
)


def test_stock_constant_matches_serving_backend():
    # docker/embedding/backends/open_clip_be.py:12 MODEL_NAME
    assert STOCK_PE_CORE_MODEL_NAME == "facebook/PE-Core-L14-336"


def test_build_default_base():
    assert build_versioned_model_name("ft-2026.06.29-lora-001") == ("facebook/PE-Core-L14-336@ft-2026.06.29-lora-001")


def test_build_custom_base():
    assert build_versioned_model_name("v2", base="facebook/Other") == "facebook/Other@v2"


def test_build_rejects_empty_version():
    with pytest.raises(ValueError):
        build_versioned_model_name("")
    with pytest.raises(ValueError):
        build_versioned_model_name("  ")


def test_build_rejects_at_in_version():
    # '@' is the reserved separator — version must not contain it.
    with pytest.raises(ValueError):
        build_versioned_model_name("ft@bad")


def test_parse_versioned():
    base, version = parse_model_name("facebook/PE-Core-L14-336@ft-2026.06.29-lora-001")
    assert base == "facebook/PE-Core-L14-336"
    assert version == "ft-2026.06.29-lora-001"


def test_parse_stock_has_no_version():
    base, version = parse_model_name("facebook/PE-Core-L14-336")
    assert base == "facebook/PE-Core-L14-336"
    assert version is None


def test_roundtrip():
    name = build_versioned_model_name("ft-001")
    base, version = parse_model_name(name)
    assert build_versioned_model_name(version, base=base) == name


def test_is_stock():
    assert is_stock("facebook/PE-Core-L14-336") is True
    assert is_stock("facebook/PE-Core-L14-336@ft-001") is False
