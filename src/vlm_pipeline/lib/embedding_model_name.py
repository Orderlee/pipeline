"""image_embeddings.model_name version encoding (L1 lib — pure stdlib).

Design §5.3 / SHARED CONTRACTS: the fine-tune version is encoded INTO the
`model_name` value rather than a separate `model_version` column, so existing
readers (fiftyone_pgvector search, AL, backfill_prod_embeddings) that filter
`WHERE model_name = %(model)s` get version isolation for free.

Convention:  "<base>@<version>"   e.g.  "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"
Stock rows keep the bare base name (no '@').

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

# Must match docker/embedding/backends/open_clip_be.py:12 MODEL_NAME.
STOCK_PE_CORE_MODEL_NAME = "facebook/PE-Core-L14-336"

_SEP = "@"


def build_versioned_model_name(version: str, *, base: str = STOCK_PE_CORE_MODEL_NAME) -> str:
    """Encode a fine-tune version into a model_name string.

    Raises ValueError on empty/blank version or a version containing the
    reserved '@' separator (which would make parsing ambiguous).
    """
    v = (version or "").strip()
    if not v:
        raise ValueError("version must be a non-empty string")
    if _SEP in v:
        raise ValueError(f"version must not contain '{_SEP}' (reserved separator): {version!r}")
    return f"{base}{_SEP}{v}"


def parse_model_name(model_name: str) -> tuple[str, str | None]:
    """Split a model_name into (base, version). version is None for stock names.

    Only the first '@' separates base from version; bases never contain '@'.
    """
    name = model_name or ""
    if _SEP not in name:
        return name, None
    base, _, version = name.partition(_SEP)
    return base, (version or None)


def is_stock(model_name: str) -> bool:
    """True if model_name carries no fine-tune version (bare base name)."""
    return _SEP not in (model_name or "")
