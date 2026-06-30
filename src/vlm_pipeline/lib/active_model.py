"""활성 임베딩 model_name 해석 (L1 lib — 순수).

design §8.1-A: AL/검색이 읽는 활성 model_name 을 embedding_active_model(migration 015) 행으로
관리한다. 행이 없거나 비어있으면 stock 으로 폴백 → 마이그레이션만 적용된 상태(미승격)에서
현행 동작 보존. 승격(H4)이 이 행을 UPDATE 해 포인터를 원자 전환한다.

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

from vlm_pipeline.lib.embedding_model_name import STOCK_PE_CORE_MODEL_NAME

DEFAULT_SCOPE = "frame_search"


def resolve_active_model_name(
    row: dict | None,
    *,
    default: str = STOCK_PE_CORE_MODEL_NAME,
) -> str:
    """embedding_active_model 행(dict) → 활성 model_name. 미설정/공백이면 default(stock)."""
    if not row:
        return default
    value = row.get("model_name")
    if value is None:
        return default
    value = str(value).strip()
    return value or default
