"""model_name 별 partial HNSW 인덱스 SQL 빌더 (L1 lib — 순수 stdlib).

design §8.1-(C): PE-Core 챔피언-챌린저 승격 시 새 model_name('@ft-<ver>')으로 코퍼스를
재임베딩한 뒤, 그 model_name 만 타는 partial HNSW 를 빌드한다. model_name 이 동적이라
정적 마이그레이션(008 의 entity_type-only partial)으로는 열거 불가 → 승격 시점에 이
빌더가 만든 SQL 을 db.create_model_partial_hnsw() (Task H3) 가 실행한다.

008 의 entity_type-only partial 인덱스는 단일 model_name 환경에서 유효하나, 두 model_name
(stock + 챌린저)이 공존하면 planner 가 model_name 필터로 후보를 솎느라 비효율 → model_name
까지 좁힌 partial 이 필요 (006 헤더가 예고한 "partial-by-model index").

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

import hashlib

_VALID_ENTITY_TYPES = ("frame", "caption", "video")
# PG 식별자 최대 63 byte. prefix + 8 hex suffix 가 그 안에 들어가도록 구성.
_NAME_PREFIX = "image_embeddings_hnsw_"


def _quote_literal(value: str) -> str:
    """SQL 문자열 리터럴 escaping — single quote 를 두 배로 (injection 방지)."""
    return value.replace("'", "''")


def model_index_name(model_name: str, entity_type: str = "frame") -> str:
    """model_name+entity_type 당 결정적이고 ≤63자인 인덱스 이름.

    이름 = image_embeddings_hnsw_<entity_type>_<sha1(model_name)[:12]>.
    model_name 에 '/', '@', '.' 등 식별자 불가 문자가 있어 해시 suffix 로 안전하게 인코딩.
    """
    if entity_type not in _VALID_ENTITY_TYPES:
        raise ValueError(f"entity_type must be one of {_VALID_ENTITY_TYPES}, got {entity_type!r}")
    digest = hashlib.sha1(model_name.encode("utf-8")).hexdigest()[:12]
    return f"{_NAME_PREFIX}{entity_type}_{digest}"


def build_partial_hnsw_sql(model_name: str, entity_type: str = "frame") -> str:
    """해당 (entity_type, model_name) 만 타는 partial HNSW 생성 SQL (멱등)."""
    name = model_index_name(model_name, entity_type)
    et = _quote_literal(entity_type)
    mn = _quote_literal(model_name)
    return (
        f"CREATE INDEX IF NOT EXISTS {name} "
        f"ON image_embeddings USING hnsw (embedding vector_cosine_ops) "
        f"WHERE entity_type = '{et}' AND model_name = '{mn}'"
    )


def build_drop_partial_hnsw_sql(model_name: str, entity_type: str = "frame") -> str:
    """진 챌린저 인덱스 은퇴용 DROP SQL (멱등)."""
    name = model_index_name(model_name, entity_type)
    return f"DROP INDEX IF EXISTS {name}"
