"""frame_embedding @asset — PE-Core-L14-336 프레임 임베딩 (pgvector 적재).

이미지 메타데이터(image_metadata)에서 미임베딩 프레임을 조회하여
embedding-service HTTP API 로 임베딩 후 image_embeddings 에 upsert.
SAM3 detection_assets.py 패턴 미러.
"""

from __future__ import annotations

from dagster import Failure, Field, asset

from vlm_pipeline.defs.embed.helpers import build_caption_embedding_rows, build_frame_embedding_rows
from vlm_pipeline.lib.embedding import get_embedding_client
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource

DEFAULT_MODEL = "facebook/PE-Core-L14-336"
# entity_type='frame' = "whole-image embedding" (bbox crop=detection 과 구분). 사용자 결정(2026-06-15):
# 전부 임베딩 → 모든 whole-image role 포함(source_image 도). 라이브 sensor 도 동일 role 로 자동 임베딩.
FRAME_ROLES = ["processed_clip_frame", "raw_video_frame", "source_image"]

_EMBED_CONFIG_SCHEMA = {
    "limit": Field(int, default_value=500),
    "model_name": Field(str, default_value=DEFAULT_MODEL),
    # 빈 리스트 → FRAME_ROLES 기본. 명시 override 가능.
    "image_roles": Field([str], default_value=[]),
}


_CAPTION_EMBED_CONFIG_SCHEMA = {
    "limit": Field(int, default_value=500),
    "model_name": Field(str, default_value=DEFAULT_MODEL),
}


@asset(
    name="caption_embedding",
    group_name="embed",
    config_schema=_CAPTION_EMBED_CONFIG_SCHEMA,
)
def caption_embedding(
    context,
    db: PostgresResource,
) -> dict:
    """미임베딩 캡션을 embedding-service 로 임베딩 후 image_embeddings upsert (cross-modal)."""
    limit = int(context.op_config.get("limit", 500))
    model_name = str(context.op_config.get("model_name", DEFAULT_MODEL))

    # pgvector-gated image_embeddings(+007 컬럼)이 존재함을 보장 (embedding-only 경로가 첫 실행일 때 대비, Codex review).
    db.ensure_runtime_schema()
    pending = db.find_pending_caption_embeddings(model_name=model_name, limit=limit)
    context.log.info("caption_embedding: pending=%d limit=%d model=%s", len(pending), limit, model_name)
    if not pending:
        return {"embedded": 0, "failed": 0, "pending": 0}

    client = get_embedding_client()
    if not client.wait_until_ready():
        raise Failure(description="embedding-service not ready — caption_embedding aborting (systemic)")

    rows, failed = build_caption_embedding_rows(pending, client=client, model_name=model_name)
    inserted = db.batch_insert_embeddings(rows)
    context.log.info("caption_embedding: inserted=%d failed=%d", inserted, len(failed))

    if pending and inserted == 0:
        raise Failure(
            description=f"caption_embedding: all {len(pending)} pending captions failed to embed (systemic failure)"
        )
    return {"embedded": inserted, "failed": len(failed), "pending": len(pending)}


@asset(
    name="frame_embedding",
    group_name="embed",
    config_schema=_EMBED_CONFIG_SCHEMA,
)
def frame_embedding(
    context,
    db: PostgresResource,
    minio: MinIOResource,
) -> dict:
    """미임베딩 프레임을 embedding-service 로 임베딩 후 image_embeddings upsert."""
    limit = int(context.op_config.get("limit", 500))
    model_name = str(context.op_config.get("model_name", DEFAULT_MODEL))
    image_roles_raw = context.op_config.get("image_roles") or []
    roles = list(image_roles_raw) if image_roles_raw else FRAME_ROLES

    # pgvector-gated image_embeddings 테이블 존재 보장 (embedding-only 경로가 첫 실행일 때 대비, Codex review).
    db.ensure_runtime_schema()
    pending = db.find_pending_frame_embeddings(model_name=model_name, limit=limit, image_roles=roles)
    context.log.info("frame_embedding: pending=%d limit=%d model=%s roles=%s", len(pending), limit, model_name, roles)
    if not pending:
        # 빈 backlog → 서비스 readiness 체크 없이 즉시 종료 (수동 no-op run 편의).
        return {"embedded": 0, "failed": 0, "pending": 0}

    client = get_embedding_client()
    # 서비스 미가동은 systemic 실패 → Failure 로 run 을 실패시켜 가시화 (per-frame fail-forward 와 구분).
    if not client.wait_until_ready():
        raise Failure(description="embedding-service not ready — frame_embedding aborting (systemic)")

    rows, failed = build_frame_embedding_rows(pending, minio=minio, client=client, model_name=model_name)
    inserted = db.batch_insert_embeddings(rows)
    context.log.info("frame_embedding: inserted=%d failed=%d", inserted, len(failed))

    # systemic 실패 감지: 처리 대상이 있었는데 한 건도 못 넣었으면 (서비스/네트워크 전면 장애) run 실패.
    if inserted == 0:
        raise Failure(
            description=f"frame_embedding: all {len(pending)} pending frames failed to embed (systemic failure)"
        )
    return {"embedded": inserted, "failed": len(failed), "pending": len(pending)}
