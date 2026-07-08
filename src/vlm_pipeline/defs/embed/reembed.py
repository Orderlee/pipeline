"""전체 코퍼스 재임베딩 asset — 새 versioned model_name 으로 incumbent 커버 집합 재임베딩.

design §8.1-(B): PE-Core 챌린저가 게이트를 통과하면, 활성(incumbent) model_name 으로 임베딩된
집합(frame+caption)을 새 model_name='<base>@<new_version>'(§5.3)으로 재임베딩한다. 커버리지 =
incumbent 집합 (전체 image_metadata 아님). 신규 프레임은 평소 frame_embedding asset 이 활성
포인터로 계속 임베딩. 페이지 루프 + ON CONFLICT upsert 로 resumable.

⚠️ 실 실행은 게이트 뒤 (ENABLE_TRAINING + 수동 launch). 게이트 미설정 시 asset 은 no-op 보고.
gpu_trainer 동시성 태그(op_tags)로 trainer + embedding-service 부하 직렬화 (R2 / dagster.yaml).
"""

from __future__ import annotations

import os

from dagster import Failure, Field, asset

from vlm_pipeline.defs.embed.helpers import build_caption_embedding_rows, build_frame_embedding_rows
from vlm_pipeline.lib.embedding import get_embedding_client
from vlm_pipeline.lib.embedding_model_name import build_versioned_model_name
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource

_REEMBED_CONFIG_SCHEMA = {
    "new_version": Field(str, default_value=""),  # 'ft-2026.06.29-lora-001' — 빈 값이면 실패
    "incumbent_model_name": Field(str, default_value=""),  # 빈 값이면 활성 포인터 사용
    "entity_types": Field([str], default_value=["frame", "caption"]),
    "limit": Field(int, default_value=500),
}


def _run_reembed(context, db: PostgresResource, minio: MinIOResource) -> dict:
    new_version = str(context.op_config.get("new_version") or "").strip()
    if not new_version:
        raise Failure(description="reembed: 'new_version' is required (e.g. 'ft-2026.06.29-lora-001')")
    new_model_name = build_versioned_model_name(new_version)  # raises on bad version
    entity_types = list(context.op_config.get("entity_types") or ["frame", "caption"])
    limit = int(context.op_config.get("limit", 500))

    db.ensure_runtime_schema()
    incumbent = str(context.op_config.get("incumbent_model_name") or "").strip() or db.get_active_embedding_model()
    if incumbent == new_model_name:
        raise Failure(description=f"reembed: incumbent == new_model_name ({incumbent!r}) — refusing self-overwrite")

    if str(os.getenv("ENABLE_TRAINING", "")).strip() not in ("1", "true", "True"):
        context.log.warning(
            "reembed: ENABLE_TRAINING not set → scaffolding no-op (incumbent=%s new=%s). "
            "Set ENABLE_TRAINING=1 + launch manually to actually re-embed.",
            incumbent,
            new_model_name,
        )
        return {"embedded": 0, "new_model_name": new_model_name, "incumbent": incumbent, "gated": True}

    client = get_embedding_client()
    if not client.wait_until_ready():
        raise Failure(description="embedding-service not ready — reembed aborting (systemic)")

    total = 0
    for et in entity_types:
        while True:
            page = db.find_reembed_targets(
                incumbent_model_name=incumbent, new_model_name=new_model_name, entity_type=et, limit=limit
            )
            if not page:
                break
            if et == "frame":
                rows, failed = build_frame_embedding_rows(page, minio=minio, client=client, model_name=new_model_name)
            else:
                rows, failed, _, _ = build_caption_embedding_rows(page, client=client, model_name=new_model_name)
            inserted = db.batch_insert_embeddings(rows)
            total += inserted
            context.log.info(
                "reembed[%s]: page=%d inserted=%d failed=%d new_model=%s",
                et,
                len(page),
                inserted,
                len(failed),
                new_model_name,
            )
            # 진전 없음(전부 embed 실패) → NOT EXISTS 가 같은 page 를 무한 재조회. 중단해 GPU 낭비 방지.
            if inserted == 0:
                context.log.warning("reembed[%s]: page made 0 progress (all failed) — stopping this entity", et)
                break
    return {"embedded": total, "new_model_name": new_model_name, "incumbent": incumbent, "gated": False}


@asset(
    name="reembed_under_version",
    group_name="embed",
    config_schema=_REEMBED_CONFIG_SCHEMA,
    op_tags={"gpu_trainer": "true"},
)
def reembed_under_version(context, db: PostgresResource, minio: MinIOResource) -> dict:
    """incumbent 커버 집합을 새 versioned model_name 으로 재임베딩 (design §8.1-B). 실행은 게이트 뒤."""
    return _run_reembed(context, db, minio)
