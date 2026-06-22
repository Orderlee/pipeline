"""frame_embedding @asset — PE-Core-L14-336 프레임 임베딩 (pgvector 적재).

이미지 메타데이터(image_metadata)에서 미임베딩 프레임을 조회하여
embedding-service HTTP API 로 임베딩 후 image_embeddings 에 upsert.
SAM3 detection_assets.py 패턴 미러.
"""

from __future__ import annotations

import os

from dagster import Failure, Field, asset

from vlm_pipeline.defs.embed.helpers import build_caption_embedding_rows, build_frame_embedding_rows
from vlm_pipeline.lib.embedding import get_embedding_client
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource

DEFAULT_MODEL = "facebook/PE-Core-L14-336"
# entity_type='frame' = "whole-image embedding" (bbox crop=detection 과 구분). 사용자 결정(2026-06-15):
# 전부 임베딩 → 모든 whole-image role 포함(source_image 도). 라이브 sensor 도 동일 role 로 자동 임베딩.
FRAME_ROLES = ["processed_clip_frame", "raw_video_frame", "source_image"]
# video-level mean-pool 은 영상 프레임만 대상. source_image(스틸 이미지)는 포함하지 않는다.
VIDEO_FRAME_ROLES = ["processed_clip_frame", "raw_video_frame"]

_EMBED_CONFIG_SCHEMA = {
    "limit": Field(int, default_value=500),
    "model_name": Field(str, default_value=DEFAULT_MODEL),
    # 빈 리스트 → FRAME_ROLES 기본. 명시 override 가능.
    "image_roles": Field([str], default_value=[]),
}


_CAPTION_EMBED_CONFIG_SCHEMA = {
    "limit": Field(int, default_value=500),
    "model_name": Field(str, default_value=DEFAULT_MODEL),
    # force_reembed=True: NOT EXISTS guard 를 무시하고 모든 caption 재임베딩 (한국어→영어 백필 전용).
    "force_reembed": Field(bool, default_value=False),
}


def _make_gemini_translate():
    """GeminiAnalyzer 를 초기화하고 translate_captions_to_english 에 바인딩한 callable 반환.

    ENABLE_GEMINI_TRANSLATE env 가 '0' 이면 None 을 반환 (번역 비활성, 한국어 그대로 임베딩).
    환경변수: GEMINI_PROJECT / GEMINI_LOCATION / GEMINI_GOOGLE_APPLICATION_CREDENTIALS 등
    GeminiAnalyzer 기존 credential 흐름 그대로 재사용.
    """
    if os.getenv("ENABLE_GEMINI_TRANSLATE", "1") == "0":
        return None
    from vlm_pipeline.lib.gemini import GeminiAnalyzer
    from vlm_pipeline.lib.gemini_translate import translate_captions_to_english

    analyzer = GeminiAnalyzer()

    def _generate(prompt: str) -> str:
        response = analyzer._generate_content_with_retry([prompt], content_type="text", source_name="caption_translate")
        return str(getattr(response, "text", "") or "").strip()

    def _translate(texts: list[str]) -> list[str]:
        return translate_captions_to_english(texts, gemini_generate=_generate)

    return _translate


@asset(
    name="caption_embedding",
    group_name="embed",
    config_schema=_CAPTION_EMBED_CONFIG_SCHEMA,
)
def caption_embedding(
    context,
    db: PostgresResource,
) -> dict:
    """미임베딩 캡션을 Gemini 번역(한→영) 후 embedding-service 임베딩, image_embeddings upsert (cross-modal).

    ENABLE_GEMINI_TRANSLATE=0 시 번역 없이 원문 임베딩 (디버그/비용 절감).
    force_reembed=True 시 기존 벡터 포함 전체 재임베딩 (한국어 벡터 영어 벡터로 교체 백필 전용).
    """
    limit = int(context.op_config.get("limit", 500))
    model_name = str(context.op_config.get("model_name", DEFAULT_MODEL))
    force_reembed = bool(context.op_config.get("force_reembed", False))

    # pgvector-gated image_embeddings(+007 컬럼)이 존재함을 보장 (embedding-only 경로가 첫 실행일 때 대비, Codex review).
    db.ensure_runtime_schema()

    if force_reembed:
        # Paginate over ALL captions (rows 501+ unreachable without offset).
        # One run processes every caption; offset advances by the actual page size returned.
        client = get_embedding_client()
        if not client.wait_until_ready():
            raise Failure(description="embedding-service not ready — caption_embedding aborting (systemic)")
        try:
            translate = _make_gemini_translate()
        except Exception as exc:
            context.log.warning("caption_embedding: Gemini translator init failed (%s), skipping translation", exc)
            translate = None
        total_pending = 0
        total_inserted = 0
        total_failed: list = []
        total_translated = 0
        total_fallback = 0
        offset = 0
        page_num = 0
        while True:
            page = db.find_all_caption_embeddings(limit=limit, offset=offset)
            if not page:
                break
            page_num += 1
            total_pending += len(page)
            context.log.info(
                "caption_embedding[force_reembed] page=%d offset=%d size=%d model=%s",
                page_num,
                offset,
                len(page),
                model_name,
            )
            rows, failed_ids, translated_count, fallback_count = build_caption_embedding_rows(
                page, client=client, model_name=model_name, translate=translate
            )
            inserted = db.batch_insert_embeddings(rows)
            total_inserted += inserted
            total_failed.extend(failed_ids)
            total_translated += translated_count
            total_fallback += fallback_count
            context.log.info(
                "caption_embedding[force_reembed] page=%d inserted=%d failed=%d "
                "translated=%d fallback=%d cumulative_inserted=%d",
                page_num,
                inserted,
                len(failed_ids),
                translated_count,
                fallback_count,
                total_inserted,
            )
            offset += len(page)

        context.log.info(
            "caption_embedding[force_reembed] COMPLETE pages=%d total_pending=%d total_inserted=%d "
            "total_failed=%d translated=%d fallback=%d translate_enabled=%s "
            # mixed Korean/English vectors share model_name; re-run with translation enabled re-covers them
            "note=re-run_with_translate_enabled_to_cover_fallback_rows",
            page_num,
            total_pending,
            total_inserted,
            len(total_failed),
            total_translated,
            total_fallback,
            translate is not None,
        )
        return {
            "embedded": total_inserted,
            "failed": len(total_failed),
            "pending": total_pending,
            "force_reembed": True,
            "translated": total_translated,
            "fallback": total_fallback,
        }

    # --- normal (non-force) path ---
    pending = db.find_pending_caption_embeddings(model_name=model_name, limit=limit)
    context.log.info("caption_embedding: pending=%d limit=%d model=%s", len(pending), limit, model_name)

    if not pending:
        return {"embedded": 0, "failed": 0, "pending": 0, "force_reembed": force_reembed}

    client = get_embedding_client()
    if not client.wait_until_ready():
        raise Failure(description="embedding-service not ready — caption_embedding aborting (systemic)")

    try:
        translate = _make_gemini_translate()
    except Exception as exc:
        context.log.warning("caption_embedding: Gemini translator init failed (%s), skipping translation", exc)
        translate = None

    rows, failed_ids, translated_count, fallback_count = build_caption_embedding_rows(
        pending, client=client, model_name=model_name, translate=translate
    )
    inserted = db.batch_insert_embeddings(rows)
    context.log.info(
        "caption_embedding: inserted=%d failed=%d translated=%d fallback=%d translate_enabled=%s "
        # mixed Korean/English vectors share model_name; re-run with force_reembed=True re-covers fallback rows
        "note=use_force_reembed_to_backfill_fallback_rows",
        inserted,
        len(failed_ids),
        translated_count,
        fallback_count,
        translate is not None,
    )

    if pending and inserted == 0:
        raise Failure(
            description=f"caption_embedding: all {len(pending)} pending captions failed to embed (systemic failure)"
        )
    return {
        "embedded": inserted,
        "failed": len(failed_ids),
        "pending": len(pending),
        "force_reembed": force_reembed,
        "translated": translated_count,
        "fallback": fallback_count,
    }


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
