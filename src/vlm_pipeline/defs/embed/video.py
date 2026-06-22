"""video_embedding @asset — video-level embeddings (frame_pool or video_model).

Method A (frame_pool): SQL-side L2-normalized mean of existing frame embeddings.
  No embedding-service call — pure pgvector aggregation.
Method B (video_model): future drop-in (raises Failure with explicit message).
"""

from __future__ import annotations

from dagster import Failure, Field, asset

from vlm_pipeline.defs.embed.assets import DEFAULT_MODEL, VIDEO_FRAME_ROLES
from vlm_pipeline.lib.video_embedding import (
    METHOD_FRAME_POOL,
    METHOD_VIDEO_MODEL,
    call_video_model_stub,
    video_model_name,
)
from vlm_pipeline.resources.postgres import PostgresResource

_VIDEO_EMBED_CONFIG_SCHEMA = {
    "method": Field(str, default_value=METHOD_FRAME_POOL, description="frame_pool | video_model"),
    "video_model_name": Field(
        str,
        default_value=video_model_name(DEFAULT_MODEL, METHOD_FRAME_POOL),
        description="model_name for video entity rows (default: <base>/framepool for frame_pool method)",
    ),
    "frame_model_name": Field(
        str,
        default_value=DEFAULT_MODEL,
        description="frame embedding model_name to aggregate from",
    ),
    "limit": Field(int, default_value=500),
    "video_roles": Field([str], default_value=[], description="image_roles filter (empty → FRAME_ROLES)"),
}


@asset(
    name="video_embedding",
    group_name="embed",
    config_schema=_VIDEO_EMBED_CONFIG_SCHEMA,
)
def video_embedding(
    context,
    db: PostgresResource,
) -> dict:
    """video-level embedding → image_embeddings (entity_type='video').

    frame_pool: SQL-side mean(frame embeddings) → l2_normalize → upsert. No service call.
    video_model: raises Failure (method B not yet implemented).
    """
    method = str(context.op_config.get("method", METHOD_FRAME_POOL))
    vmodel = str(context.op_config.get("video_model_name", video_model_name(DEFAULT_MODEL, METHOD_FRAME_POOL)))
    fmodel = str(context.op_config.get("frame_model_name", DEFAULT_MODEL))
    limit = int(context.op_config.get("limit", 500))
    roles_raw = context.op_config.get("video_roles") or []
    roles = list(roles_raw) if roles_raw else VIDEO_FRAME_ROLES

    if method == METHOD_VIDEO_MODEL:
        try:
            call_video_model_stub("<all>", vmodel)
        except NotImplementedError as exc:
            raise Failure(description=str(exc)) from exc

    if method != METHOD_FRAME_POOL:
        raise Failure(description=f"video_embedding: unknown method={method!r}. Use 'frame_pool' or 'video_model'.")

    db.ensure_runtime_schema()

    pending = db.find_pending_video_embeddings(
        video_model_name=vmodel,
        frame_model_name=fmodel,
        limit=limit,
        video_roles=roles,
    )
    context.log.info(
        "video_embedding: pending=%d limit=%d video_model=%s frame_model=%s roles=%s",
        len(pending),
        limit,
        vmodel,
        fmodel,
        roles,
    )
    if not pending:
        return {"embedded": 0, "pending": 0}

    inserted = db.aggregate_video_embeddings_framepool(
        video_model_name=vmodel,
        frame_model_name=fmodel,
        asset_ids=pending,
        video_roles=roles,
    )
    context.log.info("video_embedding: inserted=%d", inserted)

    if inserted == 0:
        raise Failure(
            description=(
                f"video_embedding: {len(pending)} pending videos processed but 0 rows upserted "
                "(frame embeddings may not satisfy the aggregation condition — check frame backlog)"
            )
        )
    return {"embedded": inserted, "pending": len(pending)}
