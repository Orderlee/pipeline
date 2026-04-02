"""PROCESS @asset — clip_captioning (Gemini JSON → labels) + clip_to_frame (clip 생성 + 이미지 추출).

clip_captioning: vlm-labels의 Gemini JSON을 정규화하여 labels 테이블에 upsert.
clip_to_frame: labels 기반 clip 생성 → ffprobe 메타 → 적응형 프레임 추출 → image_metadata.

구현은 하위 모듈(captioning, frame_extract, raw_frames, helpers)에 위치하며,
이 파일은 @asset 데코레이터 + 라우팅 래퍼만 유지합니다.
"""

from __future__ import annotations

from dagster import Field, asset

from vlm_pipeline.lib.spec_config import is_unscoped_mvp_autolabel_run
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .captioning import clip_captioning_mvp, clip_captioning_routed_impl
from .frame_extract import clip_to_frame_mvp, clip_to_frame_routed_impl
from .raw_frames import raw_video_to_frame_impl


# ═══════════════════════════════════════════════════════════════
# clip_captioning — Gemini JSON 정규화 → labels upsert
# ═══════════════════════════════════════════════════════════════

@asset(
    name="clip_captioning",
    deps=["clip_timestamp"],
    description="Gemini JSON 정규화 → labels upsert; MVP는 auto_label_*, spec/dispatch는 caption_* 백로그",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=200)},
)
def clip_captioning(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    tags = context.run.tags if context.run else {}
    if is_unscoped_mvp_autolabel_run(tags):
        return clip_captioning_mvp(context, db, minio)
    return clip_captioning_routed_impl(context, db, minio)


# ═══════════════════════════════════════════════════════════════
# clip_to_frame — clip 생성 + ffprobe + 적응형 프레임 추출
# ═══════════════════════════════════════════════════════════════

@asset(
    deps=["clip_captioning"],
    name="clip_to_frame",
    description="labels 기반 clip+프레임 추출; MVP는 find_processable 단순 경로, spec/dispatch는 frame_status·이미지 캡션",
    group_name="auto_labeling",
    config_schema={
        "limit": Field(int, default_value=1000),
        "jpeg_quality": Field(int, default_value=90),
        "max_frames_per_video": Field(int, default_value=12),
    },
)
def clip_to_frame(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    tags = context.run.tags if context.run else {}
    if is_unscoped_mvp_autolabel_run(tags):
        return clip_to_frame_mvp(context, db, minio)
    return clip_to_frame_routed_impl(context, db, minio)


processed_clips = clip_to_frame


# ═══════════════════════════════════════════════════════════════
# [DISPATCH YOLO 전용] raw_video_to_frame
# ═══════════════════════════════════════════════════════════════

@asset(
    name="raw_video_to_frame",
    deps=["raw_ingest"],
    description="[Dispatch YOLO 전용] raw video에서 직접 이미지 추출",
    group_name="yolo",
    config_schema={
        "limit": Field(int, default_value=1000),
        "jpeg_quality": Field(int, default_value=90),
        "max_frames_per_video": Field(int, default_value=24),
    },
)
def raw_video_to_frame(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    return raw_video_to_frame_impl(context, db, minio)
