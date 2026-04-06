"""Dagster Definitions — 로컬 DAG 시각화 전용 (v5 sub-asset).

기존 definitions.py를 수정하지 않고, localhost에서만 사용하는 확장 DAG.
방법 B: 각 auto_labeling asset 내부 세부 작업을 독립 sub-asset으로 분리하여
전역 Asset Lineage에서 전체 파이프라인 단계가 보이도록 구성.

설계 v5 흐름:
  labeling_spec_ingest ──┐
                         ▼
  incoming_nas → raw_ingest → ingest_router ──→ [Timestamp] ts_config_load → ts_video_scan
                                   │                          → ts_model_inference → ts_event_postprocess → ts_label_save
                                   │             [Captioning] → cap_event_extract → cap_caption_generate → cap_label_update
                                   │             [Frame]      → frame_clip_extract → frame_image_extract → frame_metadata_save
                                   │             [BBox]       → bbox_model_inference → bbox_label_save
                                   │             [Build]      → ds_split_plan → ds_copy_assets → ds_metadata_save
                                   │             [Sync]       → motherduck_sync_routed
                                   └──→ pending_ingest
                                              ▲
                                    spec_resolve_sensor

물리적 파일 이동은 raw_ingest에서 완료. 이후 단계는 DB 상태 전이만 수행.
config 조회: requester(personal) → team(default) → _fallback → 에러
"""

from dagster import (
    AssetKey,
    DefaultSensorStatus,
    Definitions,
    EnvVar,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
)

from vlm_pipeline.defs.gcp.assets import gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Spec & Config
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@asset(
    name="labeling_spec_ingest",
    description="Flex Playwright API → spec.json 수신 → labeling_specs 테이블 저장",
    group_name="spec",
)
def labeling_spec_ingest(context) -> dict:
    """Flex 워크플로우 문서에서 spec.json을 수신하여 labeling_specs 테이블에 저장."""
    context.log.info("labeling_spec_ingest: Flex API에서 spec 수신")
    return {"specs_loaded": 0}


@asset(
    name="config_sync",
    description="config/parameters/ JSON → labeling_configs 테이블 동기화",
    group_name="config",
)
def config_sync(context) -> dict:
    """config/parameters/ 폴더의 JSON 파일을 스캔하여 DB에 동기화."""
    context.log.info("config_sync: 파라미터 파일 → DB 동기화")
    return {"synced": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Routing & Pending
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@asset(
    name="ingest_router",
    description="spec 매칭 → ready_for_labeling / pending_spec 분기 (DB 상태 전이)",
    group_name="ingest",
    deps=[AssetKey("raw_ingest"), AssetKey("labeling_spec_ingest")],
)
def ingest_router(context) -> dict:
    """source_unit_name으로 spec 매칭 → DB 상태 분기."""
    context.log.info("ingest_router: spec 기반 DB 상태 분기")
    return {"routed_to_labeling": 0, "routed_to_pending": 0}


@asset(
    name="pending_ingest",
    description="pending_spec 데이터 집계 (Slack 봇 확정 → sensor로 재개)",
    group_name="pending",
    deps=[AssetKey("ingest_router")],
)
def pending_ingest(context) -> dict:
    """pending_spec 상태 데이터 집계."""
    context.log.info("pending_ingest: pending_spec 데이터 집계")
    return {"pending_count": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Timestamp Detection (5 sub-assets)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@asset(
    name="ts_config_load",
    description="config_json.timestamp 섹션 로드 (detection_model, threshold 등)",
    group_name="timestamp",
    deps=[AssetKey("ingest_router"), AssetKey("config_sync")],
)
def ts_config_load(context) -> dict:
    """labeling_configs에서 timestamp 파라미터 로드."""
    context.log.info("ts_config_load: timestamp config 로드")
    return {"config_loaded": True}


@asset(
    name="ts_video_scan",
    description="ready_for_labeling 영상 목록 조회",
    group_name="timestamp",
    deps=[AssetKey("ts_config_load")],
)
def ts_video_scan(context) -> dict:
    """raw_files에서 대상 영상 목록 조회."""
    context.log.info("ts_video_scan: 대상 영상 스캔")
    return {"video_count": 0}


@asset(
    name="ts_model_inference",
    description="이벤트 감지 모델 추론 → raw 이벤트 구간 리스트",
    group_name="timestamp",
    deps=[AssetKey("ts_video_scan")],
)
def ts_model_inference(context) -> dict:
    """detection_model로 영상별 이벤트 구간 추론."""
    context.log.info("ts_model_inference: 이벤트 감지 모델 추론")
    return {"raw_events": 0}


@asset(
    name="ts_event_postprocess",
    description="인접 구간 병합 + pre/post buffer + min/max duration 필터링",
    group_name="timestamp",
    deps=[AssetKey("ts_model_inference")],
)
def ts_event_postprocess(context) -> dict:
    """merge_gap_sec 병합, buffer 적용, duration 필터링."""
    context.log.info("ts_event_postprocess: 이벤트 후처리")
    return {"merged_events": 0, "filtered_events": 0}


@asset(
    name="ts_label_save",
    description="labels 테이블 INSERT + 라벨 JSON → MinIO(vlm-labels) 업로드",
    group_name="timestamp",
    deps=[AssetKey("ts_event_postprocess")],
)
def ts_label_save(context) -> dict:
    """이벤트 구간 라벨 DB 저장 + MinIO 업로드."""
    context.log.info("ts_label_save: 라벨 저장")
    return {"labels_saved": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Captioning (3 sub-assets)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@asset(
    name="cap_event_extract",
    description="이벤트 구간별 영상 구간 추출 + config.captioning 로드",
    group_name="captioning",
    deps=[AssetKey("ts_label_save")],
)
def cap_event_extract(context) -> dict:
    """labels에서 캡션 미생성 이벤트 조회 + 영상 구간 추출."""
    context.log.info("cap_event_extract: 이벤트 구간 추출")
    return {"events_extracted": 0}


@asset(
    name="cap_caption_generate",
    description="AI 모델로 캡션 텍스트 생성",
    group_name="captioning",
    deps=[AssetKey("cap_event_extract")],
)
def cap_caption_generate(context) -> dict:
    """captioning 모델 추론 → 캡션 텍스트 생성."""
    context.log.info("cap_caption_generate: 캡션 생성")
    return {"captions_generated": 0}


@asset(
    name="cap_label_update",
    description="labels.caption_text UPDATE + 라벨 JSON 갱신 → MinIO",
    group_name="captioning",
    deps=[AssetKey("cap_caption_generate")],
)
def cap_label_update(context) -> dict:
    """캡션 결과를 labels 테이블에 갱신 + MinIO 라벨 JSON 업데이트."""
    context.log.info("cap_label_update: 캡션 라벨 갱신")
    return {"labels_updated": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Frame Extraction (3 sub-assets)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@asset(
    name="frame_clip_extract",
    description="이벤트 구간 → clip 영상 절단 (ffmpeg) + config.frame_extraction 로드",
    group_name="frame_extraction",
    deps=[AssetKey("cap_label_update")],
)
def frame_clip_extract(context) -> dict:
    """이벤트 구간을 clip 영상으로 절단."""
    context.log.info("frame_clip_extract: clip 영상 절단")
    return {"clips_extracted": 0}


@asset(
    name="frame_image_extract",
    description="clip에서 설정 간격으로 frame 이미지 추출 → MinIO(vlm-processed)",
    group_name="frame_extraction",
    deps=[AssetKey("frame_clip_extract")],
)
def frame_image_extract(context) -> dict:
    """clip 영상에서 frame_interval_sec 간격으로 frame 추출 + MinIO 업로드."""
    context.log.info("frame_image_extract: frame 이미지 추출")
    return {"frames_extracted": 0}


@asset(
    name="frame_metadata_save",
    description="processed_clips + image_metadata 테이블 INSERT",
    group_name="frame_extraction",
    deps=[AssetKey("frame_image_extract")],
)
def frame_metadata_save(context) -> dict:
    """processed_clips, image_metadata 테이블에 메타데이터 저장."""
    context.log.info("frame_metadata_save: 메타데이터 저장")
    return {"clips_saved": 0, "images_saved": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BBox Labeling (2 sub-assets)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@asset(
    name="bbox_model_inference",
    description="spec.classes 기반 객체 감지 모델 추론 → bbox + class + confidence",
    group_name="bbox_labeling",
    deps=[AssetKey("frame_metadata_save")],
)
def bbox_model_inference(context) -> dict:
    """객체 감지 모델로 frame 이미지에서 bbox 추론."""
    context.log.info("bbox_model_inference: 객체 감지 추론")
    return {"detections": 0}


@asset(
    name="bbox_label_save",
    description="labels INSERT/UPDATE (bbox 정보) + bbox JSON → MinIO(vlm-labels)",
    group_name="bbox_labeling",
    deps=[AssetKey("bbox_model_inference")],
)
def bbox_label_save(context) -> dict:
    """bbox 라벨을 DB에 저장 + MinIO 업로드."""
    context.log.info("bbox_label_save: bbox 라벨 저장")
    return {"labels_saved": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Build Dataset (3 sub-assets)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@asset(
    name="ds_split_plan",
    description="train/val/test 분할 계획 수립 (80:10:10 셔플)",
    group_name="build",
    deps=[AssetKey("bbox_label_save")],
)
def ds_split_plan(context) -> dict:
    """대상 데이터 셔플 + 분할 비율 적용."""
    context.log.info("ds_split_plan: 분할 계획 수립")
    return {"total": 0, "train": 0, "val": 0, "test": 0}


@asset(
    name="ds_copy_assets",
    description="vlm-processed → vlm-dataset S3 copy (clip + label + metadata)",
    group_name="build",
    deps=[AssetKey("ds_split_plan")],
)
def ds_copy_assets(context) -> dict:
    """분할 계획에 따라 S3 복사 수행."""
    context.log.info("ds_copy_assets: S3 복사")
    return {"copied": 0}


@asset(
    name="ds_metadata_save",
    description="datasets + dataset_clips 테이블 INSERT + build_status 갱신",
    group_name="build",
    deps=[AssetKey("ds_copy_assets")],
)
def ds_metadata_save(context) -> dict:
    """데이터셋 메타데이터 DB 저장 + 상태 완료 처리."""
    context.log.info("ds_metadata_save: 데이터셋 메타 저장")
    return {"dataset_id": None}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Sync
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@asset(
    name="motherduck_sync_routed",
    description="DuckDB → MotherDuck 클라우드 동기화",
    group_name="sync",
    deps=[AssetKey("ds_metadata_save")],
)
def motherduck_sync_routed(context) -> dict:
    """클라우드 동기화."""
    context.log.info("motherduck_sync: 클라우드 동기화")
    return {"synced": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Sensor
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@sensor(
    name="spec_resolve_sensor",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
    description="pending_resolved 감지 → DB 상태 전이 + auto_labeling 트리거",
)
def spec_resolve_sensor(context):
    """Slack 봇 확정 → spec_status/ingest_status 전이 + auto_labeling_routed_job 트리거."""
    yield SkipReason("로컬 환경 — spec_resolve_sensor 비활성")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Jobs
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

auto_labeling_routed_job = define_asset_job(
    "auto_labeling_routed_job",
    selection=[
        ts_config_load,
        ts_video_scan,
        ts_model_inference,
        ts_event_postprocess,
        ts_label_save,
        cap_event_extract,
        cap_caption_generate,
        cap_label_update,
        frame_clip_extract,
        frame_image_extract,
        frame_metadata_save,
        bbox_model_inference,
        bbox_label_save,
    ],
    description="spec 기반 auto_labeling 전체 (timestamp → captioning → frame → bbox)",
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Definitions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ALL_ASSETS = [
    # ingest
    gcs_download_to_incoming,
    raw_ingest,
    labeling_spec_ingest,
    config_sync,
    ingest_router,
    pending_ingest,
    # timestamp
    ts_config_load,
    ts_video_scan,
    ts_model_inference,
    ts_event_postprocess,
    ts_label_save,
    # captioning
    cap_event_extract,
    cap_caption_generate,
    cap_label_update,
    # frame extraction
    frame_clip_extract,
    frame_image_extract,
    frame_metadata_save,
    # bbox
    bbox_model_inference,
    bbox_label_save,
    # build
    ds_split_plan,
    ds_copy_assets,
    ds_metadata_save,
    # sync
    motherduck_sync_routed,
]

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[auto_labeling_routed_job],
    sensors=[spec_resolve_sensor],
    resources={
        "db": DuckDBResource(db_path=EnvVar("DATAOPS_DUCKDB_PATH")),
        "minio": MinIOResource(
            endpoint=EnvVar("MINIO_ENDPOINT"),
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
        ),
    },
)
