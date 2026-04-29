"""BUILD 도메인 sensor + asset job.

Label Studio 검수 확정(`/sync-approve` → `labels.review_status='finalized'`) 된
프로젝트 중, 아직 `vlm-dataset` 빌드가 완료되지 않은 folder를 감지해
`build_dataset_single_job` 을 자동 트리거합니다.

Layer 4: Dagster sensor + job.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from dagster import (
    AssetSelection,
    DefaultSensorStatus,
    RunRequest,
    SkipReason,
    define_asset_job,
    sensor,
)

from vlm_pipeline.defs.build.assets import build_dataset
from vlm_pipeline.lib.env_utils import (
    DUCKDB_LEGACY_WRITER_TAG,
    build_duckdb_writer_tags,
    default_duckdb_path,
    int_env,
)


# ---------------------------------------------------------------------------
# Job: 단일 프로젝트 build_dataset 실행용
# ---------------------------------------------------------------------------

build_dataset_single_job = define_asset_job(
    name="build_dataset_single_job",
    selection=AssetSelection.assets(build_dataset),
    tags=build_duckdb_writer_tags(DUCKDB_LEGACY_WRITER_TAG),
    description=(
        "build_dataset asset을 config.folder 단일 프로젝트로 실행. "
        "build_dataset_on_finalize_sensor 가 트리거."
    ),
)


# ---------------------------------------------------------------------------
# Sensor
# ---------------------------------------------------------------------------

def _fetch_projects_ready_to_build(db_path: str) -> list[str]:
    """dispatch.outputs 가 요구하는 모든 검수 단계가 finalized 된 folder 조회.

    이전 SQL 은 (labels.finalized OR image_labels.finalized) OR-조건이라
    timestamp_video + bbox 둘 다 outputs 에 있는 프로젝트(예: VHC)에서 한쪽만
    finalize 돼도 후보로 잡혀, build_dataset 이 부분 빌드 후 datasets.completed
    로 잠겨 두 번째 검수가 영구 누락되는 위험이 있었다.

    수정 SQL:
      - dispatch_requests 와 join 하여 outputs 에 명시된 단계만 검사
      - outputs 에 timestamp_video 있으면 → labels.finalized 필요
      - outputs 에 bbox 있으면          → image_labels.finalized 필요 +
        post_review_clip_job 의 결과인 processed_clips 가 채워졌어야 함
        (race condition 방지: finalize 직후 60s 안에 sensor 가 트리거되면
         post_review_clip_job 이 아직 진행중이라 processed_clips 가 0건일 수
         있다. 그 시점에 빌드하면 vlm-dataset/<folder>/clips/ 가 비어버리고
         datasets.build_status='completed' 잠금에 의해 영구 누락됨.)
      - classification-only 케이스(outputs 에 timestamp_video/bbox 없음)는
        본 sensor 처리 대상 아님 (build_classification 이 별도 처리하고 종료)

    `DuckDBResource.find_projects_ready_to_build()` 와 동일 SQL 이지만,
    sensor에서는 resource 주입 없이 read-only 연결을 직접 열어 수행.
    """
    with duckdb.connect(db_path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT r.source_unit_name AS folder
            FROM raw_files r
            JOIN dispatch_requests dr
              ON dr.folder_name = r.source_unit_name
             AND dr.status = 'completed'
            WHERE r.source_unit_name IS NOT NULL
              AND r.source_unit_name <> ''
              -- timestamp_video 또는 bbox 가 outputs 에 있어야 sensor 후보
              -- (classification-only 케이스는 build_classification 이 처리하므로 제외)
              AND (dr.outputs LIKE '%timestamp_video%' OR dr.outputs LIKE '%bbox%')
              -- video 가 요구되면 모두 finalize
              AND (dr.outputs NOT LIKE '%timestamp_video%' OR EXISTS (
                    SELECT 1 FROM labels l
                    WHERE l.asset_id = r.asset_id
                      AND l.review_status = 'finalized'))
              -- bbox 가 요구되면 모두 finalize
              AND (dr.outputs NOT LIKE '%bbox%' OR EXISTS (
                    SELECT 1 FROM image_labels il
                    JOIN image_metadata im ON im.image_id = il.image_id
                    WHERE im.source_asset_id = r.asset_id
                      AND il.review_status = 'finalized'))
              -- bbox 가 요구되면 post_review_clip_job 결과(processed_clips)가
              -- 채워졌어야 함. 그렇지 않으면 부분 빌드 후 잠금 위험.
              AND (dr.outputs NOT LIKE '%bbox%' OR EXISTS (
                    SELECT 1 FROM processed_clips pc
                    JOIN raw_files r2 ON r2.asset_id = pc.source_asset_id
                    WHERE r2.source_unit_name = r.source_unit_name
                      AND pc.process_status = 'completed'))
              -- 이미 빌드된 폴더 제외
              AND NOT EXISTS (
                    SELECT 1 FROM datasets d
                    WHERE d.name = r.source_unit_name
                      AND d.build_status = 'completed')
            ORDER BY folder
            """
        ).fetchall()
        return [row[0] for row in rows]


@sensor(
    job=build_dataset_single_job,
    name="build_dataset_on_finalize_sensor",
    minimum_interval_seconds=int_env("BUILD_DATASET_SENSOR_INTERVAL_SEC", 60, 30),
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "LS 검수 완료(/sync-approve)로 labels.review_status='finalized' 가 된 프로젝트 중 "
        "아직 completed dataset이 없는 folder를 감지해 build_dataset_single_job 트리거."
    ),
)
def build_dataset_on_finalize_sensor(context):
    db_path = default_duckdb_path()
    if not Path(db_path).exists():
        yield SkipReason(f"DuckDB not found: {db_path}")
        return

    try:
        folders = _fetch_projects_ready_to_build(db_path)
    except Exception as exc:
        yield SkipReason(f"DB 조회 실패: {exc}")
        return

    if not folders:
        yield SkipReason("finalize된 신규 빌드 대상 프로젝트 없음")
        return

    context.log.info(f"build_dataset 대상 {len(folders)}건: {folders}")

    for folder in folders:
        yield RunRequest(
            run_key=f"build-dataset:{folder}",
            run_config={
                "ops": {"build_dataset": {"config": {"folder": folder}}},
            },
            tags={
                "folder_name": folder,
                "trigger": "build_dataset_on_finalize_sensor",
            },
        )
