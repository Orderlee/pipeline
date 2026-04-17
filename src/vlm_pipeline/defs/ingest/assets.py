"""INGEST @asset — NAS 미디어 → 검증 → 정규화 → MinIO vlm-raw + DuckDB raw_files (facade).

Layer 4: Dagster @asset, 같은 도메인의 ops.py만 import.

실제 구현은 5개 submodule로 분리됨:
- ``ingest_state``           — ``RawIngestState`` dataclass
- ``ingest_manifest_flow``   — manifest 로드 / hydrate / 이동
- ``ingest_archive_flow``    — archive 준비 / compaction
- ``ingest_post``            — NAS health, dispatch 상태 관리, local artifact import
- ``ingest_orchestrate``     — step 함수 + 전체 파이프라인
"""

from __future__ import annotations

from dagster import AssetKey, asset

from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .ingest_post import _build_raw_ingest_state
from .ingest_orchestrate import _run_raw_ingest_pipeline
from .ingest_state import RawIngestState

__all__ = ["RawIngestState", "raw_ingest"]


@asset(
    name="raw_ingest",
    description="NAS 미디어 검증·정규화 → MinIO(vlm-raw) 업로드 + 메타데이터 등록 + pHash 중복 검출",
    group_name="ingest",
    deps=[AssetKey(["pipeline", "incoming_nas"])],
)
def raw_ingest(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """INGEST asset — manifest 기반 미디어 파일 수집.

    sensor에서 run_config tags로 manifest_path를 전달받음.
    manifest가 없으면 DuckDB 상태 요약만 반환.
    """
    config = PipelineConfig()
    state = _build_raw_ingest_state(context)

    context.log.info(
        "raw_ingest entered: "
        f"run_id={context.run.run_id}, request_id={state.request_id or ''}, "
        f"folder={state.folder_name or ''}, manifest_path={state.manifest_path or ''}"
    )
    return _run_raw_ingest_pipeline(
        context,
        db,
        minio,
        config=config,
        state=state,
    )
