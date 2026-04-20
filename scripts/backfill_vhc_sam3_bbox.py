"""VanguardHealthCare(VHC) — SAM3 bbox backfill 일회성 스크립트.

과거 dispatch run 66a84f99에서 SAM3 server 미준비로 bbox detection이 silent skip된
288개 raw_video_frame 이미지를 SAM3 primary 엔진으로 재처리한다.

사용법 (dagster 컨테이너 내부에서 실행):
    docker exec docker-dagster-1 python3 /src/vlm/../scripts/backfill_vhc_sam3_bbox.py

또는 repo root에서:
    docker cp scripts/backfill_vhc_sam3_bbox.py docker-dagster-1:/tmp/ && \
    docker exec docker-dagster-1 python3 /tmp/backfill_vhc_sam3_bbox.py

이 스크립트는:
- DB: /data/pipeline.duckdb (컨테이너 내부 경로)
- MinIO: docker-compose 네트워크의 minio:9000
- SAM3: docker-compose 네트워크의 sam3:8002

사전 조건:
- sam3 컨테이너 healthy
- ENABLE_SAM3_DETECTION=true (or SAM3_OUTPUTS 요청 — 본 스크립트는 요청 우회)
"""

from __future__ import annotations

import os
import sys
from types import SimpleNamespace

# Dagster 컨테이너에서 모듈 경로 보장.
sys.path.insert(0, "/src/vlm")

from vlm_pipeline.defs.sam.detection_assets import _run_sam3_image_detection
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


# raw_files.source_unit_name은 "VanguardHealthCare(VHC)"지만 raw_key는 sanitize되어
# lowercase로 저장된다 ("vanguardhealthcarevhc/..."). SAM3 asset의 find_pending_images가
# raw_key LIKE prefix 매칭을 쓰므로 folder_name_override는 sanitized 형태를 써야 한다.
FOLDER_NAME = "vanguardhealthcarevhc"


class _StdoutLog:
    def info(self, msg: str) -> None:
        print(f"[INFO] {msg}", flush=True)

    def warning(self, msg: str) -> None:
        print(f"[WARN] {msg}", flush=True)

    def error(self, msg: str) -> None:
        print(f"[ERROR] {msg}", flush=True)

    def debug(self, msg: str) -> None:
        pass


class _Ctx:
    """Lightweight context stand-in for _run_sam3_image_detection."""

    def __init__(self) -> None:
        self.op_config = {
            "limit": int(os.environ.get("BACKFILL_LIMIT", "1000")),
            "score_threshold": 0.0,
            "max_masks_per_prompt": 50,
        }
        # requested_outputs에 bbox 포함해 SAM3_OUTPUTS 게이트 통과.
        # folder_name_original은 folder_name_override로 직접 전달하므로 태그 필수 아님.
        self.run = SimpleNamespace(
            tags={
                "folder_name_original": FOLDER_NAME,
                "requested_outputs": "bbox",
                "classes": os.environ.get(
                    "BACKFILL_CLASSES",
                    "person,car,truck,bus,motorcycle,bicycle,fire,smoke,flame,"
                    "cigarette,smoking,knife,gun,bat,bag,backpack,suitcase,"
                    "helmet,traffic cone,barricade,dog,cat",
                ),
            },
        )
        self.log = _StdoutLog()

    def add_output_metadata(self, metadata: dict) -> None:
        print(f"[METADATA] {metadata}", flush=True)


def main() -> int:
    os.environ.setdefault("ENABLE_SAM3_DETECTION", "true")

    db = DuckDBResource(db_path=os.environ.get("DATAOPS_DUCKDB_PATH", "/data/pipeline.duckdb"))
    minio = MinIOResource(
        endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        access_key=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
    )

    ctx = _Ctx()
    print(f"[BACKFILL] folder={FOLDER_NAME} SAM3 bbox 시작", flush=True)

    summary = _run_sam3_image_detection(
        ctx,
        db,
        minio,
        folder_name_override=FOLDER_NAME,
    )
    print(f"[BACKFILL] 완료: {summary}", flush=True)
    return 0 if summary.get("failed", 0) == 0 or summary.get("processed", 0) > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
