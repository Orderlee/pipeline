"""YOLO sensor — YOLO detection 대상 이미지 backlog 감지.

운영 정책 (2026-05-20): **YOLO 비활성, SAM3.1 단독 운영**.
- sensor 는 `env_enable_key="ENABLE_YOLO_DETECTION"` 으로 env 따라 RUNNING/STOPPED 분기.
- prod `.env` 의 ENABLE_YOLO_DETECTION=false 이면 sensor 도 STOPPED.
- 향후 YOLO 재활성 시 env 만 true 로 바꾸면 sensor 도 자동 RUNNING.
"""

from __future__ import annotations

from vlm_pipeline.defs.shared.detection_sensor_factory import build_detection_backlog_sensor

YOLO_TARGET_JOBS = {"yolo_detection_job", "mvp_stage_job"}

yolo_detection_sensor = build_detection_backlog_sensor(
    name="yolo_detection_sensor",
    label_tool="yolo-world",
    job_name="yolo_detection_job",
    target_jobs=YOLO_TARGET_JOBS,
    env_enable_key="ENABLE_YOLO_DETECTION",  # false → STOPPED, true → RUNNING
    env_interval_key="YOLO_SENSOR_INTERVAL_SEC",
    default_interval_sec=120,
    default_running=True,
    description="[SAM3.1 단독 운영 중 — env ENABLE_YOLO_DETECTION=true 시에만 활성] processed_clip_frame 중 YOLO detection 미완료 backlog 감지",
    run_key_prefix="yolo-detect",
    asset_name="yolo_image_detection",
    sensor_limit_env="YOLO_SENSOR_LIMIT",
    default_sensor_limit=200,
)
