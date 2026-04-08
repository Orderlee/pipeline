"""YOLO sensor — YOLO detection 대상 이미지 backlog 감지."""

from __future__ import annotations

from vlm_pipeline.lib.detection_sensor_factory import build_detection_backlog_sensor

YOLO_TARGET_JOBS = {"yolo_detection_job", "mvp_stage_job"}

yolo_detection_sensor = build_detection_backlog_sensor(
    name="yolo_detection_sensor",
    label_tool="yolo-world",
    job_name="yolo_detection_job",
    target_jobs=YOLO_TARGET_JOBS,
    env_interval_key="YOLO_SENSOR_INTERVAL_SEC",
    default_interval_sec=120,
    default_running=True,
    description="processed_clip_frame 중 YOLO detection 미완료 backlog 감지 시 yolo_detection_job 실행",
    run_key_prefix="yolo-detect",
)
