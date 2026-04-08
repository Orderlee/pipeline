"""SAM3 sensor — SAM3 bbox detection 대상 이미지 backlog 감지."""

from __future__ import annotations

from vlm_pipeline.lib.detection_sensor_factory import build_detection_backlog_sensor

SAM3_TARGET_JOBS = {"sam3_detection_job", "dispatch_stage_job"}

sam3_detection_sensor = build_detection_backlog_sensor(
    name="sam3_detection_sensor",
    label_tool="sam3",
    job_name="sam3_detection_job",
    target_jobs=SAM3_TARGET_JOBS,
    env_enable_key="ENABLE_SAM3_DETECTION",
    env_interval_key="SAM3_SENSOR_INTERVAL_SEC",
    default_interval_sec=120,
    default_running=True,
    description="processed_clip_frame 중 SAM3 bbox detection 미완료 backlog 감지 시 sam3_detection_job 실행",
    run_key_prefix="sam3-detect",
)
