"""Spec flow — labeling_spec_ingest, config_sync, ingest_router, sensors.

- assets/sensor: 운영용 (라우터·센서에서 config 조회, ready_for_labeling_sensor가 job 트리거).
- staging_assets/staging_sensor: Staging 전용 (명세 준수 — config 미조회, spec_resolve가 직접 트리거).
  definitions_staging에서만 staging_* 사용.
"""
