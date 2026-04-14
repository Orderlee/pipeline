"""Spec flow — labeling_spec_ingest, config_sync, ingest_router, sensors.

- assets/sensor: 운영용 (라우터·센서에서 config 조회, ready_for_labeling_sensor가 job 트리거).
- staging_assets/staging_sensor: legacy spec 호환 경로 (config 미조회, spec_resolve가 직접 트리거).
  현재 기본 prod/test runtime에는 연결하지 않는다.
"""
