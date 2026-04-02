"""공통 센서 목록 상수 및 빌더."""

from __future__ import annotations

from vlm_pipeline.defs.dispatch.production_agent_sensor import production_agent_dispatch_sensor
from vlm_pipeline.defs.dispatch.sensor import dispatch_sensor
from vlm_pipeline.defs.dispatch.sensor_run_status import (
    dispatch_run_canceled_sensor,
    dispatch_run_failure_sensor,
    dispatch_run_success_sensor,
)
from vlm_pipeline.defs.ingest.sensor import (
    auto_bootstrap_manifest_sensor,
    incoming_manifest_sensor,
    stuck_run_guard_sensor,
)

COMMON_INGEST_SENSORS = (
    incoming_manifest_sensor,
    auto_bootstrap_manifest_sensor,
    stuck_run_guard_sensor,
)

COMMON_DISPATCH_STATUS_SENSORS = (
    dispatch_run_success_sensor,
    dispatch_run_failure_sensor,
    dispatch_run_canceled_sensor,
)


def build_production_sensors(motherduck_table_sensors: list[object] | tuple[object, ...]) -> list[object]:
    return [
        *COMMON_INGEST_SENSORS,
        dispatch_sensor,
        production_agent_dispatch_sensor,
        *COMMON_DISPATCH_STATUS_SENSORS,
        *motherduck_table_sensors,
    ]


def build_staging_sensors(*, spec_resolve_sensor, dispatch_ingress_sensor, dispatch_json_sensor) -> list[object]:
    return [
        *COMMON_INGEST_SENSORS,
        dispatch_json_sensor,
        dispatch_ingress_sensor,
        *COMMON_DISPATCH_STATUS_SENSORS,
        spec_resolve_sensor,
    ]
