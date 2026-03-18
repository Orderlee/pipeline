"""Spec flow sensors — spec_resolve_sensor, ready_for_labeling_sensor.

Staging-only (IS_STAGING). 60초 주기.
"""

from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SensorEvaluationContext, sensor

from vlm_pipeline.resources.duckdb import DuckDBResource


@sensor(
    name="spec_resolve_sensor",
    minimum_interval_seconds=60,
    description="pending_resolved spec → config resolve → raw_files ready_for_labeling, spec active",
)
def spec_resolve_sensor(
    context: SensorEvaluationContext,
    db: DuckDBResource,
) -> RunRequest | None:
    """labeling_specs.spec_status='pending_resolved' 대상으로 config resolve 후 matching raw_files를 ready_for_labeling으로."""
    db.ensure_schema()
    specs = db.list_specs_by_status("pending_resolved")
    if not specs:
        return None

    for spec in specs:
        spec_id = spec.get("spec_id")
        source_unit_name = spec.get("source_unit_name") or ""
        if not spec_id or not source_unit_name:
            continue
        config_id, scope = db.resolve_config_for_requester(
            spec.get("requester_id"), spec.get("team_id")
        )
        if not config_id:
            db.update_spec_status(spec_id, "failed", last_error="config_not_found")
            context.log.warning(f"spec_resolve_sensor: spec_id={spec_id} config_not_found")
            continue
        db.update_spec_resolved_config(spec_id, config_id, scope or "fallback")
        db.update_spec_status(spec_id, "active")

        with db.connect() as conn:
            if not db._table_exists(conn, "raw_files") or "spec_id" not in db._table_columns(
                conn, "raw_files"
            ):
                continue
            rows = conn.execute(
                """
                SELECT asset_id FROM raw_files
                WHERE media_type = 'video'
                  AND ingest_status = 'completed'
                  AND COALESCE(source_unit_name, '') = ?
                  AND (spec_id IS NULL OR spec_id = '')
                """,
                [source_unit_name],
            ).fetchall()
        if not rows:
            continue
        updates = [
            {"asset_id": r[0], "ingest_status": "ready_for_labeling", "spec_id": spec_id}
            for r in rows
        ]
        db.batch_update_spec_and_status(updates)
        context.log.info(
            f"spec_resolve_sensor: spec_id={spec_id} source_unit_name={source_unit_name} "
            f"ready_for_labeling {len(updates)} rows"
        )
    return None


def _has_run_for_spec(instance: DagsterInstance, job_name: str, spec_id: str) -> bool:
    """동일 spec_id로 queued/running run이 있으면 True."""
    # TODO: instance.get_runs with tags filter; 간단히 최근 1분 내 run만 확인
    runs = instance.get_runs(limit=20)
    for run in runs:
        if run.job_name != job_name:
            continue
        if run.status.value in ("QUEUED", "STARTED") and run.tags.get("spec_id") == spec_id:
            return True
    return False


@sensor(
    name="ready_for_labeling_sensor",
    minimum_interval_seconds=60,
    description="ready_for_labeling 그룹별 auto_labeling_routed_job 실행",
)
def ready_for_labeling_sensor(
    context: SensorEvaluationContext,
    db: DuckDBResource,
) -> RunRequest | None:
    """raw_files.ingest_status='ready_for_labeling' spec 단위로 auto_labeling_routed_job 실행."""
    db.ensure_schema()
    with db.connect() as conn:
        if not db._table_exists(conn, "raw_files") or "spec_id" not in db._table_columns(
            conn, "raw_files"
        ):
            return None
        if not db._table_exists(conn, "labeling_specs"):
            return None
        rows = conn.execute(
            """
            SELECT r.spec_id, MAX(r.source_unit_name) AS source_unit_name,
                   MAX(s.resolved_config_id) AS resolved_config_id,
                   MAX(s.resolved_config_scope) AS resolved_config_scope
            FROM raw_files r
            JOIN labeling_specs s ON s.spec_id = r.spec_id
            WHERE r.ingest_status = 'ready_for_labeling'
              AND r.media_type = 'video'
              AND r.spec_id IS NOT NULL AND r.spec_id <> ''
            GROUP BY r.spec_id
            LIMIT 5
            """
        ).fetchall()
    if not rows:
        return None

    job_name = "auto_labeling_routed_job"
    instance = context.instance
    for row in rows:
        spec_id, source_unit_name, resolved_config_id, resolved_config_scope = (
            row[0], row[1], row[2], row[3]
        )
        if not spec_id:
            continue
        spec = db.get_labeling_spec_by_id(spec_id)
        if not spec:
            continue
        if (spec.get("retry_count") or 0) >= 3:
            db.update_spec_status(spec_id, "failed", last_error="retry_limit_exceeded")
            continue
        if _has_run_for_spec(instance, job_name, spec_id):
            context.log.info(f"ready_for_labeling_sensor: spec_id={spec_id} already in flight, skip")
            continue
        method = spec.get("labeling_method") or []
        if not isinstance(method, list):
            method = []
        requested_outputs = "_".join(method) if method else "timestamp"
        db.increment_spec_retry_count(spec_id)
        return RunRequest(
            job_name="auto_labeling_routed_job",
            run_key=f"ready_label_{spec_id}_{context.cursor or '0'}",
            tags={
                "spec_id": spec_id,
                "source_unit_name": source_unit_name or "",
                "resolved_config_id": resolved_config_id or "",
                "requested_outputs": requested_outputs,
            },
        )
    return None
