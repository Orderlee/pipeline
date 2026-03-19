"""Staging 전용 spec_resolve_sensor — config 미조회, retry_count, RunRequest 직접 트리거.

명세: config 조회는 3-5에서만. 매칭 시 retry_count+=1, spec_status 유지, auto_labeling_routed_job RunRequest.
definitions_staging에서만 사용.
"""

from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SensorEvaluationContext, sensor

from vlm_pipeline.resources.duckdb import DuckDBResource


@sensor(
    name="spec_resolve_sensor",
    minimum_interval_seconds=60,
    job_name="auto_labeling_routed_job",
    description="[Staging] pending_resolved → ready_for_labeling 전이 후 auto_labeling_routed_job 직접 트리거",
)
def spec_resolve_sensor(
    context: SensorEvaluationContext,
    db: DuckDBResource,
) -> RunRequest | None:
    """Staging: config 미조회, retry_count 증가, spec_status 유지, RunRequest 반환."""
    db.ensure_schema()
    specs = db.list_specs_by_status("pending_resolved")
    if not specs:
        return None

    job_name = "auto_labeling_routed_job"
    instance = context.instance
    for spec in specs:
        spec_id = spec.get("spec_id")
        source_unit_name = spec.get("source_unit_name") or ""
        if not spec_id or not source_unit_name:
            continue
        current_retry_count = int(spec.get("retry_count") or 0)
        if current_retry_count >= 3:
            db.update_spec_status(spec_id, "failed", last_error="retry_limit_exceeded")
            context.log.warning(
                f"spec_resolve_sensor: spec_id={spec_id} retry_limit_exceeded({current_retry_count})"
            )
            continue
        if _has_run_for_spec(instance, job_name, spec_id):
            context.log.info(f"spec_resolve_sensor: spec_id={spec_id} already in flight, skip")
            continue

        with db.connect() as conn:
            if not db._table_exists(conn, "raw_files") or "spec_id" not in db._table_columns(
                conn, "raw_files"
            ):
                continue
            rows = conn.execute(
                """
                SELECT asset_id FROM raw_files
                WHERE media_type = 'video'
                  AND COALESCE(source_unit_name, '') = ?
                  AND (
                        (
                            ingest_status IN ('completed', 'pending_spec')
                            AND COALESCE(spec_id, '') IN ('', ?)
                        )
                        OR (ingest_status = 'ready_for_labeling' AND spec_id = ?)
                  )
                """,
                [source_unit_name, spec_id, spec_id],
            ).fetchall()
        if not rows:
            continue

        updates = [
            {"asset_id": r[0], "ingest_status": "ready_for_labeling", "spec_id": spec_id}
            for r in rows
        ]
        db.batch_update_spec_and_status(updates)
        retry_count = db.increment_spec_retry_count(spec_id)
        if retry_count >= 3:
            db.update_spec_status(spec_id, "failed", last_error="retry_limit_exceeded")
            context.log.warning(
                f"spec_resolve_sensor: spec_id={spec_id} retry_count={retry_count} -> failed"
            )
            continue

        method = spec.get("labeling_method") or []
        if not isinstance(method, list):
            method = []
        context.log.info(
            f"spec_resolve_sensor: spec_id={spec_id} source_unit_name={source_unit_name} "
            f"ready_for_labeling {len(updates)} rows retry_count={retry_count}"
        )
        requested_outputs = "_".join(method) if method else "timestamp"
        return RunRequest(
            run_key=f"spec_resolve_{spec_id}_{retry_count}",
            tags={
                "spec_id": spec_id,
                "source_unit_name": source_unit_name,
                "requested_outputs": requested_outputs,
            },
        )
    return None


def _has_run_for_spec(instance: DagsterInstance, job_name: str, spec_id: str) -> bool:
    runs = instance.get_runs(limit=20)
    for run in runs:
        if run.job_name != job_name:
            continue
        if run.status.value in ("QUEUED", "STARTED") and run.tags.get("spec_id") == spec_id:
            return True
    return False
