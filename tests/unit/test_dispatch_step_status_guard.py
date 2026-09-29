"""Regression: close_dispatch_request must not fake-complete a never-started step.

과거 버그: asset이 silent return하고 step_status='pending'/started_at=NULL 상태에서
Dagster run이 SUCCESS로 끝나면 success sensor가 close_dispatch_request('completed')를 호출해
pending 행들을 일괄 'completed'로 도장찍어 버림. 결과적으로 '돌았다고 믿었는데 실제로는 안 돈'
상태가 DB에 그대로 가려져 bbox_status=pending + image_labels=0이 보이지 않음.

가드: started_at IS NULL이면 run 성공 여부와 무관하게 'skipped'로 기록.
"""

from __future__ import annotations

from datetime import datetime


def _make_pending_row(request_id: str, step_name: str, step_order: int) -> dict:
    return {
        "run_id": f"run-{step_name}",
        "request_id": request_id,
        "folder_name": "UnitTestFolder",
        "step_name": step_name,
        "step_order": step_order,
        "step_status": "pending",
        "model_name": None,
        "model_version": None,
        "applied_params": None,
    }


def test_close_dispatch_request_marks_never_started_steps_as_skipped(db_resource):
    """started_at IS NULL인 pending step은 run SUCCESS 이후에도 'skipped'로 남아야 한다."""
    request_id = "req-guard-1"

    db_resource.insert_dispatch_request(
        {
            "request_id": request_id,
            "folder_name": "UnitTestFolder",
            "run_mode": "",
            "outputs": "bbox",
            "labeling_method": "bbox",
            "categories": "",
            "classes": "",
            "image_profile": "current",
            "status": "running",
            "archive_pending_path": None,
            "archive_path": "/tmp/archive/UnitTestFolder",
            "max_frames_per_video": None,
            "jpeg_quality": None,
            "confidence_threshold": None,
            "iou_threshold": None,
            "requested_by": None,
            "requested_at": None,
            "processed_at": datetime.now(),
        }
    )
    db_resource.insert_dispatch_pipeline_runs(
        [
            _make_pending_row(request_id, "archive_move", 1),
            _make_pending_row(request_id, "frame_extract", 2),
            _make_pending_row(request_id, "yolo_detect", 3),
        ]
    )

    # frame_extract 만 실제로 돌았다고 가정 — started_at/completed_at 기록.
    started_at = datetime.now()
    db_resource.update_dispatch_pipeline_step(
        request_id=request_id,
        step_name="frame_extract",
        step_status="completed",
        started_at=started_at,
        completed_at=started_at,
    )

    # Dagster run이 SUCCESS로 종료 → success sensor가 close_dispatch_request 호출.
    db_resource.close_dispatch_request(request_id, status="completed")

    with db_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT step_name, step_status, started_at, completed_at, error_message
                FROM dispatch_pipeline_runs
                WHERE request_id = %s
                ORDER BY step_order
                """,
                (request_id,),
            )
            rows = cur.fetchall()

    by_name = {row[0]: row for row in rows}

    # 실제로 돈 step은 completed 유지.
    assert by_name["frame_extract"][1] == "completed"
    assert by_name["frame_extract"][2] is not None  # started_at

    # 시작도 못 한 step은 'completed'로 도장 찍히면 안 되고 'skipped'로 남아야 한다.
    for step in ("archive_move", "yolo_detect"):
        assert (
            by_name[step][1] == "skipped"
        ), f"{step} started_at=NULL인데 '{by_name[step][1]}'로 마킹됨 — guard 작동 안 함"
        assert by_name[step][2] is None  # started_at는 여전히 NULL
        assert by_name[step][4] == "never_started"
