"""PG video_metadata 카메라 씬 6축 컬럼 CRUD 검증 — env_backfill 대응 PG fixture 패턴.

design: docs/design-docs/camera-angle-grouping-2026-07-29.md §3.1, §3.2.
PG 백엔드 필요 (``DATAOPS_TEST_POSTGRES_DSN``). 미설정/접속불가 시 skip (conftest).
"""

from __future__ import annotations

from typing import Any

_SCENE_COLUMNS = (
    "camera_angle",
    "subject_scale",
    "occlusion_state",
    "environment_type",
    "daynight_type",
    "weather",
    "env_method",
    "angle_method",
)


def _seed_video(db: Any, asset_id: str, folder: str, *, archive_path: str | None = "SET") -> None:
    resolved_archive = f"/nas/archive/{folder}/{asset_id}.mp4" if archive_path == "SET" else archive_path
    db.insert_raw_files_batch(
        [
            {
                "asset_id": asset_id,
                "media_type": "video",
                "ingest_status": "completed",
                "source_path": f"/nas/incoming/{folder}/{asset_id}.mp4",
                "raw_key": f"{folder}/{asset_id}.mp4",
                "raw_bucket": "vlm-raw",
                "source_unit_name": folder,
                "original_name": f"{asset_id}.mp4",
                "checksum": f"sum-{asset_id}",
                "archive_path": resolved_archive,
            }
        ]
    )


def _scene_row(db: Any, asset_id: str) -> tuple:
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT camera_angle, subject_scale, occlusion_state, environment_type, "
                "daynight_type, weather, env_method, angle_method "
                "FROM video_metadata WHERE asset_id = %s",
                (asset_id,),
            )
            return cur.fetchone()


def test_insert_video_metadata_defaults_angle_method_to_deferred(db_resource) -> None:
    """video_loader.py 처럼 angle 컬럼을 전혀 모르는 caller 도 자동으로 'deferred' 큐에 등록된다
    (insert_video_metadata 의 meta.get("angle_method", "deferred") 기본값 — design §3.2)."""
    _seed_video(db_resource, "sc-1", "f1")
    db_resource.insert_video_metadata("sc-1", {"duration_sec": 10.0, "fps": 25.0, "frame_count": 250})

    row = _scene_row(db_resource, "sc-1")
    camera_angle, subject_scale, occlusion_state, environment_type, daynight_type, weather, env_method, angle_method = (
        row
    )
    assert angle_method == "deferred"
    assert camera_angle is None
    assert subject_scale is None
    assert occlusion_state is None
    assert weather is None
    assert environment_type is None
    assert daynight_type is None
    assert env_method is None


def test_insert_video_metadata_respects_explicit_scene_fields(db_resource) -> None:
    """weather 를 포함한 씬 6축 전체가 insert 시점에 그대로 저장되는지 (weather 는 이번에
    새로 추가된 컬럼이라 별도 검증 필요)."""
    _seed_video(db_resource, "sc-2", "f1")
    db_resource.insert_video_metadata(
        "sc-2",
        {
            "duration_sec": 5.0,
            "camera_angle": "oblique_view",
            "subject_scale": "subject_legible",
            "occlusion_state": "unoccluded",
            "environment_type": "outdoor",
            "daynight_type": "day",
            "weather": "clear",
            "env_method": "qwen2.5-vl",
            "angle_method": "qwen2.5-vl",
        },
    )
    assert _scene_row(db_resource, "sc-2") == (
        "oblique_view",
        "subject_legible",
        "unoccluded",
        "outdoor",
        "day",
        "clear",
        "qwen2.5-vl",
        "qwen2.5-vl",
    )


def test_find_deferred_scene_videos_ignores_frame_extract_count(db_resource) -> None:
    """env 와 달리 frame_extract_count 조건이 없다 — 0/미설정이어도 대상에 포함돼야 한다
    (씬 분류는 원본에서 직접 프레임을 뽑으므로 frame_extract stage 완료에 의존하지 않음)."""
    _seed_video(db_resource, "sc-3", "f2")
    db_resource.insert_video_metadata("sc-3", {"duration_sec": 3.0, "frame_extract_count": 0})

    got = {row["asset_id"] for row in db_resource.find_deferred_scene_videos(limit=100)}
    assert "sc-3" in got
    assert db_resource.count_deferred_scene_videos() == len(db_resource.find_deferred_scene_videos(limit=1000))


def test_find_deferred_scene_videos_excludes_missing_archive_path(db_resource) -> None:
    _seed_video(db_resource, "sc-4", "f2", archive_path=None)
    db_resource.insert_video_metadata("sc-4", {"duration_sec": 3.0})

    got = {row["asset_id"] for row in db_resource.find_deferred_scene_videos(limit=100)}
    assert "sc-4" not in got


def test_find_deferred_scene_videos_excludes_already_classified(db_resource) -> None:
    """angle_method/env_method 둘 다 deferred 를 벗어나면 대상에서 빠진다."""
    _seed_video(db_resource, "sc-5", "f2")
    db_resource.insert_video_metadata("sc-5", {"duration_sec": 3.0})
    db_resource.update_video_scene(
        "sc-5",
        camera_angle="level_view",
        subject_scale="subject_marginal",
        occlusion_state="truncated",
        environment_type="indoor",
        daynight_type="night",
        weather="not_applicable",
        env_method="qwen2.5-vl",
        angle_method="qwen2.5-vl",
    )

    got = {row["asset_id"] for row in db_resource.find_deferred_scene_videos(limit=100)}
    assert "sc-5" not in got


def test_find_deferred_scene_videos_includes_angle_deferred_alone(db_resource) -> None:
    """env_method 는 이미 채워졌지만(레거시 Places365 등) angle_method 만 deferred 인 행도 포함."""
    _seed_video(db_resource, "sc-6", "f3")
    db_resource.insert_video_metadata(
        "sc-6",
        {
            "duration_sec": 3.0,
            "environment_type": "outdoor",
            "daynight_type": "day",
            "env_method": "places365_cuda",
        },
    )

    got = {row["asset_id"] for row in db_resource.find_deferred_scene_videos(limit=100)}
    assert "sc-6" in got


def test_find_deferred_scene_videos_includes_env_deferred_alone(db_resource) -> None:
    """camera_angle 계열은 이미 채워졌지만 env_method 만 deferred 인 행도 포함(대칭 케이스)."""
    _seed_video(db_resource, "sc-7", "f3")
    db_resource.insert_video_metadata("sc-7", {"duration_sec": 3.0})
    db_resource.update_video_scene(
        "sc-7",
        camera_angle="oblique_view",
        subject_scale="subject_legible",
        occlusion_state="unoccluded",
        environment_type=None,
        daynight_type=None,
        weather=None,
        env_method="deferred",
        angle_method="qwen2.5-vl",
    )

    got = {row["asset_id"] for row in db_resource.find_deferred_scene_videos(limit=100)}
    assert "sc-7" in got


def test_find_deferred_scene_videos_returns_current_values_for_terminal_marker_merge(db_resource) -> None:
    """scene_backfill_helpers._terminal_marker_result 가 기존 값을 보존하려면
    find_deferred_scene_videos 가 현재 값을 함께 반환해야 한다."""
    _seed_video(db_resource, "sc-8", "f3")
    db_resource.insert_video_metadata(
        "sc-8",
        {
            "duration_sec": 3.0,
            "environment_type": "indoor",
            "daynight_type": "night",
            "env_method": "heuristic",
        },
    )

    rows = {row["asset_id"]: row for row in db_resource.find_deferred_scene_videos(limit=100)}
    row = rows["sc-8"]
    assert row["environment_type"] == "indoor"
    assert row["daynight_type"] == "night"
    assert row["env_method"] == "heuristic"
    assert row["angle_method"] == "deferred"


def test_update_video_scene_writes_all_eight_fields(db_resource) -> None:
    _seed_video(db_resource, "sc-9", "f4")
    db_resource.insert_video_metadata("sc-9", {"duration_sec": 3.0})

    db_resource.update_video_scene(
        "sc-9",
        camera_angle="plan_view",
        subject_scale="not_applicable",
        occlusion_state="truncated",
        environment_type="outdoor",
        daynight_type="night",
        weather="fog",
        env_method="qwen2.5-vl",
        angle_method="qwen2.5-vl",
    )
    assert _scene_row(db_resource, "sc-9") == (
        "plan_view",
        "not_applicable",
        "truncated",
        "outdoor",
        "night",
        "fog",
        "qwen2.5-vl",
        "qwen2.5-vl",
    )


def test_update_video_scene_writes_terminal_marker(db_resource) -> None:
    """scene_backfill 의 터미널 마커 기록 경로 — 6축은 None 유지, 두 method 만 마커.
    마커 기록 이후 이 행은 deferred selection bucket 에서 빠져야 무한 재시도를 막는다."""
    _seed_video(db_resource, "sc-10", "f4")
    db_resource.insert_video_metadata("sc-10", {"duration_sec": 3.0})

    db_resource.update_video_scene(
        "sc-10",
        camera_angle=None,
        subject_scale=None,
        occlusion_state=None,
        environment_type=None,
        daynight_type=None,
        weather=None,
        env_method="deferred_no_frames",
        angle_method="deferred_no_frames",
    )
    assert _scene_row(db_resource, "sc-10") == (
        None,
        None,
        None,
        None,
        None,
        None,
        "deferred_no_frames",
        "deferred_no_frames",
    )
    got = {row["asset_id"] for row in db_resource.find_deferred_scene_videos(limit=100)}
    assert "sc-10" not in got, "터미널 마커 이후에도 deferred selection bucket 에 남아있으면 무한 재시도"
