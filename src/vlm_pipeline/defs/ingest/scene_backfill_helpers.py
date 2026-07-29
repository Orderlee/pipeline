"""scene_backfill 순수 헬퍼 — dagster 의존 없음.

Layer 3: lib + resources 만 import 가능. dagster import 금지.
env_backfill_helpers.py 구조를 그대로 복제하되, 두 개의 완전히 독립된 라벨러를 각각
호출한다(camera-angle-grouping-2026-07-29.md 설계 이후 확장):
  - Gemini 2.5 Flash 5축(subject_scale/occlusion_state/environment_type/daynight_type/
    weather) — provenance 컬럼은 env_method.
  - DAv2(Depth Anything V2-S + 바닥평면 피팅) 서비스 전용 camera_angle 1축 — provenance
    컬럼은 angle_method. (2026-07-29 실측 GT 98편 plan-vs-rest AUC 0.947 — 같은 조건에서
    Gemini 는 오검출 26/94 로 lib/video_angle_dav2.py 로 분리했다.)

두 라벨러는 실패가 서로 전파되지 않는다 — 한쪽이 예외를 던지거나 프레임 추출에 실패해도
다른 쪽 결과는 그대로 기록되고, 실패한 쪽만 자기 provenance 컬럼이 'deferred'(재시도 대기)
또는 'deferred_no_frames'/'deferred_missing_archive'(터미널 마커)로 남는다.

각 라벨러는 자기 프레임을 독립적으로 ffmpeg 로 추출한다 — 영상당 ffmpeg 2회는 허용 비용이며,
공유 캐시를 두면 두 lib 모듈(video_scene.py / video_angle_dav2.py) 사이에 결합이 생겨
만들지 않는다.

design: docs/design-docs/camera-angle-grouping-2026-07-29.md §3.2, §7.
"""

from __future__ import annotations

import os
from typing import Any, Callable

from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.lib.video_angle_dav2 import classify_camera_angle
from vlm_pipeline.lib.video_scene import classify_video_scene

OUTCOME_SKIP = "skip"
OUTCOME_DONE = "done"
OUTCOME_FAILED = "failed"

# update_video_scene() 의 8-kwarg 계약 — 이 튜플이 두 라벨러 결과를 병합한 dict 의 key 전집이다.
_SCENE_KEYS = (
    "camera_angle",
    "subject_scale",
    "occlusion_state",
    "environment_type",
    "daynight_type",
    "weather",
    "env_method",
    "angle_method",
)
# env 그룹(Gemini) — env_method 가 provenance. camera_angle 은 이 그룹에 없다(DAv2 단독 소유).
_ENV_AXIS_KEYS = ("subject_scale", "occlusion_state", "environment_type", "daynight_type", "weather")


def _terminal_marker_result(video: dict[str, Any], marker: str) -> dict[str, Any]:
    """archive_path 자체가 없어 두 라벨러 모두 시도조차 못할 때 쓸 8필드 dict.

    실제로 'deferred' 였던 그룹만 터미널 마커로 전환한다 — find_deferred_scene_videos 는
    angle_method='deferred' OR env_method='deferred' 인 행을 함께 선택하므로, 둘 중 하나만
    deferred 인 행(예: env_method 는 이미 채워졌지만 angle_method 만 deferred)이 archive
    유실로 SKIP 되면 이미 채워져 있던 쪽까지 무조건 None 으로 덮어써 기존 데이터를 파괴하면
    안 된다.
    """
    angle_was_deferred = video.get("angle_method") == "deferred"
    env_was_deferred = video.get("env_method") == "deferred"
    result = {key: video.get(key) for key in _SCENE_KEYS}
    if angle_was_deferred:
        result["camera_angle"] = None
        result["angle_method"] = marker
    if env_was_deferred:
        for key in _ENV_AXIS_KEYS:
            result[key] = None
        result["env_method"] = marker
    return result


def process_one_video(
    video: dict[str, Any],
    *,
    qwen_classify_fn: Callable[..., dict[str, Any]] = classify_video_scene,
    angle_classify_fn: Callable[..., dict[str, Any]] = classify_camera_angle,
    path_exists_fn: Callable[[str], bool] = os.path.exists,
) -> tuple[str, dict[str, Any] | None, str | None]:
    """단일 비디오에 Qwen 5축 + Gemini camera_angle 을 각각 독립적으로 수행.

    이미 처리 완료된 그룹(method != 'deferred')은 애초에 재호출하지 않는다 — 예를 들어
    env_method 만 deferred 인 행은 Qwen 만 호출하고 Gemini 는 건드리지 않는다.

    Returns:
        (outcome, scene_result, error_msg)
        outcome: OUTCOME_SKIP / OUTCOME_DONE / OUTCOME_FAILED
        scene_result: update_video_scene() 의 8개 kwarg 이름과 정확히 일치하는 키를 갖는
            dict, 또는 (OUTCOME_FAILED 시) None. 처리하지 않았거나 실패한 그룹은 video 의
            현재값을 그대로 되돌려 덮어쓰기를 막는다.
        error_msg: 발생한 실패/스킵 사유를 "qwen: ...; gemini: ..." 형태로 결합(둘 다
            실패한 경우 둘 다 포함). 아무 문제 없었으면 None.

    아웃컴 판정:
        - OUTCOME_DONE: 최소 한 그룹이 실제로 새 값을 얻었다(성공 또는 터미널 마커 포함,
          다른 그룹이 예외로 실패해도 무관 — "실패 독립성"의 핵심).
        - OUTCOME_SKIP: 새로 성공한 그룹은 없지만 터미널 마커(no_frames/missing_archive)
          로 기록할 것은 있다.
        - OUTCOME_FAILED: 아무것도 기록할 게 없다(양쪽 다 예외, 또는 애초에 처리 대상이
          없어 방어적으로 떨어진 경우) — 행은 그대로 'deferred' 로 남아 다음 tick 재시도.
    """
    archive_path = video.get("archive_path", "")

    if not path_exists_fn(archive_path):
        return (
            OUTCOME_SKIP,
            _terminal_marker_result(video, "deferred_missing_archive"),
            f"archive_path 없음: {archive_path}",
        )

    merged: dict[str, Any] = {key: video.get(key) for key in _SCENE_KEYS}
    errors: list[str] = []
    any_success = False
    any_terminal = False

    if video.get("env_method") == "deferred":
        frames = int_env("SCENE_FRAMES", 1, minimum=1)
        try:
            qwen_result = qwen_classify_fn(archive_path, frames=frames)
        except Exception as exc:
            errors.append(f"qwen: {exc}")
        else:
            if qwen_result.get("env_method") is None:
                for key in _ENV_AXIS_KEYS:
                    merged[key] = None
                merged["env_method"] = "deferred_no_frames"
                any_terminal = True
                errors.append("qwen: 프레임 추출 불가")
            else:
                for key in _ENV_AXIS_KEYS:
                    merged[key] = qwen_result.get(key)
                merged["env_method"] = qwen_result.get("env_method")
                any_success = True

    if video.get("angle_method") == "deferred":
        try:
            angle_result = angle_classify_fn(archive_path)
        except Exception as exc:
            errors.append(f"gemini: {exc}")
        else:
            if angle_result.get("angle_method") is None:
                merged["camera_angle"] = None
                merged["angle_method"] = "deferred_no_frames"
                any_terminal = True
                errors.append("gemini: 프레임 추출 불가")
            else:
                merged["camera_angle"] = angle_result.get("camera_angle")
                merged["angle_method"] = angle_result.get("angle_method")
                any_success = True

    error_msg = "; ".join(errors) if errors else None

    if any_success:
        return OUTCOME_DONE, merged, error_msg
    if any_terminal:
        return OUTCOME_SKIP, merged, error_msg
    if errors:
        return OUTCOME_FAILED, None, error_msg
    # 방어적 fallback — find_deferred_scene_videos 가 항상 둘 중 하나는 'deferred' 인 행만
    # 반환하므로 실제로는 도달하지 않아야 한다.
    return OUTCOME_SKIP, None, "처리 대상 축 없음(선택 쿼리 불일치 방어)"
