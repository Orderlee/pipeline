"""camera_angle 전용 DAv2(Depth Anything V2-S + 바닥평면 피팅) HTTP 서비스 클라이언트.

design 배경: docs/design-docs/camera-angle-grouping-2026-07-29.md.

실측(2026-07-29, 사람이 프레임을 직접 채점한 GT 98편): plan-vs-rest AUC 0.947 로 벤치마크
최고 — 데이터셋 B(94편, GT plan 0편)에서 오검출 0/94(같은 조건에서 Gemini 2.5 Flash 단일축
전용은 26/94 오검출). `level_view`/`oblique_view` 는 어떤 모델도 분리하지 못했으므로(level
AUC ≤ 0.68) 폐기하고 `plan_view`|`non_plan`|`indeterminate` 만 산출한다 — 서비스
(docker/angle/app.py)가 깊이맵 기반 바닥평면 피팅으로 이 값을 직접 계산해서 반환하므로, 이
클라이언트는 (구 Gemini 버전과 달리) 텍스트 파싱/화이트리스트 매칭을 하지 않는다.

따라서 camera_angle 은 이 모듈이 전담한다 — lib/video_scene.py(Gemini 2.5 Flash)는 나머지
5축(subject_scale/occlusion_state/environment_type/daynight_type/weather)만 담당하고 이
축은 반환하지 않는다.

video_scene.py 와 동일한 안전 fallback 규약을 따르되, ffmpeg 프레임 추출은 이 모듈이
독립적으로 수행한다 — 공유 캐시를 두면 두 lib 모듈 사이에 결합이 생기므로 만들지 않는다
(영상당 ffmpeg 2회는 허용 비용이다):
- 프레임 추출 자체가 불가능하면 2키(camera_angle/angle_method) 를 전부 None 으로 반환한다
  (호출자가 이를 터미널 마커로 해석해 'deferred' selection bucket 에서 빼도록 함).
- HTTP 호출이 실패(네트워크/타임아웃/5xx)하면 예외를 그대로 전파한다 — 호출자가 예외를
  잡아 'deferred' 유지 + 재시도로 처리해야 한다(scene_backfill_helpers.process_one_video
  패턴과 동일).
- 서비스가 판정 근거 부족 시 예외 대신 200 + ``camera_angle="indeterminate"`` 를 반환한다
  — 이는 정상 응답이므로 예외로 취급하지 않고 그대로 반환한다.

``angle_method`` 는 서비스가 반환한 값을 그대로 저장한다(예: ``"dav2-s+plane"``,
``"dav2-s+plane:평면 피팅용 포인트 부족"``) — plan/non_plan 임계값(``PLAN_THRESHOLD_DEG``)은
서비스 소관이므로 클라이언트에서 재판정하지 않는다.
"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.request
import uuid
from functools import lru_cache
from pathlib import Path
from typing import Any

DEFAULT_ANGLE_API_URL = "http://angle-dav2-1:8000"
DEFAULT_ANGLE_TIMEOUT_SEC = 300.0


@lru_cache(maxsize=1)
def _ffmpeg_available() -> bool:
    try:
        proc = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5, check=False)
        return proc.returncode == 0
    except Exception:
        return False


def _extract_frame_bytes(video_path: str, seek_sec: float, timeout_sec: int = 60) -> bytes | None:
    """지정 시점 1프레임을 mjpeg bytes 로 추출. 실패 시 None (예외 전파하지 않음)."""
    if not _ffmpeg_available():
        return None
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-loglevel",
        "error",
        "-ss",
        f"{seek_sec:.3f}",
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        "-f",
        "image2",
        "-c:v",
        "mjpeg",
        "pipe:1",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=timeout_sec, check=False)
    except Exception:
        return None
    if proc.returncode != 0 or not proc.stdout:
        return None
    return proc.stdout


def _extract_single_frame(video_path: str | Path) -> bytes | None:
    """2s 지점 우선, 실패 시 0s fallback. 1장만 추출한다 — 다중프레임은 실측에서 더 나빴다."""
    path_str = str(video_path)
    frame = _extract_frame_bytes(path_str, 2.0)
    if frame is not None:
        return frame
    return _extract_frame_bytes(path_str, 0.0)


def _build_multipart_body(field_name: str, filename: str, content_type: str, data: bytes) -> tuple[bytes, str]:
    """단일 파일 필드 ``multipart/form-data`` 본문을 stdlib 만으로 구성한다 (requests 미사용,
    lib 계층 의존성 추가 금지).
    """
    boundary = uuid.uuid4().hex
    segments = [
        f"--{boundary}".encode(),
        f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"'.encode(),
        f"Content-Type: {content_type}".encode(),
        b"",
        data,
        f"--{boundary}--".encode(),
        b"",
    ]
    return b"\r\n".join(segments), boundary


def _call_dav2(api_url: str, frame_bytes: bytes, *, timeout: float) -> dict[str, Any]:
    """DAv2 서비스 ``POST /angle`` 1회 호출. 실패 시 예외를 그대로 전파한다.

    ``urllib.request`` 는 ``urlopen(timeout=...)`` 로 wall-clock timeout 을 직접 지원하므로
    (Vertex SDK 와 달리) ThreadPoolExecutor 로 감쌀 필요가 없다.
    """
    body, boundary = _build_multipart_body("file", "frame.jpg", "image/jpeg", frame_bytes)
    req = urllib.request.Request(
        api_url.rstrip("/") + "/angle",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def classify_camera_angle(
    video_path: str | Path,
    *,
    api_url: str | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    """camera_angle 단일축을 DAv2(Depth Anything V2-S + 바닥평면 피팅) 서비스로 판정.

    실측(2026-07-29, GT 98편): plan-vs-rest AUC 0.947, 데이터셋 B(94편, GT plan 0편)
    오검출 0/94 — 같은 조건 Gemini 2.5 Flash(26/94 오검출)보다 우수해 채택(모듈 docstring
    참고). video_scene.py(Gemini) 는 나머지 5축만 담당하고 camera_angle 은 이 함수가
    전담한다.

    Args:
        video_path: 원본(archive) 비디오 경로.
        api_url: DAv2 서비스 base URL. 미지정 시 ``ANGLE_API_URL`` env
            (기본 http://angle-dav2-1:8000).
        timeout: HTTP 요청 wall-clock timeout(초). 미지정 시 300초.

    Returns:
        {"camera_angle", "angle_method"} 2키 dict.
        프레임 추출이 완전히 실패하면 둘 다 None (호출자가 이를 터미널 마커로 해석해
        'deferred' selection bucket 에서 빼도록 함). HTTP 호출 실패는 예외로 그대로
        전파한다 — 호출자가 'deferred' 유지 + 재시도로 처리해야 한다. 서비스가 판정 근거
        부족으로 ``camera_angle="indeterminate"`` 를 200 으로 주면 정상 응답이므로 그대로
        반환한다(예외 아님).
    """
    resolved_api_url = api_url or os.environ.get("ANGLE_API_URL", DEFAULT_ANGLE_API_URL)
    resolved_timeout = timeout if timeout is not None else DEFAULT_ANGLE_TIMEOUT_SEC

    frame_bytes = _extract_single_frame(video_path)
    if frame_bytes is None:
        return {"camera_angle": None, "angle_method": None}

    payload = _call_dav2(resolved_api_url, frame_bytes, timeout=resolved_timeout)
    return {"camera_angle": payload.get("camera_angle"), "angle_method": payload.get("angle_method")}
