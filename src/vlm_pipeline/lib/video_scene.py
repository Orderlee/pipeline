"""비디오 씬 5축 통합 분류 (subject_scale / occlusion_state / environment_type /
daynight_type / weather) — Vertex Gemini 2.5 Flash 1회 호출.

design: docs/design-docs/camera-angle-grouping-2026-07-29.md §1, §3, §7.

⚠️ camera_angle 은 이 함수가 담당하지 않는다 — lib/video_angle_dav2.py(DAv2 서비스,
plan-vs-rest AUC 0.947, 2026-07-29 실측 GT 98편)로 분리돼 있다. env_method 는 아래 5축
전체의 provenance 이고, camera_angle 의 provenance(angle_method)는 video_angle_dav2.py 가
별도로 채운다.

Places365(lib/video_env.py)가 담당하던 environment_type/daynight_type 분류는 이 호출
하나로 통합돼 있다 — Places365 는 삭제하지 않고 일시정지(defs/ingest/env_backfill.py,
lib/video_env.py 그대로 유지) 상태다.

⚠️ 백엔드 이력(2026-07-29): 이 5축은 원래 Qwen(vLLM OpenAI 호환 HTTP)이 담당했으나, Qwen
컨테이너/이미지가 삭제되면서 Vertex Gemini 2.5 Flash 로 교체했다 — DAv2 는 기하(깊이맵)만
추정해 이 5축을 원리적으로 판정하지 못하므로 대상이 아니다. Gemini 6축 통합 프롬프트 실측
(2026-07-29): environment_type 23/24, daynight_type 20/24.

classify_camera_angle(video_angle_dav2.py)와 동일한 안전 fallback 규약을 따른다:
- 프레임 추출 자체가 불가능하면 5축 + env_method 를 전부 None 으로 반환한다
  (호출자가 이를 터미널 마커로 해석해 'deferred' selection bucket 에서 빼도록 함).
- Vertex 호출이 실패(네트워크/타임아웃/429/5xx)하면 예외를 그대로 전파한다 — 호출자가
  예외를 잡아 'deferred' 유지 + 재시도로 처리해야 한다(scene_backfill_helpers.process_one_video
  패턴).

참조 원본(재사용·이식): 초기 Qwen 클라이언트 스크립트 (ffmpeg 프레임 추출 + urllib 호출 +
화이트리스트 라벨 매칭) — Vertex 호출부만 lib/video_angle_dav2.py 의 구 Gemini 구현(삭제됨)
패턴으로 교체했다. PROMPT 는 실측 검증된 문구이므로 임의로 바꾸지 말 것.
"""

from __future__ import annotations

import os
import subprocess
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import Any

from .gemini import _load_generation_config_cls, _load_vertex_ai
from .gemini_credentials import resolve_gemini_credentials_path
from .gemini_json import _extract_response_text

SUBJECT_SCALE_VALUES = ("subject_legible", "subject_marginal", "not_applicable")
OCCLUSION_STATE_VALUES = ("unoccluded", "partially_occluded", "truncated", "not_applicable")
ENVIRONMENT_TYPE_VALUES = ("indoor", "outdoor")
DAYNIGHT_TYPE_VALUES = ("day", "night")
WEATHER_VALUES = ("clear", "cloudy", "rain", "snow", "fog", "not_applicable", "indeterminate")

# 축 이름 → 화이트리스트. _parse_axes/_split_key_value_lines 가 순회하는 단일 source of truth.
_AXIS_WHITELISTS: dict[str, tuple[str, ...]] = {
    "subject_scale": SUBJECT_SCALE_VALUES,
    "occlusion_state": OCCLUSION_STATE_VALUES,
    "environment_type": ENVIRONMENT_TYPE_VALUES,
    "daynight_type": DAYNIGHT_TYPE_VALUES,
    "weather": WEATHER_VALUES,
}

DEFAULT_SCENE_GEMINI_MODEL = "gemini-2.5-flash"
DEFAULT_SCENE_TIMEOUT_SEC = 300.0

# 실측 검증된 문구(2026-07-29) — 임의로 바꾸지 말 것. 5축을 한 번의 호출로 받고, 파싱하기
# 쉽도록 정확히 5줄 "key: value" 출력을 강제한다.
PROMPT = """이 CCTV 프레임을 보고 아래 5가지를 판정하라. 프레임에서 실제로 보이는 것만으로 판단하라.
- subject_scale: subject_legible(사람 키가 프레임 세로 1/4 이상) | subject_marginal | not_applicable
- occlusion_state: unoccluded | partially_occluded | truncated | not_applicable
- environment_type: indoor(천장·벽·실내조명으로 둘러싸임) | outdoor(하늘·노지·건물 외부)
- daynight_type: day(자연광) | night(어둡고 인공조명/적외선 흑백)
- weather: clear | cloudy | rain | snow | fog | not_applicable(실내) | indeterminate

정확히 5줄로만 답하라 (그 외 설명 금지):
subject_scale: <value>
occlusion_state: <value>
environment_type: <value>
daynight_type: <value>
weather: <value>
"""


@lru_cache(maxsize=1)
def _ffmpeg_available() -> bool:
    try:
        proc = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            timeout=5,
            check=False,
        )
        return proc.returncode == 0
    except Exception:
        return False


def _extract_frame_bytes(video_path: str, seek_sec: float, timeout_sec: int = 60) -> bytes | None:
    """지정 시점 1프레임을 mjpeg bytes 로 추출. 실패 시 None (예외 전파하지 않음)."""
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


def _extract_frames(video_path: str, frame_count: int) -> list[bytes]:
    """CCTV 고정 카메라 가정 — 1s 지점 우선(1초 미만 클립은 0s fallback), 이후 2s 간격.

    추출 성공한 프레임만 반환 (실패한 seek 지점은 조용히 건너뜀 — per-file fail-forward와
    동일하게 부분 성공을 허용). ffmpeg 자체가 없으면 빈 리스트.
    """
    if not _ffmpeg_available():
        return []
    count = max(1, int(frame_count))
    frames: list[bytes] = []
    for i in range(count):
        seek = 1.0 if i == 0 else 1.0 + i * 2.0
        frame = _extract_frame_bytes(video_path, seek)
        if frame is None and i == 0:
            frame = _extract_frame_bytes(video_path, 0.0)
        if frame is not None:
            frames.append(frame)
    return frames


def _match_whitelist(raw_text: str, allowed: tuple[str, ...]) -> str | None:
    """텍스트에서 가장 먼저 등장한 허용값을 찾는다 (초기 Qwen 클라이언트 `pick()` 이식).

    구조화된 출력을 프롬프트로 강제하지만, 모델이 형식을 살짝 어겨도(값에 부연설명이 붙어도)
    복원 가능하도록 정확 일치가 아니라 부분 문자열 포함으로 매칭한다.
    """
    text = raw_text.strip().lower().replace("-", "_").replace(" ", "_")
    hits = [(text.index(c), c) for c in allowed if c in text]
    return min(hits)[1] if hits else None


def _split_key_value_lines(raw_text: str) -> dict[str, str]:
    """``key: value`` 를 줄 단위로 분리해 ``{key: value_text}`` 로 반환.

    전체 텍스트를 통째로 화이트리스트 매칭하지 않는 이유: ``daynight_type`` 라벨 문자열
    자체가 ``day``/``night`` 를 부분 문자열로 포함한다(day+night+_type). 전체 텍스트
    매칭이면 실제 값이 무엇이든 라벨의 'day' 가 항상 가장 먼저 걸려 daynight_type 이
    오분류된다. 줄 단위로 먼저 쪼개 각 축의 값 텍스트만 격리한 뒤 화이트리스트 매칭하면
    이 충돌이 생기지 않는다.
    """
    result: dict[str, str] = {}
    for line in raw_text.splitlines():
        if ":" not in line:
            continue
        key_part, _, value_part = line.partition(":")
        key = key_part.strip().lower().replace("-", "_").replace(" ", "_")
        if key in _AXIS_WHITELISTS and key not in result:
            result[key] = value_part
    return result


def _parse_axes(raw_text: str) -> dict[str, str | None]:
    """5축 각각을 줄 단위로 분리 후 화이트리스트 매칭. 라인 누락/허용값 밖이면 None."""
    lines = _split_key_value_lines(raw_text)
    return {
        axis: (_match_whitelist(lines[axis], allowed) if axis in lines else None)
        for axis, allowed in _AXIS_WHITELISTS.items()
    }


def _call_gemini(frame_bytes: bytes, *, model: str, timeout: float) -> str:
    """Vertex Gemini 1회 호출. 동기 SDK 호출을 스레드로 감싸 wall-clock timeout 을 부여한다.

    이 SDK 버전의 ``GenerativeModel.generate_content`` 는 timeout kwarg 를 노출하지 않는다.
    ``with ThreadPoolExecutor() as ex:`` 형태는 ``__exit__`` 이 내부적으로
    ``shutdown(wait=True)`` 를 호출해 timeout 이 지나도 원래 작업이 끝날 때까지 반환하지
    않는 함정이 있다(구 video_angle_gemini.py 실측 확인 — 1s timeout 을 걸어도 5s 짜리 원
    작업이 끝날 때까지 with 블록이 반환하지 않았다). 여기서는 ``shutdown(wait=False)`` 를
    명시해 실제로 timeout 만큼만 대기하고 반환한다.
    """
    vertexai, generative_model_cls, part_cls = _load_vertex_ai()
    generation_config_cls = _load_generation_config_cls()

    project_value = (os.environ.get("GEMINI_PROJECT") or "your-gcp-project").strip()
    location_value = (os.environ.get("GEMINI_LOCATION") or "us-central1").strip()
    credentials_value = resolve_gemini_credentials_path()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_value

    vertexai.init(project=project_value, location=location_value)
    gen_model = generative_model_cls(model_name=model)
    image_part = part_cls.from_data(data=frame_bytes, mime_type="image/jpeg")
    generation_config = generation_config_cls(temperature=0, max_output_tokens=2048)

    def _invoke() -> Any:
        return gen_model.generate_content([image_part, PROMPT], generation_config=generation_config)

    executor = ThreadPoolExecutor(max_workers=1)
    try:
        response = executor.submit(_invoke).result(timeout=timeout)
    finally:
        executor.shutdown(wait=False)
    return _extract_response_text(response)


def _majority_vote(values: list[str | None]) -> str | None:
    present = [v for v in values if v is not None]
    if not present:
        return None
    return Counter(present).most_common(1)[0][0]


def classify_video_scene(
    video_path: str,
    *,
    frames: int = 1,
    model: str | None = None,
    timeout: float = DEFAULT_SCENE_TIMEOUT_SEC,
) -> dict[str, Any]:
    """비디오 씬 5축 통합 분류 (ffmpeg 프레임 추출 → Vertex Gemini 호출 → 화이트리스트 파싱).

    Places365 가 담당하던 environment_type/daynight_type 을 이 호출 하나로 흡수한다(§3, §7).
    camera_angle 은 이 함수가 반환하지 않는다 — lib/video_angle_dav2.py(DAv2 서비스)가
    별도로 담당한다.

    Args:
        video_path: 원본(archive) 비디오 경로.
        frames: 샘플링할 프레임 수. 1(기본)이면 단일 프레임 직답. >1이면 프레임별 분류 후
            축별 다수결(scene_backfill_helpers 가 ``SCENE_FRAMES`` env 로 이 값을 넘긴다).
        model: Vertex 모델명. 미지정 시 ``SCENE_GEMINI_MODEL`` env (기본 gemini-2.5-flash).
        timeout: Vertex 호출 wall-clock timeout(초).

    Returns:
        {"subject_scale", "occlusion_state", "environment_type", "daynight_type",
        "weather", "env_method"} 6키 dict. camera_angle/angle_method 키는 없다.
        프레임 추출 완전 실패 시 6개 키 전부 None (호출자가 'deferred_no_frames' 터미널
        마커로 해석). Vertex 호출 실패는 예외로 전파 — 호출자가 'deferred' 유지 재시도로 처리.
    """
    resolved_model = model or os.environ.get("SCENE_GEMINI_MODEL", DEFAULT_SCENE_GEMINI_MODEL)

    raw_frames = _extract_frames(video_path, frames)
    if not raw_frames:
        return {
            "subject_scale": None,
            "occlusion_state": None,
            "environment_type": None,
            "daynight_type": None,
            "weather": None,
            "env_method": None,
        }

    scale_votes: list[str | None] = []
    occlusion_votes: list[str | None] = []
    environment_votes: list[str | None] = []
    daynight_votes: list[str | None] = []
    weather_votes: list[str | None] = []
    for frame_bytes in raw_frames:
        raw_text = _call_gemini(frame_bytes, model=resolved_model, timeout=timeout)
        parsed = _parse_axes(raw_text)
        scale_votes.append(parsed["subject_scale"])
        occlusion_votes.append(parsed["occlusion_state"])
        environment_votes.append(parsed["environment_type"])
        daynight_votes.append(parsed["daynight_type"])
        weather_votes.append(parsed["weather"])

    return {
        "subject_scale": _majority_vote(scale_votes),
        "occlusion_state": _majority_vote(occlusion_votes),
        "environment_type": _majority_vote(environment_votes),
        "daynight_type": _majority_vote(daynight_votes),
        "weather": _majority_vote(weather_votes),
        "env_method": resolved_model,
    }
