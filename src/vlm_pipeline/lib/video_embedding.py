"""Video embedding method constants and seam helpers (L1 lib — pure Python).

두 method:
  - METHOD_FRAME_POOL: frame embeddings 의 L2-normalized mean (method A, 구현됨).
  - METHOD_VIDEO_MODEL: 전용 video encoder (method B, future drop-in, 미구현).

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

METHOD_FRAME_POOL = "frame_pool"
METHOD_VIDEO_MODEL = "video_model"


def video_model_name(base: str, method: str) -> str:
    """method 에 따른 model_name 문자열 생성.

    frame_pool: '<base>/framepool' (기존 frame embedding 공간과 동일 prefix 유지).
    video_model: base 그대로 반환 (전용 모델명을 직접 지정하는 경우).
    """
    if method == METHOD_FRAME_POOL:
        return f"{base}/framepool"
    return base


def call_video_model_stub(asset_id: str, model_name: str) -> None:
    """Method B (video_model) 전용 encoder 호출 seam.

    이 함수는 미래에 embedding-service /embed_video 엔드포인트 또는 별도
    video encoder 로 대체되는 drop-in 지점이다. 현재는 명시적 오류를 발생시켜
    미구현 경로를 가시화한다.
    """
    raise NotImplementedError(
        f"video_model method not configured for asset_id={asset_id!r} model={model_name!r}. "
        "Method B is a future drop-in — implement embedding-service /embed_video endpoint "
        "and replace this stub before enabling video_model method."
    )
