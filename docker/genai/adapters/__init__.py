"""GenAI adapters registry.

Phase 3: Kling.  Phase 4: Higgsfield (fal.ai) / Nanobanana (Vertex) / GPT Image (OpenAI).
"""

from .base import BaseGenAIAdapter, PollResult, SubmitResult
from .gpt_image import GPTImageAdapter
from .higgsfield import HiggsfieldAdapter
from .kling import KlingAdapter, KlingError, KlingTransientError
from .nanobanana import NanobananaAdapter
from .veo import VeoAdapter


_ADAPTERS: dict[str, type[BaseGenAIAdapter]] = {
    "kling": KlingAdapter,
    "higgsfield": HiggsfieldAdapter,
    "veo": VeoAdapter,
    "nanobanana": NanobananaAdapter,
    "gpt_image": GPTImageAdapter,
}


# UI 의 2탭 매핑 — Image→Video 와 Image→Image 분리
ENGINE_TAB: dict[str, str] = {
    "kling": "image2video",
    "higgsfield": "image2video",
    "veo": "image2video",
    "nanobanana": "image2image",
    "gpt_image": "image2image",
}


def engine_max_concurrent(engine: str) -> int:
    """engine 별 동시 in-flight 작업 상한. 0 = 무제한.

    Kling 은 resource pack 에 동시 작업(parallel) 한도가 있어 초과 시 1303 거부.
    플랜별로 다름 (현재 사용자 플랜 = 5). KLING_MAX_CONCURRENT env 로 조정.
    타 엔진은 별도 env 미설정 시 0(무제한).

    ⚠️ 동기 엔진(nanobanana/gpt_image)은 submit 시점에 결과까지 받아 deferred 가
    drain 되지 않으므로, env 가 설정돼 있어도 무조건 0 (게이트 안 함) — pending stuck 방지.
    """
    import os
    cls = _ADAPTERS.get(engine)
    if cls is not None and getattr(cls, "is_synchronous", False):
        return 0
    env_key = f"{engine.upper()}_MAX_CONCURRENT"
    raw = (os.getenv(env_key, "") or "").strip()
    if raw.isdigit():
        return int(raw)
    # env 미설정 기본값: kling 만 게이트 (플랜 동시 한도). compose 가 보통 명시.
    return 5 if engine == "kling" else 0


def get_adapter(engine: str) -> BaseGenAIAdapter:
    cls = _ADAPTERS.get(engine)
    if cls is None:
        raise ValueError(f"unknown engine: {engine!r} (registered: {sorted(_ADAPTERS)})")
    return cls()


def enabled_engines() -> list[str]:
    """`GENAI_ENGINES_ENABLED` 환경변수로 게이팅."""
    import os
    raw = os.getenv("GENAI_ENGINES_ENABLED", "")
    enabled = [e.strip() for e in raw.split(",") if e.strip()]
    if not enabled:
        return list(_ADAPTERS.keys())
    return [e for e in enabled if e in _ADAPTERS]


def engines_by_tab() -> dict[str, list[str]]:
    """`enabled_engines()` 결과를 탭별로 묶어 반환."""
    by_tab: dict[str, list[str]] = {"image2video": [], "image2image": []}
    for e in enabled_engines():
        tab = ENGINE_TAB.get(e)
        if tab:
            by_tab.setdefault(tab, []).append(e)
    return by_tab


def engine_options(engine: str) -> dict:
    """엔진별 UI dropdown 옵션."""
    if engine == "kling":
        ad = KlingAdapter()
        return {
            "models": list(ad.available_models),
            "default_model": ad.model_name,
            "modes": ["pro", "std"],
            "default_mode": ad.mode,
            "durations": ["5", "10"],
            "default_duration": ad.duration,
        }
    if engine == "veo":
        ad = VeoAdapter()
        return {
            "models": list(ad.available_models),
            "default_model": ad.model_name,
            # Veo 는 16:9 / 9:16 만 (1:1 미지원)
            "aspect_ratios": ["16:9", "9:16"],
            "default_aspect_ratio": ad.aspect_ratio,
            "durations": [str(d) for d in ad.available_durations],
            "default_duration": str(ad.duration),
        }
    return {}


def all_engine_options() -> dict[str, dict]:
    return {e: engine_options(e) for e in enabled_engines()}


__all__ = [
    "BaseGenAIAdapter",
    "PollResult",
    "SubmitResult",
    "KlingAdapter",
    "HiggsfieldAdapter",
    "VeoAdapter",
    "NanobananaAdapter",
    "GPTImageAdapter",
    "ENGINE_TAB",
    "get_adapter",
    "enabled_engines",
    "engines_by_tab",
    "engine_options",
    "all_engine_options",
]
