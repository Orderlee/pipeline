"""GenAI adapters registry.

Phase 3: Kling.  Phase 4: Higgsfield (fal.ai) / Nanobanana (Vertex) / GPT Image (OpenAI).
"""

from .base import BaseGenAIAdapter, PollResult, SubmitResult
from .gpt_image import GPTImageAdapter
from .higgsfield import HiggsfieldAdapter
from .kling import KlingAdapter
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
