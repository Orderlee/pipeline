"""임베딩 백엔드 추상 인터페이스.

HTTP 계약(/embed, /embed_text, /health)은 백엔드와 무관하게 동일하므로,
open_clip ↔ perception_models 등 구현 교체가 Dagster 쪽에 영향 없음.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class EmbeddingBackend(ABC):
    name: str
    dim: int

    @abstractmethod
    def load(self) -> None:
        """모델 로드 (startup 1회)."""

    @abstractmethod
    def embed_image(self, image_bytes: bytes) -> list[float]:
        """이미지 바이트 → 정규화 임베딩 벡터."""

    @abstractmethod
    def embed_text(self, text: str) -> list[float]:
        """텍스트 → 동일 공간 정규화 임베딩 벡터."""
