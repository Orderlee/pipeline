"""open_clip 백엔드 — facebook/PE-Core-L14-336 (timm/PE-Core-L-14-336, 1024-d).

이미지/텍스트 둘 다 동일 임베딩 공간으로 인코딩 (CLIP 계열).
"""

from __future__ import annotations

import io

from .base import EmbeddingBackend

MODEL_NAME = "facebook/PE-Core-L14-336"
HF_HUB_REF = "hf-hub:timm/PE-Core-L-14-336"
EMBED_DIM = 1024


class OpenClipBackend(EmbeddingBackend):
    name = MODEL_NAME
    dim = EMBED_DIM

    def __init__(self, device: str = "cuda:0") -> None:
        self.device = device
        self._model = None
        self._preprocess = None
        self._tokenizer = None
        self._torch = None

    def load(self) -> None:
        import open_clip
        import torch

        self._torch = torch
        model, _, preprocess = open_clip.create_model_and_transforms(HF_HUB_REF)
        self._tokenizer = open_clip.get_tokenizer(HF_HUB_REF)
        self._model = model.to(self.device).eval()
        self._preprocess = preprocess

    def is_loaded(self) -> bool:
        return self._model is not None

    def unload(self) -> None:
        """모델/전처리기/토크나이저 참조를 끊고 CUDA 캐시를 비워 VRAM 을 해제한다.

        다음 embed/warmup 호출 시 load() 로 lazy reload 된다. _torch 모듈 참조는
        유지(GPU 메모리 아님) — empty_cache 호출에 필요.
        """
        self._model = None
        self._preprocess = None
        self._tokenizer = None
        if self._torch is not None and self._torch.cuda.is_available():
            try:
                self._torch.cuda.empty_cache()
            except Exception:
                pass

    def embed_image(self, image_bytes: bytes) -> list[float]:
        from PIL import Image

        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        x = self._preprocess(img).unsqueeze(0).to(self.device)
        with self._torch.no_grad():
            feat = self._model.encode_image(x, normalize=True)
        return feat[0].cpu().float().tolist()

    def embed_text(self, text: str) -> list[float]:
        toks = self._tokenizer([text]).to(self.device)
        with self._torch.no_grad():
            feat = self._model.encode_text(toks, normalize=True)
        return feat[0].cpu().float().tolist()
