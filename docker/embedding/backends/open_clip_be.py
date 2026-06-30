"""open_clip 백엔드 — facebook/PE-Core-L14-336 (timm/PE-Core-L-14-336, 1024-d).

이미지/텍스트 둘 다 동일 임베딩 공간으로 인코딩 (CLIP 계열).
"""

from __future__ import annotations

import io
import os

from .base import EmbeddingBackend

MODEL_NAME = "facebook/PE-Core-L14-336"
HF_HUB_REF = "hf-hub:timm/PE-Core-L-14-336"
EMBED_DIM = 1024


class OpenClipBackend(EmbeddingBackend):
    dim = EMBED_DIM

    def __init__(self, device: str = "cuda:0") -> None:
        self.device = device
        self._model = None
        self._preprocess = None
        self._tokenizer = None
        self._torch = None
        # §5.3: 파인튠 가중치 서빙 시 model_name 에 버전 인코딩 (image_embeddings 버전 격리).
        # env 미설정 = stock 동작 (현행 incumbent 유지, 미승격).
        version = os.environ.get("EMBEDDING_MODEL_VERSION", "").strip()
        self.name = f"{MODEL_NAME}@{version}" if version else MODEL_NAME

    def load(self) -> None:
        import open_clip
        import torch

        self._torch = torch
        model, _, preprocess = open_clip.create_model_and_transforms(HF_HUB_REF)
        self._tokenizer = open_clip.get_tokenizer(HF_HUB_REF)

        # 승격된 merged full-weight 체크포인트 로드 (env 설정 시). 어댑터(LoRA) 서빙 코드 없음 —
        # 학습 종료 시 base 에 merge 된 full-weight 만 로드 (design §8). env 미설정이면 stock.
        ckpt_path = os.environ.get("EMBEDDING_CHECKPOINT_PATH", "").strip()
        if ckpt_path:
            if not os.path.isfile(ckpt_path):
                raise FileNotFoundError(f"EMBEDDING_CHECKPOINT_PATH not found: {ckpt_path}")
            ckpt = torch.load(ckpt_path, map_location=self.device)
            state_dict = ckpt.get("state_dict", ckpt) if isinstance(ckpt, dict) else ckpt
            model.load_state_dict(state_dict, strict=True)

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
