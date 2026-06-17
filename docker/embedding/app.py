"""임베딩 서비스 (FastAPI). PE-Core-L14-336 이미지/텍스트 → 1024-d 벡터.

백엔드는 EMBEDDING_BACKEND 로 선택 (open_clip 기본; perception_models 는 추후).
SAM3 컨테이너 패턴 미러: /health(model_loaded) → asset 의 wait_until_ready 가 폴링.
"""

from __future__ import annotations

import os

from fastapi import FastAPI, File, Form, UploadFile


def _make_backend():
    name = os.environ.get("EMBEDDING_BACKEND", "open_clip")
    device = os.environ.get("EMBEDDING_DEVICE", "cuda:0")
    if name == "open_clip":
        from backends.open_clip_be import OpenClipBackend

        return OpenClipBackend(device=device)
    if name == "perception_models":
        raise NotImplementedError(
            "perception_models 백엔드는 아직 미구현 — open_clip 사용. "
            "추후 backends/perception_be.py 추가 + EMBEDDING_BACKEND=perception_models"
        )
    raise ValueError(f"unknown EMBEDDING_BACKEND={name!r}")


app = FastAPI(title="vlm-embedding-service")
_backend = _make_backend()
_ready = {"loaded": False}


@app.on_event("startup")
def _startup() -> None:
    _backend.load()
    _ready["loaded"] = True


@app.get("/health")
def health() -> dict:
    return {"model_loaded": _ready["loaded"], "model_name": _backend.name, "dim": _backend.dim}


@app.post("/embed")
async def embed(file: UploadFile = File(...)) -> dict:
    data = await file.read()
    return {"vector": _backend.embed_image(data), "dim": _backend.dim, "model_name": _backend.name}


@app.post("/embed_text")
def embed_text(text: str = Form(...)) -> dict:
    return {"vector": _backend.embed_text(text), "dim": _backend.dim, "model_name": _backend.name}
