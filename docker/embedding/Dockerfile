# 임베딩 서비스 — PE-Core-L14-336 (open_clip) 이미지/텍스트 임베딩.
# GPU 0 배치 (GPU 1 은 SAM3 포화). torch cu124 = 프로젝트 GPU 스택과 일치.
FROM pytorch/pytorch:2.4.0-cuda12.4-cudnn9-runtime

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# 모델 가중치 캐시 (compose 에서 /data/models 볼륨 마운트 → 재기동 시 재다운로드 회피)
ENV EMBEDDING_BACKEND=open_clip \
    EMBEDDING_DEVICE=cuda:0 \
    HF_HOME=/data/models/pe-core

EXPOSE 8003

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8003"]
