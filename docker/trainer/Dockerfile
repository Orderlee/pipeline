# vlm-trainer — SAM3 (Hydra harness) + PE-Core (open_clip contrastive-LoRA) fine-tuning.
# Profile-gated, NOT auto-started, one-shot. GPU host box only (CI never runs GPU training).
# torch cu128 = same stack as docker/sam3 (so the merged checkpoint loads in the sam3 serving image).
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    tini \
    build-essential \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

COPY trainer_lib.py entrypoint.py ./
COPY configs/ ./configs/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TRAINER_TASK=sam3_detection \
    TRAIN_FULL_FT=0

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "entrypoint.py"]
