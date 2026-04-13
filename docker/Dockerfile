# ===== NVIDIA CUDA + Python 개발 환경 =====
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

# ===== 시스템 패키지 설치 =====
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    ffmpeg \
    curl \
    tini \
    git \
    build-essential \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# ===== DuckDB CLI 설치 =====
RUN curl -LO https://github.com/duckdb/duckdb/releases/download/v1.4.2/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && mv duckdb /usr/local/bin/ \
    && rm duckdb_cli-linux-amd64.zip

# ===== Google Cloud CLI 설치 =====
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y --no-install-recommends google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

# ===== rclone 설치 =====
RUN curl -fsSL https://rclone.org/install.sh | bash

# ===== 작업 디렉토리 설정 =====
WORKDIR /app

# ===== Python 패키지 설치 =====
COPY docker/app/requirements.txt .
RUN pip3 install --upgrade pip setuptools wheel \
    && pip3 install --no-cache-dir -r requirements.txt

# ===== 런타임 코드/설정 복사 =====
COPY docker/app/dagster_defs.py /app/dagster_defs.py
COPY docker/app/dagster_defs_staging.py /app/dagster_defs_staging.py
COPY docker/app/workspace.yaml /app/workspace.yaml
COPY docker/app/workspace_prod.yaml /app/workspace_prod.yaml
COPY docker/app/workspace_staging.yaml /app/workspace_staging.yaml
COPY docker/app/dg /app/dg
COPY src/python/ /src/python/
COPY src/ /src/vlm/
COPY configs/ /configs/
COPY split_dataset/ /split_dataset/
COPY gcp/ /gcp/
COPY scripts/ /scripts/
RUN chmod +x /app/dg

# ===== 비루트 사용자 생성 =====
RUN useradd -m -u 10001 appuser \
    && chown -R appuser:appuser /app /src /configs /split_dataset /gcp /scripts
USER appuser

# ===== 환경변수 설정 =====
ENV PORT=8000 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility \
    PATH="/app:${PATH}"

EXPOSE 8000

# ===== 진입점 설정 =====
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sleep", "infinity"]
