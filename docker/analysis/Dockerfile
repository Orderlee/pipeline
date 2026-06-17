# 분석 컨테이너 — JupyterLab + FiftyOne (임베딩 시각화/유사검색 surface).
# FiftyOne 은 pip 의존성 fiftyone-db(번들 mongod)를 자동 포함 → 별도 Mongo 서비스 불필요.
# mongod 데이터는 FIFTYONE_DATABASE_DIR(볼륨)로 영속. GPU 불필요(임베딩은 이미 pgvector 적재됨).
FROM python:3.11-slim

# fiftyone-db(mongod) + opencv/이미지 처리 런타임 의존성
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ffmpeg libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fiftyone_pgvector.py /workspace/fiftyone_pgvector.py
COPY embedding_dashboard.py /workspace/embedding_dashboard.py

ENV FIFTYONE_DATABASE_DIR=/data/fiftyone/db \
    FIFTYONE_DEFAULT_DATASET_DIR=/data/fiftyone/datasets \
    PYTHONUNBUFFERED=1

# 8888=Jupyter, 5151=FiftyOne, 8501=Streamlit 대시보드(docker exec 로 실행)
EXPOSE 8888 5151 8501

# compose 에서 command override (token 주입). 기본은 토큰 없는 로컬 실행.
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token="]
