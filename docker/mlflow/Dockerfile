# MLflow tracking server — 학습 추적/비교 UI (params·dataset lineage·metrics·artifacts).
# profile-gated, 자동기동 X. backend store = 공유 Postgres 의 별도 'mlflow' DB,
# artifact store = MinIO s3://vlm-dataset/_mlflow/ (5-버킷 정책 — 새 버킷 X, prefix 만).
#
# 공식 mlflow 이미지에 PG 드라이버(psycopg2-binary)+S3 업로더(boto3) 가 없어 thin layer 로 추가.
# (registry=승격 SoT, MLflow=추적 UI — 이 컨테이너 장애가 학습/승격을 막지 않음, fail-soft.)
FROM ghcr.io/mlflow/mlflow:v2.16.2

RUN pip install --no-cache-dir psycopg2-binary>=2.9.9 boto3>=1.34.0

EXPOSE 5000
