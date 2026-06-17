# analysis 컨테이너 — JupyterLab + FiftyOne (임베딩 시각화/유사검색)

임베딩 파이프라인이 `image_embeddings`(pgvector)에 적재한 1024-d 벡터를 **FiftyOne** 으로
시각화/클러스터/유사검색하는 분석 surface. 별도 FiftyOne+Mongo 스택을 세우지 않고
이 단일 컨테이너에 FiftyOne(번들 mongod)을 얹는다 (on-prem, 팀 공유, compose 일관).

## 기동
```bash
# 환경별 profile 활성 + 기동 (prod 예시; staging 은 compose-staging wrapper)
COMPOSE_PROFILES=analysis ./scripts/compose-prod.sh up -d analysis
```
- JupyterLab: `http://<HOST>:8888` (token = `JUPYTER_TOKEN`, 미설정 시 토큰 없음 — 내부망 전용)
- FiftyOne App: `http://<HOST>:5151`

## 사용 (노트북)
```python
import fiftyone_pgvector as fp
rows = fp.load_frame_embeddings(limit=5000)          # pgvector → 임베딩 로드
ds   = fp.build_fiftyone_dataset("frames", rows)      # FiftyOne 데이터셋 + UMAP 시각화
import fiftyone as fo; fo.launch_app(ds)               # :5151 에서 탐색

fp.search_by_text("a fire on the street", k=20)        # 텍스트→이미지 검색 (embedding-service /embed_text)
fp.search_by_image(rows[0]["image_id"], k=20)          # 이미지 유사 검색 (pgvector <=> cosine)
```

## ⚠️ 운영 주의 (staging 검증 시 확인)
- **MINIO_ENDPOINT**: presigned URL 이 **사용자 브라우저**에서 열려야 하므로 `ANALYSIS_MINIO_ENDPOINT`
  를 host-reachable 주소(예: `http://10.0.0.10:9000`)로 설정. 내부 docker 명(`minio:9000`)은
  브라우저에서 미도달 → FiftyOne App 에 이미지가 안 뜬다.
- **FIFTYONE_DATABASE_DIR**: `/data/fiftyone/db`(볼륨) — mongod 데이터/데이터셋 영속. `down -v` 금지.
- **자격증명**: MINIO_ACCESS_KEY/SECRET 는 MinIO root 자격 재사용. read-only 키 분리는 후속(foundation 계획).
- **검증 미완**: 이 컨테이너는 빌드/기동 후 staging 에서 E2E 검증 필요 (로컬 미검증 scaffold).
