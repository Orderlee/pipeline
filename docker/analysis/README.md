# analysis 컨테이너 — JupyterLab + FiftyOne + Streamlit (임베딩 시각화/유사검색)

임베딩 파이프라인이 `image_embeddings`(pgvector)에 적재한 1024-d 벡터를 **FiftyOne** 과
**Streamlit 대시보드**로 시각화/클러스터/유사검색하는 분석 surface.

FiftyOne 메타데이터 store 는 **`fiftyone-mongo` 사이드카**(`mongo:7`)를 사용한다.
(번들 mongod 는 slim 베이스에서 미동작 → 2026-06-15 staging 검증 후 사이드카로 전환.
Dockerfile 상단 주석과 `ENV FIFTYONE_DATABASE_DIR` 는 그 시절 잔재이며, compose 가
`FIFTYONE_DATABASE_URI=mongodb://fiftyone-mongo:27017` 로 덮어쓴다.)

## 기동

```bash
# 환경별 profile 활성 + 기동 (prod 예시; staging 은 compose-staging wrapper)
COMPOSE_PROFILES=analysis ./scripts/compose-prod.sh up -d analysis
```

| surface | 컨테이너 포트 | prod 호스트 포트 | 자동 기동 |
|---|---|---|---|
| JupyterLab | 8888 | `8888` | ✅ (compose `command:`) |
| FiftyOne App | 5151 | `5153` (`FIFTYONE_PORT`) | ❌ 수동 |
| Streamlit 대시보드 | 8501 | `8503` (`STREAMLIT_PORT`) | ❌ 수동 |

JupyterLab token = `JUPYTER_TOKEN`, 미설정 시 토큰 없음 — 내부망 전용.

> ⚠️ **JupyterLab 만 자동 기동**한다. FiftyOne 과 Streamlit 은 포트만 열려 있고 프로세스는 안 뜬다.
> **prod 재배포마다 컨테이너가 recreate 되므로 배포 후 다시 띄워야 한다.**

```bash
docker exec -d docker-analysis-1 \
  streamlit run /workspace/embedding_dashboard.py --server.port 8501 --server.address 0.0.0.0
docker exec -d docker-analysis-1 python /workspace/fiftyone_prod_launch.py
```

## 사용 (노트북)

```python
import fiftyone_pgvector as fp
rows = fp.load_frame_embeddings(limit=5000)          # pgvector → 임베딩 로드
ds   = fp.build_fiftyone_dataset("frames", rows)      # FiftyOne 데이터셋 + UMAP 시각화
import fiftyone as fo; fo.launch_app(ds)               # FiftyOne App 에서 탐색

fp.search_by_text("a fire on the street", k=20)        # 텍스트→이미지 검색 (embedding-service /embed_text)
fp.search_by_image(rows[0]["image_id"], k=20)          # 이미지 유사 검색 (pgvector <=> cosine)
```

## ⚠️ 운영 주의

- **`/workspace` 코드는 git 과 drift 한다.** 이미지에 COPY 되는 건 `fiftyone_pgvector.py` 와
  `embedding_dashboard.py` 둘뿐이고 (`Dockerfile:16-17`), 나머지 스크립트
  (`fiftyone_prod_launch.py`, `refresh_frames_labels.py`, `unify_coco_categories.py` 등)는
  실행 중인 컨테이너에 수동 복사된 것이다. 수정 사항은 반드시 repo 에도 반영할 것.
- **MINIO_ENDPOINT**: presigned URL 이 **사용자 브라우저**에서 열려야 하므로
  `ANALYSIS_MINIO_ENDPOINT` 를 host-reachable 주소로 설정.
  현재 prod 값은 `http://10.0.0.51:9000`. 내부 docker 명(`minio:9000`)은 브라우저에서 미도달
  → FiftyOne App 에 이미지가 안 뜬다.
- **FiftyOne 메타데이터**: `fiftyone-mongo` 사이드카 + `./data/fiftyone/mongo` 볼륨에 영속.
  `down -v` 금지 (데이터셋 전부 소실).
- **성능 특성**: 텍스트 검색은 partial HNSW 로 ~50ms 수준. 반면 랜딩 페이지 KMeans 클러스터링은
  캐시 미스 시 수 분 걸린다 (캐시 키 = 임베딩 행 수 → 데이터 증가 시 자동 재계산).
  데모 전에는 미리 한 번 워밍업해 둘 것.
- **자격증명**: `MINIO_ACCESS_KEY`/`SECRET` 는 MinIO root 자격 재사용. read-only 키 분리는 후속 과제.
- **cron 주의**: 호스트 crontab 의 `refresh_frames_labels` 주기 작업은 반드시 `flock` 으로 감쌀 것.
  (2026-07-06 오버랩 3중 중첩 → 스왑 쓰래싱으로 호스트 마비 사건.)
