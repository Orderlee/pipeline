# MLflow 학습 추적 — 운영 런북

> 학습 파라미터·데이터셋 lineage·metric·산출물을 추적하는 사람용 UI.
> **registry(`model_registry`)가 승격 SoT, MLflow는 보조 추적** — MLflow 장애가 학습/승격을 막지 않음(fail-soft).

## 주소
| 용도 | 주소 |
|------|------|
| 사람용 UI (브라우저) | **`http://10.0.0.10:5500`** (호스트 포트, `MLFLOW_PORT` 기본 5500) |
| trainer가 쓰는 tracking URI | `http://mlflow:5000` (`MLFLOW_TRACKING_URI`, pipeline-network 내부) |
| 서버 바인드 | `mlflow server --host 0.0.0.0 --port 5000` (컨테이너 내부) |

- **backend store**: `postgresql://airflow:…@postgres:5432/mlflow` (파이프라인 PG의 별도 `mlflow` DB)
- **artifact store**: `s3://vlm-dataset/_mlflow/` (MinIO 10.0.0.51, 5-버킷 정책 prefix)

## 현재 상태 (2026-06-30)
- `docker-mlflow-1` **구동 중(healthy)**, `:5500` 응답. `mlflow` DB 생성됨. 이미지 `datapipeline-mlflow:0.1` 빌드됨.
- ⚠️ **수동 기동 상태 — 영속 아님**: `.env`의 `COMPOSE_PROFILES`에 `mlflow` 미포함(`--profile mlflow`로 띄움). 다음 prod 배포(`compose up -d`)가 orphan 제거하면 사라질 수 있음 → 영속화는 아래 참조.

---

## 기동 (재기동 없이 — dagster 무영향)
MLflow는 **별도 추가 컨테이너**. dagster/sam3/embedding 안 건드리고 그것만 띄움:
```bash
# 1) 백엔드 DB (최초 1회)
docker exec docker-postgres-1 psql -U airflow -tc "SELECT 1 FROM pg_database WHERE datname='mlflow'" | grep -q 1 \
  || docker exec docker-postgres-1 psql -U airflow -c "CREATE DATABASE mlflow;"
# 2) 빌드 + 기동 (해당 서비스만 — 인자 없는 up -d 금지: 전체 reconcile로 dagster recreate 위험)
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose --profile mlflow up -d --build mlflow
# 3) 확인
curl -sf http://localhost:5500/health && echo OK
```
서버는 부팅 시 `mlflow` DB에 alembic 마이그레이션 자동 적용. unreachable 여도 학습/승격 무영향.

## 무엇이 기록되나 (학습 run 1회당)
trainer가 `_log_mlflow`로 fail-soft 로깅 (`docker/trainer/mlflow_logging.py`):
| 종류 | 내용 |
|------|------|
| **Params — 하이퍼파라미터** | lr, epochs, batch, LoRA rank/alpha, seed, train_method, base 모델 |
| **Params + `ds.*` Tags — 데이터셋 lineage** | `train_dataset_version_id`, `content_checksum`, `manifest_key`, ls/al counts, per_class_counts, split_ratios, **DVC**: `dvc_catalog_id`/`dvc_git_rev`/`dvc_commit_subject`/`dvc_md5` |
| **Dataset input** | lineage로 만든 MLflow Dataset 객체 (run의 "Datasets used") |
| **Metrics** | eval 점수 (SAM box mAP / PE recall@k, per-class) |
| **Artifacts** | `_models/<ver>/`의 `env_lock.json`·`train_log.jsonl`·`training_summary.json` |

→ run 페이지 = "이 모델 = 이 하이퍼파라미터 + **정확히 이 버전·체크섬의 데이터셋(어느 DVC 커밋)** + 이 metric" 추적.
run_id는 `model_registry.mlflow_run_id`에 역링크.

⚠️ **데이터 자체(이미지)는 MLflow에 안 올라감** — 지문(체크섬/카운트/DVC출처)만. 바이트는 MinIO `_trainsets/`·`_dvc/`.

## 현재 상태 확인
```sql
-- 어떤 모델이 어느 MLflow run 과 연결됐나
SELECT model, version, status, mlflow_run_id FROM model_registry ORDER BY created_at DESC LIMIT 10;
```
UI(:5500) Experiments: `sam3_detection` / `pe_core_embedding`. run_name = `MODEL_VERSION`.

## 영속화 (선택 — 배포 동반)
수동 기동은 다음 prod 배포에서 사라질 수 있음. 상시 관리하려면:
1. `docker/.env`의 `COMPOSE_PROFILES`에 `,mlflow` 추가.
2. 다음 prod 배포(main push) 또는 `docker compose --profile mlflow up -d mlflow` 재기동.
   → ⚠️ `.env` 변경은 배포(dagster 재가동) 동반 — 학습 윈도우 피해서.
**재기동 원치 않으면**: 지금처럼 수동 기동 유지, 사라지면 위 "기동 1·2단계" 재실행(DB 이미 있으면 2단계만).

## 트러블슈팅
- **`:5500` 안 열림**: `docker ps | grep mlflow`(컨테이너?) → 없으면 위 "기동". profile 미활성/이미지 미빌드/`mlflow` DB 부재가 흔한 원인.
- **컨테이너 crash-loop**: `mlflow` DB 부재 → backend store 연결 실패. `CREATE DATABASE mlflow` 먼저.
- **trainer run이 MLflow에 안 보임**: `MLFLOW_TRACKING_URI`(기본 `http://mlflow:5000`) unreachable → trainer 로그에 `[trainer][mlflow] logging failed (fail-soft)` + run_id=None. **학습/승격은 정상**(registry=SoT) — MLflow만 누락. mlflow 컨테이너 떠있는지 + 같은 network(pipeline-network) 확인.
- **artifact 업로드 실패**: MinIO 자격(`MINIO_ACCESS_KEY/SECRET`) / `vlm-dataset` 버킷 / endpoint(`MLFLOW_S3_ENDPOINT_URL`) 확인. params/metric은 PG라 영향 없음.

## env 노브
| env | 기본 | 의미 |
|-----|------|------|
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | trainer→서버. unreachable 시 fail-soft |
| `MLFLOW_PORT` | `5500` | 호스트 UI 포트 (컨테이너 5000 매핑) |
| `MLFLOW_S3_ENDPOINT_URL` | `${MINIO_ENDPOINT}` | artifact용 MinIO endpoint |
| `COMPOSE_PROFILES` | (mlflow 미포함) | `,mlflow` 추가 시 배포에 상시 포함 |
