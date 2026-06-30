# MLOps Fine-tune — Operator Runbook

> SAM3 / PE-Core 파인튜닝 트랙 운영 런북. 설계: `docs/superpowers/specs/2026-06-29-mlops-finetune-scaffolding-design.md`.
> 요약·env 노브는 `CLAUDE.md` "## MLOps — 파인튜닝 트랙". 이 문서는 **장애·판별·검증 절차**.
> ⚠️ 첫 실학습 전 blocking 산출물 (spec §14). 복구 절차를 읽지 않고 정비 윈도우를 열지 말 것.

## 핵심 주의

- 공유 GPU: `docker-sam3-1` 은 **prod·staging 공유**(memory `project_sam3_shared_container`). 정비/unload 는 staging 서빙도 영향.
- prod main push(docs/tests 제외)는 dagster 무조건 재가동(memory `project_prod_deploy_dagster_restart`). 학습 윈도우 중 prod 배포 보류.
- trainer 는 Dagster run 과 **분리된 독립 프로세스**(`docker compose run --rm trainer`). CI 재배포가 in-run op 를 고아화하므로 op 안에서 GPU 학습을 직접 돌리지 않음.

## 9. 정비락 복구 (maintenance-lock recovery)

정비 플래그(서버사이드 게이트 + PG/마커)는 **fail-safe(자동해제)** 설계. fail-stuck(영구 정지) 금지.

### 정상 흐름
1. `POST /maintenance/enter` (owner_run_id + heartbeat 기록) → `/segment`·`/embed` 가 `503`, lazy-reload 거부.
2. active GPU run drain/cancel → `/unload` → `nvidia-smi` free VRAM 확인.
3. 학습 종료 후 `POST /maintenance/exit` + `POST /warmup` → 모델 재로드 확인 → 서빙 sensor 재개.

### 락이 풀리지 않을 때 (stuck/crash)
1. guard 센서(`stuck_run_guard` 패턴)가 heartbeat 사망 또는 owner run 비RUNNING 감지 시 **자동 해제**(sensor 재개 + `/warmup` + 플래그 clear)를 시도한다. 먼저 Dagster UI 에서 guard 센서 tick 로그 확인.
2. 자동 해제가 안 되면 수동:
   ```bash
   # 정비 플래그 강제 해제 + warmup 트리거
   bash /home/user/work_p/Datapipeline-Data-data_pipeline/scripts/clear_maintenance.sh --env prod
   ```
   이 스크립트는 PG/마커 플래그 clear → `POST /maintenance/exit` → `POST /warmup` 순으로 호출.
3. 서빙 복구 검증:
   ```bash
   curl -sf http://localhost:8002/health    # sam3: model_loaded=true 기대
   curl -sf http://localhost:<embed_port>/health
   ```
   `503` 이 계속이면 컨테이너 로그(`docker logs docker-sam3-1 --tail 50`)에서 maintenance 플래그 상태 확인.

## run 이 progressing 인지 hung 인지 판별

학습 추적은 **MLflow(`MLFLOW_TRACKING_URI`, fail-soft) + Dagster UI + 컨테이너 로그 + MinIO JSONL** 다층.

1. **per-step JSONL** (가장 빠른 신호): trainer 가 `step,loss,lr,throughput` 을 stdout + `_models/<ver>/train_log.jsonl` 에 append.
   ```bash
   docker logs --tail 30 <trainer-container>        # 최근 step 이 증가 중이면 진행
   mc cat local/vlm-dataset/_models/<model>/<version>/train_log.jsonl | tail -5
   ```
   step 번호/timestamp 가 멈춰 있으면 hung. loss=NaN 이면 학습 발산.
2. **GPU 활동**: `nvidia-smi` — trainer PID 의 GPU-Util/메모리가 0 으로 떨어졌으면 hung 또는 종료.
3. **Dagster op metadata**: 기동 op 가 start/heartbeat/end 를 asset metadata+log 로 기록 → heartbeat timestamp 가 stale 이면 hung.
4. hung 확정 시: trainer 컨테이너 stop → 정비락 복구(§9) → 부분 산출물(`_models/<ver>/`)은 candidate 미등록 상태로 남음(레지스트리에 행 없음 = 안전).

## staging vs prod 검증 분리

CI 에 GPU 없음. 무엇을 어디서 검증하는가:

### staging(:3031)에서 검증 가능 (`ENABLE_TRAINING=false`, prod GPU 무영향)
- PG 마이그레이션(`train_dataset_versions`, `model_registry`) 적용 + `pg_constraint` 확인.
- 스냅샷 빌더 asset: 소형 fixture 로 `content_checksum` 결정성 + group-aware split leakage 없음.
- eval 게이트 로직: 스텁 메트릭으로 stock_base 분기 + margin/non-regression.
- `promote_model.py --dry-run`: 레지스트리 선택 로직 + recreate 계획(무변경).
- defs 로드 스모크 + 정비 플래그 PG 메커니즘.

### prod 전용 (통제된 수동 윈도우)
- 실제 GPU 학습 + 공유 sam3 대상 `/unload`+`/warmup`.
- ⚠️ **공유 sam3 때문에 GPU 사이클의 진짜 staging dry-run 불가** = 수용된 리스크. 완화: fail-safe 락(§9) + 최소 첫-run 데이터셋 + 복구 런북 대기.
- 첫 실학습은 반드시 정비 윈도우 + 본 런북 §9 숙지 후.

## MLflow 학습 추적

→ **상세 운영 런북: [`MLFLOW.md`](MLFLOW.md)** (주소 :5500, 기동, 무엇이 기록되나, 영속화, 트러블슈팅, env).
요약: registry(`model_registry`)=승격 SoT, MLflow=보조 추적(fail-soft). 재기동 없이 기동:
`CREATE DATABASE mlflow` → `docker compose --profile mlflow up -d --build mlflow` → `http://10.0.0.10:5500`.

## DVC 큐레이션 데이터 버저닝

→ **상세 운영 런북: [`DVC.md`](DVC.md)** (2-tier 워크플로, push→카탈로그, build_trainset sources 조합, 트러블슈팅).
요약: bytes=MinIO `vlm-dataset/_dvc/`(10.0.0.51) · 포인터=bare repo `/srv/data-repos/dvc-datasets.git`(10.0.0.10) · 인덱스=PG `dataset_catalog`.
- **데이터 엔지니어**: `dvc push` + `git push` → post-receive 훅이 `dataset_catalog`에 버전 행(커밋메시지) 자동 INSERT (재기동 없음).
- **AI 엔지니어**: `build_trainset` config `sources=["projA:current", ...]` → 여러 프로젝트 조합 → 불변 `train_dataset_versions`(source_spec lineage).
- 구축됨: 그룹 `dvc-data`, bare repo+훅, venv, `dvc-ingest.env`, 시드. **미완 1줄**: `sudo git config --system --add safe.directory /srv/data-repos/dvc-datasets.git` (eng-a/eng-b+훅).

## 참고 명령 모음

```bash
# 후보/승격 상태
psql "$PG_DSN" -c "SELECT model,version,status,incumbent_source FROM model_registry ORDER BY created_at DESC LIMIT 10;"
# 동결 학습셋 버전
psql "$PG_DSN" -c "SELECT train_dataset_version_id,task,total_count,al_confirmed_count,created_at FROM train_dataset_versions ORDER BY created_at DESC LIMIT 10;"
# 학습 기동 (정비 윈도우 안에서)
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker && docker compose --env-file .env run --rm trainer
```
