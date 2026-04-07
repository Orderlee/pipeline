# 운영(Production) vs 스테이징(Staging) 분리

코드와 Compose 기준으로 두 환경의 경계를 정리한 문서입니다.

## 핵심 요약

| 구분 | Production | Staging |
|------|------------|---------|
| Dagster 진입점 | `docker/app/dagster_defs.py` → `vlm_pipeline.definitions` | `docker/app/dagster_defs_staging.py` → `vlm_pipeline.definitions_staging` |
| Workspace | `docker/app/workspace_prod.yaml` | `docker/app/workspace_staging.yaml` |
| Compose 서비스 | `dagster`, `dagster-daemon`, `dagster-code-server` | `dagster-staging` (`--profile staging`) |
| Env 파일 | `docker/.env` | `docker/.env.staging` |
| `IS_STAGING` | 미설정(`false`) | `true` |
| DuckDB | `/data/pipeline.duckdb` | `/data/staging.duckdb` |
| Incoming/Archive | `/nas/incoming`, `/nas/archive` | `/nas/staging/incoming`, `/nas/staging/archive` |
| MinIO endpoint | `http://172.168.47.36:9000` | `http://172.168.47.36:9002` |
| UI 포트 | `3030` | `3031` |
| Agent ingress | `http://host.docker.internal:8080` | `http://host.docker.internal:8081` |

## 실행 경로

- Production Dagster webserver/daemon/code-server는 `workspace_prod.yaml`을 사용합니다.
- Staging Dagster(`dagster-staging`)는 `workspace_staging.yaml`을 사용합니다.
- 두 환경은 DuckDB 파일, NAS 경로, MinIO endpoint가 분리됩니다.

## 파이프라인 ingress 차이

### Production

- `production_agent_dispatch_sensor`가 `agent:8080`의 production API를 polling하는 기본 ingress입니다.
- `.dispatch/pending/*.json` 파일 ingress는 `dispatch_sensor`가 처리합니다.
- 성공 시 dispatch manifest 생성 + `dispatch_stage_job` run request를 발행합니다.

### Staging

- `staging_agent_dispatch_sensor`가 `agent:8081`의 staging API를 polling합니다.
- payload가 실행 조건을 만족하면 동일한 dispatch 코어 로직으로 manifest/run request를 생성합니다.
- payload가 비어 있으면 `waiting_for_dispatch_params`로 ack만 보내고 run을 생성하지 않습니다.
- 파일 기반 `dispatch_sensor`는 staging에서 레거시/호환 경로로만 유지됩니다.

## 코드 기준

- Definitions 조립은 `src/vlm_pipeline/definitions_profiles.py`에서 profile별로 구성됩니다.
- 진입점 파일은 얇은 래퍼입니다.
  - `src/vlm_pipeline/definitions.py`
  - `src/vlm_pipeline/definitions_staging.py`
- ingest 환경 정책은 `src/vlm_pipeline/defs/ingest/runtime_policy.py`에서 profile 기반으로 해석합니다.
- dispatch ingress 공통 코어는 `src/vlm_pipeline/defs/dispatch/service.py`의 `process_dispatch_ingress_request`입니다.

## 운영 체크리스트

- staging 실행 시 반드시 `docker compose --profile staging ...`를 사용합니다.
- production과 staging의 `DAGSTER_HOME`, DuckDB, MinIO, incoming/archive 경로가 섞이지 않도록 유지합니다.
- `dispatch_stage_job`는 `duckdb_writer=true`(legacy)로 유지하고, standalone writer job은 `duckdb_raw_writer`, `duckdb_label_writer`, `duckdb_yolo_writer` lane을 사용합니다.
- staging은 `${STAGING_REPO_PATH}` 별도 checkout을 마운트하므로, 운영 repo 변경만으로는 staging 반영이 끝나지 않습니다.
