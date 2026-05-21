# 추가하면 좋을 스택 후보 (cross-validated, 우선순위 순)

> 작성일: 2026-05-21
> 작성자: ywl (Claude Opus 4.7 + Codex 교차 검토)
> 용도: 다음 스프린트 작업 후보 정리

## 배경

VLM Data Pipeline 전체 스택을 훑고 "어떤 스택을 추가하면 좋을지" 를 두 시각으로 교차 검토한 결과.

핵심 사실 한 가지가 모든 추천의 기반:
**2026-05-19 에 DuckDB → Postgres cutover 완료** 한 직후이고, 현재 `vlm_pipeline` Postgres 에 대한 **자동 백업이 없음**.

---

## 🔴 Tier 1 — 이번 스프린트에 꼭

### 1. pg_dump + restic (또는 pgBackRest) → Postgres 백업 자동화 [Effort: S~M]

- **왜 1순위**: Postgres 가 모든 라벨/dispatch/dedupe 상태의 single source of truth 인데 [docker-compose.yaml](../../docker/docker-compose.yaml) 에는 `postgres_data` 볼륨만 있고 백업 cron/sidecar 가 없음. cutover 직후의 미백업 상태는 "복구 불가" 리스크.
- **codex 코멘트**: *"Prometheus-before-backups is wrong for this pipeline. Broken dashboards are annoying; an un-restorable Postgres volume is existential."* ← 핵심 disagreement.
- **위험**: untested backups = theater. 분기 1회 restore drill 필수.

### 2. Dagster `asset_checks` + Pandera → 파이프라인 내장 DQ 게이트 [Effort: M]

- **왜**: Postgres ↔ MinIO ↔ Label Studio 간 진실의 분기가 많음. 특히 `DATASET_REQUIRE_LS_FINALIZED=0` 이 기본값이라 **검수 안 된 데이터가 dataset 에 섞일 수 있는 구조**가 코드에 명시되어 있음. Dagster 1.6+ 의 `@asset_check` 가 이미 import 가능해서 별도 프레임워크 없이 시작 가능.
- **점검 후보**:
  - `labels.timestamp_status='completed'` ↔ MinIO `vlm-labels/<src>/events/*.json` 존재 일치
  - `raw_files.ingest_status='completed'` ↔ archive 이동 일치
  - dispatch row 수 급변 anomaly

---

## 🟡 Tier 2 — 큰 ROI, 다음 분기

### 3. Prometheus + node_exporter + nvidia_gpu_exporter → 이미 있는 Grafana 살리기 [Effort: M]

- **왜**: Grafana 12.3.1 컨테이너와 [grafana/minio-dashboard.json](../../docker/grafana/) 이 이미 있고 MinIO 가 `MINIO_PROMETHEUS_AUTH_TYPE: public` 으로 메트릭 노출 중. **수집 에이전트 한 개 추가하면 즉시 동작**.
- **codex 가 잡은 결정적 디테일**: YOLO 와 SAM3 가 둘 다 `CUDA_VISIBLE_DEVICES=1` 로 동일 GPU 공유 중 ([docker-compose.yaml:261, :302](../../docker/docker-compose.yaml)). GPU 메모리 contention 가시화가 운영적으로 시급.
  - (참고: 후속 조사에서 SAM3 는 `cuda:0` 으로 분리되어 있음을 재확인. 그러나 compose 의 `CUDA_VISIBLE_DEVICES` 와 앱 ENV 가 불일치하는 부분이 있어 별도 점검 권장)

### 4. Loki + Promtail → JSONL 실패 로그 + 컨테이너 로그 통합 [Effort: M]

- **왜**: 실패 추적이 `<manifest_dir>/failed/*.jsonl` + Dagster SQLite + 컨테이너 stdout 세 곳에 흩어져 있어 incident reconstruction 가 느림. Grafana 스택으로 수렴 가능 (ELK 불필요).

---

## 🟢 Tier 3 — 가성비 좋은 소형 추가

### 5. SOPS + age → `.env` 암호화 [Effort: S~M]

- **왜**: `.env` / `.env.test` 가 Vertex 서비스 계정, MinIO root creds, Slack signing secret, GenAI 6개 provider 키를 평문 보유. Vault 는 과함, 평문은 부족 → SOPS 가 적정점.

### 6. pre-commit + pyright (basic mode) → 커밋 시점 가드레일 [Effort: S]

- **왜**: `pyproject.toml` 에 ruff 만 있고 type checker 없음. Pydantic 2.0 + 타입 주석은 이미 깔려 있어 점진 도입 비용이 낮음. CI 실패 전에 잡힘.

### 7. GHCR (또는 Harbor self-host) → 이미지 레지스트리 [Effort: M]

- **왜**: [deploy-stack.sh:118](../../scripts/deploy/deploy-stack.sh) 에서 매 배포마다 `compose build` 로컬 재빌드 중. GHCR push/pull 로 전환 시 배포 시간 단축 + 이미지 = SHA 불변 보장.

---

## 만약 이번 스프린트에 딱 2개만 한다면

1. **Postgres 자동 백업** (pg_dump + restic, sidecar 컨테이너 1개)
2. **Dagster asset_checks** — 최소 3개부터:
   - `raw_files ↔ archive` 일치
   - `labels ↔ vlm-labels/events JSON` 일치
   - dataset 빌드 시 LS finalized 강제

codex 와 Claude 의 의견이 **이 두 가지에 대해 완전히 일치**.
두 번째까지 끝내면 그제서야 Prometheus 를 붙여서 "지금 막 만든 DQ check 의 실패율 자체를 메트릭화" 하는 흐름이 자연스럽다.

---
