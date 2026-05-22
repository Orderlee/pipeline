# 추가하면 좋을 스택 후보 (cross-validated, 우선순위 순)

> 작성일: 2026-05-21 (2026-05-22 #8~#12 + DQ inline 섹션 추가, codex 2차 검토 반영)
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

## 🔴 Tier 1 추가 — existential 갭 (2026-05-22 codex agree)

### 8. MinIO mirror/replication (vlm-raw, vlm-labels 우선) [Effort: S]

- **왜**: [docker-compose.yaml](../../docker/docker-compose.yaml) MinIO 정의에 replication 설정 없음. `vlm-raw` 는 archive purge 후 **유일본**이 되는 구조 → MinIO 인스턴스 corruption/디스크 풀 = raw media 영구 소실.
- **1차 구현**: `mc mirror --watch local/vlm-raw nas/backup/vlm-raw` sidecar 컨테이너 (cron 도 가능). `pg_dump + restic` 후보(#1) 와 같은 restic 백엔드 재사용 가능.
- **codex 경고**: ⚠️ `mc mirror --remove` 플래그 **사용 금지** — 삭제 전파로 양쪽 동시 소실 위험.

### 9. Dagster `@run_failure_sensor` → Slack [Effort: S]

- **왜**: 현재 NAS 장애만 Slack 통보 ([sensor_nas_health.py](../../src/vlm_pipeline/defs/ingest/sensor_nas_health.py)). label/yolo/sam asset run 실패는 **무음**. `SLACK_WEBHOOK_URL` 미설정 시 완전 silent → MTTD = 운영자가 다음에 Dagster UI 켜는 시점.
- **구현**: Dagster 표준 `@run_failure_sensor` 한 파일. asset_checks(#2) 의 실패도 이 sensor 가 종착점.
- **codex 가드레일**: production job 만 scope + cooldown 필수, 아니면 Slack 알림 노이즈 폭탄.

---

## 🟡 Tier 2 추가

### 10. Vertex API 견고화 (503/deadline backoff + 호출/토큰 counter) [Effort: M]

- **왜**: [helpers_gemini.py:286](../../src/vlm_pipeline/defs/label/helpers_gemini.py) retry 는 JSON parse 전용. 429 는 `GeminiAnalyzer.analyze_video` 안에 부분 처리 있으나 **503/deadline 은 무방비**. 호출량·token usage·비용 counter 부재 → 청구서 사후 인지.
- **codex 정정**: "429 무방비" 표현은 과장. 실제 공백은 503/deadline + usage metering. Prometheus(#3) 도입 시 metric path 같이 묶기 권장.

### 11. Dataset 버전 태깅 (재현성) [Effort: M]

- **왜**: [build/assets.py:210](../../src/vlm_pipeline/defs/build/assets.py) 가 `dataset_id=uuid4()` + `version="v1"` 하드코딩. 동일 spec 재빌드 시 새 uuid + 고정 prefix 덮어쓰기 → 학습 결과 회귀 시 "어떤 dataset 으로 학습했는지" 추적 불가.
- **구현**: `build_version` 컬럼 + `vlm-dataset/<spec_hash>/<build_ts>/` prefix + DB 에 spec hash·git SHA 저장.

---

## 🟢 Tier 3 추가

### 12. Integration test (testcontainers) [Effort: M]

- **왜**: `tests/integration/` 디렉토리 없음. `scripts/verify_mvp.sh` 만 수동 e2e. 5/19 DuckDB→Postgres cutover 이후 migration 회귀를 CI 가 못 잡음. `DATAOPS_TEST_POSTGRES_DSN` 미설정 시 Postgres 테스트 전체 skip 됨.
- **구현**: `testcontainers-python` 으로 Postgres + MinIO fixture. [sql/migrations/postgres/001-003](../../src/vlm_pipeline/sql/migrations/postgres/) 적용 검증 자동화.
- **codex 코멘트**: Tier 3 로 잡혔지만 cutover 회귀 위험 대비 약간 저평가됨. 단 데이터 손실/알림 누락보다는 후순위 동의.

---

## 📌 Tier 1/2 완성 조건 (codex 추가 권고, 별도 후보 아님)

- **Restore drill 문서화** — pg_dump 백업(#1) + MinIO mirror(#8) 둘 다 "**복원 미검증 = theater**". 분기 1회 restore drill 절차 (`docs/runbook.md` 에 흡수) 가 없으면 백업은 위안에 불과. 각 후보의 Definition-of-Done 에 포함.
- **MinIO inventory/checksum audit** — 미러는 "복사됨"만 보장, "무결성"은 별개. `raw_files` DB ↔ `vlm-raw` 객체 존재/checksum 대조를 **asset_checks(#2) 한 항목으로 흡수** (별도 티켓 불필요).

---

## 🔬 코드 inline DQ 추가 (별도 프레임워크 불필요, 즉시 가능)

asset_checks(#2) 의 본격 도입과 별개로, **이미 흐름 안에 있는 dict/loop 에 1~5줄 추가**로 데이터 품질 게이트를 만들 수 있는 지점들. 코드 변경 risk 가 낮고 즉시 운영 효과 발생.

| 순위 | 위치 | 추가할 검증 | 이유 |
|---|---|---|---|
| 1 | [ops_normalize.py:309](../../src/vlm_pipeline/defs/ingest/ops_normalize.py#L309) | `duration_sec is None or < 1.0` → `failed` 처리 | duration=None 영상이 silent 통과 → Gemini 청크 분할 단계 division-by-zero. 코드 2줄. |
| 2 | [timestamp.py:140-149](../../src/vlm_pipeline/defs/label/timestamp.py#L140) | `end_sec > video_duration_sec` event 필터 | 영상 길이 초과 timestamp 가 LS task 로 들어가면 사람이 검수 불가. 후처리 필터 3줄 (chunking 시그니처 변경 회피). |
| 3 | [timestamp.py:149](../../src/vlm_pipeline/defs/label/timestamp.py#L149) | `event_count > 50` → warning log | Gemini 프롬프트 오작동/카테고리 매칭 폭주 조기 탐지. 1줄, 오버헤드 0. |
| 4 | [yolo/assets.py:218-287](../../src/vlm_pipeline/defs/yolo/assets.py) | bbox 좌표 범위 clamp (`x1<0`, `x2>width` 등) + bbox 개수 이상치 warning | 음수/초과 좌표가 LS task 로 들어가면 어노테이션 툴 깨짐. detection loop 안 guard. |
| 5 | 새 cross-table sensor (또는 dispatch sensor tick 말미) | `ingest_status='completed'` ↔ `video_metadata` row 존재 / `timestamp_status='completed'` ↔ `timestamp_label_key IS NULL` 카운트 | 파이프라인 중간 탈락을 운영자가 감지 못하는 가장 흔한 케이스. SELECT 2개. |

**원칙**:
- A~C/D 단계 (ingest/label/yolo) → **기존 함수에 inline 추가**. 이미 dict/loop 안에서 처리 중이라 오버헤드 0.
- E (dataset build) → **별도 asset_check 분리** 권장. build 완료 이후 post-check 성격.
- F (cross-table) → **주기적 sensor 또는 운영 스크립트** 분리. 매 asset run 마다 돌 필요 없음.

**Open thread**:
- [sam/assets.py:148-149](../../src/vlm_pipeline/defs/sam/assets.py#L148) 의 `image_width=0, image_height=0` 기본값은 bbox 검증을 무력화시킴 (`x2>0` 항상 통과). bbox 검증 추가 전 실제 dimension 채우는 수정이 선행되어야 함.

---

## 만약 이번 스프린트에 딱 N개만 한다면

**codex 가 재정렬한 우선순위 (#8~#12 기준)**: 8 → 9 → 11 → 12 → 10

기존 2개에 합치면 **권장 4개**:

1. **#1 Postgres 자동 백업** (pg_dump + restic)
2. **#8 MinIO mirror** (sidecar + `mc mirror --watch`)
3. **#2 Dagster asset_checks** (3개 + MinIO checksum audit 1개 = 4개)
4. **#9 run_failure_sensor → Slack** (sensor 1개 파일)

네 개 모두 Effort S~M. 이 넷 + 위 DQ inline 1~3순위 (코드 6줄) 까지 끝내야 Prometheus·Loki 가 "감시할 가치 있는 신호" 위에 깔리는 순서가 됨.

codex 와 Claude 의 의견이 **이 네 가지에 대해 일치**. #10 (Vertex 견고화) 만 다음 스프린트로 미루는 데 양쪽 동의.

---
