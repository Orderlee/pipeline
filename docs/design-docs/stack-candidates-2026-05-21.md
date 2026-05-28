# 추가하면 좋을 스택 후보 (cross-validated, 우선순위 순)

> 작성일: 2026-05-21 (2026-05-22 #8~#12 + DQ inline 섹션 추가, codex 2차 검토 반영)
> **갱신: 2026-05-28** — QA 시나리오 24 캠페인 종료 후 사실관계 반영
> 작성자: ywl (Claude Opus 4.7 + Codex 교차 검토)
> 용도: 다음 스프린트 작업 후보 정리

## 📌 2026-05-28 갱신 요약

**2026-05-22~28 QA 시나리오 24 캠페인 종료**. 그 결과:

- **부분 해결됨**: #9 run_failure_sensor(기반 sensor 있음, Slack 훅 미발화), #4 MinIO retry/timeout(PR #106에서 적용 — Vertex는 미적용)
- **사실관계 갱신**: YOLO 비활성(`ENABLE_YOLO_DETECTION=false`)·SAM3 GPU 1 단독·prod `DATASET_REQUIRE_LS_FINALIZED=1` (compose default 0)
- **신규 도출 후보(아래 §13)**: N1 NAS 10GbE, N2 NAS NFS 데몬 watchdog, N3 GPU mem 임계 알림, N4 SAM3 동시 dispatch throughput 측정
- **새로 적용된 안정화 fix(아래 §14)**: PR #92(EXDEV), #93(sam3 공유), #94(archive timeout), #103(SAM3 retry+workers=2+expandable_segments), #106(sensor NFS probe + boto3 retry)
- **여전히 미해결 핵심**: #1 PG 자동 백업, #2 asset_checks, #3 Prometheus, #8 MinIO mirror, #10 Vertex 503/deadline, #11 dataset 버전 태깅, #12 integration test, inline DQ #1~3·#5

## 배경

VLM Data Pipeline 전체 스택을 훑고 "어떤 스택을 추가하면 좋을지" 를 두 시각으로 교차 검토한 결과.

핵심 사실 한 가지가 모든 추천의 기반:
**2026-05-19 에 DuckDB → Postgres cutover 완료** 한 직후이고, 현재 `vlm_pipeline` Postgres 에 대한 **자동 백업이 없음**.

---

## 🔴 Tier 1 — 이번 스프린트에 꼭

### 1. pg_dump + restic (또는 pgBackRest) → Postgres 백업 자동화 [Effort: S~M]

- **상태 (2026-05-28)**: ❌ 미완료. cutover 후 8일 경과, 여전히 백업 0.
- **왜 1순위**: Postgres 가 모든 라벨/dispatch/dedupe 상태의 single source of truth 인데 [docker-compose.yaml](../../docker/docker-compose.yaml) 에는 `postgres_data` 볼륨만 있고 백업 cron/sidecar 가 없음. cutover 직후의 미백업 상태는 "복구 불가" 리스크.
- **codex 코멘트**: *"Prometheus-before-backups is wrong for this pipeline. Broken dashboards are annoying; an un-restorable Postgres volume is existential."* ← 핵심 disagreement.
- **위험**: untested backups = theater. 분기 1회 restore drill 필수.

### 2. Dagster `asset_checks` + Pandera → 파이프라인 내장 DQ 게이트 [Effort: M]

- **상태 (2026-05-28)**: ❌ 미완료. `DATASET_REQUIRE_LS_FINALIZED` 는 **prod `.env`에서 1로 override** 되어 운영 위험은 일부 완화 — 단 compose default 가 0이라 신규 환경 배포 시 재발 가능. 게이트 자체는 여전히 부재.
- **왜**: Postgres ↔ MinIO ↔ Label Studio 간 진실의 분기가 많음. 특히 `DATASET_REQUIRE_LS_FINALIZED=0` 이 기본값(compose)이라 **검수 안 된 데이터가 dataset 에 섞일 수 있는 구조**가 코드에 명시되어 있음. Dagster 1.6+ 의 `@asset_check` 가 이미 import 가능해서 별도 프레임워크 없이 시작 가능.
- **점검 후보**:
  - `labels.timestamp_status='completed'` ↔ MinIO `vlm-labels/<src>/events/*.json` 존재 일치
  - `raw_files.ingest_status='completed'` ↔ archive 이동 일치
  - dispatch row 수 급변 anomaly

---

## 🟡 Tier 2 — 큰 ROI, 다음 분기

### 3. Prometheus + node_exporter + nvidia_gpu_exporter → 이미 있는 Grafana 살리기 [Effort: M]

- **상태 (2026-05-28)**: ❌ 미완료. 2026-05-27 NAS·GPU OOM 인시던트에서도 metric 부재로 사후 진단만 가능 — 도입 필요성 ↑.
- **왜**: Grafana 12.3.1 컨테이너와 [grafana/minio-dashboard.json](../../docker/grafana/) 이 이미 있고 MinIO 가 `MINIO_PROMETHEUS_AUTH_TYPE: public` 으로 메트릭 노출 중. **수집 에이전트 한 개 추가하면 즉시 동작**.
- **갱신된 GPU 컨텐션 근거 (2026-05-28)**:
  - YOLO 는 정책상 비활성 (`ENABLE_YOLO_DETECTION=false`, SAM3.1 단독 운영). 컨테이너는 떠있어도 GPU inference 안 함.
  - SAM3 는 `CUDA_VISIBLE_DEVICES=1` 로 GPU 1 단독, `SAM3_WORKERS=3` (3×~3.7GB=11.1GB, 여유 ~5GB).
  - dagster 는 `CUDA_VISIBLE_DEVICES=0,1` (Places365=GPU 0, NVENC=GPU 0/1 round-robin).
  - 실제 contention: **GPU 1 = SAM3(11.1GB) + NVENC(burst) + 외부 사용자(예: jsh ComfyUI 782MB)** — 2026-05-27 OOM 발생 지점. 모니터링이 시급한 이유는 그대로(이번에는 OOM 사전 알림 부재로 part1 ~250 프레임 손실).

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

- **상태 (2026-05-28)**: ❌ 미완료. **2026-05-27 인시던트가 정확히 이 갭을 가리킴** — MinIO와 NFS가 같은 NAS 10.0.0.51에 있어 그 NAS 1대 장애(이번엔 NFS 데몬 down) 시 raw media + 데이터 동시 영향. 우선순위 ↑.
- **왜**: [docker-compose.yaml](../../docker/docker-compose.yaml) MinIO 정의에 replication 설정 없음. `vlm-raw` 는 archive purge 후 **유일본**이 되는 구조 → MinIO 인스턴스 corruption/디스크 풀 = raw media 영구 소실.
- **1차 구현**: `mc mirror --watch local/vlm-raw nas/backup/vlm-raw` sidecar 컨테이너 (cron 도 가능). `pg_dump + restic` 후보(#1) 와 같은 restic 백엔드 재사용 가능.
- **codex 경고**: ⚠️ `mc mirror --remove` 플래그 **사용 금지** — 삭제 전파로 양쪽 동시 소실 위험.

### 9. Dagster `@run_failure_sensor` → Slack [Effort: S]

- **상태 (2026-05-28)**: 🟡 **부분 완료** — [sensor_run_status.py:123 `dispatch_run_failure_sensor`](../../src/vlm_pipeline/defs/dispatch/sensor_run_status.py#L123) 가 `@run_status_sensor(run_status=FAILURE)` 로 구현돼 있음. 단 **DB `dispatch_requests.status='failed'` 업데이트만** 하고 **Slack 알림 미발화**. 종착점 sensor 골격은 갖춰졌으니 Slack 훅(+ cooldown/scope) 추가만 하면 됨.
- **왜**: 현재 NAS 장애만 Slack 통보 ([sensor_nas_health.py](../../src/vlm_pipeline/defs/ingest/sensor_nas_health.py)). label/yolo/sam asset run 실패는 **무음**. `SLACK_WEBHOOK_URL` 미설정 시 완전 silent → MTTD = 운영자가 다음에 Dagster UI 켜는 시점.
- **남은 구현**: 기존 `dispatch_run_failure_sensor` 에 Slack post(중복 방지 cooldown 포함) 추가. asset_checks(#2) 의 실패도 이 sensor 가 종착점.
- **codex 가드레일**: production job 만 scope + cooldown 필수, 아니면 Slack 알림 노이즈 폭탄.

---

## 🟡 Tier 2 추가

### 10. Vertex API 견고화 (503/deadline backoff + 호출/토큰 counter) [Effort: M]

- **상태 (2026-05-28)**: ❌ Vertex 측 미완료. **MinIO 측은 별건으로 PR #106 에서 해결** (boto3 connect/read timeout + adaptive retry, env override). Vertex Gemini 호출 retry/metering 은 여전히 부재.
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
| 4 | [yolo/assets.py:218-287](../../src/vlm_pipeline/defs/yolo/assets.py) | bbox 좌표 범위 clamp (`x1<0`, `x2>width` 등) + bbox 개수 이상치 warning | 음수/초과 좌표가 LS task 로 들어가면 어노테이션 툴 깨짐. detection loop 안 guard. **(2026-05-28: `ENABLE_YOLO_DETECTION=false` 정책으로 우선순위 ↓, YOLO 재활성 시 복귀)** |
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

## 🆕 §13 — 2026-05-22~28 캠페인에서 새로 도출된 후보

### 🔴 N1. NAS 10.0.0.51 10GbE 활성화 (하드웨어 업그레이드) [Effort: L, 인프라 트랙]

- **왜**: QA 시나리오 24 진행 중 호스트(10.0.0.10) eno1·NAS 모두 **1GbE**로 잡힘 (`Supported link modes: 10baseT/Half 10baseT/Full`, Speed 1000Mb/s). 대규모 dispatch(1250+1250) 시 MinIO + NFS 동시 트래픽이 1GbE 포화 → load 244·393 spike·upload crawl·NFS 데몬 다운 인시던트의 **underlying cause**.
- **비용**: 호스트 10GbE NIC(예: Intel X550-T2) + NAS 10GbE 포트 활성 + 10GbE 스위치 포트 + 케이블 + 다운타임. 단순 설정 아닌 **구매·승인·물리 작업 트랙**.
- **ROI**: 매우 높음 — 코드 fix(#1 retry, #3 probe, #4 boto3 retry)는 증상 완화일 뿐, 근본은 대역폭.

### 🔴 N2. NAS NFS 데몬 watchdog / auto-restart (ASUSTOR ADM) [Effort: S, NAS-side]

- **왜**: 2026-05-27 NFS 데몬(nfsd/mountd)이 부하 중 죽음 → `rpcinfo -p 10.0.0.51` 에서 등록 사라짐 → 호스트 마운트 끊김 → 컨테이너 D-state pileup → load 393 폭증. 호스트 측 `x-systemd.automount` 는 추가됐지만(자동 재마운트) **NAS 데몬 자체가 살아있어야** 동작.
- **구현**: ADM 콘솔에서 NFS 서비스 watchdog (또는 cron 으로 5분 간격 `nfsd` 프로세스 체크 + 죽으면 재시작), 헬스 로그를 호스트로 syslog forward.
- **비용**: NAS admin 작업 (사용자가 ADM 에서 직접).
- **ROI**: 높음 — 단일 실패 모드 재발 방지.

### 🟡 N3. GPU 메모리 임계치 사전 알림 (#3 Prometheus 종속) [Effort: S]

- **왜**: 2026-05-27 SAM3 GPU1 OOM(CUDA out of memory, 503) 이 GPU mem 98%(여유 11MB)에 도달한 **후에야** 발생 — 사전 경보 0. workers=4 와 jsh ComfyUI 공유 누적이 임계 도달 직전이었음에도 알림 없음.
- **구현**: nvidia_gpu_exporter(#3) 도입 후 `gpu_memory_used / gpu_memory_total > 0.85` 5분 sustained 시 Slack alert. #9 run_failure_sensor 와 같은 알림 종단 사용.
- **ROI**: 중간 — #3 도입의 자연 후속.

### 🟢 N4. SAM3 동시 dispatch throughput 측정 [Effort: S, 측정 only]

- **왜**: `SAM3_WORKERS=3` (2026-05-28 적용)의 이득은 **단일 run엔 없음, 동시 다중 bbox dispatch 시에만 발현** ([detection_assets.py:198-234](../../src/vlm_pipeline/defs/sam/detection_assets.py#L198) 가 `/segment` 순차 호출이라 1 worker만 활성). 실제로 동시 dispatch 가 운영에서 얼마나 발생하는지·workers=3이 throughput을 얼마나 올리는지 측정 데이터 없음.
- **구현**: bbox 폴더 2~3개 동시 dispatch 시나리오 한 번 (Effort S). 데이터로 workers 적정값 재평가.
- **ROI**: 낮음~중간 — 운영 의사결정 데이터.

---

## 🆕 §14 — 2026-05-22~28 캠페인에서 prod 적용된 안정화 fix

해당 PR들이 main 머지·prod 배포 완료. 향후 본 문서의 후속 작업 우선순위 산정 시 이미 적용된 baseline 으로 간주.

| PR | 변경 | 효과 |
|---|---|---|
| #80 | dispatch limit 50→100000 | Gemini 50× 영상 처리 |
| #85 | `archive_move.py` shutil.move(directory) → `os.rename` | Python 3.10/11/12 copytree 폴백 회피 (디렉토리 archive 가 분→초) |
| #87 | `ls_task_create_sensor` in-flight 가드 | 동일 job 4× 누적 발화 방지 |
| #82 | `ls_tasks` timeout 600→1800s + API key 마스킹 | 12,532 frame LS 등록 회복, secrets 누출 차단 |
| #90/#91 | NVENC dual-GPU round-robin + ffmpeg arg 순서 fix | `-c:v h264_nvenc -gpu N` 정상 순서 → reencode fallback 0 |
| **#92** | **incoming/archive 공통 부모 단일 bind `/nas/data`** | **컨테이너 EXDEV 해소 → archive_finalize 3-4h → <1초 (시나리오 24 핵심 효과)** |
| #93 | sam3 `profiles:["sam3"]` 단일 공유 컨테이너 + `SAM3_API_URL` override | staging↔prod 포트 8002 충돌 해소 |
| #94 | `archive_move_timeout` 300→600s + 회복 케이스 로그 INFO 강등 | 알람 노이즈 ↓ |
| #103 | SAM3 detection `_retry_transient`(503/timeout backoff) + `SAM3_WORKERS` default 4→2 + `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True` | NAS blip 시 ~250 프레임 영구 손실 차단 + GPU1 OOM 근본 차단(여유 ~5-9GB) |
| **#106** | **`probe_path_reachable()` sensor 가드 + boto3 `connect_timeout=10`/`read_timeout=30`/`adaptive retries`** | sensor 300s `DEADLINE_EXCEEDED` 차단 + MinIO transient blip 흡수 |

운영 `.env`: `SAM3_WORKERS=3` (동시 dispatch 대비 OOM 안전선 안에서 throughput ↑), `COMPOSE_PROFILES=sam3`, `NAS_DATA_ROOT=/home/user/mou/nas_200tb`, `DATASET_REQUIRE_LS_FINALIZED=1`.

### 검증된 throughput (시나리오 24, S2 baseline 대비)
- part2 ts+cap: **3h 01m 47s (−27%)** — qa_s24v3
- part1 bbox: **5h 29m 13s (−29%)** — qa_s24v4
- archive_finalize: **<1초** (폴더 fast-path) — 양쪽 run 동일 초 start→done

---
