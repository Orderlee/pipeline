# CLAUDE.md — VLM Data Pipeline

> 코드를 읽으면 아는 것은 생략. 코드만으로는 알 수 없는 규칙·환경·운영 맥락만 기록.

---

## 프로젝트 한 줄 요약

CCTV/보안 영상을 수집 → 중복제거 → Gemini 라벨링 → YOLO 검출 → 학습 데이터셋 빌드하는 **Dagster 기반 미디어 데이터 파이프라인**.

---

## 🤖 AI Agent Core Action Rules

- **Skill Discovery First:** 사용자가 작업을 지시하면, 스스로 코드를 처음부터 짜기 전에 반드시 시스템 도구를 거쳐 `.agent/skill/` 디렉토리를 먼저 검색하세요.
- 요청과 관련된 스킬 문서가 발견되면, 해당 문서(`SKILL.md`)의 지침을 완벽하게 읽고 그 룰에 맞추어 작업을 수행하세요.

---

## 빌드 & 실행

```bash
# 의존성 설치 (editable)
pip install -e ".[dev]"

# 로컬 테스트
pytest tests/unit -q
pytest tests/integration -q

# Docker (production)
cd docker && docker compose up -d

# Docker (staging) — profile 분리
cd docker && docker compose --profile staging up -d dagster-staging

# Dagster UI
#   production : http://<HOST>:3030
#   staging    : http://<HOST>:3031

# DuckDB 쿼리 (호스트에서 직접)
python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
```

---

## 환경 이중 구조 (Production vs Staging)

| 항목 | Production | Staging |
|------|-----------|---------|
| DuckDB | `/data/pipeline.duckdb` | `/data/staging.duckdb` |
| MinIO endpoint | `http://172.168.47.36:9000` | `http://172.168.47.36:9002` |
| MinIO Console | `:9001` | `:9003` |
| Dagster port | `3030` | `3031` |
| Incoming | `/nas/incoming` | `/nas/staging/incoming` |
| Archive | `/nas/archive` | `/nas/staging/archive` |
| DAGSTER_HOME | `/app/dagster_home` | `/app/dagster_home_staging` |
| env file | `docker/.env` | `docker/.env.staging` |
| `IS_STAGING` | (unset) | `true` |

Staging은 `docker compose --profile staging`으로만 기동. Production과 **Dagster storage/runtime을 완전 분리**해야 heartbeat 충돌 없음.

---

## 코딩 규칙

- **Python 3.10+**, formatter/linter: `ruff` (line-length 120)
- **Dagster**: `@asset` 우선, `@op+@job` 필요 시만
- **Import 계층** — 코드에 5-layer 주석 있음. 하위→상위 import 금지
  - L1-2: `lib/` (순수 Python, key_builders 포함) → L3: `ops` → L4: `assets/sensors` → L5: `definitions.py`
  - `lib/spec_config.py`는 순수 태그 파싱만. DB 의존 함수는 `defs/spec/config_resolver.py`에 위치
  - MinIO 키 빌더는 `lib/key_builders.py`에 통합. 각 `defs/` 모듈은 thin wrapper로 위임
- **모듈 분할 규칙** — 대형 파일은 도메인별 서브모듈로 분할
  - `defs/process/`: `assets.py`(라우팅) + `helpers.py` + `captioning.py` + `frame_extract.py` + `raw_frames.py`
  - `defs/label/`: `assets.py`(라우팅) + `label_helpers.py` + `timestamp.py` + `artifact_*.py`
  - `resources/`: `duckdb_base.py` + `duckdb_phash.py` + `duckdb_migration.py` + `duckdb_ingest_*.py`
- **커밋**: conventional commits (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`)
  - "어떻게 수정했다"보다 **"무엇과 왜 수정했는지"** (`.gitmessage.txt` 참고)
- **에러 처리**: per-file fail-forward — 한 파일 실패해도 나머지 계속 처리
- **테스트**: pytest, in-memory DuckDB fixture, mocked MinIO (`moto[s3]`), `tests/conftest.py` 공통 fixture

---

## 핵심 운영 규칙 (코드에 안 드러나는 것)

### DuckDB 동시성
- DuckDB = 단일 파일 write lock ⇒ `tags={"duckdb_writer": "true"}` 필수
- `run_coordinator`에서 `duckdb_writer` tag concurrency=1로 제한

### NFS/NAS 장애 대응
- sensor에서 `OSError/PermissionError/TimeoutError` → graceful skip, 다음 tick 재시도
- NAS 지연 시 권장 설정:
  - `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES=20`
  - `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=3`
  - `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300`

### 파일 오류 정책
- `file_missing`, `empty_file`, `ffprobe_failed` → **DB 미삽입 + archive 미이동**
- 추적은 JSONL 실패 로그(`<manifest_dir>/failed/*.jsonl`)에만 기록
- transient 오류(DuckDB lock) → retry manifest 자동 생성, failed row 아님

### Archive 이동
- `source_unit_type=directory`이고 모든 파일 성공 → 폴더째 archive 이동
- chunked manifest → 파일 단위 누적 이동 (조기 폴더 이동 방지)
- archive 폴더명 충돌 → `__2`, `__3` suffix 자동 분기
- archive 이동 완료된 파일**만** `ingest_status=completed` 유지

### MinIO 버킷/경로 정책
- `vlm-raw` · `vlm-labels` · `vlm-processed` · `vlm-dataset` (4개 고정)
- `raw_key = <source_unit_name>/<rel_path>` — `YYYY/MM` prefix 금지
- 이벤트 JSON source of truth = `vlm-labels`만. `vlm-processed`에 중복 저장 금지

### Staging 초기화 (깨끗한 재테스트)
1. `dagster-staging` 중지
2. staging MinIO 4개 버킷 객체 전체 삭제
3. `docker/data/staging.duckdb` 삭제
4. `docker/data/dagster_home_staging/storage/` 삭제
5. 재기동
- ⚠️ staging incoming/archive 원본 폴더는 명시 요청 없으면 **절대 삭제 금지**

---

## 서비스 네트워크 & 볼륨 (코드에서 놓치기 쉬운 것)

- Docker network: `pipeline-network`
- **호스트 ↔ 컨테이너 경로 매핑** (compose의 bind mount):
  - `/home/pia/mou/incoming` → `/nas/incoming`
  - `/home/pia/mou/archive` → `/nas/archive`
  - `/home/pia/mou/staging` → `/nas/staging` (staging only)
  - 코드→실행 경로: `../src` → `/src/vlm` (read-only)
- DuckDB **호스트 실제 경로**: `./docker/data/pipeline.duckdb`
- YOLO 서버: GPU 1번 전용 (`cuda:1`, `NVIDIA_VISIBLE_DEVICES=1`)
- Places365 모델 캐시: `/data/models/places365` (auto_download=false, 고정 캐시만 사용)
- `PYTHONPATH` (컨테이너): `/:/src/python:/src/vlm`

---

## 자주 쓰는 스크립트

| 스크립트 | 용도 |
|---------|------|
| `scripts/query_local_duckdb.py` | 로컬 DuckDB 읽기 쿼리 (lock 회피 fallback 포함) |
| `scripts/backfill_video_metadata.py` | video_metadata 결손 백필 |
| `scripts/cleanup_duplicate_assets.py` | checksum duplicate 정리 |
| `scripts/recompute_archive_checksums.py` | archive 원본 재해시 |
| `scripts/reupload_minio_from_archive.py` | archive 기준 MinIO 재업로드 |
| `scripts/staging_test_dispatch.py` | staging dispatch 테스트 |
| `scripts/verify_mvp.sh` | E2E 검증 |

---

## Label Studio 연동

- compose: `docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d`
- LS UI: `http://<HOST>:8084` (기본 8080이나  -agent 충돌로 `LS_PORT=8084` 사용)
- 필수 env: `LS_API_KEY` (LS 계정 설정에서 발급), `WEBHOOK_HOST` (LS→webhook 접근 IP)
- sensor `ls_task_create_sensor`: Dagster UI에서 수동 ON 필요 (default=STOPPED)
- webhook 등록 (프로젝트별): `python src/gemini/ls_webhook.py register --project <id>`
- presigned URL 만료(기본 7일) 시: `python src/gemini/ls_tasks.py renew --project-name <name>`
- Slack 알림/slash command는 `SLACK_WEBHOOK_URL`, `SLACK_SIGNING_SECRET` 설정 시 활성화

---

## GCS 외부 수집

- 버킷: `khon-kaen-rtsp-bucket` (주), `adlibhotel-event-bucket`, `kkpolice-event-bucket`
- 스크립트: `gcp/download_from_gcs_rclone.py`
- Dagster schedule: `gcs_download_schedule` (매일 04:00 KST)
- 0바이트 파일 복구: `GCS_ZERO_BYTE_RETRIES` (기본 2)

---

## DuckDB 파일 교체 시 주의

1. **반드시 서비스 중지** 후 교체
2. 기존 `.wal` 파일 존재 여부 확인 → 있으면 백업 후 삭제
3. stale WAL이 새 DB에 재적용되면 corruption 발생

---

## Gemini / Vertex AI

- 프로젝트: `gmail-361002`, 리전: `us-central1`
- 기본 모델: `gemini-2.5-flash`
- credential 우선순위: `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` → `GOOGLE_APPLICATION_CREDENTIALS` → `GEMINI_SERVICE_ACCOUNT_JSON`
- 450MB 초과 영상 → preview mp4 자동 생성 (Vertex 524MB 제한 회피)

---

## YOLO-World

- 모델: `yolov8l-worldv2.pt` (`/data/models/yolo/`)
- dependency 함정: `clip` 패키지 없으면 컨테이너 부팅 실패 → `git+https://github.com/ultralytics/CLIP.git` 필요
- health check: `GET /health` → `model_loaded=true`
