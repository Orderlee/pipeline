# AGENTS.md — VLM Data Pipeline (Codex / 자동화 에이전트용)

이 파일은 **`CLAUDE.md`와 동일한 사실·우선순위**를 바탕으로, Codex 등 도구가 읽기 쉬운 형태로 정리한 **짧은 운영 컨텍스트**입니다.  
코드만 보면 알 수 있는 구현 디테일은 생략합니다.

## 더 긴 참고 문서

| 문서 | 용도 |
|------|------|
| `CLAUDE.md` | Claude Code / Cursor용 요약(본 문서와 쌍) |
| `CLAUDE2.md` | DuckDB 스키마 표·장문 플레이북·검증 이력 등 **전체 레퍼런스** |
| `.cursor/rules/` | Cursor 전용 규칙(한국어 응답, 역할 분담 등) |
| `.agent/CONTEXT.md` | 범용 에이전트 컨텍스트(있는 경우) |

---

## 프로젝트 한 줄

CCTV/보안 영상 수집 → 중복 제거 → **Gemini(Vertex)** 라벨링 → **YOLO-World** 검출 → 학습 데이터셋 빌드 — **Dagster + DuckDB + MinIO** 기반 파이프라인.

---

## 디렉터리

- `src/vlm_pipeline/` — MVP 파이프라인 패키지 (`definitions.py`, `defs/`, `lib/`, `sql/`)
- `docker/` — Compose, Dagster workspace, `.env` / `.env.staging`
- `scripts/` — DuckDB 쿼리, 백필, 검증, staging dispatch 보조
- `gcp/` — GCS → incoming 다운로드

---

## Production vs Staging

| 항목 | Production | Staging |
|------|------------|---------|
| DuckDB (컨테이너) | `/data/pipeline.duckdb` | `/data/staging.duckdb` |
| MinIO API | `http://172.168.47.36:9000` | `http://172.168.47.36:9002` |
| Dagster UI 포트 | `3030` | `3031` |
| Incoming | `/nas/incoming` | `/nas/staging/incoming` |
| `DAGSTER_HOME` | `/app/dagster_home` | `/app/dagster_home_staging` |
| env | `docker/.env` | `docker/.env.staging` |
| `IS_STAGING` | unset | `true` |

Staging은 **`docker compose --profile staging`** 으로만 기동. Production과 **스토리지·런타임 분리** 필수(heartbeat 충돌 방지).

---

## 파이프라인 흐름 (요약)

- **운영(MVP):** incoming → manifest 센서 → INGEST → DEDUP → LABEL → PROCESS → BUILD → (MotherDuck sync 옵션)
- **Staging(dispatch):** `incoming/.dispatch/pending/*.json` → `dispatch_sensor` → `dispatch_stage_job` 등 (`definitions_staging.py`)

---

## 코딩·품질

- Python **3.10+**, **ruff** (line length 120)
- Dagster: **`@asset` 우선**
- Import **5-layer** — 하위에서 상위 import 금지 (`lib` → `ops` → `assets/sensors` → `definitions`)
- 커밋: **conventional commits** (`feat:`, `fix:`, …)
- 에러: **per-file fail-forward**
- 테스트: pytest, in-memory DuckDB, MinIO mock (`moto[s3]`)

---

## 운영 규칙 (필수)

- **DuckDB:** 단일 writer — job에 `tags={"duckdb_writer": "true"}`, 코디네이터에서 동시 1개
- **MinIO 버킷(고정):** `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset` — `raw_key = <source_unit>/<rel_path>` (YYYY/MM prefix 금지)
- **라벨 JSON SoT:** `vlm-labels`만 (processed에 이벤트 JSON 중복 저장 금지)
- **파일 오류:** DB 미삽입·archive 미이동, JSONL 실패 로그로만 추적
- **transient DB lock:** retry manifest 경로 (failed row로 쌓지 않음)

---

## 자주 쓰는 명령

```bash
pip install -e ".[dev]"
pytest tests/unit -q && pytest tests/integration -q
cd docker && docker compose up -d
cd docker && docker compose --profile staging up -d dagster-staging
python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
```

**MotherDuck sync dry-run (운영 컨테이너 예시):**

```bash
docker exec pipeline-dagster-1 python3 /src/python/local_duckdb_to_motherduck_sync.py \
  --local-db-path /data/pipeline.duckdb --db pipeline_db --share-update AUTOMATIC --dry-run
```

---

## 외부 서비스

- **Gemini / Vertex:** 프로젝트·리전·credential 우선순위는 `CLAUDE.md` 동일. 대형 영상은 preview 경로(Vertex 용량 한도).
- **YOLO:** `yolov8l-worldv2.pt`, 컨테이너에 `clip` 의존성 필요.
- **GCS 수집:** `gcp/download_from_gcs_rclone.py`, 스케줄 `gcs_download_schedule`
- **MotherDuck:** `MOTHERDUCK_TOKEN`, `MOTHERDUCK_DB`, `MOTHERDUCK_SHARE_UPDATE` 등 — 상세는 `CLAUDE.md` / sync 스크립트 주석

---

## 에이전트 작업 시

- 스키마·테이블 DDL은 **`src/vlm_pipeline/sql/schema.sql`** 및 **`CLAUDE2.md`** 와 정합 유지
- Docker/경로 변경 시 **staging / production 분리** 재확인
- 저장소 규칙상 **코드 직접 수정**은 사용자가 명시할 때만(Cursor 역할 분담) — Codex MCP 경로 우선

---

*본 문서는 `CLAUDE.md`를 단일 소스로 두고 동기화합니다. 장문 표·이력은 `CLAUDE2.md`를 사용하세요.*
