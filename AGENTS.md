# AGENTS.md — VLM Data Pipeline

CCTV 영상·이미지 수집 → 중복 제거 → Gemini(Vertex) 이벤트 라벨링 → SAM3 bbox → Label Studio 검수 → 학습셋 구축.
Dagster + PostgreSQL + MinIO. DuckDB는 `pg_duckdb` 분석 전용; YOLO는 기본 비활성(`ENABLE_YOLO_DETECTION=false`).

## 사고 방지

- 이 호스트 checkout은 운영 배포 루트. `src/`, `configs/`, `scripts/`, Compose 수동 핫픽스 금지 — 배포의 `rsync --delete` + `git reset --hard`가 덮어씀.
- `main` 배포는 Dagster 3개 서비스를 재생성하여 실행 중 작업을 끊음. 배포·재시작은 작업 범위부터 확인.
- Compose는 `scripts/compose-prod.sh` / `scripts/compose-staging.sh`만 사용 — project/env 누락 시 운영을 건드림. staging wrapper는 별도 `_test` clone에서 실행.
- `.env`·인증 파일의 비밀 값 출력·문서화 금지. `scripts/archive/`는 폐기 코드 — 운영 명령으로 안내하지 않음.

## 구현·데이터 계약

아래 패키지 경로는 `src/vlm_pipeline/` 기준.

- DB 변경은 `PostgresResource`(`db`)로. 센서 조회는 `lib/sensor_db.py` 사용, 조회 연결에 write 금지.
- import 방향: `definitions*` → `defs/` → `resources/`·`lib/`. `lib/`(L1–2)는 `dagster`, `vlm_pipeline.{defs,resources,ops}` import 금지 — lazy import도 검사 대상.
- MinIO 버킷 5개 고정: `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification`. 라벨 JSON 정본은 `vlm-labels`; processed에 중복 저장 금지.
- `raw_key`는 정규화한 `<source_unit_name>/<rel_path>`; `YYYY/MM` 금지. file unit·GCP 예외는 `defs/ingest/ops_register.py`, `lib/env_utils.py` 확인.
- `dispatch_stage_job`에 clip 분할 추가 금지. 사람 `finalized` 후 `post_review_clip_job`; dataset은 `DATASET_REQUIRE_LS_FINALIZED=1` 유지.
- 파일 오류는 실패 기록 후 나머지 진행. 검증 실패 파일은 DB 등록·archive 이동 제외.
- GCP manifest: `pending → processed → completed(summary)`. `_DONE` 후 chunk 이력 대신 unit/signature 요약 유지 (`defs/ingest/compaction.py`).
- PG migration은 forward-only. 기존 파일명 변경 금지 — 적용 이력 키는 전체 파일명. 두 `030_*`도 유지.

## 검사

저장소 루트, 의존성 있는 venv에서 실행(`pyproject.toml` 미추적). 이 호스트의 PATH `python`은 깨진 venv → `/home/user/anaconda3/bin/python` 명시.
새 테스트는 `.gitignore`의 `!tests/unit/<파일>` allowlist에 넣어야 CI가 돈다(확인 `git ls-files tests/`).

```bash
PYTHONPATH=src python -m pytest tests/unit -q --tb=short
python3 scripts/check_lib_layer_imports.py
ruff check src/ tests/
ruff format --check src/ tests/
```

Ruff는 CI와 같은 `0.7.4` / `ruff.toml`. 통합 검사는 **별도 테스트 PG**의 `DATAOPS_TEST_POSTGRES_DSN`을 지정한 뒤 `PYTHONPATH=src python -m pytest tests/integration -q` — fixture가 DB 생성·삭제하며 운영 DSN으로도 fallback함.

## 필요한 문서만 읽기

코드 변경 전 `.agent/skill/*/SKILL.md`에서 해당 절차 검색. 전체 README·CLAUDE를 선독하지 않음.

| 작업 | 진입점 |
|---|---|
| 라벨링·dispatch | `docs/logic/Auto_Labeling_기능_명세서.md` + `src/vlm_pipeline/definitions_production.py` |
| DB·키 | `src/vlm_pipeline/sql/migrations/postgres/*.sql` 헤더 + `lib/key_builders.py` |
| 배포·환경 | `docs/references/deployment-guide.md` + `scripts/deploy/deploy-stack.sh` |
| LS 연동 | `docs/references/label-studio-ops-guide.md` + `src/gemini/ls_*.py` |
| 학습 | `.agent/skill/mlops-finetune/SKILL.md` |
| 리뷰·협업 | `REVIEW.md` / `docs/references/{multi-agent,agent-teams}.md` |
| 그 외 | `docs/index.md` → 해당 하위 index; 운영 맥락은 `CLAUDE.md` 해당 절 |

공유할 설계·운영 판단은 `docs/`에 기록. `.gitignore`의 로컬 전용 문서는 공유 근거로 인용하지 않음.
