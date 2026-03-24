# ANTIGRAVITY.md — VLM Data Pipeline

Google Antigravity(또는 Knowledge Item으로 임포트할 때)용 **1페이지 진입점**입니다.  
Antigravity는 이 파일을 **자동으로 읽지 않을 수 있으므로**, 작업 시작 시 이 문서를 열거나 Knowledge로 등록해 두세요.

## 무엇을 하느냐

**CCTV/보안 영상** → Ingest → Dedup → **Gemini** 라벨 → **YOLO** 검출 → 데이터셋 빌드 (**Dagster + DuckDB + MinIO**).

## 먼저 읽을 파일 (짧은 순)

1. **`CLAUDE.md`** — 운영·환경·빌드·Staging vs Prod (가장 짧고 밀도 높음)
2. **`AGENTS.md`** — Codex/에이전트용 요약(본 프로젝트와 동일 맥락, 표 형식)
3. **`CLAUDE2.md`** — DuckDB 8테이블 표, 긴 플레이북·검증 이력

## 디렉터리 한 줄

- `src/vlm_pipeline/` — 파이프라인 코드
- `docker/` — Compose, env, Dagster workspace
- `scripts/` — 운영 스크립트

## Staging 테스트할 때

- UI **3031**, DB **`staging.duckdb`**, MinIO **9002**
- Dispatch 트리거: `…/staging/incoming/.dispatch/pending/*.json`  
  예시: `scripts/staging_dispatch_trigger.example.json`
- 초기화(스토리지·센서까지): `AGENTS.md`에 정리된 **Staging 완전 초기화** 절차가 `CLAUDE2.md`에도 있음 — 필요 시 해당 섹션 검색

## 질문할 때 넣으면 좋은 맥락

- **production vs staging**
- **run id / asset 이름 / 에러 로그 한 줄**
- **어느 job** (`mvp_stage_job`, `dispatch_stage_job`, …)

---

*내용 변경 시 원본은 `CLAUDE.md`를 고치고, `AGENTS.md`와 이 파일은 그에 맞춰 갱신하는 것을 권장합니다.*
