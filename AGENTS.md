# AGENTS.md — VLM Data Pipeline

이 문서는 에이전트를 위한 **짧은 맵**입니다.
세부 설계, 계획, 운영 레퍼런스는 `docs/` 아래 기록 시스템을 우선 참조합니다.

## 먼저 볼 문서

1. `README.md` — 사람용 개요와 운영 흐름
2. `CLAUDE.md` — 운영 컨텍스트·환경·금기사항 (가장 조밀함)
3. `docs/index.md` — 문서 전체 목차
4. 작업 성격에 맞는 하위 인덱스
   - `docs/design-docs/index.md`
   - `docs/exec-plans/index.md`
   - `docs/references/index.md`
5. 에이전트 라우팅·effort·escalation 룰: `docs/references/multi-agent.md` +
   페르소나 로스터/라우팅표: `docs/references/agent-teams.md`
6. 작업별 운영 절차: `.agent/skill/<name>/SKILL.md` (**코드를 짜기 전에 먼저 검색**)

## 프로젝트 한 줄

CCTV/보안 영상 수집 → 중복 제거 → Gemini(Vertex) 이벤트 라벨링 → SAM3 검출 →
Label Studio 사람 검수 → 학습 데이터셋 빌드.
스택은 **Dagster + PostgreSQL + MinIO** 입니다.

> ⚠️ DuckDB 와 MotherDuck 은 write path 에서 제거됐습니다 (2026-05-19 PG cutover).
> DuckDB 는 `pg_duckdb` extension 경유 분석 쿼리로만 남아 있고, MotherDuck 동기화 코드는
> `scripts/archive/` 로 이동됐습니다. YOLO-World 는 `ENABLE_YOLO_DETECTION=false` 로 비활성이며
> bbox 는 SAM3 가 담당합니다.

## 핵심 경로

- `src/vlm_pipeline/` — 파이프라인 패키지 (Dagster assets/sensors/resources)
- `src/gemini/` — Label Studio 연동 (`ls_*.py`: task 생성, webhook, finalize, sync)
- `src/python/` — NAS 폴더 트리 → Postgres KPI 수집 도구
- `docker/` — Compose, 서비스별 Dockerfile, workspace, env
- `scripts/` — 운영/검증 스크립트 (`scripts/archive/` 는 사용 종료분)
- `docs/` — 설계, 실행 계획, 운영 참고 문서
- `.agent/skill/` — 작업 절차 스킬 문서

## 운영 환경 요약

| 항목 | Production (`main`) | Staging (`dev`) |
|------|----------------------|--------------|
| Dagster UI | `http://10.0.0.10:3030` | `http://10.0.0.10:3031` |
| Postgres | `vlm_pipeline` @ `docker-postgres-1` (host `:15433`) | `vlm_pipeline_staging` @ `pipeline-test-postgres-1` |
| MinIO endpoint | `http://10.0.0.51:9000` | `http://10.0.0.51:9002` |
| Incoming (호스트) | `/home/user/mou/nas_primary/incoming` | `/home/user/mou/nas_primary/staging/incoming` |
| Incoming (컨테이너) | `/nas/data/incoming` | `/nas/data/incoming` |
| Compose project | `docker` | `pipeline-test` |
| env | `docker/.env` | `docker/.env.test` |
| compose wrapper | `./scripts/compose-prod.sh` | `./scripts/compose-staging.sh` |

prod/staging 은 같은 compose 서비스 정의를 쓰고, branch 와 env 파일만 다릅니다.
`docker compose` 를 직접 호출하지 말고 **wrapper 스크립트를 사용**하세요 (env-file/project 이름 누락 사고 방지).

> 스테이징 스택은 상시 기동이 아닙니다. 필요할 때 `./scripts/compose-staging.sh up -d` 로 올리고,
> `:3031` 이 응답하지 않는다고 장애로 판단하지 마세요.

## 필수 규칙

- **write 는 `PostgresResource`(`db`) 로만.** sensor 는 `lib/sensor_db.py` read-only 연결만 사용
- MinIO 버킷은 `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification` **5개 고정**
- 라벨 JSON source of truth 는 `vlm-labels` — `vlm-processed` 에 중복 저장 금지
- `raw_key = <source_unit_name>/<rel_path>` — `YYYY/MM` prefix 금지
- `lib/`(L1-2)에서 `dagster`/`defs`/`resources`/`ops` import 금지 (CI + pre-commit 이 차단)
- 파일 단위 오류는 per-file fail-forward — 한 파일 실패가 나머지를 중단시키면 안 됨
- 호스트에서 `src/`·`configs/`·`scripts/`·compose 파일 **수동 수정 금지** —
  이 저장소가 곧 프로덕션 배포 루트라, 다음 배포의 `rsync --delete` + `git reset --hard` 로 소실됨
- GCP auto-bootstrap manifest 는 `pending -> processed -> completed(summary)`로 compact 하며,
  `_DONE` 이후에는 chunk별 processed manifest 대신 source unit/signature summary 1개만 남김
- 주요 설계 판단과 운영 규칙은 채팅만으로 끝내지 말고 `docs/`에 남깁니다
- 새 작업은 `AGENTS.md -> .agent/skill/ -> docs/index.md -> 관련 하위 index` 순서로 탐색합니다

## 문서 운영 원칙

git 추적되는 문서 (팀 공유):

- `README.md`: 제품/운영 개요
- `AGENTS.md`: 에이전트용 진입점
- `CLAUDE.md`: 운영 맥락·금기사항
- `REVIEW.md`: PR 리뷰 기준
- `docs/`: 설계, 계획, 참고 문서의 기록 시스템

`.gitignore` 로 제외된 **로컬 전용** 메모 (fresh clone 에 없음 — 인용/링크 금지):
`WORKLOG.md`, `CLAUDE2.md`, `LABEL_STORAGE_POLICY.md`, `ANTIGRAVITY.md`.
공유가 필요한 내용이면 `docs/` 로 옮기세요.
