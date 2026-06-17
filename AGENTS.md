# AGENTS.md — VLM Data Pipeline

이 문서는 에이전트를 위한 **짧은 맵**입니다.  
세부 설계, 계획, 운영 레퍼런스는 `docs/` 아래 기록 시스템을 우선 참조합니다.

## 먼저 볼 문서

1. `README.md` — 사람용 개요와 운영 흐름
2. `docs/index.md` — 문서 전체 목차
3. 작업 성격에 맞는 하위 인덱스
   - `docs/design-docs/index.md`
   - `docs/exec-plans/index.md`
   - `docs/references/index.md`
4. 에이전트 라우팅·effort·escalation 룰: `docs/references/multi-agent.md`

## 프로젝트 한 줄

CCTV/보안 영상 수집 → 중복 제거 → Gemini(Vertex) 라벨링 → SAM3 segmentation 검출 → 학습 데이터셋 빌드  
스택은 Dagster + PostgreSQL + MinIO 입니다 (DuckDB/MotherDuck은 PG cutover 이후 제거).

## 핵심 경로

- `src/vlm_pipeline/` — 파이프라인 패키지
- `docker/` — Compose, workspace, env
- `scripts/` — 운영/검증 스크립트
- `docs/` — 설계, 실행 계획, 운영 참고 문서

## 운영 환경 요약

| 항목 | Production (`main`) | Test (`dev`) |
|------|----------------------|--------------|
| NAS root (host) | `/home/user/mou/nas_200tb` → `/nas/data` | `/home/user/mou/nas_200tb/staging` → `/nas/data` |
| PostgreSQL | `docker-postgres-1:15433` / `vlm_pipeline` | `pipeline-test-postgres-1:15432` / `vlm_pipeline_staging` |
| Dagster UI | `3030` | `3031` |
| Compose 실행 | `./scripts/compose-prod.sh` | `./scripts/compose-staging.sh` |
| env | `docker/.env` | `docker/.env.test` |

prod/test는 같은 compose 서비스 정의를 쓰고, branch·env 파일·host path·endpoint·feature flag만 다릅니다.

## 필수 규칙

- 메타데이터/라벨 source of truth는 PostgreSQL입니다 (DuckDB write path 제거).
- 직접 `docker compose` 호출 금지 — 반드시 `compose-prod.sh` / `compose-staging.sh` wrapper 사용.
- MinIO 버킷은 `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification` 5개 고정입니다.
- 라벨 JSON source of truth는 `vlm-labels`입니다.
- GCP auto-bootstrap manifest는 `pending -> processed -> completed(summary)`로 compact하며, `_DONE` 이후에는 chunk별 processed manifest 대신 source unit/signature summary 1개만 남깁니다.
- 주요 설계 판단과 운영 규칙은 채팅만으로 끝내지 말고 `docs/`에 남깁니다.
- 새 작업은 `AGENTS.md -> docs/index.md -> 관련 하위 index` 순서로 탐색합니다.

## 문서 운영 원칙

- `README.md`: 제품/운영 개요
- `AGENTS.md`: 에이전트용 진입점
- `docs/`: 설계, 계획, 참고 문서의 기록 시스템
- 로컬 전용 메모나 IDE 설정은 보조 수단일 뿐, 핵심 source of truth가 아닙니다.

## 로컬 보조 자료

- `CLAUDE.md`는 로컬 상세 운영 요약으로 사용할 수 있습니다.
- git 추적하지 않는 장문 참고 자료가 있을 수 있지만, 핵심 판단은 가능하면 `docs/`로 이관합니다.
