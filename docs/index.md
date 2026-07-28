# Docs Index

이 디렉터리는 이 저장소의 **기록 시스템**입니다.  
설계 판단, 실행 계획, 운영 참고 문서는 채팅이나 로컬 메모에만 남기지 말고 여기로 모읍니다.

## 문서 탐색 순서

1. `README.md` — 사람용 개요
2. `AGENTS.md` — 에이전트용 맵
3. 아래 하위 인덱스

## 하위 인덱스

- [Design Docs](design-docs/index.md)
- [Exec Plans](exec-plans/index.md)
- [References](references/index.md)

## 현재 주요 문서

- 설계/명세
  - [Auto_Labeling_기능_명세서](logic/Auto_Labeling_기능_명세서.md)
  - [명세 대비 현행 갭 및 수정사항](logic/Auto_Labeling_명세_대비_현행_갭_및_수정사항.md)
  - [Dispatch_Labeling_Method_체계_및_Skip_Import_설계안](logic/Dispatch_Labeling_Method_체계_및_Skip_Import_설계안.md)
  - [Dagster pgvector 임베딩 설계](design-docs/2026-06-15-dagster-pgvector-embeddings-design.md)
  - [MLOps 파인튜닝 스캐폴딩 설계](superpowers/specs/2026-06-29-mlops-finetune-scaffolding-design.md)
  - [SourceA 일일 수집 설계](superpowers/specs/2026-07-06-sourcea-daily-download-design.md)
- 실행 계획 / 로드맵
  - [Streamlit/FiftyOne 분석 스택 — 추가 개발 계획](analysis-stack-fiftyone-streamlit-roadmap-2026-07-27.md)
  - [준지도 학습 도입 타당성](semi-supervised-feasibility-2026-07-27.md)
  - [프로덕션 PG 롤아웃 계획](exec-plans/active/production-pg-rollout-plan.md)
  - [QA 시나리오 플레이북](exec-plans/active/qa-scenarios-playbook.md)
  - [PLAN](PLAN.md)
- 감사 / 상태 리포트
  - [파이프라인 흐름 감사 — 코어](pipeline-flow-audit-2026-07-01-core.md)
  - [파이프라인 흐름 감사 — MLOps/DVC](pipeline-flow-audit-2026-07-01.md)
  - [파이프라인 상태 리포트](pipeline-status-report-2026-07-01.md)
  - [Pseudo-label QA 타당성](pseudo-label-qa-feasibility.md)
- 운영 참고
  - [배포 가이드](references/deployment-guide.md)
  - [Git 워크플로 가이드](git-workflow-guide.md)
  - [운영 트러블슈팅 런북](runbook.md) — 및 주제별 런북 모음 [`runbook/`](runbook/)
  - [Label Studio 운영 가이드](references/label-studio-ops-guide.md)
  - [Label Studio 운영 런북](runbook/labelstudio-ops.md)
  - [FiftyOne 운영](runbook/fiftyone-operations.md), [HNSW 튜닝](runbook/hnsw-tuning.md)
  - [PG 복구 드릴](runbook/pg-restore-drill.md), [임베딩 백업/복구](runbook/embedding-backup-restore.md)
  - [NAS 10.0.0.36 → 10.0.0.51 마이그레이션](references/minio-host-endpoint-migration.md)
  - [Production source-a/source-b 라벨·전처리 정리 Runbook](references/production-label-preprocess-cleanup-runbook.md)
  - [에이전트 라우팅](references/multi-agent.md), [에이전트 팀](references/agent-teams.md)
- GenAI Studio
  - [사용설명서·운영 가이드 모음](genai_rollout/)

## 역사 문서

브랜치 기반 `dev=staging` / `main=production` 전환 전, 그리고 **DuckDB → PostgreSQL cutover
(2026-05-19) 전**에 작성된 문서는 역사 기록으로만 유지합니다. 현재 스택 설명으로 읽지 마세요.

- [MVP 아키텍처 (2026-02-20, DuckDB 시절)](design-docs/mvp-architecture-2026-02-20.md)
- [DuckDB 락 경합 분석](design-docs/duckdb-lock-contention-analysis.md) ·
  [DuckDB 락 수정 계획](exec-plans/duckdb-lock-fix-plan.md)
- [DB 마이그레이션 토폴로지 (cutover 전 결정 문서)](references/db_migration_topology.md)
- [운영-테스트 환경 분리 및 자동 배포 계획](exec-plans/운영_테스트_환경_분리_자동배포_계획.md)
  — 문서 내 상태값("runner 설치 대기")은 stale, CI/CD 는 이미 가동 중

## 새 문서 작성 규칙

- 설계/명세: `docs/design-docs/` 또는 `docs/logic/`
- 실행 계획: `docs/exec-plans/active/` 또는 `docs/exec-plans/completed/`
- 운영 참고/리포트: `docs/references/`
- 생성 스냅샷/자동 생성 결과: `docs/generated/`

당장은 기존 문서를 대규모 이동하지 않고, 새 문서부터 이 구조를 따릅니다.
