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
  - [Dispatch_Labeling_Method_체계_및_Skip_Import_설계안](logic/Dispatch_Labeling_Method_체계_및_Skip_Import_설계안.md)
  - [YOLO_World_자연어_프롬프트_연결_설계안](logic/YOLO_World_자연어_프롬프트_연결_설계안.md)
- 실행 계획
  - [PLAN](PLAN.md)
  - [staging_agent_api_dispatch_plan](staging_agent_api_dispatch_plan.md)
- 운영 참고
  - [Label Studio 운영 가이드](references/label-studio-ops-guide.md)
  - [PRODUCTION_VS_STAGING](PRODUCTION_VS_STAGING.md)
  - [RUN_2f50c9bb_MOTHERDUCK_QUERY](RUN_2f50c9bb_MOTHERDUCK_QUERY.md)
  - [QA_PHASE3_REPORT](QA_PHASE3_REPORT.md)
  - [QA_PHASE4_REPORT](QA_PHASE4_REPORT.md)
  - [Production khon/adlib 라벨·전처리 정리 Runbook](references/production-khon-adlib-label-preprocess-cleanup-runbook.md)

## 새 문서 작성 규칙

- 설계/명세: `docs/design-docs/` 또는 `docs/logic/`
- 실행 계획: `docs/exec-plans/active/` 또는 `docs/exec-plans/completed/`
- 운영 참고/리포트: `docs/references/`
- 생성 스냅샷/자동 생성 결과: `docs/generated/`

당장은 기존 문서를 대규모 이동하지 않고, 새 문서부터 이 구조를 따릅니다.
