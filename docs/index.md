# Docs Index

This directory is the **record system** for this repository.
Design decisions, execution plans, and operational reference documents should be consolidated here rather than left in chat logs or local notes.

## Navigation Order

1. `README.md` — human-facing overview
2. `AGENTS.md` — agent map
3. Sub-indexes below

## Sub-Indexes

- [Design Docs](design-docs/index.md)
- [Exec Plans](exec-plans/index.md)
- [References](references/index.md)

## Current Key Documents

- Design / Specification
  - [Auto_Labeling_기능_명세서](logic/Auto_Labeling_기능_명세서.md)
  - [Dispatch_Labeling_Method_체계_및_Skip_Import_설계안](logic/Dispatch_Labeling_Method_체계_및_Skip_Import_설계안.md)
  - [YOLO_World_자연어_프롬프트_연결_설계안](logic/YOLO_World_자연어_프롬프트_연결_설계안.md)
- Execution Plans
  - [Production-Test Environment Separation and Auto-Deploy Plan](exec-plans/운영_테스트_환경_분리_자동배포_계획.md)
  - [PLAN](PLAN.md)
- Operational Reference
  - [Deployment Guide](references/deployment-guide.md)
  - [Operational Troubleshooting Runbook](runbook.md)
  - [Label Studio Operations Guide](references/label-studio-ops-guide.md)
  - [RUN_2f50c9bb_MOTHERDUCK_QUERY](RUN_2f50c9bb_MOTHERDUCK_QUERY.md)
  - [QA_PHASE3_REPORT](QA_PHASE3_REPORT.md)
  - [QA_PHASE4_REPORT](QA_PHASE4_REPORT.md)
  - [Production source-a/source-b Label & Pre-processing Cleanup Runbook](references/production-label-preprocess-cleanup-runbook.md)

## Historical Documents

- Documents using the `staging` naming convention, predating the `dev=test`, `main=production` branch-based runtime transition, are preserved as historical records.
- Representative examples:
  - [staging_agent_api_dispatch_plan](staging_agent_api_dispatch_plan.md)
  - [PRODUCTION_VS_STAGING](PRODUCTION_VS_STAGING.md)

## New Document Rules

- Design / Specification: `docs/design-docs/` or `docs/logic/`
- Execution plans: `docs/exec-plans/active/` or `docs/exec-plans/completed/`
- Operational reference / reports: `docs/references/`
- Generated snapshots / auto-generated output: `docs/generated/`

For now, avoid large-scale relocation of existing documents; apply this structure to new documents going forward.
