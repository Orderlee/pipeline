# Staging QA — MCP·역할 분배 (`STAGING_QA_TEST_GUIDE.md` 연동)

| 역할 | 담당 | 하는 일 |
|------|------|---------|
| **환경 초기화·스크립트 실행** | **Cursor** | `scripts/cleanup_staging_test.sh`, `docker compose` 기동 확인, `incoming` 복사 → `archive_pending` 이동 대기 → 트리거 배치, DuckDB/SQL·경로 증빙 수집 |
| **표준 트리거·라운드 실행** | **Cursor + repo 스크립트** | `scripts/staging_test_dispatch.py` (`--outputs` 등). 단, **복사 직후 트리거 금지** — `incoming_to_pending_sensor`(약 15s) 이후 배치 |
| **다양한/수동 trigger JSON 초안** | **Gemini MCP** (`ask-gemini`) | 가이드 **방법 B**·실패 케이스 JSON 설계. **`invalid_outputs` 테스트는 반드시 `"outputs": ["bad_output"]` 단일 항목** (`STAGING_QA_TEST_GUIDE.md` 「잘못된 output」 절). `["bad_output","not_real"]` 등 이중 나열·유효+무효 혼합 금지 |
| **코드 버그·파이프라인 개입** | **Codex MCP** (`user-codex` / `ask-openai`) | 스크립트 실패, Dagster op 실패, 스키마 불일치 등 **수정 지시·패치 검토** |
| **QA 결과 보고서** | **Cursor** | `STAGING_QA_TEST_GUIDE.md` 템플릿 기준 `STAGING_QA_RESULT_REPORT*.md` 작성·갱신 |

## 권장 순서 (가이드 § 추천 테스트 순서)

1. `timestamp` → 2. `timestamp`+`captioning` → 3. `bbox` (YOLO 기동) → 4. `bbox`+`captioning` → 5. 두 폴더 복사·트리거 1개 → 6. 실패 케이스  

각 라운드 전 **`cleanup_staging_test.sh`** 권장 (또는 폴더명 충돌 시 전체 초기화).

## 트리거 배치 타이밍 (필수)

`folder_name`은 **`archive_pending/<folder_name>`** 에 있어야 `dispatch_sensor`가 수락함.  
→ **incoming에 복사 → 최소 30~45초 대기 → `.dispatch/pending/*.json` 배치.**
