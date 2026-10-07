---
description: 공개 API와 동작을 보존하는 비자명한 파일 분할·이름 변경·구조 재편의 Codex 검토 절차
---

# Codex refactor

동작 보존이 핵심인 대규모 분할·이동·이름 변경에 쓴다. 기능 변경·버그 수정은 `codex_collab`을 쓴다. 단일 rename, import 정리, 주석 변경은 직접 처리한다.

1. 대상의 공개 심볼·호출부·보존할 동작을 확인하고, 목표 구조와 허용 변경 범위를 정한다.
2. Codex에 파일 경로, 목표, 공개 API 보존, 함수 본문 변경 금지 조건을 주어 read-only 분할안을 받는다.
3. 모듈 경계·순환 import·적용 순서를 사용자에게 보이고 승인을 받는다.
4. 한 단위씩 이동하고 기존 경로는 facade/re-export로 유지한다. 개선·버그 수정은 별도 변경으로 분리한다.
5. `ruff check`, 기존 경로 import smoke, 관련 unit test를 실행한다. Dagster 대상이면 definitions load도 확인한다.

## 제약

- 하위 에이전트는 파일을 수정·커밋하지 않는다.
- import 방향(`definitions* → defs → resources/lib`)을 깨지 않는다.
- `lib/`에 Dagster·defs·resources·ops import를 새로 넣지 않는다.
