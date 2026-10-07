# Copilot 지침

- 작업 전 `.agent/skill/`에서 관련 지침을 찾는다.
- 변경 범위를 좁히고 기존 데이터 계약과 레이어 경계를 따른다.
- DB 변경은 `PostgresResource` 경로와 forward-only migration 규칙을 따른다.
- 비밀 값과 로컬 환경 파일을 출력하거나 커밋하지 않는다.
