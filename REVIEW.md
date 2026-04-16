# REVIEW.md — VLM Data Pipeline Review Standards

> Claude PR 자동 리뷰 및 수동 리뷰 기준 문서

---

## Critical (반드시 수정 요청)

- DuckDB writer 태그 누락: DB 쓰기 asset에 `tags={"duckdb_writer": "true"}` 없음
- Import layer 위반: L5→L3 방향 등 하위→상위 import
- 하드코딩된 credential/API 키/비밀번호
- MinIO 버킷 이름이 `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset` 4개 고정 규칙 위반
- 라벨 JSON을 `vlm-processed`에 중복 저장 (source of truth는 `vlm-labels`만)
- production/test 환경 분리 위반 (경로, 포트, DuckDB 파일 혼재)

## Warning (강한 권고)

- 새 asset/op에 대한 unit test 미작성
- `@op+@job` 사용 시 `@asset`으로 대체 가능한지 미검토
- per-file fail-forward 패턴 미준수 (한 파일 실패가 전체를 중단)
- sensor에서 OSError/PermissionError를 catch하지 않음 (NAS 장애 시 크래시)
- ruff line-length 120 초과
- `raw_key`에 `YYYY/MM` prefix 포함 (금지 규칙)
- archive 이동 로직에서 suffix 충돌 처리 누락

## Info (참고 의견)

- 함수/모듈이 너무 커서 분할 권장 (기존 패턴: `assets.py` + `helpers.py` 분리)
- 독스트링 누락 (새로 작성한 public 함수)
- 매직 넘버 대신 상수/config 사용 권장
- `lib/key_builders.py`에 MinIO 키 빌더 통합 패턴 준수 여부

## Review Scope Exclusions

- `docs/**`, `*.md` 변경만 있는 PR은 리뷰 대상 아님
- `.cursor/**`, `.agent/**` 변경은 무시
- `tests/**`만 변경된 PR은 테스트 품질만 확인

## Deployment Impact Check

dev → main PR (릴리즈 PR)의 경우 추가 확인:
- `docker/Dockerfile` 변경 시 이미지 빌드 영향
- `docker-compose.yaml` 변경 시 볼륨/네트워크 영향
- env 변수 추가/변경 시 `.env.example` 동기화 여부
- `scripts/deploy/` 변경 시 롤백 호환성
