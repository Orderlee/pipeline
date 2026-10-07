# 상태: 폐기 — 기존 명령은 `definitions.py`, `dagster-staging`, 직접 Compose 호출을 가정하지만 현재 운영 정의는 `definitions_production.py`와 wrapper를 사용한다.

# Dagster lineage 점검

새 asset·sensor·job 변경 때만 사용한다.

1. `definitions_production.py`에서 asset/sensor가 등록되는지와 해당 `defs/` export를 확인한다.
2. asset dependency, `define_asset_job` selection, resource key를 코드에서 대조한다. `lib/`에는 Dagster/defs/resources/ops import를 넣지 않는다.
3. host venv에서 definitions load 또는 관련 unit test를 실행한다. staging 컨테이너 검증이 필요하면 별도 `_test` clone에서 `scripts/compose-staging.sh`만 쓴다.
4. 실패 시 upstream 이름·export·selection 중 하나만 고치고 다시 load한다. 배포/재시작은 이 문서의 범위가 아니다.
