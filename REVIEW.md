# PR 리뷰 기준

매 PR에서 아래 저장소 계약 위반을 확인한다.

- **계층 import:** 5-layer 구조를 지킨다. `lib/`(L1–2)은 `dagster`, `vlm_pipeline.defs`, `resources`, `ops`를 import하지 않는다. lazy import도 금지하며 `scripts/check_lib_layer_imports.py`가 검사한다.
- **테스트 추적:** `.gitignore`가 `tests/`를 기본 제외하고 allowlist만 추적한다. 새 테스트가 의도한 allowlist에 포함됐는지 확인한다.
- **파일 오류 처리:** 배치 처리에서 개별 파일 오류를 기록하고 다음 파일로 진행한다. 검증 실패 파일은 DB 등록·archive 이동 대상에서 제외한다.
- **MinIO 계약:** 고정 버킷은 `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification`. 라벨 JSON 정본은 `vlm-labels`; `raw_key`는 정규화한 `<source_unit_name>/<rel_path>`이며 `YYYY/MM` prefix를 붙이지 않는다.
- **마이그레이션:** PostgreSQL migration은 forward-only다. 기존 파일을 수정·이름 변경하지 말고 신규 변경은 새 migration으로 추가한다. PR에 migration 파일이 미커밋 상태로 남지 않게 한다.
- **비밀 값:** credential, API key, password 등 비밀 값을 커밋하지 않는다.
