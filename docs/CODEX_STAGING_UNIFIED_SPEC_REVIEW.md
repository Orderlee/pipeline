# Staging 전용 Auto Labeling 통합 명세 — 검수 결과

> 기준: `docs/CODEX_STAGING_AUTO_LABELING_UNIFIED_SPEC_TASK.md` + `auto_labeling_unified_spec.md`  
> 구현: Codex(일반 목적 에이전트) 수행 → Cursor 검수

---

## 검수 체크리스트 결과

| 항목 | 상태 | 비고 |
|------|------|------|
| schema.sql에 labeling_specs, labeling_configs, requester_config_map 및 raw_files/video_metadata 컬럼 반영 | ✅ | 12~14번 테이블 추가. raw_files/video_metadata 확장 컬럼은 duckdb_base ALTER로 처리 주석 명시 |
| duckdb_base.py에서 위 테이블/컬럼 ensure 로직 | ✅ | _ensure_raw_files_spec_columns, _ensure_video_metadata_stage_columns 호출 |
| insert_raw_files_batch에 source_unit_name(, spec_id) 반영 | ✅ | 테이블 컬럼 존재 시에만 INSERT에 포함 (동적 컬럼 조회) |
| ingest ops/assets에서 manifest.source_unit_name → raw_files 저장 | ✅ | ops.py 165~166에서 record["source_unit_name"] 설정 |
| defs/spec/assets.py, defs/spec/sensor.py 신규 생성 및 내용 일치 | ✅ | labeling_spec_ingest, config_sync, ingest_router, pending_ingest / spec_resolve_sensor, ready_for_labeling_sensor |
| clip_timestamp_routed 등이 run tag(spec_id, requested_outputs, resolved_config_id) 기반 동작 | ✅ | run tag 기반 분기 구현됨. captioning_routed, clip_to_frame_routed, bbox_labeling은 스텁+TODO |
| definitions.py에서 IS_STAGING일 때만 새 assets/sensors/job 등록 | ✅ | auto_labeling_routed_job, spec_resolve_sensor, ready_for_labeling_sensor, spec·라우팅 asset 8개 |
| 기존 staging (dispatch_sensor, dispatch_stage_job) 유지 | ✅ | 기존 로직 변경 없음 |

---

## 확인된 사항

1. **스키마·리소스**
   - `labeling_specs`, `labeling_configs`, `requester_config_map` 스키마 및 duckdb_base ALTER로 raw_files/video_metadata 확장 컬럼 보장.
   - `duckdb_spec.py` 신규, DuckDBResource에 DuckDBSpecMixin 적용.
   - `duckdb_ingest.py`: `batch_update_spec_and_status`, `list_completed_videos_for_spec_router` 추가.

2. **Spec 플로우**
   - `ingest_router`: source_unit_name 기준 spec 매칭, config resolve, pending_spec / ready_for_labeling 전이.
   - `spec_resolve_sensor`: pending_resolved → config resolve → raw_files ready_for_labeling, spec active.
   - `ready_for_labeling_sensor`: spec 단위로 auto_labeling_routed_job 실행, run_key·tags 설정, 중복 실행 방지, retry_count >= 3 시 failed.

3. **env_utils**
   - `CATEGORY_TO_CLASSES`, `derive_classes_from_categories()` 추가 (spec categories→classes 파생).

4. **테스트**
   - 로컬에서 `pytest tests/unit` 실행 시 현재 셸에 dagster 미설치로 import 에러. **프로젝트 venv 또는 docker 내에서 테스트 필요.**

---

## TODO / 추후 보강 (구현 측 요약 그대로)

| 위치 | 내용 |
|------|------|
| defs/spec/assets.py — labeling_spec_ingest | Flex/Slack 등 외부 spec JSON 수신 시 `db.upsert_labeling_spec(...)` 호출하도록 확장 |
| defs/process — clip_captioning_routed | spec_id + timestamp_status=completed 백로그 조회 후 기존 clip_captioning 로직 재사용, caption_status 갱신 |
| defs/process — clip_to_frame_routed | spec_id + timestamp 완료 백로그로 clip·프레임 추출 후 frame_status 갱신 |
| defs/yolo — bbox_labeling | spec_id·frame_status=completed 백로그, target_classes = intersection(spec.classes, config.bbox.target_classes) 적용 |
| defs/spec/sensor.py — _has_run_for_spec | 동일 spec 중복 방지가 최근 20 run 기준이라, tag 필터 등 보강 검토 |
| config/parameters/_fallback.json | config_sync에서 필수. 스테이징/배포 환경에 경로·파일 존재 여부 확인 |

---

## 검수 결론

- **승인**: staging 전용 변경은 할당서·통합 명세와 일치하며, production 경로는 건드리지 않았고, 스키마·리소스는 backward compatible하게 추가됨.
- **권장**:  
  1) 프로젝트 venv 또는 docker에서 `pytest tests/unit` 및 (가능하면) staging 구동 한 번 수행.  
  2) 위 TODO 순서대로 labeling_spec_ingest 수신 경로 → clip_captioning_routed / clip_to_frame_routed / bbox_labeling 내부 로직 구현.  
  3) 스테이징 검증 후 운영 코드에 동일 구조 적용 시, definitions.py의 IS_STAGING 분기만 조정하면 됨.
