# GenAI Studio Rollout — Phase Status

이 파일은 cron 깨어날 때 매번 읽어 다음 phase 결정에 사용.
세션 간 컨텍스트 전달용.

## 현재 상태

- **current_phase**: 6 (= 운영 task / main PR)
- **completed_phases**: [0, 1, 2, 3, 3.5, 4, 5]
- **last_commit**: db6ab680 (Phase 5)
- **last_qa_passed_at**: 2026-05-10 07:30 (Phase 5)
- **next_phase_scheduled_at**: 사용자 키 입력 + main PR 시점 (수동 트리거)

## 진행 규칙 (v2 — OS-level 스케줄링으로 갱신)

이전 버전: Claude Code in-process `CronCreate` 사용 → REPL idle + 세션 alive 조건이라 발화 신뢰성 낮음. **두 번 연속 발화 실패** 후 OS-level 로 전환.

새 메커니즘:
1. **알림** : `scripts/genai_phase_check.sh` 가 `last_qa_passed_at` 부터 4h 경과 시 desktop notification + stdout. `/tmp/genai_phase_check.lock` 으로 중복 방지.
2. **사용자 crontab 1줄** :
   ```
   */30 * * * * /home/user/work_p/Datapipeline-Data-data_pipeline_test/scripts/genai_phase_check.sh
   ```
   (30분마다 검사. 알림은 phase 당 1회만.)
3. **사용자 트리거** : 알림 받으면 Claude Code 세션 열고 "Phase N 시작" 한 마디.
4. **새 세션** : 이 파일을 읽고 `current_phase` 의 작업을 시작.

phase 완료 시:
- `completed_phases` 에 번호 추가
- `current_phase` 를 다음 번호로 갱신
- `last_commit` / `last_qa_passed_at` 업데이트 (스크립트가 다음 4h 카운팅의 기준)
- `/tmp/genai_phase_check.lock` 자동 만료 (다음 phase 알림 가능)

QA 실패 → 갱신 안 함, 사용자 개입 대기.

## Phase 0 — 사전 결정 (완료)

- API 키: 빈칸으로 두고 코드 진행 (사용자 후입력)
- Higgsfield 접근 경로: fal.ai 채택
- basic auth: 단순 user/pass env-based
- 4h 텀 정책: 옵션 A (야간 무관, 언제든 4h 후)
- 휴식 의미: 부하 분산/토큰 절약

## Phase 1 — 완료 (2026-05-08)

- [x] Postgres 마이그레이션 `002_genai.sql` 작성 (Codex T1, 멱등성 통과)
- [x] `postgres_dedup.py` candidate SELECT 에 신규 3컬럼 + `find_project_genai_pairs` 신규
- [x] `postgres_ingest_raw.py` conditional 컬럼 (has_source_type/genai_engine/label_policy)
- [x] `ops_register.py` manifest pass-through + fail-loud pre-validation
- [x] **신규** `postgres_genai.py` mixin (Codex review 권고로 추가) — Phase 2/3 가 의존할 write-side 메서드 정의 (insert_genai_batch/jobs_batch, update_*, recompute_batch_status, get/list/find_pending_*)
- [x] PG smoke test: 멱등성 / GenAI pair JOIN / CHECK / FK CASCADE / mixin SQL 시뮬레이션 모두 통과
- [x] Syntax check 통과

### 알려진 follow-up (Phase 2/3 진입 시 다룸)

- INGEST sensor 자체는 미수정 — sensor_incoming.py 가 `{manifest_dir}/pending/*.json` 만 본다는 사실을 Phase 3 orchestrator 가 준수해야 함. orchestrator 가 GenAI 결과 manifest 도 같은 queue 디렉토리에 emit 하도록.
- `find_project_genai_pairs` 의 inner JOIN: input_asset_id IS NULL (text-to-*) 시나리오는 사용자 명시 (이미지 N장 + 프롬프트) 시나리오에 해당 없음 → 그대로 유지.
- CHECK constraint enum 명명: `genai_batches` 의 inline CHECK 가 unnamed. 5번째 엔진 추가 시 ALTER 가 까다로움. 후속 마이그레이션에서 명명 권고.

## Phase 2 — 완료 (2026-05-08, ~22:50 KST)

- [x] `postgres_dedup.py` candidate query 에 source_type 필터 추가 (R5: GenAI rows 가 regular path 안 섞이게)
- [x] `build/assets.py` `_build_project` 에 GenAI early-return 분기 + `_build_project_genai` 신규
- [x] Codex review 의 6 issue 즉시 반영:
  - HIGH (2) `_genai_pairs_or_empty` bare except 분리 — AttributeError 만 catch, 나머지 raise (silent dataset 'completed' 회귀 차단)
  - HIGH (5) partial failure → build_status `failed`/`partial`/`completed` 차등 + manifest 의 `skipped_pairs` 필드
  - MED (1) folder_prefix invariant 검사 (cross-batch 오염 방어)
  - MED (3) output_media mismatch 검증 (batch.output_media vs raw_files.media_type)
  - MED (6) `dataset.config` JSON 에 `batch_ids`/`engines`/`prompt_hashes`/`source_unit_name` 보강 (Phase 3 rebuild 의존)
  - LOW (4) prompt PII 회피 — manifest 기본 `prompt_hash` 만, `GENAI_MANIFEST_INCLUDE_PROMPT=1` 시에만 평문
  - manifest `schema_version: 1` 추가
- [x] PG smoke: source_type 필터 (cam_test → 1 row, genai_b1 → 0 row), GenAI pairs JOIN 1 row 정상

### Phase 3 진입 전 처리할 follow-up (Codex 잔여)

- LOW (7) **운영자 task**: prod 의 `raw_files.source_type` 분포 확인 — `NULL`/`camera`/`nas_upload` 외 값 있는지.
  ```sql
  SELECT source_type, COUNT(*) FROM raw_files
  WHERE ingest_status='completed' GROUP BY source_type;
  ```
- (8) Phase 3 시작 전 결정: rebuild API (`rebuild_genai_dataset(batch_id|dataset_id)`) — 현재 `_build_project_genai` 는 호출마다 새 dataset_id 생성 → datasets 테이블 누적. idempotent rebuild 필요 시 별도 entry point.
- Phase 1 의 follow-up (CHECK constraint 명명, sensor manifest queue 위치) 은 그대로 유효.

## Phase 3 — 완료 (2026-05-09, ~23:30 KST)

- [x] `docker/genai/` FastAPI 컨테이너 골격
  - `app.py` — POST /genai/batches, GET 목록/상세, basic auth + 업로드 제한
  - `db/pg.py` — psycopg2 thin layer (PostgresGenAIMixin SQL 1:1 미러)
  - `adapters/{base,kling,__init__}.py` — Protocol + Kling 어댑터 (JWT, mocking 모드)
  - `jobs/{submit,finalize}.py` — N장 fan-out + per-job NAS atomic write
  - `storage/{nas_writer,manifest}.py` — atomic write + manifest 빌더
  - `templates/{base,index,batches,batch_detail}.html` + `static/style.css`
  - `Dockerfile` + `requirements.txt` (fastapi, uvicorn, jinja2, psycopg2-binary, PyJWT, requests)
- [x] `defs/genai/{__init__,sensor}.py` — Dagster `genai_poll_sensor` (Postgres polling, default STOPPED)
  - `definitions_production.py` 의 sensors 리스트에 등록
- [x] `defs/ingest/ops_register.py` — manifest items[seq] → record._genai_batch_id/_genai_seq_in_batch 통과
- [x] `defs/ingest/ops_normalize.py` — `_link_genai_asset_if_any` 헬퍼 + INSERT 후 `update_genai_job_assets` 호출
- [x] `docker/docker-compose.yaml` — `genai` 서비스 추가 (`profiles: ["genai"]`, 8088 포트, NAS volume mount)
- [x] `docker/.env.test` — `KLING_*`, `FAL_KEY`, `OPENAI_API_KEY`, `GENAI_*` 빈칸 placeholder
- [x] OS-level scheduling — `scripts/genai_phase_check.sh` (동작 확인됨: notify-send + stdout)

### Phase 3 의 mocking 모드 동작

API 키 미설정 시 `KlingAdapter.is_mock=True` 자동 감지:
- `submit()` → `kling-mock-<uuid>` provider_job_id 반환
- `poll()` → 2초 후 'done' + mock URL
- `download_result()` → 미니 mp4 placeholder bytes

→ 사용자가 키를 채우기 전에도 e2e (UI submit → NAS atomic write → ingest sensor → build asset → vlm-dataset paired manifest) 검증 가능.

### Phase 4 진입 전 처리할 follow-up

- Codex review 의견 (수신 시 즉시 반영)
- e2e dry-run: staging Postgres + NAS + MinIO 위에서 N=2 batch (mock) 실행
- 사용자가 `KLING_ACCESS_KEY/SECRET_KEY` 채운 뒤 verify 명령으로 실제 호출 확인

## Phase 3.5 — 완료 (2026-05-10, ~06:50 KST)

Codex Q3 HIGH follow-up: sensor 의 어댑터 import 의존을 끊기 위해 HTTP API 경유로 전환.

- `docker/genai/app.py` 신규 internal API:
  - `GET /internal/jobs/pending?limit=N` — 비동기 엔진 (is_synchronous=False) pending 목록
  - `POST /internal/jobs/{job_id}/poll` — 어댑터 poll() 호출 + 'done' 시 BackgroundTasks 로 finalize 분리 (Codex HIGH #2 — sensor timeout 60s vs download timeout 300s race 회피)
  - 인증: `GENAI_INTERNAL_TOKEN` 헤더 (basic auth 와 분리)
- `defs/genai/sensor.py` 재작성: requests 로 internal API 호출만, 어댑터 import 0
- compose `x-runtime-dagster-env` 에 `GENAI_INTERNAL_BASE/TOKEN/POLL_INTERVAL/BATCH` 추가

## Phase 4 — 완료 (2026-05-10, ~06:50 KST)

3 신규 어댑터 + UI 2탭. 모두 mocking 모드 지원 (키 미설정 시 placeholder 결과).

- `adapters/nanobanana.py` — Vertex AI gemini-2.5-flash-image (동기). google-genai SDK. Vertex SA 미존재 시 mocking. safety block / text-only 응답 fallback 명시.
- `adapters/gpt_image.py` — OpenAI gpt-image-1 (동기). openai SDK `images.edit`. OPENAI_API_KEY 미설정 시 mocking.
- `adapters/higgsfield.py` — fal.ai 경유 (비동기). fal-client SDK. FAL_KEY 미설정 시 mocking. ⚠ 운영자: 실 모델 ID `fal-ai/higgsfield/i2v-soul` 검증 필요 (Codex HIGH #1).
- `adapters/__init__.py` — 4 엔진 등록 + `ENGINE_TAB` 매핑 + `engines_by_tab()` 헬퍼.
- `jobs/submit.py` — sync engine path 활성화 (NotImplementedError → finalize_sync_results 일괄 호출).
- `jobs/finalize.py` — 신규 `finalize_sync_results` (in-memory bytes → NAS atomic write + manifest).
- `templates/index.html` — 2탭 UI (Image→Video / Image→Image), JS 탭 전환.
- `static/style.css` — `.tab-bar/.tab-btn/.tab-panel` 스타일.
- `requirements.txt` — google-genai, openai, fal-client 추가.
- `docker-compose.yaml` — `./app/credentials:/app/credentials:ro` mount, GEMINI 관련 env, GENAI_INTERNAL_TOKEN 추가.
- `.env.test{,.example}` — `GENAI_INTERNAL_TOKEN`, 4-엔진 default `GENAI_ENGINES_ENABLED`, GEMINI_PROJECT/LOCATION, HIGGSFIELD_FAL_MODEL placeholder.

### Codex review 반영

- HIGH #1 (Higgsfield 모델 ID 검증) — env-driven 으로 주입 가능, 코드 주석에 "fal.ai 대시보드 확인" 명시. 실제 검증은 운영자 task.
- HIGH #2 (finalize blocking) — BackgroundTasks 분리로 sensor timeout 무관하게 안전.
- MED #4 (pending SQL 동적화) — `_async_engines()` 헬퍼로 어댑터 `is_synchronous=False` 필터.
- MED #5 (nanobanana fallback) — block_reason / finish_reason / text_excerpt 명시 노출.

### Phase 4 의 mocking 모드 동작 (모든 엔진)

- Kling/Higgsfield: submit → mock provider_id 반환, poll 2초 후 'done' + placeholder MP4
- Nanobanana/GPT Image: submit 시점에 placeholder PNG 반환 (동기), poll 호출 안 됨

→ 사용자가 키를 채우기 전에도 e2e 4 엔진 모두 검증 가능.

## Phase 5 — 완료 (2026-05-10, ~07:30 KST)

운영 강화: quota / rate-limit / 비용 모니터링 / 부분 실패 재시도 / 운영 가이드.

- `docker/genai/limits.py` 신규: `check_rate_limit` (sliding 60s in-memory) + `check_daily_quota` (PG 집계, options_json::jsonb 추출). 0 = off.
- `docker/genai/db/aggregates.py` 신규: `cost_summary(range)` — 엔진별 done/failed/cost_units 집계.
- `app.py`:
  - POST `/genai/batches`: rate-limit + daily quota 검사 (429 응답)
  - GET `/genai/costs?range=day|week|month` + `templates/costs.html` (range tabs, totals, by_engine, limits status)
  - POST `/genai/jobs/{job_id}/retry`: 실패 job 1건 재시도. **atomic CAS** (`UPDATE ... WHERE status='failed' RETURNING`) 로 race 차단 (Codex HIGH #3). 외부 API 실패 시 status 되돌림.
- `jobs/submit.py`: options_json 에 `input_total_bytes` 자동 주입 (quota 집계용).
- `jobs/finalize.py` `_maybe_write_outputs_manifest`: **PG advisory lock** (`pg_try_advisory_xact_lock(hashtext('genai_manifest:'+batch_id))`) 으로 manifest 다중 writer race 차단 (Codex HIGH #8).
- `templates/`: base.html nav 에 Costs, batch_detail.html 의 failed job 에 retry 버튼 (HTMX).
- `static/style.css`: range-tabs, dl grid 스타일.
- `docker-compose.yaml` + `.env.test{,.example}`: `GENAI_RATE_LIMIT_PER_MIN/DAILY_BATCH_LIMIT/DAILY_BYTES_LIMIT` placeholder (default 0).
- **신규** `docs/genai_rollout/operator_guide.md` — main 승격 체크리스트, e2e 검증, 트러블슈팅, 금기사항 (CI rsync 와 호스트 수동 편집 충돌).

### Codex review 반영 (HIGH 2 + MED 4)

| 등급 | issue | fix |
|------|-------|-----|
| HIGH #3 | retry race — 외부 API 중복 호출 | `UPDATE ... WHERE status='failed' RETURNING` atomic CAS, 빈 RETURNING 시 409 |
| HIGH #8 | manifest 다중 writer race | `pg_try_advisory_xact_lock` 로 single writer 직렬화 |
| MED #2 | options_json regex 파싱 취약 | `options_json::jsonb ->> 'input_total_bytes'` 안전 캐스팅, invalid JSON 보호 |
| MED #1 | rate-limit 멀티 컨테이너 분산 | operator_guide 에 single-container 정책 명시 + 워커 증설 시 redis/PG 이전 권고 |
| MED #7 | operator_guide 의 prod 편집 위험 | guide 끝에 CLAUDE.md 금기사항 (호스트 수동 편집 → CI 소실) 명시 추가 |
| MED #4 | retry sync 엔진 + ingest 중복 | 현재 raw_files.checksum UNIQUE 로 dedup. 추후 `batch_id+seq+kind` 복합 UNIQUE 검토 (Phase 6 follow-up) |
| LOW #5/#6 | anonymous quota / cost OR 인덱스 | 현 규모 수용. 데이터 증가 시 source IP 분리 + `submitted_at > since` 단순 필터 분리 |

### Phase 6 (운영) 으로 이관

- 운영자 키 입력 + e2e 4 엔진 실 호출 검증
- Higgsfield 모델 ID (`fal-ai/higgsfield/i2v-soul`) fal.ai 대시보드 검증
- staging 24h 안정화 → main PR
