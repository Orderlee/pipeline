# 코어 파이프라인 흐름 정합성 감사 — Ingest·Dispatch·Label·SAM3·Build

> 2026-07-01. 초점: **프로덕션 코어 흐름의 구조·상태머신 정합성 + 갭 탐지**(로직, 실행가능성 아님). 브랜치 `main`(로컬).
> 방법: 6 에이전트 read-only end-to-end 추적(stage별 1개) + 각 발견을 **독립 Opus 에이전트가 적대적 검증**(REFUTE 우선). 23 raw → **18 확인 / 3 유력 / 2 반증**.
> 자매 문서 [`pipeline-flow-audit-2026-07-01.md`](pipeline-flow-audit-2026-07-01.md)(MLOps/DVC/pseudo 트랙)와 상보 — 이 문서는 그 감사가 다루지 않은 **성숙한 코어 흐름**(수집→디스패치→Gemini라벨→SAM3검출→LS검수→데이터셋빌드)을 다룬다.

---

## 결론 (한 줄)

**골격은 건전하다.** dedup 결정성, MinIO 키 도출, 챔피언-챌린저 게이트, 스냅샷 불변성, 프리페치 격리 — 검증 대상 메커니즘 대부분이 **정상**으로 확인됐다. 문제는 **6개의 반복 패턴**에 몰려 있고, 그중 **BUILD-1 하나만 진짜 "즉시 고쳐야 하는 조용한 오류"**(모델파생 라벨이 finalized 학습셋에 유입 → no-self-training 불변식 위반)다. 나머지는 크래시-윈도우/재처리불가/미배선 성격.

---

## 갭의 6개 근본 패턴 (개별 발견 → 패턴으로 묶음)

| 패턴 | 근본 원인 | 관련 발견 | 한 방 수정 |
|------|-----------|-----------|-----------|
| **A. 이중 상태머신 미교차검증** | routed/dispatch 흐름이 `timestamp_status`만 쓰고 `auto_label_status`는 NULL로 방치 → MVP `auto_labeling_sensor`가 "미처리"로 오인 | LABEL-1, LABEL-2, LABEL-3 | routed 완료/실패 시 `auto_label_status`도 세팅 **또는** MVP 후보쿼리+센서 CTE에 `timestamp_status/label_key` 가드 추가 → **3개 동시 해결** |
| **B. 폴더 스코프 vs 자산 스코프 게이팅 비대칭** | video finalized 게이트가 `source_unit_name`(폴더) join, image 게이트는 per-asset | **BUILD-1** 🔴 | video 게이트를 `l.asset_id=r.asset_id` per-asset 로 (image 쿼리와 대칭) |
| **C. 커밋/이동이 durable 부작용에 선행 + 리컨사일러 부재** | DB commit·파일 move 가 MinIO put·RunRequest 앞에서 일어남, non-terminal 상태 TTL 리퍼 없음(디스패치-태그 경로만 abort 복구) | INGEST-3, INGEST-1, DISPATCH-5, DISPATCH-2, SAM3-4 | `batch_update_status`에 completed 스킵 가드 + stale `uploading`/`building`/`running` 리퍼 센서(stuck_run_guard 일반화) |
| **D. 동일 키/PK 재사용이 검수결과 클로버** | `sam3_shadow_compare`가 detection 키 + `image_label_id` 공유, reviewed/finalized 가드 없음 | SAM3-1 | shadow 전용 키 + insert에 review_status 가드 |
| **E. 터미널 상태 재처리 불가** | `failed`/`canceled` 를 재실행 경로가 없음(수동 DB 편집 필요) | DISPATCH-4, SAM3-3, LABEL-3 | 중복체크를 non-terminal 로 스코프 + 실패 후보쿼리에 bounded retry |
| **F. 미배선 자산** | asset은 등록됐으나 어떤 job/sensor도 selection 안 함 | BUILD-3 | classification finalize 센서 추가 또는 "수동 전용" 명시 |

---

## 🔴 즉시 (조용한 correctness 오류)

### BUILD-1. Video finalized 게이트가 폴더 스코프 → 미검수 비디오가 finalized 데이터셋에 유입 [확인]
- **경로**: `DATASET_REQUIRE_LS_FINALIZED=1`(prod 기본, docker-compose.yaml:47) 시 `find_project_video_candidates`(postgres_build.py:42-47)의 finalized EXISTS 절이 `r2.source_unit_name = r.source_unit_name`(폴더) join — **자산 단위 아님**. 폴더 안 비디오 1개만 finalize 돼도 폴더 전체 비디오가 조건 통과. image 쿼리는 `il.image_id=im.image_id`로 per-image 게이팅(postgres_build.py:89,93) — **비대칭**.
- **결과**: 100개 비디오 폴더에서 리뷰어가 1개만 finalize → 센서가 빌드 발화 → `build_dataset`이 나머지 99개(auto-generated/미검수) events JSON까지 vlm-dataset 에 복사 + dataset `completed` 마킹. **모델파생/미검수 라벨이 "finalized" 학습셋에 유입**(no-self-training·GT-only 불변식 위반). 이후 `NOT EXISTS datasets.completed` 게이트가 폴더를 **영구 재빌드 제외**.
- **가중**: 센서 readiness 체크(postgres_build.py:255-259)는 per-asset 인데 빌드 쿼리는 폴더 스코프 — 두 쿼리가 정확히 어긋난다.
- **수정**: video finalized 필터를 per-asset(`EXISTS ... l.asset_id=r.asset_id AND review_status='finalized'`)로, 복사 루프에서도 미finalize 비디오 스킵(image 쿼리 미러).

---

## 🟠 HIGH

### LABEL-1. auto_labeling_sensor가 dispatch로 이미 라벨된 비디오를 Gemini 재호출 [확인] — 패턴 A
routed 흐름은 `timestamp_status='completed'`만 세팅, `auto_label_status`는 NULL 유지. MVP `gemini_pending` CTE(sensor.py:33-36)는 `COALESCE(auto_label_status,'pending')='pending'` 매칭 → 모든 dispatch 비디오가 백로그로 잡힘. 센서가 spec_id/folder 태그 없이 `auto_labeling_job` 발화 → MVP 브랜치 → **같은 비디오 Gemini 재호출**. 같은 MinIO 키 overwrite(발견의 "다른 키" 표현은 부정확) + **카테고리필터 dispatch 라벨을 비필터 MVP 라벨로 clobber** + 평행 `labels` lineage. → 낭비 비용 + 라벨 오염.

### LABEL-2. auto_labeling_sensor in-flight 가드가 dispatch_stage_job 누락 → 동일 폴더 Gemini 동시처리 [확인] — 패턴 A
`AUTO_LABELING_TARGET_JOBS`={auto_labeling_job, mvp_stage_job}만. `dispatch_stage_job` 실행 중에도 가드 통과 → dispatch가 폴더 라벨링 중인데 auto_labeling_job이 같은 비디오 MVP 처리. 두 run이 같은 asset에 Gemini 동시호출 + 같은 vlm-labels 객체 race(run_coordinator가 두 job에 concurrency 태그 없음).

### SAM3-1. sam3_shadow_compare가 검출 키+PK 공유 → 재실행 시 사람검수 박스 clobber + review_status 리셋 [확인, 잠재] — 패턴 D
`sam3_image_detection`과 `sam3_shadow_compare`가 동일 `build_sam3_detection_key` + `stable_image_label_id`. shadow 후보(`find_sam3_shadow_candidates`)는 SAM3/reviewed 제외 안 함, `.pseudo.json` 스냅샷도 안 씀, insert가 `ON CONFLICT DO UPDATE SET review_status`로 무조건 덮음. → LS 검수 후 shadow 재실행 시 사람 박스 **복구불가 손실** + review_status → `auto_generated` 조용히 회귀. `update_image_labels_in_db`는 `finalized`만 가드하고 `reviewed`는 미보호. **현재는 YOLO 비활성(`ENABLE_YOLO_DETECTION=false`)이라 잠재** — 재활성 시 발현.

---

## 🟡 MEDIUM

- **INGEST-3** [확인] — 파이프라인 예외 시 `_mark_failed_records`가 `state.records` 전체를 `failed`로 batch update(status 가드 없음, postgres_ingest_raw.py:202-233) → 이미 `completed`+archive_path 세팅된 행까지 `failed`로 되돌림. 50파일 중 49 완료 + 1 dedup 게이트 실패 → 49개가 archive/MinIO엔 있는데 DB는 `failed` → 중복 재수집. (image asset만 게이트 트립, archive_path는 COALESCE로 미삭제라 self-inconsistent — blast medium.)
- **DISPATCH-1(concurrent-first)** [확인, high→medium] — 동일 폴더 최초 디스패치 TOCTOU: `get_in_flight_dispatch_requests`+`has_active_dispatch_run` 둘 다 사전행/사전run 필요 → 최초엔 둘 다 empty. file sensor + agent sensor가 다른 request_id로 동시 진입 → 각각 row insert + 다른 run_key RunRequest → **같은 incoming 폴더에 두 run** (archive move 충돌은 `__2` suffix로 부분완화, 중복 raw_files/MinIO 업로드는 잔존). folder_name UNIQUE/advisory-lock 부재.
- **DISPATCH-3** [확인] — `from_archived=true` 경로가 `dispatch_requests` row 미생성 + run-status finalizer의 `_TARGET_JOBS`에 `upload_label_job` 없음 + stuck_run_guard 미커버 → **디스패치 상태머신에 전혀 안 보임**. 실패/행이면 raw_files는 `archived`에 조용히 잔류, 자동 재시도 없음(데이터 손상 아님, 관측성/orphan 갭).
- **DISPATCH-4 (= 상태머신 DISPATCH-1)** [확인, 2 에이전트] — `get_dispatch_request_status`가 상태 무관 반환 → 터미널(`failed`/`canceled`) request_id 재드롭 시 `duplicate_request_id`로 **영구 거부**(file sensor는 `reject` 정책, failed/로 이동). 폴더가 idle·적격이어도 재실행 안 됨. 재시도는 새 request_id 필요.
- **LABEL-3** [확인] — patterns A+E. routed Gemini 실패 → `timestamp_status='failed'`. routed 후보쿼리·captioning 모두 `pending`/`completed`만 매칭, `failed`→`pending` 리셋 코드 없음 → **routed 머신에서 영구 orphan**(MVP 브랜치가 auto_label_status=NULL로 우연히 rescue하나 그게 LABEL-1의 divergent lineage). op은 SUCCESS 반환.
- **BUILD-3** [확인] — `build_classification`이 어떤 sensor/schedule/job selection에도 없음 → classification_* 폴더가 라벨링 완료해도 **vlm-classification 산출물/classification_datasets 행 자동 생성 안 됨**(수동 materialize만). 지원 머신(`find_projects_for_classification_build`)은 존재·정상이나 호출자 없음.

---

## 🟢 LOW (self-heal / cosmetic / 좁은 크래시윈도우)

- **INGEST-2** [확인, med→low] — 폴더 fast-path archive가 `os.rename`으로 디렉토리 통째 이동하는데 `total`은 미디어 확장자만 카운트 → 매니페스트 밖 stray 파일(.txt/.srt/dotfile/temp)이 DB/MinIO/JSONL 흔적 없이 archive로 쓸려감. **미디어 데이터는 정상**(복구가능, CLAUDE.md의 "미처리는 JSONL 추적" 정책 위반).
- **DISPATCH-5** [확인, low] — file sensor가 DB insert(`running`) → JSON move → yield 순서. move 전 크래시/move 실패 시 다음 tick 재읽기 → duplicate reject가 `upsert_failed_dispatch_request`로 라이브 run row를 `failed`로 clobber(Case A) 또는 디스패치 유실(Case B). CI dagster 재배포가 잦아 윈도우 비이론적이나 blast=단일 폴더.
- **SAM3-2 / BBOX-1** [확인, low] — asset의 프레임 1개만 성공해도 `bbox_status='completed'`(completed_assets set에서 실패 프레임 미제거). 단 `bbox_status`는 **write-only**(src 전체에서 READ 0곳) + 다음 tick self-heal → cosmetic 부정확.
- **SAM3-4** [확인, low] — ls_sync가 DB review commit → MinIO write 순서. write 실패+즉시 `/finalize` 시 stale auto 박스가 `image_label_annotations`에 GT로 투영. reviewed 행은 다음 tick 재기록 가능이라 복구경로 존재.
- **BBOX-2** [확인, low] — `raw_video_to_frame`(deps=raw_ingest only)→SAM3 경로가 `timestamp_status` 안 거치고 `bbox_status='completed'` 도달 가능. **의도된 YOLO 전용 바이패스**(CLAUDE.md가 두 status를 독립 신호로 규정) — 순진한 audit/join 쿼리만 오분류.
- **DISPATCH-2** [확인, low] — row insert+file move가 yield 앞 → 크래시 시 folder orphan. 단 다음 동일폴더 디스패치의 stale-cleanup이 self-heal.

---

## 유력(PLAUSIBLE) — 메커니즘 확인, 트리거 좁음

- **INGEST-1** [med] — 크래시로 `uploading` orphan 행이 미복구 → 이후 `find_any_by_checksum` dedup이 그 orphan을 dedup 타깃으로 삼아 **신규 파일 조용히 skip(바이트 유실)**. dedup-poison 메커니즘은 확인. 단 발견의 인과("sensor-path가 uploading 삽입+request_id 없어 finalizer early-return")는 **부정확**: auto_bootstrap은 `upload_enabled=False`라 `pending` 삽입, `uploading` 삽입하는 dispatch run은 `dispatch_request_id` 태그를 실제로 가짐 → clean FAILURE/CANCELED면 abort 복구됨. orphan-forever는 hard SIGKILL + 문서화된 `stuck_run_guard 좀비 갭` + 동일 checksum 재수집이 겹쳐야. 좁지만 실재.
- **SAM3-3** [low] — `ls_task_status='failed'`가 터미널, 재픽업 센서 없음 → 수동 리셋 필요(PART 1 확인). 단 "리셋 시 video task 중복" PART 2는 **반증**(project title suffix 안정 + `fetch_existing_task_stems` 스킵으로 create 경로 멱등).
- **BUILD-4** [low] — rebuild마다 새 `datasets` row(`building`) insert, orphan `building` 리퍼 없음 → 테이블 clutter. 단 트리거("transient NFS 에러") **부정확**(복사 루프는 try/except continue로 삼킴) + 모든 게이팅 소비자가 `build_status='completed'`만 필터 → orphan이 재빌드 차단/completed 위장 안 함. **inert clutter**, 상태머신 데드락 아님.

## 반증(REFUTED)

- **DISPATCH-2("stale running이 폴더 wedge")** — `service.py:382-391`의 request-scoped stale-row cleanup이 다음 동일폴더 디스패치에서 `canceled`로 닫음. lazy하지만 재디스패치=복구경로. wedge 안 됨.
- **BUILD-2("mixed timestamp+bbox, zero bbox clip이 폴더 빌드 데드락")** — `processed_clips`는 bbox 리뷰가 아니라 auto Gemini timestamp 라벨당 생성. timestamp finalized면 그 auto/completed 행이 존재→clip_to_frame이 clip 생성→게이트 통과. "finalized인데 clip 0"은 공존 불가. (단 bbox-ONLY 폴더 latent 이슈는 별개로 존재하나 YOLO 비활성으로 gated.)

---

## ✅ 검증됨 — 정상 (부서진 게 아님)

- dedup 결정성(checksum + phash), `find_by_checksum(completed_only)` 정합
- MinIO 키 도출 일관(`build_gemini_label_key`/`build_sam3_detection_key`/`stable_image_label_id`)
- SAM3 검출 후보 게이트(`NOT EXISTS label_tool='sam3'`) → 실패 프레임 다음 tick 재선택(self-heal)
- ls_sync `finalized` 가드(단 `reviewed`는 미보호 = SAM3-4)
- timestamp `.pseudo.json` write-once 스냅샷(MLOps 감사 C-1 수정분, 정상)
- 매니페스트 dedup/backpressure(in-flight run limit, per-tick limit, source-unit dedup, retry run_key)
- archive 폴더 충돌 `__2`/`__3` suffix

---

## 수정 우선순위 (권고 — Pareto)

**즉시(1개, 조용한 불변식 위반):**
1. **BUILD-1** — video finalized 게이트 per-asset 화. 이거 하나가 학습셋 GT 오염 + 영구 재빌드 제외를 만든다. 작은 SQL 변경.

**높은 ROI(1 수정 = 3 발견, 패턴 A):**
2. routed 완료/실패 시 `auto_label_status` 세팅 **또는** MVP 후보쿼리+센서 CTE에 `timestamp_status`/`timestamp_label_key` 가드 → **LABEL-1/2/3 동시 해결**(Gemini 중복비용 + 라벨 clobber + failed orphan).

**YOLO 재활성 전 필수:**
3. **SAM3-1** — shadow 전용 키 + review_status 가드(현재 잠재, 재활성 시 GT 파괴).

**리컨사일러 계층(패턴 C, 함께):**
4. `batch_update_status`에 completed 스킵 가드(INGEST-3) + stale `uploading`/`running`/`building` TTL 리퍼 센서(INGEST-1, DISPATCH-2/5, BUILD-4를 한 메커니즘으로 흡수, stuck_run_guard 일반화).

**재처리 경로(패턴 E, 저위험 개선):**
5. 디스패치 중복체크를 non-terminal 로 스코프(DISPATCH-4) + `ls_task_status='failed'`/`timestamp_status='failed'` bounded 재시도(SAM3-3, LABEL-3).

**관측성/운영:**
6. DISPATCH-3(from_archived 추적) · BUILD-3(classification 트리거) · INGEST-2(stray 파일 JSONL) — 데이터 손상 아님, 형편 될 때.

**결론**: 코어 흐름은 **구조적으로 건전**하고 대부분의 위험은 크래시-윈도우/재처리불가/미배선(진짜 데이터 손상은 좁은 트리거 뒤)이다. 유일하게 **일상 운영에서 조용히 불변식을 깨는 건 BUILD-1**. 그다음이 패턴 A(라벨 이중머신)로, 1 수정에 3 발견이 딸려온다.
