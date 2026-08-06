# 데이터 엔지니어 성장 로드맵 — 12개 페르소나 협업 결과 (2026-07-29 실측 검증판)

> - **작성**: 2026-07-28, 12개 페르소나(cto · data-engineer · dataops · ai-data · ai-engineer · ai-modeler · mlops · ops · qa-strategist · deploy-auditor · dagster-impl · tech-scout) 병렬 자문 → 합의 기준 종합.
> - **2026-07-29 팩트체크**: 초판이 로컬 sanitized 미러에서 작성돼 사실관계 오류가 다수 있었다. 6개 페르소나가 실제 repo·실행 컨테이너·prod DB 에 직접 대조해 검증했고, **22개 검증 항목 중 8개가 사실무근**으로 확인돼 삭제·정정했다. Fable 교차검증으로 우선순위도 재정렬했다.
> - 아래 표기: **[검증]** = 실측 확인, **[삭제]** = 전제가 거짓, **[정정]** = 사실이나 수치·범위가 틀렸음.
> - **운영 참고**: `docs/**`·`*.md`는 배포 workflow의 `paths-ignore` 대상이라 이 파일을 dev/main에 커밋해도 배포(=라벨링 중단)를 트리거하지 않는다.

---

## 0. 초판에서 삭제된 항목 — 착수하면 순손실

초판이 "가장 강한 신호"로 꼽은 합의 항목 상당수가 실재하지 않았다. 다시 백로그에 올리지 말 것.

| 초판 주장 | 실측 결과 |
|---|---|
| **동시성/GPU 게이트가 무방비** — `dagster.yaml` 에 `tag_concurrency_limits` 블록이 아예 없고, yaml 3개가 3-way drift, 검증 테스트도 스테일 | **[삭제]** `docker/app/dagster_home/dagster.yaml:17-21` 에 `gpu_trainer:1` / `pg_writer:1` 존재. git 추적(커밋 `3c476fa`), **prod 컨테이너 내부 파일도 동일**. dagster.yaml 은 repo 에 **1개뿐**이라 3-way drift 불가. `tests/unit/test_dagster_yaml_concurrency_tags.py` 는 실제 파일을 읽으므로 스테일 아님 |
| **`dagster.yaml` 이 리뷰 사각지대** (rsync exclude 라 아무도 안 봄) | **[삭제]** rsync `--exclude='dagster_home/'` 는 gitignore 된 `storage/` 를 `--delete` 로부터 보호하는 용도. `dagster.yaml` 은 tracked 라 `deploy-stack.sh:71-77` 의 `git reset --hard ${GITHUB_SHA}` 가 배포 SHA 로 정렬한다. **이미 PR 리뷰 → 머지 → 배포 경로를 탄다** (rsync exclude 와 git reset 을 혼동한 오판정) |
| **복구 도구 2종 버그** — `clear_maintenance.sh` 죽은 URL, `promote_model.py --model-version-id type=int` | **[삭제]** 커밋 `337e57e`(2026-07-28, 초판 작성 당일)로 **이미 수정됨**. 현재 `localhost:8002`/`:8004`, `--max-time` + non-zero exit. `promote_model.py:233` 은 type 선언 제거됨 |
| **pg-backup 이 profile 로 꺼져 있어 cutover 이후 백업 0회** | **[삭제]** `docker/.env` 의 `COMPOSE_PROFILES` 에 `backup` 포함. `docker-pg-backup-1` 기동 중이고 restic 스냅샷 2026-07-29 자 존재 |
| **Slack 알림은 NAS 장애뿐** | **[정정]** 3곳 배선돼 있음: `sensor_nas_health` + `sensor_cross_table_consistency` + LS 웹훅 계열 |
| **staging 전용 defs 모듈 ~731 LOC 이 환경 패리티 훼손** | **[삭제]** 그런 모듈 없음. `IS_STAGING` 은 `lib/env_utils.py`(범용 헬퍼)와 `lib/runtime_profile.py` 에서만 정의되고, 나머지는 기존 로직 안의 `if IS_STAGING:` 분기 조각. 최대 후보인 `lib/minio_cross_sync.py` 도 150 LOC 의 정상 유틸 |
| **DuckDB 잔재 resources 12파일** | **[삭제]** `resources/` 에 duckdb 파일 **0개**. 13개 문자열 매치는 전부 마이그레이션 역사 주석. CLAUDE.md 쪽("전부 제거됨")이 정확 |
| **`dup_group_id` 를 split key 로 쓰면 근사중복 train/test 누수 차단** | **[삭제]** prod `raw_files` 129,970건이 **전부 video**, `dup_group_id`/`phash` **0건**(이미지 전용 pHash 경로). "dedup 이 이미 계산하는" 이라는 전제 자체가 거짓. 영상 내 프레임 누수는 이미 `source_asset_id` 그룹 분할이 차단하고 있고, 잔여 위험인 **영상 간** 근사중복은 프레임 임베딩 클러스터를 split key 로 써야 잡힌다 |

---

## 1. 실측으로 확인된 문제 (유효)

| 항목 | 실측 근거 |
|---|---|
| **`rollback.sh` 가 조용한 no-op 가능** | `rollback.sh:29` 가 같은 태그(`gpu-cu124`)로 retag 후 `--force-recreate` 없이 `up -d`. `deploy-stack.sh:212-213` 이 "`docker compose up -d` 가 image SHA 변경을 항상 감지하진 않음(같은 tag 라 unchanged 로 판단)"이라고 명시하고, 배포 경로는 `stop`+`rm -f`(`:201-202`)로 회피하는데 롤백엔 그 단계가 없었다. 게다가 헬스체크는 되돌리려던 나쁜 코드도 통과시킨다 → **"롤백 완료" 출력 후 exit 0 인데 아무것도 안 바뀜**. 부수적으로 git 불변식도 깨짐(`docker tag` 만 하고 git 명령 0개) |
| **run 실패가 무음** | `defs/dispatch/sensor_run_status.py:44-58` 의 `_resolve_dispatch_request_id()` 가 dispatch 태그 없는 run 을 전부 None 처리 → SAM3 detection·embedding·clip·build·GCS 실패는 DB 에도 Slack 에도 안 남음. **이 파일에 Slack 을 붙이면 dispatch 만 잡히고 "했다"고 착각하게 된다** (필터 없는 별도 센서가 정답) |
| **SAM3 drain 이 구조적으로 불능** | `docker/sam3/app.py:41-49` 정비 플래그가 모듈 전역 dict, uvicorn `--workers 3`(fork 된 독립 프로세스) → `POST /maintenance/enter` 는 워커 1개에만 도달. **더 나쁜 사실**: `set_gpu_maintenance(active=True)` 를 호출하는 코드가 `src/`·`scripts/` 전체에 **0건**(`active=False` 만 존재)이고 `gpu_maintenance_lock` 은 0행 → fail-safe 가 감시하는 경로(PG)와 실제로 쓰이는 경로(HTTP)가 서로 다르다. `app.py:52-53` 의 TTL 도 저장만 하고 검사하지 않는 장식 |
| **MLOps 체인은 끊긴 게 아니라 한 번도 흐른 적 없음** | `model_registry` **0행**, `train_dataset_versions` **0행**, `vlm-dataset/_trainsets/`·`_models/` **0객체**. `docker/trainer/entrypoint.py:62-65` 에 M-1 미배선 TODO 주석, `defs/train/eval.py:102,116` 채점부 `NotImplementedError` |
| **pseudo-label QA 가 현재 오독 생산기** | GT 248장 중 `.pseudo.json` 스냅샷 **0장**(스냅샷 writer 는 2026-07-07 도입, GT 는 그 이전 검출분). 지금 materialize 하면 `macro_f1=0.0` 이 "SAM3 성능 0" 으로 읽힌다. 배포 후 검출분은 정상(2026-07-16 표본 50/50 스냅샷 존재) |
| **`dataset_split` 이 그룹 추가만으로 재분할** | `lib/dataset_split.py` 의 `rng.shuffle(keys)` 가 리스트 길이 의존(Fisher-Yates 가 길이만큼 난수 소비) → 그룹 1개만 늘어도 seed 고정에 무관하게 순열이 바뀐다. 진짜 해악은 "버전 간 비교 불가"가 아니라 **v1 의 train 그룹이 v2 의 test 로 이동해 incumbent 가 자기 학습 데이터로 채점받고 candidate 를 부당하게 탈락시키는 것**. 실측(UUID 키 300회 평균 test 이월률): 12→13 **66.3%**, 20→21 82.9%, 50→51 75.6% |
| **pgvector 0.8.2 (prod·staging 공통)** | 0.8.3 이 HNSW vacuum 인덱스 corruption 수정, **0.8.4 가 "hnsw graph not repaired" + vacuum 중 insert 오류 추가 수정**, 최신 stable 은 **0.8.5**. 단 `image_embeddings` 는 사실상 append-only(`NOT EXISTS` 필터로 재선택 안 함, upsert UPDATE 분기는 partial retry 한정)라 트리거 확률 낮음 |
| **prod Postgres 이미지에 Dockerfile 이 없음** | `POSTGRES_IMAGE=pgduckdb/pgduckdb:15-v1.1.1` 퍼블릭 이미지 + apt pin. 레포에 빌드 정의 없음. staging 은 로컬 빌드 `datapipeline-pg-pgvector:15-v1.1.1` 라 `docker image prune` 에 취약 |
| **컨트롤플레인 SQLite** | `run/event/schedule storage` 전부 SQLite. 단 "단일 파일"은 오류 — event log 는 run 별 샤딩이라 `storage/` 가 **3.2GB / .db 4,481개 + compute_logs 4,338 디렉토리**. 센서 21개, `max_concurrent_runs=20` |
| **asset_check 가 ingest 한 파일에만, 전부 WARN** | `defs/ingest/asset_checks.py` 가 repo 유일한 `@asset_check` 정의 파일. 3개 전부 `blocking=False` + `WARN`. 단 코드 docstring 이 이미 "운영 안정화 후 ERROR 승격 가능"이라 명시 — 새 발견 아님 |
| **reconciliation 부재** | `raw_files ↔ MinIO ↔ archive` 정기 대조 asset/schedule/sensor 없음 |
| **`dataset_pull.py` md5 스텁** | `_computed_md5()` 가 항상 None → 실 pull 이 바이트가 맞아도 매번 exit 3. 단 현재 미사용 스캐폴딩 |

### 수치 정정

| 초판 | 실측 |
|---|---|
| test split ≈ 25장 | **31장**(seed=42). 그러나 **소스 영상 2편**의 프레임이라 **유효 n=2**, `person` 클래스는 test 4장뿐 |
| `video_metadata` status 컬럼 ~20개 | 총 42컬럼 중 순수 `*_status` 는 **6개**. status/error/completed_at 트리플 6세트로 세야 21개 |
| `al_confirmed_count` 영구 0 | 맞지만 더 강함 — `train_dataset_versions` **테이블 자체가 0행**. `postgres_train.py:99-118` 의 `find_al_confirmed_image_ids()` 가 `return set()` 하드코딩. AL 큐(`fiftyone_pgvector.py`)는 read-only 이고 **fire/smoke centroid 기준이라 현 GT(patient/person)와 클래스축 불일치** |
| pgvector 타겟 0.8.3 | **0.8.5** |

---

## 2. 백로그 (재정렬)

### 착수 완료 (2026-07-29)

- [x] **`rollback.sh` 3중 결함 수정** — `--force-recreate` 추가(no-op 차단), 컨테이너 image ID 실검증([4/4] 단계, 불일치 시 exit 1), 태그→SHA 추출 후 git reset. 가드 3종: 비-SHA 태그 거부 / dirty tree 시 skip(커밋 안 된 남의 작업 파괴 방지) / `main()` 래핑으로 자기수정 차단. `docker compose` 직접 호출도 `compose-prod.sh` wrapper 로 교체
- [x] **전역 run 실패 Slack 센서** — `defs/shared/sensor_run_alert.py` 신규. 기존 `sensor_run_status.py` 에 붙이면 dispatch 만 잡히므로 **필터 없는 별도 `@run_status_sensor(FAILURE)`** 로 구현. CANCELED 는 제외(운영자 수동 취소 + stuck_run_guard 자동 취소가 섞여 소음). 실측 최근 90일 FAILURE 18건 ≈ **0.2건/일** 이라 throttle 불필요 — 빈도가 오르면 `sensor_nas_health` 의 `NAS_ALERT_COOLDOWN_SEC` 패턴을 가져오면 된다

### P1 — 이번 분기

- [x] **pg Dockerfile 정식 자산화** (2026-07-29) — `docker/postgres/Dockerfile` 신규. `ARG PGVECTOR_APT_VERSION=0.8.5-1.pgdg12+1` 로 **버전 고정**(staging 의 기존 로컬 이미지는 `apt install` 을 버전 없이 호출해 빌드 시점마다 다른 pgvector 가 들어갔다). 빌드 검증: pgvector 0.8.5 설치, `pg_extension_update_paths` 가 **0.8.2 → 0.8.5 경로 확인**, HNSW 인덱스 + DELETE + VACUUM 동작, `pg_duckdb` 1.1.0 및 USER/ENTRYPOINT 보존. `docker/postgres/` 는 CI 재빌드 트리거 목록에 없어 커밋해도 이미지 재빌드가 일어나지 않는다. 핀 회귀 방지 테스트 3개 추가
- [ ] **pgvector 0.8.2 → 0.8.5 적용** — 위 Dockerfile 로 **준비 완료, 정비 창 대기**. 트리거는 달력이 아니라 **첫 대량 벡터 삭제 직전**(PE-Core 승격 후 구버전 벡터 정리, 벌크 delete). append-only 인 지금은 vacuum 이 dead tuple 을 거의 안 만난다. 절차: `docker build -t datapipeline-postgres:15-pgvector0.8.5 docker/postgres` → `.env` 의 `POSTGRES_IMAGE` 교체 → `compose-prod.sh up -d --force-recreate postgres` (**전체 DB 다운타임**) → `ALTER EXTENSION vector UPDATE` → recall/EXPLAIN 재검증
- [x] **SAM3 워커 간 정비 전파 + TTL 강제** (2026-07-29, **코드만 — 미배포**) — 정비 상태를 모듈 전역 dict → **워커 공유 파일**(`SAM3_MAINTENANCE_STATE_PATH`, 기본 `/tmp/sam3_maintenance.json`)로 이전. uvicorn `--workers N` 은 fork 된 독립 프로세스지만 **같은 컨테이너 안**이라 파일 하나면 충분하다 — PG 드라이버 없이 컨테이너의 `vlm_pipeline-free` 설계를 유지한다. 함께 `_is_expired()` 로 **TTL 을 실제로 강제**해(기존엔 저장만 하고 검사 안 함) `/maintenance/exit` 를 잊어도 스스로 풀리게 했다. 손상된 상태 파일은 fail-open(게이트가 닫히면 SAM3 전면 503 이므로), 쓰기 실패는 500 으로 노출(조용히 실패하면 운영자가 진입 성공으로 오인).
  검증: 모듈 인스턴스 2개를 워커로 삼은 재현에서 **옛 구현 `워커2.active=False` → 새 구현 `True`**. 정비 관련 테스트 33개 통과, 신규 4개(타 워커 상태 존중 / TTL 자동해제 / heartbeat 가 해제된 게이트를 되살리지 않음 / 손상 파일 fail-open).
  ⚠️ 남은 것: 컨테이너 재시작을 가로지르는 유지와 `maintenance_guard_sensor` 연동은 여전히 PG `gpu_maintenance_lock` 쓰기 경로가 필요하다(미배선). ⚠️ **`SAM3_WORKERS=1` 우회안은 채택하지 않았다** — exit 를 잊었을 때 workers=3 은 1/3 저하지만 workers=1 은 **전면 영구 정지**가 된다. 정비 창에는 `docker stop docker-sam3-1` 이 더 안전하다(GPU 11.1GB 전량 해제).
  ⚠️ **배포 주의**: `docker/sam3/` 는 CI 이미지 재빌드 트리거 목록에 있어, 커밋하면 prod SAM3 재빌드 + dagster 재기동이 일어난다
- [x] **`_split_groups` 안정 정렬** (2026-07-29) — `rng.shuffle(keys)` → 결정적 per-key 해시 **정렬**(`sha256(f"{seed}:{k}")`). 순수 함수 2줄 변경, 시그니처 불변이라 호출부(`dataset.py:105`, `:482`) 수정 불필요. 효과 실측(ratios 0.8/0.1/0.1, "그룹 추가 시 기존 test 그룹 축출" 위반 시드 비율): 옛 구현 n=20→21 **6/200**, 30→31 **17/200**, 40→41 **24/200** → 새 구현 **셋 다 0/200**. 회귀 테스트는 시드 50개를 훑으며, 옛 구현으로 되돌린 사본에서 실제로 실패함을 확인했다(시드 하나로는 옛 구현도 통과해 가드가 안 된다). 지금 한 이유는 `train_dataset_versions` 가 0행이라 마이그레이션 비용이 0이기 때문. ⚠️ 독립 해시 **버킷팅**은 채택하지 않았다 — n=12 에서 비율 분산이 커 test 그룹 0개가 나올 수 있고 `dataset_split.py` 의 non-empty 보장을 버리게 돼 코드가 오히려 는다. `min_per_split` 1→2 상향도 **하지 않았다**(레코드 단위 floor 라 test_groups=2 에서도 통과, 유효 n 은 그대로 2 — 효과 없음)
- [ ] **trainer↔registry M-1 배선** — `TRAIN_DATASET_VERSION_ID` 독취 + `insert_candidate_model_version` 호출. M-2(eval 채점부)의 선행

### P2 — 조건부 / 나중

- [x] **pseudo QA abstain 표기** (2026-07-29) — `annotate_scorability()` 가 `scorable_items = gt_items - missing_pseudo` 와 `abstained` 를 리포트에 붙인다. abstain 이면 markdown 머리말이 표 대신 **"ABSTAIN (채점 가능 표본 0) … macro_f1 은 측정 불가이며 성능 0 이 아니다"** 를 렌더하고, asset 로그도 `info` → `warning` 으로 바뀐다. 메타데이터에 `scorable_items`/`abstained` 노출. 테스트 4개 추가(bbox abstain / 스냅샷 1개라도 있으면 비-abstain / timestamp abstain / GT 0 도 abstain), 관련 28개 통과. ⚠️ **정기 스케줄은 여전히 걸지 말 것** — 현재 출력이 전량 all-FN 이다. `scorable_items` 가 0을 벗어나는 날이 곧 스케줄 도입 시점
- [ ] **SAM3 eval 최소표본 abstain floor** — 논리는 맞으나 `_score_candidate`/`_score_incumbent` 가 `NotImplementedError` 이고 candidate 0행이라 실행 경로가 없다. M-1 → M-2 이후, 스코어러가 싣는 `n_eval_images` 필드 계약과 함께 한 번에
- [x] **reconciliation 측정 1회** (2026-07-29) — 정기 잡을 만들기 전에 drift 가 실제로 0이 아닌지부터 쟀다. 결과: **drift 는 0이 아니지만 산발적이지 않고 한 코호트에 집중**돼 있다.

  | 그룹 | MinIO 존재 | 결손 |
  |---|---|---|
  | `completed` (200 표본) | 200 | **0** |
  | `checksum IS NULL` (200 표본) | 200 | **0** |
  | `uploading` (200 표본) | 16 | **184 (92%)** |
  | `failed` (10 전수) | 0 | **10 (100%)** |

  - `raw_files` 129,970행 = `completed` 129,089 / `uploading` **871** / `failed` 10. `raw_key` 는 전부 distinct, `checksum` UNIQUE 제약 실재하며 위반 0건.
  - **`uploading` 871건은 전부 `source-h`, 2026-04-16 03:38–04:14 의 36분 창**에서 나왔다. 3개월 넘게 고착된 단일 코호트이며, 2026-06 심층감사의 "source-h key 불일치" 건이 여전히 미해결인 것으로 보인다.
  - `checksum IS NULL` 694건은 전부 `completed` 이고 **MinIO 객체는 정상 존재** — 객체 결손이 아니라 메타데이터 결손이다(중복 판정에서 이 694건이 빠진다).
  - 결론: **상시 reconciliation 잡의 근거는 아직 약하다.** 정상 경로(`completed`)는 200/200 정합이고 드리프트는 과거 사고 잔재 1건이다. 정기 잡보다 (a) source-h 871행 정리 (b) `checksum` NULL 694행 백필 이 먼저다. 잡은 재발이 관측된 뒤에 만들어도 늦지 않다

- [x] **source-h 코호트 원인 규명 + 복구 스크립트** (2026-07-29, **dry-run 만 실행 — prod 미변경**) — 위 측정의 후속. 업로드 실패가 아니라 **키 불일치**였다:

  | | 값 |
  |---|---|
  | MinIO 실제 객체 | `source-h/<카테고리>/<원본 한글명>.mp4` — **871개** (smoke 430 / helmet 370 / fire 61 / falldown 10) |
  | DB `raw_key` | `source-h/<카테고리>/<sanitize 로마자명>.mp4` — **871행**, 카테고리 분포 동일 |
  | 매핑 | `sanitize_path_component`+`sanitize_filename` 로 **871/871 완전 일치** |

  `lib/env_utils.py:87` 이 "INGEST 의 raw_key 는 `sanitize_path_component` 기준"이라 명시하므로 **DB 가 정본이고 객체 키가 비정규**다. 따라서 복구는 재수집이 아니라 **버킷 내 서버사이드 복사**로 끝난다 — 바이트 재업로드도, NAS 접근도 불필요.
  - NAS 원본은 이미 없다(`source_path` 가 구 마운트 `/nas/incoming/...`, archive 에도 source-h 없음) → **살아있는 사본은 MinIO 비정규 키 객체뿐**이므로 검증 전 원본 삭제 금지.
  - 영향: `ingest_status='completed'` 가 dedup·build·labeling 쿼리를 전부 게이트한다(`postgres_dedup/build/labeling.py`). `video_metadata` 는 871행 다 있는데도 **871개 영상이 라벨링 파이프라인에서 통째로 빠져 있다.**
  - `scripts/repair_unsanitized_raw_keys.py` 신규(기본 dry-run). prod dry-run 결과: **복사 대상 804 / 이미 존재 67 / 크기 불일치 0 / DB 행 없음 0**. 804 는 2026-06 심층감사에 기록된 "source-h 804 key 불일치" 숫자와 정확히 일치 — 같은 건이 미해결로 남아 있던 것이다.
  - **2026-07-29 적용 완료 (복사만)**: `--apply` 로 804건 서버사이드 복사, 실패 0. 재실행 시 "복사 대상 0 / 이미 존재 871" 로 멱등. 독립 검증에서 `uploading` 871행의 `raw_key` 가 **871/871 MinIO 존재, 결손 0** (복구 전 표본은 92% 결손). 원본 `source-h/` 은 보존했다.
  - ⚠️ **상태 전이는 의도적으로 보류**: `--mark-completed` 는 (a) CLAUDE.md 의 "archive 이동 완료된 파일만 completed" 규칙과 어긋나고(이 코호트는 NAS 원본이 없어 archive 이동 자체가 불가능), (b) `auto_labeling_sensor` 가 **기본 RUNNING** 이라 전이 즉시 **871건이 Gemini 라벨링 대상**이 된다(`auto_label_status`/`timestamp_status` 둘 다 871건 전부 pending 확인). Vertex AI 비용·GPU 가 실제 발생하므로 운영자 명시 결정 사항으로 남긴다. 지금은 바이트만 정위치에 있고 파이프라인 상태는 그대로다
- [x] **`dataset_pull.py` md5 구현** (2026-07-29) — `_computed_md5()` 스텁(항상 None)을 제거하고 `lib/dvc_pull.compute_dvc_md5()` 로 대체. DVC 규칙을 그대로 구현했다: 파일=내용 md5, 디렉토리=각 파일의 `{"md5","relpath"}` 를 relpath 정렬 후 `json.dumps(sort_keys=True)` 해시 + `.dir` 접미사. **컨테이너의 dvc 3.67.1 로 `dvc add` 를 실제 돌려 대조 검증**했다(디렉토리 `846b769158c938838ce462bb0a116d21.dir`, 파일 `b1946ac92492d2347c6235b4d2611184` 양쪽 일치). 이전엔 카탈로그에 `dvc_md5` 가 있는 정상 케이스에서 바이트가 맞아도 **항상 exit 3** 이었다 — "검증 없음"보다 나쁜 "가짜 실패". 테스트 5개 추가(파일/디렉토리 DVC 일치 / 없는 경로 None / 정상 pull 통과 / 변조 감지)
- [ ] **`checksum` NULL 694행 백필** — `scripts/backfill_checksums_from_minio.py` 신규(기본 dry-run). 대상은 전부 `source-b-202512` prefix, 2026-03-05 하루치, `source_unit_name` 도 빈 단일 코호트. NAS 원본이 없어(`archive_path` 가 구 마운트 `/nas/archive/...`, 현행 경로에도 없음) 기존 `recompute_archive_checksums.py` 를 쓸 수 없고 **MinIO 가 유일한 소스**다. `lib/checksum.sha256_stream()` 추가(해시 + 바이트 수 동시 반환 → 부분 다운로드를 `file_size` 로 걸러냄).
  전수 dry-run(742MB, 20초): **694/694 백필 가능, UNIQUE 충돌 0, 크기 불일치 0, 오류 0**. 다운스트림 트리거 없음(이 행들은 이미 `completed` 이고 `checksum` 은 중복 판정에만 쓰인다). **적용만 남음** — prod DB 쓰기가 권한 분류기에 차단돼 미실행.
  왜 필요한가: `checksum` UNIQUE 가 정확-중복의 1차 방어선인데 NULL 은 제약을 비껴가므로, 이 694건은 현재 **중복 검출에서 통째로 빠져 있다**
- [ ] **asset check 확장 + blocking 승격** — `raw_ingest_archive_consistency` 하나만 몇 주 false-positive 율 관찰 후 ERROR 승격 검토
- [ ] **`video_metadata` 정규화 / 파티셔닝 / 컨트롤플레인 PG 이관** — 전부 L 사이즈, 지금 급하지 않음
- [ ] **골든 벤치마크셋 동결** — `_split_groups` 안정화가 선행

### 착수 불필요 (명시적 기각)

- **Dagster SQLite 이벤트로그 retention** — 3.2GB 는 이 호스트에서 소음 수준이고, 컨트롤플레인 PG 이관과 공유하는 작업이 0이라 "저비용 선행조치"가 아니다(버리게 될 삭제 코드를 쓰는 것). 게다가 `storage/` 하위 순진한 삭제는 **센서 RUNNING/STOPPED 토글을 초기화**하고 `dispatch_sensor` 는 코드 기본값이 STOPPED 라 라벨링이 조용히 멈춘다
- **`purge_pipeline_data.py` 에 NAS 삭제 경로 추가** — archive 는 복구 원본이고(`reupload_minio_from_archive.py` 가 그걸 전제로 존재) CLAUDE.md 가 삭제 금지로 명시한다. `--all` 이 이미 DB 를 비우는 스크립트에 되돌릴 수 없는 NAS 삭제를 더하면 데이터 손실 footgun 이 된다. **부재는 의도된 설계**로 문서화하고 종결. NAS quota 가 동인이면 그건 별도 retention 정책 결정 사안

---

## 3. 로드맵 전체가 놓친 진짜 병목

**GT 248장 / 2클래스(patient·person) / 소스 영상 12편 / 1폴더.**

test split 이 항상 영상 2편이라 유효 n=2 이고, `lib/train_eval_gate.py:25` 의 `primary_margin=0.01` · `per_class_floor=-0.02` 는 이 표본에서 통계적으로 의미가 없다(`defs/train/eval.py:141-145` 의 TODO 도 같은 취지). eval 게이트·AL 영속화·split 안정화·승격 로직을 아무리 다듬어도 **입력이 이걸 지탱하지 못한다.**

학습 트랙의 실제 P1 은 코드가 아니라 **Label Studio 검수 처리량**이며, 소스 영상이 그룹 기준 최소 30~40편은 돼야 test 가 3~4편 이상 확보되고 그때부터 eval 논의가 의미를 갖는다. 이건 `ai-data-engineer` 영역이다.

### 부수 발견 (초판에 없던 것)

- **이미지 인제스트 경로는 prod 에서 한 번도 실행된 적 없다** — `raw_files` 129,970건 전부 video, `phash`/`dup_group_id` 100% NULL. 그런데 phash 계산 실패는 `gated_failed` 로 **run 전체를 실패시키는** 의미론을 갖는다. 누군가 incoming 에 이미지 배치를 처음 넣는 날 콜드 코드가 실행된다. 값싼 완화책: 그날 전에 staging 에서 이미지 배치 1회 테스트
- **`tests/unit` 113개 중 40개가 git 미추적** — CI 는 tracked 파일만 체크아웃하므로 이 40개를 아예 돌리지 않는다. 로컬 전체 스위트 실패 52건은 거의 전부 여기서 나온다(tracked 만 보면 1건). 커밋하거나 삭제해서 "초록인데 안 도는" 상태를 끝내야 한다

---

## 4. 아키텍처 학습 로드맵

초판 유지. 단 아래 항목은 근거가 바뀌었다.

- **Tier 1-3 분산 락·리스·fencing token** — 유지하되 프레이밍 수정: `gpu_maintenance_lock` 의 문제는 fencing 부재가 아니라 **락 테이블이 실제 게이트 결정에 참여하지 않는다**는 것(진입 경로가 PG 를 쓰지 않음). 배울 주제는 같지만 이 코드베이스의 결함 원인은 다르다
- **Tier 2-12 평가 통계·분할 설계** — `dup_group_id` 인사이트는 **폐기**(위 §0). 대신 유효한 주제: 그룹 분할에서 **유효 표본수 = 그룹 수**이지 이미지 수가 아니라는 것, 그리고 안정 분할(stable partitioning)
- **Tier 3-13 Lakehouse** — `pg_ducklake` v1.0 이 PG extension 형태로 존재. 이미 `pg_duckdb` 를 쓰는 이 스택엔 DuckLake 보다 이쪽이 더 직접적인 파일럿 후보
- **Tier 3-15 MLflow alias** — prod MLflow **2.16.2** 실측(2.9 훨씬 상회), alias API 이미 사용 가능. 커스텀 `model_registry` 와의 매핑 설계만 남음
- **Tier 3-16 폴링→CDC** — Dagster 공식 마이그레이션 가이드가 "AutomationCondition 은 외부 변화 **감지**를 대체하지 못하고 감지 이후 오케스트레이션만 대체"라고 명시. 초판 판단 유효(prod dagster 1.13.12)

---

## 5. CTO 3픽 (재작성)

초판 3픽 중 2개가 오판정 위에 있었다(동시성 게이트 = 이미 정상, staging 전용 코드 = 존재하지 않음).

1. **break-glass 도구의 신뢰 회복** — 롤백이 성공을 보고하며 아무것도 안 할 수 있고, 정비 모드가 워커 1/3 에만 걸리며, fail-safe 가 실사용 경로를 못 본다. 장애 시 쓰는 도구가 장애를 키우는 구조가 가장 비싼 부채다 (1번 완료, SAM3 잔여)
2. **관측 최소선** — run 실패 알림(완료). 그 다음은 exporter 가 아니라 **무엇이 실제로 조용히 실패하는지** 한 달 관찰
3. **GT 처리량** — 학습 트랙의 모든 코드 항목이 여기에 막혀 있다. 인프라를 더 다듬는 대신 검수 파이프라인 처리량에 투자하는 게 레버리지가 크다

관통 테마는 초판과 동일하게 **"선언된 상태 ≠ 실제 상태"** 이지만, 실측 후 목록이 바뀌었다: 조용한 no-op 롤백, 워커 1/3 정비 플래그, 쓰이지 않는 PG 락, 0행 레지스트리, CI 가 안 도는 40개 테스트, 스냅샷 없는 QA. **초판 자신이 이 테마의 사례였다** — 검증 없이 선언된 8개 항목이 실제 상태와 달랐다.
