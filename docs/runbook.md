# 운영 트러블슈팅 런북

문제 유형별 진단 및 즉시 조치 가이드.

현재 branch-based runtime 기준:
- `main` = production, `dev` = staging
- 스테이징은 상시 기동이 아니라 필요할 때 올려서 검증하는 구조입니다

> ## ⚠️ 이 문서를 읽기 전 — 용어/명령 치환표
>
> 이 런북에는 **DuckDB 시절(2026-05-19 Postgres cutover 전)** 의 명령과 경로가 많이 남아 있습니다.
> 과거 장애 대응 지식 자체는 유효하므로 보존했지만, **명령을 그대로 복사하면 동작하지 않습니다.**
> 아래로 치환해서 읽으세요.
>
> | 문서에 적힌 것 | 현재 실제 |
> |---|---|
> | `python3 scripts/query_local_duckdb.py --sql "…"` | `docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "…"` (스크립트는 `scripts/archive/` 로 이동됨) |
> | `pipeline-dagster-1` / `pipeline-dagster-daemon-1` | `docker-dagster-1` / `docker-dagster-daemon-1` (스테이징은 `pipeline-test-*`) |
> | `/data/pipeline.duckdb`, `/data/staging.duckdb` | Postgres `vlm_pipeline` / `vlm_pipeline_staging` |
> | `/nas/incoming`, `/nas/archive` | `/nas/data/incoming`, `/nas/data/archive` (단일 부모 바인드 `/nas/data`) |
> | `/home/user/mou/staging/...` | `/home/user/mou/nas_primary/staging/...` |
> | `10.0.0.10` (호스트), `10.0.0.36`/`.51` (MinIO) | `10.0.0.10` (호스트), `10.0.0.51` (MinIO/NAS) |
> | `duckdb_writer` / `duckdb_*_writer` 태그 경쟁 | **해당 태그 없음.** 현재는 `max_concurrent_runs: 20` + `gpu_trainer` limit 1 |
> | MotherDuck 동기화 (§9) | **코드 제거됨** — `scripts/archive/` 에만 존재 |
> | YOLO 검출 (§8) | `ENABLE_YOLO_DETECTION=false`, bbox 는 SAM3 담당 |

**공통 도구:**
```bash
# DB 읽기
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "<SQL>"

# 컨테이너 DSN: postgresql://airflow:***@docker-postgres-1:5432/vlm_pipeline (호스트 노출 :15433)
# Manifest 경로: /nas/data/incoming/.manifests/pending | failed
# 실패 로그:     /nas/data/incoming/.manifests/failed/*.jsonl
```

---

## 1. Dagster 서버/프로세스

### Dagster UI 접속 불가 (포트 충돌, LOCATION_ERROR)
```bash
docker logs docker-dagster-1 | tail -n 100
docker compose restart dagster
ss -tln | grep 3030
```

### test daemon heartbeat 충돌
- **원인:** test가 production과 동일한 Dagster runtime storage 공유
- **영구 조치:**
  - test runtime은 production과 별도 `DAGSTER_HOME`/storage를 사용
  - runtime storage는 git working tree 밖으로 분리
- **확인:** 재기동 후 heartbeat 충돌 로그 없음, sensor tick 정상 순환

### STARTED/CANCELING run이 장시간 점유 (backpressure 문제)
- **원인:** worker 프로세스가 없는데 UI에 `STARTED` / `CANCELING` 잔류 (고아 run).
  ⚠️ 과거 원인이던 `duckdb_writer` / `duckdb_raw_writer` / `duckdb_label_writer` /
  `duckdb_yolo_writer` **슬롯 경쟁은 현재 존재하지 않습니다** — 해당 태그가 코드에서 제거됨.
  지금 남은 tag limit 은 `gpu_trainer`(1) 뿐이고 그 외엔 `max_concurrent_runs: 20` 만 적용됩니다.
  즉 요즘 이 증상은 대개 gRPC 단절/컨테이너 재기동으로 생긴 고아 run 이며,
  `stuck_run_guard_sensor` 가 자동 정리를 시도합니다.
- **즉시 조치:**
  ```bash
  # 1) 비정상 run 상태 확인
  docker exec docker-dagster-1 bash -lc "python3 - <<'PY'
  from dagster import DagsterInstance
  from dagster._core.storage.dagster_run import RunsFilter, DagsterRunStatus

  inst = DagsterInstance.get()
  runs = inst.get_runs(
      filters=RunsFilter(statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING]),
      limit=20,
  )
  for run in runs:
      print(run.run_id, run.job_name, run.status)
  PY"

  # 2) 강제 종료 (⚠️ repair_stale_dagster_runs.py 스크립트는 삭제되어 더 이상 없음)
  #    Dagster GraphQL terminateRun — 워커가 이미 죽은 좀비는 FORCE 정책으로만 정리된다
  docker exec docker-dagster-1 bash -lc "python3 - <<'PY'
  import requests
  RUN_IDS = ['<run_id_1>', '<run_id_2>']
  MUT = '''mutation(\$runId: String!) {
    terminateRun(runId: \$runId, terminatePolicy: MARK_AS_CANCELED_IMMEDIATELY) {
      __typename ... on TerminateRunSuccess { run { runId status } }
      ... on PythonError { message }
    } }'''
  for rid in RUN_IDS:
      r = requests.post('http://localhost:3030/graphql', json={'query': MUT, 'variables': {'runId': rid}})
      print(rid, r.json()['data']['terminateRun'])
  PY"

  # 3) 필요 시 실패 지점부터 재실행 (Dagster UI: run → Re-execute → From failure,
  #    또는 GraphQL launchPipelineReexecution FROM_FAILURE)

  # 4) 30~60초 뒤 queue가 다시 launch되는지 확인
  docker exec docker-dagster-daemon-1 bash -lc "tail -n 200 /opt/dagster/logs/daemon.log | grep -E 'Launching run|QueuedRunCoordinator|backpressure' | tail -n 50"
  ```
- **영구 조치:**
  ```
  STUCK_RUN_GUARD_ENABLED=true
  STUCK_RUN_GUARD_INTERVAL_SEC=120
  STUCK_RUN_GUARD_TIMEOUT_SEC=10800
  STUCK_RUN_GUARD_ORPHANED_RUN_TIMEOUT_SEC=900
  STUCK_RUN_GUARD_AUTO_REQUEUE_ENABLED=true
  STUCK_RUN_GUARD_TARGET_JOBS=mvp_stage_job,ingest_job,dispatch_stage_job
  ```
  (prod `.env` 에는 아직 `motherduck_sync_job` 이 남아 있으나 그런 job 은 존재하지 않는 무해한 잔재입니다.)
- **디스크/빌드 캐시 점검:** `database or disk is full`, `disk I/O error`가 보이면 stale run 정리 전에 아래를 같이 확인
  ```bash
  df -h /
  docker system df
  docker image prune -f
  docker builder prune -f
  ```

### git switch 차단 (legacy test runtime 파일 충돌)
- **원인:** `runs.db`, `schedules.db`, `.nux/`, `.telemetry/` 같은 runtime 파일이 git working tree 내 생성
- **영구 조치:** test Dagster storage를 git working tree 밖으로 유지. 브랜치 전환 전 test 컨테이너 중지

---

## 2. DuckDB (⚠️ 레거시 — 2026-05-19 Postgres cutover 이전 내용)

> 이 섹션의 절차는 **현재 스택에 그대로 적용되지 않습니다.** 파일 기반 DuckDB 는 write path 에서
> 제거됐고 `scripts/query_local_duckdb.py` 도 `scripts/archive/` 로 이동했습니다.
> 스키마 문제는 `psql`(`\d <table>`) + `src/vlm_pipeline/sql/migrations/postgres/` 의 forward-only
> 마이그레이션(`_pg_migrations` 테이블이 이력 추적)으로 진단하세요.
> 아래는 과거 인시던트 기록으로만 보존합니다.

### raw_files 테이블 미존재
- **대표 에러:** `Catalog Error: Table with name raw_files does not exist`
- **원인:** DB 파일만 있고 테이블이 없는 상태에서 즉시 조회
- **즉시 복구:**
  ```bash
  python3 - <<'PY'
  import duckdb; from pathlib import Path
  ddl = Path('/src/vlm/vlm_pipeline/sql/schema.sql').read_text(encoding='utf-8')
  conn = duckdb.connect('/data/pipeline.duckdb'); conn.execute(ddl); conn.close()
  PY
  ```
- **영구 조치:** `ingested_raw_files()` 시작 직후 `db.ensure_schema()` 호출

### DuckDB lock/conflict
- **대표 에러:** `Could not set lock on file`, `Conflicting lock is held`
- **확인:**
  ```bash
  lsof ./docker/data/pipeline.duckdb
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
  ls -1 /nas/data/incoming/.manifests/pending/retry_*.json 2>/dev/null | tail -n 20
  ```
- **조치:** writer run 종료 대기 → transient 오류는 retry manifest 자동 생성 확인 → queue 과적재 시 backpressure 값 조정
- **완료 조건:** retry manifest로 흡수, `raw_files.failed` 누적 증가 없음

### DuckDB 파일 교체 시 WAL 문제
- **원인:** DB 파일 교체 후 기존 `.wal`이 남아 stale WAL 재적용
- **즉시 조치:** app 중지 → stale WAL을 별도 경로로 이동
- **운영 원칙:** DB 파일 교체는 항상 서비스 중지 상태에서, 기존 WAL 존재 여부 반드시 확인

### checksum 중복 (UNIQUE 제약 누락)
- **원인:** `CREATE TABLE IF NOT EXISTS` 방식이라 예전에 만들어진 테이블은 현재 `UNIQUE(checksum)` 제약이 자동 적용 안됨
- **진단 순서:**
  1. `raw_key` 중복과 `checksum` 중복 분리해서 확인
  2. `archive_path` 중복 별도 확인
  3. 운영 DB에 `UNIQUE(checksum)` 실제 존재 여부 확인
  4. DB checksum 그대로 믿지 말고 archive 원본 파일 재해시
- **복구:**
  ```bash
  python3 scripts/recompute_archive_checksums.py  # archive 기준 checksum 재계산
  python3 scripts/cleanup_duplicate_assets.py      # duplicate group 정리
  # ※ 실행 전 반드시 DB 백업
  ```

### image_metadata 마이그레이션 오류
- **대표 에러:** `image_metadata__migrated does not exist`
- **원인:** `ensure_schema()` 런타임 위험 동작, 스키마 카탈로그 상태 불일치
- **조치 (Postgres-primary 이후):**
  - 과거에 `scripts/repair_image_metadata_schema.py` (DuckDB schema repair) 가 있었으나 2026-05-19 PG cutover 후 제거됨.
  - PG 환경에서 schema mismatch 시 `psql -d vlm_pipeline` 으로 `\d image_metadata` 확인 + 누락 컬럼만 `ALTER TABLE ... ADD COLUMN` 수동 적용. 또는 dagster_home init schema 재실행.
  - 이후 운영 Dagster 재기동, stale run 정리, 재실행.

### test DuckDB not found
- **대표 에러:** `DuckDB not found: /data/staging.duckdb`
- **복구 방법:**
  - 운영 상태 재현: `pipeline.duckdb -> staging.duckdb` 복제
  - 빈 test 재테스트: `staging.duckdb` 삭제 후 스키마만 적용한 새 DB 생성

---

## 3. 센서 / auto_bootstrap

### auto_bootstrap 180초 gRPC timeout
- **대표 에러:** `DagsterUserCodeUnreachableError`, `Deadline Exceeded`
- **원인:** NAS/NFS 지연으로 discovery·스캔 I/O 초과, hidden entry 포함
- **NAS 지연 시 권장 설정:**
  ```
  AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES=20
  AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=3
  DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300
  ```
- **재발 시 확인:**
  1. hidden entry discovery 포함 여부 (`.Trash-1000`, `.DS_Store` 등)
  2. `max_units_per_tick`, `discovery_max_top_entries` 값
  3. 최근 tick 로그에서 `processed_units/budget/discovery_elapsed/scan_elapsed`

### 센서 스캔 지연 (폴더 증가)
- **원인:** 스캔 한도 부족, `_DONE` 마커 확인 미비
- **조치:**
  ```
  AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=100  # (기본값에서 상향 필요 시)
  ```
- **확인:** `.env`의 `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK` 값, 날짜 폴더 내 `_DONE` 파일 존재 여부

### pending queue 과적재
- **확인:**
  ```bash
  find /nas/data/incoming/.manifests/pending -maxdepth 1 -name '*.json' | wc -l
  ls -lt /nas/data/incoming/.manifests/pending/*.json 2>/dev/null | head -n 20
  ```
- **권장 기본값:**
  ```
  INCOMING_SENSOR_INTERVAL_SEC=180
  AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC=180
  AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS=200
  AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK=20
  INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS=2
  ```
- **완료 조건:** pending backlog 안정 구간 회복, lock/conflict 재발 빈도 감소

---

## 4. MinIO

### endpoint 혼재 (production 9000/9001, test 9002/9003)
- **원인:** Console 포트와 S3 API 포트 혼동
- **조치:** production은 `MINIO_ENDPOINT=http://10.0.0.51:9000`, test는 `MINIO_ENDPOINT=http://10.0.0.51:9002`로 유지
- `9001 = production Console`, `9003 = test Console`

### Console 다운로드 실패 (test)
- **원인:** 객체 손상 아님. `Console(9003) -> API(9002)` 경로 또는 브라우저 세션 문제
- **진단 순서:**
  1. test endpoint(`9002`)에 있는지 확인
  2. `boto3.head_object()` / `get_object()`로 직접 확인
  3. presigned URL 또는 `download_file()`로 실제 다운로드 확인
- **운영 기준:** Console 다운로드 실패만으로 객체 재생성/삭제 금지

### 버킷 자동 생성 안됨
- **원인:** MinIO는 write 시 bucket auto-create 안함. `ensure_bucket()` helper가 upload/copy 경로에서 미호출
- **영구 조치:** `upload()`, `upload_fileobj()`, `copy()` 전에 `_ensure_bucket_once()` 호출

### raw_key에 `YYYY/MM` prefix 혼재
- **원인:** 과거 ingest 로직이 `datetime.now().strftime("%Y/%m")` prefix를 붙임
- **현재 정상 규칙:** `raw_key = <source_unit_name>/<rel_path>` (예: `source-b-event-bucket/20260204/fire_1_131000.mp4`)
- **복구 절차:**
  ```bash
  python3 scripts/reupload_minio_from_archive.py  # archive 기준 재정렬 업로드
  # DuckDB raw_files.raw_key도 동일 기준으로 갱신
  ```

### frame 경로 규칙
- **정상 경로:** `vlm-processed/<raw-prefix>/<video-stem>/<video-stem>_00000001.jpg`
- **금지:** `_tmp/...` prefix, `/frames/` 하위 폴더, 원본 stem 없는 파일명
- **경로 변경 후 필요한 정리:**
  1. 진행 중 extraction run cancel
  2. `vlm-processed` 버킷 정리
  3. `image_metadata`의 `video_frame` row 삭제
  4. `video_metadata.frame_extract_*` 초기화
  5. extraction 재실행

---

## 5. Ingest

> 아래 `PSQL` 은 다음의 축약입니다:
> `alias PSQL='docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c'`
> (스테이징은 `pipeline-test-postgres-1` / `vlm_pipeline_staging`)

### raw_files vs video_metadata 개수 불일치
- **확인:**
  ```bash
  PSQL "SELECT COUNT(*) FROM raw_files;"
  PSQL "SELECT COUNT(*) FROM video_metadata;"
  PSQL "SELECT COUNT(*) AS missing FROM raw_files rf
        LEFT JOIN video_metadata vm ON rf.asset_id = vm.asset_id
        WHERE rf.media_type='video' AND vm.asset_id IS NULL;"
  ```
  같은 판정을 `raw_ingest` 의 asset check `raw_ingest_video_metadata_consistency` 와
  `cross_table_consistency_sensor`(5분 주기)도 자동으로 수행한다.
- **조치:** ⚠️ `scripts/backfill_video_metadata.py` 는 **DuckDB 레거시 스크립트**로
  `ALLOW_LEGACY_DUCKDB_SCRIPT=1` 가드가 걸려 있어 현재 PG 스택에 바로 쓸 수 없다.
  누락분은 해당 asset 을 재materialize 하거나 `video_env_backfill_job` 계열 경로를 쓴다.
- **완료 조건:** `missing=0`

### failed 급증
- **확인:**
  ```bash
  PSQL "SELECT ingest_status, COUNT(*) FROM raw_files GROUP BY 1 ORDER BY 1;"
  PSQL "SELECT COALESCE(error_message,'(null)') AS msg, COUNT(*) AS cnt
        FROM raw_files WHERE ingest_status='failed' GROUP BY 1 ORDER BY cnt DESC LIMIT 30;"
  ls -lt /nas/data/incoming/.manifests/failed/*.jsonl 2>/dev/null | head
  ```
- **조치:** 파일 오류(`file_missing`, `empty_file`, `ffprobe_failed`)는 DB 미삽입 대상이므로 원본 파일 복구 후 재수집
- **완료 조건:** 동일 오류 재발 없음, 실패 로그만 남고 DB 오염 없음

### archive 이동 실패
- **확인:**
  ```bash
  PSQL "SELECT asset_id, source_path, archive_path, error_message
        FROM raw_files WHERE ingest_status='failed' AND error_message LIKE 'archive_move_failed%'
        ORDER BY updated_at DESC LIMIT 30;"
  find /nas/data/archive -type f -name '<파일명>' | head
  ```
- **`archive_move_timeout` 인 경우:** 이동 op 에는 600초 제한(`ARCHIVE_MOVE_TIMEOUT_SEC`)이 있다.
  타임아웃 시 run 을 실패시키지 않고 `complete_uploaded_assets_without_archive()` 로 넘어가
  **`ingest_status='completed'` + `archive_path=NULL`** 로 확정한다.
  즉 **failed 로 안 잡히고 조용히 archive 없는 completed 가 쌓인다.** 이것만 따로 확인:
  ```bash
  PSQL "SELECT COUNT(*) FROM raw_files WHERE ingest_status='completed' AND archive_path IS NULL;"
  ```
  같은 판정을 asset check `raw_ingest_archive_consistency` 가 WARN(non-blocking)으로 알려준다 —
  blocking 이 아니므로 run 은 초록색이어도 이 카운트는 올라갈 수 있다.
- **폴더 단위 이동이 갑자기 느려졌다면:** archive fast-path 는 `os.rename` 1회(~1s)로 끝나야 한다.
  incoming 과 archive 가 **다른 마운트**에 있으면 `EXDEV` 로 전체 복사가 되어 타임아웃이 폭증한다.
  `NAS_DATA_ROOT` 단일 부모 바인드(`/nas/data`)가 유지되고 있는지 확인할 것.
- **조치:**
  - archive 실존 시 → `completed + archive_path`로 복구
  - archive 미존재 시 → manifest 재발행으로 재처리
- **완료 조건:** archive 존재 건의 상태가 `completed`, orphan row 없음

---

## 6. GCS 다운로드

### 0바이트 파일
- **확인:**
  ```bash
  find /nas/data/incoming/gcp -type f \( -iname '*.mp4' -o -iname '*.mov' -o -iname '*.jpg' \) -size 0 | head -n 30
  ```
- **조치:**
  ```bash
  python3 gcp/download_from_gcs_rclone.py \
    --download --mode date-folders \
    --download-dir /nas/data/incoming/gcp \
    --buckets source-a-rtsp-bucket \
    --zero-byte-retries 4
  ```
- **완료 조건:** 0바이트 미디어 파일 0건, 대상 폴더 정상 `_DONE` 생성

### 인증/권한 확인
```bash
gcloud auth list
gsutil ls gs://source-a-rtsp-bucket/
# 필요 권한: storage.objects.list, storage.objects.get
```

---

## 7. Gemini / VertexAI

### vertexai import 실패
- **대표 에러:** `ModuleNotFoundError: No module named 'vertexai'`
- **원인:** `google-cloud-aiplatform` 미설치 또는 Docker dependency 누락
- **영구 조치:**
  - `docker/app/requirements.txt`, `pyproject.toml`에 `google-cloud-aiplatform` 추가
  - Docker 이미지 재빌드 후 `app`, `dagster` 재기동
- **검증:**
  ```bash
  docker exec pipeline-app-1 python3 -c "import vertexai"
  docker exec docker-dagster-1 python3 -c "from gemini.assets.config import VIDEO_PROMPT"
  ```

### Gemini credentials not found (test)
- **대표 에러:** `FileNotFoundError: Gemini credentials not found`
- **원인:** `docker/.env.test`에 credential 경로 누락
- **영구 조치:** `docker/.env.test`에 최소 아래 값 유지:
  ```
  GEMINI_PROJECT=<project>
  GEMINI_LOCATION=<location>
  GEMINI_GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/<service-account>.json
  ```

### 대용량 파일 payload 초과
- **대표 에러:** `400 Request payload size exceeds the limit: 524288000 bytes`
- **원인:** 원본 영상(>500MB)을 직접 request payload에 포함
- **영구 조치:** `>450MB` 영상은 Gemini 호출 전 preview mp4 먼저 생성 (오디오 제거, 해상도/fps/bitrate 축소)
- **비용 산정 시:** raw source 기준 vs current pipeline preview 기준 두 시나리오 분리

### ffmpeg temp 파일 overwrite 오류
- **대표 에러:** `ffmpeg_clip_extract_failed: File '/tmp/tmp....mp4' already exists. Overwrite? [y/N]`
- **원인:** clip 출력 temp 경로를 `NamedTemporaryFile(delete=False)`로 선생성 → ffmpeg가 overwrite 여부 묻고 비대화식 `N` 처리
- **영구 조치:** ffmpeg 출력에 "아직 존재하지 않는 temp 경로 문자열"만 전달. partial temp file 실패 시 cleanup

---

## 8. YOLO (⚠️ 현재 비활성 — bbox 는 SAM3 담당)

> `ENABLE_YOLO_DETECTION=false` 이고 `docker-yolo-1` 컨테이너도 정지 상태입니다.
> bbox 장애를 쫓고 있다면 여기가 아니라 **SAM3** 를 보세요:
>
> ```bash
> curl -fsS http://127.0.0.1:8002/health              # model_loaded, device, gpu_memory
> curl -fsS http://127.0.0.1:8002/maintenance/status  # 정비 모드로 막혀 있는지
> docker logs --tail 100 docker-sam3-1
> ```
>
> - SAM3 결과는 `vlm-labels/<source>/sam3_segmentations/*.json` + `image_labels`(`label_tool='sam3'`)
> - SAM3 는 prod·staging 이 **같은 컨테이너를 공유**하므로 재기동 시 양쪽 영향
> - `SAM3_WORKERS` 를 올리면 GPU1 OOM(503) 위험 — 2026-05-27 인시던트 이력 확인 후 조정
> - ⚠️ 정비 플래그가 uvicorn worker 별 메모리라 `/maintenance/enter` 가 3개 worker 전부를
>   막지 못한다 (drain 미완). 학습 전 drain 을 신뢰하기 전에 확인 필요
>
> 아래 YOLO 내용은 플래그를 다시 켤 때를 위한 참고용입니다.

### 모델/dependency 문제
- **사용 모델:** `yolov8l-worldv2.pt` (`docker/data/models/yolo/yolov8l-worldv2.pt`)
- **필수 env:**
  ```
  YOLO_MODEL_PATH=/data/models/yolo/yolov8l-worldv2.pt
  YOLO_DEFAULT_CLASSES=...
  ```
- **CLIP dependency 누락:** YOLO-World 이미지에 `git+https://github.com/ultralytics/CLIP.git` 필요
- **확인:**
  ```bash
  # pipeline-yolo-1이 healthy
  # /health에서 model_loaded=true
  # /info에서 모델 경로 기대값 일치
  ```

### YOLO 실행 순서 문제
- **원인:** frame 생성 전 YOLO asset이 먼저 뜸
- **영구 조치:** legacy test 전용 YOLO asset을 분리해 `raw_ingest -> clip/frame 생성 -> yolo_image_detection` 순서 보장

---

## 9. 데이터 정합성

### ~~Local vs MotherDuck 불일치~~ — ❌ MotherDuck 동기화는 제거됨

MotherDuck 동기화는 코드에서 완전히 사라졌습니다 (`grep motherduck src/` → 0 hits).
`motherduck_sync_job` / `motherduck_*_sensor` / `motherduck_daily_schedule` / `defs/sync/` 전부 없고,
스크립트는 `scripts/archive/local_duckdb_to_motherduck_sync.py` 에만 남아 있습니다.
`.env` 의 `MOTHERDUCK_*` 변수도 잔재입니다.

현재 DB 정합성 확인은 Postgres 에서 직접:

```bash
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  SELECT ingest_status, COUNT(*) FROM raw_files GROUP BY 1 ORDER BY 2 DESC;"
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  SELECT COUNT(*) FROM raw_files r
  LEFT JOIN video_metadata v USING (asset_id)
  WHERE r.media_type='video' AND r.ingest_status='completed' AND v.asset_id IS NULL;"
```

> ⚠️ 스냅샷용 카운트는 `pg_stat_user_tables.n_live_tup` 를 믿지 말고 `COUNT(*)` 를 쓰세요
> (autovacuum 통계가 크게 뒤처져 0 으로 보이는 사례 있음).
> 상시 정합성 감시는 `cross_table_consistency_sensor` (5분) + `raw_ingest`/`clip_timestamp` 의
> asset check 3종이 담당합니다.

### archive / MinIO / DB 개수 불일치
- **정렬 방법:**
  1. archive에서 DB에 없는 파일 전수 확인
  2. 초과 파일을 세 종류로 분리: 운영 marker(`_DONE`) / 잡파일(`.DS_Store`) / 실제 데이터
  3. 규칙: `_DONE`→유지, `.DS_Store`→삭제, 실제 데이터→checksum으로 duplicate 판단
- **운영 기준:** "정합"은 archive 전체 물리 파일 수가 아니라 **archive 데이터 파일 수** 기준

---

## 10. Test 초기화

staging 재테스트를 위한 완전 초기화 순서 (스테이징 clone 에서 실행):

```bash
# 1. staging 컨테이너 중지
docker stop pipeline-test-dagster-1 pipeline-test-dagster-daemon-1 pipeline-test-dagster-code-server-1

# 2. staging MinIO 객체 전부 삭제 (endpoint: 10.0.0.51:9002 / 콘솔 :9003)
#    버킷 5개: vlm-raw, vlm-labels, vlm-processed, vlm-dataset, vlm-classification

# 3. staging DB 초기화 — Postgres `vlm_pipeline_staging` @ pipeline-test-postgres-1
#    (구 docker/data/staging.duckdb 파일은 write path 가 아니라 무관한 잔재)

# 4. staging Dagster runtime 상태 삭제 (run·sensor·schedule 토글 초기화)
rm -rf docker/app/dagster_home/storage

# 5. 재기동
./scripts/compose-staging.sh up -d
```

> ⚠️ **4번을 하면 센서 ON/OFF 토글도 초기화된다.** `dispatch_sensor` 와
> `production_agent_dispatch_sensor` 는 **코드 기본값이 STOPPED** 이므로,
> 재기동 후 Dagster UI 에서 다시 켜지 않으면 자동 라벨링이 조용히 멈춘 상태가 된다.

**절대 지우면 안 되는 것:**
- `/home/user/mou/nas_primary/staging/incoming`
- `/home/user/mou/nas_primary/staging/archive`

**staging 데이터 plane 마운트:** prod 와 동일하게 **단일 부모 바인드**를 쓴다 —
`NAS_DATA_ROOT=/home/user/mou/nas_primary/staging` → `/nas/data`
(incoming/archive 를 따로 마운트하면 archive 폴더 이동이 `EXDEV` 로 전체 복사가 된다).

---

## 11. Label Studio

### 최초 기동 순서
```bash
docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d
# LS 접속 후 admin 계정 생성 → Account Settings에서 API key 발급
# .env에 LS_API_KEY, WEBHOOK_HOST 설정 후 ls-webhook 재기동
```

### webhook 등록 (프로젝트 생성 후 1회)
```bash
python src/gemini/ls_webhook.py register --project <project_id>
python src/gemini/ls_webhook.py list  # 등록 확인
```

### presigned URL 만료 (기본 7일)
```bash
python src/gemini/ls_tasks.py renew --project-name <project_name>
```

### ls_task_create_sensor 미동작
- ⚠️ 이 센서는 **코드 기본값이 RUNNING** 이다 (`default_status=DefaultSensorStatus.RUNNING`).
  "수동 ON 필요" 는 옛 정보 — 꺼져 있다면 누가 UI 에서 끈 것이거나 storage 초기화 후 상태다
- `LS_API_KEY` 미설정 시 job 실패 → `.env` 확인
- 대상 조건: `dispatch_requests` 에 `status='completed' AND ls_task_status='pending'` 행이 있어야 tick 이 일함
- job 이 SUCCESS 인데도 태스크가 안 생기는 경우가 있다 — 서브프로세스 실패가 삼켜질 수 있으므로
  run 로그에서 `ls_tasks.py create` 출력을 직접 확인할 것

### LS → MinIO 접근 불가 (presigned URL 오류)
- presigned URL은 `MINIO_ENDPOINT` 기준 생성 → LS 컨테이너에서 해당 주소 도달 가능한지 확인
- `docker exec pipeline-labelstudio-1 curl -I <presigned_url>`

---

## 11. NAS (CIFS) 장애 대응

### 증상: 파이프라인 hang (archive_finalize 멈춤, 파일 접근 타임아웃)

**진단:**
```bash
# NAS 네트워크 연결 확인
timeout 3 ping -c 3 10.0.0.51

# NAS 파일 접근 테스트 (5초 타임아웃)
timeout 5 stat /home/user/mou/nas_primary/incoming/
timeout 5 ls /home/user/mou/nas_primary/archive/

# CIFS 연결 통계 (reconnect 횟수, open files, 에러 확인)
cat /proc/fs/cifs/Stats

# 커널 CIFS 에러 로그
sudo dmesg | grep -i cifs | tail -20
```

**주요 이상 징후:**
- `open on server` 값이 음수 → CIFS 세션 상태 corruption
- reconnect 횟수가 빠르게 증가 → SMB 서비스 불안정
- `stat`/`ls` 명령이 타임아웃 → NAS I/O hang

**즉시 조치: CIFS 재마운트**
```bash
sudo umount -l /home/user/mou/incoming
sudo umount -l /home/user/mou/archive
sudo umount -l /home/user/mou/nas_primary/staging
sudo mount -a
# 검증
timeout 5 ls /home/user/mou/incoming/
```

**파이프라인 보호 메커니즘:**
- `raw_ingest` 시작 시 NAS 헬스체크 (5초 타임아웃, 실패 시 skip)
- `archive_finalize`의 `shutil.move`에 타임아웃 적용 (`ARCHIVE_MOVE_TIMEOUT_SEC`, 기본 300초)
- 타임아웃 시 archive 건너뛰고 업로드 완료분은 `completed` 처리 → 후속 스텝 진행

### CIFS 마운트 옵션 권장 설정

현재 `/etc/fstab` 기본 옵션에 아래를 추가하면 NAS 장애 감지와 복원이 빨라진다:

```
# 추가 권장 옵션
soft,echo_interval=30,actimeo=1,closetimeo=1
```

| 옵션 | 현재 | 권장 | 효과 |
|------|------|------|------|
| `soft` | 미설정 | 추가 | 응답 없을 때 에러 반환 (hard mount는 무한 대기) |
| `echo_interval` | 60 | 30 | 연결 끊김 감지 주기 절반으로 단축 |
| `actimeo` | 미설정 | 1 | 파일 속성 캐시 1초 (이미 적용 중) |

**변경 방법 (sudo 필요):**
```bash
# /etc/fstab 편집 후
sudo mount -o remount /home/user/mou/incoming
sudo mount -o remount /home/user/mou/archive
```

### 관련 환경변수

| 변수 | 기본값 | 용도 |
|------|--------|------|
| `ARCHIVE_MOVE_TIMEOUT_SEC` | 300 | archive shutil.move 타임아웃 (초) |
| `INGEST_META_WORKERS` | 4 | 체크섬/ffprobe 병렬 워커 수 (최대 8) |
| `INGEST_UPLOAD_WORKERS` | 4 | MinIO 업로드 병렬 워커 수 (최대 16) |
