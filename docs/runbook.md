# 운영 트러블슈팅 런북

문제 유형별 진단 및 즉시 조치 가이드.

**공통 도구:**
```bash
# DuckDB 읽기 (락 회피)
python3 scripts/query_local_duckdb.py --sql "<SQL>"

# 호스트 DB 경로: ./docker/data/pipeline.duckdb
# 컨테이너 DB 경로: /data/pipeline.duckdb
# Manifest 경로: /nas/incoming/.manifests/pending | failed
```

---

## 1. Dagster 서버/프로세스

### Dagster UI 접속 불가 (포트 충돌, LOCATION_ERROR)
```bash
docker logs pipeline-dagster-1 | tail -n 100
docker compose restart dagster
ss -tln | grep 3030
```

### staging daemon heartbeat 충돌
- **원인:** staging이 production과 동일한 Dagster runtime storage 공유
- **영구 조치:**
  - `dagster-staging`은 `DAGSTER_HOME=/app/dagster_home_staging` 사용
  - `dagster.yaml`에서 `run_storage`, `event_log_storage`, `schedule_storage`, `local_artifact_storage` 모두 `/data/dagster_home_staging/storage`로 분리
- **확인:** 재기동 후 heartbeat 충돌 로그 없음, sensor tick 정상 순환

### STARTED run이 장시간 점유 (backpressure 문제)
- **원인:** `duckdb_writer=true`(legacy dispatch) 또는 `duckdb_raw_writer` / `duckdb_label_writer` / `duckdb_yolo_writer` 슬롯 경쟁. worker 프로세스가 없는데 UI에 `STARTED` 잔류
- **즉시 조치:**
  ```bash
  # Dagster instance API로 CANCELING → CANCELED 처리
  # 이후 sensor가 신규 run 발행 가능 상태로 복구
  ```
- **영구 조치:**
  ```
  STUCK_RUN_GUARD_ENABLED=true
  STUCK_RUN_GUARD_INTERVAL_SEC=120
  STUCK_RUN_GUARD_TIMEOUT_SEC=10800
  STUCK_RUN_GUARD_AUTO_REQUEUE_ENABLED=true
  STUCK_RUN_GUARD_TARGET_JOBS=mvp_stage_job,ingest_job,motherduck_sync_job
  ```

### git switch 차단 (staging runtime 파일 충돌)
- **원인:** `runs.db`, `schedules.db`, `.nux/`, `.telemetry/` 같은 runtime 파일이 git working tree 내 생성
- **영구 조치:** staging Dagster storage를 `/data/dagster_home_staging/storage`로 유지. 브랜치 전환 전 staging 컨테이너 중지

---

## 2. DuckDB

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
  ls -1 /nas/incoming/.manifests/pending/retry_*.json 2>/dev/null | tail -n 20
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
- **조치:**
  ```bash
  python3 scripts/repair_image_metadata_schema.py
  # 이후 운영 Dagster 재기동, stale run 정리, 재실행
  ```

### staging DuckDB not found
- **대표 에러:** `DuckDB not found: /data/staging.duckdb`
- **복구 방법:**
  - 운영 상태 재현: `pipeline.duckdb → staging.duckdb` 복제
  - 빈 staging 재테스트: `staging.duckdb` 삭제 후 스키마만 적용한 새 DB 생성

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
  find /nas/incoming/.manifests/pending -maxdepth 1 -name '*.json' | wc -l
  ls -lt /nas/incoming/.manifests/pending/*.json 2>/dev/null | head -n 20
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

### endpoint 혼재 (9000 vs 9001)
- **원인:** 콘솔 포트(9001)와 S3 API 포트(9000) 혼동
- **조치:** `MINIO_ENDPOINT=http://172.168.47.36:9000`으로 통일 (docker-compose.yaml, .env.example, docker/.env)
- `9002 = staging API`, `9003 = staging Console`

### Console 다운로드 실패 (staging)
- **원인:** 객체 손상 아님. `Console(9003) → API(9002)` 경로 또는 브라우저 세션 문제
- **진단 순서:**
  1. staging endpoint(`9002`)에 있는지 확인
  2. `boto3.head_object()` / `get_object()`로 직접 확인
  3. presigned URL 또는 `download_file()`로 실제 다운로드 확인
- **운영 기준:** Console 다운로드 실패만으로 객체 재생성/삭제 금지

### 버킷 자동 생성 안됨
- **원인:** MinIO는 write 시 bucket auto-create 안함. `ensure_bucket()` helper가 upload/copy 경로에서 미호출
- **영구 조치:** `upload()`, `upload_fileobj()`, `copy()` 전에 `_ensure_bucket_once()` 호출

### raw_key에 `YYYY/MM` prefix 혼재
- **원인:** 과거 ingest 로직이 `datetime.now().strftime("%Y/%m")` prefix를 붙임
- **현재 정상 규칙:** `raw_key = <source_unit_name>/<rel_path>` (예: `adlibhotel-event-bucket/20260204/fire_1_131000.mp4`)
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

### raw_files vs video_metadata 개수 불일치
- **확인:**
  ```bash
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM video_metadata;"
  python3 scripts/query_local_duckdb.py --sql "
    SELECT COUNT(*) AS missing FROM raw_files rf
    LEFT JOIN video_metadata vm ON rf.asset_id = vm.asset_id
    WHERE rf.media_type='video' AND vm.asset_id IS NULL;"
  ```
- **조치:**
  ```bash
  # 호스트
  python3 scripts/backfill_video_metadata.py --db ./docker/data/pipeline.duckdb --statuses completed --log-every 20
  # 컨테이너
  python3 /src/vlm/scripts/backfill_video_metadata.py --db /data/pipeline.duckdb --statuses completed --log-every 20
  ```
- **완료 조건:** `missing_video_meta=0`

### failed 급증
- **확인:**
  ```bash
  python3 scripts/query_local_duckdb.py --sql "
    SELECT ingest_status, COUNT(*) FROM raw_files GROUP BY 1 ORDER BY 1;"
  python3 scripts/query_local_duckdb.py --sql "
    SELECT COALESCE(error_message,'(null)') AS msg, COUNT(*) AS cnt
    FROM raw_files WHERE ingest_status='failed' GROUP BY 1 ORDER BY cnt DESC LIMIT 30;"
  ls -lt /nas/incoming/.manifests/failed/*.jsonl 2>/dev/null | head
  ```
- **조치:** 파일 오류(`file_missing`, `empty_file`, `ffprobe_failed`)는 DB 미삽입 대상이므로 원본 파일 복구 후 재수집
- **완료 조건:** 동일 오류 재발 없음, 실패 로그만 남고 DB 오염 없음

### archive 이동 실패
- **확인:**
  ```bash
  python3 scripts/query_local_duckdb.py --sql "
    SELECT asset_id, source_path, archive_path, error_message
    FROM raw_files WHERE ingest_status='failed' AND error_message LIKE 'archive_move_failed%'
    ORDER BY updated_at DESC LIMIT 30;"
  find /nas/archive -type f -name '<파일명>' | head
  ```
- **조치:**
  - archive 실존 시 → `completed + archive_path`로 복구
  - archive 미존재 시 → manifest 재발행으로 재처리
- **완료 조건:** archive 존재 건의 상태가 `completed`, orphan row 없음

---

## 6. GCS 다운로드

### 0바이트 파일
- **확인:**
  ```bash
  find /nas/incoming/gcp -type f \( -iname '*.mp4' -o -iname '*.mov' -o -iname '*.jpg' \) -size 0 | head -n 30
  ```
- **조치:**
  ```bash
  python3 gcp/download_from_gcs_rclone.py \
    --download --mode date-folders \
    --download-dir /nas/incoming/gcp \
    --buckets khon-kaen-rtsp-bucket \
    --zero-byte-retries 4
  ```
- **완료 조건:** 0바이트 미디어 파일 0건, 대상 폴더 정상 `_DONE` 생성

### 인증/권한 확인
```bash
gcloud auth list
gsutil ls gs://khon-kaen-rtsp-bucket/
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
  docker exec pipeline-dagster-1 python3 -c "from gemini.assets.config import VIDEO_PROMPT"
  ```

### Gemini credentials not found (staging)
- **대표 에러:** `FileNotFoundError: Gemini credentials not found`
- **원인:** `.env.staging`에 credential 경로 누락
- **영구 조치:** `.env.staging`에 최소 아래 값 유지:
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

## 8. YOLO

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
- **영구 조치:** staging 전용 YOLO asset을 분리해 `raw_ingest → clip/frame 생성 → yolo_image_detection` 순서 보장

---

## 9. 데이터 정합성

### Local vs MotherDuck 불일치
- **확인:**
  ```bash
  # 로컬
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM video_metadata;"
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files WHERE ingest_status='failed';"
  # MotherDuck: motherduck_sync_job 실행 후 동일 쿼리 재확인
  ```
- **조치:** sync 전에 로컬 이상치 먼저 정리 → sync 실행 → 동일 쿼리로 local/cloud 동시 검증
- **MotherDuck 수동 동기화:**
  ```bash
  python3 /src/python/local_duckdb_to_motherduck_sync.py \
    --db pipeline_db --local-db-path /data/pipeline.duckdb \
    --share-update MANUAL --tables raw_files video_metadata
  ```

### archive / MinIO / DuckDB / MotherDuck 개수 불일치
- **정렬 방법:**
  1. archive에서 DB에 없는 파일 전수 확인
  2. 초과 파일을 세 종류로 분리: 운영 marker(`_DONE`) / 잡파일(`.DS_Store`) / 실제 데이터
  3. 규칙: `_DONE`→유지, `.DS_Store`→삭제, 실제 데이터→checksum으로 duplicate 판단
- **운영 기준:** "정합"은 archive 전체 물리 파일 수가 아니라 **archive 데이터 파일 수** 기준

### MotherDuck 수동 동기화
```bash
python3 /src/python/local_duckdb_to_motherduck_sync.py \
  --db pipeline_db --local-db-path /data/pipeline.duckdb \
  --share-update MANUAL --tables raw_files video_metadata
```

---

## 10. Staging 초기화

staging 재테스트를 위한 완전 초기화 순서:
```bash
# 1. dagster-staging 중지
# 2. staging MinIO 객체 전부 삭제 (endpoint: 172.168.47.36:9002)
#    - vlm-raw, vlm-labels, vlm-processed, vlm-dataset
# 3. staging DuckDB 삭제
rm docker/data/staging.duckdb
# 4. staging Dagster runtime DB 삭제
rm -rf docker/data/dagster_home_staging/storage
# 5. 재기동
```

**절대 지우면 안 되는 것:**
- `/home/pia/mou/staging/incoming`
- `/home/pia/mou/staging/archive`

**staging 컨테이너 볼륨 마운트 필수:**
```yaml
- /home/pia/mou/staging/incoming:/nas/staging/incoming
- /home/pia/mou/staging/archive:/nas/staging/archive
```

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
- Dagster UI → Sensors → `ls_task_create_sensor` 수동 ON (default=STOPPED)
- `LS_API_KEY` 미설정 시 job 실패 → `.env` 확인

### LS → MinIO 접근 불가 (presigned URL 오류)
- presigned URL은 `MINIO_ENDPOINT` 기준 생성 → LS 컨테이너에서 해당 주소 도달 가능한지 확인
- `docker exec pipeline-labelstudio-1 curl -I <presigned_url>`

---

## 11. NAS (CIFS) 장애 대응

### 증상: 파이프라인 hang (archive_finalize 멈춤, 파일 접근 타임아웃)

**진단:**
```bash
# NAS 네트워크 연결 확인
timeout 3 ping -c 3 172.168.47.36

# NAS 파일 접근 테스트 (5초 타임아웃)
timeout 5 stat /home/pia/mou/incoming/
timeout 5 ls /home/pia/mou/archive/

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
sudo umount -l /home/pia/mou/incoming
sudo umount -l /home/pia/mou/archive
sudo umount -l /home/pia/mou/staging
sudo mount -a
# 검증
timeout 5 ls /home/pia/mou/incoming/
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
sudo mount -o remount /home/pia/mou/incoming
sudo mount -o remount /home/pia/mou/archive
```

### 관련 환경변수

| 변수 | 기본값 | 용도 |
|------|--------|------|
| `ARCHIVE_MOVE_TIMEOUT_SEC` | 300 | archive shutil.move 타임아웃 (초) |
| `INGEST_META_WORKERS` | 4 | 체크섬/ffprobe 병렬 워커 수 (최대 8) |
| `INGEST_UPLOAD_WORKERS` | 4 | MinIO 업로드 병렬 워커 수 (최대 16) |
