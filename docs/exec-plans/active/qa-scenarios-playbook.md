# 🧪 QA Scenarios Playbook

> **목적**: appdata + test/test2/test3 폴더로 QA 진행 시 사용. 문서 단독으로 컨텍스트 없이 실행 가능.
> **시작일**: 2026-05-20
> **소속**: prod (10.0.0.10:3030) — VLM Data Pipeline
> **상태**: Phase 1+2+3+4 prod 적용 완료, NVENC 활성, B6 race fix 적용.

---

## 📌 0. 변경 이력

| 날짜 | 변경 | 담당 |
|---|---|---|
| 2026-05-20 | 초안 — 시나리오 1, 2 | claude |
| | | |

> 시나리오 추가/변경 시 위 표 + 본문 "## 시나리오 N" 섹션 + "## 측정 결과" 표에 row 추가.

---

## 📌 1. 환경 / 전제

### 1.1 인프라

| | prod | staging |
|---|---|---|
| Dagster UI | `http://10.0.0.10:3030` | `http://10.0.0.10:3031` |
| incoming | `/home/user/mou/nas_200tb/incoming/` | `/home/user/mou/nas_200tb/staging/incoming/` |
| archive | `/home/user/mou/nas_200tb/archive/` | `/home/user/mou/nas_200tb/staging/archive/` |
| MinIO | `http://10.0.0.51:9000` | `http://10.0.0.51:9002` |
| PG container | `docker-postgres-1` | `pipeline-test-postgres-1` |
| PG database | `vlm_pipeline` | `vlm_pipeline_staging` |
| dagster daemon | `docker-dagster-daemon-1` | `pipeline-test-dagster-daemon-1` |
| dagster webserver | `docker-dagster-1` | `pipeline-test-dagster-1` |

### 1.2 활성 설정 (적용된 Phase 1-4)

`docker/.env` (prod) 의 키:
```bash
INGEST_UPLOAD_WORKERS=16         # Phase 1 B2
INGEST_REENCODE_WORKERS=6        # Phase 1 B1
INGEST_REENCODE_THREADS=4        # Phase 1 B1
INGEST_ARCHIVE_WORKERS=8         # Phase 1 B4
DAGSTER_GRPC_HEARTBEAT_TIMEOUT=60 # Phase 1 B3
MINIO_MULTIPART_CHUNK_MB=16      # Phase 2 B2
MINIO_UPLOAD_MAX_CONCURRENCY=16  # Phase 2 B2
REENCODE_USE_NVENC=true          # Phase 2 B1 — GPU 인코딩 활성
YOLO_SENSOR_LIMIT=200            # Phase 3 — sensor backlog cap
ENABLE_YOLO_DETECTION=false      # 정책: SAM3.1 단독 운영
ENABLE_SAM3_DETECTION=true
```

`docker/app/dagster_home/dagster.yaml` 에 `code_servers.local_startup_timeout: 120 / reload_timeout: 120` 추가됨.

### 1.3 소스 데이터 (원본 — **삭제 금지**)

```
/home/user/mou/nas_200tb/
├── test           (20 mp4, ~857MB)   ← 소형 검증용 #1
├── test2          (20 mp4, ~718MB)   ← 소형 검증용 #2
├── test3          (20 mp4, ~796MB)   ← 소형 검증용 #3
└── appdata        (2500 mp4, ~96GB)  ← 대형 스케일 검증용
```

⚠️ **금지**: 위 4개 폴더 + 그 안의 파일은 절대 삭제 금지. 모든 cleanup 은 **derived** 데이터 (incoming/<folder>/, archive/<folder>/, MinIO, PG) 만 대상.

---

## 📌 2. 사전 점검 (시나리오 시작 전 1회)

```bash
# A. 원본 폴더 존재 확인 (4개 모두)
for d in test test2 test3 appdata; do
  echo "--- $d ---"
  ls /home/user/mou/nas_200tb/$d | head -2
  echo "count: $(ls /home/user/mou/nas_200tb/$d | wc -l)"
done
# 기대: test=20, test2=20, test3=20, appdata=2500

# B. prod Dagster ready
curl -sf http://10.0.0.10:3030/server_info | head -1
# 기대: {"dagster_webserver_version":"1.13.4",...}

# C. NVENC + Phase 활성 확인
docker exec docker-dagster-1 python3 -c "
from vlm_pipeline.lib import video_reencode
a = video_reencode._resolve_ffmpeg_preset_args()
print(f'encoder={a[a.index(chr(45)+chr(99)+chr(58)+chr(118))+1]}', 'no-scenecut=' in str(a), 'nvenc' in a[a.index(chr(45)+chr(99)+chr(58)+chr(118))+1])
"
# 기대: encoder=h264_nvenc, no-scenecut 포함, NVENC True

# D. heartbeat 0 (지난 5분)
docker logs docker-dagster-daemon-1 --since 5m 2>&1 | grep -c "No heartbeat"
# 기대: 0 (또는 init 직후 1)

# E. PG 깨끗 (test/appdata 잔여 0)
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -tA -c "
SELECT 'leftover=' || COUNT(*) FROM raw_files
WHERE source_unit_name IN ('test','test2','test3') OR source_unit_name ILIKE '%appdata%';"
# 기대: leftover=0

# F. archive 잔여 확인 — archive/<folder> 가 비어있어야 fast-path 활성화됨.
#    잔여물 있으면 existing_archive != None 으로 fast-path 비활성 → per-file 7+ 분 소요 (2026-05-20 발견).
for f in test test2 test3 appdata_all_scenario1 appdata_part1_scenario2 appdata_part2_scenario2; do
  n=$(ls /home/user/mou/nas_200tb/archive/$f 2>/dev/null | wc -l)
  [ "$n" -eq 0 ] && echo "✅ archive/$f=0" || echo "🚨 archive/$f=$n (cleanup_stage 필요)"
done
# 기대: 모두 ✅. 🚨 가 있으면:
#   docker run --rm -v /home/user/mou/nas_200tb:/nas alpine rm -rf /nas/archive/<folder>
```

위 5가지가 모두 정상이어야 시나리오 시작.

---

## 📌 3. 공통 헬퍼 (모든 시나리오에서 사용)

### 3.1 dispatch JSON 템플릿

```bash
# 변수만 바꿔서 사용
make_dispatch_json() {
  local request_id="$1"
  local folder_name="$2"
  local labeling_methods="$3"   # 예: '["bbox"]' 또는 '["bbox","timestamp_video","captioning_video"]'
  local categories='["fire","smoke","weapon","violence","falldown"]'
  local now=$(date -Iseconds)
  cat <<EOF
{
  "request_id": "${request_id}",
  "folder_name": "${folder_name}",
  "labeling_method": ${labeling_methods},
  "categories": ${categories},
  "requested_by": "qa_playbook",
  "requested_at": "${now}",
  "image_profile": "current",
  "max_frames_per_video": 16,
  "confidence_threshold": 0.25
}
EOF
}
```

### 3.2 incoming 에 폴더 배치

**소형 (test/test2/test3) — `cp -r` 실복사**:
```bash
copy_to_incoming() {
  local src_folder="$1"   # 예: test
  docker run --rm -v /home/user/mou/nas_200tb:/nas alpine \
    cp -r "/nas/${src_folder}" "/nas/incoming/${src_folder}"
}
```

**대형 (appdata) — `ln` hardlink (0 byte 추가)**:
```bash
# 전체 2500
hardlink_appdata_all() {
  local dest_folder="$1"   # 예: appdata_all_scenario1
  docker run --rm -v /home/user/mou/nas_200tb:/nas alpine sh -c "
    mkdir -p /nas/incoming/${dest_folder}
    cd /nas/appdata
    for f in appdata_safety*.mp4; do ln \"\$f\" \"/nas/incoming/${dest_folder}/\$f\"; done
    echo \"count: \$(ls /nas/incoming/${dest_folder}/ | wc -l)\"
  "
}

# 분할 (0001-1250 → part1, 1251-2500 → part2)
hardlink_appdata_split() {
  local part1="$1"   # 예: appdata_part1_scenario2
  local part2="$2"   # 예: appdata_part2_scenario2
  docker run --rm -v /home/user/mou/nas_200tb:/nas alpine sh -c "
    set -e
    mkdir -p /nas/incoming/${part1} /nas/incoming/${part2}
    cd /nas/appdata
    for f in appdata_safety0[0-9][0-9][0-9].mp4 appdata_safety1[0-2][0-9][0-9].mp4; do
      [ \"\$f\" \\> \"appdata_safety1250.mp4\" ] && break
      ln \"\$f\" \"/nas/incoming/${part1}/\$f\"
    done
    for f in appdata_safety1[2-9][0-9][0-9].mp4 appdata_safety2[0-9][0-9][0-9].mp4; do
      [ \"\$f\" \\< \"appdata_safety1251.mp4\" ] && continue
      ln \"\$f\" \"/nas/incoming/${part2}/\$f\"
    done
    echo \"${part1}=\$(ls /nas/incoming/${part1}/ | wc -l) ${part2}=\$(ls /nas/incoming/${part2}/ | wc -l)\"
  "
}
```

### 3.3 dispatch JSON 드롭

```bash
drop_dispatch() {
  local json_path="$1"    # 호스트의 JSON 파일 경로
  local request_id="$2"   # JSON 의 request_id (파일명용)
  docker run --rm \
    -v /home/user/mou/nas_200tb:/nas \
    -v "${json_path}:/tmp/in.json:ro" \
    alpine sh -c "
      cp /tmp/in.json /nas/incoming/.dispatch/pending/${request_id}.json
      ls -la /nas/incoming/.dispatch/pending/
    "
}
```

### 3.4 진행 상태 확인

```bash
# folder 의 raw_files 상태 분포
pg_progress() {
  local folder="$1"
  docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
    SELECT source_unit_name, ingest_status, COUNT(*)
    FROM raw_files WHERE source_unit_name='${folder}'
    GROUP BY 1,2 ORDER BY 1,2;
  "
}

# folder 의 라벨 산출물
pg_artifacts() {
  local folder="$1"
  docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
    SELECT
      (SELECT COUNT(*) FROM image_metadata im JOIN raw_files r ON r.asset_id=im.source_asset_id WHERE r.source_unit_name='${folder}') AS frames,
      (SELECT COUNT(*) FROM image_labels il JOIN image_metadata im ON im.image_id=il.image_id JOIN raw_files r ON r.asset_id=im.source_asset_id WHERE r.source_unit_name='${folder}') AS image_labels,
      (SELECT COUNT(*) FROM labels l JOIN raw_files r ON r.asset_id=l.asset_id WHERE r.source_unit_name='${folder}') AS gemini_events;
  "
}

# dispatch 최종 status
dispatch_status() {
  local request_id="$1"
  docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
    SELECT request_id, status, error_message
    FROM dispatch_requests WHERE request_id='${request_id}';
  "
}

# 실시간 milestone 로그 follow (Ctrl-C 로 종료)
tail_milestones() {
  docker logs -f docker-dagster-daemon-1 2>&1 | grep --line-buffered -E \
    "raw_ingest entered|MinIO 업로드 완료|archive_finalize:|RAW VIDEO EXTRACT 완료|SAM3 완료|RUN_SUCCESS|RUN_FAILURE|reencode_encoder=|empty_output|race"
}

# heartbeat 카운트 (지난 5분, 정상=0)
heartbeat_check() {
  docker logs docker-dagster-daemon-1 --since 5m 2>&1 | grep -c "No heartbeat"
}

# GPU 상태
gpu_check() {
  nvidia-smi --query-gpu=index,memory.used,memory.free,utilization.gpu --format=csv,noheader
}

# DB 테이블별 상위 20행 샘플 — 각 stage 데이터가 실제로 어떻게 들어갔는지 확인용.
# raw_files / video_metadata / image_metadata / labels / image_labels / processed_clips / dispatch_*
# 까지 8개 테이블을 folder 별로 join 해서 20행 출력. cleanup 전에 산출물 검증 시 호출.
pg_sample() {
  local folder="$1"   # 예: 'test' (정확 매치) 또는 'appdata%' (LIKE wildcard)
  docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
\\echo '=== raw_files (top 20) ==='
SELECT asset_id, source_unit_name, media_type, ingest_status,
       raw_key, archive_path, file_size,
       to_char(created_at,'MM-DD HH24:MI:SS') AS created
FROM raw_files
WHERE source_unit_name LIKE '${folder}'
ORDER BY created_at DESC LIMIT 20;

\\echo '=== video_metadata (top 20) ==='
SELECT vm.asset_id, vm.duration_sec, vm.width, vm.height, vm.fps,
       vm.auto_label_status, vm.timestamp_status, vm.caption_status, vm.bbox_status,
       vm.timestamp_label_key
FROM video_metadata vm JOIN raw_files r ON r.asset_id=vm.asset_id
WHERE r.source_unit_name LIKE '${folder}'
ORDER BY vm.extracted_at DESC NULLS LAST LIMIT 20;

\\echo '=== image_metadata (top 20) ==='
SELECT im.image_id, im.source_asset_id, im.image_key, im.frame_index, im.timestamp_sec
FROM image_metadata im JOIN raw_files r ON r.asset_id=im.source_asset_id
WHERE r.source_unit_name LIKE '${folder}'
ORDER BY im.created_at DESC LIMIT 20;

\\echo '=== labels (Gemini events, top 20) ==='
SELECT l.label_id, l.asset_id, l.event_index, l.event_count,
       l.timestamp_start_sec, l.timestamp_end_sec,
       LEFT(l.caption_text, 60) AS caption_preview,
       l.label_tool, l.label_status
FROM labels l JOIN raw_files r ON r.asset_id=l.asset_id
WHERE r.source_unit_name LIKE '${folder}'
ORDER BY l.created_at DESC LIMIT 20;

\\echo '=== image_labels (SAM3/YOLO bbox, top 20) ==='
SELECT il.image_label_id, il.image_id, il.object_count, il.label_format,
       il.label_tool, il.label_status, il.review_status,
       il.labels_bucket, LEFT(il.labels_key, 80) AS labels_key
FROM image_labels il
JOIN image_metadata im ON im.image_id=il.image_id
JOIN raw_files r ON r.asset_id=im.source_asset_id
WHERE r.source_unit_name LIKE '${folder}'
ORDER BY il.created_at DESC LIMIT 20;

\\echo '=== processed_clips (top 20) ==='
SELECT pc.clip_id, pc.source_asset_id, pc.source_label_id,
       pc.process_status, pc.clip_bucket, pc.clip_key,
       pc.start_sec, pc.end_sec
FROM processed_clips pc JOIN raw_files r ON r.asset_id=pc.source_asset_id
WHERE r.source_unit_name LIKE '${folder}'
ORDER BY pc.created_at DESC LIMIT 20;

\\echo '=== dispatch_requests (top 20) ==='
SELECT request_id, folder_name, run_mode, labeling_method, status,
       ls_task_status, to_char(created_at,'MM-DD HH24:MI:SS') AS created
FROM dispatch_requests
WHERE folder_name LIKE '${folder}' OR request_id LIKE '%${folder}%'
ORDER BY created_at DESC LIMIT 20;

\\echo '=== dispatch_pipeline_runs (top 20) ==='
SELECT dpr.run_id, dpr.folder_name, dpr.step_name, dpr.step_status,
       dpr.input_count, dpr.output_count, dpr.error_count,
       to_char(dpr.started_at,'MM-DD HH24:MI:SS') AS started,
       to_char(dpr.completed_at,'MM-DD HH24:MI:SS') AS completed
FROM dispatch_pipeline_runs dpr
WHERE dpr.folder_name LIKE '${folder}' OR dpr.request_id LIKE '%${folder}%'
ORDER BY dpr.created_at DESC LIMIT 20;
"
}

# 사용 예:
#   pg_sample 'test'                    # 정확히 'test' folder
#   pg_sample 'appdata_part1_scenario2' # 분할 part1
#   pg_sample 'appdata%scenario2%'      # 시나리오 2 의 모든 appdata 폴더
```

### 3.5 MinIO 객체 확인

```bash
# 1회 alias 등록 (재기동 시 다시 해야 함)
docker exec docker-minio-1 mc alias set prodR http://10.0.0.51:9000 minioadmin minioadmin

# folder 별 객체 카운트
minio_count() {
  local folder="$1"
  for b in vlm-raw vlm-labels vlm-processed vlm-dataset vlm-classification; do
    n=$(docker exec docker-minio-1 mc ls --recursive prodR/${b}/${folder}/ 2>/dev/null | wc -l)
    echo "${b}/${folder}: ${n}"
  done
}
```

### 3.6 Cleanup (각 stage 후)

`/tmp/cleanup_qa.sql` 작성:
```sql
-- :keyword 변수 (psql -v keyword='%test%' 형식으로 전달)
BEGIN;
DELETE FROM image_labels WHERE image_id IN (SELECT im.image_id FROM image_metadata im JOIN raw_files r ON r.asset_id=im.source_asset_id WHERE r.source_unit_name LIKE :'keyword');
DELETE FROM labels WHERE asset_id IN (SELECT asset_id FROM raw_files WHERE source_unit_name LIKE :'keyword');
DELETE FROM processed_clips WHERE source_asset_id IN (SELECT asset_id FROM raw_files WHERE source_unit_name LIKE :'keyword');
DELETE FROM image_metadata WHERE source_asset_id IN (SELECT asset_id FROM raw_files WHERE source_unit_name LIKE :'keyword');
DELETE FROM video_metadata WHERE asset_id IN (SELECT asset_id FROM raw_files WHERE source_unit_name LIKE :'keyword');
DELETE FROM dispatch_pipeline_runs WHERE request_id IN (SELECT request_id FROM dispatch_requests WHERE folder_name LIKE :'keyword' OR request_id LIKE :'keyword');
DELETE FROM dispatch_requests WHERE folder_name LIKE :'keyword' OR request_id LIKE :'keyword';
DELETE FROM raw_files WHERE source_unit_name LIKE :'keyword';
COMMIT;
```

```bash
cleanup_stage() {
  local keyword_pattern="$1"     # 예: 'test' 또는 '%appdata_%scenario1%'
  local folder_pattern="$2"      # rm 용 glob 예: 'test' 또는 'appdata_*scenario1*'

  # 1) PG cascade
  docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline \
    -v keyword="${keyword_pattern}" < /tmp/cleanup_qa.sql

  # 2) MinIO 5 버킷
  for b in vlm-raw vlm-labels vlm-processed vlm-dataset vlm-classification; do
    docker exec docker-minio-1 mc ls prodR/${b}/ 2>/dev/null | awk '{print $NF}' | grep -F "${folder_pattern%/*}" | while read p; do
      docker exec docker-minio-1 mc rm --recursive --force "prodR/${b}/${p}" 2>&1 | tail -1
    done
  done

  # 3) archive 폴더 (root 권한)
  docker run --rm -v /home/user/mou/nas_200tb:/nas alpine sh -c "rm -rf /nas/archive/${folder_pattern}"

  # 4) incoming 폴더 — intra-run dedup 으로 남은 파일 (uploaded 안 된 duplicate 원본) 또는
  #    archive_finalize 실패로 남은 잔여물 정리. ⚠️ 원본 폴더(test, test2, test3, appdata) 는
  #    /home/user/mou/nas_200tb/<folder> 에 있고, incoming 의 사본만 삭제됨 (안전).
  docker run --rm -v /home/user/mou/nas_200tb:/nas alpine sh -c "rm -rf /nas/incoming/${folder_pattern}"
}

# 사용 예:
# cleanup_stage 'test' 'test'
# cleanup_stage '%appdata_%scenario1%' 'appdata_*scenario1*'
```

⚠️ **incoming 청소 중요성** — 2026-05-20 Stage 4 발견:
- intra-run dedup 이 발생하면 (같은 batch 안에 동일 checksum 파일이 2개+) 첫 번째만 archive 로 이동되고
  나머지 dedup 파일은 incoming/ 에 남는다.
- 다음 시나리오에서 `auto_bootstrap_manifest_sensor` (RUNNING 시) 가 이 잔여 폴더를 picking up 할 수 있음.
- `AUTO_BOOTSTRAP_REQUIRE_DONE_MARKER=true` 이면 `_DONE` 없는 폴더는 skip 되어 즉시 위험은 없으나,
  의도치 않은 누적/혼선을 방지하려면 cleanup_stage 가 항상 incoming/<folder> 도 삭제해야 함.

### 3.7 원본 보존 검증 (cleanup 직후 항상)

```bash
verify_source_intact() {
  for d in test test2 test3; do
    cnt=$(ls /home/user/mou/nas_200tb/$d 2>/dev/null | wc -l)
    [ "$cnt" -eq 20 ] && echo "✅ $d=20" || echo "🚨 $d=$cnt (expected 20)"
  done
  cnt=$(ls /home/user/mou/nas_200tb/appdata 2>/dev/null | wc -l)
  [ "$cnt" -eq 2500 ] && echo "✅ appdata=2500" || echo "🚨 appdata=$cnt (expected 2500)"
}
```

cleanup 후 위 함수 호출 → 모두 ✅ 이어야 안전.

---

## 📌 4. 시나리오 1 — 소형 순차 + appdata 전체 1 dispatch

### 목적
- 소형 polyglot iter (다양한 method) → 각각 cleanup 검증
- 단일 large dispatch (2500) 가 limit=100000 default 로 끊김 없이 처리되는지 검증

### 단계 체크리스트

#### Stage 1 — test → bbox
- [ ] `copy_to_incoming test`
- [ ] dispatch JSON (`folder_name="test"`, `labeling_method=["bbox"]`) 작성 → `drop_dispatch`
- [ ] `tail_milestones` 로 RUN_SUCCESS 확인 (~3-5 min 예상)
- [ ] `pg_artifacts test` 측정 → 측정 표에 기록
- [ ] `pg_sample test` — DB 8 테이블 top-20 행 출력 검증 (cleanup 직전 1회)
- [ ] `cleanup_stage 'test' 'test'`
- [ ] `verify_source_intact`

#### Stage 2 — test2 → timestamp+caption
- [ ] `copy_to_incoming test2`
- [ ] dispatch JSON (`folder_name="test2"`, `labeling_method=["timestamp_video","captioning_video"]`)
- [ ] tail + 측정 + cleanup `test2` + verify

#### Stage 3 — test3 → 혼합 (bbox + ts + caption)
- [ ] `copy_to_incoming test3`
- [ ] dispatch JSON (`folder_name="test3"`, `labeling_method=["bbox","timestamp_video","captioning_video"]`)
- [ ] tail + 측정 + cleanup `test3` + verify

#### Stage 4 — appdata 전체 1 dispatch (2500 files)
- [ ] `hardlink_appdata_all appdata_all_scenario1`
- [ ] dispatch JSON (`folder_name="appdata_all_scenario1"`, `labeling_method=["bbox","timestamp_video","captioning_video"]`)
- [ ] tail + 측정 (~2-3시간 예상) + cleanup `'%appdata_%scenario1%'` `'appdata_*scenario1*'` + verify

---

## 📌 5. 시나리오 2 — 소형 순차 + appdata 분할 병렬 (1250 + 1250)

### 목적
- Stage 1-3 시나리오 1과 동일 (회귀 검증)
- Stage 4 만 분할 병렬 — 두 run 경합 시 throughput/heartbeat/race 확인

### 단계 체크리스트

#### Stage 1-3 (시나리오 1 의 Stage 1-3 그대로 반복)
- [ ] 시나리오 1 종료 후 깨끗하면 그대로 진행. cleanup 잘 되었는지 `verify_source_intact` + `pg_artifacts test` 0 확인.

#### Stage 4 — appdata 분할 2 dispatch
- [ ] `hardlink_appdata_split appdata_part1_scenario2 appdata_part2_scenario2`
- [ ] dispatch JSON × 2 작성:
  - part1: `folder_name="appdata_part1_scenario2"`, `labeling_method=["bbox"]`
  - part2: `folder_name="appdata_part2_scenario2"`, `labeling_method=["timestamp_video","captioning_video"]`
- [ ] **두 JSON 거의 같은 시각에 드롭** (drop_dispatch × 2 sequentially within 1 sec)
- [ ] tail (양쪽 run 동시 진행 관찰) + 측정 + cleanup `'%appdata_%scenario2%'` `'appdata_*scenario2*'` + verify

---

## 📌 6. 측정 결과 표 (시나리오 별로 채움)

### 시나리오 1

| Stage | folder | method | wall time | raw_files | frames | image_labels | gemini_events | failures | heartbeat |
|---|---|---|---|---|---|---|---|---|---|
| 1 | test | bbox | 5m 37s | 20/20 | - | - | - | 0 | 0 |
| 2 | test2 | ts+cap | 3m 13s | 20/20 | - | - | - | 0 | 0 |
| 3 | test3 | mix | 3m 1s | 20/20 | 0 (mix→no raw_video_to_frame) | 0 | - | 0 | 0 |
| 4 | appdata_all_scenario1 | mix | **4h 00m** (raw_ingest+50 Gemini) | **2496/2500** (4 intra-run dedup) | 0 | 0 | 50 vids ts+cap done | 0 fail / 9+ archive_move_timeout (auto-recovered) | 0 |

#### Stage 4 상세 — 2026-05-20 10:45:45 → 14:45:39 KST (01:45:59 → 05:45:39 UTC)
- **Upload phase**: 01:46:08 → 03:38:08 = **1h 52m** (uploaded=2496, dedup_skip=4, 2.7s/file avg)
- **Archive_finalize**: 01:46:08 → 05:42:36 = **3h 56m 28s** (2496 archives, 5.7s/file avg — Phase 0 baseline 13.3s/file의 **2.3× 개선**)
- **clip_timestamp + clip_captioning**: 05:42:41 → 05:45:33 = **2m 52s** (50 videos / 172s = 3.44s/video, workers=5, batch=50)
- **RUN_SUCCESS**: 05:45:39 — dispatch_run_success_sensor finalized with `aborted_raw_files=0`
- **ls_task_create_sensor**: 05:46:04 → 05:49:34 = ~3.5m (LS project 87/88 자동 생성)
- ⚠️ **Critical finding (limit=50)**: 아래 § 8.5 참조

⚠️ NFS metadata 동시성으로 archive 후반에 9+ 개의 `archive_move_timeout: 300s 초과` 경고 (모두 destination-exists check 로 자동 recover, benign).

### Phase 0 baseline 대비 throughput
- archive_finalize wall time: 13.3 s/file → **5.7 s/file** (Phase 1 INGEST_ARCHIVE_WORKERS=8 효과)
- upload throughput: 0.05 obj/sec/run (1 run) → 2.7s/file ≈ 0.37 obj/sec (Phase 2 MultipartUpload 효과)
- heartbeat: 298회/6h → **0회/4h** (Phase 1 DAGSTER_GRPC_HEARTBEAT_TIMEOUT=60 효과)

### 시나리오 2

| Stage | folder | method | wall time | raw_files | frames | image_labels | gemini_events | failures | heartbeat |
|---|---|---|---|---|---|---|---|---|---|
| 1 | test | bbox | **11m 15s** (archive_move 7m07s — fast-path skip) | 20/20 | 200 | 200 | 0 | 0 | 19 |
| 2 | test2 | ts+cap | | / 20 | | | / 0 | | |
| 3 | test3 | mix | | / 20 | | | / | | |
| 4 part1 | appdata_part1_scenario2 | bbox | | / 1250 | | | / 0 | | |
| 4 part2 | appdata_part2_scenario2 | ts+cap | | / 1250 | | | / | | |

#### Stage 1 메모 (시나리오 2)
- archive/test/ 에 이전 잔여 4 파일 → fast-path 비활성 → archive_move 7m07s (vs 시나리오 1 Stage 1: 5m37s)
- raw_video_to_frame 완료: 200 frames, 0 failed
- SAM3 detections: **56** (fire/flame/smoke/knife/gun/bat/baseball bat/sword/dagger/violence/fight/person_fallen 카테고리)
- cleanup 후 LS task NoSuchKey 에러 발생 (race) — LS project 89 빈 채로 남음, 운영 영향 없음

### Phase 0 baseline (비교용, 2026-05-19 측정)
- appdata bbox 1247: archive_finalize 시작까지 1h 53m, 총 4h 36m
- appdata ts 1249: 총 4h 10m
- 동시 두 run 시 upload 0.05 obj/sec/run
- heartbeat 298회 (6h 동안)
- SAM3 200/9648 frames (B5 cap)

---

## 📌 7. 시나리오 추가 가이드

새 시나리오 추가 시:

1. **상단 "변경 이력" 표에 row 추가**
2. **본문에 `## 📌 N. 시나리오 N — <한 줄 요약>` 섹션 신설**:
   - 목적
   - 단계 체크리스트 (각 단계 `- [ ]`)
   - cleanup 시점
3. **"측정 결과 표" 에 빈 row 추가** (시나리오 N table)
4. 새 헬퍼 필요하면 "공통 헬퍼" 섹션에 추가 (모든 시나리오 공유)

### 시나리오 아이디어 (TODO)

- [ ] 시나리오 3: **GPU 동시 부하** — 동일 시각에 bbox dispatch × 3 (SAM3 GPU 경합)
- [ ] 시나리오 4: **fail-forward 검증** — 일부 손상 mp4 + 정상 mp4 혼합
- [ ] 시나리오 5: **NVENC on/off A/B** — 같은 50 file 을 NVENC on / off 비교 (REENCODE_USE_NVENC env toggle)
- [ ] 시나리오 6: **race 회귀 검증** — dispatch drop 직후 같은 폴더에 추가 파일 — B6 fix 후 동작 확인
- [ ] 시나리오 7: **연속 dispatch** — 1번 끝나기 전에 다음 1번 시작 (queue 동작 확인)

#### 2026-05-20 QA 발견 기반 추가 (A-E)

- [ ] 시나리오 8 (A): **archive 잔여물 회귀 보호** — 의도적으로 `archive/test/` 에 N 파일 남긴 채 test dispatch → fast-path 비활성(existing_archive!=None) → per-file path 진입 동작 + wall time 측정 + 사전점검 § 2 단계 F 가 catch 하는지 검증. 회귀 보호.
- [ ] 시나리오 9 (B): **auto_labeling_sensor backlog 자연소진** — 시나리오 1 Stage 4 처럼 limit 초과 backlog 100 정도 의도적으로 만들고 sensor ON 상태에서 측정. 60s 간격 picking, cursor (event_seq) 정확 증가, in-flight 중복 트리거 없는지, 처리 throughput, 종료 시점(backlog=0) cursor 안정성. limit=100000 + sensor wiring 회귀 보호.
- [ ] 시나리오 10 (C): **auto_bootstrap chunked 자동 dispatch** — `_DONE` 마커와 함께 incoming 에 폴더 던지고 (manual dispatch JSON 없이) `auto_bootstrap_manifest_sensor` 의 chunked manifest 생성 → 여러 작은 dispatch 자동 트리거 검증. `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK`, `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES`, manifest 분할 단위, 동시 in-flight 제한 측정. 운영 정상 패턴 검증.
- [ ] 시나리오 11 (D): **cleanup race (LS task NoSuchKey 회피)** — Stage 1-3 에서 매번 발견: cleanup_stage 가 ls_task_create_job RUN_SUCCESS 보다 빨라서 NoSuchKey + LS project orphan. 헬퍼 추가: `cleanup_stage` 가 dispatch_requests.ls_task_status='created' 확인 후 (또는 ls_task_create_job 종료 확인 후) 진행하도록 sleep/poll. 회귀 검증.
- [ ] 시나리오 12 (E): **classification_video flow** — 지금까지 어느 시나리오에서도 안 돌린 분기. dispatch JSON 의 `outputs` 에 `classification_video` 추가 (현재 spec 미상 — `labeling_method` 와 별도일 가능성) → `vlm-labels/<folder>/classification/<file>.json` 객체 생성 + `labels` 테이블에 `label_format='video_classification_json'`, `caption_text=<predicted_class>` 행 적재 확인. 미검증 분기 커버리지.

#### 보너스 (우선순위 ↓)

- [ ] F. **Label Studio webhook → post_review_clip_job** — LS UI 에서 사람 검수 후 `/sync-approve` 호출 → `post_review_clip_job` 트리거 → labels 의 timestamp 기반 clip 분할 + 이미지 추출 검증. LS 워크플로 완성도.
- [ ] G. **GCS download schedule** — 매일 04:00 KST `gcs_download_schedule` 실제 fire 시 동작 검증 (3개 버킷: source-c/source-b/source-a-rtsp). `GCS_ZERO_BYTE_RETRIES` 동작.
- [ ] H. **ingest retry manifest** — ffmpeg empty_output 4회 retry 후 영구실패 → retry manifest 자동 생성 + jsonl failed log 확인. 재처리 path 검증.
- [ ] I. **NVENC fallback (Phase 3 Option C)** — NVENC re-encode 가 실패하는 input (예: 특정 codec) → `reencode_with_fallback` 의 in-place fallback 동작 검증. `video_metadata.reencode_reason` 컬럼에 `fallback:<Error>:<msg>` 적재 확인.

---

## 📌 8. 트러블슈팅

### dispatch JSON 이 30초 이상 pickup 안 됨
- `docker logs docker-dagster-daemon-1 --since 1m | grep dispatch_sensor` 확인
- pending dir 존재? `ls /home/user/mou/nas_200tb/incoming/.dispatch/pending/`
- dispatch_sensor STOPPED 상태? Dagster UI 에서 확인

### raw_ingest 가 ffmpeg empty_output fallback 반복
- NVENC re-encode 산출물의 frame seek 호환성 이슈
- `REENCODE_USE_NVENC=false` 로 한 번 돌려봐서 차이 비교
- 4 회 retry 안에 회복되면 무시 OK

### archive 폴더 안 비워짐 (이전 데이터 잔여)
- `ls /home/user/mou/nas_200tb/archive/ | grep <keyword>` 로 확인
- `docker run --rm -v /home/user/mou/nas_200tb:/nas alpine rm -rf /nas/archive/<folder>`

### MinIO mc 가 권한 거부 에러
- `mc alias set` 재실행 (재기동 후 alias 사라짐)
- prod: `mc alias set prodR http://10.0.0.51:9000 minioadmin minioadmin`

### 🚨 (Scenario 2 Stage 1 발견) archive_move 7분 소요 — archive 잔여물로 fast-path 비활성

**증상** (2026-05-20 시나리오 2 Stage 1, test 폴더):
- 20 파일 dispatch, dedup_skip=0 (모든 파일 unique)
- 조건상 fast-path 활성화 되어야 하지만 archive_move 가 **7분 7초** 소요 (per-file path 사용됨)

**원인**:
- archive/test/ 에 이전 QA 의 잔여 4 파일 존재 (cleanup partial)
- archive_finalize.py 197-203 fast-path 조건 중 `existing_archive is None` 실패
- per-file ThreadPool 8 worker NFS rename → 평균 21s/file × 20 = 420s

**예방**:
- 사전 점검 § 2 단계 F 추가 (`archive/<folder>` 잔여 0 확인)
- `cleanup_stage` 가 archive/<folder> 도 삭제하지만, 이전 cleanup 이 partial 이었다면 사전 점검에서 catch 해야 함

**근본 대응 옵션 (추후)**:
- archive_finalize 의 fast-path 가 existing_archive 의 내용을 보고 비어있거나 안전하면 정리 후 rename 활성화 — 복잡, 안전성 검토 필요. 현재로선 사전 cleanup 으로 회피.

### 🚨 (Stage 4 발견) archive_move_timeout 다발 — dedup 으로 fast-path 비활성 + NFS dir lock 경합

**증상** (시나리오 1 Stage 4):
- archive_finalize 단계가 3h 56m 소요 (2496 파일, ~5.7s/file)
- 후반에 `archive_move_timeout: 300s 초과` 경고 9건+ 다발, 모두 destination-exists check 로 auto-recover
- ThreadPoolExecutor 8 worker 가 같은 archive dir 에 rename → NFS dir inode lock 경합

**원인**:
- [src/vlm_pipeline/defs/ingest/archive_finalize.py](../../src/vlm_pipeline/defs/ingest/archive_finalize.py) fast-path 조건:
  ```python
  len(uploaded) >= source_unit_total_file_count
  ```
- 2496 < 2500 (intra-run dedup 4 건) → fast-path 비활성 → per-file ThreadPool path 사용
- NFS server 의 directory inode lock 으로 동시 rename serial 화 → 각 rename 30~300s+ 소요

**fix (2026-05-20 commit)**:
- `archive_uploaded_assets()` 에 `duplicate_skip_count` 파라미터 추가
- fast-path 조건: `len(uploaded) + duplicate_skip_count >= source_unit_total_file_count`
- `ingest_orchestrate.py` 가 `state.records` 에서 `error_message.startswith("duplicate_of:")` 개수 카운트 + 전달
- dedup orphan 파일은 archive 에 추가 이동되지만 DB 추적 안 됨 (intentional skip, harmless)
- 결과: 폴더 단위 rename 1 회 (~1s) 로 archive_finalize 완료, 100~1000× 개선 기대
- 회귀 방지 test: `test_archive_uploaded_assets_dedup_count_enables_fast_path`

### 🚨 (Stage 4 발견) clip_timestamp / clip_captioning limit=50 — 단일 large dispatch 시 2% 만 처리됨

**증상** (시나리오 1 Stage 4 — 2026-05-20 측정):
- 2500 파일 dispatch → raw_ingest+archive 까지는 2496 모두 정상 (3h 56m)
- clip_timestamp 만 50 files 처리 후 RUN_SUCCESS, 나머지 2446 video_metadata.auto_label_status='pending' 으로 정체
- dispatch_pipeline_runs 의 gemini_timestamp/gemini_caption/yolo_detect/frame_extract = "skipped" by dispatch_run_success_sensor (aborted_raw_files=0 → 정상 종료로 판단)

**원인**:
- [src/vlm_pipeline/defs/label/assets.py:50](../../src/vlm_pipeline/defs/label/assets.py#L50) `config_schema={"limit": Field(int, default_value=50)}` — clip_timestamp
- [src/vlm_pipeline/defs/label/assets.py:68](../../src/vlm_pipeline/defs/label/assets.py#L68) — clip_captioning 도 동일
- [src/vlm_pipeline/defs/label/timestamp.py:182,298](../../src/vlm_pipeline/defs/label/timestamp.py#L182) — `limit = int(context.op_config.get("limit", 50))`
- backlog 을 picking up 하는 sensor (`auto_labeling_sensor`) 는 코드(`defs/label/sensor.py`)에는 존재하나 `definitions_production.py` 에 **wiring 안 됨**

**정상 (의도된) 동작 패턴**:
- 운영자가 `auto_bootstrap_manifest_sensor` ON 으로 두면 incoming/ 폴더가 50-file chunk 로 자동 분할 → manifest N개 → dispatch N번 → 각각 50 video Gemini 처리
- 즉 dispatch JSON 1개 = 50 file 처리가 "by design". 한 번에 2500 파일을 던지면 50 만 처리됨.

**fix 옵션 (시나리오 진행 전 정해야 함)**:
1. **A: limit 상향 (간단)** — `assets.py` 두 곳 `default_value=50` → 예: `5000`. assets/timestamp.py 의 fallback 도 동일하게. 이미지 재빌드 + 재배포. 영향: 1 dispatch 가 Gemini quota 한도까지 한 번에 처리 → 메모리/quota burst 위험.
2. **B: backlog sensor 활성화** — `auto_labeling_sensor` 를 `definitions_production.py` 에 wire (sensors=(..., auto_labeling_sensor)). 1 dispatch 후 sensor 가 60s 간격으로 50개씩 자동 처리 → backlog 자연 소진. 영향: 분당 5,000 video Gemini 호출의 sustained load (quota OK 면 안전).
3. **C: 운영 패턴 유지** — auto_bootstrap_manifest_sensor RUNNING 으로 두고 manual dispatch 큰 JSON 던지지 않기 (= 큰 폴더는 chunked auto-dispatch 만 허용). QA scenario 도 chunked 로 재설계 필요.

**현재 상태 (2026-05-20)**: ✅ **A+B+C 모두 적용 + prod 재배포 완료** (PR #80, commit bae0c2a + 2daf536).

| | 적용 전 | 적용 후 (prod 14:08 KST) |
|---|---|---|
| `clip_timestamp limit default` | 50 | **100000** |
| `classification_video limit default` | 50 | **100000** |
| `auto_labeling_sensor` 로드 | ❌ | ✅ RUNNING |
| `auto_labeling_job` 정의 | ❌ | ✅ `selection=[clip_timestamp, clip_captioning, classification_video]` |
| `auto_bootstrap_manifest_sensor` | STOPPED | ✅ RUNNING (GraphQL toggle) |
| CI `src/vlm_pipeline/` rebuild trigger | ❌ | ✅ deploy-test.yml + deploy-production.yml 둘 다 fix |

이제 시나리오 2 (분할 1250+1250) 진행 가능. `auto_bootstrap_manifest_sensor` 가 RUNNING 이므로 leftover dedup'd folder 가 자동 picking up 될 수 있음 — 단, `AUTO_BOOTSTRAP_REQUIRE_DONE_MARKER=true` 이므로 `_DONE` 마커 없는 폴더는 안전 skip.

### auto_bootstrap 이 dispatch 폴더 또 picking up (B6 회귀)
- PR #78 (B6 fix) 적용 확인:
  ```bash
  docker exec docker-dagster-1 python3 -c "
  import inspect
  from vlm_pipeline.defs.ingest import sensor_bootstrap_helpers as h
  print('processed/' in inspect.getsource(h._load_dispatch_requested_folders))"
  ```
  `True` 이어야 함. False 면 prod 가 PR #78 이전 버전 — 재배포 필요.

---

## 📌 9. 참고 링크

- [Phase 1-4 bottleneck 분석 doc](./scale-bottleneck-fixes-2026-05-19.md)
- PR #75 Phase 1 — heartbeat + archive parallel + dispatch limits
- PR #76 Phase 2 — NVENC 코드 + MINIO env
- PR #77 Phase 3 — YOLO Field default + sensor backlog override + YOLO 비활성 정책
- PR #78 Phase 4 — B6 race condition fix (auto_bootstrap processed/ exclusion)

CLAUDE.md 에 환경 / 운영 정책 / 코드 layer 정보.
