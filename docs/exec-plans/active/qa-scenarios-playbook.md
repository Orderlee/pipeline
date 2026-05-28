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
| 2026-05-22 | §0.5 시나리오 인덱스 표 추가 + §7 카테고리별 재정렬 (총 23 시나리오) + classification/post_review_clip 중복 통합 | claude |
| 2026-05-22 | §0.7 측정 결과 종합 표 추가 (실행된 모든 row 한눈에) — §6 상세 메모는 유지 | claude |
| 2026-05-22 | NVENC dual-GPU round-robin (`REENCODE_NVENC_GPU_INDICES`) + SAM3 workers=3→4 prod 적용 — 시나리오 24 신규 추가 | claude |
| 2026-05-22 | 🚨 시나리오 24 1차 실행 중 NVENC ffmpeg arg 순서 bug 발견 (`-c:v -gpu 1 h264_nvenc`) → fix dev push eb9d2e9. 1차 측정 무효, 2차는 main 머지 후 재실행 | claude |
| 2026-05-26 | 시나리오 24 2차 완료 (qa_s24v2): NVENC dual-GPU 정상 동작 (peak ENC 80%/GPU, fallback 0). 단 wall time은 S2 대비 +18-19%. archive_finalize가 새 bottleneck | claude |
| 2026-05-27 | 시나리오 24 3차 (qa_s24v3): EXDEV fix(폴더 fast-path <1s) 효과 입증 — part2 ts+cap **3h01m47s (S24v2 −39%)**. part1 은 NAS 10.0.0.51 일시 outage→load 393→SAM3 GPU1 OOM→orphan 자동취소(무효). 코드수정 필요항목 도출: SAM3 retry 부재, GPU OOM(workers=4), sensor NFS 가드 | claude |
| 2026-05-28 | 시나리오 24 4차 (qa_s24v4 part1 재측정): SAM3_WORKERS 4→2 적용 후 part1 bbox **5h29m13s** clean 완주 — SAM3 12542/12542 **fail=0**, GPU1 여유 10.6GB, NAS 안정. S2 −29%, S24v2 −40%. EXDEV fast-path + NVENC + workers=2 종합 입증. retry(#1) + expandable_segments(#2b) dev 준비 완료(741439b) → 후속 main 머지로 prod 적용 | claude |
| 2026-05-27 | 🔍 시나리오 15 EXDEV root cause 정정: NAS dataset 은 이미 통합됐고 진짜 원인은 **docker-compose 의 incoming/archive 별도 bind-mount** (컨테이너 안 별개 mount point). 검증 run(qa_s15)으로 확정. fix 는 compose 단일 부모 mount. 시나리오 15 미완료 상태 유지 | claude |
| 2026-05-27 | ✅ 시나리오 15 완료: incoming/archive 공통 부모 단일 bind(/nas/data) → 컨테이너 EXDEV 해소. PR #92 머지 → prod 배포(251b9fb). staging+prod 둘 다 컨테이너 내부 os.rename 성공, archive_finalize 폴더 fast-path <1s 확인 | claude |
| 2026-05-27 | ✅ sam3 단일 공유 컨테이너화: profiles:["sam3"] + prod COMPOSE_PROFILES=sam3, staging 은 SAM3_API_URL=http://docker-sam3-1:8002 공유 참조. PR #93 머지 → prod 배포(6e68830). 기존 staging sam3 포트 8002 충돌(배포 실패) 해소. staging+prod deploy GREEN, 양쪽 dagster→sam3 200 | claude |

> 시나리오 추가/변경 시 위 표 + 본문 "## 시나리오 N" 섹션 + "## 측정 결과" 표에 row 추가.

---

## 📌 0.5 시나리오 인덱스 (전체 한눈에)

> 시나리오 추가 시 여기에도 row 추가. 상태: ✅ 완료 / 🔄 진행 중 / ⏳ 대기

### 정의된 시나리오 (Stage 단위 큰 흐름)

| # | 이름 | 카테고리 | 상태 | 본문 |
|---|---|---|---|---|
| 1 | 소형 순차 + appdata 전체 1 dispatch | 운영 흐름 | ✅ 완료 (2026-05-20+21) | §4 |
| 2 | 소형 순차 + appdata 분할 1250+1250 병렬 | 운영 흐름 / 부하 | 🔄 진행 중 (2026-05-22) | §5 |
| 3 | GPU 동시 부하 (bbox × 3 동시) | 부하 | ⏳ 대기 | §7 |
| 4 | fail-forward (손상 mp4 + 정상 혼합) | 장애 대응 | ⏳ 대기 | §7 |
| 5 | NVENC on/off A/B 비교 | 성능 | ⏳ 대기 | §7 |
| 6 | race 회귀 (B6 fix) | 회귀 보호 | ⏳ 대기 | §7 |
| 7 | 연속 dispatch (queue 동작) | 운영 흐름 | ⏳ 대기 | §7 |
| 8 | archive 잔여물 회귀 보호 | 회귀 보호 | ⏳ 대기 | §7 |
| 9 | auto_labeling_sensor backlog 자연소진 | 분기 커버리지 | ⏳ 대기 | §7 |
| 10 | auto_bootstrap chunked 자동 dispatch | 운영 정상 흐름 | ⏳ 대기 | §7 |
| 11 | cleanup race 회피 (LS task NoSuchKey) | 회귀 보호 | ⏳ 대기 | §7 |
| 12 | classification_video + vlm-classification 두 분기 | 분기 커버리지 | ⏳ 대기 | §7 |
| 13 | SAM3 workers=1/3/5 throughput 비교 | 성능 | ⏳ 대기 | §7 |
| 14 | dual SAM3 (GPU 0 + GPU 1) | 성능 | ⏳ 대기 | §7 |
| 15 | 컨테이너 EXDEV 회피 (incoming/archive 단일 bind mount) | 운영 인프라 | ✅ 완료 (2026-05-27, PR #92, staging+prod 검증) | §7 |
| 16 | post_review_clip_job 전체 흐름 (LS 검수 → clip 분할) | 분기 커버리지 / LS | ⏳ 대기 | §7 |
| 17 | GCS download schedule | 외부 시스템 | ⏳ 대기 | §7 |
| 18 | ingest retry manifest (ffmpeg empty_output 4 retry) | 장애 대응 | ⏳ 대기 | §7 |
| 19 | NVENC fallback (Phase 3 Option C) | 회귀 보호 | ⏳ 대기 | §7 |
| 20 | ls_task_create_sensor in-flight 회귀 (PR #87) | 회귀 보호 | ⏳ 대기 | §7 |
| 21 | archive _DONE marker 보류 회복 (failed jsonl) | 장애 대응 | ⏳ 대기 | §7 |
| 22 | NFS slot exhaustion 부하 테스트 (slot=2 vs 32 vs 128) | 부하 | ⏳ 대기 | §7 |
| 23 | dedup orphan 정리 (fast-path archive 잔여) | 운영 정리 | ⏳ 대기 | §7 |
| 24 | NVENC dual-GPU round-robin throughput 측정 (PR 2026-05-22) | 성능 / 회귀 보호 | ⏳ 대기 | §7 |

**카테고리 요약**: 운영 흐름 4, 부하 3, 회귀 보호 6, 분기 커버리지 4, 성능 4, 장애 대응 3, LS/외부 시스템 2, 운영 인프라/정리 2.

---

## 📌 0.7 측정 결과 종합 (모든 실행 한눈에)

> 실행된 모든 측정의 핵심 row 만. **상세 메모/finding 은 § 6 참조**. 새 실행 후 여기 row 추가 + §6 상세 갱신.

### 시나리오 1 — 소형 순차 + appdata 전체 1 dispatch

| 실행일 | Stage | wall time | raw_files | frames | img_labels | gemini_events | heartbeat | 핵심 |
|---|---|---|---|---|---|---|---|---|
| 2026-05-20 | 1 test bbox | 5m 37s | 20/20 | - | - | - | 0 | ✅ baseline |
| 2026-05-20 | 2 test2 ts+cap | 3m 13s | 20/20 | - | - | - | 0 | ✅ |
| 2026-05-20 | 3 test3 mix | 3m 1s | 20/20 | 0 | 0 | - | 0 | mix→frame_extract skip |
| 2026-05-20 | 4 appdata 2500 mix | **4h 00m** | 2496/2500 (4 dedup) | 0 | 0 | **50 vids only** | 0 | 🚨 limit=50 발견 |
| 2026-05-21 재측정 (post-fix) | 4 appdata 2500 mix | **7h 14m 57s** | 2496/2500 | 0 | 0 | **2779 (2493 vids 전체)** | 415 | ✅ limit=100000 + archive_move fix 검증 |

### 시나리오 2 — 소형 순차 + appdata 분할 1250+1250 병렬

| 실행일 | Stage | wall time | raw_files | frames | img_labels | gemini_events | heartbeat | 핵심 |
|---|---|---|---|---|---|---|---|---|
| 2026-05-20 | 1 test bbox | 11m 15s | 20/20 | 200 | 200 | 0 | 19 | archive 잔여물 → fast-path skip 7m07s |
| 2026-05-20 | 2 test2 ts+cap | 2m 31s | 20/20 | 0 | 0 | 23 | 0 | fast-path 활성 |
| 2026-05-20 | 3 test3 mix | 2m 47s | 20/20 | 0 | 0 | 23 | 0 | mix→frame_extract skip |
| 2026-05-20 | 4 part1 bbox | **9h 00m 26s** | 1249/1250 | **12,532** | **12,532** | 0 | 67 (공유) | 🚨 NFS slot=2 병렬경합 |
| 2026-05-20 | 4 part2 ts+cap | **5h 09m 20s** | 1247/1250 | 0 | 0 | 1600 | (공유) | 🚨 NFS slot=2 병렬경합 |
| 2026-05-22 재측정 (slot=128 + 모든 fix) | 1 test bbox | 5m 0s | 20/20 | 200 | 200 | 0 | (공유) | ✅ fast-path 회복 |
| 2026-05-22 | 2 test2 ts+cap | 2m 30s | 20/20 | 0 | 0 | 26 | (공유) | ✅ |
| 2026-05-22 | 3 test3 mix | 2m 35s | 20/20 | 0 | 0 | 18 | (공유) | ✅ |
| 2026-05-22 | 4 part1 bbox | **7h 44m 47s** | 1249/1250 | **12,532** | **12,532** | 0 | 454 (공유) | ✅ SAM3 1976 detections (prior 56→1976), workers=3 |
| 2026-05-22 | 4 part2 ts+cap | **4h 09m 32s** | 1247/1250 | 0 | 0 | **1650 (1247 vids 전체)** | (공유) | ✅ slot=128, Gemini 25×↑ vs prior 50 |

### 핵심 비교 — 시나리오 1 Stage 4 (prior vs post-fix)
- Gemini 처리: 50 vids → **2493 vids (50×)** ✅ limit=100000 효과
- wall: 4h → 7h 15m (전체 처리는 더 오래지만 valid 측정)
- archive_move: 3h 56m → 4h 9m (NFS EXDEV 로 per-file path, 비슷)

### 핵심 비교 — 시나리오 2 (prior slot=2 vs post-fix slot=128 + 모든 fix)
- part1 wall: 9h 00m → **7h 44m (14% ↓)**
- part2 wall: 5h 09m → **4h 09m (24% ↓)** ✅ NFS slot 효과
- 병렬 max wall (Stage 4 종료까지): 9h → **7h 44m (14% ↓)**
- part2 Gemini: 50 vids → **1247 vids (25×)** ✅ limit=100000 효과
- SAM3 detections: 56 → **1976 (35×)** ✅ SAM3 workers=3 + classification 정확도
- 병렬 stuck 회피 ✅ (prior 에서 part2 가 13m 만에 stuck 됐던 패턴 사라짐)
- LS task: 5× 600s timeout 실패 → **1번 성공 (12,532 frame 7m 안에)** ✅ timeout 1800 + in-flight fix
- heartbeat 454 (per-file NFS 경합 — 폴더 fast-path 무효한 한계)

### 베이스라인 (Phase 0, 2026-05-19)
- appdata bbox 1247: archive_finalize 시작까지 1h 53m, 총 4h 36m
- appdata ts 1249: 총 4h 10m
- 동시 두 run 시 upload 0.05 obj/sec/run
- heartbeat 298회 (6h)
- SAM3 200/9648 frames (B5 cap)

### 시나리오 24 — NVENC dual-GPU round-robin (1차 05-22 / 2차 05-26 / 3차 05-27 / 4차 05-28)

| 실행일 | Stage | wall time | raw_files | NVENC util | 핵심 |
|---|---|---|---|---|---|
| 2026-05-22 1차 | part1 bbox | 8h 30m 07s | 1249/1249 | 0% (bug) | 🚨 ffmpeg arg 순서 bug — 측정 무효 |
| 2026-05-22 1차 | part2 ts+cap | 4h 59m 16s | 1247/1247 | 0% (bug) | 같은 bug |
| 2026-05-26 2차 | part1 bbox | 9h 07m 04s | 1249/1249 | peak ~80%/GPU | NVENC fix. fallback=0. archive per-file ~3-4h (EXDEV) 가 bottleneck |
| 2026-05-26 2차 | part2 ts+cap | 4h 57m 32s | 1247/1247 | peak ~80%/GPU | 〃 |
| 2026-05-27 3차 | part2 ts+cap | **3h 01m 47s** | 1247/1247 | peak ~80%/GPU | ✅ **EXDEV fix(폴더 fast-path <1s) 후. S24v2 −1h56m(−39%), S2 −1h08m(−27%)** |
| 2026-05-27 3차 | part1 bbox | (무효, CANCELED 7h29m) | 1249/1249 ingest | — | 🚨 NAS 10.0.0.51 일시 outage→load 244~393→SAM3 GPU1 OOM(503)→orphan 자동취소. SAM3 11240/12532(fail 238) |
| 2026-05-28 4차 | part1 bbox | **5h 29m 13s** | 1250/1250 | peak ~80%/GPU | ✅ **EXDEV+NVENC+SAM3 workers=4→2(OOM fix) 후. SAM3 12542/12542 fail=0, 손실 0. S2 −2h15m(−29%), S24v2 −3h38m(−40%)** |

**핵심 결론 (3-4차)**: EXDEV fix가 archive_finalize를 **시간→<1초**로 줄여 part2 −39%, part1 −29~40% 단축. SAM3 workers=2 + #1 retry로 GPU1 OOM 차단(여유 10.6GB) → fail=0 손실 0 입증. NVENC arg fix(PR #91) + EXDEV fix(PR #92) + sam3 profile(PR #93) + B+C 노이즈 완화(PR #94) + 본 사이클 retry/expandable_segments(741439b dev 머지 대기)까지 모든 fix 검증 완료.

→ 상세 메모 + 인시던트 분석 §6 시나리오 24 절 참조.

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

> ⚠️ **2026-05-22 fix**: 이전엔 `/tmp/cleanup_qa.sql` 파일 호스트에 미리 생성해야 했음. 파일
> 없으면 stdin 빈 채로 redirect → psql 아무것도 안 실행 → silent fail (잔여물 남음).
> 회피: cleanup_stage 가 heredoc 으로 SQL 인라인 — 별도 파일 의존 제거.

```bash
cleanup_stage() {
  local keyword_pattern="$1"     # 예: 'test' 또는 '%appdata_%scenario1%'
  local folder_pattern="$2"      # rm 용 glob 예: 'test' 또는 'appdata_*scenario1*'

  # 1) PG cascade — heredoc inline (파일 의존 없음)
  docker exec -i docker-postgres-1 psql -U airflow -d vlm_pipeline \
    -v keyword="${keyword_pattern}" <<'SQL'
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
SQL

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

#### Stage 4 재측정 (2026-05-21, post-fix) — qa_v3_s1_stage4_20260521_122702

| 항목 | prior (2026-05-20) | post-fix (2026-05-21) | 비교 |
|---|---|---|---|
| wall time | 4h 00m | **7h 14m 57s** | 길어졌지만 valid 측정 (전체 Gemini 처리) |
| raw_files | 2496/2500 | 2496/2500 | 동일 (4 intra-run dedup) |
| ts_done / cap_done | **50** / 50 | **2493** / **2493** | **×50 개선** ✓ (limit=100000 fix 효과 확정) |
| gemini_events | 50 | **2779** | 전체 처리 |
| frames / image_labels | 0 / 0 | 0 / 0 | mix → frame_extract skip (design) |
| archive_move elapsed | 3h 56m | 4h 9m (per-file path, NFS EXDEV) | 큰 차이 없음 |
| heartbeat | 0 | **415 (지난 8h)** | per-file path NFS 경합 ↑ |
| LS task | created | created (4× 누적 발화 후 정상화) | sensor race finding |

**Fix 효과 검증**:
- ✅ **PR #80 limit=50→100000**: 50→2493 vids 전체 Gemini 처리 (50× 개선)
- ✅ **PR #85 archive_move.py fix**: 폴더 fast-path NFS EXDEV 즉시 fallback (이전 stuck 회피, src=`'Invalid cross-device link'` 로그 확인됨)
- ✅ **PR #86 GPU 격리**: dagster=GPU 0 / SAM3=GPU 1 명시. ffmpeg `-gpu 0`. 안정성 강화.
- ✅ **PR #82 LS task error_message + timeout**: timeout 1800s 안에 정상 처리. API key 마스킹.

**남은 finding (다음 QA 이연)**:
- ⚠️ **NFS 서버 측 EXDEV**: incoming/archive 가 같은 device id 인데도 server 가 cross-device 응답 → 폴더 fast-path 불가 → 운영 측 NAS dataset 통합 또는 NFS server 측 native move API 검토.
- ⚠️ **ls_task_create_sensor race**: in-flight check 없어서 첫 job status='created' 될 때까지 60s 간격으로 새 job 누적 발화 (이번 케이스 4 job 누적). 코드 fix 필요.
- ⚠️ **heartbeat 415**: per-file path NFS 경합으로 grpc 응답 지연 다발. NFS 폴더 fast-path 가 동작 안 하는 이상 회피 어려움.
- ⚠️ **archive _DONE marker 보류**: prior 실패 로그 jsonl 잔존 시 marker 안 만들어짐 — manifest dir 의 failed/ jsonl 정리 정책 필요.

### 시나리오 2

| Stage | folder | method | wall time | raw_files | frames | image_labels | gemini_events | failures | heartbeat |
|---|---|---|---|---|---|---|---|---|---|
| 1 | test | bbox | **11m 15s** (archive_move 7m07s — fast-path skip) | 20/20 | 200 | 200 | 0 | 0 | 19 |
| 2 | test2 | ts+cap | **2m 31s** (fast-path 활성, archive 51s) | 20/20 | 0 | 0 | 23 | 0 | 0 |
| 3 | test3 | mix | **2m 47s** (fast-path, archive 53s) | 20/20 | 0 (mix→no raw_video_to_frame) | 0 | 23 | 0 | 0 |
| 4 part1 | appdata_part1_scenario2 | bbox | **9h 00m 26s** (archive 4h36m, 병렬 경합) | 1249/1250 (1 dedup) | **12,532** | **12,532** | 0 | 0 | 67 (공유) |
| 4 part2 | appdata_part2_scenario2 | ts+cap | **5h 09m 20s** (archive 3h55m, 병렬 경합) | 1247/1250 (3 dedup) | 0 | 0 | 1600 (ts 1238 / cap 1238) | 0 | (공유) |

#### Stage 1 메모 (시나리오 2)
- archive/test/ 에 이전 잔여 4 파일 → fast-path 비활성 → archive_move 7m07s (vs 시나리오 1 Stage 1: 5m37s)
- raw_video_to_frame 완료: 200 frames, 0 failed
- SAM3 detections: **56** (fire/flame/smoke/knife/gun/bat/baseball bat/sword/dagger/violence/fight/person_fallen 카테고리)
- cleanup 후 LS task NoSuchKey 에러 발생 (race) — LS project 89 빈 채로 남음, 운영 영향 없음

#### Stage 4 메모 (시나리오 2) — 2026-05-20 16:04 KST 동시 dispatch
- **🚨 NFS RPC slot 부족 root cause 발견**: `tcp_slot_table_entries=2` (기본값) → 두 run 병렬 시 거의 직렬화 → part1 단독 추정 4-5h → 병렬 9h (약 **2× 느림**)
- **heartbeat 67회** (Stage 1 의 3.5× 이상) — NFS wait 로 인한 grpc 응답 지연 다발
- part1 archive_move 16576s (4h 36m) — fast-path 활성됐어도 NFS slot 경합으로 매우 느림
- part2 archive_move 14102s (3h 55m)
- **mix vs bbox 차이 발견**: 시나리오 1/2 Stage 3 (mix) 는 frames=0 인데 Stage 4 part1 (bbox only) 는 frames=12,532 — dispatch routing 또는 method 조합 처리 차이 (코드 확인 필요한 별개 finding)
- part1 `ls_task_status=failed` (error_message 비어있음, 원인 미상)
- **fix 필요 (다음 QA 전)**:
  1. `sudo sysctl -w sunrpc.tcp_slot_table_entries=128` + `/etc/sysctl.d/99-nfs-rpc.conf` 영구 적용
  2. NFS mount remount (umount→mount)
  3. 효과: 두 run 병렬 시 throughput 2x+ 회복 기대
- **dedup-aware fast-path 는 정상 동작**: part1 1249 raw_files + dedup 2개 = archive 에 1251 files (orphan 2). part2 1247 + 4 dedup = archive 1251 (orphan 4). 무해.

### Phase 0 baseline (비교용, 2026-05-19 측정)
- appdata bbox 1247: archive_finalize 시작까지 1h 53m, 총 4h 36m
- appdata ts 1249: 총 4h 10m
- 동시 두 run 시 upload 0.05 obj/sec/run
- heartbeat 298회 (6h 동안)
- SAM3 200/9648 frames (B5 cap)

### 시나리오 24 — NVENC dual-GPU round-robin throughput

| Stage | folder | method | wall time | raw_files | NVENC 사용 | 핵심 |
|---|---|---|---|---|---|---|
| 1차 (2026-05-22, bug) | appdata_part1_scenario2 | bbox | **8h 30m 07s** | 1249/1250 | 0% (bug) | 🚨 fallback 자연 완료, 측정 무효 |
| 1차 (2026-05-22, bug) | appdata_part2_scenario2 | ts+cap | **4h 59m 16s** | 1247/1250 | 0% (bug) | 🚨 fallback 자연 완료 |
| 2차 (2026-05-26, fix) | appdata_part1_s24v2 | bbox | **9h 07m 04s** | 1249/1250 | peak ~80%/GPU | ✅ fallback=0, S2 대비 +1h 22m (+18%) |
| 2차 (2026-05-26, fix) | appdata_part2_s24v2 | ts+cap | **4h 57m 32s** | 1247/1250 | peak ~80%/GPU | ✅ fallback=0, S2 대비 +48m (+19%) |

#### 🚨 NVENC ffmpeg arg 순서 bug — 1차 측정 무효

**증상**:
- dispatch 시작 직후부터 dagster-daemon WARNING `reencode fallback to original after 3 retries: ... reencode_failed:returncode=1` 다발 (~28/min, 누적 1570+)
- `nvidia-smi --query-gpu=utilization.encoder` 결과 GPU 0/1 모두 **0%** — NVENC silicon 미사용 확인
- compute log 의 `reencode_encoder=h264_nvenc gpu=` INFO 도 사실상 미반영

**원인** (PR #90 round-robin 도입 시 slicing 오류):
```python
# BUG (split [:1] / [1:])
STANDARD_PRESET_FFMPEG_ARGS_NVENC_BASE[:1] + ["-gpu", str(gpu_idx)] + STANDARD_PRESET_FFMPEG_ARGS_NVENC_BASE[1:]
# → ["-c:v", "-gpu", "1", "h264_nvenc", "-profile:v", ...]   ← codec 미선택
```
ffmpeg 가 `-c:v -gpu` 로 해석 → codec 미지정 → 실행 실패 → 3 retry → 원본 fallback.

**수동 검증**:
- `ffmpeg -y -i <src> -c:v -gpu 1 h264_nvenc ...` 직접 호출 시 동일 returncode=1 재현
- `ffmpeg -y -i <src> -c:v h264_nvenc -gpu 1 ...` (올바른 순서) 는 정상 (rc=0, NVENC encoder util 점등)
- 6 worker 동시 NVENC GPU 1 호출 시에도 모두 정상 (rc=0)

**수정** ([dev branch eb9d2e9](../../src/vlm_pipeline/lib/video_reencode.py), `fix(reencode): NVENC ffmpeg arg 순서`):
- `[:2]` 로 `-c:v h264_nvenc` 한 쌍 보존 후 `-gpu N` 삽입
- `STANDARD_PRESET_FFMPEG_ARGS_NVENC` (정적 리스트, 하위 호환) 도 동일 패턴으로 수정

**영향 평가**:
- 현재 실행 중인 시나리오 24 1차는 사실상 "NVENC off 상태로 S2 재실행" 과 동치 → 완료까지 둘 다 fallback 으로 원본 업로드. 결과는 S2 수치와 거의 같을 것으로 기대 (NVENC 효과 없음).
- 2차 측정은 `main` 머지 + prod 재배포 후 fresh Scenario 24 로 진행.

**다음 단계 체크리스트**:
- [x] 1차 runs (`2891e99b`, `30d3ace0`) 자연 완료 → cleanup 완료 (2026-05-25)
- [x] `dev` → `main` PR #91 머지 → prod 자동 재배포 성공 (commit 049abab)
- [x] Scenario 24 2차 dispatch (qa_s24v2_part1+part2) — 둘 다 SUCCESS
- [x] 2차 측정 결과 §0.7 / 본 절 반영

#### 2차 측정 결과 (2026-05-26, NVENC fix 검증)

**NVENC dual-GPU 동작 검증** ✅
- 12 concurrent ffmpeg (두 dispatch run × 6 worker), 모두 `-c:v h264_nvenc -gpu 0` / `-gpu 1` 올바른 순서로 호출
- nvidia-smi NVENC encoder utilization: peak GPU 0 76% / GPU 1 81%, 합산 ~130% 단일 GPU 한계 초과
- 누적 `reencode fallback` = **0건** (1차의 1570+ 대비 완전 해소)
- 라운드로빈 분배 균등 (장기 평균 GPU 0 ≈ GPU 1)

**throughput 분석** (예상과 다름)
- part1 bbox: 9h 07m 04s — S2 baseline 7h 44m 대비 **+1h 22m (+18% 느림)**
- part2 ts+cap: 4h 57m 32s — S2 baseline 4h 09m 대비 **+48m (+19% 느림)**
- 가설: NVENC가 빨리 reencode 끝 → upload 빨리 끝 → 둘이 거의 동시에 archive_finalize 진입 → **NFS per-file rename 경합이 새 bottleneck**. 즉 NVENC가 만든 시간 절약이 NFS 부하로 전가됨.
- 동시에 ENC 시간 단축은 측정되지만 archive_finalize 가 dominant cost. archive_finalize wall ≈ S2 와 동일 수준 (~3-4h)

**결론**
- NVENC dual-GPU **자체 효과는 검증됨** (ENC % 2× 활용, fallback 0).
- 하지만 전체 wall 단축은 컨테이너 EXDEV 가 풀려야 가능. → **2026-05-27 검증: root cause 는 NAS dataset 이 아니라 docker-compose 의 incoming/archive 별도 bind-mount** (컨테이너 안 별도 mount point → cross-mount rename EXDEV). 시나리오 15 (compose 단일 부모 mount fix) 적용 후 archive_finalize 가 ~1s 로 단축되면 NVENC dual-GPU 의 wall 이득도 비로소 측정 가능. 상세는 시나리오 15 절 참조.

#### 3차 측정 결과 (2026-05-27, EXDEV fix 후 + 인시던트)

**EXDEV fix 효과 입증** ✅
- archive_finalize 양쪽 **폴더 fast-path <1초** (part2 02:12:41 start=완료, part1 02:21:09 start=완료) — S24v2의 per-file ~1.5-3h 대비 결정적 단축
- **part2 ts+cap: 3h 01m 47s** — S24v2 4h57m **−1h56m(−39%)**, S2 4h09m **−1h08m(−27%)**. archive에서 ~1.5h 절약이 wall에 직접 반영
- NVENC dual-GPU 정상 (ENC GPU0/1 peak ~80%, reencode fallback 0)

**🚨 인시던트 — part1 무효 (NAS outage 연쇄)**
- part1 bbox: SAM3 detection 중 **NAS 10.0.0.51 일시 outage** 발생
- 연쇄: NAS down → `/nas/data` NFS hard-mount hang(D-state) + MinIO retry 폭주 → **호스트 load 244~393 폭증**(16코어, 대부분 I/O-wait) → SAM3 uvicorn worker CPU 굶음 + GPU1 **CUDA OOM**(workers=4 ×3.7GB=14.8GB가 jsh ComfyUI 782MB와 GPU1 16GB 공유 → 부하 중 파편화로 OOM) → `503/500 segment` 다발 → SAM3 detection 238+ 프레임 fail-forward 영구 실패 → run **orphaned**(code-server gRPC >900s) → `stuck_run_guard`가 자동 취소(11240/12532에서 CANCELED)
- NAS 복귀 후 자동 회복: SAM3 worker OOM 사망→uvicorn 재생성(clean 3.7GB×4), jsh ComfyUI 종료, load 393→1.0

**도출된 코드 수정 필요 (우선순위)**
1. 🔴 **SAM3 detection retry 부재** — [detection_assets.py:198-234](../../src/vlm_pipeline/defs/sam/detection_assets.py#L198-L234) MinIO download + `/segment` 호출이 모든 예외에 즉시 fail. transient(503/500/connection/timeout)에 bounded retry+backoff 추가 → 이번 ~250 프레임 손실 방지
2. 🔴 **SAM3 GPU1 OOM 완화** — `SAM3_WORKERS` 4→2~3 + `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True`(OOM 메시지가 직접 제안) + idle-unload(600s) burst 재로딩 fragility 개선
3. 🟡 **sensor NFS 스캔 타임아웃 가드** — archive_dispatch_sensor/auto_bootstrap_manifest_sensor가 NAS flap 시 300s 초과→gRPC DEADLINE. 스캔 전 NETWORK_MOUNT_PROBE로 빠른 skip
4. 🟡 **MinIO 클라이언트 timeout/retry 튜닝** (boto3 Config connect/read timeout + adaptive retries)
- 인프라(코드 외): NAS 10.0.0.51 **1GbE** 링크에 MinIO+NFS 집중 → 대규모 QA 시 포화→flap. 10GbE 활성 또는 MinIO/NFS 트래픽 분리가 load 폭주 근본 해결

#### 사후 운영 finding (다음 QA 보강)
- ffmpeg subprocess stderr 가 [-300:] 로 잘려서 실제 에러 메시지 안 보임 (RuntimeError 도 reason 추가 [:180] 로 더 잘림). NVENC 같이 codec init 실패 케이스에서 진단이 어려웠음. 추후 stderr 캡처 ≥1500 chars 로 확대 검토.
- archive_move_timeout 300s 다발 (auto-recovery 동작하지만 WARNING 노이즈) — Option B (timeout 600s) + C (회복 케이스 INFO 강등) 적용 완료(PR #94).

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

### 시나리오 정의 (대기 중, 카테고리별)

> 위 §0.5 인덱스의 시나리오 3~23 정의. 새 시나리오 추가 시 인덱스 + 아래 적절 카테고리 둘 다 갱신.

#### 운영 정상 흐름 (4)
- [ ] **시나리오 7 — 연속 dispatch (queue 동작)**: 1번 끝나기 전에 다음 1번 시작 → run_coordinator queue 동작 + duckdb_writer tag concurrency 검증.
- [ ] **시나리오 10 — auto_bootstrap chunked 자동 dispatch**: `_DONE` 마커와 함께 incoming 에 폴더 던지고 (manual dispatch JSON 없이) `auto_bootstrap_manifest_sensor` 의 chunked manifest 생성 → 여러 작은 dispatch 자동 트리거 검증. `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK`, `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES`, manifest 분할 단위, 동시 in-flight 제한 측정.

#### 부하 / 동시성 (3)
- [ ] **시나리오 3 — GPU 동시 부하**: 동일 시각에 bbox dispatch × 3 (SAM3 GPU 경합).
- [ ] **시나리오 22 — NFS slot exhaustion 부하 테스트**: slot=2 vs 32 vs 128 환경에서 동시 100+ NFS rename — heartbeat / rpc_wait 빈도 비교.

#### 회귀 보호 (6)
- [ ] **시나리오 6 — race 회귀 (B6 fix)**: dispatch drop 직후 같은 폴더에 추가 파일 — auto_bootstrap 가 dispatch 중인 폴더 다시 picking up 안 하는지.
- [ ] **시나리오 8 — archive 잔여물 회귀 보호**: 의도적으로 `archive/test/` 에 N 파일 남긴 채 test dispatch → fast-path 비활성(existing_archive!=None) → per-file path 진입 동작 + wall time 측정 + 사전점검 § 2 단계 F 가 catch 하는지 검증.
- [ ] **시나리오 11 — cleanup race (LS task NoSuchKey)**: Stage 1-3 에서 매번 발견된 race. cleanup_stage 가 dispatch_requests.ls_task_status='created' 확인 후 진행하도록 sleep/poll 추가 + 회귀 검증.
- [ ] **시나리오 19 — NVENC fallback (Phase 3 Option C)**: NVENC re-encode 가 실패하는 input → `reencode_with_fallback` 의 in-place fallback 동작 + `video_metadata.reencode_reason` 에 `fallback:<Error>:<msg>` 적재 확인.
- [ ] **시나리오 20 — ls_task_create_sensor in-flight 회귀 (PR #87)**: 동시 dispatch 다수 (예: 5개) 동시 완료 시 sensor 가 1번만 fire 하는지 확인.

#### 분기 커버리지 (4)
- [ ] **시나리오 9 — auto_labeling_sensor backlog 자연소진**: 시나리오 1 Stage 4 처럼 limit 초과 backlog 100 정도 의도적 생성 + sensor ON 측정. 60s 간격 picking, cursor(event_seq) 정확 증가, in-flight 중복 트리거 없는지, throughput, 종료 시점(backlog=0) cursor 안정성. limit=100000 + sensor wiring 회귀 보호.
- [ ] **시나리오 12 — classification_video + vlm-classification 두 분기**:
  - **목적**: 미검증 분기. `classification_video` (Gemini 단일 클래스 분류) + `build_classification` (vlm-classification 카테고리별 원본 복사) 두 단계 모두 검증.
  - **dispatch JSON**: `labeling_method=["classification_video"]` 또는 `outputs=["classification_video"]` (spec 확인 필요)
  - **검증 산출물**:
    - PG `labels`: `label_format='video_classification_json'`, `caption_text=<predicted_class>` 행
    - MinIO `vlm-labels/<folder>/classification/<file>.json` 객체
    - (build 단계 후) MinIO `vlm-classification/<folder>/{video|image}/<category>/<file>` 원본 복사 (DB 미적재)
  - **단계 체크**:
    - [ ] `copy_to_incoming test` (20 mp4)
    - [ ] dispatch JSON outputs=`classification_video` + categories 5개
    - [ ] RUN_SUCCESS 후 `pg_sample test` 로 labels.label_format='video_classification_json' 확인
    - [ ] `mc ls prodR/vlm-labels/test/classification/` JSON 객체 + sample cat 으로 schema 검증
    - [ ] build_dataset 트리거 후 `mc ls --recursive prodR/vlm-classification/test/` 카테고리 폴더 구조 확인
- [ ] **시나리오 16 — post_review_clip_job 전체 흐름 (LS 검수 → clip 분할)**: LS UI 에서 timestamp 라벨 → `/sync-approve` 호출 → `post_review_clip_job` 트리거 → `clip_to_frame` 실행 → MinIO `vlm-processed/<folder>/clips/<file>_<start>_<end>.mp4` + frames 생성 검증.

#### 성능 측정 (3)
- [ ] **시나리오 5 — NVENC on/off A/B**: 같은 50 file 을 `REENCODE_USE_NVENC` env toggle 로 비교 (wall time, GPU 부하).
- [ ] **시나리오 13 — SAM3 workers=1/3/5 throughput 비교**: test 폴더 SAM3 step wall time + GPU memory 사용 시계열(nvidia-smi). workers 늘릴수록 throughput vs memory 증가율 plot.
- [ ] **시나리오 14 — dual SAM3 (GPU 0 + GPU 1)**: sam3-gpu0 + sam3-gpu1 두 컨테이너 띄우고 nginx upstream 또는 dagster client round-robin → throughput 2× 검증.
- [ ] **시나리오 24 — NVENC dual-GPU round-robin throughput**: 2026-05-22 PR 적용. dagster ffmpeg NVENC 가 GPU 0/1 round-robin (`REENCODE_NVENC_GPU_INDICES=0,1`). appdata 1247+ reencode 케이스 wall time 측정 vs prior (GPU 0 only) 비교. 기대: reencode bottleneck 부분 ~2× 빨라짐 (전체 wall 15-25% ↓ 추정). 검증: `docker logs docker-dagster-1 | grep 'reencode_encoder=h264_nvenc gpu='` 에서 gpu=0/1 양쪽 나오는지.

#### 장애 대응 / 외부 시스템 (5)
- [ ] **시나리오 4 — fail-forward**: 일부 손상 mp4 + 정상 mp4 혼합 → DB 미삽입 + archive 미이동 + jsonl 실패 로그만 기록 확인.
- [ ] **시나리오 17 — GCS download schedule**: 매일 04:00 KST `gcs_download_schedule` 실제 fire 시 동작 검증 (3개 버킷: source-c/source-b/source-a-rtsp) + `GCS_ZERO_BYTE_RETRIES` 동작.
- [ ] **시나리오 18 — ingest retry manifest**: ffmpeg empty_output 4회 retry 후 영구실패 → retry manifest 자동 생성 + jsonl failed log 확인.
- [ ] **시나리오 21 — archive _DONE marker 보류 회복**: 의도적으로 failed jsonl 만들고 dispatch → marker 안 만들어지는지, jsonl 정리 후 marker 다시 만들어지는지.

#### 운영 인프라 / 정리 (2)
- [x] **시나리오 15 — 컨테이너 EXDEV 회피 (incoming/archive 단일 bind mount)** ✅ **완료 (2026-05-27, PR #92)**. root cause: NAS dataset 은 이미 통합됐으나 docker-compose 가 `INCOMING_HOST_PATH`/`ARCHIVE_HOST_PATH` 를 각각 따로 bind-mount → 컨테이너 안 `/nas/incoming`·`/nas/archive` 별개 mount point → `os.rename` cross-mount = `[Errno 18]` EXDEV → archive_finalize per-file fallback(1250 file 3-4h). **fix**: 공통 부모 `${NAS_DATA_ROOT}` 단일 bind(`/nas/data`) + `INCOMING_DIR`/`ARCHIVE_DIR`/`MANIFEST_DIR`/`ARCHIVE_PENDING_DIR`/`DATAOPS_NAS_ROOTS` 를 `/nas/data/*` 로 재지정. GCS download dir 도 `INCOMING_DIR` 기준 derive. **검증**: staging qa_s15val + prod 둘 다 컨테이너 내부 `incoming→archive` os.rename 성공, 로그 `폴더 단위 아카이브 이동 완료`, archive_finalize start→done <1s (이전 per-file 20 file 27s). `/nas/datasets` CIFS 격리 유지(shadowing 없음).
- [ ] **시나리오 23 — dedup orphan 정리**: fast-path 활성 시 archive 에 남는 dedup orphan 파일을 별도 cleanup job 으로 정리. orphan 식별 쿼리 + 자동 삭제 dry-run + apply 검증.

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
