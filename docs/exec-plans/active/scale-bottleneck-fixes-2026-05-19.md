# 96GB 스케일 테스트 — 5대 병목 + 자산 batch-cap 발견

> **컨텍스트**: 2026-05-19 appdata (2500 mp4 / 96GB) prod QA 중 surface 된 병목들.
> **테스트 설정**: 1250개 timestamp/Gemini 경로 + 1250개 bbox/YOLO+SAM3 경로, 동시 dispatch.
> **결과**: 두 run 모두 RUN_SUCCESS (총 5h 7m 소요, 0 failures). 그러나 **B5/B6/B7 자산 cap 으로 인해 입력의 일부만 라벨링됨**.
> **진행 상태 (2026-05-20 기준 — 모두 완료)**:
> - ✅ **Phase 1 prod 배포** (PR #75, commit `f7c4f8e`): heartbeat 1/63s → 0/min 실측.
>   - B3 dagster.yaml code_servers + DAGSTER_GRPC_HEARTBEAT_TIMEOUT=60
>   - B4 archive_finalize ThreadPool (worker 8개, fast-path 100% 유지 — Codex 발견으로 95% 완화 철회)
>   - B5 dispatch 자산 limit 200/1000 → 100000
>   - B2 INGEST_UPLOAD_WORKERS 4 → 16
>   - B1 INGEST_REENCODE_WORKERS 3 → 6, THREADS=4
> - ✅ **Phase 2 prod 배포** (PR #76, commit `c071ddb`): NVENC 코드 + MINIO env.
>   - B1 lib/video_reencode.py 에 NVENC 옵션 (`-no-scenecut 1`, `-tune ll` 제거)
>   - B2 MINIO_MULTIPART_CHUNK_MB=16 + MINIO_UPLOAD_MAX_CONCURRENCY=16 env
>   - `minio.py` Field default 변경은 user/linter 원복 → env 만으로 처리
> - ✅ **Phase 3 prod 배포** (PR #77): YOLO Field default 500 → 100000 (Codex 누락 발견 보강) + `detection_sensor_factory` 에 `asset_name`/`sensor_limit_env`/`default_sensor_limit` 파라미터 + RunRequest 에 `run_config.ops.<asset>.config.limit` 주입 (sensor backlog override) + YOLO 비활성 정책 (sensor env-gate, ENABLE_YOLO_DETECTION=false).
> - ✅ **Phase 4 prod 배포** (PR #78): B6 race condition fix — `auto_bootstrap_manifest_sensor` 가 dispatch processing 중 폴더 picking up. `_load_dispatch_requested_folders()` 가 이제 `.dispatch/pending/` + `.dispatch/processed/` 둘 다 읽어 excluded set 구성.
> - ✅ **NVENC prod 활성**: `REENCODE_USE_NVENC=true` 적용 (2026-05-20). staging 30 file 검증에서 0 failures, ffmpeg empty_output fallback 2회 자동 retry 회복, NVENC 호환성 OK.
> - ✅ **appdata 검증 데이터 cleanup**: PG/MinIO/archive 모든 appdata-prefix 데이터 wiped (97.3GB 해방, 원본 `/appdata` 2500 파일 보존).
>
> **보류된 후속 항목**: Re-encode 비동기 분리 (NVENC 활성으로 효과 도달), DockerRunLauncher (heartbeat 안정화로 우선순위 ↓), mc CLI 전환 (TB 스케일 시 검토), archive fast-path 0.95 완화 (Codex 발견 orphan 위험).

## 핵심 발견 요약

| # | 병목 | 위치 | 영향 |
|---|---|---|---|
| 1 | ffmpeg 강제 재인코딩 (keyframe_ratio<0.02) | `lib/video_reencode.py:45` | 5–10초/파일 × 2500 = 4–7h CPU |
| 2 | MinIO upload 동시성 (UPLOAD_WORKERS=4) | `defs/ingest/ops_common.py:11` | ~5h 총 (2 run 경합) |
| 3 | gRPC heartbeat-timeout=20s | Dagster CLI 기본 | 매 63초 grpc 재시작 (298회 over 6h) |
| 4 | archive_finalize 단일 스레드 + folder fast-path skip | `defs/ingest/archive_finalize.py:229` + `:194` | ~2h 추가 (1247-file run 기준) |
| **5** | **🚨 자산-수준 batch cap (1250 input → 일부만 처리)** | `defs/process/assets.py:32,55,84` + `defs/sam/detection_assets.py:39` | **bbox: SAM3 200/9648 frames (2%), ts: clip_timestamp 50–200/1249 videos** |

### 측정된 종료 시각 (실측)

| 단계 | bbox 89368925 | ts cea7df46 |
|---|---|---|
| raw_ingest entered | 05:48:50 UTC | 05:48:51 UTC |
| archive_finalize:start | 07:41:31 (1h 52m) | 08:01:26 (2h 12m) |
| raw_video_to_frame 완료 | 10:21:33 (4h 32m) | SKIPPED |
| dispatch_sam3_image_detection START | 10:21:37 | SKIPPED |
| Gemini 요청 시작 | SKIPPED | 09:54:31 |
| clip_timestamp STEP_SUCCESS | 62ms (skip) | 09:58:53 (4m24s, 50 videos) |
| **RUN_SUCCESS** | **10:24:50** (**4h 36m**) | **09:58:59** (**4h 10m**) |

### 최종 산출물
- bbox: 1247 raw_files completed, **9648 frames extracted**, **200 SAM3 image_labels** (225 detections total)
- ts: 1249 raw_files completed, **38 Gemini label events** (auto/completed)
- 0 step failure, 0 traceback
- 298 heartbeat warnings (B3 confirmed)

---

## 측정 데이터 요약

| 단계 | 처리량 | 1250파일 ETA | 비고 |
|---|---|---|---|
| video_meta_extract (ffprobe + reencode check) | ~18 file/min | ~70 min | ffmpeg 재인코딩이 dominant |
| MinIO upload (단일 run) | ~0.21 obj/sec | ~100 min | UPLOAD_WORKERS=4 기본값 |
| MinIO upload (2 run 경합) | ~0.05 obj/sec/run | ~250 min | 워커풀이 절반씩 나뉨 |
| **archive_finalize (per-file)** | **~10 file/min** | **~125 min** | 단일 스레드 NFS rename loop |
| gRPC heartbeat | timeout=20s | 매 63초 재시작 | ffmpeg CPU 점유 시 starvation |

**관측 시간대**:
- 14:48 KST dispatch
- 15:25 KST 첫 raw_files insert (bbox)
- 16:41 KST bbox upload 1247/1247 완료 → archive_finalize 시작
- 16:50 KST bbox archive 70/1247 진행, ts upload 1025/1249 진행
- 측정된 archive rate 10/min (1h+ 더 필요할 전망)

---

## 📌 병목 #1: 모든 appdata 파일에 강제 ffmpeg 재인코딩

### 원인 정밀화
**위치**: `src/vlm_pipeline/lib/video_reencode.py:45`
```python
KEYFRAME_RATIO_THRESHOLD = 0.02  # 하드코딩
```
appdata mp4 들은 keyframe_ratio = **0.0067** → 임계값 0.02 미달 → 재인코딩 강제.

| 검사 항목 | appdata 결과 | 통과? |
|---|---|---|
| codec h264 | h264 | ✅ |
| profile baseline | baseline | ✅ |
| no b-frames | (통과 추정) | ✅ |
| level ≤ 42 | (통과 추정) | ✅ |
| **keyframe_ratio ≥ 0.02** | **0.0067** | ❌ → 재인코딩 |

재인코딩 사양: `-preset veryfast`, GOP 30, baseline H.264 (CPU only). 약 5–10초/파일 × 2500 = **약 4–7시간 총 CPU 시간**.

### 해결 방안

| 방안 | 작업량 | 효과 | 위험 |
|---|---|---|---|
| **A. NVENC GPU 인코딩 도입 (권장)** | 중 (1–2일) | **10–20× 가속** (5초→0.3초/파일) | GPU 점유 — YOLO/SAM3와 경합 시 timeline 충돌 |
| **B. KEYFRAME_RATIO_THRESHOLD 완화** | 소 (10분) | appdata 류 skip → 100% 시간 절약 | downstream cv2 seek/RTSP 송출 호환성 ↓ |
| **C. Re-encode 비동기 분리** | 대 (1주) | raw_ingest unblock, downstream만 차단 | 아키텍처 변경 |
| **D. 병렬도 증가 (즉시)** | 소 (env 1개) | 3→8 ffmpeg 동시 → CPU bound 한계까지 가속 | 다른 컨테이너 CPU 굶주림 가속 (#3 악화) |

#### 즉시 적용 가능: 방안 D (env 튜닝)
```bash
# docker/.env 에 추가
INGEST_REENCODE_WORKERS=6      # 현재 3 → 6 (CPU 코어에 따라)
INGEST_REENCODE_THREADS=4      # ffmpeg -threads 4
```
- 코드 위치: `src/vlm_pipeline/defs/ingest/ops_common.py` — `DEFAULT_REENCODE_WORKERS`, `DEFAULT_REENCODE_THREADS`

#### 권장 본질해법: A (NVENC) — 코드 변경
```python
# lib/video_reencode.py
STANDARD_PRESET_FFMPEG_ARGS_NVENC = [
    "-c:v", "h264_nvenc",           # GPU H.264 인코더
    "-profile:v", "baseline",
    "-level", "4.2",
    "-pix_fmt", "yuv420p",
    "-g", "30", "-keyint_min", "30",
    "-bf", "0",                     # no B-frames
    "-preset", "p4",                # NVENC 프리셋 (p1=fastest, p7=slowest/best)
    "-movflags", "+faststart",
    "-c:a", "aac", "-b:a", "128k",
]
# 사용 분기: env REENCODE_USE_NVENC=true 시 위 args 사용
```
NVENC 사전 확인:
```bash
docker exec docker-dagster-daemon-1 sh -c "ffmpeg -hide_banner -encoders 2>&1 | grep nvenc"
# h264_nvenc 가 listing 되어야 함 + nvidia driver + nvidia-container-toolkit 필요
```

### 권장 단계
1. **즉시** `INGEST_REENCODE_WORKERS=6` 설정 → 다음 dispatch 부터 적용 (10분)
2. **이번 주** NVENC 도입 검토 + 작은 샘플로 출력 호환성 검증
3. **다음 sprint** Re-encode 비동기화 (방안 C) — 본질적 분리

---

## 📌 병목 #2: MinIO upload 처리량 ~0.2 obj/sec

### 원인 정밀화
**위치**: `src/vlm_pipeline/defs/ingest/ops_common.py:11–12`
```python
DEFAULT_UPLOAD_WORKERS: int = 4    # 기본 4
MAX_UPLOAD_WORKERS:     int = 16   # 상한 16
```
`INGEST_UPLOAD_WORKERS` env 미설정 → 기본 4 사용. 측정된 0.21 obj/sec ≈ 0.05 obj/sec/worker = **20초/파일** (NAS 읽기 → 네트워크 → MinIO PUT 합산, 38MB/파일).

NFS 읽기 (10.0.0.51) + MinIO PUT (10.0.0.36) = 2hop **direct** 패턴이라 raw bandwidth 한계 가능성.

### 해결 방안

| 방안 | 작업량 | 효과 | 위험 |
|---|---|---|---|
| **A. INGEST_UPLOAD_WORKERS=16 (즉시)** | 소 (env 1개) | 이론적 4×, 실측 2–3× 기대 | MinIO 동시 connection 한계 — 보통 256/노드라 16은 안전 |
| **B. MinIO TransferConfig 튜닝** | 소 (코드 5줄) | multipart_chunksize 8→16MB, max_concurrency 10→20 | 대용량 파일에서만 효과 |
| **C. mc cp -r 외부 호출** | 중 (1일) | mc batch 모드, 50–100× 빠름 (Go binary, 멀티스트림) | 진행상황 추적 어려움, 에러처리 복잡 |
| **D. NAS→MinIO 직접 전송** | 대 | hop 1번 줄임 | MinIO/NAS 같은 LAN 인프라 변경 |

#### 즉시 적용: 방안 A
```bash
# docker/.env 추가
INGEST_UPLOAD_WORKERS=16
```

#### 함께 적용 권장: 방안 B 코드 패치
`src/vlm_pipeline/resources/minio.py:60` 의 `TransferConfig`:
```python
# 현재 추정
multipart_threshold = 8 * 1024 * 1024
multipart_chunksize = 8 * 1024 * 1024
max_concurrency = 10
# 권장
multipart_threshold = 16 * 1024 * 1024  # 38MB 파일에 멀티파트 활용
multipart_chunksize = 16 * 1024 * 1024
max_concurrency = 20
use_threads = True
```

### 권장 단계
1. **즉시** `INGEST_UPLOAD_WORKERS=16` → 효과 확인 (5분)
2. **여전히 느리면** minio.py TransferConfig 튜닝
3. **96GB→TB 스케일 가면** mc CLI 호출로 전환

---

## 📌 병목 #5 (가장 중대): 자산-수준 batch cap → 대형 dispatch 미완 처리

### 원인 정밀화
모든 후처리 자산이 sensor-driven backlog 모델에 맞춰 **op_config `limit`** 으로 하드코딩된 batch cap 을 가진다. dispatch 한 번에 1250 input → 자산 한 번에 N 만 처리. 나머지는 다음 sensor tick 을 기다림.

| 자산 | 위치 | default limit | appdata 1250 vs cap |
|---|---|---|---|
| `raw_video_to_frame` (raw_video 모드) | `defs/process/assets.py:55,84` + `raw_frames.py:49`, `frame_extract_mvp.py:60`, `frame_extract_routed.py:96` | **1000** | 1247 input → 1000 frame extract (247 미처리) |
| `dispatch_sam3_image_detection` | `defs/sam/detection_assets.py:39,97` | **200** (frames!) | 9648 extracted → SAM3 200 frames (98% 미처리) |
| `clip_timestamp` | `defs/process/captioning.py:45` (`limit=200`) — 그러나 실측 50개 처리 (filter 단계서 추가 제한) | **200** | 1249 input → 200 attempted, 50 processed |
| `clip_captioning` | `defs/process/captioning.py:182` | **200** | (해당 테스트선 fast-skip) |
| `sam3_shadow_compare` | `defs/sam/assets.py:35` | **200** | sensor-driven backlog only |

### 왜 이렇게 설계되었는가
원래 가정: **sensor 가 매 tick 마다 batch 를 fetch → 자산 limit 만큼 처리 → 자동 재발화 (다음 tick)**. 백로그 sensor 가 매 60s 마다 돌면서 batch by batch 처리. dispatch 가 도입되며 "**one-shot** 으로 1250개를 한 번에 처리" 시나리오에는 부적합.

### 영향 시나리오
appdata 1250 video bbox dispatch:
- 1 run 으로 → SAM3 가 200 frames 만 검출 → 220개 image_labels
- 전체 9648 frames 검출하려면 → **9648 / 200 = ~50번 run 재시작** 필요
- 또는 sensor 가 잔여 frame 발견 시 자동 trigger 되어야 하는데, dispatch flow 는 sensor 와 분리되어 있음

### 해결 방안

| 방안 | 작업량 | 효과 | 위험 |
|---|---|---|---|
| **A. dispatch 시 op_config 로 limit override** | 소 (dispatch JSON 1줄) | 즉시 적용 가능, code 변경 0 | dispatch JSON 작성자가 limit 인지해야 |
| **B. dispatch 자산의 limit default 를 매우 크게 (e.g. 100000)** | 소 (코드 5줄) | 모든 dispatch 가 input 전체 처리 | sensor backlog 모드도 영향받음 — 별도 자산으로 분리 필요 |
| **C. dispatch 와 sensor backlog 자산 분리** | 중 (1주) | dispatch 전용 자산 = no limit, sensor 자산 = limit 유지 | 자산 graph 복잡도 ↑ |
| **D. dispatch 가 자동으로 자산 재실행 loop** | 대 (2주) | sensor 와 동일한 백로그 처리, code 단순 | dispatch 시 wall time 매우 길어짐 (한 번에 5h+) |

#### 즉시 적용: 방안 A (dispatch JSON 에 op_config)
```json
{
  "request_id": "appdata_qa_bbox_20260519_v2",
  "folder_name": "appdata_safety_bbox_20260519_qa_v2",
  "labeling_method": ["bbox"],
  "categories": ["fire", "smoke", "weapon", "violence", "falldown"],
  "op_config": {
    "raw_video_to_frame_raw": {"limit": 5000},
    "dispatch_sam3_image_detection": {"limit": 50000}
  }
}
```
※ op_config 가 dispatch 페이로드에서 자산까지 전달되는지 코드 확인 필요. 안 된다면 방안 B 적용.

#### 권장 본질해법: B + C
1. **방안 B 즉시 (한 시간 작업)**: dispatch 진입 자산 (`raw_video_to_frame`, `dispatch_sam3_image_detection`, `clip_timestamp` dispatch path) 의 limit default 를 100000 으로.
2. **방안 C 다음 sprint**: sensor backlog 자산 (`yolo_image_detection`, `sam3_image_detection`, `sam3_shadow_compare`) 와 dispatch 자산을 명시적 분리 — 다른 op_config 정책.

### 권장 단계
1. **이번 주** dispatch 자산 limit default 상향 (B) — 코드 PR
2. **이번 주** op_config 전달 경로 확인 (A 보조)
3. **다음 sprint** dispatch/sensor 자산 분리 (C)

---

## 📌 병목 #4 (신규): archive_finalize 단일 스레드 NFS rename loop

### 원인 정밀화
**위치**: `src/vlm_pipeline/defs/ingest/archive_finalize.py:229`
```python
for idx, item in enumerate(uploaded, 1):
    _move_single_file(context, item, unit_archive_dir, ...)
```
1247개 파일을 순차적으로 `shutil.move` (= NFS rename) — 측정 10/min ≈ **6초/파일**.

#### 폴더 단위 fast-path 가 작동 안 함
`archive_finalize.py:190-195` 에는 폴더 자체를 rename 하는 fast path 가 있음:
```python
if (
    not is_chunked_manifest
    and source_unit_path
    and source_unit_path.exists()
    and len(uploaded) >= source_unit_total_file_count  # ← 여기서 막힘
    and existing_archive is None
):
    _move_with_timeout(str(source_unit_path), str(base_unit_archive_dir))
```
appdata 의 경우 **uploaded=1247 < total_file_count=1250** (3개가 dedup/error 로 누락) → fast path skip → 1250-file 폴더를 1247번 per-file move 로 처리 (느림).

### 해결 방안

| 방안 | 작업량 | 효과 | 위험 |
|---|---|---|---|
| **A. archive_move 병렬화 (ThreadPool)** | 소 (10줄) | 10-20× 가속 (6초→0.3-0.5초/파일) | NFS metadata 동시성 한계 — 너무 많으면 server overload |
| **B. fast-path 조건 완화** | 소 (조건문 1줄) | 폴더 단위 1회 rename = 즉시 완료 | 누락된 3개 파일이 archive 폴더에 남아있게 됨 — cleanup 로직 필요 |
| **C. partial-uploaded 폴더는 mv 명령 shell 호출** | 중 | shell `mv` 가 shutil.move 보다 빠르며 글로빙 지원 | 에러 처리 복잡 |

#### 즉시 적용: 방안 A
```python
# archive_finalize.py 의 sequential loop를 ThreadPoolExecutor 로:
from concurrent.futures import ThreadPoolExecutor

archive_workers = int(os.getenv("INGEST_ARCHIVE_WORKERS", "8"))
with ThreadPoolExecutor(max_workers=archive_workers) as pool:
    futures = {
        pool.submit(_move_single_file, context, item, unit_archive_dir, ...): item
        for item in uploaded
    }
    for idx, fut in enumerate(as_completed(futures), 1):
        item = futures[fut]
        try:
            fut.result()
        except Exception as exc:
            _mark_archive_result(item, None, f"archive_move_failed:{exc}")
        if idx == 1 or idx == total or idx % 50 == 0:
            context.log.info(f"archive progress={idx}/{total} success={len(archived_items)}")
```

#### 함께 적용 권장: 방안 B (fast-path 완화)
```python
# 변경 전
and len(uploaded) >= source_unit_total_file_count
# 변경 후
and len(uploaded) >= source_unit_total_file_count * 0.95  # 95% 이상이면 폴더 단위 이동
```
또는: 폴더 단위 mv 후 남은 dedup 파일들은 `__leftover` 디렉토리로 별도 cleanup.

### 권장 단계
1. **즉시** 환경변수 `INGEST_ARCHIVE_WORKERS` 도입 (env-only 기본 8) — 코드 변경 PR
2. **이번 주** fast-path 조건 완화 + leftover cleanup 로직
3. **다음 sprint** `mv` shell 호출 대안 검토

---

## 📌 병목 #3: gRPC heartbeat-timeout=20s 너무 짧음

### 원인 정밀화
**위치**: Dagster CLI 기본값 — `dagster api grpc --heartbeat-timeout 20` (코드 안에 별도 설정 없음).

관측: ffmpeg CPU 점유 시 (`docker-dagster-daemon-1` CPU 147%) gRPC 응답 지연 → 매 63초마다 코드서버 grpc 서브프로세스 재시작.
```
2026-05-19 06:19:51 - dagster.code_server WARNING - No heartbeat received in 20 seconds, shutting down
2026-05-19 06:20:54 - dagster.code_server WARNING - No heartbeat received in 20 seconds, shutting down
... (매 63초)
```

영향: 진행 중인 run 자체는 안 죽음 (워커 detached) — 그러나:
- sensor evaluation 일시 실패 (다음 tick 까지 회복)
- gRPC subprocess fork 비용 누적
- 더 큰 스케일에서는 실제 run 종료까지 갈 수 있음

### 해결 방안

| 방안 | 작업량 | 효과 | 위험 |
|---|---|---|---|
| **A. dagster.yaml `code_servers` 설정** | 소 (5줄) | heartbeat 60–90s 로 늘림 | 코드 변경 후 데몬 재시작 필요 |
| **B. compose CMD 에 직접 플래그** | 소 | 즉시 적용 | Dagster 업그레이드 시 호환성 이슈 |
| **C. ffmpeg를 별도 컨테이너로 분리** | 대 (1–2주) | 근본 해결 — 데몬 CPU 보호 | 아키텍처 변경, 분산 실행 모델 도입 |
| **D. 데몬 컨테이너 CPU limit 상향** | 소 (compose 1줄) | CPU starvation 완화 | 호스트 자원 양보 필요 |

#### 즉시 적용: 방안 A
`docker/app/dagster_home/dagster.yaml` 에 추가:
```yaml
code_servers:
  local_startup_timeout: 120
  reload_timeout: 120
```
환경 변수 방식 (Dagster 1.6+ 지원):
```bash
# docker/.env 추가
DAGSTER_GRPC_TIMEOUT_SECONDS=60
DAGSTER_CODE_SERVER_RPC_TIMEOUT=60
```

#### 본질 해법: 방안 C — `run_launcher` 변경
현재 `DefaultRunLauncher` → 같은 컨테이너 fork. `DockerRunLauncher` 로 바꾸면 ffmpeg 워커가 별도 컨테이너로 분리.

```yaml
# dagster.yaml
run_launcher:
  module: dagster_docker
  class: DockerRunLauncher
  config:
    image: datapipeline:gpu-cu124
    network: pipeline-network
    container_kwargs:
      auto_remove: true
```

### 권장 단계
1. **즉시** dagster.yaml에 timeout 상향 (10분 작업)
2. **다음 dispatch 부터 확인** — heartbeat 경고 사라지는지
3. **중장기** DockerRunLauncher 도입 검토

---

## 🗺️ 통합 실행 순서 (권장)

### Phase 1: 즉시 (오늘, 1h, 위험도 ⭐)
```bash
# docker/.env 추가
INGEST_UPLOAD_WORKERS=16
INGEST_REENCODE_WORKERS=6
INGEST_REENCODE_THREADS=4
INGEST_ARCHIVE_WORKERS=8         # B4 (코드 패치 선반영 필요)
DAGSTER_GRPC_HEARTBEAT_TIMEOUT=60
```
+ `dagster.yaml` 에 `code_servers.reload_timeout: 120` 추가
+ `archive_finalize.py` ThreadPoolExecutor 패치 (B4 해법 A)
+ `docker compose restart dagster-daemon dagster-code-server`

**기대 효과**:
- re-encode 시간 50% ↓
- upload 2-3× ↑
- archive 10× ↑ (10/min → 100+/min)
- heartbeat 안정화

**검증**: 새 dispatch 후 5분 내
- `docker logs docker-dagster-daemon-1 --since 5m | grep "upload progress"` — rate 확인
- `docker logs docker-dagster-daemon-1 --since 5m | grep heartbeat` — 0개여야 함

### Phase 2: 이번 주 (1–2일, 위험도 ⭐⭐)
- `minio.py` TransferConfig 튜닝 (PR)
- NVENC 옵션 코드 추가 + flag-off 상태로 머지 (`REENCODE_USE_NVENC=false` 기본)
- 작은 샘플(50개)로 NVENC 출력 호환성 검증 → flag-on 전환

**기대 효과**: re-encode 시간 80% ↓ (NVENC 적용시)

**검증**: NVENC on/off A/B 테스트 — 같은 50개 파일에 대해 wall time 비교, 출력 파일 cv2 seek 동일성 확인

### Phase 3: 다음 sprint (1–2주, 위험도 ⭐⭐⭐)
- Re-encode를 raw_ingest 에서 분리 → `reencode_job` 별도 dagster job
- DockerRunLauncher 도입 검토
- MinIO 동시성 한계 측정 (필요시 mc 호출 전환)

**기대 효과**: raw_ingest 차단 제거 → ingest 30분 내 종료, 라벨링 단계 즉시 시작 가능

---

## 📊 ROI 매트릭스

| 방안 | 작업량 | 효과 | 적용 시점 |
|---|---|---|---|
| **🚨 dispatch 자산 limit default 상향 (B5)** | 1시간 | **★★★★★** (대형 dispatch 가 비로소 가능) | **즉시 (최우선)** |
| **INGEST_UPLOAD_WORKERS=16** (B2) | 1분 | ★★★★ | 즉시 |
| INGEST_REENCODE_WORKERS=6 (B1) | 1분 | ★★★ | 즉시 |
| **archive_finalize 병렬화** (B4) | 30분 | ★★★★ | 즉시 |
| dagster.yaml heartbeat (B3) | 5분 | ★★★ | 즉시 |
| archive fast-path 완화 (B4) | 30분 | ★★★★ | 이번 주 |
| MinIO TransferConfig (B2) | 30분 | ★★ | 이번 주 |
| NVENC 도입 (B1) | 1일 | ★★★★★ | 이번 주 |
| op_config dispatch→asset 전달 검증 (B5 보조) | 1일 | ★★★ | 이번 주 |
| Re-encode 분리 (B1) | 1주 | ★★★★ | 다음 sprint |
| **dispatch/sensor 자산 분리 (B5)** | 1주 | ★★★★ | 다음 sprint |
| DockerRunLauncher (B3) | 1–2주 | ★★★ | 필요시 |

---

## 부록 A — 관련 코드 위치 빠른 인덱스

| 항목 | 파일 | 라인 |
|---|---|---|
| Re-encode 임계값 (B1) | `src/vlm_pipeline/lib/video_reencode.py` | `KEYFRAME_RATIO_THRESHOLD` (45) |
| Re-encode FFmpeg 인자 (B1) | `src/vlm_pipeline/lib/video_reencode.py` | `STANDARD_PRESET_FFMPEG_ARGS` (29–42) |
| Re-encode 판정 함수 (B1) | `src/vlm_pipeline/lib/video_reencode.py` | `needs_reencode()` (96–138) |
| Upload worker 기본값 (B2) | `src/vlm_pipeline/defs/ingest/ops_common.py` | `DEFAULT_UPLOAD_WORKERS` (11), `MAX_UPLOAD_WORKERS` (12) |
| Re-encode worker 기본값 (B1) | `src/vlm_pipeline/defs/ingest/ops_common.py` | `DEFAULT_REENCODE_WORKERS`, `DEFAULT_REENCODE_THREADS` |
| Upload 루프 (B2) | `src/vlm_pipeline/defs/ingest/ops_normalize.py` | `_execute_upload` 주변 (~516) |
| MinIO TransferConfig (B2) | `src/vlm_pipeline/resources/minio.py` | `__init__` 의 transfer config (60) |
| **archive 단일 스레드 loop (B4)** | `src/vlm_pipeline/defs/ingest/archive_finalize.py` | 229–233 |
| **archive folder fast-path 조건 (B4)** | `src/vlm_pipeline/defs/ingest/archive_finalize.py` | 190–199 |
| **archive_move shutil wrapper (B4)** | `src/vlm_pipeline/defs/ingest/archive_move.py` | 23–35 (`_move_with_timeout`) |
| Dagster 설정 (B3) | `docker/app/dagster_home/dagster.yaml` | (전체) |
| Daemon CMD (B3) | `docker/docker-compose.yaml` | `dagster-daemon` 서비스 |

---

## 부록 B — Phase 1 실행 체크리스트

### 코드 변경 (PR 필수)
- [ ] `archive_finalize.py` ThreadPoolExecutor 패치 — `INGEST_ARCHIVE_WORKERS` env 도입 (B4 해법 A)

### Env / 설정 변경 (호스트 직접)
- [ ] `docker/.env` 백업: `cp docker/.env docker/.env.bak.scale-fix-$(date +%Y%m%d)`
- [ ] 5개 env 추가
  - `INGEST_UPLOAD_WORKERS=16` (B2)
  - `INGEST_REENCODE_WORKERS=6` (B1)
  - `INGEST_REENCODE_THREADS=4` (B1)
  - `INGEST_ARCHIVE_WORKERS=8` (B4)
  - `DAGSTER_GRPC_HEARTBEAT_TIMEOUT=60` (B3)
- [ ] `docker/app/dagster_home/dagster.yaml` 백업 + `code_servers.reload_timeout: 120` 추가 (B3)
- [ ] PG 백업 (혹시 모를 변경): `pg_dump`

### 재기동 + 검증
- [ ] `./scripts/compose-prod.sh build app` (코드 변경 반영)
- [ ] `./scripts/compose-prod.sh up -d --force-recreate dagster dagster-code-server dagster-daemon`
- [ ] `docker logs docker-dagster-daemon-1 -f` 로 startup 확인 (30s)
- [ ] `curl -sf http://10.0.0.10:3030/server_info` → 200 확인

### 효과 측정
- [ ] 새 appdata dispatch (작은 샘플 100개 권장) → 측정 4가지
  - upload rate (목표 ≥0.5 obj/sec)
  - archive rate (목표 ≥100 file/min)
  - heartbeat warning count (목표 0/min)
  - 100-file run wall time (목표 ≤10분)
- [ ] 결과 본 md 부록 C 에 추가 기록

---

## 부록 C — 향후 측정 지표 (재현 시 비교용)

### Phase 0, 미적용 baseline (관측된 실측치)
- ffmpeg 동시: **3**
- upload worker: **4** (기본)
- archive worker: **1** (단일 스레드 loop)
- heartbeat-timeout: **20s**
- video_meta_extract rate: **18 file/min** (ts run, ffmpeg keyframe probe + reencode 판정)
- upload rate (단일 run): **0.21 obj/sec**
- upload rate (2 run 동시): **0.05 obj/sec/run** (워커풀 분배)
- archive rate: **10 file/min** = 0.17 file/sec (NFS rename loop)
- heartbeat warning rate: **1/63s**
- bbox upload 완료 시점: dispatch +**1h 53m**
- ts upload 진행: dispatch +**2h 2m** 시점 82%

### Phase 1 적용 후 목표
- ffmpeg 동시: **6+**
- upload worker: **16**
- archive worker: **8** (병렬화)
- heartbeat-timeout: **60s**
- upload rate: **≥0.5 obj/sec** (2.5×)
- archive rate: **≥100 file/min** (10×)
- heartbeat warning: **0**
- ETA 단축: **8–10h → 2–3h** (2500-file 전체)

### NVENC 적용 후 목표
- re-encode/file: **5s → 0.3–0.5s**
- video_meta_extract: **70min → 10–15min**
- 총 wall time: **≤ 1h** (2500 파일)

### 측정 명령 모음
```bash
# upload rate (마지막 1분)
docker logs docker-dagster-daemon-1 --since 1m 2>&1 | grep -c "MinIO 업로드 완료"

# archive rate
docker logs docker-dagster-daemon-1 --since 1m 2>&1 | grep "archive progress" | tail -1

# heartbeat warnings
docker logs docker-dagster-daemon-1 --since 5m 2>&1 | grep -c "No heartbeat"

# PG 진행도
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
SELECT source_unit_name, ingest_status, COUNT(*)
FROM raw_files WHERE source_unit_name LIKE '<unit_prefix>%'
GROUP BY source_unit_name, ingest_status;"

# GPU 사용량
nvidia-smi --query-gpu=index,memory.used,utilization.gpu --format=csv,noheader
```
