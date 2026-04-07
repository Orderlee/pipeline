# 데이터 파이프라인 병목 분석 및 개선 방향

> 작성일: 2026-04-07  
> 대상: 데이터 파이프라인 담당자  
> 배경: 현재 파이프라인 1회 실행 시 약 1시간 소요 — 병목 구간 분석 및 개선 방향 정리

---

## 1. 현재 파이프라인 구조

```
Manifest Sensor (tick)
  ↓
raw_ingest          NAS → DuckDB(raw_files) + MinIO(vlm-raw)
  ↓
clip_timestamp      MinIO → Gemini(Vertex AI) → MinIO(vlm-labels) + DuckDB(auto_label_status)
  ↓
clip_to_frame       MinIO → ffmpeg 클립 추출 → MinIO(vlm-processed) + DuckDB(processed_clip_frame_metadata)
  ↓
yolo_detection      MinIO → YOLO server(HTTP) → DuckDB(image_labels)
  ↓
build_dataset
```

데이터가 4번 시스템 간 이동하며, 각 단계에서 순차 처리가 기본 구조로 되어 있음.

---

## 2. 병목 구간 요약

| 순위 | 구간 | 파일 | 현재 동작 | 예상 개선폭 |
|------|------|------|----------|------------|
| 🔴 1 | Gemini API 순차 호출 | `defs/label/timestamp.py` L40~140 | 50개 × 30~90s = 최대 75분 | **5~10x** |
| 🔴 2 | DuckDB writer 전체 직렬화 | `dagster.yaml` | 모든 asset이 1개씩 순차 쓰기 | **2~3x** |
| 🟠 3 | Video preview 재인코딩 순차 재시도 | `defs/label/label_helpers.py` L72~179 | 960px→640px→480px 순차 재시도 | 2x |
| 🟠 4 | 긴 영상 청크 처리 순차 | `defs/label/label_helpers.py` L181~254 | 660s 단위 청크를 순차 처리 | 2x |
| 🟡 5 | Ingest 낱개 INSERT | `defs/ingest/ops.py` L341 | 파일 1개 = DB write 1회 | 2~3x |
| 🟡 6 | YOLO batch_size 부족 | `defs/yolo/assets.py` L190 | 기본값 4 (GPU 여유 있음) | 1.5~2x |

---

## 3. 상세 분석

### 3-1. Gemini API 순차 호출 🔴

**현재 코드** (`timestamp.py` L40~140, `assets.py` L108~179):
```python
candidates = db.find_auto_label_pending_videos(limit=50)
for idx, cand in enumerate(candidates, start=1):
    video_path = materialize_video(minio, cand)          # MinIO 다운로드
    gemini_video_path = prepare_gemini_video(video_path) # ffmpeg 전처리
    response = analyzer.analyze_video(gemini_video_path) # ← 30~90s BLOCK
    upload_label(minio, response)
    db.update_auto_label_status(cand)
```

- `analyze_video()` 호출 시 Vertex AI 응답 올 때까지 완전 blocking
- 50개 순차 처리 → 이 구간만 최대 75분
- rate limit 429 발생 시 backoff(2s→4s→8s) 추가

**개선 방향**: `ThreadPoolExecutor`로 5개 동시 호출
```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def analyze_one(cand):
    video_path = materialize_video(minio, cand)
    gemini_video_path = prepare_gemini_video(video_path)
    response = analyzer.analyze_video(gemini_video_path)
    upload_label(minio, response)
    db.update_auto_label_status(cand)

max_workers = int(os.getenv("GEMINI_MAX_WORKERS", "5"))
with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = {pool.submit(analyze_one, cand): cand for cand in candidates}
    for fut in as_completed(futures):
        fut.result()
```

> Vertex AI 기본 quota 10 QPS → `GEMINI_MAX_WORKERS=5` 가 안전선.  
> quota 증설 신청 시 workers 늘릴 수 있음.

---

### 3-2. DuckDB writer 전체 직렬화 🔴

**현재 설정** (`docker/app/dagster_home/dagster.yaml` L7~15):
```yaml
run_coordinator:
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 4
    tag_concurrency_limits:
      - key: duckdb_writer
        value: "true"
        limit: 1    # ← 전 파이프라인에서 동시 쓰기 1개만
```

- `raw_ingest`(raw_files 테이블)와 `clip_timestamp`(gemini_labels 테이블)가 **서로 다른 테이블**을 쓰는데도 같은 태그로 묶여 순차 실행됨
- DuckDB는 파일 단위 write lock이라 완전히 피할 수는 없지만, **실제로 충돌하지 않는 테이블끼리는 분리 가능**

**개선 방향**: 테이블별 태그 분리

```yaml
tag_concurrency_limits:
  - key: duckdb_raw_writer      # raw_ingest 전용
    value: "true"
    limit: 1
  - key: duckdb_label_writer    # clip_timestamp, clip_to_frame 전용
    value: "true"
    limit: 1
  - key: duckdb_yolo_writer     # yolo_detection 전용
    value: "true"
    limit: 1
```

각 asset의 태그도 함께 변경 필요:
- `raw_ingest` → `duckdb_raw_writer`
- `clip_timestamp`, `clip_to_frame` → `duckdb_label_writer`
- `yolo_image_detection` → `duckdb_yolo_writer`

> 설정 파일 수정만으로 적용 가능. 변경 후 컨테이너 재시작 필요(1회).

---

### 3-3. Video Preview 재인코딩 순차 재시도 🟠

**현재 코드** (`label_helpers.py` L72~179):
```python
# 영상이 GEMINI_SAFE_VIDEO_BYTES(450MB) 초과 시
# 960px → 640px → 480px 순서로 ffmpeg 재인코딩 시도
for scale in [960, 640, 480]:
    result = ffmpeg_reencode(video, scale)  # 30~60s 소요
    if result.size < threshold:
        break
```

- 대용량 영상 1개에 최대 3회 × 30~60s = 최대 3분 소요
- 현재 `INGEST_REENCODE_WORKERS=3` 환경변수가 있지만 이 단계엔 미적용

**개선 방향**: Gemini 호출 전 preview 생성을 ThreadPoolExecutor로 선처리하거나, 첫 번째 시도 실패 시 즉시 다음 scale로 병렬 시도.

---

### 3-4. 긴 영상 청크 처리 순차 🟠

**현재 코드** (`label_helpers.py` L181~254):
```python
# 1시간 초과 영상 → 660s 윈도우, 600s stride로 청크 분할
for chunk in chunks:
    result = analyze_chunk(chunk)   # Gemini 호출, 순차
merge_overlapping_events(results)
```

**개선 방향**: 청크를 `ThreadPoolExecutor(max_workers=3)`으로 동시 처리 후 merge.

---

### 3-5. Ingest 낱개 INSERT 🟡

**현재 코드** (`ops.py` L341):
```python
for file in files:
    # ... 전처리 ...
    db.insert_raw_files_batch([db_record])  # 파일마다 1건씩 INSERT
```

**개선 방향**:
```python
records = []
for file in files:
    records.append(db_record)
    if len(records) >= 50:
        db.insert_raw_files_batch(records)
        records = []
if records:
    db.insert_raw_files_batch(records)
```

> DuckDB lock 획득/해제 횟수를 1/50로 줄임.

---

### 3-6. YOLO batch_size 🟡

**현재 코드** (`yolo/assets.py` L190):
```python
batch_size = int(context.op_config.get("batch_size", 4))  # 기본값 4
```

GPU 여유가 있다면 8~16으로 증가 시 HTTP round-trip 횟수 감소.  
환경변수 또는 op_config로 조정 가능.

---

## 4. 네트워크 / I/O 관점 추가 분석

### 데이터 이동 횟수 문제 (Chatty Pattern)

현재 `clip_timestamp` 기준 영상 1개당:
1. MinIO에서 영상 다운로드
2. Vertex AI 호출 (cross-region, us-central1)
3. MinIO에 label JSON 업로드
4. DuckDB write

→ 50개 영상이면 **150 round-trip 직렬**

**개선**: 위 3-1의 병렬화로 round-trip 횟수는 같지만 대기 시간을 겹쳐서 줄임.

### 프레임 소파일 문제

`clip_to_frame` 단계에서 JPEG 프레임을 MinIO에 개별 PUT:
```
vlm-processed/{clip_id}/frame_0001.jpg
vlm-processed/{clip_id}/frame_0002.jpg
...  (클립 1개당 수십~수백 장)
```

현재 규모에선 문제 없으나, 데이터 규모 커지면 MinIO metadata overhead 증가.  
**단, tar/parquet으로 변경 시 Label Studio presigned URL 방식과 충돌** → 보류.  
Dataset export 시점에만 패키징 적용 권장.

---

## 5. 개선 우선순위 및 적용 방법

### Phase 1 — 설정 변경만 (즉시 적용, 재시작 1회)

```
dagster.yaml DuckDB 태그 테이블별 분리
환경변수: YOLO batch_size 4 → 16
환경변수: INGEST_UPLOAD_WORKERS 4 → 8
```

### Phase 2 — 코드 수정 (Dagster UI Reload만으로 반영)

```
Gemini ThreadPoolExecutor (GEMINI_MAX_WORKERS=5)   ← 가장 임팩트 큼
긴 영상 청크 병렬화 (label_helpers.py)
Ingest bulk INSERT 50건 배치 (ops.py)
```

### Phase 3 — 중기

```
presigned URL renew → Dagster schedule 자동화 (현재 수동)
Gemini quota 증설 신청 → GEMINI_MAX_WORKERS 상향
```

> **컨테이너 재시작 필요 여부**  
> - `dagster.yaml`, `.env` 변경 → 재시작 필요  
> - `src/` 하위 Python 코드 수정 → Dagster UI Reload만으로 반영 (bind mount 구조)

---

## 6. 예상 효과 (Phase 1+2 적용 시)

| 현재 | 개선 후 |
|------|--------|
| ~60분 | **~10~15분** |

Gemini 병렬화(5~10x) + DuckDB 태그 분리(2~3x)만 적용해도 대부분의 병목 해소 가능.
