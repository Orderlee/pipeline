# 영상→이미지 추출 + YOLO-World 파라미터 테스트 결과 (Codex 서브에이전트 활용)

Codex MCP 서브에이전트(ai-engineer, data-engineer, data-scientist, ml-engineer, mlops-engineer, machine-learning-engineer) 관점에서 **영상 프레임 추출** 및 **YOLO-World** 파라미터 권장값을 정리한 테스트 결과 문서입니다.

- **작업 범위:** **이미지 추출** + **YOLO-World** 만 (Gemini·captioning·전체 staging dispatch 제외)
- **테스트 데이터:** `staging/tmp_data_2` 내 영상
- **흐름:** 영상 → ffmpeg 프레임 추출(JPEG) → (선택) YOLO-World-L detection

### 프리셋 이름이 의미하는 것 (interval 기반)

현재 벤치는 **`frame_interval_sec`**(N초당 1프레임) 기준입니다. `interval_2s_q90` = 2초당 1프레임 + JPEG 품질 90.

| 표기 | 파라미터 | 의미 |
|------|----------|------|
| **interval_Ns** | `frame_interval_sec` | N초마다 1프레임 균등 추출. **작을수록** 많이, **클수록** 적게 추출. |
| **q90 / q85** | `jpeg_quality` | JPEG 품질 90 또는 85. |

---

## 1. 파라미터 값별 정리

### 1.1 이미지 추출 (파이프라인 / `video_frames`)

| 파라미터 | 쓸 수 있는 값 | 의미·효과 |
|----------|---------------|-----------|
| **`frame_interval_sec`** | 예: `0.5`, `1.0`, `2.0` | **N초마다 1프레임** 균등 추출. **작을수록** 많이(예: 0.5→2fps), **클수록** 적게(예: 2→0.5fps). |
| **`jpeg_quality`** | `1` ~ `100` | JPEG 압축 품질. 운영 기본 **90**, 용량 절감 **85** 권장. |
| **`workers`** | 정수 `1` ~ (예: `4`) | 이미지 추출 **병렬 워커 수**(영상 단위). 늘리면 I/O 여유 있을 때 단축 가능. 디스크/NFS 병목 시 과다하면 지연될 수 있음. |

### 1.2 YOLO-World (HTTP API / `yolo_image_detection` config)

| 파라미터 | 쓸 수 있는 값 | 의미·효과 |
|----------|---------------|-----------|
| **`confidence_threshold` (conf)** | `0.2` ~ `0.3` (일반) | 검출 박스 **최소 신뢰도**. **낮을수록** 검출 수↑(노이즈↑), **높을수록** 확실한 것만(미검↑). 기본 **0.25**. |
| **`iou_threshold` (iou)** | `0.4` ~ `0.5` | NMS IoU. **낮을수록** 겹치는 박스 제거 완화(박스 수↑), **높을수록** 병합 강함. 기본 **0.45**. |
| **`batch_size`** | `4` \| `6` \| `8` | 한 번에 서버로 보내는 이미지 수(파이프라인). 서버 상한 **8** (`YOLO_MAX_BATCH`). **클수록** 호출 횟수↓, GPU 활용↑(메모리 여유 필요). |
| **`max_det`** (서버) | 기본 `300` | 이미지당 최대 검출 개수. 보통 **고정**. |
| **`imgsz`** (서버 env) | 기본 `640` | 입력 리사이즈. **고정 권장** (속도·정확도 균형). |
| **텍스트 클래스 (프롬프트)** | env `YOLO_DEFAULT_CLASSES` 또는 코드 기본 | YOLO-World에 넘기는 **클래스 이름 목록** (`set_classes`). staging 예: `person,car,...,dog,cat` (21개). |

### 1.3 벤치에서 고정한 YOLO 호출 조건 (실측 시간과 동일)

- **API:** `POST /detect/batch`, `conf=0.25`, `iou=0.45`, 배치 **8장**씩 전체 추출 이미지 순회.
- **환경:** 호스트 → `http://localhost:8001` (Docker `pipeline-yolo-1`).

---

## 2. 테스트 환경

| 항목 | 값 |
|------|-----|
| 영상 경로 | `staging/tmp_data_2` |
| 영상 파일 수 | **26개** (mp4 등) |
| 폴더 구성 | `fall_down`, `gs건설`, `smoking`, `violence`, `weapon` |
| 샘플 메타 (fall_down 1건) | 1920×1080, 30fps, duration ≈ 21.1초 |
| YOLO 모델 | YOLO-World-L (`yolov8l-worldv2.pt`), Docker 서비스 |
| 프레임 추출 | ffmpeg mjpeg, `video_frames.plan_frame_timestamps` + `extract_frame_jpeg_bytes` |

### 2.1 이미지 추출에 사용된 영상 파일 목록 (상세)

경로 기준: `staging/tmp_data_2` (아래는 상대 경로). **길이** = 재생 시간(초), **파일 크기** = MB.

| 상대 경로 | 재생 길이(초) | 해상도 | fps | 파일 크기(MB) |
|-----------|---------------|--------|-----|---------------|
| fall_down/20250711_AM_PK_FC_.mp4 | 21.1 | 1920×1080 | 30.0 | 80.00 |
| fall_down/20250711_AM_PK_FC_2.mp4 | 27.2 | 1920×1080 | 30.0 | 100.00 |
| fall_down/20250711_AM_PK_FC_3.mp4 | 21.0 | 1920×1080 | 30.0 | 36.74 |
| fall_down/20250711_AM_PK_FC_4.mp4 | 22.3 | 1920×1080 | 30.0 | 39.00 |
| fall_down/20250711_AM_PK_FC_5.mp4 | 22.7 | 1920×1080 | 30.0 | 39.65 |
| fall_down/20250711_AM_PK_FC_6.mp4 | 21.5 | 1920×1080 | 30.0 | 37.92 |
| fall_down/20250711_AM_PK_FC_7.mp4 | 20.9 | 1920×1080 | 30.0 | 34.14 |
| gs건설/평촌_자이2단지/우리병원앞/2026-02-20-132722_2026-02-20-170248_1000388$1$0$0.mp4 | 8371.4 | 1920×1080 | 15.0 | 1026.30 |
| gs건설/평촌_자이2단지/우리병원앞/2026-02-20-132722_2026-02-20-170248_1000388$1$0$0_1.mp4 | 4554.1 | 1920×1080 | 15.0 | 558.29 |
| smoking/20250814_AM_BE_DHC_1.mp4 | 21.8 | 1920×1080 | 30.0 | 80.00 |
| smoking/20250814_AM_BE_DHC_2.mp4 | 23.2 | 1920×1080 | 30.0 | 80.00 |
| smoking/20250814_AM_BE_DHC_3.mp4 | 20.9 | 1920×1080 | 60.0 | 80.00 |
| smoking/20250814_AM_BE_DHC_4.mp4 | 21.3 | 1920×1080 | 60.0 | 80.00 |
| smoking/20250814_AM_BE_VSB_1.mp4 | 21.1 | 1920×1080 | 60.0 | 80.00 |
| violence/20250718_AM_SM_TGO_1.mp4 | 20.7 | 1920×1080 | 30.0 | 80.00 |
| violence/20250718_AM_SM_TGO_2.mp4 | 20.8 | 1920×1080 | 30.0 | 80.00 |
| violence/20250718_AM_SM_TGO_3.mp4 | 20.8 | 1920×1080 | 30.0 | 80.00 |
| violence/20250718_AM_SM_TGO_4.mp4 | 20.5 | 1920×1080 | 30.0 | 80.00 |
| violence/20250718_AM_SM_TGO_5.mp4 | 20.3 | 1920×1080 | 30.0 | 80.00 |
| weapon/20250812_AM_IS_CD_BW_1.mp4 | 20.4 | 1920×1080 | 60.0 | 80.00 |
| weapon/20250812_AM_IS_CD_BW_2.mp4 | 20.9 | 1920×1080 | 60.0 | 80.00 |
| weapon/20250812_AM_IS_CD_BW_3.mp4 | 20.8 | 1920×1080 | 60.0 | 80.00 |
| weapon/20250812_AM_IS_CD_BW_4.mp4 | 26.2 | 1920×1080 | 60.0 | 80.00 |
| weapon/20250812_AM_IS_CD_BW_5.mp4 | 30.2 | 1920×1080 | 60.0 | 100.00 |
| weapon/20250812_AM_IS_CD_BW_6.mp4 | 20.6 | 1920×1080 | 30.0 | 80.00 |
| weapon/20250812_AM_IS_CD_BW_7.mp4 | 21.6 | 1920×1080 | 30.0 | 80.00 |

- **총 26개** 파일. **fall_down** 7개, **gs건설** 2개(장편: 약 2.3시간·1.3시간), **smoking** 5개, **violence** 5개, **weapon** 7개.
- 벤치에서 `--max-videos 6` 사용 시 위 목록 상위 6건(fall_down 6개)만 사용. 전체 26건 기준 추정치는 문서 내 표 참고.

---

## 3. 파라미터 정의 (상세 표)

### 3.1 프레임 추출 (clip_to_frame / raw_video_to_frame)

| 파라미터 | 설명 | 코드/벤치 | 테스트 범위 |
|----------|------|------------|-------------|
| `frame_interval_sec` | N초마다 1프레임 균등 추출 | 1.0 (1초당 1프레임) | 0.5, 1.0, 2.0 |
| `jpeg_quality` | JPEG 품질 (1–100) | 90 | 85, 90 |

### 3.2 YOLO-World (yolo_image_detection / Docker 서버)

| 파라미터 | 설명 | 코드/서버 기본값 | 테스트 범위 |
|----------|------|------------------|-------------|
| `confidence_threshold` (conf) | 검출 최소 신뢰도 | 0.25 | 0.2, 0.25, 0.3 |
| `iou_threshold` (iou) | NMS IoU 임계값 | 0.45 | 0.4, 0.45, 0.5 |
| `batch_size` | YOLO API 배치 크기 (이미지 수) | 4 | 4, 6, 8 |
| `max_det` (서버) | 이미지당 최대 검출 개수 | 300 | 300 (고정 권장) |
| `imgsz` (서버, env) | 추론 해상도 | 640 | 640 (고정 권장) |

---

## 4. 서브에이전트별 권장 파라미터

Codex 서브에이전트별로 “영상에서 이미지 추출 후 YOLO-World로 검출” 시 우선 고려할 값을 정리했습니다.

| 서브에이전트 | 추출 권장 | YOLO 권장 | 비고 |
|--------------|-----------|-----------|------|
| **ai-engineer** | interval_1s_q90, workers=2 | conf=0.25, iou=0.45, batch=4 | 기본선, 지연/품질 균형 |
| **data-engineer** | interval_2s_q90 또는 interval_1s_q85, workers=2 | conf=0.25, iou=0.45, batch=6 | 저장·처리량 균형 |
| **data-scientist** | interval_1s_q90(탐색), interval_0.5s_q90(고밀도) | conf=0.2, iou=0.45, batch=6 | 분석·실험용 |
| **ml-engineer** | interval_1s_q90 기준선 | conf=0.3, iou=0.5, batch=4 | 정밀도 우선 |
| **mlops-engineer** | interval_1s_q90 + workers=2, 고밀도는 별도 큐 | conf=0.25, iou=0.45, batch=8 | SLO·worker 상한 |
| **machine-learning-engineer** | interval_1s_q90 baseline, workers=2 고정 | conf=0.25, iou=0.45, batch=6 | 재현성·실험 축 분리 |

---

## 5. 테스트 시나리오 및 결과 요약

### 5.1 테스트 시나리오 (이미지 추출 + YOLO만)

1. **tmp_data_2** 영상으로 **이미지 추출만** 실행 (ffmpeg mjpeg, `video_frames.plan_frame_timestamps(..., frame_interval_sec=N)`).
2. 프리셋별로 **frame_interval_sec**, **jpeg_quality** 변경해 실행.
3. 지표: **추출 이미지 총 개수**, **추출 용량(MB)**, **추출 소요 시간**, (선택) **YOLO 처리 시간·총 검출 수**.

예시 (벤치 스크립트):

```bash
# 이미지 추출 + 용량·시간 측정 (YOLO 제외)
PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py --video-dir staging/tmp_data_2 --max-videos 6 --no-yolo

# YOLO 포함 시 (localhost:8001 서버 필요)
PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py --video-dir staging/tmp_data_2 --yolo-url http://localhost:8001

# worker 값별 비교 (추출 시간만, 1/2/4 병렬)
PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py --video-dir staging/tmp_data_2 --workers 1 2 4 --no-yolo
```

### 5.2 샘플 영상 기준 예상치 (참고)

- **샘플:** 1920×1080, 30fps, 약 21초. interval_2s → 약 11장, interval_1s → 약 22장, interval_0.5s → 약 43장.

### 5.3 권장 기본 조합 (interval 기반)

| 용도 | frame_interval_sec | jpeg_quality | workers | YOLO conf / iou / batch |
|------|--------------------|--------------|---------|--------------------------|
| **기본(운영)** | 1.0 | 90 | 2 | 0.25, 0.45, 4 |
| **적게 추출** | 2.0 | 90 | 2 | 0.25, 0.45, 4 |
| **많이 추출** | 0.5 | 90 | 1~2 | 0.2, 0.45, 6 |
| **용량 절감** | 1.0 | 85 | 2 | 0.25, 0.45, 8 |

---

## 6. 이미지 추출 시 발생 용량 + worker 비교 (실측)

`staging/tmp_data_2` 영상으로 **interval 기반** 이미지 추출 실측.  
(스크립트: `scripts/staging_video_extract_yolo_bench.py`, YOLO 사용 시: `conf=0.25`, `iou=0.45`, 배치 8장/요청)

### 6.1 interval 기반 추출 (1초당 1프레임) — 많이/적게 비교

**변경 사항:** 기본 이미지 추출이 **1초당 1프레임** 방식(`frame_interval_sec`)으로 변경됨. `plan_frame_timestamps(..., frame_interval_sec=N)` 사용 시 N초마다 1프레임 균등 추출.

### 6.1.1 프리셋 정의

| preset | frame_interval_sec | 의미 | 추출 밀도 |
|--------|--------------------|------|-----------|
| interval_2s_q90 | 2.0 | 2초당 1프레임 | **적게** 추출 |
| interval_1s_q90 | 1.0 | **1초당 1프레임** (기본) | 기본 |
| interval_0.5s_q90 | 0.5 | 0.5초당 1프레임 (2fps) | **많이** 추출 |
| interval_1s_q85 | 1.0 | 1초당 1프레임, JPEG 품질 85 | 기본 + 용량 절감 |

- 6개 영상(총 재생시간 약 135초) 기준: interval_2s → 약 68장, interval_1s → 약 136장, interval_0.5s → 약 272장 예상.

> **§6 실측 갱신 (2026-03-20):** 동일 호스트에서 `tmp_data_2`, `--max-videos 6`, `--no-yolo`, `scripts/staging_video_extract_yolo_bench.py` 로 재측정. **wall-clock(초)** 은 로컬 디스크·NFS·CPU에 따라 달라질 수 있음. 프레임 수·용량(MB)은 재현 시 동일해야 함.

### 6.2 실측 요약 (6영상, workers=1)

| preset | **추출 이미지 총 개수** | **추출 용량** | 추출 소요(초) |
|--------|-------------------------|---------------|----------------|
| interval_2s_q90 | **71** | **22.18 MB** | 54.4 |
| interval_1s_q90 | **139** | **43.5 MB** | 61.2 |
| interval_0.5s_q90 | **274** | **86.43 MB** | 87.7 |
| interval_1s_q85 | **139** | **37.89 MB** | 70.2 |

### 6.3 worker 값별 비교 (interval 프리셋)

벤치 실행: `--max-videos 6 --no-yolo --workers 1 2 4`. 완료 후 아래 빈 칸을 JSON/로그 기준으로 채우면 됩니다.

| preset | 추출 이미지 총 개수 | 추출 용량 | **workers=1** (초) | **workers=2** (초) | **workers=4** (초) |
|--------|-------------------------|-----------|--------------------|--------------------|--------------------|
| interval_2s_q90 | 71 | 22.18 MB | **54.4** | **34.7** | **33.2** |
| interval_1s_q90 | 139 | 43.5 MB | **61.2** | **48.1** | **40.0** |
| interval_0.5s_q90 | 274 | 86.43 MB | **87.7** | **66.8** | **88.8** |
| interval_1s_q85 | 139 | 37.89 MB | **70.2** | **51.5** | **65.1** |

- 벤치 재실행: `PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py --video-dir staging/tmp_data_2 --max-videos 6 --no-yolo --workers 1 2 4` (전 프리셋) 또는 특정 프리셋만 `--preset-names interval_0.5s_q90 interval_1s_q85`.
- **worker 패턴 (본 호스트):** `interval_0.5s_q90`·`interval_1s_q85` 에서 workers=2가 최단, workers=4는 경합으로 역전. `interval_2s`/`interval_1s_q90` 은 본 측정에선 workers=4까지 완만히 단축. **NFS 등 다른 환경에서는 w4 악화가 난 사례가 있어**, 배포 전 동일 스크립트로 재벤치 권장.

### 6.4 실행 방법

```bash
# interval 기반 많이/적게 비교 (workers 1,2,4, 전 프리셋)
PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py --video-dir staging/tmp_data_2 --max-videos 6 --no-yolo --workers 1 2 4

# 특정 프리셋만 (시간 절약)
PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py --video-dir staging/tmp_data_2 --max-videos 6 --no-yolo --workers 1 2 4 --preset-names interval_0.5s_q90 interval_1s_q85
```

---

## 7. Codex MCP로 추가 실험 시

- **cwd:** 이 repo 루트 (`/home/pia/work_p/Datapipeline-Data-data_pipeline`)를 `codex` 도구 호출 시 `cwd`로 전달하면 `.codex/config.toml` 및 `.codex/agents/` 에이전트가 로드됩니다.
- **활용할 서브에이전트:** ai-engineer, data-engineer, data-scientist, ml-engineer, mlops-engineer, machine-learning-engineer.
- **예시 프롬프트:**  
  “`staging/tmp_data_2` 영상으로 프레임 추출 + YOLO-World 검출할 때, 처리 시간과 검출 품질 균형을 위해 `max_frames_per_video`, `jpeg_quality`, `confidence_threshold`, `batch_size` 조합을 추천해 주고, 각 파라미터별로 짧은 근거를 달아줘.”

실측 재현은 `staging_video_extract_yolo_bench.py` + `--yolo-url` 로 동일 조건을 맞출 수 있습니다.

---

## 8. 실측 결과 표 (요약)

**interval 프리셋 실측(§6)** — 4개 조건, 6영상, workers=1 기준.

| preset | **추출 이미지 총 개수** | **추출 용량** | 추출 소요(초) |
|--------|-------------------------|---------------|----------------|
| interval_2s_q90 | 71 | **22.18 MB** | 54.4 |
| interval_1s_q90 | 139 | **43.5 MB** | 61.2 |
| interval_0.5s_q90 | 274 | **86.43 MB** | 87.7 |
| interval_1s_q85 | 139 | **37.89 MB** | 70.2 |

- **worker 비교**는 §6.3 참고.

---

## 9. Codex 전달용: 서브에이전트별 테스트·분석 추가 지시

**아래 지시문을 Codex에 전달할 때 `cwd`를 이 repo 루트로 두고, 문서는 `docs/CODEX_SUBAGENT_VIDEO_EXTRACT_YOLO_TEST.md`를 수정 대상으로 지정하세요.**

---

### 9.1 Codex에 전달할 프롬프트 (복사용)

```
이 문서(docs/CODEX_SUBAGENT_VIDEO_EXTRACT_YOLO_TEST.md)는 영상 이미지 추출 + YOLO-World 파라미터 테스트 결과 정리본이다.

다음 작업을 수행해줘.

1. **테스트 실행**  
   - 영상 경로: staging/tmp_data_2 (문서 §2.1에 파일 목록·길이·크기 있음)  
   - 스크립트: `PYTHONPATH=src python3 scripts/staging_video_extract_yolo_bench.py`  
   - 필요 시 `--max-videos 6` 또는 `--workers 1 2 4`, `--no-yolo` / `--yolo-url http://localhost:8001` 등으로 실행해 실측값을 보강해도 됨.

2. **서브에이전트별 역할에 맞는 테스트·분석**  
   아래 6명의 서브에이전트를 각각 활용해서, 문서 §6 interval 실측·worker 비교를 바탕으로 §10 각 하위 절에 **분석 내용을 추가**해줘.  
   - **ai-engineer**: interval 프리셋(적게/기본/많이) 트레이드오프, 운영 권장.  
   - **data-engineer**: 추출 용량·worker별 소요 시간, 스토리지·처리량 관점.  
   - **data-scientist**: interval별 관측량·분석용 프리셋 권장.  
   - **ml-engineer**: YOLO conf/iou/batch, 모델 입력(interval·품질) 관점.  
   - **mlops-engineer**: worker 수, 추출/YOLO 시간, 리소스·운영 관점.  
   - **machine-learning-engineer**: 재현성·baseline·실험 설계(interval 축 분리).

3. **문서 갱신**  
   - §2.1 영상 파일 목록은 이미 상세 기재되어 있음. 추가로 실측 시 사용한 파일 집합(예: max-videos 6일 때의 파일 목록)이 다르면 그에 맞게 표나 각주를 보강해줘.  
   - §10 각 서브에이전트 절에 위 역할에 맞는 분석 내용을 2~4문단 분량으로 작성해 넣어줘.

4. **interval 기반(1초당 1프레임) 분석**  
   - 기본 추출이 `frame_interval_sec`(N초당 1프레임) 방식. §6 실측(interval_2s=적게, interval_1s=기본, interval_0.5s=많이, worker 비교)을 반영하여 §10에 **interval 프리셋 권장**과 **worker 확장 시 주의점**을 반영해줘.
```

---

## 10. 서브에이전트별 분석 (Codex 실행 후 채움)

각 서브에이전트가 **자기 역할에 맞게** 문서의 실측 데이터와 파라미터 정의를 바탕으로 분석한 내용을 아래에 채운다.

### 10.1 ai-engineer

interval 기반으로 역할 관점의 기본선도 `interval_1s_q90`으로 옮기는 편이 자연스럽다. §6 기준(2026-03-20 재측정, workers=1) 6개 영상에서 `interval_2s_q90`은 71장·22.18MB·54.4초, `interval_1s_q90`은 139장·43.5MB·61.2초, `interval_0.5s_q90`은 274장·86.43MB·87.7초, `interval_1s_q85`는 139장·37.89MB·70.2초였다. 추출량(프레임 수)은 간격에 거의 비례하고, q85는 q90과 동일 프레임 수에서 용량만 약 12.9% 절감되는 패턴이 확인됐다. AI 기능을 운영에 붙이는 입장에서는 기본 추천을 `interval_1s_q90`, 비용 민감 경로를 `interval_2s_q90` 또는 `interval_1s_q85`, 고밀도 재검토를 `interval_0.5s_q90`로 분리하는 구성이 해석하기 쉽다.

worker 확장에서는 "많이 추출할수록 worker를 무조건 늘린다"는 규칙을 피해야 한다. §6.3 기준 본 호스트에서는 `interval_2s`/`interval_1s_q90`은 workers=1→2→4로 완만히 단축됐지만, `interval_0.5s_q90`·`interval_1s_q85`는 workers=2가 최단이고 4에서 다시 느려져 경합 신호가 났다. NFS 등 다른 환경에서는 상한 worker에서 역전이 더 자주 나므로, **배포 전 동일 벤치 스크립트로 재측정**하는 것이 안전하다.

### 10.2 data-engineer

interval 체계에서는 저장량과 처리량 예측이 더 직접적이다. §6 실측만 봐도 `interval_2s_q90`은 71장·22.18MB, `interval_1s_q90`은 139장·43.5MB, `interval_0.5s_q90`은 274장·86.43MB로 간격에 거의 비례한다. `interval_1s_q85`는 프레임 수 139로 `interval_1s_q90`과 같고 용량만 37.89MB로 줄어, **객체 수는 동일·바이트만 절감**하는 운영 실험에 적합하다. 데이터 엔지니어링 관점에서는 대량 적재 기본선을 `interval_2s_q90`(객체 수↓) 또는 `interval_1s_q85`(객체 수 동일·용량↓) 중 선택하고, 표준 품질 큐는 `interval_1s_q90`, 고밀도 백필은 `interval_0.5s_q90`를 별도 큐로 두는 편이 안전하다.

worker 확장 시 주의점은 "최대 worker"가 아니라 "스토리지 경합 한계"를 먼저 계약화하는 것이다. §6.3에서 `interval_0.5s_q90`·`interval_1s_q85`는 workers=2가 최단이고 4에서 역전됐다. 반면 `interval_2s`/`interval_1s_q90`은 본 호스트에서 4까지 단축됐다. interval 기반 배치는 **환경별로 `workers=2` vs `4` 우위가 갈리므로** NFS/로컬 각각 벤치 후 표준값을 고정하는 것이 좋다. `interval_0.5s_q90`은 파일 수가 커져 inode·listing·업로드 호출이 증폭되므로, worker만 늘리기보다 수명주기·압축 정책을 먼저 정하는 편이 비용에 직접적이다.

### 10.3 data-scientist

interval 기준으로 보면 분석자가 가장 먼저 고를 값은 "이 이벤트를 몇 초 간격으로 관찰해야 하느냐"다. §6에서 `interval_1s_q90`은 `interval_2s_q90`보다 프레임 수가 139 대 71로 늘었고, 짧은 행동 변화나 순간 포즈를 볼 기회도 그만큼 많아진다. 따라서 일반적인 탐색 분석과 라벨링 샘플 생성에는 `interval_1s_q90`이 가장 균형이 좋다. 반면 희소 사건 탐색, 짧은 접촉 장면, 무기·낙상처럼 순간성이 강한 이벤트의 recall을 더 보고 싶다면 `interval_0.5s_q90`이 유력한 실험축이다. §6 실측에서 6영상 기준 274장으로 `interval_1s_q90`(139장) 대비 관측 시점이 약 2배에 가깝게 늘었다. `interval_2s_q90`은 coarse screening이나 클래스 분포 파악처럼 장면 수준 통계가 목적일 때만 권장된다.

worker는 분석자의 관심사인 표본 품질을 높여주지 않고 wall-clock만 바꾸므로, 실험 축으로 섞지 않는 편이 좋다. §6.3에서 고밀도·q85 프리셋은 workers=4가 2보다 느려질 수 있어, 실험 설계에서는 interval을 먼저 고정하고 worker는 환경별 벤치로 `2` 또는 `4` 중 하나로 묶는 것이 낫다. 프레임 밀도와 worker를 동시에 바꾸는 설계는 해석을 어렵게 만든다.

### 10.4 ml-engineer

interval 체계는 모델 입력을 더 직접적으로 정의한다는 점에서 엔지니어링적으로 해석이 쉽다. `interval_2s_q90`과 `interval_1s_q90`의 차이는 곧 모델이 보는 시간축 간격 차이이며, 실측도 71장 대 139장으로 거의 비례했다. 모델 입력 기준선으로는 `interval_1s_q90`이 가장 적절하다. 너무 성기지 않아 순간 이벤트 누락 가능성을 줄이면서도, `interval_0.5s_q90`처럼 중복 프레임이 급격히 늘어 같은 장면을 과대표집할 위험은 상대적으로 낮기 때문이다. `interval_2s_q90`은 모델 성능 한계보다 샘플링 한계가 먼저 드러날 수 있어, 회귀 기준선보다는 저비용 운영선이나 장면 분류 수준 점검에 더 적합하다. `interval_1s_q85`는 §6에서 동일 프레임 수 대비 용량만 줄어든 것이 확인됐으므로, 화질 저하가 임계값 민감 클래스에 미치는 영향을 별도 축으로 보기 좋다.

worker 확장 시 주의점은 추출 병렬화가 모델 품질 개선과 무관하다는 점이다. 병목이 디코딩·저장이면 worker 패턴이 프리셋마다 달라질 수 있으며(§6.3), 이 변동이 모델 입력 내용을 바꾸지는 않는다. baseline 평가나 threshold sweep에서는 worker를 환경별로 고정하고 interval만 바꾸는 비교를 우선해야 한다. `interval_0.5s_q90`처럼 프레임 수가 급증할 때 worker와 I/O 노이즈가 섞이면 conf/iou 튜닝 해석이 흐려질 수 있다.

### 10.5 mlops-engineer

interval 전환 이후 MLOps 기본선은 `interval_1s_q90 + workers=2`(또는 본인 환경 벤치상 유리한 worker)에 가깝다. §6 기준(workers=1) `interval_1s_q90`은 139장·43.5MB·61.2초이며, workers=2·4에서 각각 48.1초·40.0초로 단축됐다. `interval_2s_q90`은 71장·22.18MB·54.4초( workers=2·4는 34.7·33.2초)로 비용 효율이 더 높다. 운영 SLO 기준으로 표준 큐는 `interval_1s_q90`, 저비용/백필은 `interval_2s_q90` 또는 `interval_1s_q85`, 고밀도 검수는 `interval_0.5s_q90` 분리가 맞다. `interval_1s_q85`는 동일 프레임 수로 저장소 바이트만 줄이는 대체안으로 활용 가능하다.

주의할 점은 worker를 수평 확장 레버로 과신하면 안 된다는 것이다. §6.3에서 `interval_0.5s_q90`·`interval_1s_q85`는 workers=4가 2보다 느렸고, 반대로 `interval_2s`/`interval_1s_q90`은 본 호스트에서 4까지 개선됐다. runbook에서는 **스토리지 종류별로 worker 상한을 벤치로 고정**하고, `interval_0.5s_q90`은 산출물·업로드 급증으로 장애 반경이 커지므로 별도 큐·경보를 두는 편이 좋다.

### 10.6 machine-learning-engineer

interval 기반 실험 설계에서는 baseline을 `interval_1s_q90`으로 두는 것이 가장 재현성이 높다. 이유는 설정 의미가 명확하고, §6에서 이미 실측치가 채워져 있으며, `interval_2s_q90` 대비 입력량이 거의 2배로 늘어 recall 차이를 관찰하기 좋기 때문이다. 이후 실험 축은 두 갈래로 나누는 편이 깔끔하다. 하나는 시간축 촘촘함을 보는 `interval_2s_q90 → interval_1s_q90 → interval_0.5s_q90` 축이고, 다른 하나는 같은 시간축에서 압축만 바꾸는 `interval_1s_q90 → interval_1s_q85` 축이다. 이렇게 두면 frame density와 JPEG 품질 효과를 분리해 원인을 설명할 수 있다. §6에 `interval_0.5s_q90`·`interval_1s_q85` 실측이 반영됐으므로, 두 축은 `interval_1s_q90` 대비 밀도·용량 효과를 검증한 뒤 기본선 대체 여부를 결정하면 된다.

worker는 baseline 정의에 포함하지 말고 실행 파라미터로 고정하는 것이 좋다. 시스템 요인은 모델 입력 계약과 독립이므로, baseline 문서에는 `interval_1s_q90` 등 입력 축만 명시하고 worker는 환경별 벤치 표로 관리하는 편이 재현성에 유리하다. `interval_0.5s_q90`에서 worker와 샘플링 밀도를 동시에 바꾸면 해석이 섞이기 쉽다.

### 10.7 interval 기반(1초당 1프레임) 비교 — 서브에이전트 요약

§10.1~10.6을 종합하면, interval 기반 기본 권장은 `interval_1s_q90`이다. `interval_2s_q90`은 6개 영상 기준 71장·22.18MB(workers=1·54.4초)로 비용과 저장량을 줄이며 관측 시점을 성기게 한다. `interval_1s_q90`은 139장·43.5MB(61.2초)로 표준 해상도에 가깝다. `interval_0.5s_q90`은 274장·86.43MB(87.7초)로 고밀도 분석선이고, `interval_1s_q85`는 139장·37.89MB로 **동일 프레임·저용량** 저장소 절감 후보다.

worker 정책은 프리셋·스토리지에 따라 갈린다. §6.3에서 `interval_2s`/`interval_1s_q90`은 workers=4까지 단축됐고, `interval_0.5s_q90`·`interval_1s_q85`는 workers=2가 최단·4에서 역전됐다. 공통 권장은 **환경별 벤치 후** worker 상한을 정하고, `interval_0.5s_q90`은 별도 큐·SLO·경보로 운영하는 편이 안전하다.

---

## 11. 참고 코드 위치

- 프레임 추출: `src/vlm_pipeline/lib/video_frames.py` (`plan_frame_timestamps`, `target_frame_count`, `frame_interval_sec`, `extract_frame_jpeg_bytes`)
- clip_to_frame: `src/vlm_pipeline/defs/process/assets.py` (`clip_to_frame`, `_extract_clip_frames`)
- raw_video_to_frame: `src/vlm_pipeline/defs/process/assets.py` (`raw_video_to_frame`)
- YOLO 클라이언트: `src/vlm_pipeline/lib/yolo_world.py` (conf, iou, max_det)
- YOLO asset: `src/vlm_pipeline/defs/yolo/assets.py` (confidence_threshold, iou_threshold, batch_size)
- YOLO 서버: `docker/yolo/app.py` (IMGSZ, MAX_BATCH, conf, iou)
- dispatch 테스트: `scripts/staging_test_dispatch.py` (IMAGE_PARAM_PRESETS)
- **이미지 추출·용량·worker 벤치:** `scripts/staging_video_extract_yolo_bench.py` (프리셋별·`--workers 1 2 4` 추출 시간 비교, 선택 YOLO)
