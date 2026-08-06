# pe_inference — 분포-IoU 이벤트 탐지 추론 (전달용 최소 패키지)

CLIP(PE-Core-L14-336) + **분포 IoU** 방식으로 화재/연기/쓰러짐 등을 탐지하고 **오버레이+히스토그램 시각화**와 **프레임별 IoU 결과(CSV)**를 뽑는 최소 실행 패키지.
> 프롬프트 조합 생성·인코딩은 포함 안 함. **모델 엔진과 프롬프트 feature JSON은 이미 있다고 가정.**

## 1. 폴더 구성 (이게 전부)
```
pe_inference/
├── 01_TuningFree_v2.py     # 진입점: dist IoU 판정 + 디바운스 + 시각화 로직 전부
├── utils/
│   ├── utils.py            # 오버레이/영상 저장(VideoMaker)·색상
│   ├── trt_load.py         # TensorRT 엔진 로드/추론
│   ├── trt_utils.py        # 이미지 전처리(336 리사이즈·정규화)
│   └── arial.ttf           # 시각화 폰트
└── README.md
```
- 어느 위치에서 실행해도 됨(폰트 경로는 파일 기준 절대경로로 해결).

## 2. 사전 준비 (별도로 있어야 하는 것)
1. **모델 엔진**: `PE-Core-L14-336.engine` (TensorRT). 경로를 `--model_path`로 지정.
2. **프롬프트 JSON**: `text_features_*.json` (항목 = `class/prompt/ID/feature`). 경로를 `--text_json_path`로 지정.
3. **파이썬 환경**: `torch`, `tensorrt`, `torchvision`, `opencv-python(cv2)`, `pillow(PIL)`, `pandas`, `numpy`.
4. **ffmpeg**: `--save_video`(영상 저장) 시 필요. PATH에 있어야 함.

## 3. 실행
```bash
export PATH=/path/to/env/bin:$PATH          # ffmpeg가 PATH에 있도록(영상 저장 시)
CUDA_VISIBLE_DEVICES=0 python 01_TuningFree_v2.py \
  --video_path  <영상폴더 또는 단일영상> \
  --output_path <출력폴더> \
  --text_json_path <text_features.json> \
  --model_path  <PE-Core-L14-336.engine> \
  --classes 0,1,2,3 \
  --iou_threshold 0.15 --iou_mode hist --iou_hist_bins 80 \
  --queue_size 5 --alarm_duration_threshold 3 \
  --model_input_fps 2 --save_speed 2 \
  --category DEMO --detail x \
  --analysis --save_video --display
```

### 주요 인자
| 인자 | 의미 |
|---|---|
| `--video_path` | 폴더(내부 mp4 전부) 또는 단일 영상 |
| `--output_path` | 결과 저장 루트. `<OUT>/<category>_<detail>/{analysis,inference,pred}` 생성 |
| `--text_json_path` | 프롬프트 feature JSON |
| `--model_path` | TensorRT 엔진 |
| `--classes` | 사용할 클래스(0 normal,1 falldown,2 fire,3 smoke) |
| `--iou_threshold` | 알람 임계(연기·쓰러짐 권장 0.15) |
| `--iou_mode hist` / `--iou_hist_bins 80` | 히스토그램 IoU 방식/bin 수 |
| `--queue_size 5` / `--alarm_duration_threshold 3` | 디바운스(최근 5중 3프레임↑ dip 시 알람) |
| `--model_input_fps` | 프레임 샘플링 fps(추론 2 권장) |
| `--analysis` | 프레임별 IoU CSV 저장 |
| `--save_video` | 오버레이+분포 히스토그램 영상 저장(ffmpeg 필요) |
| `--display` | 프레임에 통계/IoU 텍스트 표시 |

> 카테고리별 임계를 다르게 주려면(예 화재 0.20) 현재 스크립트는 전역 `--iou_threshold` 하나만 받으므로, 카테고리별로 나눠 돌리거나 코드에서 클래스별 임계를 확장하세요.

## 4. 출력
- `<OUT>/<cat>_<detail>/analysis/<영상>.csv` — 프레임별 클래스 IoU(임계 무관, 재계산 가능).
- `<OUT>/<cat>_<detail>/inference/<영상>.mp4` — **분포 시각화**: 원본 오버레이 + 하단 클래스별 cos 히스토그램 패널(normal vs 이벤트 분포 겹침=IoU).
- 콘솔: 영상별 알람/최저 IoU 요약.

## 5. 원리 (한 줄)
프레임마다 이미지 임베딩 vs 각 클래스 텍스트벡터 cos → 클래스별 점수 히스토그램 → **normal 분포와 이벤트 분포의 hist-IoU < 임계**면 후보 → 디바운스(5중 3↑) 통과 시 알람.

## 6. 빠른 확인 예
```bash
CUDA_VISIBLE_DEVICES=0 python 01_TuningFree_v2.py \
  --video_path ./samples --output_path ./out \
  --text_json_path /path/text_features.json --model_path /path/PE-Core-L14-336.engine \
  --classes 0,1,2,3 --iou_threshold 0.15 --iou_mode hist --iou_hist_bins 80 \
  --queue_size 5 --alarm_duration_threshold 3 --model_input_fps 2 --save_speed 2 \
  --category DEMO --detail x --analysis --save_video --display
# → ./out/DEMO_x/analysis/*.csv, ./out/DEMO_x/inference/*.mp4
```
