# 카메라 앵글 그룹화 기준 — 프롬프트 인식률 편차 진단용 상시 메타데이터 축

2026-07-29 / 협업: pipeline-explorer(실측) · ai-data-engineer(taxonomy) · ai-modeler(검증설계) · cto(저장·범위)

## 0. 문제와 목적

"같은 상황임에도 **CCTV 각도에 따라 프롬프트 인식률이 달라진다**."

- 대상은 Gemini 이벤트 라벨링에 국한하지 않는다. **프롬프트로 구동되는 모든 인식 stage** — Gemini 이벤트/캡션, SAM3 텍스트프롬프트 segmentation, VLM 캡셔닝 — 가 공통으로 카메라 기하에 영향받는다는 가설.
- 따라서 앵글은 일회성 진단 산출물이 아니라 **ingest 시 부여되는 상시 per-video 메타데이터 축**으로 만든다. 소비자: FiftyOne 그룹뷰(1차), 프롬프트 라우팅/튜닝(2차), 데이터셋 층화(3차).

## 1. 기준 — "도(degree)"를 버린다

### 왜 각도 숫자로 정의하면 실패하는가

초기 프롬프트(`docker/qwen/classify_angle.py` — 2026-07-29 Qwen 폐기와 함께 삭제됨)는 VLM에게 `70-90 deg` / `30-70 deg` 같은 **절대각도 숫자**를 요구한다. 단안 정지프레임에는 스케일 기준도 기준물체도 없어 모델이 각도를 계산할 근거가 없다 → **실측 오분류**: 40~50° 벽부착 카메라를 `overhead`로 답했다. 이는 "본 것을 답함"이 아니라 "숫자를 지어냄"의 예측 가능한 결과다.

**교정 원칙**: bin은 각도가 아니라 **프레임에서 직접 보이는 증거**로 정의한다. 사람이 같은 프레임을 보고 같은 답을 낼 수 있어야 한다.

### Axis 1 — `camera_angle` (그룹화의 본체)

| 값 | 판별규칙 (프레임에서 보이는 것만 묻는다) | 결부된 실패 가설 | 구 4-bin 대응 |
|---|---|---|---|
| `plan_view` | 사람이 보인다면 정수리·어깨선만 보이고 얼굴(눈코입)과 몸통 앞/옆면은 거의 안 보인다. 서 있는 사람이 세로로 길지 않고 원형·타원형 덩어리로 보인다. | **실루엣 붕괴** — 서있음/쓰러짐의 종횡비 정보가 소실돼 자세 의존 카테고리(falldown/violence)를 프롬프트가 구분 못 한다 | overhead |
| `oblique_view` | 얼굴 또는 몸통 앞/옆면이 식별 가능하고, 서 있는 사람 실루엣이 가로보다 세로로 뚜렷이 길다. 바닥면과 상반신이 한 프레임에 함께 보인다. | **대조군(baseline)** — 실루엣 보존, 특정 실패기제 없음 | high_angle + elevated **병합** |
| `level_view` | 바닥면이 거의 안 보이거나 발밑이 화면 하단에 바로 잘려 있다. 사람이 여럿이면 서로 겹쳐 보이는 경우가 흔하다. | **상호 폐색 지배** — 실루엣은 있으나 겹침. 원인이 각도가 아니라 폐색일 수 있어 Axis 3으로 분리 검증 | eye_level |
| `indeterminate` | 위 세 문장의 판단 근거(사람 또는 바닥/구조물 기준선)가 프레임에 전혀 없다. | 강제선택 폐지 | (신설) |

**40~50° 오분류가 재현되지 않는 이유**: 40-50° 벽부착 카메라는 전형적으로 얼굴/정면상체가 보이고 서있는 사람이 세로로 길다 → 새 규칙상 `oblique_view`가 명백하다. 단일 즉답(각도 추정)이 아니라 **복수 시각신호의 동시 충족**(얼굴 가시성 + 종횡비 + 바닥 가시성)을 요구해 연속량 경계에서 흔들릴 자유도가 준다.

**왜 3(+1)개인가**
- 3개 각각이 **서로 다른 처방**에 대응한다: `plan_view`→프롬프트에 탑뷰 자세 가이드 추가 / `oblique_view`→처방 불필요(대조군) / `level_view`→폐색 완화·구도 조정. 병합하면 어느 처방이 맞는지 못 가린다.
- `high_angle`/`elevated`를 나누는 건 **같은 foreshortening 연속선을 더 쪼개는 것**이라 30°/70° 경계 불안정을 이름만 바꿔 재현한다. 병합이 정답.
- 검정력: bin을 늘리면 bin당 표본이 쪼개져 MDE가 나빠진다 (§4 표).
- `indeterminate` 신설 이유: 강제 4택은 근거 없는 프레임에도 답을 뱉게 해 **노이즈를 각도 신호로 착시**시킨다.

### Axis 2 — `subject_scale` (별도 축, tilt와 교차)

| 값 | 판별규칙 |
|---|---|
| `subject_legible` | 주 피사체(사람)의 키가 프레임 세로의 약 1/4 이상 |
| `subject_marginal` | 그 미만 (원경·광각으로 피사체가 작다) |
| `not_applicable` | 판단 대상(사람)이 프레임에 없다 |

**분리 이유**: `plan_view + 줌인`(피사체 큼)과 `level_view + 광각원경`(피사체 작음)이 실재한다 — 원인 경로가 다르므로 한 축에 뭉개면 안 된다. 2-bin인 이유: PG에 픽셀 bbox 좌표가 없어(§2 실측) 정밀 비율측정이 불가하고 정성판단만 가능하다 → 세분화는 허위정밀도.

### Axis 4~6 — Places365 역할 흡수 (`environment_type` / `daynight_type` / `weather`)

Places365 가 하던 실내외·주야 판정을 **같은 Qwen 호출 1회**가 함께 반환한다. Places365 는 삭제하지 않고 일시정지(`INGEST_DEFER_VIDEO_ENV_CLASSIFICATION=true` + `video_env_backfill_schedule` STOPPED, 둘 다 이미 그 상태였다).

| 축 | 값 | 비고 |
|---|---|---|
| `environment_type` | `indoor` / `outdoor` | 001 에 이미 있는 컬럼 재사용 |
| `daynight_type` | `day` / `night` | 동일 |
| `weather` | `clear` / `cloudy` / `rain` / `snow` / `fog` / `not_applicable`(실내) / `indeterminate` | **신규 컬럼** — Places365 가 못 하던 축 |

**통합 근거**
- 호출 1회·프레임 1장이 Places365 3프레임 GPU 추론 + 별도 앵글 호출을 **둘 다 대체**한다 → 순 감소.
- dagster 컨테이너의 torch/Places365 GPU 상주가 불필요해져 GPU 0(현재 14.9/16.4GB) 여유 확보.
- `env_method` 가 provenance 를 담으므로 두 라벨러의 결과가 DB 에서 구분된다(`places365_cuda` vs `qwen2.5-vl`).
- `outdoor_score` / `avg_brightness` 는 Qwen 경로에서 `NULL` — VLM 은 캘리브레이션된 연속값을 주지 못한다. 필요해지면 프레임 밝기만 따로 계산해 붙인다.

**⚠️ Places365 라벨과의 직접 비교는 불가능함이 확인됐다**: `env_method='places365_cuda'` 18,046행은 **전부 레거시 `/nas/archive/` 경로**이고 그 파일들은 디스크에 실존하지 않는다(NAS 이전 때 낙오). 현행 `/nas/data/archive/` 경로 106,384행은 **전부 `deferred`**. 따라서 검증은 사람 육안 대조로 했다(§7).

### Axis 3 — `occlusion_state` (별도 축)

`unoccluded` / `partially_occluded` / `truncated`(화면경계 절단) / `not_applicable`(판단대상 없음)

**분리 이유**: 폐색은 각도와 무관하게 가구·타인물로 발생한다. `level_view`와 상관은 예상되지만 등치가 아니다 — 등치 취급하면 **"각도 때문"과 "폐색 때문"을 영영 못 나눈다**. 이 분리가 진단의 핵심이다.

### 채택하지 않은 것

- `lens_wide_fisheye` 플래그 — 유병률 불명, 컬럼만 늘고 검증계획 없음. **필요해지면 추가**(같은 관찰 패스에서 한계비용 0).
- 각도 신뢰도(confidence) 컬럼 — 현재 분류기는 단어매칭이라 캘리브레이션된 확률을 못 준다. 대신 `angle_method`에 모델 식별자를 남겨 재현성을 확보.
- 카메라 식별자 정규화 — §6.

## 2. 근거 실측 (prod PG / MinIO, 2026-07-29)

| 사실 | 값 | 설계에 미친 영향 |
|---|---|---|
| `raw_files` | 129,970 전량 `media_type='video'` | 앵글은 per-video 축 |
| `source_unit_name` | distinct 96, 최다 `site-b` 80,062(61.6%), 빈값 17,228 | **카메라 단위 아님이 증명됨** — `site-b` 서브폴더는 `오리지널영상`/`converted_mp4`/`데모용_클립` = **처리단계**, 같은 사건 3중 중복. `site-c`는 파일명 숫자 prefix로 최소 7대 카메라 혼재 |
| 픽셀 bbox 좌표 | `image_labels` 454,726행 중 PG에 좌표 있는 건 `image_label_annotations` 1,558박스/248이미지/**단일 소스 VHC**뿐 | **bbox 종횡비로 앵글 역산 불가** → VLM 시각판별로 결정 |
| SAM3 zero-box | 454,726행 중 **249,686행(54.9%)이 박스 0개** | §4의 **최고 검정력 결과변수** |
| Gemini 이벤트 outcome | `source-c` 12,636 JSON 기준 zero-event 비율: violence 85.0%(160 cams) / smoke 69.5% / fire 75.5% / falldown 69.1% | 카메라간 SD 9.9pp, ICC 0.075 → **design effect 9.22** |
| `environment_type` | **86.1%(111,924)가 `deferred`** — 백필 미소진 | 앵글도 같은 구조를 쓰므로 **스케줄 가동 확인이 운영 필수조건** |
| `video_metadata` | 42컬럼, JSONB 없음. `environment_type`/`daynight_type`/`outdoor_score`/`avg_brightness`/`env_method` = per-video 서술자 선례 | 앵글 컬럼의 형태·이름 규칙을 그대로 복제 |
| 해상도/길이 | 144종(1920x1080 42.7% / 3840x2160 26.7%, 세로영상 수백건), duration 중앙 5.0s / 평균 85.4s | §4 교란변수 (카메라 수준에서 둘 다 **null** 로 측정됨) |

### ⚠️ 별건 버그 (이 설계와 무관하게 보고)

`video_metadata.timestamp_status='completed'`는 **1,314행**뿐인데 Gemini 이벤트 행을 가진 asset은 **4,443개**다 — 즉 4,162개 영상이 이벤트를 받았는데도 `pending`으로 남아 있다. CLAUDE.md가 "라벨링 완료 지표"로 안내하는 `timestamp_status`는 **하위 분석의 분모로 쓸 수 없다.** `docs/pipeline-flow-audit-2026-07-01-core.md`의 LABEL-1/2/3 이중 상태머신 발견과 일치. 별도 티켓 대상.

## 3. 테이블 & 파이프라인 흐름

### 3.1 스키마 — `video_metadata` 4컬럼 추가 (`017_video_camera_angle.sql`)

```sql
ALTER TABLE video_metadata
  ADD COLUMN IF NOT EXISTS camera_angle    TEXT,   -- plan_view|oblique_view|level_view|indeterminate
  ADD COLUMN IF NOT EXISTS subject_scale   TEXT,   -- subject_legible|subject_marginal
  ADD COLUMN IF NOT EXISTS occlusion_state TEXT,   -- unoccluded|partially_occluded|truncated|not_applicable
  ADD COLUMN IF NOT EXISTS angle_method    TEXT;   -- 'qwen2.5-vl-7b-awq' | 'deferred' | 'deferred_*'
CREATE INDEX IF NOT EXISTS video_metadata_camera_angle_idx
  ON video_metadata (camera_angle) WHERE camera_angle IS NOT NULL;
```

**신규 테이블이 아니라 컬럼인 이유**
- `environment_type`/`daynight_type`과 **grain·성격이 동일한 per-video 서술자**다. 형제를 다른 곳에 두면 모든 소비자에 조인이 하나 더 붙는다.
- FiftyOne 투영 경로가 **이미 존재**한다 (`docker/analysis/fiftyone_pgvector.py` `_fetch_video_env`) — 같은 SELECT에 컬럼만 추가.
- de-growth 로드맵의 정규화 항목([de-growth-roadmap-2026-07-28.md:49](../de-growth-roadmap-2026-07-28.md))은 **스테이지별 status 컬럼 ~20개**를 타겟한다. 서술자 컬럼 추가는 그 방향과 충돌하지 않는다.
- `angle_method`가 provenance(어느 모델이 매겼는지)를 담아 모델파생 라벨 추적 요건을 충족한다 — `env_method`와 동일 규약.
- 마이그레이션 러너 함정 회피: **`DO $$` 블록 미사용**, 순수 `ALTER`/`CREATE INDEX`만.

### 3.2 흐름 — Places365 패턴 복제

```
ingest (ops_normalize._stage_upload_task)
  └ insert_video_metadata(... angle_method='deferred')      ← 모든 영상이 큐에 등록됨
        ↓
video_camera_angle_backfill  (asset + 스케줄, env_backfill 형제)
  └ find_deferred_angle_videos(limit)                        -- angle_method='deferred' AND archive_path NOT NULL
  └ lib/video_angle.classify_video_angle(path)               -- ffmpeg 1프레임 → Qwen HTTP → 3축
  └ update_video_angle(asset_id, ...)                        -- 성공: method='qwen2.5-vl-7b-awq'
                                                             -- 파일없음/프레임없음: 'deferred_missing_archive'/'deferred_no_frames' (터미널 마커)
                                                             -- 예외: 'deferred' 유지 → 다음 tick 재시도
        ↓
FiftyOne  (docker/analysis/, untracked 증분 주입)
  └ ds.set_values("camera_angle", {...}, key_field="id")      -- 전체 재빌드 금지
```

**설계 결정과 근거**

| 결정 | 근거 |
|---|---|
| ingest에서 **동기 추론하지 않고** `deferred` 기록 | Qwen 호출 0.8s/영상을 ingest 핫패스에 넣으면 1,250파일 폴더당 +17분. env가 이미 같은 이유로 deferred |
| 프레임 추출 stage에 **의존하지 않음** | 앵글 분류기가 원본에서 ffmpeg로 직접 1프레임을 뽑는다 → env의 `frame_extract_count>0` 조건이 불필요해 **129,970 전량이 즉시 드레인 대상**(env는 18k만) |
| 기존 `env_backfill`에 **얹지 않고** 별 asset | env는 86.1%가 백로그다. 얹으면 앵글이 그 백로그를 그대로 상속한다. 실패 도메인 분리 |
| 재시작·재개는 **체크포인트 파일 없이** | `angle_method='deferred'` 조건 재조회 자체가 멱등 재개다 (env_backfill과 동일) |
| 프레임 1장 기본, `ANGLE_FRAMES` 노브 | 앵글은 고정 카메라 속성이고 `indeterminate`가 탈출구다. 3장 다수결은 3배 비용(130k → 87h) — `indeterminate` 비율이 높게 나오면 그때 올린다 |

### 3.3 비용

| 항목 | 값 |
|---|---|
| 전량 라벨링 | 0.8s/영상 × 129,970 ≈ **29시간** (GPU 0, 배치 분할 가능) |
| 신규 유입 | ingest 처리량 영향 **0** (백필 비동기) |
| GPU | GPU 0 현재 14.9/16.4GB (vLLM 9.5 + embedding-service 5.4). **정비모드 진입 금지** — `/maintenance/enter`는 SAM3 `/segment`를 503으로 만들어 prod bbox를 세운다. off-peak 분할 실행으로 충분 |
| 디스크 | PG 수 MB. 단 루트가 **96%(41GB free)** — 상시 위험. vLLM 이미지 18.9GB가 최대 회수 후보 |

**우선순위**: 전량 29시간을 먼저 쓰지 말고, **프롬프트 outcome이 존재하는 부분집합부터** 돌려 §4 검증을 통과시킨 뒤 전량으로 확장한다. outcome 없는 영상의 앵글은 상시 축으로서 가치가 있지만(FiftyOne·층화) 진단에는 기여하지 않는다.

## 4. 검증 — 이 기준이 실제로 인식률을 갈라내는지

기준만 만들고 "FiftyOne 필터가 하나 늘었다"로 끝나면 실패다. **분석 단위는 영상이 아니라 카메라(또는 그 대리키)** 다.

### 4.1 결과변수 (프롬프트별로 하나씩)

| 프롬프트 stage | 결과변수 | 규모 | 비고 |
|---|---|---|---|
| **SAM3 텍스트프롬프트** | 프레임당 박스 0개 비율 (`image_labels.object_count=0`) | 454,726행 / 249,686 zero (54.9%) | **최고 검정력.** PG만으로 즉시 집계, 카테고리 프롬프트가 고정돼 있어 해석이 깔끔 |
| **Gemini 이벤트** | 카테고리별 zero-event 비율 `p0` | 12,636 JSON / 160 cams | MinIO `vlm-labels/**/events/*.json` **객체 크기 2 B == `[]`** 로 판정 (다운로드 불필요). `timestamp_status`는 쓰지 말 것(§2 버그) |
| (확인용) | 고재현율 probe 프롬프트 재실행 시 `[]`가 뒤집히는 비율 | ~150편 표본 | **miss와 정상 true-negative를 유일하게 분리하는 수단** |

`0 events`가 "인식 실패"인지 "정상적으로 이벤트 없음"인지 DB만으로는 구분되지 않는다. 이 모호성은 **측정 단계에서 해결할 필요가 없다** — 카메라 단위로 집계하면 콘텐츠 차이가 평균화된다. 단 "true-event 유병률이 앵글군간 같다"는 가정이 필요하고, 그 가정은 probe로 검증한다.

### 4.2 검정과 교란변수 — 카메라 수준에서 이미 측정된 값

| 축 | 카메라 수준 효과 (violence) | 판정 |
|---|---|---|
| 해상도 (720x576 vs 1280x720) | −1.78pp, 95% CI [−7.4, +3.8], **R²=0.008** | **null** — "null이 어떻게 보이는지"의 캘리브레이션 기준 |
| duration (중앙 19.5s 분할) | +0.61pp, t=0.24 | **null** |
| 카테고리/프롬프트 | **15.5pp** (violence 85.0 vs smoke 69.5) | **현재까지 측정된 최대 효과** |
| 카메라 정체성 | SD **9.9pp**, 범위 56–100% | 실재, 원인 미규명 ← 앵글이 이걸 설명하는지가 질문 |
| angle | 미측정 | 열린 질문 |

- **영상 단위 검정 금지**: ICC 0.075 × 평균 110.5영상/카메라 → design effect 9.22. 10,691영상은 실질 ~1,160 독립관측이고, 영상 단위 카이제곱은 SE를 3.0배 과소추정해 **가짜 양성**을 만든다.
- **검정**: 카메라 수준 `p0`에 대한 2표본 t/Wilcoxon (n_videos 가중). 순서형 3-bin이면 **선형추세/Jonckheere–Terpstra 1 df** — 쌍별 6회 검정보다 낫다.
- **GLMM/랜덤효과는 과잉이다.** 노출(앵글)이 카메라 수준 상수이고 중첩이 1단이라 카메라 집계 t-검정이 정확히 옳은 추정량이다. 하지 말아야 할 단 하나는 영상 단위 검정.
- **층화·고정**: 카테고리 고정(섞으면 결과정의가 섞임) / day-night는 앵글군간 혼합비가 불균형일 때만 주간 한정 / `environment_type`은 86% 결측이라 손대지 말 것 / `site-b` 3중복은 `checksum`·`dup_group_id`로 **자동 dedup 불가**(pHash는 이미지 전용, 재인코딩·트리밍은 바이트가 다름) → 표본에서는 `오리지널영상`만 canonical 채택.

### 4.3 필요 표본 (카메라 수, σ=9.9pp, 80% power, α=0.05)

| 설계 | bin당 카메라 | MDE |
|---|---|---|
| 65 cams(n≥30), **2 bin** | 32 | **7.0pp** |
| 65 cams, 3 bin | 21 | 8.6pp |
| 65 cams, 4 bin (극단쌍) | 16 | 9.9pp |
| 65 cams, 4 bin + Bonferroni(6) | 16 | 12.3pp |

**카메라당 영상 ≥30 요구** — n=30에서 카메라별 측정 SE 6.3pp vs 신호 9.9pp(비 0.63). n=10이면 SE 10.8pp로 **측정오차가 신호를 넘는다.**

1차 분석은 **`plan_view` vs `level_view` 2-bin 대비**로 시작한다(MDE 최선 + Qwen 라벨이 가장 믿을 만한 양 끝단). 3-bin은 기술적 추세로만 보고.

### 4.4 라벨 노이즈 내성

대칭 오분류율 p는 관측 대비를 정확히 **(1−2p)** 배로 희석하고 필요 n은 **1/(1−2p)²** 로 늘어난다.

| p | 희석계수 | MDE_true (65 cams, 2 bin) | Δ=15pp 검출에 필요한 카메라 |
|---|---|---|---|
| 0.10 | 0.80 | 8.8pp | 11 |
| 0.20 | 0.60 | 11.7pp | 20 |
| 0.30 | 0.40 | 17.5pp | 43 |
| 0.35 | 0.30 | 23.3pp | 77 → 사실상 불가 |

**생존선: 참효과 15pp면 p≤0.30, 10pp면 p≤0.20. p≈0.35 이상이면 분석은 죽는다.**

- **그러나 p를 추정하지 말고 없애라**: 분석 단위가 ~160 카메라면 **사람이 100%를 30~40분에 본다**(프레임당 10~15초). p→0이 되고 위 표가 무의미해진다. Qwen의 역할은 사람 검수용 사전 정렬로 내려간다. 노이즈 추정 장치를 만드는 것보다 싸다.
- **self-consistency는 사람 GT를 대체할 수 없다.** 실측된 실패(40-50°→overhead)는 모델의 "overhead" 개념 자체에 있는 **체계적 편향**이라 프롬프트를 바꿔도 동일하게 재현된다 → 일관성은 훌륭해 보이면서 정확히 중요한 방향으로 틀린다. 유일한 정당한 용도: 두 프롬프트 불일치율 x%는 오류율의 **하한** x/2 을 주고, 모호 부분집합을 우선 검수 대상으로 표시한다.

### 4.5 기각 기준 (라벨링 착수 전에 확정)

| # | 기각 조건 | 임계 |
|---|---|---|
| A | **노출 대비 부재** — 한 bin이 카메라의 >80%를 차지하거나, 소수 bin이 <10 카메라 | 중단 — "효과 없음"이 아니라 **답할 수 없음** |
| B | **실질 영(null)** — 극단 bin간 Δ(카메라 `p0`)의 95% CI ⊂ (−10pp, +10pp) | 중단 — 앵글은 레버가 아니다 |
| C | **분산 설명력 부족** — 카메라 `p0`에 대한 angle R² < 0.10 | 중단. 캘리브레이션: 해상도가 **R²=0.008** 이었고 그게 null의 모습이다 |
| D | **probe 평탄** — `[]` 영상의 probe rescue rate가 bin간 5pp 미만 차이 | 중단 — 그 `[]`들은 각도 탓 miss가 아니다 |

### 4.6 미리 알아야 할 천장 (솔직한 평가)

- 최고 성적 카메라조차 자기 영상의 **56.0%**에서 `[]`를 반환한다(코퍼스 평균 86.4%). 현실적 앵글 이득은 ~10pp 수준.
- 반면 **카테고리/프롬프트 축은 이미 15.5pp**를 흔든다(violence 85.0 vs smoke 69.5). 해상도·duration은 둘 다 null.
- 즉 최선의 결과에서도 앵글별 프롬프트 튜닝의 가치는 **카테고리 단위 프롬프트 작업의 약 2/3**이다. 그리고 56pp의 카메라-불변 바닥은 이미징 문제가 아니다.
- **권고**: 앵글 검증은 (표본 설계상 1일 작업이므로) 하되, 프롬프트/카테고리 작업 앞에 세우지 마라. Δ<10pp면 "각도에 따라 인식률이 달라진다"의 답은 **"미미하게, 행동으로 옮길 만큼은 아니다"** 다.

### 4.7 각도가 원인이 아닐 때 드러나는 방식

- angle ≤3pp인데 카메라 SD 9.9pp 유지 → 카메라 효과는 실재하나 **각도가 아니다**(장면 배치, 군중 밀도, 설치 높이, 렌즈 왜곡, 폐색 기하).
- probe rescue rate >50%이고 **모든 bin에서 균일** → 원인은 **프롬프트 재현율**. 86.4%는 카메라가 아니라 Gemini의 under-trigger.
- miss가 저해상도+야간에 집중 → 이미징 품질. 단 해상도는 카메라 수준에서 이미 null이라 가능성 낮다.
- 모든 축에서 `p0` 균일 → **카테고리 정의** 문제("violence"가 프롬프트 타겟으로 너무 모호).

## 5. FiftyOne 뷰

**성공 기준**: `plan_view`/`level_view`로 필터한 뷰를 열었을 때 miss 사례가 몰려 있고, 그 miss들의 공통 시각패턴(실루엣 붕괴/폐색)이 5~10건 나열만으로 눈에 보여야 한다. hit/miss를 나란히 놓는 뷰가 최소 1개 필요하다.

- **`frames`(187,994) 데이터셋에 주입하지 말고 별도 소형 per-video 데이터셋을 만든다.**
  - grain 일치: 1 sample = 1 video = 1 angle. `frames`는 per-frame이라 앵글이 중복 방송되고 뷰 상한 5000/187,994에서 육안 triage가 불가능하다.
  - `frames`는 `refresh_frames_labels.py`(2h cron)가 의존한다 — 과거 오버랩으로 호스트를 스왑 쓰래싱시킨 이력이 있어 건드릴 이유가 없다.
- 주입은 **증분**만: `ds.set_values("camera_angle", {...}, key_field="id")` (`enrich_frames_captions.py` 선례). **전체 재빌드 금지** — `fiftyone_full_build.py`는 `RESUME=0`에서 `delete_dataset`을 먼저 실행하고, 188K 미디어 재fetch + UMAP ×2로 수 시간이다.
- 필드: `camera_angle` / `subject_scale` / `occlusion_state` / `angle_method` + 분석용 `prompt_outcome`(hit/miss/**not_asked**) · `outcome_stage`(gemini_event/sam3) · `diag_sample_tag`.
  - **`not_asked`가 없으면 miss와 "그 소스가 애초에 그 카테고리를 안 물어봄"이 섞여 인식률이 오염된다.**

| 뷰 | 구성 | 목적 |
|---|---|---|
| 핵심 대조 | 카테고리 고정 → `camera_angle` group-by → 그룹 내 `prompt_outcome` 정렬 | 이 앵글에서 miss가 몰리는지 한 스크롤로 |
| 3×2 매트릭스 | `camera_angle`(3) × `prompt_outcome`(hit/miss), 셀당 15~30 → 총 ≤180장 | 셀별 표본 수 자체가 1차 신호(빈 셀 = 강한 정황) |
| 교란 점검 3종 | `camera_angle` 고정 + `daynight_type`/해상도 tier/`source_unit_name` 재분할 | 쏠림이 "사실은 야간/저해상도/특정 고객사"가 아닌지 육안 반증 |
| (선택) 임베딩 오버레이 | frame 임베딩 187,994(PE-Core-L14-336) UMAP에 `camera_angle` 색 | 앵글이 임베딩 공간에서도 분리되면 라벨 노이즈가 아니라는 약한 방증 |

## 6. 하지 않는 것 (명시적 범위 제외)

| 항목 | 판정 | 이유 / 재검토 조건 |
|---|---|---|
| **카메라 식별자 정규화 (소스별 regex 96종)** | **하지 않음 — 최대 함정** | 앵글은 픽셀에서 추론하므로 카메라 정체성이 라벨링에 불필요하다. 검증(§4)에는 필요하지만 그건 **상위 5개 소스만** 수작업으로 충분. 조건부 재검토: per-camera 프롬프트 라우팅을 실제로 결정한 후 |
| `video_metadata` wide-table 정규화 동반 | 하지 않음 | 독립 L 항목. 진단에 L을 묶는 게 스프린트를 먹는 전형 |
| 신규 sensor | 하지 않음 | 반응할 이벤트 없음. 스케줄로 충분 |
| Streamlit 페이지 신설 | 하지 않음 | 소비자는 FiftyOne group-by |
| `labeling_specs` 활용 | 하지 않음 | prod 0행 + `source_unit_name` UNIQUE 없음 = 미검증 표면 위 축조 |
| 앵글 confidence·다중프레임 투표 | 조건부 | `indeterminate` 비율이 높게 나오면 `ANGLE_FRAMES=3` |
| `frames` 데이터셋에 `camera_angle` 주입 | 조건부 | 별도 데이터셋으로 신호 확인 후, cross-analysis가 실제로 필요할 때만 |
| **앵글 라벨 사람 검수 (~160 카메라 전량)** | **필요** | §4.4. 이게 없으면 진단 전체가 검증되지 않은 라벨러 위에 선다. 30~40분 작업 |
| PTZ 카메라 존재 여부 확인 | 필요 | PTZ가 섞이면 "고정 각도" 가정 자체가 깨진다. 현재 미확인 |

## 7. 축별 검증 결과 — 4/6 축 사용가능, `camera_angle` 만 블로커 (2026-07-29 실측)

**결론: Places365 대체(실내외·주야·날씨)는 검증 통과. `camera_angle` 축만 여전히 못 쓴다.**

### 육안 대조 (읽을 수 있는 영상 12편, 소스 12종, 사람이 프레임을 직접 보고 채점)

| 축 | 정확도 | 판정 |
|---|---|---|
| `environment_type` | **11/12** (건설장비 클로즈업 1편만 outdoor→indoor) | ✅ 사용가능 |
| `daynight_type` | **12/12** | ✅ 사용가능 |
| `weather` | **11/12** (실내 1편이 `clear` — `not_applicable` 이어야) | ✅ 사용가능 |
| `subject_scale` | 명백한 오류 없음 (별도 정량 채점 안 함) | 🟡 잠정 |
| `occlusion_state` | 명백한 오류 없음 (별도 정량 채점 안 함) | 🟡 잠정 |
| `camera_angle` | **1/12** — 12편 중 11편이 `level_view` | ⛔ **블로커** |

`camera_angle` 오답 예: 지하철 에스컬레이터 위, 차고 위, 정원 위 — 명백히 내려다보는 `oblique_view` 인데 전부 `level_view`.

### `camera_angle` 프롬프트 3변형 모두 붕괴 (동일 표본)

| 변형 | 결과 |
|---|---|
| ① 도(degree) 기반 4-bin (초기) | 40~50° 벽부착 → `overhead` 과대분류 |
| ② 증거 기반 3+1-bin (`plan_view` 먼저 나열) | `plan_view` **10/12 (83%)** |
| ③ ② + 순서 교정 + `plan_view` 하드 부정게이트 | `level_view` **11/12 (92%)** |

- 육안 대조 2편(둘 다 명백히 `oblique_view`)이 ②③ 모두 오답 — **오답 방향만 바뀜.**
- CCTV 는 대부분 `oblique_view` 여야 정상이므로 83~92% 쏠림은 데이터가 아니라 **라벨러 편향**이다.
- **기각기준 A(§4.5)가 라벨러에 대해 발동**했다. 단 이는 "각도 가설이 틀렸다"가 아니라 **"현재 도구로는 가설을 시험할 수 없다"** 는 뜻이다.
- 같은 호출·같은 프레임에서 다른 4축은 11~12/12 로 잘 맞는다 → **모델 전반의 무능이 아니라 tilt 판정 특유의 실패**다.

### 측정 (동일 표본 12편, source_unit_name 12종 분산, 각 영상 1프레임)

| 프롬프트 변형 | 결과 분포 | 판정 |
|---|---|---|
| ① 도(degree) 기반 4-bin (초기 Qwen 클라이언트, 현재 삭제) | 40~50° 벽부착 → `overhead` 과대분류 (3편 육안대조) | 실패 |
| ② 증거 기반 3+1-bin (§1, `plan_view` 먼저 나열) | `plan_view` **10/12 (83%)** / oblique 1 / level 1 | 실패 |
| ③ ② + 순서 교정(`oblique_view` 먼저) + plan_view 하드 부정게이트 | `level_view` **11/12 (92%)** / oblique 1 | 실패 |

- 육안 대조 2편(EV충전구역·야간 도로, 둘 다 명백히 `oblique_view`)이 ②에서 `plan_view`, ③에서 `level_view` → **두 변형 모두 오답, 오답 방향만 바뀜.**
- CCTV 현실상 통상 천장·벽 부착 카메라는 대부분 `oblique_view`여야 한다. 83~92%가 한 bin에 쏠린 건 데이터가 아니라 **라벨러 편향**이다.
- **기각기준 A(§4.5)가 라벨러에 대해 발동**했다: 한 bin이 >80%면 노출 대비가 없어 답할 수 없다. 단 이는 "각도 가설이 틀렸다"가 아니라 **"현재 도구로는 가설을 시험할 수 없다"** 는 뜻이다.

### 다음 수단 (ponytail 순 — 싼 것부터)

1. **few-shot** — 프롬프트에 bin별 예시 프레임 2~3장을 인라인. 이 종류의 bin 붕괴에 가장 흔히 듣는 처방이고 서버·코드 변경 0. 먼저 시도할 것.
2. **Gemini 2.5 Flash 로 교체** — 이미 파이프라인에 배선된 의존성(`src/gemini/`, Vertex `your-gcp-project`/`us-central1`)이고 7B AWQ보다 시각추론이 훨씬 강하다. 130k × 프레임 1장이면 비용도 미미. `angle_method` 가 provenance를 담으므로 라벨러 교체는 스키마 변경 없이 된다 — **이 설계의 유일한 실질 변경점은 `lib/video_angle.py` 의 백엔드 한 곳**이다.
3. **pairwise 비교** — VLM은 절대 분류보다 A-vs-B 비교에 강하다. 기준 프레임 대비 "어느 쪽이 더 위에서 내려다보는가"로 정렬 후 컷.
4. **사람 GT 우선** — §4.4 결론대로 분석 단위가 ~160 카메라면 사람이 30~40분에 100% 라벨링한다. 1~3이 다 실패해도 진단 자체는 진행 가능하다. **어느 경로든 이 GT는 라벨러 검증용으로 필요하다.**

### 롤아웃 게이트 (축별로 다르다)

| 대상 | 게이트 |
|---|---|
| `environment_type` / `daynight_type` / `weather` | **통과** — `video_scene_backfill` 을 켜도 된다. Places365 는 계속 일시정지 |
| `camera_angle` / `subject_scale` / `occlusion_state` | 위 1~4 중 하나로 **육안 GT 50편 정확도 확인 + 최대 bin 점유율 <70%** 일 때까지 신뢰 금지 |

⚠️ 한 호출이 6축을 한꺼번에 쓰므로 백필을 켜면 `camera_angle` 에도 미검증 값이 들어간다. `angle_method` 가 그 값을 `qwen2.5-vl` 로 표시하므로 **나중에 앵글만 다른 라벨러로 재처리 가능**하다 (`env_method` 와 분리해 둔 이유). 앵글 축을 신뢰해 분석에 쓰기 전에는 반드시 위 게이트를 통과시켜라.

인라인 경로(`INGEST_DEFER_VIDEO_SCENE_CLASSIFICATION=false`)를 켜면 신규 유입분이 ingest 시점에 분류되고, Places365 가 일시정지 상태이므로 `environment_type`/`daynight_type`/`env_method` 소유권이 Qwen 으로 넘어간다.

## 8. 열린 질문

1. `labels.category` / dispatch `categories` 실제 문자열이 `falldown`인지 `fall`인지 소스별 확인 — 기본(백워드호환) 프롬프트는 예시가 `"fall"`이다 ([gemini_prompts.py](../../src/vlm_pipeline/lib/gemini_prompts.py) L106).
2. 빈 `source_unit_name` 17,228행의 유입 경로와 대체 식별자.
3. `source-a-rtsp-bucket` 불일치 — PG 이벤트 행 824 asset vs MinIO events JSON 57개.
4. 표본 내 PTZ 카메라 존재 여부.
5. `environment_type` 86.1% deferred 백로그의 원인(스케줄 STOPPED 여부) — 앵글 백필이 같은 함정에 빠지지 않게 선확인.
