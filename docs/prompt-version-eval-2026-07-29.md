# source-h 프롬프트 뱅크 버전 비교 — v1.0.8.0 vs v1.0.8.4

- 생성: 2026-07-30 01:18:22 (KST)
- 대상: MinIO `source-h/` + `source-h/` 두 prefix 전용, 영상 871편
- 인코더: PE-Core-L14-336 `/embed_text` (userwatch 뱅크 `feature` 와 cosine=1.000000 동일)
- 결정 규칙: 클래스 점수 = max over (3키프레임 × 그 클래스 프롬프트) cosine → argmax
- GT: **폴더명 파생 weak GT** (사람 검수 아님) — helmet→normal, falldown/fire/smoke

## 1. 프롬프트 뱅크 커버리지

| class | 의미 | v1.0.8.0 프롬프트 | v1.0.8.4 프롬프트 | 데이터(영상) |
|---|---|---|---|---|
| 0 | normal | 10,703 | 8,625 | 370 |
| 1 | falldown | 160 | 3,000 | 10 |
| 2 | fire | 573 | 2,250 | 61 |
| 3 | smoke | 1,044 | 2,250 | 430 |
| 4 | smoking | 0 | 0 | 0 |
| — | 합계 | 12,480 | 16,125 | 871 |

> class 4(smoking)은 **두 버전 모두 프롬프트 0개** → 구조적으로 예측 불가.
> source-h 데이터에도 smoking 폴더가 없어 이번 비교의 커버 범위는 class 0–3 이다.

## 2. 전체 정확도

| 버전 | 규칙 | 정확 | n | accuracy | 95% CI (Wilson) |
|---|---|---|---|---|---|
| v1.0.8.0 | max (제품) | 580 | 871 | 66.590% | 63.391% – 69.644% |
| v1.0.8.0 | top-10 평균 | 601 | 871 | 69.001% | 65.852% – 71.984% |
| v1.0.8.4 | max (제품) | 534 | 871 | 61.309% | 58.031% – 64.487% |
| v1.0.8.4 | top-10 평균 | 536 | 871 | 61.538% | 58.264% – 64.712% |

**클래스 균등 관점 (macro recall — 클래스별 recall 의 단순평균)**

| 집계 | v1.0.8.0 | v1.0.8.4 | Δ |
|---|---|---|---|
| macro recall (전 4클래스) | 61.3% | 65.0% | +3.7%p |
| macro recall (n≥30 클래스 3개만) | 51.7% | 63.3% | +11.6%p |
| micro accuracy (=전체 정확도) | 66.6% | 61.3% | -5.3%p |

> **micro 와 macro 가 반대 방향이다.** source-h 은 절반이 smoke(430/871)라 micro accuracy 가 smoke 성능에 지배된다. 클래스를 균등하게 보면 v1.0.8.4 가 낫고, 이 데이터 구성 그대로 보면 v1.0.8.0 이 낫다 — 어느 쪽을 채택할지는 **운영 시 클래스 분포와 오탐 비용**이 결정한다.

## 3. 짝지어진 비교 (동일 871편, McNemar exact)

| 결과 | n |
|---|---|
| 둘 다 정답 | 452 |
| **v1.0.8.4 만 정답 (개선)** | 82 |
| **v1.0.8.0 만 정답 (퇴행)** | 128 |
| 둘 다 오답 | 209 |

순개선 = -46편 (-5.28%p), McNemar exact p = 0.00183

> 카메라 단위 clustering(design effect 9.22, 설계문서 §통계) 때문에 위 CI/p 는 **영상 독립 가정**의 낙관적 값이다. 카메라 수준 결론엔 그대로 쓰지 말 것.

## 4. 클래스별 (weak GT)

| GT class | n | v1.0.8.0 recall | v1.0.8.4 recall | Δ |
|---|---|---|---|---|
| 0 normal | 370 | 99.2% | 100.0% | +0.8%p |
| 1 falldown | 10 ⚠️n소 | 90.0% | 70.0% | -20.0%p |
| 2 fire | 61 | 9.8% | 62.3% | +52.5%p |
| 3 smoke | 430 | 46.0% | 27.7% | -18.4%p |

### 혼동행렬 — v1.0.8.0 (행=GT, 열=예측)

| GT \ pred | normal | falldown | fire | smoke |
|---|---|---|---|---|
| **normal** | 367 | 0 | 0 | 3 |
| **falldown** | 1 | 9 | 0 | 0 |
| **fire** | 45 | 0 | 6 | 10 |
| **smoke** | 227 | 0 | 5 | 198 |

### 혼동행렬 — v1.0.8.4 (행=GT, 열=예측)

| GT \ pred | normal | falldown | fire | smoke |
|---|---|---|---|---|
| **normal** | 370 | 0 | 0 | 0 |
| **falldown** | 3 | 7 | 0 | 0 |
| **fire** | 20 | 0 | 38 | 3 |
| **smoke** | 245 | 0 | 66 | 119 |

### 예측이 바뀐 패턴 (v1.0.8.0 → v1.0.8.4, 상위 10)

| GT | v1.0.8.0 예측 | v1.0.8.4 예측 | n | 판정 |
|---|---|---|---|---|
| smoke | smoke | normal | 80 | ❌퇴행 |
| smoke | smoke | fire | 43 | ❌퇴행 |
| smoke | normal | smoke | 43 | ✅개선 |
| fire | normal | fire | 29 | ✅개선 |
| smoke | normal | fire | 23 | 오답→오답 |
| fire | smoke | fire | 6 | ✅개선 |
| smoke | fire | normal | 4 | 오답→오답 |
| fire | smoke | normal | 3 | 오답→오답 |
| fire | fire | normal | 3 | ❌퇴행 |
| normal | smoke | normal | 3 | ✅개선 |

## 5. camera_angle (DAv2, migration 017)

| camera_angle | n | 비율 |
|---|---|---|
| non_plan | 870 | 99.9% |
| plan_view | 1 | 0.1% |

3프레임 라벨 불일치(프레임간 불안정): 4 / 871 (0.5%)

| camera_angle | v1.0.8.0 acc | v1.0.8.4 acc | n |
|---|---|---|---|
| non_plan | 66.6% | 61.3% | 870 |
| plan_view | 100.0% | 100.0% | 1 |

## 6. 커버리지 — 카메라(장소) 단위

> 설계문서 기준 분석 단위는 **카메라**다(ICC 0.075, design effect 9.22). 카메라는 파일명의 장소 토큰에서 파생했다(폴더별로 토큰 위치가 반대라 양쪽 레이아웃 처리).

> ⚠️ **카메라가 3곳뿐이다.** design effect 9.22 를 감안하면 유효 표본은 영상 871편이 아니라 사실상 클러스터 3개 수준이다 — §3 의 p 값은 이 사실을 반영하지 않는다.

| camera_id | n | 폴더 | v1.0.8.0 | v1.0.8.4 | Δ |
|---|---|---|---|---|---|
| area-b | 424 | falldown,fire,helmet,smoke | 58% | 34% | -24%p |
| area-a | 312 | falldown,fire,helmet,smoke | 82% | 85% | +3%p |
| ODCarea-a | 135 | falldown,fire,helmet,smoke | 57% | 90% | +33%p |

카메라 3곳 / 영상 871편.

### 카메라 × GT 교차표 — ⚠️ 카메라와 클래스가 교란되어 있다

| camera_id | normal | falldown | fire | smoke | 합 |
|---|---|---|---|---|---|
| area-b | 121 | 3 | 7 | 293 | 424 |
| area-a | 240 | 4 | 38 | 30 | 312 |
| ODCarea-a | 9 | 3 | 16 | 107 | 135 |

> 카메라마다 클래스 구성이 전혀 다르다. 따라서 위 카메라별 정확도 차이는 **카메라 난이도가 아니라 클래스 구성 차이**를 상당 부분 반영한다. 예: 최대 카메라의 v1.0.8.4 하락은 그 카메라에 smoke 가 몰려 있어서 생긴 결과에 가깝다. 카메라 효과를 보려면 클래스를 고정한 뒤 비교해야 한다(FiftyOne 에서 `ground_truth` + `camera_id` 를 함께 필터).

## 7. 커버리지 — 프롬프트 뱅크 실사용률

> 실제로 '1위'를 차지한 적이 있는 프롬프트 수 = 뱅크가 이 데이터에서 실제로 쓰인 정도.

| 버전 | 뱅크 크기 | 1위를 차지한 고유 프롬프트 | 사용률 |
|---|---|---|---|
| v1.0.8.0 | 12,480 | 81 | 0.65% |
| v1.0.8.4 | 16,125 | 113 | 0.70% |

**v1.0.8.0 최다 1위 프롬프트 (상위 8)**

| n | prompt |
|---|---|
| 227 | a clear view of the loading dock from a cctv camera |
| 136 | A security camera view displays a loading dock on the parking lot in the evening |
| 97 | Visible smoke in the upper-right corner around the warehouse in the evening. |
| 94 | the loading dock from the cctv camera |
| 28 | Docking area with empty parking spaces and debris, captured by a security camera and under clear weather |
| 21 | Visible smoke around the middle area around the storage room in the morning. |
| 14 | a CCTV view the warehouse remains still. |
| 14 | A few people notice smoke on the left side of the warehouse in the morning. |

**v1.0.8.4 최다 1위 프롬프트 (상위 8)**

| n | prompt |
|---|---|
| 306 | It is a warehouse. People are scattered throughout the area. The camera lens is dirty. |
| 63 | It is a construction site. The area is mostly empty. There are dust smudges on the camera lens. |
| 56 | It is a rooftop. The area is mostly empty. Vehicle headlights are shining. |
| 53 | It is a construction site. The area is mostly empty. Vehicle headlights are shining. |
| 30 | It is a warehouse. The environment looks typical. White smoke is spreading. |
| 25 | It is a parking lot. The area is mostly empty. Flames are burning. |
| 25 | It is a warehouse. Only a few people are visible. White smoke is spreading. |
| 19 | It is a warehouse. People are scattered throughout the area. The scene looks hazy overall. |

## 8. 데이터 무결성

- sha256 대조 대상: 871 / 871 (media 스테이지에서 검증)
- 카메라(파일명 파생) 고유 수: 3 → 영상/카메라 ≈ 290.3
- `ingest_status`: 871편 전부 `uploading` — `completed` 게이트 때문에 이 코호트는 정식 라벨링 파이프라인에서 누락된 상태다(기존 인시던트).
- `source-h/` prefix 객체 중 67건은 DB/원본과 **다른 바이트**(더 작음) → 미디어는 `source-h/<한글>`(871/871 일치)에서 읽었다. FiftyOne `08_lower_key_byte_mismatch` 뷰 참조.

## 9. FiftyOne

- URL: <http://10.0.0.10:5153/datasets/source-h>
- 샘플 = 영상 1편, 이미지 = 가운데 키프레임. `keyframe_paths` 에 3장 경로.
- 핵심 필드: `outcome`(both_correct/only_v1.0.8.4/only_v1.0.8.0/both_wrong), `correct_v1_0_8_*`, `score_v1_0_8_*_<class>`, `margin_v1_0_8_*`, `conf_delta`, `camera_angle`, `tilt_deg`, `angle_votes`.
- saved views 01~09 (불일치/개선/퇴행/둘다오답/각도교차/저마진/falldown/바이트불일치/각도불안정)
- `eval_v1_0_8_0` · `eval_v1_0_8_4` evaluation → App 에서 혼동행렬 패널.
- 브레인 키: `emb_viz`(이 배포 관례) + `umap`(별칭, 동일 좌표) + `text_search`(prompt-capable — App 검색바에 임의 문장을 넣으면 코사인 랭킹).
- 프롬프트 유사도: `class_best_*`(클래스별 1위 프롬프트+코사인), `top10_*`(뱅크 전체 최근접 10), `top10_text_*`(평문). CSV: `sourceh_top_prompts.csv`(17,420행), `sourceh_class_scores.csv`(6,968행).
- **필터 사이드바: 저장된 뷰 `00_analysis` 를 진입점으로 쓸 것.** 노이즈 21필드를 `exclude_fields` 로 제외해 렌더 필드가 77 → 56 개로 줄어든다(2026-07-29 DOM 실측).
  > ⚠️ `app_config.sidebar_groups` 에서 경로를 빼는 것만으로는 **숨겨지지 않는다** — FiftyOne 1.19 는 미배정 필드를 자동 생성 `PRIMITIVES` 그룹에 모아 맨 아래에 붙인다. sidebar_groups 는 **그룹핑·순서**만 통제하고, 실제 제거는 뷰의 `exclude_fields` 뿐이다. `metadata`/`id`/`filepath`/`created_at`/`last_modified_at` 는 기본 필드라 제외 자체가 거부된다.
- 사이드바 그룹: ① 판정 / ② trade-off / ③ 버전차 근거 / ④ 층화·교란 (여기까지 펼침) / ⑤ 예측 상세 / ⑥ 원점수(버전간 직접비교 금지) / ⑦ 프롬프트 상세 / ⑧ QA·무결성 / ⑨ 조회 키·provenance. 썸네일 칩은 ground_truth/camera_angle/pred×2/outcome 만.

### trade-off 를 보는 방법

| 도구 | 무엇이 보이나 |
|---|---|
| 브레인 키 `tradeoff_viz` | **before/after 산점도** — x=v1.0.8.0, y=v1.0.8.4 의 GT클래스 상대점수. 대각선 위=개선 / 아래=퇴행 / 좌하=둘 다 못 맞춤. 올가미 선택 → 이미지 즉시 표시 |
| 워크스페이스 `tradeoff` | 위 산점도 + Samples 좌우 분할, outcome 색 |
| 필드 `transition` | 값별 개수가 **곧 전이표**(21종). 클릭하면 해당 전이 샘플만 필터 |
| 필드 `gt_rel_delta` | GT클래스 상대점수의 버전차. 정렬하면 가장 많이 잃은/얻은 순 |
| 뷰 `10~13` | 예측이 바뀐 242편 / 잃은 것 / 얻은 것 / smoke→fire 66편 |

#### GT 무관 — 버전이 달라지면서 예측이 바뀐 것 (`pred_shift`)

> 정답 여부를 개입시키지 않고 **변화 자체**만 본다. GT 접두어가 없어 같은 변화가 쪼개지지 않는다(11범주 vs `transition` 21범주).

| n | 예측 변화 |
|---|---|
| 86 | smoke→normal |
| 52 | normal→fire |
| 49 | smoke→fire |
| 45 | normal→smoke |
| 7 | fire→normal |
| 2 | falldown→normal |
| 1 | fire→smoke |

바뀜 242편 / 유지 629편. 지배적 변화는 **smoke→normal**(검출 상실)과 **normal→fire·smoke→fire**(fire 과검출 방향)로, v1.0.8.4 가 전반적으로 **fire 쪽으로 기울고 smoke 를 놓치는** 방향으로 이동했다.

> FiftyOne: 사이드바 `② 버전변화 (GT 무관)` 의 `pred_shift` 값별 개수가 위 표다. 브레인 키 `shift_viz`(x=옛 답에서 멀어진 정도, y=새 답으로 당긴 정도) + 워크스페이스 `shift`, 뷰 `14~16`(가장 많은 변화 top-3).


#### ★ 변화의 **방향**이 크기보다 강한 신호다 (`shift_direction`)

| 방향 | n | v1.0.8.0 | v1.0.8.4 | 순변화 |
|---|---|---|---|---|
| 회수 (normal→이벤트) | 97 | 0.0% | 74.2% | +74.2%p |
| 오분류 (이벤트→다른이벤트) | 50 | 86.0% | 14.0% | -72.0%p |
| 상실 (이벤트→normal) | 95 | 89.5% | 3.2% | -86.3%p |
| 변화없음 | 629 | 71.9% | 71.9% | +0.0%p |

> **회수**(v1.0.8.0 이 normal 로 놓친 것)에서 v1.0.8.4 가 압도적으로 이기고, **상실·오분류**(이미 이벤트로 잡던 것)에서 압도적으로 진다. 크기(`shift_mag`)보다 방향이 채택 여부를 결정한다.
> 근거: 회수 전환의 v1.0.8.0 정답률은 정의상 0% — normal 예측이 이벤트 GT 에 맞을 수 없기 때문이다. 반대로 상실·오분류 전환은 v1.0.8.0 이 이미 86~90% 맞히고 있었다.

**운영 규칙 비교** (저마진 전환 Q1·Q2 를 분석에서 제외한 750편 기준 — 그 구간은 마진이 v1.0.8.4 코사인 표준편차의 12~17% 로 노이즈·GT 품질에 지배된다):

| 규칙 | 정확도 | v1.0.8.0 대비 | v1.0.8.4 대비 |
|---|---|---|---|
| v1.0.8.0 단독 | 65.5% | — | −4.0%p |
| v1.0.8.4 단독 | 69.5% | +4.0%p | — |
| **`회수` 전환만 채택** | **73.3%** | **+7.9%p** | **+3.9%p** |
| 크기규칙 (`shift_mag` ≥ 0.022) | 70.9% | +5.5%p | +1.5%p |

> 즉 **v1.0.8.4 를 쓰되, 이미 이벤트로 잡힌 것을 다른 이벤트나 normal 로 바꾸는 전환은 보류**하는 것이 최선이다. FiftyOne 뷰 `17_dir_recover`(97) / `18_dir_lose`(95) / `19_dir_swap`(50) / `20_analysis_scope`(750) 로 바로 검수할 수 있고, 워크스페이스 `shift-direction` 은 이미지 임베딩 공간에서 회수/상실이 어디에 몰렸는지 보여준다.

> ⚠️ 전체 871편 기준으로는 v1.0.8.4 가 −5.3%p 지만, 저마진 전환 121편을 빼면 **+4.0%p** 로 부호가 뒤집힌다. 어느 범위를 쓰는지 밝히지 않은 단일 수치는 이 데이터셋에서 무의미하다.


#### 변화가 이미지 임베딩 공간에서 체계적인가 (연관도 수치)

> `dscore_pred_*` 는 이미지 임베딩과 **독립된 값이 아니라 그것의 함수**다(`cos(e, 프롬프트)` 의 차이). 그래서 '두 값의 비교'가 아니라 **'변화가 임베딩 공간에서 몰려 있나'** 를 재는 것이 맞다. 아래는 그 연관도다.

| 지표 | 값 | 해석 |
|---|---|---|
| 이미지 임베딩 → `pred_changed` 예측 AUC (5-fold) | **0.866** ± 0.029 | 0.5=이미지로 전혀 예측 불가 / 1.0=완전히 예측 가능 |
| `pred_shift` kNN(k=10) 이웃 동질성 | 0.702 | 무작위 기대 0.416 → **1.69배** |

→ 버전 변화는 무작위 잡음이 아니라 **특정 시각적 영역에 집중**돼 있다. FiftyOne 에서 브레인 키 `emb_viz` + Color by `shift_mag_q.label`(워크스페이스 `shift-where`)로 **어떤 화면이 흔들렸는지** 직접 볼 수 있다.


#### 정답 기준 `transition` 상위 전이 (예측이 바뀐 242편):

| n | 전이 | 판정 |
|---|---|---|
| 80 | GT smoke : smoke→normal | ❌퇴행 |
| 43 | GT smoke : smoke→fire | ❌퇴행 |
| 43 | GT smoke : normal→smoke | ✅개선 |
| 29 | GT fire : normal→fire | ✅개선 |
| 23 | GT smoke : normal→fire | 오답→오답 |
| 6 | GT fire : smoke→fire | ✅개선 |
| 4 | GT smoke : fire→normal | 오답→오답 |
| 3 | GT fire : smoke→normal | 오답→오답 |

- 워크스페이스 `angle-explore`(색=tilt_bin.label) · `outcome-explore`(색=outcome): Samples ↔ Embeddings 좌우 분할. 기본 레이아웃은 둘이 **탭**이라 점을 골라도 이미지를 동시에 못 본다.

> ⚠️ **Embeddings 패널 Color by 함정 2개** (실측):
> 1. Classification 필드는 **`.label` 서브경로 필수** — `tilt_bin` 은 `null`, `tilt_bin.label` 은 정상. `camera_angle`/`ground_truth`/`pred_*` 모두 동일.
> 2. 연속 float 은 색이 안 나온다 — `tilt_deg` 는 고유값 628개라 카테고리 색상 생성이 실패하고 무의미한 컬러바만 뜬다. 그래서 5도 구간 `tilt_bin`(7개 범주)을 만들었다.
