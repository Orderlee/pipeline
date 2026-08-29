# 프롬프트 × 이미지 임베딩 분석 — 접근법과 데이터 설계 (2026-08-26)

> 목적 4가지: ① image↔prompt 임베딩 연관성 ② 클러스터(카테고리)별로 잘 붙는 프롬프트 ③ 그런 프롬프트를 만드는 방법 ④ 프루닝·품질·큐레이션.
> 이 문서는 **실측(2026-08-26)** 과 그 위에서 나온 설계다. 숫자의 출처 스크립트는 `docker/analysis/` 에 있다.

## 0. 먼저 알아야 할 제약 (실측)

| 사실 | 값 | 의미 |
|---|---|---|
| sourcei GT 유효표본 | deff 232, ICC 0.51 → **≈32** (겉보기 7,498) | 뱅크 간 비교는 GT 로 판정 불가. 규칙 비교만 가능 |
| 클래스↔카메라 교락 | 카메라 15 중 7개 단일 클래스, fire 는 4카메라·20이벤트 | "뱅크가 좋다"와 "카메라가 쉽다"를 못 가름 |
| 지도학습 상한 | 선형 프로브 0.364 ≈ zero-shot 0.35 (카메라 홀드아웃) | 라벨 잡음·카메라 이질성이 천장. 프롬프트 탓이 아님 |
| 친화도 행렬 적재 범위 | 9현장 55군집만 (fire_smoke·cohort-b·appdata·loc-c·ax 없음) | 화재 현장이 빠져 있어 목적 2 의 fire 답이 편향 |

→ **결론: 목적 1·2·4 는 GT 없이 풀어야 하고, 풀 수 있다.** GT 는 마지막 검증에서 "규칙"과 "쌍대 상대 비교"에만 쓴다.

## 1. 목적 1 — image ↔ prompt 임베딩은 어떻게 연결돼 있나

### 실측 (`embed_geometry.py`, 배경 20,000 프레임 × 121,614 문장 = 2.4G 셀)
- 코사인 평균 **0.121**, SD 0.042. 이미지↔이미지 0.515, 문장↔문장 0.586 → **모달리티 갭(cone)**: 두 모달리티가 각자 좁은 원뿔에 모여 있고 서로는 멀다. 절대 코사인은 의미 없고 **상대 순위**만 의미 있다.
- 이원 분산분해: **프레임 주효과 28.8% / 문장 주효과 20.0% / 상호작용 51.3%**.
  - 프레임 주효과 = "이 프레임은 어떤 문장이든 잘 붙음"(밝기·구도). 판별 정보 0.
  - 문장 주효과 = "이 문장은 어떤 프레임이든 잘 붙음"(일반 문장). 판별 정보 0. **max 풀링(argmax/top-K)이 이걸 증폭**한다 — 대용량 뱅크가 많이 쏘는 이유.
  - 상호작용 51% 만이 클래스를 가른다.
- 라벨-free 정규화 실측(SAM3 약참조, v1.0.8.0 argmax): 문장 z-정규화 → smoke→fire 혼동 0.42→0.34 개선 but 오탐 1.08→2.05% 악화; 이미지 중심 제거 → fire 재현율 0.849→0.872, 오탐 1.6%. **공짜 점심 없음 — 정규화는 임계와 함께 GT 로 튠해야 한다.**

### 방법론
1. **분산분해를 표준 진단으로**: 뱅크/현장이 바뀔 때 상호작용 비율이 오르면 좋아진 것. 라벨 불필요.
2. **문장 주효과 제거**: 문장별 배경 평균 μ_j (배경 프레임 20k 에서) 를 저장해 두고 c'(i,j)=c(i,j)−μ_j 로 채점. 이게 "목소리 큰 문장" 억제의 최소 형태.
3. **프레임 주효과 제거**: 프레임별 전체 문장 평균을 빼는 것은 이미 분포-IoU 의 per-frame adaptive binning 이 하고 있는 일 — IoU 규칙이 이론적으로 옳은 이유.

## 2. 목적 2 — 클러스터별로 어떤 프롬프트가 붙나

### 실측 (`cluster_prompt_affinity.py`, sentence_affinity 121,614 × 55)
- 특이도 z(문장, 군집) = 그 군집에서의 편차를 군집 내 표준화. 클래스별 상위 20 평균으로 "붙는 강도" 산출 → `19_cluster_class_attachment.csv`, 상위 문장 → `20_cluster_top_sentences.csv`.
- **결정적 발견: 특이도는 장소 어휘가 지배한다.** fire 1위 군집(ktt_loc-d#10, 창고 선반)에 붙는 fire 문장은 "fire near storage shelves on the right inside a warehouse" — 불이 아니라 **선반·창고**에 붙었다. 같은 군집의 normal 문장 "A CCTV view shows a warehouse aisle" 이 z 5.17 로 **더 강하게** 붙는다. falldown 1위(vietnam#8, 계단)도 "It is a staircase … slumped" — 계단이 붙인 것.
- 즉 **군집 평균 친화도로는 "이벤트를 잡는 문장"과 "그 장소를 잡는 문장"을 구별할 수 없다.** sourcei GT 에서 "장면 선행 템플릿(It is a staircase…)이 오탈취" 로 나온 것과 같은 현상.
- smoke 1위(vietnam#14)는 "man holds pencil near mouth … smoke" — 흡연 문장. 같은 군집 normal 상위가 "holding a lit cigarette"(z 5.66) — **클래스 상충**(smoking 이 어떤 뱅크엔 normal). 클래스 정의부터 흔들린다.

### 방법론 — 필요한 것은 "군집 × (이벤트 vs 정상) 대조"
군집 안에서 **이벤트 프레임과 정상 프레임을 갈라서** 각각의 친화도를 내고 그 차이를 봐야 한다. 차이가 큰 문장 = 그 군집에서 이벤트를 잡는 문장, 차이가 0 인데 절대값이 높은 문장 = 장소 문장(양쪽 다 켜짐 → 삼킴 위험).
- 갈라 주는 라벨: SAM3 검출(약참조, 전 프레임), 이벤트 윈도우(labels 테이블 시간구간), GT(sourcei).
- 이걸 위해 **sentence_affinity 를 (군집 × 참조클래스) 로 분할 적재**해야 한다 (§5 스키마).

## 3. 목적 3 — 이벤트를 잡는 프롬프트를 만드는 방법

실측에서 일관되게 나온 규칙 (sourcei GT hit/trap + frames SAM3 + 친화도 특이도):
1. **이벤트 문장 = 현상 선행 + 최소 장소.** "A tiny fire appears as a bright point on the floor" / "Intense flames are shooting up" 형이 이김. 장소 명사는 1개, 문장 앞에 두지 않는다("It is a staircase. …" 금지).
2. **normal 문장 = 카메라·장소 서술만, 사람 자세 금지.** lying/crouching/bending/sweeping 을 normal 에 넣으면 이벤트를 삼킨다(sourcei 누락 1등 문장 16개 전부).
3. **대조 쌍으로 생성한다.** 같은 장소 골격에 "with visible flames" / "no flame, clear air" 를 붙여 **문장 주효과를 상쇄**시킨 쌍을 만든다. 쌍의 차 벡터가 곧 그 군집의 이벤트 방향.
4. **생성 → 프로브 → 채택** 을 자동화: 후보 문장을 `/embed_text` 로 임베딩 → (a) 문장 주효과(배경 평균) 계산 → (b) 군집 × 참조클래스 대조 친화도 → (c) 대조차 ≥ 임계 & 배경 평균 ≤ 임계 인 것만 채택. GT 없이 돈다.
5. **클래스당 수십 문장.** hit 의 50% 를 8~36문장이 냈다. 수천 문장은 주효과만 키운다.
6. **현장군 팩.** sourcei(점 불꽃·바닥)와 frames(큰 불꽃·실내 구석)가 반대였다. 하나로 합치지 말 것.

## 4. 목적 4 — 프루닝 · 품질 · 큐레이션

### 프루닝 (라벨-free, 즉시 가능)
| 기준 | 계산 | 제거 대상 |
|---|---|---|
| 문장 주효과 | 배경 20k 프레임 평균 코사인 μ_j 상위 | 어디서나 켜지는 문장 (오탐 공급원) |
| 특이도 부재 | 군집 간 편차 SD 하위 | 아무 군집에도 안 붙는 문장 (기여 0) |
| 대조 부재 | 군집 내 (이벤트−정상) 친화도 차 ≈ 0 인데 절대값 높음 | 장소 문장으로 위장한 이벤트 문장 (삼킴) |
| 중복 | 문장 임베딩 코사인 > 0.95 쌍 | 한 쌍에서 주효과 큰 쪽 |
| 클래스 상충 | 뱅크 간 클래스 다수결 비율 < 0.7 | 정의부터 다시 (smoking/normal) |

### 품질 (GT 소량으로)
- **쌍대 게이트만**: 후보 뱅크 vs 현 뱅크를 같은 카메라에서 재고 카메라 군집 부트스트랩 CI. 점추정 순위표 금지(1위가 9종으로 흔들림).
- **규칙은 확정 가능**: top-K ≥ argmax, IoU 는 임계 재캘리브레이션(0.15→0.45 대) 후 최선. 이것은 CI 로 유의.

### 큐레이션 루프
```
후보 생성(대조 쌍) → /embed_text → 주효과·특이도·대조 산출(라벨-free)
   → 통과분만 뱅크 초안 → 쌍대 GT 게이트(카메라 CI) → 현장군 팩으로 발행(prompt_banks)
   → 프로덕션 발화 로그 축적 → 불일치 셀 사람 감사 → GT 증분 → 다음 라운드
```

## 5. 데이터 · DB 를 어떻게 만들어야 하나

### 5-1 지금 당장 부족한 것
| 부족 | 왜 필요 | 어떻게 |
|---|---|---|
| **친화도 행렬의 (군집 × 참조클래스) 분할** | 목적 2 의 핵심. 현재는 군집 평균만 있어 장소/이벤트 구별 불가 | `sentence_affinity` 에 `ref_class` 컬럼 추가(SAM3 normalized_class / 이벤트 윈도우 / GT 중 출처 표기) |
| **fire 현장 친화도 적재** | 9현장만 있음. fire_smoke·cohort-b·appdata 누락 | prompt_cos_db affinity 스텝 잔여 실행 (디스크 12GB 확보 후) |
| **문장 주효과 테이블** | 프루닝 1순위 기준 | `sentence_background(content_hash, mu_bg, sd_bg, n_bg, bg_sample_id)` — 배경 표본은 고정·버전 관리 |
| **이벤트 단위 GT** | 프레임 GT 는 윈도우 라벨(deff 232) | 평가 단위를 `(src_video, event_index)` 로; onset/offset 프레임 표시 |
| **카메라 다양성** | 15대·7대 단일 클래스 | 라벨 예산을 "카메라당 20~30 이벤트 × 60~80대"로. 프레임 수 늘리기 금지 |
| **불일치 감사 큐** | 정보량 최대 지점 | 프롬프트↔SAM3 불일치(fire_smoke 439), 31뱅크 불일치 4,535 → 사람 검수 |
| **클래스 정의 정본** | smoking/normal 상충 2,106 | `label_ontology.json` 에 smoking 처리 확정 → 뱅크 재라벨 |

### 5-2 제안 스키마 (analysis 스키마, Postgres)
```sql
-- 문장 주효과 (배경 표본 고정)
CREATE TABLE analysis.sentence_background (
  content_hash text NOT NULL, bg_sample_id text NOT NULL,   -- 예: 'frames_neg_20k_seed0'
  n_bg int NOT NULL, mu_bg real NOT NULL, sd_bg real NOT NULL,
  PRIMARY KEY (content_hash, bg_sample_id));

-- 군집 × 참조클래스 대조 친화도  (기존 sentence_affinity 의 확장)
CREATE TABLE analysis.sentence_affinity_ref (
  content_hash text NOT NULL, group_kind text NOT NULL, group_key text NOT NULL,
  ref_source text NOT NULL,      -- 'sam3' | 'event_window' | 'gt'
  ref_class text NOT NULL,       -- normal|falldown|fire|smoke
  n_frames int NOT NULL, mean_cos real NOT NULL, p90_cos real NOT NULL,
  PRIMARY KEY (content_hash, group_kind, group_key, ref_source, ref_class));
-- 대조 = mean_cos(ref_class=event) − mean_cos(ref_class=normal) 를 뷰로

-- 문장 품질 원장 (프루닝·채택 근거를 한 행에)
CREATE TABLE analysis.sentence_quality (
  content_hash text PRIMARY KEY, class_majority text, class_agreement real,   -- 뱅크 간 다수결 비율
  mu_bg real, specificity_sd real, contrast_max real, contrast_group text,
  dup_of text, hit int, trap int, selectivity real, computed_at timestamptz DEFAULT now());

-- 이벤트 단위 GT (프레임 GT 를 대체)
CREATE TABLE gt_events (
  event_id text PRIMARY KEY, src_video text, event_index int, camera text, site text,
  class text, onset_sec real, offset_sec real, onset_frame text, label_source text, reviewer text);
```

### 5-3 파이프라인
1. **배경 표본 고정**: frames 에서 SAM3 none/person 20,000 을 seed 고정으로 뽑아 `bg_sample_id` 부여. 모든 문장 주효과는 이 표본 기준.
2. **채점 커널 재사용**: `prompt_cos_db.py` 의 affinity 스텝을 (group × ref_class) 로 확장. 커널·상수(K=10, 80-bin) 는 그대로.
3. **문장 품질 원장 일일 갱신**: cron(prompt_cos_cron.sh) 에 `quality` 스테이지 추가 → `sentence_quality` upsert.
4. **뱅크 발행 게이트**: `prompt_banks.eval_summary` JSONB 에 (a) 라벨-free 3지표 통과율, (b) 쌍대 GT CI, (c) 현장군 팩 키를 기록. CI 가 0 을 포함하면 "발행하되 채택 아님".
5. **평가 단위 전환**: sourcei `ground_truth` → `gt_events` 로 재구성(789 이벤트). 지표는 이벤트 macro-F1 + 카메라 군집 CI.

### 5-4 하지 말 것
프레임 단위 CI · 뱅크 순위표 · 자동 프롬프트 탐색을 GT 로 채점 · SAM3 를 GT 로 취급 · 파인튠/비선형 프로브(라벨 15카메라로는 불가).
