# source-h 프롬프트 차이 시각화 — 문장 *정체성* 이 아니라 *품질* 로 칠한다

- 작성: 2026-07-31
- 대상: FiftyOne 데이터셋 `source-h` (13,144 프레임) / `docker/analysis/sourceh_prompt_geometry.py`
- 요청: "Embeddings 패널에서 Color by 로 프롬프트 뱅크 차이를 더 디테일하게 보고, 불필요한 것은 제거"

## 1. 원안과 그 반증

원안은 **승자 문장으로 이미지 UMAP 을 칠하기**(문장이 이미지 공간을 어떻게 분할하는지)였다.
`ai-modeler` 가 라이브 13,144장에서 직접 측정해 반증했다.

| 측정 | 값 | 함의 |
|---|---|---|
| UMAP 영역 분산 ↔ LOO 제거이득 spearman | **+0.13 (v080) / −0.10 (v084)** | 무상관. "흩어진 영토 = 나쁜 문장" 전제가 틀림 |
| 최악 문장 `"Visible smoke in the upper-right corner…"` (n=490, GT 전부 normal) | 원공간 응집도 0.966, UMAP 분산 0.60 | 나쁜 자석은 **조밀**하다 |
| v084 top-1 문장 점유율 | **43.3%** (top-2 = 50%) | 화면이 2~5색 |
| 승자문장 → 카메라 예측력 | **86.8% / 82.3%** (베이스라인 60.7%) | "프롬프트 영토" ≈ "카메라" |
| 두 뱅크 공통 문장 | **0개** | 문장 정체성 기준 색 범례를 공유 불가 → 토글 비교 원리적 불가 |
| top-K=20 컷이 `기타`로 묻는 것 | 이벤트 프레임의 40~51%, 저순도 문장의 78~90% | 관심 대상을 정확히 지움 |

`codex` 는 별개로, 원공간 삼각형 3스칼라(`cos(img,p080)`, `cos(img,p084)`,
**`cos(p080승자, p084승자)`**)가 2D 투영보다 직접적이라고 지적했다.

## 2. 채택 설계 — 품질 축은 뱅크 간 공유 범례가 된다

문장 정체성은 공유 불가지만 **문장 품질 스케일은 공유 가능**하다. 이 한 번의 치환으로
"토글 비교 불가" 제약이 풀린다.

### 2-1. 새 per-frame Color-by 필드 (전부 Classification, 저카디널리티)

| 필드 | 값 | 읽는 법 |
|---|---|---|
| `winner_purity_v080` / `_v084` | 5구간 (`0-25%`…`90-100%`) | 그 프레임을 이긴 문장의 **선언클래스 순도** = `(GT == 문장의 선언 class).mean()`. 낮은 색 영역 = 엉뚱한 클래스를 선언한 문장이 먹고 있음 |
| `winner_loo_v080` / `_v084` | 4구간 (`유해 +10↑` / `유해 +1~9` / `중립 0` / `유익`) | 그 승자 문장을 **지우면** 전체 정답이 몇 장 늘어나나 |
| `winner_pair_cos` | 5구간 (고정 임계) | `cos(v080승자, v084승자)`. 높음 = 같은 자리를 고쳐 씀, 낮음 = 딴 문장이 영토를 뺏음 |
| `camera` (기존) | 3 | **널 모델**. 위 그림들이 이것과 닮았으면 프롬프트 얘기가 아님 |

**선언클래스 순도**를 쓰는 이유: 다수결 순도는 위 최악 문장을 1.00 으로 평가한다(전부 normal
프레임이므로). 선언 기준으로는 0.00 이고, 그게 정확한 판정이다. 순도↔LOO spearman
= −0.54 / −0.38 (쓸만한 프록시). UMAP 분산은 +0.13 / −0.10 (쓸모 없음).

**넣지 않는 것**: 원공간 응집도 스칼라. 고정 카메라 3대라 동적범위가 0.90~0.99 로 사실상
상수이며 의미가 아니라 카메라를 인코딩한다.

### 2-2. `prune` 스테이지 (신규) — 위 필드의 계산 근거이자 삭제 의사결정 산출물

`stage_guide` 는 문장 **추가**의 counterfactual(FN 구조율/유발 FP)을 이미 계산한다.
**삭제**의 counterfactual 이 없었고, 이번 결론("개선의 98.6%가 경쟁 문장 소거")의 근거가
바로 그것이다.

- `bank_top2_stream(X, bank, drop=None)` — 클래스별 per-frame 1·2위 cosine + 1위의 클래스-로컬
  인덱스. `[batch, block]` 타일만 상주 (기존 `bank_best_stream` 패턴 확장). 뱅크당 ~5초.
- **LOO 제거이득**: 문장 p 를 지우면 그 클래스 점수가 클래스 내 2위로 떨어진다 → p 가 이기던
  프레임만 argmax 재계산 → `Δ(정답 수)`.
- **탐욕 그룹 제거**: LOO 는 근사 중복 문장이 서로 백업할 때 과소평가한다(실측: v080 의
  `red…smoke` 계열 103문장 개별 LOO 합 +7 인데 통째 제거 시 −0.04pp). 매 라운드 재적합하며
  최대 `PRUNE_ROUNDS` 회 반복, 곡선을 남긴다.
- 산출: `{GEO}/prune.json`, `{REPORT_DIR}/prune_<version>.csv` (문장별 승수·선언순도·LOO·최종채택).

예상 규모(실측 예고): v080 승자 201개 중 **34개 순유해 → +292장(+2.2pp)**,
v084 319개 중 **41개 → +141장(+1.1pp)**.

### 2-3. 문장 단위 산점도 (`sourceh_report_charts.py` c4)

x=승수(log), y=선언클래스 순도, 크기=|LOO 이득|, 색=선언 클래스, 뱅크별 2패널.
우하단(크고 더러움) = 우선 삭제. **랭킹 CSV 위의 편의물이지 대체물이 아니다.**

## 3. 같이 고치는 버그 3개 (신규 시각화보다 우선순위 높음)

1. **`stage_guide` 서사 숫자 하드코딩** (`sourceh_prompt_geometry.py:847-866`).
   문자열 리터럴 `1,541 / 16 / 1,520 / 458 / 444 / 13` 이 라이브 값 `1,548 / 17 / 1,526 / 452 / …`
   와 어긋나고, `v1.0.9.0` 으로 재실행하면 **"기준 뱅크: v1.0.9.0" 헤더 밑에 v1.0.8.4 숫자**가
   그대로 찍힌다. → `flips.json` 에서 읽어 포맷. `stage_flips` 가 `broken_reasons` 도 함께 덤프.
2. **`sourceh_bank_eval.sh` 루프에 `gap` 누락** (`analyze flips guide viz slim report`).
   그런데 slim 사이드바가 `gap_cluster`/`gap_deficit` 을 "다음 타깃"으로 노출하고 뷰
   `05~07_gap_*` 도 남아 **옛 버전의 미검출 군집이 조용히 표시**된다. → 루프에 `gap`·`prune` 추가.
3. **artifact 소유권 모순**: `stage_flips` 가 `why_text` 를, `stage_gap` 이 `v084_missed` 를
   매번 쓰는데 둘 다 `SLIM_DROP_FIELDS` 에 있다(쓰고→지우고→다시 쓰는 순환).
   codex 진단대로 순서 문제가 아니라 소유권 문제 → **각 스테이지가 애초에 안 쓰게** 하고,
   `stage_selftest` 에 불변식을 추가한다: *자기 소스에서 `ds.set_values("리터럴")` 로 쓰는 필드
   집합 ∩ `SLIM_DROP_FIELDS` = ∅*. 수동 매니페스트가 아니라 소스 자체를 검사하므로 드리프트가 없다.

같은 패턴의 낭비를 `stage_viz` 에서도 제거한다: `gt_cos_v080/v084`, `margin_quadrant`,
`margin_v084_bin`, `cover_viz` 는 **쓰자마자 slim 이 지운다**. 계산·쓰기 자체를 삭제.
(`v084_missed` 는 `gap_cluster is not None` 과 정확히 동치이고, 이름에 `v084` 가 박혀 있어
`BANK_B` 가 바뀌면 조용히 거짓말한다 — codex 지적.)

## 4. 제거 목록 (codex ↔ ai-modeler 이견은 실측 근거 우선으로 중재)

**삭제**

| 항목 | 근거 |
|---|---|
| `shift_mag_q` | 13,144 중 10,880(82.8%)이 "변화없음" 한 통. 존재 이유였던 `flip_confidence` 는 871영상 시절 필드로 이 데이터셋에 없음. 심각도 정렬은 `margin_delta` 담당 (뷰 30/31 이 실제로 그걸로 정렬) |
| `dscore_pred_v080` / `_v084` | 유일 소비자가 `shift_viz`. 자기/경쟁 분해는 `flip_reason` + `why_before/after` 가 담음 |
| `gt_rel_delta` | 코드 주석(791-792)에서 이미 기각됨(fixed 1,541 중 354건 역부호). `margin_delta` 가 대체 |
| `tilt_bin` | 고정 카메라 3대 = 카메라 프록시(두 bin 에 9,758장). 뱅크 A/B 는 동일 프레임 대응비교라 층화 교란이 원리적으로 불가 |
| `shift_viz` brain + `shift` 워크스페이스 | 축이 dscore_pred 2개 = GT-free 좌표. 전 프레임에 GT 가 있는데 GT-free 축은 `margin_viz` 에 엄격히 열등 |
| `v084_missed`, `why_text` | 위 §3-3 |
| `gt_cos_v080/v084`, `margin_quadrant`, `margin_v084_bin`, `cover_viz` | 위 §3 말미 — 쓰고 바로 지우던 것 |

**유지** — `relabel_transition`(영상단위 결론의 부호를 뒤집은 원인, GT 출처 추적 핵심),
`camera`(널 모델), `margin_v080/v084`, `margin_delta`, `gap_cluster/gap_deficit`,
`why_before/after`, `top_prompt_*`, `class_best_v1_0_8_4`, `shift_direction`,
`text_search` + `explore` 워크스페이스(사용자 결정 — 앱에서 자연어로 프레임 찾는 유일 수단).

**codex 와의 이견**: codex 는 `shift_mag_q`·`shift_viz` 유지 의견이었으나, modeler 의 라이브
실측(82.8% 단일 통 / GT-free 축 열등)이 더 강한 근거라 삭제를 채택한다.

## 5. slim 후 최종 표면

- brain: `emb_viz`, `margin_viz`, `text_search` (3)
- 워크스페이스 5: `flips`(emb_viz+flip) · `margin`(margin_viz+flip) ·
  **`prompt`(emb_viz+winner_purity_v084)** · `gap`(emb_viz+gap_cluster) · `explore`(emb_viz+ground_truth)
- 사이드바 6그룹: ① 판정 · ② 근거 · **③ 프롬프트 품질** · ④ 다음 타깃 · ⑤ 층화 · ⑥ 상세

## 6. 검증

- `stage_selftest` — 기존 스트리밍 검증 + **`bank_top2_stream` == 순진 행렬곱 top-2** +
  **소유권 불변식**(§3-3). 데이터 불필요, `python3 sourceh_prompt_geometry.py selftest`.
- 라이브 — `prune` 실행 후 `prune.json` 의 `total_gain` 이 탐욕 곡선 마지막 값과 일치하는지,
  새 필드 5종의 값 분포를 로그로 확인.
- **널 모델 확인**: `prompt` 워크스페이스에서 Color by 를 `camera` 로 바꿔 그림이 닮았는지
  먼저 볼 것. 닮았으면 그 그림은 프롬프트에 대해 아무것도 말하지 않는다.

## 7. 하지 않는 것

- 문장을 sample 로 승격해 이미지와 한 UMAP 에 찍기 — 승자 520개만 보이고 비승자 28,085개는
  안 보이며, 프레임↔문장 엣지가 없어 "삼각관계"를 문자 그대로 그리지도 못한다 (codex).
- 승자 문장 정체성 기준 색칠(top-K / 커버리지 컷) — §1 로 반증됨. 굳이 본다면 `n≥20` 컷이
  그나마 낫고 `기타` 를 `tail_impure`/`tail_pure` 로 쪼개야 하지만, 어떤 컷도 저순도 문장의
  44~48%가 기타에 남는 것을 막지 못한다.
- 원공간 응집도 필드 — 카메라를 인코딩할 뿐.
