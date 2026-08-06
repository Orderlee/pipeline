# APO 개선용 FiftyOne 시각화 — 단계 계획서

- 작성: 2026-08-03
- 목적: **APO(Auto Prompt Optimization) 성능 개선**. 현장 배포 시 성능이 안 나오는 문제를
  FiftyOne 에서 진단·개입 가능하게 만든다.
- 상태: **Phase 0 진행 중** (source-h 프로토타입, 지표 스펙 적대적 검증 단계)
- 이 문서만 보고 진행 가능하도록 자체 완결로 쓴다. 수치는 전부 라이브 실측이며 출처를 병기한다.

---

## 0. APO 가 무엇인가 (Notion 출처)

Retrieval 기반 영상 이상상황 탐지에서 텍스트 프롬프트 세트를 운영 데이터에 맞게
자동 생성·평가·정제·적용하는 루프. **오탐(FP) 감소가 1차 목표.**

```
[Generate]  오탐/미탐 이미지 → PE-lang 1B 로 프롬프트 생성   ⚠️ 에폭당 랜덤 50장 상한
[Inference] PE-Core-L14-336 추론 → 프롬프트 개별점수 Score_p
              Score_p = FP_p/N_normal   − TP_p/N_abnormal   (abnormal 프롬프트)
                      = FN_p/N_abnormal − TN_p/N_normal     (normal 프롬프트)
              높을수록 삭제 1순위
[Optimize]  Score_p 상위 K=40 = G_del → 유전 알고리즘
              edit-based M=8 (add/del/rep) + evolution-based N=16 (crossover/mutation), T=40회
              평가지표 = val hit_rate (이미지별 topk 안에 든 정답 클래스 프롬프트 수), k_keep=5
[Inference] best 집합 재추론 → val hit_rate 최고일 때만 best_text_features 갱신
```

| 항목 | 값 | 출처 |
|---|---|---|
| 실적 | `PE-Core-L14-336_with_prompt_optimization_260310` 제로베이스 **0.3641 / 0.5110** | [APO 스프린트] 1-3 |
| KPI | 태국드론 정부과제 — **프롬프트 최적화 48H 이내** | 정부과제 페이지 |
| split | 카테고리별 8:2 (train 생성용 / val 평가·갱신 / test outer) | [자동화] APO 개선 계획 |
| 고도화 방향 | 운영팀·고객사도 프롬프트 엔지니어링 가능한 UI, 벡터 export 아웃소싱 | APO 고도화 3·4 |

참고 Notion: `[APO 스프린트] 1-2 프롬프트 최적화 코드` · `[자동화] APO 개선 작업 계획 정리` ·
`[특허] APO 내용 정리` · `ProAPO 논문 정리`

---

## 1. 진단 — APO 에 꽂히는 실측 3개

전부 source-h 13,144프레임(사람 재라벨 GT) 라이브 측정. 상세 근거는
`docs/prompt-geometry-2026-07-31.md` §14.

### D1. 개별 Score_p 랭킹은 상호작용을 놓친다
`G_del` 은 **개별** 점수 상위 K=40 인데, 실측에서 개별 LOO 합과 배치 제거 실측이 매 라운드
어긋났다 (v080 R1: 개별합 **+286** vs 실측 **+364**). 나쁜 문장 뒤에 또 나쁜 문장이 있어
같이 지워야 드러난다. 유전 알고리즘이 탐색으로 일부 메우지만 **시작 집합이 개별 랭킹이라
탐색 공간이 처음부터 좁다.**

### D2. 생성 입력이 랜덤이다
`[Generate]` 가 오탐/미탐에서 **랜덤 50장**을 뽑는다. source-h 기준 생성 후보(FP+FN)는
**1,859장**이고, 그중 **604장(32.5%)이 margin < 0.005** — 사실상 동전 던지기 구간이다.
랜덤 50장이면 **평균 16장**이 여기 해당한다.
> ⚠️ "저margin = 라벨 오류"는 **아직 미검증 가정**이다. Phase 0 에서 `relabel_transition`
> (사람이 GT 를 고친 이력)이 저margin 구간에 몰리는지로 검정한다. 안 몰리면 이 항목은 철회.

### D3. 사이트 전이에서 결과가 뒤집힌다
| 삭제셋 전이 | 영상 폴드 (카메라 공유) | **카메라 폴드** (현장 전이) |
|---|---|---|
| v1.0.8.0 | +11.79pp | +0.61 / +1.32 / **+10.37pp** |
| v1.0.8.4 | +1.68pp | **−0.09 / −0.18 / −0.17pp** |

v084 는 영상 폴드에선 이득인데 카메라를 건너면 손해다. val hit_rate 로 40세대를 돌려도
이 차이를 못 본다. **"현장 나가면 성능이 안 나온다"의 유력한 원인.**

관련: 승자 문장 201개 중 **2대 이상 카메라에서 이기는 것은 56개뿐.**
못 본 카메라에서는 학습 카메라 비승자("예비군")가 승자의 66%를 차지하고,
그 문장들이 held 프레임의 84%를 정확도 92%로 결정한다. 승자 상위 25개만 남기면 **−55.3pp.**

---

## 2. 화면 4종 (source-h 기준 확정, Phase 0 검증 중)

추가 필드는 **프레임 단위 5개만**. 문장 단위 지표는 CSV 가 담당한다
(FiftyOne 샘플 = 이미지 하나라, 문장 지표를 필드로 내리면 "이 프레임을 이긴 문장의 값"
으로만 의미가 있어 금방 불어난다).

| 화면 | brain / Color by | 결정 | 추가 필드 |
|---|---|---|---|
| `1-generate` | `emb_viz` / `error_type` | 생성에 넣을 이미지 선별 → 태그 `apo_generate` | `error_type` `margin_bin` `err_cluster` |
| `2-audit` | Samples | 문장 하나 선택 → 가져간 사진 보고 삭제/수정/유지 | (기존 `top_prompt_*` 활용) |
| `3-prune` | `emb_viz` / `winner_del_effect` | G_del 확정 전 "지우면 잃는 것" 확인 | `winner_del_effect` |
| `4-site` | `emb_viz` / `winner_site_scope` | 새 현장에서 무너질 문장 식별 | `winner_site_scope` |

저장 뷰 4개: `01_생성후보`(FP+FN, 1,859) · `02_라벨의심`(∩ margin<0.005, 604) ·
`03_삭제영향` · `04_사이트특이`

**brain run 은 `emb_viz` 하나만 쓴다.** 새로 만들지 않는다.
`app_config.active_fields` 는 allowlist 이고 **여기 없는 필드로 Color by 하면 App 이
`TypeError: Cannot read properties of undefined (reading 'id')` 로 죽는다** (실측).
→ 워크스페이스 색 필드에서 목록을 파생할 것.

---

## 3. frames_captions 이식성 — 실측 판정

| | source-h | frames_captions |
|---|---|---|
| 프레임 | 13,144 | **199,972** |
| **사람 GT** | **100%** | **`bank_gt` 40장 = 0.02%** |
| 이미지 임베딩 | 100% | **100%** ✅ |
| 캡션 | — | 11,978 (6.0%) |
| 코호트 | 카메라 3대 | **project 22개** |
| weak label (SAM3 `normalized_class`) | — | 187,994 (94%) — `none` 112,543 + `person` 66,285 = **95%가 비이벤트** |
| 이벤트 weak label | — | fall 4,135 / smoke 3,214 / fire 1,578 = **4.5%** |

**project 별 이벤트 편중** (같은 지표를 전 project 에 적용하면 안 되는 이유):

| project | n | 이벤트 | 구성 |
|---|---|---|---|
| icce_2025 | 73,390 | 1,488 | fall 318 / smoke 669 / fire 501 |
| vietnam_data | 33,766 | 1,245 | **fall 1,239 / smoke 5 / fire 1** ← 사실상 fall 전용 |
| appdata | 24,572 | 2,726 | fall 1,367 / fire 327 / smoke 1,032 |
| gwangjin_raw | 10,899 | 199 | 거의 fall |

### 화면별 이식 판정

| 화면 | 판정 | 이유 |
|---|---|---|
| `1-generate` | ❌ **불가** | FP/FN 정의에 GT 필수 |
| `2-audit` | ⚠️ **반쪽** | 문장→프레임 조회는 됨. purity/stolen 판정 불가 |
| `3-prune` | ❌ **불가** | LOO 제거이득 = 정답 증감. GT 없으면 계산 성립 안 함 |
| `4-site` | ✅ **오히려 강화** | GT 불필요. project 22개라 source-h(n=3)보다 통계력 압도적 |
| `attach` (프롬프트 매칭) | ✅ **그대로** | GT 불필요. 200K×12,480 타일링, 뱅크당 ~75초 예상 |

**weak label 을 GT 대용으로 쓰지 말 것** — 모델 파생 라벨이고, CLAUDE.md 의 **자기학습 금지**
원칙 위반이다. 코드의 concordance 계산도 주석에 "참고 신호 — recall 아님"으로 박혀 있다.

### GT 없이 되는 축 4개

| 지표 | GT | 의미 | 구현 |
|---|---|---|---|
| `pred_margin` (top1−top2) | 불필요 | 낮으면 모델이 헷갈림 → 능동학습 우선순위 | `stage_attach` 에 이미 있음 |
| 문장 점유 집중도 | 불필요 | 한 문장의 점유율. source-h v084 top1=43% 병리를 GT 없이 탐지 | `atlas` 재사용 |
| `n_projects_win` | 불필요 | 22개 중 몇 곳에서 이기나 → 사이트 전이 예측 | `bank_reach_stream(groups=project)` |
| `bank_shift` | 불필요 | 뱅크 A→B 예측 변화 | `stage_score` 에 이미 있음 |

코드에 **`minn_tier()`** 가 이미 있다 — `no_gt` / `counts_only` / `exploratory`(n≥30) /
`reportable`(n≥100). GT 부족 상황에서 어디까지 주장할 수 있는지를 이미 게이팅한다.

---

## 4. 단계 계획

### Phase 0 — source-h 화면 4종 완성 **(진행 중)**

GT 100% 라 **지표가 맞는지 검증 가능한 유일한 환경.** 여기서 틀린 지표는 GT 없는 데서는
영영 못 잡는다.

| | 작업 | 예상 |
|---|---|---|
| 0-1 | `error_type` · `margin_bin` · `err_cluster` + 층화 추출 규칙 + 화면1 | 반나절 |
| 0-2 | 화면2 배선 (문장→프레임 역방향 조회 UX 확정) | 2시간 |
| 0-3 | `winner_del_effect` (프레임 단위 counterfactual) + 화면3 | 반나절 |
| 0-4 | `winner_site_scope` + 널모델 검정 + 화면4 | 2시간 |

**게이트**: 각 지표는 ① 경계값이 데이터에서 나왔는가 ② `camera`/`ground_truth` 의
재인코딩이 아닌가(널 모델) ③ 한 범주가 90% 넘지 않는가 ④ APO 의 어느 결정을 실제로 바꾸는가
— 4개를 통과해야 채택.

> ⚠️ 이 단계에서 뒤집힐 수 있는 가정 3개:
> (1) "margin<0.005 = 라벨 의심" — `relabel_transition` 으로 검정 필요
> (2) `err_cluster` 가 그냥 카메라로 갈릴 위험 (승자문장→카메라 예측력 82~87%)
> (3) 화면4 가 `camera` 색칠과 닮으면 폐기

### Phase 1 — frames_captions 에 `attach` + 화면4

GT 불필요한 것만 먼저. **source-h 이 못 하는 사이트 전이 검정을 제대로 하는 게 목적.**

| | 작업 | 비고 |
|---|---|---|
| 1-1 | `BANK_ATTACH=<ver>` 로 199,972 프레임에 프롬프트 매칭 | ~75초/뱅크. `set_values` 배치 필수 |
| 1-2 | `bank_reach_stream(groups=project)` → `n_projects_win` | 22 project |
| 1-3 | `winner_site_scope` (공통/사이트특이) + 화면4 | source-h 정의 그대로 이식 |
| 1-4 | project 간 leave-one-project-out 전이 검정 | **n=22** — source-h n=3 의 한계가 여기서 풀린다 |

**운영 제약**
- FiftyOne Embeddings 패널 **5,000점 상한** → 200K 전체는 못 그린다. 뷰로 좁힐 것
- 호스트 RAM 62.5G 공유 + OOM 이력 → 배치 쓰기 유지
- project 22개 중 이벤트가 거의 없는 곳(gwangjin 계열)은 전이 검정에서 제외하거나 별도 tier

### Phase 2 — 능동학습으로 GT 확보

전량 라벨링은 불가능. **project 당 100~200장**이면 `minn_tier` 의 `reportable`(n≥100) 에 든다.
22 project × 150장 ≈ **3,300장**.

우선순위 규칙 (전부 GT-free 신호):
1. `pred_margin` 하위 — 모델이 가장 헷갈리는 것
2. project × 클래스 커버리지 공백 — 아예 표본이 없는 칸
3. 점유 집중 문장이 가져간 프레임 — 한 문장이 대량으로 먹는 영역

투입 경로: Label Studio 검수 → `finalized` → GT 확정
(`.agent/skill/` 및 CLAUDE.md §Label Studio 연동 참조)

**게이트**: project 당 `reportable` 도달 전에는 화면1·3 을 열지 않는다.

### Phase 3 — frames_captions 에 화면1·3 확장

Phase 2 로 GT 가 생긴 project 부터 순차 적용. 전 project 동시 적용 금지 —
이벤트 구성이 project 마다 달라(vietnam=fall 전용) 지표가 의미를 잃는다.

---

## 5. 하지 말 것 (실측으로 기각됨 — 되살리지 말 것)

| 안 | 기각 근거 |
|---|---|
| 프롬프트를 점으로 찍는 joint UMAP (이미지+문장 한 좌표계) | text↔image cos 중앙 **0.147** / text↔text 0.631 / image↔image 0.756 — 세 분포가 겹치지 않아 최근접 질의가 엔티티 타입 분류기가 되고 UMAP 은 modality 2덩이만 보여준다 |
| 영상 임베딩(프레임 센트로이드) | 프레임 중앙값 커버리지와 spearman **0.993** 인 재인코딩. 회수가능 오답 지목력 **36.3%** vs `gap_cluster` **62.3%**(오라클 69.1%). 집계 7종 상호 ρ 0.894~0.993 이라 방식 선택도 무의미 |
| 승자 문장으로 이미지 UMAP 색칠 | UMAP 영역 분산 ↔ LOO 제거이득 spearman **+0.13 / −0.10**(무상관). 나쁜 문장이 오히려 조밀. 승자문장→카메라 예측력 **82~87%**(베이스라인 60.7%)라 사실상 카메라 지도. v084 top-1 문장이 43.3% 점유라 화면이 2~5색 |
| 예산 목적 뱅크 축소 (승자만 남기기) | 승자만 남겨도 성능 **완전 동일**(정의상 정보량 0)인데, 새 카메라에서 **−55.3pp**. 비승자는 중복도 죽음도 아닌 **예비군**(v084 `reach>0` 4,312개, 완전 불활성 0개) |
| weak label(SAM3)을 GT 대용으로 | 모델 파생 라벨 → 자기학습 금지 원칙 위반. 95%가 비이벤트라 분모도 안 맞음 |
| 뱅크 npz 수정 | **읽기 전용.** prune 의 "삭제"는 in-memory bool 마스크 시뮬레이션이고 CSV `dropped` 도 제안이지 실행 기록이 아니다 |

---

## 6. 자산 목록 (이미 있는 것)

### 코드 — `docker/analysis/prompt_geometry.py`
| 함수/스테이지 | 용도 | GT 필요 |
|---|---|---|
| `bank_top2_stream(X, bank, drop=)` | 클래스별 per-frame 1·2위 + argmax. drop 마스크 지원 | ✕ |
| `bank_reach_stream(X, bank, best, groups=)` | 문장별 `reach` + 그룹(카메라/project)별 reach | ✕ |
| `_Pruner` (score/hits/loo_gains/greedy) | LOO + 탐욕 배치 제거 | ○ |
| `attach` | 뱅크 1벌 → 프레임에 매칭 문장 부착 | ✕ |
| `atlas` | 문장↔이미지↔영상 연결 2방향 CSV | ✕ |
| `prune` | 문장별 순도·LOO·탐욕제거 + 홀드아웃 | ○ |
| `gap` / `flips` / `guide` / `slim` | 미검출 군집 / 플립 분해 / 후보 프로브 / 표면 큐레이션 | ○ |
| `score` (frames 프로필) | 도메인 샤드 GT-free 채점 | ✕ |
| `minn_tier()` | GT 표본 수 → 주장 가능 tier | — |

`sourceh_only = {analyze, ablate, flips, guide, slim, prune, atlas, attach}` 는 frames 프로필에서
`SystemExit` — GT 분모가 필요하다는 코드상 선언이다.

### 산출물 — `/data/fiftyone/sourceh_v2/report/`
- `prune_<ver>.csv` — **전 뱅크**. gidx·wins·purity·stolen·loo_gain·reach·카메라별 reach·n_cams_win·dropped·text
- `prompt_frames_<ver>.csv` — 문장→영상/프레임
- `video_prompts_<ver>.csv` — 영상→문장 + `margin_min`
- `c4_sentence_prune.png` — x=reach, y=선언순도, 하단 예비군 히스토그램
- `prompt_authoring_guide.md` — 후보 문장 채택 가이드 (카메라 층화 게이트)

### 실행
```bash
# 뱅크 1벌 부착 (GT 불필요)
BANK_ATTACH=v1.0.8.0 docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py attach

# 전체 비교 파이프라인 (GT 필요, source-h 전용)
./docker/analysis/bank_eval.sh v1.0.8.0 v1.0.8.4
#   analyze → gap → flips → prune → atlas → viz → guide → slim → report

# 자가검증 (데이터 불필요)
docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py selftest
```

---

## 7. 미해결 / 확인 필요

1. **APO 의 train/val/test split 단위** — Notion 엔 "카테고리별 8:2"만 있고 영상/카메라 단위
   분할 언급이 없다. 프레임 단위면 누수이고 D3 의 직접 원인이다.
   *(사용자 지시로 이번 범위에서 제외. 코드 접근 가능해지면 최우선 확인)*
2. **탐욕 제거 미수렴** — source-h 12라운드에서 아직 안 끝났고, 라운드마다 같은 라벨에 재적합하므로
   **전이 검정 없이 `PRUNE_ROUNDS` 를 올리면 안 된다.**
3. **카메라 n=3 한계** — source-h 의 leave-one-camera-out 은 표본 3개. 방향은 예비군 활성화율
   66%/84% 로 독립 확인되지만 **크기는 신뢰 금지.** Phase 1 의 project n=22 가 이걸 푼다.
4. **APO 실데이터 미확인** — APO 내장 데이터셋(약 700장) + 현대백화점/금호타이어 는 아직 안 봤다.
   source-h 프로토타입이 APO 실데이터에서 재현되는지는 별도 검증 필요.

---

## 8. 참고

- 실측 상세: `docs/prompt-geometry-2026-07-31.md` (§13 큐레이션 · §14 프롬프트 품질 축)
- 설계 근거: `docs/superpowers/specs/2026-07-31-prompt-quality-viz-design.md`
- FiftyOne App 함정: 워크스페이스 셀렉터는 우상단 `⊞ Unsaved` · `active_fields` allowlist 밖
  Color by 는 크래시 · `?workspace=` URL 전환은 산발 크래시 · 목록은 페이지 로드 시 fetch(F5 필요)
- 검토 방식: 페르소나(`ai-modeler`, `ai-data-engineer`) 병렬 + **Gemini 3.1 Pro**(codex 쿼터 소진
  대체)의 적대적 검증. 의견이 갈린 항목은 **라이브 실측 우선**으로 중재.
