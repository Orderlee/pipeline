# 분석 계층 표준화 설계 (Analysis Standardization)

> 상태: **승인됨** (2026-09-16) — UI 는 **S6-lite**, git 편입은 **엔진 2개**
> 작성 2026-09-16 · 계기: "버전 비교 승자 표"(노션 3차 사이클) 구현 중 발견된 구조 문제
> 통계 설계는 Codex(`gpt-5.6-sol`, effort ultra) 교차검증을 거쳤고, 인용된 수치·줄번호는
> 전부 이 세션에서 결정론 도구로 재현했다.
> 이 문서는 노션 §7 **"거버넌스 게이트 (R1)"** 의 실행 설계다. 그 항목은 이미
> *"분석 산출물이 CSV·바이너리·JSON·도구 내부 필드로 흩어져 있다 → 표준 산출물 3종과
> 검증기를 정립하고 분석 코드를 정본화한다"*, 공수 2~4d, **DB 직결 비교 화면의 선행 조건**
> 으로 못박혀 있다. 새 아이디어가 아니라 미뤄 둔 트리거를 당기는 것이다.

---

## 0. 한 줄 요약

새 현장 데이터셋이 생길 때마다 **스크립트를 새로 쓰는 구조**를 끊고, 코호트는 **설정 1줄**로
편입되게 만든다. 표준화 대상은 통계·평가·리포트 계층이며, 인제스트 계층은 대상이 아니다.

---

## 1. 문제 — 실측

`docker/analysis/` 2026-09-16 기준 전수 조사.

| 항목 | 실측 |
|---|---|
| 파이썬 파일 | 176 |
| **git 미추적** | **54** (`analysis_standard.py`·`sourcei_stat_ab.py` 포함) |
| 파일명에 현장/데이터셋 이름 | 33 (`sourcei_*` 12, `cohorta_*` 4, `certbody_sitej_*` 3, …) |
| **`"sourcei"` 또는 `sourcei_gt` 경로를 하드코딩** | **92** |
| 코호트를 env/argparse 로 받음 | **11** |

`92 : 11`. 파일명 기준(33)보다 훨씬 넓다 — **이름이 중립적인 스크립트도 대부분 sourcei 를
하드코딩**하고 있다. 즉 "현장 스크립트를 정리한다"로는 문제가 해결되지 않는다.

산출물도 흩어져 있다. `frames_bank/report` 아래 실파일:

```
csv 88 · json 87 · png 71 · npz 54 · npy 34 · log 32 · md 8 · jsonl 6 · tsv 3 · state 2
= 385 파일 / 10 포맷 / 12 디렉토리
```

산출물 루트 자체도 갈라져 있다: `frames_bank/report`(87) · `sourceh/prompts`(20) ·
`sourceh_v*`(8) · `uploads/sitej_certbody`(5) · `sourcei`(5) …

### 1.1 이 구조가 실제로 만든 사고

추상적 부채가 아니라 이미 발현됐다.

1. **표준 러너가 낡은 데이터를 본다.** `analysis_standard.py` 의 유일한 어댑터
   `load_sourcei()` 는 `preds.npz` 를 읽는데 그건 **7,498장 구코호트**다. 라이브 `sourcei`
   는 **6,032장**. 표준이라 불리는 것이 stale 스냅샷 위에서 돌고 있다.
   **가드레일 정의 자체도 같은 병에 걸려 있다** — `GUARDRAILS` 의 `evidence` 는 정적 문자열
   이라 코호트가 바뀌어도 갱신되지 않는다. G3 의 `ICC 0.827` 은 구코호트 값이고 **라이브에서는
   0.368 로 threshold 아래라 발동하지 않는다**(31개 버전 전부). 이 세션에서 실제로 그 문자열을
   라이브 판정으로 오해했다. → [ICC 추정량 감사](2026-09-16-icc-estimator-audit.md)
2. **통계 구현 오류가 복제 대기 중이다.** `sourcei_stat_ab.py:296` 의 Westfall–Young 은
   가설마다 새 Rademacher 행렬을 뽑고(`wild_p()` 안에서 `rng.choice`), `:328` 이 그걸 쌓아
   `max` 를 취한다 → WY 의 존재 이유인 **가설 간 상관구조가 파괴**된다. 바로 아래 `:300`
   의 짝 부트스트랩 `BOOT` 은 루프 밖에서 한 번만 뽑아 올바르게 공유되고 주석도
   "모든 변형에 같은 재표본"이라 적혀 있다 — 저자가 원칙은 알았는데 한쪽에서만 놓쳤다.
   `:318` 주석은 "단계강하"인데 구현은 단일-step 이다. **"선례를 따른다"가 안전장치가 못 된다.**
3. **추적 파일이 미추적 파일에 의존한다.** 패널(`plugins/user-prompt-compare/__init__.py`)은
   추적되는데 그게 쓸 통계 엔진은 추적되지 않는다. CLAUDE.md 의 "bind mount 라 이 repo 의
   커밋이 곧 실행 코드" 원칙이 미추적 파일에는 성립하지 않는다.

---

## 2. 이미 있는 표준화 씨앗 — 새로 만들지 않는다

네 개가 이미 존재한다. 설계의 핵심은 **새 프레임워크를 짓는 것이 아니라 이 넷을 연결하는 것**이다.

| 씨앗 | 무엇이 이미 맞나 | 무엇이 빠졌나 |
|---|---|---|
| `analysis_standard.py` `run(D, outdir)` | `D` 가 평범한 dict 계약(`gt`/`group`/`classes`/`events`/`bank_scores`). S0~S5 스테이지는 **전부 데이터셋 비의존**. 가드레일 G1~G8 정의 보유 | 어댑터가 **1개**뿐. `main()` 이 `if a.dataset != "sourcei": raise SystemExit`. 입력이 stale. git 미추적 |
| `camera_confound_probe.py` | `DATASET`/`GROUP`/`SOURCE` 를 env 로 받고 **파일명에 현장이 없다**. 이미 올바른 형태 | 표준 러너와 연결돼 있지 않음. 산출물 계약 없음 |
| `prompt_standard.py` | *"같은 규칙이 세 곳에 흩어져 있었다 → 코드 한 곳에 고정"* — 같은 논리의 성공 선례. **이미 tracked** | 프롬프트 규칙 한정 |
| `bank_tags_contract.py` | 생산자↔소비자 드리프트 탐지기 = **계약 검사기 선례** (447줄) | 뱅크 태그 한정 |

---

## 3. 범위 — 무엇을 표준화하고 무엇을 안 하나

33개 현장 스크립트는 세 부류이고 **전부를 표준화하면 안 된다**.

| 부류 | 예 | 판정 |
|---|---|---|
| **A. 인제스트/빌드** | `sourcei_build.py`, `certbody_sitej_ingest.py`, `cohorta_gt_ingest.py`, `sourcea_thumb_embed.py` | **표준화 대상 아님.** 소스 디렉토리 구조·파일명 규약이 현장마다 실제로 다르다 (SourceA 은 경로에 클래스+카메라+날짜가 박혀 있고, cohorta 은 COCO json, sourcei 는 SAM3 이벤트 구간). 표준화할 것은 **출력 계약**(= `D` dict)이지 입력 파서가 아니다 |
| **B. 통계/평가** | `sourcei_cluster_ci.py`, `sourcei_stat_ab.py`, `sourcei_native_stats.py`, `sourcei_ceiling_probe.py`, `filter_ab*.py`, `optbank_compare_all.py` | **주 대상.** `(gt, group, preds)` 만 있으면 도는 순수 통계인데 현장 이름이 붙어 있다 |
| **C. 리포트/차트** | `sourcei_gt_charts.py`, `sourcei_gt_xlsx.py`, `frames_fire_charts.py` | **2차 대상.** 산출물 계약이 정해진 뒤 |

> **이번 사이클 범위는 B 의 계약 정립 + 첫 소비자 1건(버전 비교 승자 표)까지.**
> 기존 B 스크립트 전량 이관은 Phase 4 로 분리한다 — 계약이 실사용 1회를 통과하기 전에
> 대량 이관하면 계약 오류를 92곳에 복제한다.

---

## 4. 설계

### 4.1 코호트 레지스트리 (`cohort.py`, 신규)

새 데이터셋 편입 비용 = **설정 1줄**, 새 스크립트 0개.

```python
COHORTS = {
    "sourcei":      dict(prompts="sourcei-prompts",      gt="ground_truth", group="camera"),
    "sitej_subway": dict(prompts="sitej_subway-prompts", gt="ground_truth", group="session"),
}
```

나머지(클래스 목록·버전 목록·예측)는 **데이터셋에서 발견**하고 하드코딩하지 않는다.
이 경로는 이미 실측 검증했다 — `(bank_version, gidx % 100000) → category.label` 맵으로
sourcei 31개 버전 전부 **unmapped 0건**, macro-F1 재현 성공 (V1.0.10.3 0.7908).

#### 자동 유도하면 **안 되는** 항목: `group`

`sitej_subway` 는 `camera` 가 58대라 자동 유도하면 그걸 고른다. 그런데 그 코퍼스는
**연출 동시녹화**라 카메라 홀드아웃에 28% 누수가 있고 올바른 군집키는 `session` 이다.
군집키를 잘못 잡으면 CI 가 좁아져 **"유의하다"는 거짓 결론**이 나온다 — 이 계층이
막으려는 바로 그 사고다.

→ `group` 은 **명시 필수**. 레지스트리에 없는 데이터셋은 `camera` 로 조용히 폴백하지 않고
**`G0 군집키 미지정` 가드레일을 발동시키고 산출을 거부**한다.
(이 repo 의 반복 버그 형태 "부재에 기댄 안전"을 새로 만들지 않기 위해.)

### 4.2 `load_sourcei` → `load_cohort` 승격

- stale `preds.npz` 의존을 끊고 **라이브 FiftyOne 데이터셋에서 직접** `gt`/`group`/예측을 만든다
- `main()` 의 `if a.dataset != "sourcei": raise SystemExit` 제거
- `--out` 기본값을 코호트에서 유도 (`{ROOT}/{dataset}/`)

### 4.3 표준 산출물 3종 + 검증기

노션 R1 이 요구한 "표준 산출물 3종". 10포맷 385파일을 **한 계약**으로 수렴시킨다.

| # | 산출물 | 포맷 | 역할 |
|---|---|---|---|
| 1 | `standard_report.json` | JSON | 기계가 읽는 **정본**. 모든 수치·가드레일 판정·지문 |
| 2 | `standard_card.md` | Markdown | 사람이 읽는 요약 카드 (이미 `run()` 이 생성 중) |
| 3 | `<stage>.csv` | CSV | 표 형태 스테이지 산출 (이미 `S3_scoring.csv` 존재) |

png/npz/npy 는 **부산물**로 남기되 정본이 아님을 명시한다 (재생성 가능해야 한다).

#### 지문(fingerprint) — stale 을 조용히 넘기지 않기 위한 계약

`standard_report.json` 헤더에 반드시 포함:

초안(`dataset_last_modified` + `engine_git_sha`)은 **불충분** 판정을 받았다. 필수 항목:

| 군 | 항목 |
|---|---|
| 신원 | `report_schema` · `method_id` · `status` · `run_id` · `environment`(prod/staging) |
| 데이터 | **frames·prompts 양쪽** dataset ID + 이름 · 양쪽 `max(last_modified_at)#count` |
| 의미 해시 | 정렬된 `(sample_id, gt, group, winner_gidx_*)` 해시 · `(norm_version, local_gidx, category)` prompt-map 해시 · version↔winner-field 매핑 + **예측벡터 해시** |
| 설정 | `config_hash`(target_classes/rule/α/B/seed/보정법/quantile 방식) · 정렬된 group 목록 · draw hash |
| 코드 | **`engine_bundle_sha256`**(경로+내용 동시) · git commit + **dirty/untracked 상태** · 런타임 패키지 버전 |

> **해시는 git 추적을 대체하지 못한다.** `engine_git_sha` 는 엔진이 미추적인 동안 보조
> 메타데이터일 뿐이므로 **Phase 0 편입이 패널 배선보다 먼저**다.

**패널 정책은 전부 fail-closed** — 아티팩트 없음/손상/schema 불일치/revision 불일치/
`status ∈ {running, failed, refused}`/`display_allowed=false` → **표를 그리지 않는다**.
"경고와 함께 stale 표 표시"는 **금지**다(§7 "경고가 아니라 거부").

**원자적 발행 필수** — 현재 `run()` 의 직접 쓰기는 부분 JSON 을 노출할 수 있다.
`json.dump(..., allow_nan=False)` → `flush`/`fsync` → `os.replace` → `complete=true`.
prod/staging 경로 분리.

`bank_tags_contract.py`(447줄)가 이미 같은 역할을 뱅크 태그에 하고 있으므로 그 패턴을 따른다.

> ⚠️ 산출물 경로 `docker/data/` 는 **gitignore 대상이고 rsync 소스도 아니다** —
> 아티팩트는 배포로 전파되지 않는 호스트 로컬 상태다. 그래서 지문이 더 중요하다.

### 4.4 통계 계약 — S6-lite (교차검증 결론)

원래 요구는 "동순위 묶음 + 유의하게 갈린 경계에서만 순위 숫자"였다. **기각됐다.**

**기각 사유 — 이 저장소의 G3 가 이미 금지하고 있다:**

```
G3  현장 간 이질성(ICC)   trigger: ICC > 0.5
    action: 뱅크/모델 순위표 만들지 말 것 · 쌍대 설계로만 비교
    evidence: 혼합효과 ICC 0.827(macro-F1)/0.932(정확도)
```

sourcei 실측 ICC 0.827 → G3 발동. 게다가 "비유의"는 **동치관계가 아니므로**
(`A~B`, `B~C`, `A>C` 가 동시에 가능) 단일 `=` 블록은 필연적으로 거짓말이 된다.

> 실질적으로는 두 입장이 같은 화면으로 수렴한다. 요구 "유의하지 않은 비교는 순위 미표시"를
> sourcei 에 끝까지 적용하면 **순위 숫자가 0개**가 된다(상위 8버전 인접 델타 CI 가 전부 0 포함).
> S6-lite 는 그 결론을 UI 형태로 못박은 것이다.

**S6-lite 계약:**

| 항목 | 규칙 |
|---|---|
| 순위 숫자 | **생성하지 않는다.** 점추정 내림차순은 **표시 순서로만** |
| 판정 | 전 쌍 공유-draw **single-step max-\|t\|** → `1위 후보 / 유의 열세` 불리언 |
| `=` 기호 | **예측벡터 완전동일 별칭에만.** 비유의 묶음에 쓰지 않는다 |
| **G1·G2** 발동 시 | 후보 판정도 `보류` (G3 는 라이브 미발동 — [ICC 감사](2026-09-16-icc-estimator-audit.md)) |
| 화면 이름 | "승자 순위표"가 아니라 **"버전 비교 — 1위 후보와 불확실성"** |

**부트스트랩 계약** (전부 교차검증 반영):

1. **고정 `target_classes`** — draw 마다 present 를 재선택하면 draw 마다 estimand 가 바뀐다
   (결손 draw 는 3-class macro 가 아니라 2-class macro). 코호트 전체 클래스를 고정하고
   하나라도 결손이면 `NaN`(전 버전 공통 invalid) + `invalid_draw_rate` 저장.
   **실측 1.10%** (B=20,000 중 19,780 유효) — 감당 가능하다.
   per-version CI 는 **"고정-class 조건부 CI"** 라벨이 붙을 때만 정직하다.
2. **공유 draw 행렬 `M[B,U]` 를 딱 한 번** 만들고 모든 가설이 같은 행을 쓴다.
   가설마다 새로 뽑으면 `sourcei_stat_ab.py:296` 의 버그가 재발한다.
3. **family = QC·중복제거 후 고유 예측벡터 `C(U,2)` 전체 쌍.** 점추정 정렬 후 인접 쌍만
   검정하는 것은 **사후 선택된 family** 라 부당하다. `U=24` → 276 쌍.
4. **Romano–Wolf** (이름이 "Westfall–Young"이 아니라 이것이 정확). 부트스트랩 p 값은
   **귀무 중심화 필수**: `t_null = |(delta_boot − delta_hat)/se|`,
   `p = (1 + #{max_t ≥ t_obs})/(B+1)`. 백분위 CI 불리언에 Holm 을 붙이는 것은 형식적으로
   성립하지 않는다.
5. **B 기본 20,000** (2,000 은 탐색용). 단 **어떤 B 도 G=15 의 정보량을 늘리지 못한다** —
   B 는 몬테카를로 오차만 줄인다.
6. **CGM wild cluster bootstrap 을 raw macro-F1 에 그대로 적용하지 않는다** — CGM 은
   회귀계수·군집 residual 전제다. 카메라별 confusion matrix 를 미리 만들고 multinomial
   가중 합산으로 draw 비용을 줄인다.
7. **`n_eff` 는 정확도 기반 deff 를 쓰지 않는다** (macro-F1 과 지표 불일치). 영향함수 기반을
   쓰거나, 구현을 검증 못 하면 **`n_eff(acc)` 라고 정확히 표기**한다. 설명 없는 `n_eff` 금지.
8. **실질적 유의는 별도 열.** 임의 `Δ≥0.01` AND 조건은 기각. `Δ=+0.0004 · 통계적 차이 있음 ·
   실질 중요성 기준 미정` 처럼 그대로 보여준다. ε 의 근거가 될 수 없는 것: MDE·카메라
   변동·n_eff. 근거가 되는 것: 배포 miss/false-alarm 비용, 독립 재라벨 연구, 사전 합의 허용오차.
9. **QC 순서**: integrity → **corrupt 제외** → 복원 완결성 → exact dedup → 통계.
   `v1.0.2.0`(corrupt)과 `v1.0.2.1`(valid)을 같은 별칭으로 접으면 **손상 버전을 세탁**하는 것이다.
   따라서 "31→24"는 QC **전** 관측치이고 최종 family 크기는 corrupt 제외 후 재계산한다.
10. **`macro_present` 의 `0.0` 반환은 조용한 오답 경로**다([:96](../../../docker/analysis/analysis_standard.py#L96)).
    이벤트 0 일 때 `0.0` 이 아니라 `undefined/refused` 여야 한다.

### 4.5 소비자 계약 — 패널은 읽기만

FiftyOne 패널은 `analysis_standard` 를 **import 하지 않는다**. 가드레일 판정 결과까지
`standard_report.json` 에 실어 패널은 **순수 읽기 전용**으로 유지한다.

이유 둘: (a) 추적 파일이 미추적 파일에 의존하는 구조를 만들지 않는다, (b) "패널 상태 쓰기
0회 / 렌더 타임 계산 금지" 제약과 일치한다.

---

## 5. 실행 계획 (Phase)

각 Phase 는 **그 자체로 동작하는 산출물**을 낸다. 앞 Phase 가 실사용 1회를 통과하기 전에
다음으로 넘어가지 않는다.

### Phase 0 — 편입과 계약 (선행, 이게 없으면 나머지가 무의미)
- `analysis_standard.py` git 편입 (+ 같이 쓰이는 미추적 통계 모듈 선별 편입)
- `cohort.py` 신설 — 레지스트리 + `G0 군집키 미지정` 가드레일
- `standard_report.json` 지문 스키마 확정 + 검증기
- **완료 기준**: `git ls-files` 로 엔진이 추적됨 / 레지스트리에 없는 데이터셋이 산출을 거부함

### Phase 1 — 범용 로더
- `load_sourcei` → `load_cohort(dataset)` 승격, stale `preds.npz` 의존 제거
- `main()` 의 sourcei 하드 게이트 제거
- **완료 기준**: `--dataset sitej_subway` 가 스크립트 추가 없이 완주 / sourcei 산출이
  라이브 6,032장 기준으로 갱신됨(구 7,498 재현이 아니라 **차이가 설명됨**)

### Phase 2 — 버전 비교를 S6 스테이지로 (**S6-lite**)
- 원래 요청 1~3번이 여기 안착하되 **순위 숫자는 생성하지 않는다** (§4.4)
- 34번째 현장 스크립트를 만들지 않고 표준 러너의 스테이지가 된다
- 순수 통계는 `version_compare_stats.py` 로 분리 — S6 가 호출한다
- **[ICC 감사](2026-09-16-icc-estimator-audit.md) D1~D3 를 같이 처리한다:**
  - **D1** `GUARDRAILS.evidence` 를 정적 문자열에서
    `{stale_evidence, live_measured, cohort, measured_at}` 로 바꾸고, 발동 판정 옆에
    **측정 시점의 라이브 값**을 싣는다
  - **D2** S4 가 `ref_bank` 한 개로 계산하는 것을 **전 버전 deff/ICC 분포**(min·median·max)
    보고로 바꾸고, 가드레일 판정을 임의 한 버전이 아니라 분포 기준으로 낸다
    (라이브 deff 105~194 = 1.85배 → 보고 유효표본이 57 vs 31 로 갈린다)
  - **D3** 혼합효과 ICC 산출 시 `n_cameras_used / n_cameras_total` 병기 필수
    (라이브 **6/15**). 사용 카메라가 절반 미만이면 그 값으로 게이트 판정을 하지 않는다
- **완료 기준**: sourcei·sitej_subway 양쪽에서 표가 나오고, exact duplicate 가 `=` 별칭으로
  접히며, corrupt 는 별도 `제외` 목록에 있고, **G1·G2** 발동이 헤더에 보이고,
  `invalid_draw_rate` 가 아티팩트에 있으며, 가드레일 판정 옆에 **라이브 측정치**가 함께 있음

### Phase 3 — 패널 배선 (읽기 전용)
- compare 패널에 마크다운 표 섹션 추가. 지문 불일치 시 "재계산 필요" 표시
- **완료 기준**: 두 화면 컨트롤을 다 눌러도 사이드바 필터 유지 / 패널 상태 쓰기 0회 유지

### Phase 4 — 기존 스크립트 이관 (별건, 점진)
- B 부류를 `load_cohort` + 스테이지로 이관. **한 번에 하지 않는다**
- `sourcei_stat_ab.py` 의 WY 공유-draw 버그는 이관 시점에 수정 (지금 고치면 미추적 파일을
  고치는 셈이라 Phase 0 편입 이후)

---

## 6. 하지 않는 것

- **인제스트 계층(A) 표준화** — 소스 구조가 현장마다 실제로 다르다. 출력 계약만 맞춘다
- **92개 하드코딩 일괄 치환** — 계약이 실사용 1회를 통과하기 전 대량 이관은 오류를 복제한다
- **`frames` 코호트 편입** — GT 가 203,869 중 40장뿐(`bank_gt`)이고 군집 필드가 없다. **hard skip**
- **새 프레임워크 도입** — 씨앗 4개를 연결할 뿐, 의존성을 추가하지 않는다

---

## 7. 위험

| 위험 | 완화 |
|---|---|
| 표준화가 진행 중인 분석을 멈춘다 | `docker/analysis/**` 는 배포 `paths-ignore` 대상이라 라벨링은 끊기지 않는다. 기존 스크립트는 Phase 4 까지 그대로 돈다 |
| 계약이 틀린 채로 굳는다 | Phase 2 실사용 1회 통과 전까지 이관 금지. 첫 소비자가 계약을 검증한다 |
| `group` 을 잘못 잡아 거짓 유의가 나온다 | 명시 필수 + `G0` 가드레일. 폴백 없음 |
| 지문이 있어도 아무도 안 본다 | 소비자(패널)가 불일치 시 **표를 안 그린다**. 경고가 아니라 거부 |
| codex 교차검증이 크레딧으로 막힌다 | Phase 0·1 은 통계 판단이 없어 선행 가능. Phase 2 만 대기 |

---

## 8. 확정된 결정 (2026-09-16)

| # | 질문 | 결정 |
|---|---|---|
| 1 | UI 형태 | **S6-lite** — 순위 숫자 생성 안 함, "1위 후보 / 유의 열세" 불리언만 |
| 2 | Phase 0 git 편입 범위 | **엔진 2개만** (`analysis_standard.py`, `sourcei_stat_ab.py`). 나머지 52개는 Phase 4 이관 시 편입 |
| 3 | 산출물 루트 | **신규 산출물만** `frames_bank/report/<cohort>/`. 기존 5개 루트는 건드리지 않는다(Phase 4) |
| 4 | Phase 4 이관 순서 | 미정 — Phase 2 가 계약을 검증한 뒤 재논의 |

## 9. 모듈 분해 (Phase 0~3)

| 파일 | 상태 | 책임 | 위험 |
|---|---|---|---|
| `docker/analysis/prompt_data_contract.py` | 신규 | 버전 정규화 · local gidx · corrupt 판정 | MED/HIGH |
| `docker/analysis/cohort.py` | 신규 | 코호트 레지스트리 + `load_cohort(name, stages)` | MED/HIGH |
| `docker/analysis/version_compare_stats.py` | 신규 | 순수 통계 코어(공유 draw · 고정 class · Romano–Wolf · top-set) | HIGH |
| `docker/analysis/analysis_standard.py` | 수정 | S6 스테이지 · stage 선택 · stage 상태 · 원자 발행 | HIGH |
| `plugins/user-prompt-compare/__init__.py` | 수정 | JSON 읽기 + 지문 검증 + 5열 마크다운 **만** | MED |

> **패널(4,216줄)을 분석기가 import 하지 않는다.** 버전 정규화·local gidx·corrupt 판정을
> `prompt_data_contract.py` 로 빼서 양쪽이 그 모듈을 본다. (초안의 "패널 헬퍼 재사용"은 기각됐다.)

> `sourcei_stat_ab.py` 는 **이번 사이클에 변경하지 않는다.** WY 공유-draw 버그 수정은
> 추적 편입 후 Phase 4 의 별도 HIGH 작업이다.
