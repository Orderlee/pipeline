# FiftyOne 뱅크 버전 스키마 & 분석 필터 설계

- 작성일: 2026-08-14
- 대상: `sourcei` (1차) → `source-h` → 신규 데이터셋 전체
- 관련 코드: `docker/analysis/prompt_geometry.py`, `docker/analysis/plugins/user-prompt-probe/`,
  `docker/analysis/plugins/user-prompt-compare/`, `docker/analysis/fiftyone_app_setup.py`,
  `docker/analysis/prompt_scores_export.py`
- 선행 문서: `docs/superpowers/specs/2026-08-07-viz-curation-platform-design.md`

> **개정 이력**
> - r1 (초안): 버전접미사 필드 453 flat 을 삭제하고 버전 없는 고정 슬롯으로 대체.
> - r2: **삭제 계획 철회.** 반증 검증에서 `wave_iou_*` / `wave_pred_*` 에 실사용
>   소비자가 발견되어(§8-1) 파괴적 마이그레이션이 무효화됐다. **필드는 그대로 두고 뷰로
>   가린다** — 설정만 바꾸는 무손실 설계로 전환. SAM3 축은 사용자 판단으로 기본 세트에서 제외.
> - **r3 (현행):** "버전을 필드명에 넣는 것이 맞는가"라는 사용자 질문에 대한 답을 반영.
>   비용을 실측해(§1-5) r2 의 근거 하나를 정정하고 — 상한은 스키마 로딩이 아니었다 —
>   실제 부채가 **정합성(명명 규칙 3종 · 해석기 2벌 중복)** 임을 확인. 대응으로
>   **D7 계약 테스트**를 추가하고(공유 모듈은 import 경로가 없어 불가 — 실측),
>   근본 해법인 npz 이관은 **착수 조건을 명시해 보류**(§5-2).
>   🔴 이 과정에서 **라이브 무증상 버그**가 발견됐다 — `prompt_scores_export.py` 가
>   `winner_gidx_*` 를 전 버전에서 해석하지 못해 거버넌스 export 의 문장 귀속 층이
>   비어 있다(§3 D7-1). M6 으로 분리해 **2026-08-14 수정·검증 완료**.
> - **r3d (현행): M8·M9 완료** (사용자 요청 2건).
>   **M8** 정기 실행 경로 — 원인을 재정정했다: `export` 는 이미 `validate_dir()` 을 자체
>   실행하고 위반 시 exit 1 이다. 없던 것은 배선이 아니라 **트리거**였다 → `bank_health.sh`
>   (flock + 저하 실행 FAIL 처리). crontab 등록만 승인 대기.
>   **M9** 규칙별 예측 슬롯 `pred_{wave,argmax,topk}_{a,b}` — **D2 를 부분 철회**한다.
>   `argmax` 는 `winner_gidx` → 문장 클래스 조인으로 **재계산 없이** 29버전 전부 복원했고
>   (v1.0.8.0 에서 기존 필드와 7,498/7,498 일치), 그 결과 두 규칙이 버전 간에 **반대
>   방향으로** 움직인다는 것이 드러났다(§M9).
> - r3c: M0–M5 완료 — sourcei 에 필터 세트 적용(무손실, 설정 전용). 구현은
>   `prompt_geometry.py` 가 아니라 `fiftyone_app_setup.py` 로 갔다(§5 구현 위치 정정).
>   설계 정정 1건: **FiftyOne 에 default-view 가 없어** 저장 뷰는 자동 적용되지 않는다 →
>   주 수단을 뷰에서 **접힌 사이드바 그룹**으로 바꿨다(§5 M0–M5 검증). 남은 미착수는
>   §5-3 의 `validate` 정기 실행 경로와 source-h 필터 세트 재측정(§6).
> - r3b: **M7 완료** — `bank_tags_contract.py` 신설·검증. pre-fix 리졸버 주입 시 정상적으로
>   빨개지는 것(exit 1, C4 가 `winner_gidx` 29건 지목)까지 확인했다.
> - r3a: M6 검증 중 **이 문서 자신의 주장 하나를 정정**. "validator 도 통과한다"는 틀렸고,
>   `validate` 는 pre-fix 상태를 7,498건으로 정확히 잡는다. 사고의 성격이 "탐지 불가"가
>   아니라 **"탐지되었으나 실행되지 않음"** 으로 바뀌었고, 그에 맞춰 §8-3 의 교훈과
>   §5-3 의 후속 항목(정기 `validate` 실행 경로)을 고쳤다.

---

## 1. 문제

### 1-1. 사용자 질문

> "뱅크 버전이 늘어나면 filter에도 관련된 내용을 계속 추가해야 하는 거야?"

지금 구조에서는 **그렇다**. 증가율이 실측된다.

### 1-2. 실측 (2026-08-14, `docker-analysis-1` 라이브)

| 데이터셋 | 샘플 | top 필드 | **flat 경로 (=App 사이드바 필터 엔트리)** | 버전접미사 flat |
|---|---:|---:|---:|---:|
| `sourcei` | 7,498 | 248 | **686** | 498 (73%) |
| `source-h` | 13,144 | 233 | 663 | 568 (86%) |
| `frames_captions` | 199,972 | 33 | **69** | **0** |

`sidebar_groups` 는 그룹핑·순서만 통제하고 필드를 **숨기지 못한다**. 분석가가 실제로 마주하는
필터는 그룹에 등록된 131개가 아니라 **flat 스키마 전량 686개**다.

### 1-3. 증식의 정체 — 그리고 그것의 소비자

| 필드군 | flat | top | 전개 | **외부 소비자 (검증 완료)** |
|---|---:|---:|---|---|
| `wave_vs_topk_<vtag>` | 186 | 31 | Classification ×6 | 생산자 + 설정(사이드바/Color-by)뿐 |
| `wave_pred_<vt>` | 174 | 29 | Classification ×6 | `prompt_scores_export.py:46` (dist_iou 규칙), `fiftyone_app_setup.py:29`, **sourcei 라이브 `active_fields`·`color_scheme`** |
| `wave_iou_{falldown,fire,smoke}_<vtag>` | 93 | 93 | float | **`@user/prompt-probe` v1.1.0 (enabled) `__init__.py:247`** → 채택근거 `p_iou` 컬럼 |
| `winner_gidx_<vtag>` | 29 | 29 | int | **`@user/prompt-compare` 23곳** + 스크립트 6개 |
| `pred_<vt>` | 6 | 1 | Classification | `prompt_scores_export.py` |
| `probe_{bar,out,votes,topc}_<vtag>` | 8 | 8 | scalar | 생산자 전용 |
| `top_prompt_<vt>` / `pred_margin_<vtag>` | 2 | 2 | scalar | `prompt_scores_export.py` |

**증식의 66%가 `wave` 계열**이고, `sourcei-prompts` 의 distinct `bank_version` 은 29종이므로:

- **현재: 새 뱅크 버전 1개 = 필터 +15.6개** (453 ÷ 29)

핵심 정정: 초안은 이 493개를 "아무도 안 읽는다"고 판단해 삭제하려 했으나 **틀렸다**.
`wave_iou_*` 는 프롬프트 큐레이션 오퍼레이터가 29버전 전부에서 읽고 있고(실측: 29/29 해석,
7,498 샘플 전량 채워짐), `wave_pred_*` 는 거버넌스 익스포트의 dist_iou 규칙 전체가 걸려 있다.

> **따라서 증식 문제는 "필드가 많다"가 아니라 "필드가 전부 사이드바에 뜬다"이다.**
> 데이터는 도구가 쓴다. 사람이 안 볼 뿐이다. 고칠 대상은 스키마가 아니라 **노출**이다.

### 1-4. 부수 문제

1. **명명 규칙 3종 공존** — 생산자 현행 2종 + 구 표기 1종(아래 2번).
   `vt = version.replace(".","_")` → `v1_0_8_4` 와 `vtag(v)` (`prompt_geometry.py:141`)
   → `v1084` 가 필드군마다 갈린다. 소비자들은 이걸 알고 **여러 후보를 시도**한다
   (`user-prompt-probe:_ver_tags:206`, `prompt_scores_export:suffixes:58`).
   즉 규칙 통일은 소비자 수정을 동반해야 한다 (→ §3 D7).
2. **구 슬러그 잔존 13필드** (`*_v080`, `*_v084`). 신 슬러그와 중복이지만
   `_pick_field` 의 **폴백 경로이자 selftest 단언 대상**(`:1225-1226`)이라 고아가 아니다.
3. **`prompt_scores_export.py:suffixes()` 는 구 규칙**(`parts[-3:]`)만 만든다.
   `pred_margin_v080` 처럼 신 슬러그 쌍이 없는 필드가 여기 걸려 있어, 구 슬러그를 지우면
   이 익스포터가 조용히 `None` 이 된다.
4. **`sourcei` 사이드바 8그룹의 작성 코드가 없다.** `stage_slim()`
   (`prompt_geometry.py:3459-3491`)이 7그룹 레이아웃을 코드로 만들지만 **source-h 전용**이고
   (`:3411` 에서 다른 데이터셋이면 `SystemExit`), 그 그룹명(`⑦ 분포IoU(wave)` 등)은
   sourcei 의 라이브 그룹명(`② 씬 조건`, `④ 검출`, `⑨ 기타`)과 일치하지 않는다.
   sourcei 의 현재 구성은 세션에서 손으로 만들어졌고 재현·리뷰·롤백이 불가능하다.
5. **버전 하드코딩 3곳**: `fiftyone_app_setup.py:29` (`CLASS_FIELD_CANDIDATES` 에
   `pred_v1_0_8_0`, `wave_pred_v1_0_8_0`), sourcei 라이브 `active_fields`,
   라이브 `color_scheme`. 버전을 바꿀 때마다 손으로 고쳐야 하는 지점들이다.

### 1-5. "버전을 필드명에 넣는 것이 맞는가" — 비용 실측

r2 는 스키마 증식의 상한을 "App 스키마 로딩 시간"으로 적었다. **측정 결과 그 근거는 틀렸다.**

| 데이터셋 | docs | avgObjSize | 컬렉션 | flat | **스키마 로드** |
|---|---:|---:|---:|---:|---:|
| `sourcei` | 7,498 | **25.2 KB** | 189 MB | 686 | **1.1 ms** |
| `source-h` | 13,144 | 23.8 KB | 313 MB | 663 | 1.2 ms |
| `frames_captions` | 199,972 | 15.9 KB | 3,181 MB | 69 | 0.3 ms |

- 스키마 로드 1.1 ms — 병목이 아니다.
- 문서 크기는 25.2 KB vs 15.9 KB. 버전 필드가 샘플당 약 9 KB, sourcei 컬렉션의 **약 35%
  (~65 MB)** 를 차지한다. 이 규모에서는 아프지 않다 (공유 호스트 RAM 압박의 주범도 아님).

**따라서 버전 네이밍의 부채는 용량이 아니라 정합성이다.** 29버전 × 7,498프레임 × 5지표는
본래 **배열**인데 그 축(version)을 **식별자**로 인코딩했고, 그 대가가 §1-4 의 1·3번 항목이다:

- 명명 규칙 **3종** 공존 — `v1_0_8_0`(vt) / `v1084`(vtag) / `v080`(구)
- 태그 해석기 **2벌 중복**, 각자 **서로 다른 부분집합**만 처리하며 공통 출처가 없다:
  - `user-prompt-probe:_ver_tags:213` → `["v1080", "v080"]`
  - `prompt_scores_export:suffixes:58` → `["v1_0_8_0", "v080"]`
  - 각자 자기가 읽는 필드군에는 맞지만, **세 번째 소비자가 생기면 세 번째 부분 구현이 생긴다.**
  - 3파트 버전(`v1.0.8`)에서는 `suffixes` 의 두 번째 후보(`v108`)와 `_ver_tags` 의 첫 후보
    (`v108`)가 겹쳐, 규칙 세대가 다른 필드를 같은 이름으로 지목할 수 있다.
- 이 부채는 이미 두 번 물렸다 — `vtag()` 주석(`prompt_geometry.py:147-150`)이 기록한
  버전 태그 붕괴 사고, 그리고 이 문서의 r1 계획 전체 무효화(§8-1).

대응은 §3 D7(계약 테스트, 지금)과 §5-2(npz 이관, 조건부 보류)로 나눈다.
D7 을 라이브에 적용하는 과정에서 **이 부채가 이미 사고로 실현돼 있음**이 확인됐다 → §3 D7-1.

---

## 2. 목표 / 비목표

### 목표

- **G1** 뱅크 버전이 늘어도 **분석가가 보는 필터가 늘지 않는다** (증가율 0).
- **G2** 기본 화면 필터를 **한 워크플로를 끝낼 수 있는 최소 집합**으로 줄인다.
- **G3** 취사선택을 취향이 아니라 **측정된 분별력**으로 결정한다.
- **G4** 사이드바·뷰·`active_fields` 를 **코드가 생성**한다 (손 세팅 금지).
- **G5** 데이터셋별 하드코딩 없이 같은 레시피가 적용된다.
- **G6** **기존 도구가 하나도 깨지지 않는다** (r2 에서 추가된 제약).
- **G7** 버전 태그 해석이 **한 곳에만** 존재한다 — 생산자가 규칙을 바꿔도 소비자가 조용히
  어긋나지 않는다 (r3 에서 추가. §1-5 의 정합성 부채가 근거).

### 비목표

- 패널·오퍼레이터의 다중 버전 기능 축소. 29버전 전체 접근은 그대로 유지된다.
- `wave` 판정 규칙 자체의 변경.
- 필드 삭제. **r2 에서 명시적으로 범위 밖**이다 (§8-1).

---

## 3. 설계 결정

### D1. 필드는 남기고 **뷰로 가린다** (무손실)

r1 의 삭제 계획을 철회한다. 대신:

- 스키마·값은 **손대지 않는다**. 도구(`@user/prompt-probe`, `@user/prompt-compare`,
  `prompt_scores_export.py`)는 29버전 전부를 지금처럼 태그로 조회한다.
- 분석가의 기본 화면은 **allowlist 기반 저장 뷰 `00_분석`** 이다.
  `exclude_fields(전체 스키마 − keep-set)` 으로 만든다.

핵심은 allowlist 라는 점이다. **drop-list 가 아니라 keep-list 이므로, 새 버전이 필드를
추가해도 그 필드는 자동으로 뷰 밖에 있다** → 분석가가 보는 필터 증가율 **+0** (G1).

r1 의 슬롯(+1/버전)보다 낫고, 파괴적이지 않으며, 코드 변경도 더 적다.

> `ponytail:` 스키마는 계속 커진다(29버전 248필드 → 60버전이면 ~450). **상한은 스키마
> 로딩이 아니다** — 실측 1.1 ms 로 무해하다(§1-5). 실제 상한은 문서 크기(샘플당 ~9 KB,
> 컬렉션의 35%)이고 이 역시 현 규모에선 아프지 않다. 지금 미리 지우는 것은 도구를
> 깨뜨리면서 얻는 것이 없다. 진짜 부채는 용량이 아니라 정합성이며 D7 이 그것을 다룬다.

### D2. 슬롯 필드를 만들지 않는다

r1 은 `bank_a_wave_pred` 같은 버전 없는 슬롯 10필드를 새로 만들려 했다. 철회한다:

- 소비자들이 **태그 접미사로만 필드를 찾는다**
  (`_ver_tags:213` → `["v1080","v080"]`, `suffixes:58` → `["v1_0_8_0","v080"]`).
  버전 없는 이름은 이들이 **영원히 찾지 못한다**.
- 뷰 allowlist 를 `ds.info["bank_run"].slots` 에서 생성하면 슬롯 필드 없이 같은 효과가 난다.
- 부수 이득: 사이드바에 `wave_pred_v1_0_8_0` 이라고 뜨는 것이 **곧 출처 표시**다.
  버전 없는 `bank_a_wave_pred` 는 어느 뱅크인지 화면에서 알 수 없다.

즉 **신규 필드 0개, 삭제 0개.** 바뀌는 것은 설정뿐이다.

> **D2 부분 철회 (M9, 2026-08-14 · 사용자 요청).** 위 논거는 **기존 필드를 대체**하는
> 슬롯에 대해서는 그대로 유효하지만, **추가**하는 슬롯에는 해당되지 않는다 — 소비자는
> 원본 `wave_pred_<vt>` / `winner_gidx_<vtag>` 를 그대로 읽으므로 아무것도 깨지지 않는다.
> 그리고 D2 가 내세운 "필드명이 곧 출처" 논거는 각 슬롯 필드의 **description**
> (`pred_wave_a` → "분포 IoU (제품 판정 규칙) · 뱅크 v1.0.8.0")으로 해결된다.
> (초판은 사이드바 그룹 라벨에도 버전을 넣었으나 2026-08-14 사용자 결정으로 뺐다 —
> 버전 확인·교체를 `@user/bank-slots` 오퍼레이터가 담당하므로 그룹명은 `① 판정` 이다.)
>
> 결정적으로 D2 를 유지하면 **못 하는 분석이 있었다**: `argmax_k1` 의 `pred_<vt>` 는
> `v1.0.8.0` 에만 존재해 규칙별 버전 비교가 원리적으로 불가능했다. §M9 참조.

### D3. `ds.info["bank_run"]` = 비교쌍 기록 + 뷰 생성의 입력

`sourcei` 는 현재 `probe_*` 키만 있고 어떤 버전이 언제 붙었는지 기록이 없다.
`frames_captions` 형식을 따른다:

```python
ds.info["bank_run"] = {
    "run_id": f"attach-{ts}", "profile": PROFILE,
    "slots": {"a": "v1.0.8.0", "b": "v1.0.8.4"},   # ← 뷰/사이드바 생성의 입력
    "versions": [...],                              # 스키마에 존재하는 전 버전 (감사용)
    "ts": "...", "total": len(ds),
}
```

### D4. 숨김은 그룹이 아니라 **뷰**로

`sidebar_groups` 에서 경로를 빼도 필드는 계속 렌더된다 (`prompt_eval.py:80` 주석에 측정 기록).
실제 축소는 **저장 뷰의 `exclude_fields`** 로만 가능하다. `stage_slim` 의 `00_analysis` 패턴을
denylist 에서 allowlist 로 뒤집은 것이 D1 이다.

### D5. 사이드바·뷰·`active_fields` 를 한 함수가 생성

손 세팅 금지. `_sidebar_bank_group()` 의 멱등 append 패턴을 확장해 그룹·`active_fields`·
저장 뷰를 함께 만든다. `active_fields` 는 allowlist 이고 **여기 없는 필드로 Color-by 하면
App 이 죽으므로**, 워크스페이스의 색 필드에서 파생한다 (`stage_slim:3506` 방식).

라이브 `active_fields`·`color_scheme` 이 `wave_pred_v1_0_8_0` 을 참조하고 있으므로,
비교쌍을 바꿀 때 이 셋(뷰·사이드바·색)이 **한 트랜잭션으로 같이** 갱신돼야 한다.

### D6. 데이터셋 종속 제거

버전접미사 정규식(`_v\d+(_\d+)*$`) + keep-set 으로 뷰를 생성하면 `stage_slim()` 의
source-h 전용 `SystemExit` 가드가 불필요해진다. 단 **필터 선정(§4)은 데이터셋마다 재측정**한다.

### D7. 태그 해석에 **관측 경로**를 둔다 — 공유 코드가 아니라 공유 계약

§1-5 가 확인한 실제 부채를 값싸게 제거한다. 필드명·데이터는 **손대지 않는다**.

현재 두 소비자가 각자 부분 구현을 갖고 있다:

| 소비자 | 함수 | 만드는 후보 | 담당 필드군 |
|---|---|---|---|
| `@user/prompt-probe` | `_ver_tags:213` | `["v1080", "v080"]` | `wave_iou_*`, `winner_gidx_*` (vtag 계열) |
| `prompt_scores_export.py` | `suffixes:58` | `["v1_0_8_0", "v080"]` | `pred_*`, `wave_pred_*` (vt 계열) + 구 |

**공유 모듈은 불가능하다 (실측).** 컨테이너의 `PYTHONPATH` 는 미설정이고 `/workspace` 는
플러그인 프로세스의 `sys.path` 에 없다. 즉 `docker/analysis/bank_tags.py` 를 만들어도
플러그인이 import 할 수 없다. (배선을 새로 까는 것은 8줄짜리 중복을 없애자고 치르는 비용으로
과하다.)

**대신 공유 코드가 아니라 공유 계약을 둔다.** 각 소비자는 자기 해석기를 유지하고,
`docker/analysis/bank_tags_contract.py` 하나가 셋을 모두 불러 드리프트를 검사한다.
방향이 `테스트 → 소비자` 라서 import 문제가 없다 (검증 완료: `importlib.util.
spec_from_file_location` 으로 플러그인을, `sys.path.insert(0,"/workspace")` 로 스크립트를 로드).

검사할 계약 — 실제 29개 버전 + 경계 케이스(`v1.0.5.0` vs `v2.0.5.0`, 3파트 `v1.0.8`) 전수:

1. **생산자 도달성**: 각 소비자의 후보 목록이 자기 담당 필드군의 **실존 필드명**을 만들어내는가.
   (`prompt_geometry.vtag()` 를 생산자 정본으로 삼는다.)
2. **충돌 없음**: 서로 다른 버전이 같은 태그로 붕괴하지 않는가
   (`vtag()` 주석 `:147-150` 이 기록한 사고의 회귀 검사).
3. **폴백 유효**: 구 슬러그 폴백이 여전히 실존 필드를 가리키는가.

### D7-1. 계약 위반 1건은 이미 발생해 있다 (수습 대상) 🔴

이 계약을 라이브에 적용하자마자 **실사용 중인 무증상 버그**가 나왔다.

`prompt_scores_export.py` 의 `RULE_FIELDS["argmax_k1"]` 은 winner 필드로 `winner_gidx_{v}` 를
쓰는데, `suffixes()` 는 `["v1_0_8_0", "v080"]` 만 만든다. 실존 필드는 **`winner_gidx_v1080`**
(vtag 계열)이다 → **어느 후보와도 일치하지 않는다.**

`sourcei` 라이브 실측:

| bank | argmax_k1 pred | argmax_k1 margin | **winner** | dist_iou pred |
|---|---|---|---|---|
| `v1.0.8.0` | `pred_v1_0_8_0` | `pred_margin_v080` | **None** | `wave_pred_v1_0_8_0` |
| `v1.0.8.4` | **None** | **None** | **None** | `wave_pred_v1_0_8_4` |
| `v1.0.13.2` | **None** | **None** | **None** | `wave_pred_v1_0_13_2` |

`winner_gidx_v080` 은 존재하지 않고 `winner_gidx_v1080` 은 존재한다. `source-h` 도 동일하다
(두 데이터셋 모두 winner 해석 성공 0건).

**결과**: 정본 3층 export 의 **문장 귀속 층(`prompt_frame_pred`)의 `winner_gidx` 가
전 버전에서 `null`** 이었다. `cmd_export` 는 `f_pred` 가 없을 때만 규칙을 건너뛰므로
(`:115-120`), winner 만 없으면 **행은 그대로 쓰이고 값만 빈다**. 추가로 `v1.0.8.0` 을
제외한 모든 버전은 `pred_<vt>` 자체가 없어 `argmax_k1` 규칙이 통째로 건너뛰어진다
(이쪽은 리졸버가 아니라 attach 단계의 데이터 공백).

> **정정 (2026-08-14, 실측):** 이 문서의 이전 판은 "exit 0 이고 validator 도 통과한다"고
> 적었으나 **틀렸다.** `validate` 는 이 상태를 정확히 잡는다 — sourcei 산출물로 pre-fix 를
> 재현한 결과 **`argmax_k1 인데 winner_gidx 가 NULL` 7,498건**(수정 후 0건).
> 즉 **탐지 장치는 있었고 정상 동작했다.** 무증상이었던 것은 export 시점뿐이고
> (`resolve()` 가 조용히 `None` 을 반환 → exit 0), 실패한 것은 **export → validate 루프가
> 이 두 데이터셋에서 돌지 않았다는 것**이다. 사고의 성격이 "탐지 불가"가 아니라
> "탐지되었으나 아무도 돌리지 않음"이므로, 대응도 D7 계약 테스트만이 아니라
> **정기 실행 경로**가 필요하다.

생산자가 2026-08-11 에 태그 규칙을 바꿨고(`vtag()` 전 파트 조인) 소비자 하나가
따라가지 않은 것이 원인이다 — §8-1 이 r1 을 무효화시킨 것과 같은 뿌리다.

**수습 (2026-08-14 적용 완료 — §5 M6)**: `suffixes()` 가 vtag 후보를 만들도록 수정.

**이득**: 네 번째 규칙이 생기거나 생산자가 또 규칙을 바꿔도 **CI/수동 실행 한 번으로 드러난다.**
지금은 드러나는 경로가 없어서 몇 달치 export 가 조용히 손실됐다.

**비용**: 계약 테스트 1개(~40줄) + `suffixes()` 1줄 수정. 필드·데이터·값 변경 0.

### 기각한 대안

| 안 | 기각 사유 |
|---|---|
| **r1: 삭제 + 버전 없는 슬롯** | `@user/prompt-probe` 의 `p_iou` 컬럼과 `prompt_scores_export` 의 dist_iou 규칙이 **조용히** 죽는다(예외도 로그도 없음). 슬롯명은 두 소비자의 태그 해석기가 영원히 못 찾는다. §8-1 |
| **B. Long/EAV** (`bank_scores=[{version,…}]`) | `ListField(EmbeddedDocument)` 3단 경로는 **App 전체를 죽인다**(FiftyOne 1.19 `pullSidebarValue` 가 keys[0]/keys[1] 만 봄). 리스트 필터는 원소 간 AND 가 안 돼 "version=v084 **그리고** margin>0.02" 가 다른 원소에 걸려 조용히 오답이 된다. |
| **C. 넓은 스키마 + 사이드바 자동생성** | 손 세팅은 없애지만 노출 증식은 그대로. |
| **D. 패널 전용** | 사이드바 필터·Color-by·태깅 포기 = FiftyOne 을 쓰는 이유 포기. |
| **E. 지금 명명 규칙 통일 (개명)** | r1 이 증명한 실패 경로다 — 소비자가 **태그로** 필드를 찾으므로 개명은 두 도구를 조용히 깨뜨린다. 게다가 구 슬러그 13필드는 `_pick_field` 의 폴백 경로이자 selftest 단언 대상(`:1225-1226`)이라 지울 수도 없다. 이름을 건드리지 않고 해석기만 합치는 D7 이 같은 위험을 0 으로 만든다. |
| Classification → StringField (flat ×6 → ×1) | Color-by 가 `.label` 을 요구한다. 색칠을 잃는다. |

---

## 4. 분석 필터 설계 (사용자 관점)

### 4-1. 이 데이터셋의 분석 목적

`PROFILES["sourcei"]` 주석이 명시한다 — **recall 벤치마크가 아니라 오탐(FP) 스트레스 테스트**다.
GT 이벤트 구간이 falldown 57 / fire 5 / smoke 6 뿐이고 normal 721구간(near_miss 509 포함)이
모수이기 때문이다. 필터 세트는 이 질문에 봉사한다:

> "정상 장면 중 뱅크가 이벤트라고 잘못 부른 것은 무엇이고, 어떤 조건에서, 왜 그런가."

### 4-2. 선정 원칙

| 원칙 | 내용 |
|---|---|
| **P1 분별력** | 데이터를 안 가르는 필터는 필터가 아니다. 상수·전량 null·한 통 쏠림(top1 ≥ 90%) 축출. |
| **P2 예산** | 기본 펼침 위젯 합이 워크플로 하나를 끝낼 범위(≈12)를 넘지 않는다. |
| **P3 중복 금지** | 같은 개념을 두 이름으로 노출하지 않는다. |
| **P4 역할 분리** | 연속값은 필터가 아니라 **Color-by·정렬 축**. 사이드바엔 구간화된 형태만. |
| **P5 앵커 있는 축만** | GT 또는 뱅크 자신의 산출만 판정축으로 쓴다. **다른 모델의 의견은 제외** (§4-3 SAM3). |
| **P6 코호트는 뷰로** | 자주 쓰는 조합은 새 필드가 아니라 **저장 뷰**로 (스키마 비용 0). |

### 4-3. SAM3 축 제외 (사용자 판단, 2026-08-14)

`sam3_hit`(hit 79% / miss 21%), `sam3_n` 을 기본 필터 세트에서 뺀다.

- SAM3 결과는 **다른 모델의 의견**이지 GT 가 아니다. 뱅크 판정을 평가하는 축으로 쓰면
  앵커 없는 모델-대-모델 비교가 된다.
- 저장소가 이미 같은 판단을 내려뒀다 — `sam3_shadow_compare` 는
  "**게이트 아님, 2차 sanity 신호만**" 으로 명시돼 있다.
- 프로젝트 불변식 "자기학습 금지 — 모델 파생 라벨(`auto_generated`)로 학습/eval 금지" 와 같은 성격.
- ~~`sam3`, `sam3_labels` 는 sourcei 에서 null 100% 라 어차피 죽은 필드다.~~
  **🔴 정정 (2026-08-14): 이 문장은 측정 버그였다.** `sam3` 는 `Detections` 필드라
  라벨 경로가 `sam3.detections.label` 인데 분별력 측정이 일괄로 `.label` 을 붙여
  `sam3.label` 을 조회했고, 존재하지 않는 경로가 `{None: 7498}` 을 돌려준 것을
  "빈 필드" 로 기록했다. **실제로는 detection 25,007개**(person 24,085 / fallen person 680
  / smoke 169 / fire 73)가 5,926/7,498 프레임에 들어 있다.
  → SAM3 축을 판정 필터에서 빼는 근거는 **P5(다른 모델의 의견이라 앵커가 아니다)** 뿐이며,
  "데이터가 없다" 가 아니다. **삭제 대상이 아니다.**

**2026-08-14 추가 결정 — 필드 자체를 삭제했다** (사용자: "분석에 아무 의미가 없다").
`sam3` / `sam3_labels` / `sam3_hit` / `sam3_n` 4필드 + `sam3` 워크스페이스 제거.
sourcei 252 → **248 필드**.

- 앞서 이 문서가 적었던 "null 100% 라 죽은 필드" 는 **측정 버그였다**(§4-4 정정 참고).
  실제로는 detection 25,007개가 들어 있었으므로 **삭제 근거는 "빈 필드" 가 아니라
  P5(다른 모델의 의견이라 판정 앵커가 아니다) + 사용자 판단**이다.
- 되돌릴 수 있게 먼저 백업했다: `/data/fiftyone/sourcei-sam3-backup-20260814.json`
  (10.6 MB · 7,498 문서 · detection 25,007). Mongo `_id` 기준이라 `$set` 으로 복원 가능.
- `sam3` 워크스페이스는 `sam3_hit.label` 로 색칠하고 있어 **함께 지웠다** — 남기면
  존재하지 않는 필드로 Color-by 해 App 이 죽는다.
- 분석쪽 소비자는 없었다 (grep 결과 `sourcei_build.py` 생산자 외 0건).
- ⚠️ **`sourcei_build.py stage_sam3` 를 다시 돌리면 되살아난다** (`:474-490` 이 4필드를,
  `:517` 이 워크스페이스를 만든다).
- 🔴 **그러나 `stage_sam3` 를 지우면 안 된다.** `stage_build` 가 SAM3 박스로
  `person_count` / `fallen_person_count` / **`person_count_bin`** 을 만든다(`:478-481`).
  `person_count_bin` 은 **② 층화 그룹의 필터**이고 6분위 최대 버킷 32% 로 가장 균형 잡힌
  층화 축 중 하나다 (§4-4). 스테이지를 들어내면 재빌드 때 이 세 필드가 같이 사라진다.
  2026-08-14 에 실제로 삭제 직전까지 갔다가 이 결합 때문에 되돌렸다.
  → **SAM3 는 "판정축으로 안 쓴다"(P5) 이지 "파이프라인에서 없앤다" 가 아니다.**
  App 표면에서만 뺀 현재 상태가 의도된 상태다: 박스는 안 보이고, 거기서 뽑은 사람 수
  층화는 살아 있다.

### 4-4. 분별력 측정 결과 (버전무관 top 필드 55개)

**축출 — P1 위반이 측정으로 확정된 것:**

| 필드 | 측정값 | 판정 |
|---|---|---|
| ~~`sam3`, `sam3_labels`~~ | ~~null 100%~~ → **측정 버그. 실제 detection 25,007개 / 5,926 프레임** | **축출 아님** — P5 로만 판정축에서 제외, ⑧ 기타에 보존 |
| `adopted`, `attached_bank`, `view_unit` | distinct **1** | 상수 |
| `clock_daynight` | distinct 1 + null 78% | 상수+결손 |
| `clock_hour` | null **78%** | 결손 |
| `rule_cross` | `둘다 무변화` 7,488/7,498 = **99.9%** | 한 통 쏠림 |
| `environment` | `indoor` **98.1%** | 한 통 쏠림 (실내 데이터셋) |
| `weather`, `weather_margin` | `undetermined` **98.1%** | 한 통 쏠림. weather 축은 신뢰도 자체도 낮음 |
| `wave_role` | `중간` **94.2%** | 한 통 쏠림 |
| `fallen_person_count` | top1 **91.5%** | 한 통 쏠림 |
| `cos_best_{normal,falldown,fire,smoke}` | distinct 7,350+ | 연속값 → P4 (Color-by 축) |
| `daynight_margin`, `environment_margin`, `person_margin`, `probe_bar_all` | distinct 7,342+ | 연속값 → P4 |
| `t_sec`, `probe_topc_all`, `winner_peak_piece`, `caption` | 고카디널리티 | 필터 부적합 (모달 표시용) |
| `category` | `normal` 87% — `ground_truth`(58%)와 의미가 다른데 이름이 비슷 | **P3**: `ground_truth` 만 노출 |
| `pred_after_probe`, `probe_effect`, `probe_out_all` | 유효 버킷이 8건(0%) 규모 | 상세로 강등 |
| `sam3_hit`, `sam3_n` | 분별력은 있으나 | **P5**: 다른 모델 의견 (§4-3) |

**채택 — 분별력이 확인된 것:**

| 필드 | distinct | 최대 버킷 | 값 분포 |
|---|---:|---:|---|
| `ground_truth` | 4 | 57.7% | normal 4,323 / smoke 1,542 / falldown 1,404 / fire 229 |
| `event_kind` | 9 | 32.8% | other 2,459 / smoke 1,542 / **near_miss 1,503** / falldown 1,404 / fire 229 / drop 176 |
| `camera` | 15 | 24.1% | 최고의 층화 축 |
| `close_call` | 5 | 25.0% | 5분위 구간 — 설계상 필터용(P4 를 이미 만족) |
| `runner_up` | 4 | 33.1% | fire 2,480 / smoke 2,456 / falldown 1,765 / normal 797 |
| `person_count_bin` | 6 | 32.0% | 2-3 / 0 / 1 / 4-6 / 7-10 / 11+ |
| `daynight` | 2 | 73.0% | day 5,470 / **night 2,028** |
| `source_unit` | 3 | 58.1% | cheonho / v3 / v2 |
| `src_video` | 109 | 21.2% | 코호트 드릴다운 |
| `event_index`, `frame_in_event` | 513 / 999 | — | 이벤트 내 위치 (범위) |
| `winner_ablate_role` | 3 | 50.8% | 구조조각 주도 / 배경자석 / 배경편향 |
| `wave_gain` | 314 | 14.3% | wave 기여도 (범위) |

> `daynight` 은 여기서 살린다 — `source-h` 은 야간 프레임이 0장이라 무의미했지만
> `sourcei` 는 night 2,028장(27%)으로 실재하는 축이다. **분별력은 데이터의 성질이지
> 코드의 성질이 아니다** — 데이터셋마다 재측정해야 하는 이유.

### 4-5. 최종 필터 세트 — 4그룹 / flat 83

`<A>`, `<B>` 는 `ds.info["bank_run"].slots` 의 비교쌍 (현재 A=`v1_0_8_0`, B=`v1_0_8_4`).

| 그룹 | 펼침 | 필드 | flat | 답하는 질문 |
|---|---|---|---:|---|
| **① 판정** | ✅ | `ground_truth`, `wave_pred_<A>`, `wave_pred_<B>`, `runner_up` | 24 | 무엇이라 불렀나 / GT 와 갈렸나 |
| **② 층화** | ✅ | `camera`, `daynight`, `person_count_bin`, `source_unit`, `src_video` | 20 | 어떤 조건에서 나는가 |
| **③ 이벤트 맥락** | ✅ | `event_kind`, `event_index`, `frame_in_event` | 8 | near_miss 인가, 이벤트 어디쯤인가 |
| **④ 근거·심각도** | ⬜ | `close_call`, `winner_ablate_role`, `wave_gain`, `wave_iou_{falldown,fire,smoke}_<A,B>`, `wave_vs_topk_<A>`, `wave_vs_topk_<B>` | 31 | 얼마나 아슬아슬했나, 규칙이 갈렸나 |

**기본 펼침 3그룹 = 12위젯** (P2 만족). 전체 flat **83**.

`winner_gidx_*` 29개는 어느 그룹에도 넣지 않고 `00_분석` 뷰에서 제외한다 — 사람이 쓰는
필터가 아니라 패널의 조인 키다. 패널은 `fo.load_dataset()` 으로 전체 데이터셋을 다시 읽으므로
뷰에서 제외해도 조인은 정상 동작한다 (§8-2 에서 검증).

### 4-6. 저장 뷰 (자주 쓰는 코호트 — 새 필드 없이)

P6 에 따라 파생 플래그 필드를 **만들지 않는다**:

| 뷰 | 정의 | 용도 |
|---|---|---|
| `00_분석` (기본) | allowlist `exclude_fields` | 기본 화면. 여기가 flat 83 |
| `01_오탐` | `ground_truth=normal` ∧ `wave_pred_<A>≠normal` | 이 데이터셋의 주 목적. 한 클릭 |
| `02_near_miss` | `event_kind=near_miss` (1,503장) | FP 스트레스의 최대 코호트 |
| `03_AB불일치` | `wave_pred_<A> ≠ wave_pred_<B>` | 버전 교체 영향 |
| `99_전체` | 제외 없음 | 디버깅·교차확인 탈출구 (SAM3 축 포함) |

파생 필드 대신 뷰를 쓰는 이유: 스키마 비용 0 이고, 비교쌍이 바뀌면 뷰는 재생성으로 따라가지만
필드는 전량 재계산이 필요하다.

### 4-7. 효과

| 지표 | 현재 | 설계 후 | |
|---|---:|---:|---|
| 기본 화면 flat 필터 | **686** | **83** | **−88%** |
| 기본 펼침 위젯 | 사실상 전량 노출 | **12** | |
| 새 뱅크 버전 1개당 필터 증가 | **+15.6** | **+0** | allowlist 라 자동 제외 |
| 삭제되는 필드 | — | **0** | 무손실 |
| 신규 필드 | — | **0** | 설정만 변경 |

---

## 5. 마이그레이션 (sourcei) — 무손실, 설정 전용

| 단계 | 작업 | 되돌리기 |
|---|---|---|
| **M0** | 현재 `app_config`(sidebar_groups·active_fields·color_scheme) + 저장뷰·워크스페이스 목록을 JSON 으로 덤프 | 이 덤프가 롤백 전체 |
| **M1** | `ds.info["bank_run"]` 기록 (`slots` + 스키마에 존재하는 전 버전 목록). 지금은 출처 기록이 아예 없다 | 키 삭제 |
| **M2** | ✅ **완료** — `fiftyone_app_setup.py filters` 서브커맨드. keep-set → 사이드바 6그룹 + `active_fields` + 저장뷰 5종을 한 번에 생성 (멱등, 기본 dry-run) | — |
| **M3** | ✅ **완료** — `CLASS_FIELD_CANDIDATES` 하드코딩을 `class_field_candidates(ds)` 로 교체, 버전 접미 필드를 `bank_run.slots` 에서 파생 | 코드 되돌림 |
| **M4** | ✅ **완료 (2026-08-14 05:08 KST)** — sourcei 적용. `active_fields`·`color_scheme` 재작성 | `restore` 서브커맨드 |
| **M5** | ✅ **완료** — 아래 검증표 | — |

> **구현 위치 정정**: 초안은 `prompt_geometry.py` 에 `stage_filters` 를 두려 했으나
> **`fiftyone_app_setup.py`** 로 옮겼다. 그 파일이 이미 "App 설정 정본화"(색상·워크스페이스)
> 모듈이고 M3 이 어차피 같은 파일을 고쳐야 했다 — App 설정 소유자를 하나로 유지한다.
> `prompt_geometry.py` 는 numpy 를 끌고 오는 분석 계산 모듈이라 설정 작업에 맞지 않는다.
| **M6** | ✅ **완료 (2026-08-14)** — `prompt_scores_export.py:suffixes()` 가 vtag 후보를 만들도록 수정 + 회귀 단언 4종 추가. 검증은 아래 | `git revert` |
| **M7** | ✅ **완료 (2026-08-14)** — `bank_tags_contract.py` 신설. 계약 4종(C0 생산자 정본 / C1 충돌 / C2 도달성 / C4 라이브 스키마) + 폴백(C3). 검증은 아래 | 파일 삭제 |

**필드 삭제·생성 단계가 없다.** 전부 설정과 코드이므로 롤백은 M0 덤프 복원 + 코드 되돌림이다.

### M6 완료 기록 (2026-08-14)

수정 (`docker/analysis/prompt_scores_export.py`):

```python
out = ["v" + "_".join(parts), "v" + "".join(parts)]   # vt + vtag(추가)
if len(parts) >= 3:
    out.append("v" + "".join(parts[-3:]))             # 구 표기 폴백
return list(dict.fromkeys(out))                        # 3파트 버전은 vtag==구 → 중복 접기
```

후보 순서는 **신 → 구**라 기존 해석 결과가 바뀌지 않는다 (`pred_margin_{v}` 는 그대로 `v080`).

| 검증 | 결과 |
|---|---|
| `selftest` (호스트 + 컨테이너) | 통과. 회귀 단언 4종 추가 — vtag 후보 존재 / 구 슬러그 폴백 유지 / 3파트 중복 접힘 / `v1.0.5.0`≠`v2.0.5.0` 붕괴 방지 |
| `winner_gidx` 해석 (sourcei·source-h, 3개 버전 표본) | **0건 → 전건** (`winner_gidx_v1080`/`v1084`/`v10132`) |
| E2E `export --dataset sourcei --bank v1.0.8.0` | `argmax_k1` frames 7,498 / sentences 12,480, `[pred_v1_0_8_0 / pred_margin_v080 / winner_gidx_v1080]`. 산출 `prompt_frame_pred.jsonl` 14,996행 |
| 계약 검증 (`check`) | **수정 후 0건** / 동일 산출물로 pre-fix 재현 시 **7,498건** |
| 배포 | `docker cp` 전 md5 대조로 `/workspace` drift 없음 확인 후 반영 |

**남은 것 (이 수정 범위 밖)**: `argmax_k1` 의 `pred_<vt>` 는 여전히 `v1.0.8.0` 에만 존재한다.
sourcei 에 `pred_v1_0_8_4` 가 아예 없어 다른 버전은 규칙 자체가 건너뛰어진다 — 리졸버가 아니라
**attach 단계의 데이터 공백**이므로 별건이다.

### M7 검증 — `bank_tags_contract.py`

```bash
docker exec docker-analysis-1 python /workspace/bank_tags_contract.py
python3 bank_tags_contract.py --pure-only     # fiftyone 없는 호스트/CI (C4 생략)
```

| 검증 | 결과 |
|---|---|
| 컨테이너 전체 실행 | **계약 통과 12건 / 위반 0** — 버전 32개(경계 7 + 라이브 29, 중복 제거) |
| C4 라이브 도달성 | sourcei 실재 필드 **176건** · source-h **180건** 전부 도달 가능 |
| **빨개지는지 확인** (필수) | pre-fix `suffixes()` 를 주입해 재실행 → **exit 1, 위반 7건**. C2 가 "생산자 태그 미포함 31건", C4 가 "도달 불가 **29/176**건"(= `winner_gidx_*` 정확히 29개)을 지목 |
| 기존 selftest 회귀 | `prompt_scores_export.py selftest` 통과 / `user-prompt-probe` self-check 통과 (플러그인은 미수정) |
| 환경 저하 | 호스트에 numpy 없어 플러그인·`prompt_geometry` 로드 실패 시 **fail-soft** — 건너뛴 검사를 출력에 남기고 나머지는 수행. 검사기가 환경 때문에 죽지 않는다 |

⚠️ 이 파일은 **리졸버 층만** 덮는다. 산출물 층(`prompt_frame_pred` 등)은
`prompt_scores_export.py validate` 가 담당하며, 그 정기 실행 경로는 여전히 없다 (§5-3).

### M0–M5 검증 (2026-08-14, sourcei)

```bash
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py dump sourcei,source-h <path>
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py filters sourcei            # dry-run
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py filters sourcei --apply
```

| 항목 | 결과 |
|---|---|
| 사이드바 배치 | ① 판정 24 · ② 층화 20 · ③ 이벤트 맥락 8 · ④ 근거·심각도 31 = **큐레이션 83경로**, ⑨ 버전별 원자료 468 + ⑧ 기타 135 (접힘). **686 전 경로가 배치돼 미분류 0** |
| 기본 펼침 | 3그룹 = **12 위젯** |
| `active_fields` | 28 — 워크스페이스 6종의 Color-by 루트가 **전부 포함**됨 (`rule_cross`·`sam3_hit` 포함. 큐레이션에선 빠진 필드지만 빼면 그 워크스페이스가 에러 화면이 된다) |
| 저장 뷰 | `00_분석` 7,498/flat **94** · `01_오탐` **123** · `02_near_miss` **1,503** · `03_AB불일치` **308** · `99_전체` 7,498/flat 686 |
| `bank_run` | `slots={a: v1.0.8.0, b: v1.0.8.4}` · 스키마 태그 60개 기록 (이전에는 출처 기록 자체가 없었다) |
| 무손실 확인 | 필드 수 **248 / flat 686 변화 없음**. `ds.info` 기존 `probe_*` 9키 보존 |
| 도구 회귀 | `bank_tags_contract` 통과 · `prompt-compare` 조인 3버전 정상(229/503/275장) · `prompt_scores_export` 해석 정상 |
| 멱등성 | `--apply` 재실행 시 동일 결과 |
| 오적용 가드 | `CURATED_DATASETS={"sourcei"}` — source-h 실행 시 이유를 출력하고 skip (`--force` 로만 강행) |

> **설계 정정 — "기본 뷰"는 자동 적용되지 않는다.**
> `fo.DatasetAppConfig` 에 default-view 필드가 **없다**(실측: `active_fields`,
> `color_scheme`, `sidebar_groups`, `media_fields`, `plugins` 등뿐). 따라서 `00_분석` 은
> 분석가가 뷰 드롭다운에서 골라야 적용된다.
> → **주 수단은 뷰가 아니라 접힌 사이드바 그룹**이다. 아무것도 고르지 않아도 기본 화면은
> 펼친 3그룹 12위젯이고, 나머지 603경로는 접힌 그룹 뒤에 있다. `00_분석` 뷰(flat 94)는
> 스키마까지 줄이고 싶을 때의 **강한 옵션**으로 남는다.

> **83 vs 94**: 83 은 큐레이션 필드의 flat 경로 수이고, 94 는 여기에 FiftyOne 기본 필드
> (`id`/`filepath`/`tags`/`metadata`/`created_at`/`last_modified_at`)의 11경로를 더한
> 뷰의 실제 총계다. `exclude_fields` 는 기본 필드를 제외할 수 없다.

### M9 — 규칙별 예측 슬롯 ✅ 완료 (2026-08-14, 사용자 요청)

문제: 필드명이 버전으로 갈려 사이드바가 `wave_pred_v1_0_8_0` / `wave_pred_v1_0_8_4` 로
보였고, 더 나쁘게는 **`argmax_k1` 의 버전 비교가 아예 불가능**했다 —
`pred_<vt>` 가 `v1.0.8.0` 에만 존재한다.

`fiftyone_app_setup.py slots` 신설. 규칙 × 슬롯의 **버전 없는** 필드를 만든다:

| 슬롯 | 출처 | sourcei 결과 |
|---|---|---|
| `pred_wave_{a,b}` | `wave_pred_<vt>` 복사 (분포 IoU = 제품 판정 규칙) | 7,498/7,498 |
| `pred_argmax_{a,b}` | **`winner_gidx_<vtag>` → 문장 `category` 조인** | 7,498/7,498 |
| `pred_topk_{a,b}` | `vote_<vt>` 복사 | 생략 — sourcei 는 이 규칙으로 채점된 적 없음 |

**데이터 공백은 재계산 없이 메워졌다.** `winner_gidx_<vtag>` 는 29버전 전부 존재하고,
그 gidx 가 가리키는 문장의 클래스가 곧 argmax 예측이다. 유도 검증: `v1.0.8.0` 의 유도값이
기존 `pred_v1_0_8_0` 과 **7,498/7,498 완전 일치**. GPU·임베딩 재계산 0.

어느 뱅크인지는 `bank_run.slots` + 사이드바 그룹 라벨이 말한다.
비교쌍 교체 = `slots --slots=X,Y --apply` 후 `filters --apply`.

**즉시 나온 결과 — 두 규칙이 반대로 움직인다** (GT=normal 4,323장 기준 오탐):

| 규칙 | A=v1.0.8.0 | B=v1.0.8.4 | |
|---|---:|---:|---|
| `argmax` (K=1) | 28 | **444** | 16배 악화 |
| `wave` (제품 규칙) | 123 | **53** | 2.3배 개선 |

A→B 판정 변화는 argmax 968장 / wave 308장. **한 숫자로 "어느 버전이 낫다"고 말하면
틀린다** — 이 데이터셋의 존재 이유(FP 스트레스)에서 규칙에 따라 결론이 뒤집힌다.
이전에는 볼 수 없던 비교다.

⚠️ 이 서브커맨드만 **필드를 쓴다**(`filters` 는 설정 전용). sourcei 248 → **252 필드**
(flat 686 → 710), `00_분석` 뷰 flat 94 → **106**, 기본 펼침 12 → **14 위젯**.
롤백은 `ds.delete_sample_fields(["pred_wave_a", ...])` — 원본은 손대지 않았으므로 언제든 재생성.

### 5-1. 실행 원칙

- 코드는 `docker/analysis/` 에 커밋 후 `docker cp` 로 반영.
  **분석 코드만 담은 `main` push 금지** — `docker/analysis/**` 는 `paths-ignore` 밖이라
  main push 가 dagster 3종만 recreate 시켜 라벨링을 끊고 analysis 에는 아무 효과가 없다.
- 공유 호스트다. M4 는 다른 세션의 App 화면을 바꾸므로 사전 고지.

### 5-2. 보류: 29버전 행렬의 npz 이관 (착수 조건 명시)

§1-5 의 결론대로 **원리적 정답은 버전 축을 FiftyOne 에서 빼는 것**이다:
per-version 행렬(29 × 7,498 × 5)은 이미 `work/geometry/wave_<tag>.npz` **31개 6.4 MB** 로
디스크에 있다. FiftyOne 은 비교쌍 2개만 들면 되고, 그러면 §1-4 의 명명 부채가 근원에서 사라진다.

**지금 하지 않는 이유** — 측정상 아픈 데가 없고(§1-5), 실작업이 가볍지 않다:

- `@user/prompt-probe:_cos_columns` 는 `frames_view.values(f)` 의 **뷰 순서**로 인덱싱하는데
  (`won_idx` 가 그 순서의 정수 인덱스), npz 행 순서는 `embed.npz` 의 `key` 순서다.
  → **프레임 id ↔ npz 행 매핑**을 새로 세워야 한다 (`stage_score` 의
  `key2i = {str(k): i for i, k in enumerate(z["key"])}` 가 참고 구현).
- 대상이 **enabled 상태의 오퍼레이터**라 회귀 시 큐레이션 근거 컬럼이 조용히 빈다(§8-1).

**착수 조건 (하나라도 충족되면 별도 스펙으로 승격):**

1. **세 번째 소비자**가 per-version 필드를 읽어야 할 때 — D7 리졸버가 있어도 세 번째
   접근 경로가 생기는 순간 이관이 더 싸진다.
2. `@user/prompt-probe` 를 **다른 이유로 어차피 손댈 때** — 매핑 작업을 그 변경에 얹는다.
3. 컬렉션이 **실제로 아플 때** — 판정 기준은 §9 의 `collstats` 재측정.
   현재 sourcei 189 MB / avgObjSize 25.2 KB / 스키마 로드 1.1 ms 가 기준선이다.
4. 뱅크 버전이 **60종을 넘을 때** (현재 29). 그때 top 필드는 ~450 이 된다.

**선행 조건**: 이관 전에 §8-4 의 `wave_vs_topk` npz 복원 가능성부터 확인해야 한다.
복원 불가면 이관은 그 필드에 대해 **데이터 손실**이 되므로 별도 백업이 선행되어야 한다.

### 5-3. M8 — 정기 실행 경로 ✅ 완료 (2026-08-14)

> **원인 재정정.** 앞선 판은 "`export → validate` 정기 실행 경로가 없다"고 적었으나
> 배선은 이미 있었다 — `cmd_export` 는 마지막 줄이 `return validate_dir(args.out)` 이고
> `validate_dir` 은 위반 시 **1을 반환**한다(`:284-301`). 즉 export 는 이미 자체 검증하고
> 시끄럽게 실패한다. 없던 것은 검사기도 배선도 아니라 **트리거**였다 —
> **아무도 `export` 를 돌리지 않았다.**

`docker/analysis/bank_health.sh` 신설. 검사 대상은 **리졸버 층**(`bank_tags_contract.py`)이다:

- 초 단위로 끝나고 D7-1 사고를 정확히 잡는다(pre-fix 에서 C4 가 "도달 불가 29/176건" 으로 실패).
- 산출물 층은 `export` 가 자체 검증하므로 중복하지 않는다. 29버전 export 는 1시간 규모라
  정기 실행에 맞지 않는다.
- **`flock -n` 필수** — 2026-07-06 에 2시간 주기 cron 이 실행 시간을 넘겨 3중 중첩되며
  호스트를 스왑 쓰래싱(load 165)으로 몰아넣은 이력이 있다.
- 컨테이너 미기동 시 조용히 skip(장애 아님), 위반 시 로그 + stderr + 비정상 종료.

**저하 실행 방지**: 요청한 데이터셋을 못 읽으면 `bank_tags_contract.py` 가 **FAIL 로 처리**한다.
실측으로 걸린 함정 — 초판 래퍼가 `--datasets "sourcei source-h"` 을 한 인자로 넘겨 라이브
검사가 통째로 빠진 채 "통과(버전 7개)" 를 보고했다. 가드를 래퍼가 아니라 **검사기 안**에
둔 이유다(래퍼는 또 만들어지고 또 빠뜨린다).

| 실행 | 결과 |
|---|---|
| 정상 | `OK ✅ 계약 통과 12건 — 버전 32개` · exit 0 |
| 없는 데이터셋 | `FAIL rc=1` · exit 1 (초판에서는 exit 0 "버전 7개" 로 통과했다) |
| 중첩 | `SKIP 이전 실행이 진행 중` · exit 0 |

**미설치**: crontab 등록은 공유 호스트 상태 변경이라 사용자 승인 대기 중. 제안 줄:

```cron
17 7 * * * /home/user/work_p/Datapipeline-Data-data_pipeline/docker/analysis/bank_health.sh
```

### 5-4. 이번 범위 밖 (남은 것)

- 구 슬러그 13필드(`*_v080`, `*_v084`) 정리. D7 이 후보 순서를 정본화하면 신 슬러그가 항상
  먼저 잡히므로 **제거 가능해지지만**, `prompt_scores_export` 가 참조하는
  `pred_margin_v080` 은 신 슬러그 쌍이 없어 백필이 선행되어야 한다.
- 생산자 측 규칙 단일화(`vt` 를 버리고 `vtag` 로 통일). D7 이 읽기를 흡수하므로 급하지 않고,
  하려면 기존 필드 전량 재백필이 따라온다.

---

## 6. 전 데이터셋 확장

| 데이터셋 | 현재 | 조치 |
|---|---|---|
| `sourcei` | 686 flat / 498 버전접미사 | §5 전 단계 |
| `source-h` | 663 flat / **568 버전접미사(86%)**, `sidebar_groups=None` | 같은 레시피. **필터 세트는 §4-4 를 재측정해 재선정** — sourcei 목록 복사 금지. source-h 워크플로(flip 검수·사분면·프롬프트 품질·gap)에 맞춰야 하고, `daynight` 처럼 한쪽에서만 사는 축이 있다. source-h 의 라이브 `active_fields`·`color_scheme` 도 `wave_pred_v1_0_8_0` 을 참조하므로 M4 를 동일하게 적용 |
| `frames_captions` | 69 flat / **0 버전접미사** | 무변경. 이 설계의 기준 구현 |
| 신규 데이터셋 | — | 처음부터 `bank_run` + `stage_filters` 로 생성 |

---

## 7. 롤백

| 실패 유형 | 복구 |
|---|---|
| 사이드바/뷰/색이 깨짐 | M0 덤프 복원 — **필드는 건드리지 않았으므로 데이터 위험 0** |
| App 이 Color-by 로 죽음 | `active_fields` 에 해당 경로 추가 (allowlist 위반이 원인) |
| 오퍼레이터 `p_iou` 가 빈 값 | `wave_iou_<class>_<tag>` 존재 확인. 이 설계는 지우지 않으므로 발생 시 별개 원인 |
| (장래 가지치기 시) 필드 유실 | `/data/fiftyone/sourcei/work/geometry/wave_<tag>.npz` 31개 6.4MB → `set_values`. 단 **`wave_vs_topk` 재구성 가능성은 미검증**(§8-4) |

---

## 8. 검증

### 8-1. 반증 검증에서 초안이 뒤집힌 지점 ⚠️

r1 의 핵심 주장 "`wave_*` 계열은 생산자 외 소비자가 없다" 는 **REFUTED (confidence: high)**.
발견된 소비자:

| 소비자 | 위치 | 삭제 시 증상 |
|---|---|---|
| `@user/prompt-probe` v1.1.0 (**enabled**) | `plugins/user-prompt-probe/__init__.py:247` `_pick_field(sch, "wave_iou_"+c+"_{tag}", version)` → `:585` `p_iou` 컬럼 | `:267` 이 `None` 을 넣고 **조용히 진행**. CSV 는 정상 생성되고 제품규칙 IoU 열만 전부 빈다. 채택 근거 3축(코사인/마진/제품IoU) 중 하나가 소리 없이 사라짐 |
| `prompt_scores_export.py` | `:46` `"dist_iou": ("wave_pred_{v}", …)` | `resolve()` 실패 → `⏭ dist_iou … 건너뜀` 후 `continue`. 거버넌스 JSONL 에서 **제품 판정 규칙 전체가 누락**되는데 exit 0 이고 validator 도 통과한다 — 규칙이 통째로 건너뛰어져 **행이 0개**라 검사할 대상 자체가 없기 때문이다. (D7-1 의 winner 결손과 대조: 그쪽은 행이 `null` 로 **쓰이므로** validator 가 7,498건으로 잡는다. `pred_` 결손 = 무증상, `winner` 결손 = 유증상.) |
| sourcei 라이브 `active_fields` (exclude=False) | `wave_pred_v1_0_8_0` 포함 | allowlist 에 없는 필드로 Color-by → App 에러 화면 (저장소 실측 기록: `prompt_geometry.py:3500-3504`) |
| sourcei 라이브 `color_scheme` | `wave_pred_v1_0_8_0` 참조 | 색 지정이 유령 필드를 가리킴 |
| sourcei 라이브 `sidebar_groups` | `① 판정` 6경로, `⑨ 기타` 4경로 | 유령 경로 |

결정적으로, r1 이 제안한 **버전 없는 슬롯명은 두 소비자의 태그 해석기가 영원히 못 찾는다** —
`_ver_tags:213` 은 `["v1080","v080"]` 만, `suffixes:58` 은 `["v1_0_8_0","v080"]` 만 만든다.
즉 r1 은 "지금 버전"이 아니라 **모든 버전에 대해** 두 도구를 영구히 깨뜨렸을 것이다.

이 발견이 r2 의 무손실 설계를 강제했다. **두 증상 모두 예외도 로그도 없이 조용하다** —
분석 스택에서 가장 나쁜 실패 유형이라, 배포 후 한동안 아무도 몰랐을 가능성이 높다.

### 8-2. 확인 완료 (문제 없음)

- sourcei 저장 뷰 3종(`probe_fixed`, `probe_entered`, `rule_cross_diff`) — wave 참조 0
- sourcei 워크스페이스 6종(`explore`, `kind`, `sam3`, `site`, `rules`, `compare`) — wave 참조 0
- `/data/fiftyone/sourcei/report/` — wave 참조 0
- `@user/prompt-compare` 는 `winner_gidx_*` 로만 조인 (`wave_gain`/`wave_role` 만 별도 참조)
- 컨테이너 `/workspace` 미추적 스크립트의 `wave_role` 참조는 **prompts 데이터셋 필드**로 무관
- 패널 버전 드롭다운은 `<dataset>-prompts` 의 distinct `bank_version` 에서 채워짐
  (프레임 스키마가 아님) → 프레임쪽 필드 노출과 무관

### 8-3. 부수 발견 — 이 설계와 무관하게 이미 깨져 있던 것

D7 계약을 라이브에 적용하는 순간 `prompt_scores_export.py` 의 winner 필드 해석이
**sourcei·source-h 양쪽 전 버전에서 실패**하고 있음이 드러났다 (§3 D7-1).
이 문서의 어떤 변경도 원인이 아니며, 2026-08-11 생산자 태그 규칙 변경 이후로 계속된 상태였다.
필터 작업과 분리해 **M6 으로 먼저 수습했다 (2026-08-14 완료·검증, §5 M6 완료 기록).**

교훈: 이 세션에서 스키마 드리프트가 **두 번** 나왔고 **둘의 성격이 다르다.**

| | `@user/prompt-probe` 의 `p_iou` (§8-1) | `prompt_scores_export` 의 winner (D7-1) |
|---|---|---|
| 탐지 장치 | **없음** — `:267` 이 `None` 을 넣고 CSV 는 정상 생성 | **있음** — `validate` 가 7,498건 보고 |
| 실제로 드러났나 | 아니오 (이 세션의 반증 검증이 처음 발견) | 아니오 — **아무도 돌리지 않았다** |

즉 하나는 **탐지 불가**, 하나는 **탐지되었으나 미실행**이다. D7 계약 테스트는 전자를 덮고,
후자에는 `export → validate` 를 정기적으로 도는 경로가 따로 필요하다 (§5-3).
두 경우 모두 공통 원인은 같다 — **생산자의 태그 규칙 변경이 소비자에게 전달되는 경로가 없다.**

### 8-4. 미해결 / 알려진 공백

- **`wave_vs_topk` npz 복원 가능성 — ✅ 검증 완료 (2026-08-18), 복원 가능.**
  `wave_<tag>.npz` 는 `pred`(wave)만 갖고 `pk`(top-k)는 없지만, `pk = bank_vote_stream(
  embed.npz 벡터, 뱅크 npz, RULE_K)` 로 결정론적 재계산된다 — 3버전(v1080/v10132/v1010)
  × 7,498 프레임 전수 대조 **100% 일치(불일치 0)** 실측. 키 매핑은 위치 인덱스가 아니라
  `basename(dirname)/basename(filepath)` 문자열 조인이라(§`stage_wave`) §5-2 의 뷰-순서
  리스크와 무관하다. **전제**: `embed.npz`·뱅크 npz·`ledger.jsonl` 보존 + `RULE_K` 는
  `wave.json` 에 실행값(현재 10)이 남아 재현 가능. **§5-2 착수 시 별도 백업 불필요** —
  세 원자재는 이관 자체의 필수 입력이라 이미 보존 대상이다.
- **Codex 교차검증 없음** — 워크스페이스 크레딧 소진으로 호출 실패. 5개 모델 중 4개만 응답.
- 응답한 4개 모델은 A안(고정 슬롯)에 **만장일치였으나 전부 틀렸다.** 어느 모델도 필드군별
  소비자를 확인하지 않았고, 한 모델은 "패널 드롭다운이 프레임 스키마에서 버전을 읽는다"는
  사실오류를 제시했다. 설계를 바로잡은 것은 모델 합의가 아니라 **반증 검증과 직접 측정**이었다.
- §4-4 의 분별력 수치는 **현재 7,498 프레임 기준**. 데이터가 늘면 재측정 필요.
- `ground_truth`(normal 58%)와 `PROFILES` 주석의 GT(normal 721구간)는 **단위가 다르다**
  (프레임 vs 구간). FP 스트레스 성격은 구간 단위에서 성립한다.
- `sourcei` 사이드바 8그룹의 원 작성자는 끝내 특정하지 못했다. M2 가 코드 생성으로
  대체하므로 실질 문제는 해소되지만, 기존 구성의 의도가 있었다면 유실될 수 있다.

---

## 9. 부록 — 재현 명령

```bash
# flat 스키마 = App 사이드바 필터 엔트리 수
docker exec docker-analysis-1 python -c "
import fiftyone as fo
for n in ('sourcei','source-h','frames_captions'):
    ds = fo.load_dataset(n)
    print(n, len(ds), len(ds.get_field_schema()), len(ds.get_field_schema(flat=True)))"

# 필드군별 flat 전개
docker exec docker-analysis-1 python -c "
import re, collections, fiftyone as fo
ds = fo.load_dataset('sourcei')
pat = re.compile(r'^(.*?)_(v\d+(?:_\d+)*)(?=\$|\.)')
fam = collections.Counter()
for f in ds.get_field_schema(flat=True):
    m = pat.match(f)
    if m: fam[m.group(1)] += 1
print(fam.most_common())"

# 분별력 (distinct / 최대버킷 / null)
# ⚠️ 값 경로를 `.label` 로 일괄 붙이면 **틀린다**. EmbeddedDocumentField 는 Classification
#    뿐 아니라 Detections/Classifications 도 되고, 그 경우 경로는 `.detections.label` /
#    `.classifications.label` 이다. 없는 경로는 `{None: N}` 을 돌려주는데 이것을 "빈 필드"로
#    오독한 것이 2026-08-14 `sam3` 오판의 원인이었다 (실제로는 detection 25,007개).
docker exec docker-analysis-1 python -c "
import fiftyone as fo
ds = fo.load_dataset('sourcei')
def value_path(ds, f):
    fld = ds.get_field(f)
    if not isinstance(fld, fo.EmbeddedDocumentField):
        return f
    dt = getattr(fld, 'document_type', None)
    name = getattr(dt, '__name__', '')
    return f + {'Classification': '.label', 'Detections': '.detections.label',
                'Classifications': '.classifications.label',
                'Polylines': '.polylines.label', 'Keypoints': '.keypoints.label',
                }.get(name, '.label')
for f in ['ground_truth','event_kind','camera','close_call','rule_cross','sam3','sam3_labels']:
    cv = ds.count_values(value_path(ds, f))
    print(f, value_path(ds, f), len(cv), sorted(cv.items(), key=lambda kv: -kv[1])[:4])"

# 라이브 App 상태의 버전 하드코딩 (M0 덤프 대상)
docker exec docker-analysis-1 python -c "
import re, fiftyone as fo
ds = fo.load_dataset('sourcei')
af = ds.app_config.active_fields
print('active_fields wave:', [p for p in af.paths if 'wave' in p])
print('color_scheme wave:', re.findall(r'[\w.]*wave[\w.]*', str(ds.app_config.color_scheme.to_dict())))
for g in ds.app_config.sidebar_groups:
    w = [p for p in g.paths if 'wave' in p]
    if w: print(g.name, g.expanded, w)"

# 소비자 전수 (플러그인 포함 — 초안이 놓친 지점)
cd docker/analysis && grep -rn "wave_pred\|wave_iou\|wave_vs_topk\|winner_gidx" \
  --include=*.py . | grep -v "^./prompt_geometry.py"
docker exec docker-analysis-1 fiftyone plugins list

# 스키마 비용 — §5-2 착수조건 3의 판정 기준. 기준선(2026-08-14):
#   sourcei 189MB / avgObjSize 25.2KB / schema 1.1ms
docker exec docker-analysis-1 python -c "
import time, fiftyone as fo
for n in ('sourcei','source-h','frames_captions'):
    ds = fo.load_dataset(n)
    st = ds._sample_collection.database.command('collstats', ds._sample_collection.name)
    t0=time.time(); flat=ds.get_field_schema(flat=True); t1=time.time()
    print(n, st['count'], st['avgObjSize'], round(st['size']/1e6,1), len(flat), round(1000*(t1-t0),1))"

# D7 계약 검사 (M7) — 리졸버 층 회귀 탐지기
docker exec docker-analysis-1 python /workspace/bank_tags_contract.py
python3 bank_tags_contract.py --pure-only      # fiftyone 없는 환경 (C4 생략)

# 태그 해석기 2벌의 현재 동작 (D7 계약 테스트가 감시할 대상)
cd docker/analysis && sed -n '206,224p' plugins/user-prompt-probe/__init__.py
cd docker/analysis && sed -n '58,72p' prompt_scores_export.py

# D7-1 라이브 버그 재현 — winner 가 전 버전에서 None 인지 확인 (M6 의 회귀 판정 기준)
docker exec docker-analysis-1 python -c "
import sys; sys.path.insert(0,'/workspace')
import prompt_scores_export as pse, fiftyone as fo
sch = fo.load_dataset('sourcei').get_field_schema()
for bank in ['v1.0.8.0','v1.0.8.4','v1.0.13.2']:
    print(bank, pse.suffixes(bank),
          {r: pse.resolve(sch, f, bank) for r,(f,_,_) in pse.RULE_FIELDS.items()},
          'winner=', pse.resolve(sch, 'winner_gidx_{v}', bank))
print('winner_gidx_v080', 'winner_gidx_v080' in sch, '/ winner_gidx_v1080', 'winner_gidx_v1080' in sch)"

# 계약 테스트가 소비자를 불러올 수 있는지 (D7 의 전제 — 검증 완료)
docker exec docker-analysis-1 python -c "
import importlib.util, sys
spec = importlib.util.spec_from_file_location('probe',
    '/data/fiftyone/datasets/__plugins__/user-prompt-probe/__init__.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
sys.path.insert(0,'/workspace'); import prompt_scores_export as pse, prompt_geometry as pg
print(m._ver_tags('v1.0.8.0'), pse.suffixes('v1.0.8.0'), pg.vtag('v1.0.8.0'))"
```

---

## 10. 런북 — 새 뱅크 버전이 생겼을 때

### 10-1. 실시간으로 보이나? — 층에 따라 다르다

| 층 | 자동인가 | 근거 |
|---|---|---|
| **패널 버전 드롭다운** (문장 비교) | ✅ **준자동** | 드롭다운은 `<ds>-prompts` 의 distinct `bank_version` 에서 채워지고(`user-prompt-compare:727`), 캐시 키에 `ds.last_modified_at` 이 들어간다(`load_prompt_bundle:67`). `promptmap` 이 문장을 쓰면 키가 바뀌어 **다음 상호작용에서 자동 반영**된다. 코드 수정 0 |
| **사이드바 필터** (프레임) | ❌ **의도적으로 아니다** | 필터 세트가 allowlist 라 새 버전 필드는 `⑨ 버전별 원자료` 접힌 그룹으로 **자동 편입**된다 → 기본 화면이 안 늘어난다(G1 의 목적 그 자체). 비교하려면 **슬롯 교체 1회** |

즉 "필터에 계속 추가해야 하나?"의 최종 답: **추가할 필요 없다.** 보고 싶은 두 버전을
슬롯에 올릴 뿐이고, 그 조작이 사이드바 구성을 바꾸지 않는다.

### 10-2. 절차 (예: `v1.0.14.0` 추가)

```bash
export BANK_PROFILE=sourcei
NEW=v1.0.14.0
# ⚠️ ORDER 는 ds.info["bank_run"]["bank_order"] 를 그대로 쓰고 **끝에만 append** 한다
ORDER="v1.0.1.0,v1.0.2.0,…,v1.0.13.2,$NEW"

# 1. 뱅크 벡터 (CSV → /embed_text → npz).  1.6만 문장 ≈ 2분
python prompt_geometry.py bank --csv <path.csv> --version $NEW --profile sourcei
#    큐레이션 파생 버전이면 대신: bankfrom --tag <tag> --version $NEW --notes "..."

# 2~4. 프레임 필드 + 문장 샘플
BANK_LIST="$ORDER" python prompt_geometry.py attach     --profile sourcei   # winner_gidx_v10140 …
BANK_LIST="$ORDER" python prompt_geometry.py wave       --profile sourcei   # wave_pred_/wave_iou_ …
BANK_LIST="$ORDER" python prompt_geometry.py promptmap  --profile sourcei   # → 패널 드롭다운 자동 반영

# 5~6. App 표면 (비교 대상으로 올릴 때만)
python /workspace/fiftyone_app_setup.py slots   sourcei --slots=v1.0.8.0,$NEW --apply
python /workspace/fiftyone_app_setup.py filters sourcei --apply
```

`attach`/`wave`/`promptmap` 의 전제는 `load_all()` 이 읽는 3가지뿐이다 —
`work/ledger.jsonl`, `work/embed.npz`, `<PROMPT_DIR>/<version>.npz`.
**`analyze` 는 전제가 아니다** (cache.npz 를 안 쓴다).

### 10-3. ⚠️ BANK_LIST 순서는 데이터의 일부다

`gidx = BANKS.index(version) * GIDX_OFFSET + 로컬인덱스` 이므로 **순서를 바꿔 재실행하면
기존 `winner_gidx_*` 전부가 다른 문장을 가리킨다** — 조용히, 전 버전에서.

- 정본 순서는 이제 `ds.info["bank_run"]["bank_order"]` 에 기록된다 (M9 에서 추가).
  그전에는 **어디에도 없었고**, gidx 블록에서 역산해야만 알 수 있었다.
- ⚠️ **semver 정렬이 아니다** — 실측 순서에서 `v1.0.10.3` 이 `v1.0.8.4` **뒤**(22번)다.
  `sorted()` 로 재구성하면 전부 어긋난다. 반드시 기록된 순서를 그대로 쓰고 끝에만 붙인다.

### 10-4. ⚠️ gidx 여유 (실측 80% 소진)

| 항목 | 값 |
|---|---|
| `GIDX_OFFSET` | 100,000 |
| 최대 뱅크 (`v1.0.13.2`) | **79,842 문장 (80%)** |
| 블록 침범 | 현재 없음 |

**10만 문장을 넘는 뱅크가 오면 gidx 블록이 이웃 버전을 침범**하고 `winner_gidx` 조인이
조용히 오염된다. `filters --apply` 가 매번 이 여유를 검사해 70% 초과 시 경고하고,
침범이 실제로 발생하면 `gidx_overflow` 에 기록 + 에러를 출력한다.
넘칠 조짐이 보이면 `GIDX_OFFSET` 상향(= 전 버전 `promptmap` 재빌드)이 선행돼야 한다.

### 10-5. 코드 수정이 필요한 곳: 없음

새 버전 때문에 손으로 고쳐야 하는 지점은 **0곳**이다. 검증: 슬롯에 한 번도 넣은 적 없는
`v1.0.13.2` 로 `slots`/`filters` dry-run → 7,498/7,498 해석, 그룹 라벨이
`① 판정 (A=v1.0.8.0 · B=v1.0.13.2)` 로 자동 갱신.

이 세션 이전에는 2곳이 걸렸다 — `fiftyone_app_setup.CLASS_FIELD_CANDIDATES` 하드코딩(M3 해소),
그리고 `prompt_scores_export.suffixes()` 가 애초에 깨져 있었다(M6 해소).
