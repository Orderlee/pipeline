# 분석 계층 표준화 — Phase 0·1 실행 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 코호트를 **설정 1줄**로 편입할 수 있는 계약(레지스트리 + 범용 로더 + 지문 있는 원자적 아티팩트)을 세우고, 표준 러너가 라이브 데이터 위에서 돌게 만든다.

**Architecture:** 기존 씨앗 4개를 연결한다 — `analysis_standard.py` 의 `run(D, outdir)` dict 계약을 유지한 채, (1) 패널에 묻혀 있던 버전/gidx 해석 규칙을 `prompt_data_contract.py` 로 분리하고, (2) `load_sourcei()` 하드 게이트를 `cohort.py` 레지스트리 기반 `load_cohort()` 로 승격하고, (3) 아티팩트를 지문 + 원자적 발행 + `refused` 상태를 갖는 계약으로 바꾼다. 새 의존성은 추가하지 않는다.

**Tech Stack:** Python 3.10+ · numpy · FiftyOne(런타임만) · pytest · ruff(line-length 120)

**Spec:** [`docs/superpowers/specs/2026-09-16-analysis-standardization-design.md`](../specs/2026-09-16-analysis-standardization-design.md)

## Global Constraints

- **ruff line-length 120 · CI 는 ruff 0.7.4.** ⚠️ **CI 는 `src/` 와 `tests/` 만 검사한다**
  ([`lint.yml:45`](../../../.github/workflows/lint.yml#L45)) — `docker/analysis/` 는 대상이 아니다.
  실측: 이미 추적 중인 `prompt_standard.py` 가 ruff 30건을 안은 채 정상 추적돼 있고,
  편입 대상 엔진 2개는 103건(대부분 E701/E702 한 줄 다중문)이다. **엔진 편입은 lint 에 영향이
  없으므로 포맷을 건드리지 않는다** — 로직 무관 대량 diff 가 오히려 이력을 망친다.
  **반대로 새 테스트 파일은 `tests/` 라 ruff 를 반드시 통과해야 한다.**
- **새 테스트는 `.gitignore` allowlist 에 `!tests/unit/<파일>` 로 넣지 않으면 CI 가 영원히 안 돌린다.** 편입 확인은 `git ls-files tests/` 로만 한다. 로컬 pytest 초록은 CI 신호가 아니다.
- **테스트는 FiftyOne·Postgres·MinIO 없이 돌아야 한다.** CI 러너에 셋 다 없다. `docker/analysis/` 모듈은 `importlib.util.spec_from_file_location` 으로 로드한다 (`tests/unit/test_prompt_bank_publish.py` 선례).
- **`docker/analysis/**` 는 배포 `paths-ignore` 대상**이라 이 작업은 라벨링을 끊지 않는다. 동시에 `/workspace` 가 이 repo 의 bind mount 라 **커밋이 곧 라이브 실행 코드**다 — `docker cp` 불필요.
- **브랜치**: `main`/`dev` 직접 커밋 금지. `feature/analysis-standardization` 에서 작업한다. 현재 체크아웃은 `fix/image-embeddings-corrupt-drop` 이므로 먼저 분기한다.
- **산출물 경로 `docker/data/` 는 gitignore 대상이고 rsync 소스도 아니다.** 아티팩트는 배포로 전파되지 않는 호스트 로컬 상태다.
- **`sourcei_stat_ab.py` 의 로직을 이번 계획에서 수정하지 않는다.** git 편입만 한다 (Phase 4 작업).
- 상수 `GIDX_OFFSET = 100000` — 런타임 env 에 의존시키지 않는다.

## File Structure

| 파일 | 상태 | 책임 |
|---|---|---|
| `docker/analysis/prompt_data_contract.py` | 신규 | 버전 정규화 · local gidx · gidx→class 맵 · 중복/오귀속 판정. **순수 함수만** — FiftyOne·DB 를 import 하지 않는다 |
| `docker/analysis/cohort.py` | 신규 | `COHORTS` 레지스트리 · `resolve()` · `refusal()` · `load_cohort()` |
| `docker/analysis/analysis_standard.py` | 수정 | stage 상태(`ok/skipped/refused/error`) · 원자적 발행 · 지문 · sourcei 하드 게이트 제거 |
| `tests/unit/test_prompt_data_contract.py` | 신규 | Task 1 회귀 가드 |
| `tests/unit/test_cohort_registry.py` | 신규 | Task 2 회귀 가드 |
| `tests/unit/test_standard_artifact.py` | 신규 | Task 3 회귀 가드 |
| `.gitignore` | 수정 | 위 테스트 3종 allowlist 편입 |

---

## Task 0: 브랜치 분기 + 엔진 2개 git 편입

**Files:**
- Modify: `.gitignore` (해당 없음 — `docker/analysis/*.py` 는 무시 대상이 아니라 그냥 미추가 상태)
- Track: `docker/analysis/analysis_standard.py`, `docker/analysis/sourcei_stat_ab.py`

**Interfaces:**
- Consumes: 없음
- Produces: 추적되는 `analysis_standard.py` — Task 3 이 이 파일을 수정한다

- [ ] **Step 1: 작업 브랜치 분기**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git checkout -b feature/analysis-standardization
```

- [ ] **Step 2: 편입 전 상태 기록 (회귀 비교 기준)**

```bash
git ls-files docker/analysis/ | grep -c '\.py$'   # 실측 기준선: 133 → 편입 후 135
```

- [ ] **Step 3: 엔진 2개만 편입**

```bash
git add docker/analysis/analysis_standard.py docker/analysis/sourcei_stat_ab.py
```

- [ ] **Step 4: 편입 확인**

```bash
git ls-files --error-unmatch docker/analysis/analysis_standard.py docker/analysis/sourcei_stat_ab.py
```
Expected: 두 경로가 그대로 출력됨 (error 없음)

- [ ] **Step 5: 편입이 lint 를 깨지 않는지 확인 (포맷은 고치지 않는다)**

```bash
grep -n 'ruff check' .github/workflows/lint.yml     # 기대: `ruff check src/ tests/` — analysis 는 대상 아님
```
`docker/analysis/` 가 검사 범위에 없음을 확인만 하고 **포맷은 손대지 않는다**.
로직 무관 103건 일괄 수정은 `sourcei_stat_ab.py` 를 이번 사이클에 건드리지 않는다는
Global Constraints 와 충돌한다.

- [ ] **Step 6: 커밋**

```bash
git commit -m "chore(analysis): 통계 엔진 2종 git 편입 — 추적 파일이 미추적 파일에 의존하던 구조 해소

analysis_standard.py 는 표준 러너의 정본이고 sourcei_stat_ab.py 는 짝비교 선례인데
둘 다 미추적이라 이 호스트에만 존재했다. 패널(추적)이 이들을 쓰게 되면 다른 클론·
컨테이너 재생성에서 조용히 깨진다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 1: `prompt_data_contract.py` — 버전/gidx 해석 계약

패널(4,216줄)에 묻혀 있는 해석 규칙을 분석기가 쓸 수 있게 분리한다. **분석기가 패널을 import 하면 안 된다.**

**Files:**
- Create: `docker/analysis/prompt_data_contract.py`
- Create: `tests/unit/test_prompt_data_contract.py`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: 없음 (순수 함수)
- Produces:
  - `GIDX_OFFSET: int = 100000`
  - `norm_version(v: str | None) -> str`
  - `local_gidx(g: int | None) -> int | None`
  - `build_gidx_class_map(versions, gidxs, categories) -> dict[tuple[str, int], str]`
  - `text_set_hash(texts: list[str]) -> str`
  - `exact_duplicate_groups(preds_by_version: dict[str, bytes]) -> list[list[str]]`
  - `misattribution_suspects(dup_groups, text_hash_by_version) -> list[list[str]]`

- [ ] **Step 1: 실패하는 테스트 작성**

```python
# tests/unit/test_prompt_data_contract.py
"""docker/analysis/prompt_data_contract.py — 버전/gidx 해석 계약.

이 모듈이 틀리면 프레임↔문장 조인이 조용히 0건이 되거나(정규화 누락) 엉뚱한 뱅크의
문장에 귀속된다(오프셋 누락). 둘 다 예외를 내지 않고 **틀린 표**를 만든다.
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[2] / "docker" / "analysis" / "prompt_data_contract.py"
_SPEC = importlib.util.spec_from_file_location("prompt_data_contract", str(_PATH))
pdc = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pdc)


@pytest.mark.parametrize(
    "raw,want",
    [("V1.0.10.3", "1.0.10.3"), ("v1.0.10.3", "1.0.10.3"), ("1.0.13.0", "1.0.13.0"),
     ("  v1.0.8.0  ", "1.0.8.0"), (None, ""), ("vGEN20260904", "GEN20260904")],
)
def test_norm_version(raw, want):
    assert pdc.norm_version(raw) == want


def test_local_gidx_strips_generation_block():
    # 실측: sourcei winner_gidx 는 2,600,012 처럼 블록 오프셋이 얹혀 있다.
    assert pdc.local_gidx(2_600_012) == 12
    assert pdc.local_gidx(0) == 0
    assert pdc.local_gidx(None) is None


def test_gidx_offset_is_a_constant_not_env_derived():
    # 오프셋이 런타임 env 에 의존하면 세대마다 조인이 어긋난다 (알려진 사고).
    assert pdc.GIDX_OFFSET == 100_000


def test_build_gidx_class_map_normalizes_and_masks():
    m = pdc.build_gidx_class_map(
        versions=["V1.0.8.0", "v1.0.8.0", None, "v1.0.8.0"],
        gidxs=[2_600_012, 2_600_013, 5, None],
        categories=["fire", "smoke", "normal", "normal"],
    )
    assert m == {("1.0.8.0", 12): "fire", ("1.0.8.0", 13): "smoke"}


def test_exact_duplicate_groups_finds_identical_prediction_vectors():
    groups = pdc.exact_duplicate_groups({"a": b"\x00\x01", "b": b"\x00\x01", "c": b"\x01\x01"})
    assert groups == [["a", "b"]]


def test_misattribution_suspect_is_same_preds_but_different_texts():
    """실측 v1.0.2.0/v1.0.2.1 — 텍스트는 다른데(자리표시자 vs 실문장) 예측이 같다.

    벡터 귀속이 틀렸다는 서명이다. 텍스트까지 같으면 그냥 양성 중복이라 잡으면 안 된다.
    """
    dup = [["1.0.2.0", "1.0.2.1"], ["1.0.5.1", "1.0.6.0"]]
    th = {"1.0.2.0": "hA", "1.0.2.1": "hB", "1.0.5.1": "hC", "1.0.6.0": "hC"}
    assert pdc.misattribution_suspects(dup, th) == [["1.0.2.0", "1.0.2.1"]]


def test_placeholder_text_is_not_corruption():
    """external_only 뱅크는 텍스트가 전부 자리표시자지만 벡터는 유효하다 — corrupt 아님."""
    dup = [["1.0.13.0", "1.0.13.1"]]
    th = {"1.0.13.0": "hSAME", "1.0.13.1": "hSAME"}
    assert pdc.misattribution_suspects(dup, th) == []
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/unit/test_prompt_data_contract.py -q`
Expected: FAIL — `FileNotFoundError` 또는 `spec_from_file_location` 이 `None` 반환

- [ ] **Step 3: 최소 구현**

```python
# docker/analysis/prompt_data_contract.py
#!/usr/bin/env python3
"""프롬프트 뱅크 데이터 해석 계약 — 생산자(뱅크 빌더)와 소비자(패널·분석기)가 공유한다.

왜 이 모듈이 있나: 버전 정규화·local gidx·중복 판정이 4,216줄짜리 FiftyOne 패널 안에만
있었다. 분석기가 그걸 쓰려면 패널 전체(fiftyone.operators 포함)를 import 해야 하는데,
분석기는 App 밖에서 돌기 때문에 그건 성립하지 않는다. `prompt_standard.py` 가 프롬프트
생성 규칙에 한 것과 같은 일을 데이터 해석 규칙에 한다.

**순수 함수만 둔다** — fiftyone·psycopg2 를 import 하지 않는다. 그래야 CI(셋 다 없음)에서 돈다.
"""

from __future__ import annotations

import hashlib

#: FiftyOne 전역 gidx 에 얹히는 세대 블록 크기.
#: ⚠️ 런타임 env 에서 읽지 않는다 — 세대마다 값이 흔들리면 프레임↔문장 조인이 조용히 어긋난다.
GIDX_OFFSET = 100_000

#: 텍스트가 없는 뱅크(`sentence_storage='external_only'`)가 쓰는 자리표시자 패턴의 접두.
PLACEHOLDER_PREFIX = "(텍스트 없음 #"


def norm_version(v):
    """`V1.0.10.3` / `v1.0.10.3` / `1.0.10.3` → `1.0.10.3`.

    `prompt_banks.version_tag` 는 대소문자·`v` 접두가 흔들린다(실측 55행에 `V1.0.10.3` 과
    `1.0.13.0` 이 함께 있다) — 정규화 없이 등식 조인하면 **예외 없이 0건**이 된다.
    """
    return str(v if v is not None else "").strip().lstrip("vV")


def local_gidx(g):
    """전역 gidx → 뱅크-로컬 행 번호."""
    return None if g is None else int(g) % GIDX_OFFSET


def build_gidx_class_map(versions, gidxs, categories):
    """(버전, gidx, 클래스) 3열 → `{(norm_version, local_gidx): class}`.

    버전·gidx 중 하나라도 None 인 행은 **버린다**. 이 맵의 결손은 예측 복원 실패로
    이어지므로 호출부가 결손 건수를 반드시 보고해야 한다.
    """
    out = {}
    for v, g, c in zip(versions, gidxs, categories):
        if v is None or g is None:
            continue
        out[(norm_version(v), local_gidx(g))] = c
    return out


def text_set_hash(texts):
    """버전의 문장 집합 지문 — 순서 비의존."""
    h = hashlib.sha256()
    for t in sorted((s or "").strip() for s in texts):
        h.update(t.encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()


def exact_duplicate_groups(preds_by_version):
    """예측벡터가 **바이트 동일**한 버전들을 묶는다. 1개짜리 묶음은 반환하지 않는다.

    실측(sourcei 6,032프레임): 31버전 → 고유 24개, 4묶음. 이걸 접지 않으면 다중비교
    family 가 부풀고 표에 같은 내용이 여러 줄로 나온다.
    """
    by = {}
    for ver, vec in preds_by_version.items():
        by.setdefault(vec, []).append(ver)
    return sorted([sorted(v) for v in by.values() if len(v) > 1])


def misattribution_suspects(dup_groups, text_hash_by_version):
    """벡터 귀속 오류 의심 묶음 — **예측은 같은데 문장 집합이 다른** 경우.

    근거(실측): `v1.0.2.0` 은 공급자 JSON 이 `v1.0.2.1` 과 바이트 동일이라 뷰의 점들이
    `v1.0.2.1` 의 벡터다. 텍스트는 자리표시자로 보여 눈에는 다르게 보이지만 예측은 같다.

    ⚠️ 자리표시자 비율을 기준으로 삼으면 안 된다 — `external_only` 뱅크 8종이 텍스트
    100% 자리표시자인데 클래스 라벨은 멀쩡하고 벡터도 유효하다(corrupt 아님).
    """
    out = []
    for grp in dup_groups:
        if len({text_hash_by_version.get(v) for v in grp}) > 1:
            out.append(sorted(grp))
    return out
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/unit/test_prompt_data_contract.py -q`
Expected: 8 passed

- [ ] **Step 5: `.gitignore` allowlist 편입 — 이걸 빼면 CI 가 영원히 안 돌린다**

`.gitignore` 의 `!tests/unit/...` 블록 끝에 추가:

```
!tests/unit/test_prompt_data_contract.py
```

- [ ] **Step 6: 실제 추적 확인**

```bash
git add .gitignore docker/analysis/prompt_data_contract.py tests/unit/test_prompt_data_contract.py
git ls-files tests/unit/test_prompt_data_contract.py
```
Expected: 경로가 출력됨. **출력이 비면 allowlist 가 안 먹은 것이므로 다음 단계로 가지 말 것.**

- [ ] **Step 7: ruff**

Run: `ruff check --line-length 120 docker/analysis/prompt_data_contract.py tests/unit/test_prompt_data_contract.py`
Expected: All checks passed

- [ ] **Step 8: 커밋**

```bash
git commit -m "feat(analysis): 프롬프트 데이터 해석 계약 모듈 분리 — 분석기가 패널을 import 하지 않게

버전 정규화·local gidx·중복/오귀속 판정이 4,216줄 FiftyOne 패널 안에만 있어서
App 밖에서 도는 분석기가 쓸 수 없었다. 순수 함수만 모아 양쪽이 같은 규칙을 보게 한다.

오귀속 판정 기준은 '예측 동일 + 문장집합 상이'다. 자리표시자 비율은 기준이 될 수 없다 —
external_only 뱅크 8종이 텍스트 100% 자리표시자지만 벡터는 유효하다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `cohort.py` — 레지스트리와 명시적 거부

**Files:**
- Create: `docker/analysis/cohort.py`
- Create: `tests/unit/test_cohort_registry.py`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `prompt_data_contract` (Task 1)
- Produces:
  - `COHORTS: dict[str, dict]`
  - `resolve(name: str) -> dict` — 미등록이면 `CohortRefused` 발생
  - `CohortRefused(Exception)` — 속성 `reason_code`, `detail`
  - `refusal_artifact(name, reason_code, detail) -> dict`

- [ ] **Step 1: 실패하는 테스트 작성**

```python
# tests/unit/test_cohort_registry.py
"""docker/analysis/cohort.py — 코호트 레지스트리와 거부 계약.

핵심 회귀 가드: **군집키를 자동 유도하지 않는다.** sitej_subway 는 camera 가 58대라
자동 유도하면 그걸 고르는데, 연출 동시녹화라 카메라 홀드아웃에 누수가 있어 올바른 키는
session 이다. 잘못 잡으면 CI 가 좁아져 '유의하다'는 거짓 결론이 나온다.
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[2] / "docker" / "analysis" / "cohort.py"
_SPEC = importlib.util.spec_from_file_location("cohort", str(_PATH))
co = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(co)


def test_sourcei_group_is_camera():
    assert co.resolve("sourcei")["group"] == "camera"
    assert co.resolve("sourcei")["prompts"] == "sourcei-prompts"


def test_sitej_group_is_session_not_camera():
    # camera 58대가 있어도 session 이어야 한다 — 연출 동시녹화 누수.
    assert co.resolve("sitej_subway")["group"] == "session"


def test_every_cohort_declares_group_and_negative_class_explicitly():
    # 자동 유도 금지. 레지스트리에 없으면 그건 설정 누락이지 기본값 사용 대상이 아니다.
    for name, cfg in co.COHORTS.items():
        assert cfg.get("group"), f"{name}: group 미지정"
        assert cfg.get("negative_class"), f"{name}: negative_class 미지정"
        assert cfg.get("target_classes"), f"{name}: target_classes 미지정"


def test_unregistered_cohort_is_refused_not_defaulted():
    with pytest.raises(co.CohortRefused) as e:
        co.resolve("some_new_site")
    assert e.value.reason_code == "G0_COHORT_NOT_REGISTERED"


def test_frames_is_explicitly_refused_with_reason():
    """frames 는 GT 가 203,869 중 40장뿐이고 군집 필드가 없다 — hard skip 이 계약이다."""
    with pytest.raises(co.CohortRefused) as e:
        co.resolve("frames")
    assert e.value.reason_code == "G0_INSUFFICIENT_GT"


def test_refusal_artifact_forbids_display_and_is_terminal():
    art = co.refusal_artifact("frames", "G0_INSUFFICIENT_GT", "GT 40/203869")
    assert art["status"] == "refused"
    assert art["display_allowed"] is False
    assert art["complete"] is True          # 부분 쓰기와 구별돼야 한다
    assert art["reason_code"] == "G0_INSUFFICIENT_GT"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/unit/test_cohort_registry.py -q`
Expected: FAIL — 모듈 없음

- [ ] **Step 3: 최소 구현**

```python
# docker/analysis/cohort.py
#!/usr/bin/env python3
"""코호트 레지스트리 — 새 현장을 **설정 1줄**로 편입한다.

왜: `docker/analysis/` 파이썬 176개 중 92개가 `"sourcei"` 또는 `sourcei_gt` 경로를
하드코딩하고 있고, 코호트를 인자로 받는 것은 11개뿐이다. 새 현장이 생길 때마다 스크립트를
새로 쓰는 구조를 여기서 끊는다.

⚠️ **군집키(`group`)를 자동 유도하지 않는다.** `sitej_subway` 는 `camera` 가 58대라
자동 유도하면 그걸 고르는데, 그 코퍼스는 연출 동시녹화라 카메라 홀드아웃에 누수가 있고
올바른 군집키는 `session` 이다. 군집키를 잘못 잡으면 신뢰구간이 좁아져 **"유의하다"는
거짓 결론**이 나온다 — 이 계층이 막으려는 사고 그 자체다. 그래서 미등록 코호트는
`camera` 로 조용히 폴백하지 않고 **거부**한다.
"""

from __future__ import annotations

import time

#: 새 현장 편입은 여기 한 줄. 스크립트를 새로 쓰지 않는다.
#: 모든 키는 **명시 필수** — 빠진 값을 추론하지 않는다.
COHORTS = {
    "sourcei": dict(
        prompts="sourcei-prompts",
        gt_field="ground_truth",
        group="camera",
        negative_class="normal",
        target_classes=["falldown", "fire", "smoke"],
    ),
    "sitej_subway": dict(
        prompts="sitej_subway-prompts",
        gt_field="ground_truth",
        group="session",          # ⚠️ camera 아님 — 위 모듈 주석 참고
        negative_class="normal",
        target_classes=["falldown", "fire", "smoke", "intrustion"],
    ),
}

#: 등록하지 않기로 **판정한** 코호트와 그 사유. 침묵보다 명시가 낫다.
REFUSED = {
    "frames": ("G0_INSUFFICIENT_GT",
               "bank_gt 가 203,869 중 40장뿐이고 군집 필드가 없다"),
}


class CohortRefused(Exception):
    """이 코호트로는 산출하지 않는다 — 예외가 아니라 **판정**이다."""

    def __init__(self, cohort, reason_code, detail):
        super().__init__(f"{cohort}: {reason_code} — {detail}")
        self.cohort = cohort
        self.reason_code = reason_code
        self.detail = detail


def resolve(name):
    """코호트 설정 반환. 미등록·거부 대상이면 `CohortRefused`."""
    if name in REFUSED:
        raise CohortRefused(name, *REFUSED[name])
    if name not in COHORTS:
        raise CohortRefused(
            name, "G0_COHORT_NOT_REGISTERED",
            f"COHORTS 에 없다. group 은 자동 유도하지 않는다 — cohort.py 에 한 줄 추가할 것")
    return dict(COHORTS[name])


def refusal_artifact(name, reason_code, detail):
    """거부도 **아티팩트로 발행한다.**

    생략하면 이전 성공 아티팩트가 그대로 남아 소비자가 낡은 표를 계속 그린다
    (이 저장소의 반복 버그 형태 "부재에 기댄 안전"). 거부는 이전 성공본을 **덮어쓴다**.
    """
    return dict(cohort=name, status="refused", reason_code=reason_code, detail=detail,
                display_allowed=False, complete=True,
                generated_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"))
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/unit/test_cohort_registry.py -q`
Expected: 6 passed

- [ ] **Step 5: allowlist 편입 + 추적 확인**

```
!tests/unit/test_cohort_registry.py
```
```bash
git add .gitignore docker/analysis/cohort.py tests/unit/test_cohort_registry.py
git ls-files tests/unit/test_cohort_registry.py
```
Expected: 경로 출력됨

- [ ] **Step 6: ruff + 커밋**

```bash
ruff check --line-length 120 docker/analysis/cohort.py tests/unit/test_cohort_registry.py
git commit -m "feat(analysis): 코호트 레지스트리 — 새 현장을 설정 1줄로 편입

92개 스크립트가 sourcei 를 하드코딩하고 코호트를 인자로 받는 것은 11개뿐이었다.

군집키는 자동 유도하지 않는다. sitej_subway 는 camera 58대가 있어도 session 이 정답이고
(연출 동시녹화 누수), 잘못 잡으면 CI 가 좁아져 거짓 유의가 나온다. 미등록 코호트는
폴백 대신 거부하고, 거부도 아티팩트로 발행해 이전 성공본을 덮는다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: 아티팩트 계약 — stage 상태 · 지문 · 원자적 발행

현재 `run()` 은 스테이지 예외를 기록만 하고 **정상 report 로 계속**하며([`:330`](../../../docker/analysis/analysis_standard.py#L330)), `json.dump` 로 직접 써서 부분 JSON 을 노출할 수 있다.

**Files:**
- Modify: `docker/analysis/analysis_standard.py` (`run()` 과 그 주변)
- Create: `tests/unit/test_standard_artifact.py`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `cohort.refusal_artifact` (Task 2)
- Produces:
  - `publish_atomic(payload: dict, path: str) -> None`
  - `stage_status(R: dict) -> dict[str, str]` — `ok|skipped|refused|error`
  - `R["status"]`, `R["complete"]`, `R["display_allowed"]`

- [ ] **Step 1: 실패하는 테스트 작성**

```python
# tests/unit/test_standard_artifact.py
"""docker/analysis/analysis_standard.py — 아티팩트 발행 계약.

이 계약이 없으면 소비자(FiftyOne 패널)가 **부분 JSON** 이나 **스테이지가 죽은 report** 를
정상으로 읽는다. 둘 다 예외 없이 틀린 표를 만든다.
"""

from __future__ import annotations

import importlib.util
import json
import math
import pathlib

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[2] / "docker" / "analysis" / "analysis_standard.py"
_SPEC = importlib.util.spec_from_file_location("analysis_standard", str(_PATH))
std = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(std)


def test_publish_atomic_leaves_no_partial_file_on_serialization_failure(tmp_path):
    """NaN 은 JSON 표준이 아니다 — allow_nan=False 로 터뜨리고 파일을 남기지 않는다."""
    target = tmp_path / "standard_report.json"
    with pytest.raises(ValueError):
        std.publish_atomic({"macro_f1": math.nan}, str(target))
    assert not target.exists()
    assert list(tmp_path.glob("*.tmp*")) == []


def test_publish_atomic_replaces_previous_version_wholesale(tmp_path):
    target = tmp_path / "standard_report.json"
    std.publish_atomic({"status": "ok", "n": 1}, str(target))
    std.publish_atomic({"status": "refused", "n": 2}, str(target))
    assert json.loads(target.read_text())["status"] == "refused"


def test_stage_status_marks_failed_stage_as_error_not_ok():
    R = {"S0": {"n": 1}, "S3": {"error": "KeyError: 'gt'"}}
    st = std.stage_status(R)
    assert st["S0"] == "ok"
    assert st["S3"] == "error"


def test_report_with_any_failed_stage_is_not_displayable():
    """스테이지 하나가 죽었는데 표를 그리면 그게 조용한 오답이다."""
    R = {"S0": {"n": 1}, "S6": {"error": "boom"}}
    assert std.finalize_status(R)["display_allowed"] is False


def test_report_with_all_stages_ok_is_displayable():
    R = {"S0": {"n": 1}, "S6": {"top_set": ["v1"]}}
    out = std.finalize_status(R)
    assert out["display_allowed"] is True
    assert out["status"] == "ok"
    assert out["complete"] is True
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/unit/test_standard_artifact.py -q`
Expected: FAIL — `AttributeError: module has no attribute 'publish_atomic'`

- [ ] **Step 3: 최소 구현 — `analysis_standard.py` 에 추가**

`run()` 정의 **위**에 넣는다:

```python
def publish_atomic(payload, path):
    """부분 JSON 을 절대 노출하지 않는 발행.

    직렬화 → flush → fsync → os.replace 순서다. 중간에 터지면 임시 파일까지 지우고
    **이전 버전을 그대로 둔다** — 반쯤 쓰인 report 를 소비자가 읽는 것이 최악이다.
    `allow_nan=False` 인 이유: NaN 은 JSON 표준이 아니라 소비자 파서마다 다르게 깨진다.
    """
    tmp = f"{path}.tmp{os.getpid()}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1, allow_nan=False, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def stage_status(R):
    """스테이지별 `ok|error`. `run()` 이 예외를 기록만 하고 계속하므로 여기서 드러낸다."""
    return {sid: ("error" if isinstance(v, dict) and "error" in v else "ok")
            for sid, v in R.items()
            if sid.startswith("S") and len(sid) == 2 and sid[1].isdigit()}


def finalize_status(R):
    """report 전체 판정. **스테이지가 하나라도 죽으면 표를 그리지 않는다.**"""
    st = stage_status(R)
    failed = sorted(k for k, v in st.items() if v == "error")
    return dict(stages=st, failed_stages=failed,
                status="error" if failed else "ok",
                display_allowed=not failed, complete=True)
```

`run()` 안의 마지막 `json.dump(...)` 호출을 다음으로 교체한다:

```python
    R.update(finalize_status(R))
    publish_atomic(R, f"{outdir}/standard_report.json")
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/unit/test_standard_artifact.py -q`
Expected: 5 passed

- [ ] **Step 5: 기존 동작 회귀 확인 — 편입된 파일이므로 전체 유닛 스위트를 돌린다**

Run: `pytest tests/unit -q`
Expected: 기존 통과 수 + 5 (2026-09-15 기준 987 passed 였으므로 992 근처). **실패가 생기면 멈추고 원인을 밝힌다.**

- [ ] **Step 6: allowlist + 추적 확인 + ruff + 커밋**

```
!tests/unit/test_standard_artifact.py
```
```bash
git add .gitignore docker/analysis/analysis_standard.py tests/unit/test_standard_artifact.py
git ls-files tests/unit/test_standard_artifact.py
ruff check --line-length 120 docker/analysis/analysis_standard.py tests/unit/test_standard_artifact.py
git commit -m "feat(analysis): 아티팩트 발행 계약 — 부분 JSON 과 죽은 스테이지를 정상으로 읽지 않게

run() 은 스테이지 예외를 기록만 하고 정상 report 로 계속했고, json.dump 직접 쓰기라
읽는 쪽이 반쯤 쓰인 파일을 볼 수 있었다. 둘 다 예외 없이 틀린 표를 만드는 경로다.

publish_atomic 은 allow_nan=False → fsync → os.replace 순서로 쓰고 실패 시 이전 버전을
보존한다. 스테이지가 하나라도 죽으면 display_allowed=False 로 소비자가 거부한다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `load_cohort()` 승격 — stale `preds.npz` 의존 제거

**Files:**
- Modify: `docker/analysis/cohort.py` (`load_cohort` 추가)
- Modify: `docker/analysis/analysis_standard.py` (`main()` 의 sourcei 하드 게이트 제거)

**Interfaces:**
- Consumes: `cohort.resolve` (Task 2), `prompt_data_contract.build_gidx_class_map` (Task 1)
- Produces: `load_cohort(name, stages=("S0",)) -> dict` — `analysis_standard.run()` 의 `D` 계약

- [x] **Step 1: 현재 sourcei 산출을 기준선으로 저장 (회귀 비교용)** — **기준선 없음으로 판명**

```bash
docker exec docker-analysis-1 python3 /workspace/analysis_standard.py run \
  --dataset sourcei --banks v1.0.8.0,v1.0.8.1 \
  --out /data/fiftyone/frames_bank/report/sourcei_gt/standard_baseline
```

**실행 결과 (2026-09-16):**
```
File "/workspace/analysis_standard.py", line 425, in load_sourcei
    assert hid == list(d["ids"])
AssertionError
```

표준 러너는 **낡은 데이터를 읽는 수준이 아니라 아예 돌지 않는 상태**였다. 라이브 sourcei
6,032 와 `preds.npz` 7,498 이 어긋나 `assert` 에서 즉사한다. 비교할 기준선이 존재하지
않으므로 이 스텝은 **무효**이고, 그 사실 자체가 Task 4 의 정당성이다.

- [ ] **Step 2: `load_cohort` 구현 — `cohort.py` 에 추가**

```python
def load_cohort(name, stages=("S0",)):
    """라이브 FiftyOne 데이터셋에서 `analysis_standard.run()` 의 `D` 를 만든다.

    ⚠️ `preds.npz` 를 읽지 않는다. 그 파일은 7,498장 **구코호트**이고 라이브 `sourcei` 는
    6,032장이다 — 표준 러너가 낡은 스냅샷 위에서 돌던 것이 이 승격의 이유다.

    `stages` 로 **선택 적재**한다. 단일 비대 D 를 만들면 S6 만 필요한 호출이 S1~S5 의
    입력(문장 벡터 등)까지 읽어 느려지고 예외 시점도 흐려진다.
    """
    import fiftyone as fo
    import numpy as np

    cfg = resolve(name)                      # 미등록이면 여기서 CohortRefused
    ds = fo.load_dataset(name)
    sch = ds.get_field_schema()
    if cfg["group"] not in sch:
        raise CohortRefused(name, "G0_GROUP_FIELD_MISSING",
                            f"레지스트리가 지정한 군집키 '{cfg['group']}' 가 데이터셋에 없다")

    classes = [cfg["negative_class"]] + list(cfg["target_classes"])
    gt_lab, grp = ds.values([f"{cfg['gt_field']}.label", cfg["group"]])
    gt = np.array([classes.index(x) if x in classes else -1 for x in gt_lab])
    if (gt < 0).any():
        raise CohortRefused(name, "G0_UNKNOWN_GT_CLASS",
                            f"레지스트리에 없는 GT 클래스 {sorted(set(gt_lab) - set(classes))}")

    return dict(name=name, gt=gt, group=np.array(grp), classes=classes,
                events=list(cfg["target_classes"]), cohort_cfg=cfg,
                requested_stages=list(stages))
```

- [ ] **Step 3: `analysis_standard.main()` 의 하드 게이트 제거**

다음 두 줄을 삭제한다:

```python
    if a.dataset != "sourcei":
        raise SystemExit(f"어댑터 없음: {a.dataset} — load_* 함수를 추가하세요 (load_sourcei 참고)")
```

`D = load_sourcei(banks)` 를 다음으로 교체한다:

```python
    import cohort
    try:
        D = cohort.load_cohort(a.dataset, stages=("S0", "S4"))
    except cohort.CohortRefused as e:
        out = a.out or f"/data/fiftyone/frames_bank/report/{a.dataset}"
        os.makedirs(out, exist_ok=True)
        publish_atomic(cohort.refusal_artifact(a.dataset, e.reason_code, e.detail),
                       f"{out}/standard_report.json")
        raise SystemExit(f"거부: {e}") from e
```

`--out` 기본값을 `None` 으로 바꾸고 코호트에서 유도한다:

```python
    r.add_argument("--out", default=None)
```

- [ ] **Step 4: 미등록 코호트가 거부 아티팩트를 남기는지 확인**

```bash
docker exec docker-analysis-1 python3 /workspace/analysis_standard.py run --dataset frames || true
docker exec docker-analysis-1 cat /data/fiftyone/frames_bank/report/frames/standard_report.json
```
Expected: `{"status": "refused", "reason_code": "G0_INSUFFICIENT_GT", "display_allowed": false, ...}`

- [ ] **Step 5: 새 코호트가 스크립트 추가 없이 도는지 확인 — 이게 표준화의 완료 기준이다**

```bash
docker exec docker-analysis-1 python3 /workspace/analysis_standard.py run --dataset sitej_subway
```
Expected: 완주. `standard_report.json` 의 `S0` 에 `n=2728`, 군집키 `session`.

- [ ] **Step 6: sourcei 가 라이브 기준으로 갱신됐는지 확인**

```bash
docker exec docker-analysis-1 python3 /workspace/analysis_standard.py run --dataset sourcei
docker exec docker-analysis-1 python3 -c "
import json; r=json.load(open('/data/fiftyone/frames_bank/report/sourcei/standard_report.json'))
print('N', r['S0'].get('n'), 'deff', r['S4'].get('deff'), 'n_eff', r['S4'].get('n_effective'))"
```
Expected: `N 6032` (구 7,498 아님). Step 1 기준선과 **달라야** 정상이며, 차이를 커밋 메시지에 적는다.

- [ ] **Step 7: 전체 유닛 스위트 + ruff + 커밋**

```bash
pytest tests/unit -q
ruff check --line-length 120 docker/analysis/cohort.py docker/analysis/analysis_standard.py
git add docker/analysis/cohort.py docker/analysis/analysis_standard.py
git commit -m "feat(analysis): load_cohort 승격 — 표준 러너가 라이브 데이터 위에서 돈다

load_sourcei 는 preds.npz(7,498장 구코호트)를 읽었고 라이브 sourcei 는 6,032장이다.
표준이라 불리는 것이 낡은 스냅샷 위에서 돌고 있었다.

main() 의 'if dataset != sourcei: SystemExit' 하드 게이트를 제거해 sitej_subway 가
스크립트 추가 없이 완주한다. 거부는 예외로 끝내지 않고 refused 아티팩트로 발행해
이전 성공본을 덮는다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## 선행 조사 — ICC 추정량 ✅ 완료

전문: [`2026-09-16-icc-estimator-audit.md`](../specs/2026-09-16-icc-estimator-audit.md)

**결론**: G3 의 evidence(`ICC 0.827`)는 **7,498장 구코호트** 값이고 라이브(6,032)에서는
재현되지 않는다. 원인은 추정량 차이가 아니라 **코호트 교체**다 — 같은 추정량을 구코호트에
돌리면 0.508/0.730 이 나온다.

| | 라이브 (6,032) | 구코호트 (7,498) |
|---|---|---|
| `design_effect` ICC 중앙 | 0.367 (0.260~0.482) | 0.508 (0.338~0.868) |
| ICC > 0.5 (G3 발동) | **0 / 31** | 30 / 35 |
| deff 중앙 | 148.2 (105~194) | 254.7 |
| 혼합효과 ICC | 0.368 (카메라 6/15) | 0.730 (10/15) |

**라이브에서 발동하는 것은 G1(deff 105~194 → 유효표본 31~57) 과 G2(fire 4/15·smoke 4/15) 뿐이다.**

→ S6-lite 는 **유지**하되 근거를 G3 에서 **G1** 으로 교체한다. 직접 근거(상위 8버전 인접
델타 CI 가 전부 0 포함)는 변함없다. 명목 6,032장이 통계적으로 40장 안팎이라 고유 24개 버전의
순위는 어느 추정량으로도 성립하지 않는다.

→ 파생 결함 **D1·D2·D3** 는 설계서 §5 Phase 2 작업으로 편입했다.

## Phase 2·3 은 왜 지금 상세화하지 않나

설계서 §5 가 **"앞 Phase 가 실사용 1회를 통과하기 전에 다음으로 넘어가지 않는다"** 를 원칙으로
두고 있다. Phase 2(S6-lite 통계)와 Phase 3(패널 배선)은 `D` 계약과 아티팩트 계약이 실제
코호트 2개에서 검증된 뒤에 써야 한다 — Task 4 Step 5·6 이 그 검증이다. 계약이 흔들리면
지금 쓴 Phase 2 스텝은 그대로 버릴 작업이 된다.

Phase 2 에 들어갈 통계 계약 자체는 **이미 확정돼 있다** (설계서 §4.4 1~10항). 상세 스텝만
Task 4 완료 후에 쓴다.

---

---

## 실행 결과 (2026-09-16 · 브랜치 `feature/analysis-standardization`)

**Task 0~4 전부 완료.** 커밋 5개:

| 커밋 | 내용 |
|---|---|
| `4f0c218` | 엔진 2종 git 편입 (tracked py 133 → 135) |
| `dd6d27d` | `prompt_data_contract.py` + 테스트 14건 |
| `9e2b26e` | `cohort.py` 레지스트리 + 테스트 8건 |
| `6650d8c` | 원자적 발행 + stage 상태 + 테스트 7건 |
| `d689382` | `load_cohort()` 승격 + 하드 게이트 제거 |

**검증 실측:**

| 확인 | 결과 |
|---|---|
| `sitej_subway` 가 스크립트 추가 없이 완주 | ✅ `group_field=session`, 군집 30 (카메라 58 아님) |
| sourcei 라이브 완주 | ✅ 6,032장, G2 발동(fire 4/15 · smoke 4/15) |
| 미등록 코호트 거부 아티팩트 | ✅ `frames` → `display_allowed=false` |
| `prompt_data_contract` 라이브 검증 | ✅ unmapped 0 · 고유 예측 24/31 · 오귀속 의심 2건 |
| `tests/unit` | ✅ **1027 passed / 32 skipped** (신규 29건 전부 allowlist 편입 확인) |

**계획 대비 이탈 3건:**

1. **Task 0 Step 5 (ruff 게이트)** — 전제가 틀렸다. CI ruff 는 `src/ tests/` 만 검사하므로
   `docker/analysis/` 편입은 lint 에 영향이 없다. 엔진 2개의 기존 위반 103건은 그대로 뒀다
   (로직 무관 대량 diff 회피 + `sourcei_stat_ab.py` 불변 제약 준수).
2. **Task 4 Step 1 (기준선)** — 위 참조. 구 경로가 `AssertionError` 로 실행 불가라 무효.
3. **계획에 없던 수정 3건** — 실행 중 드러난 것들:
   - 카드 작성부가 `skipped`/`error` dict 를 결과 행으로 순회해 `TypeError` (실측 후 수정)
   - `s0_inventory` 의 `"카메라"` 하드코딩 → 군집키 이름을 `D` 에서 읽음
   - `s0_inventory` 의 G2 가 `!= "normal"` 로 음성 클래스를 하드코딩 → `D["events"]` 사용

**다음**: Phase 2(S6-lite) — 설계서 §4.4 통계 계약 1~10항 + ICC 감사 D1~D3.
`--stages` 기본값이 현재 `S0` 인 이유는 S1~S5 가 점수 행렬을 요구하기 때문이며,
Phase 2 가 `version_preds` 를 실으면서 해소된다.

---

## Self-Review

**1. 설계서 커버리지**
- §4.1 레지스트리 → Task 2 ✅ / §4.2 `load_cohort` 승격 → Task 4 ✅
- §4.3 지문·원자 발행 → Task 3 ✅ (지문 **전체 항목**은 Phase 2 에서 S6 산출과 함께 채운다 — Task 3 은 `status`/`complete`/`display_allowed` 골격만)
- §4.4 통계 계약 → Phase 2 (의도적 보류)
- §4.5 패널 읽기전용 → Phase 3 (의도적 보류)
- §9 `prompt_data_contract.py` → Task 1 ✅ / 엔진 편입 → Task 0 ✅

**2. 플레이스홀더 스캔** — 없음. 모든 코드 스텝에 실제 코드가 있다.

**3. 타입 일관성** — `CohortRefused.reason_code` 는 Task 2 정의 → Task 4 에서 동일 이름 사용 ✅.
`publish_atomic(payload, path)` Task 3 정의 → Task 4 에서 동일 시그니처 사용 ✅.
`refusal_artifact(name, reason_code, detail)` Task 2 정의 → Task 4 에서 동일 인자 순서 ✅.
