# 시각화·큐레이션 플랫폼 Phase 0+1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** FiftyOne 위에 (1) 고시인성 고정 색상, (2) topk/wave 불일치 워크스페이스, (3) `user-prompt-compare` Panel(모드 A: sourcei 프레임↔sourcei-prompts 문장 연동 비교, 모드 B: 프로젝트별 embedding 비교)을 얹는다.

**Architecture:** 모든 코드는 `docker/analysis/`(git 정본)에 있고 `docker cp`로 `docker-analysis-1` 컨테이너에 배포한다(App 재시작 불필요, prod dagster 미트리거). Panel은 서버사이드에서 두 번째 데이터셋을 `fo.load_dataset()`으로 읽어 Plotly `scattergl`로 그린다 — 조인·계산은 순수 함수로 분리해 selftest로 검증한다.

**Tech Stack:** FiftyOne 1.19.0 (OSS), `fiftyone.operators.Panel` + `PlotlyView`, numpy, MongoDB 사이드카(읽기만), Python 3.10+.

**Spec:** `docs/superpowers/specs/2026-08-07-viz-curation-platform-design.md` (H1 확정, Phase 2 정지 규칙 승인됨)

## Global Constraints

- brain key는 `"emb_viz"` 하드코딩 — 다른 키는 App Color by까지 죽인다 (스펙 §5.5)
- `MAX_POINTS = 20_000`, 초과 시 층화 서브샘플 + 경고 (스펙 §5.5)
- `embedding`(1024-d) 필드 절대 미로드 — 좌표 N×2 + 스칼라 메타만 (스펙 §5.5)
- 콜백 안 UMAP/t-SNE fit 금지 — 사전계산 좌표만 (스펙 §5.5)
- Plotly `scattergl` 강제 (`scatter` 금지) (스펙 §5.5)
- 프로세스 캐시: 데이터셋당 1엔트리, 합계 상한 64MB (스펙 §5.5)
- 조인 필드 방향: `sourcei`(프레임).`winner_gidx_v080` ↔ `sourcei-prompts`(문장).`gidx` — 뒤집기 금지 (스펙 §3)
- 규칙 명칭 3벌 고정: `argmax_k1` / `topk_vote` / `dist_iou` — "topk" 단독 표기 금지 (스펙 §3)
- 배너 문구는 Task 7의 상수 문자열 그대로 — 임의 수정 금지 (스펙 §5.4)
- `src/vlm_pipeline/` 수정 금지 (prod 재빌드 = 라벨링 중단) (스펙 §1.1)
- 테스트는 pytest가 아니라 **selftest 패턴** (`python __init__.py` 실행 시 assert) — `user-prompt-probe`의 `_self_check()` 전례. repo `tests/`는 gitignore allowlist 함정이 있어 쓰지 않는다
- 커밋은 conventional commits, 본문에 "무엇과 왜". 각 커밋 끝: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- 실행 환경: 스크립트는 전부 `docker exec -i docker-analysis-1 python ...`로 컨테이너 안에서 실행 (호스트에는 fiftyone 없음)

---

### Task 1: fiftyone 버전 핀 (리스크 R8)

**Files:**
- Modify: `docker/analysis/requirements.txt`

**Interfaces:**
- Consumes: 없음
- Produces: 없음 (환경 안정화 — 이후 이미지 재빌드가 일어나도 1.19.0 유지)

- [ ] **Step 1: 현재 상태 확인**

Run: `grep -n "^fiftyone" docker/analysis/requirements.txt`
Expected: `fiftyone` (무핀)

- [ ] **Step 2: 핀 적용**

`docker/analysis/requirements.txt`에서 `fiftyone` 줄을 다음으로 교체:

```
fiftyone==1.19.0
```

- [ ] **Step 3: 검증**

Run: `grep -n "^fiftyone==" docker/analysis/requirements.txt && docker exec docker-analysis-1 python -c "import fiftyone; assert fiftyone.__version__=='1.19.0', fiftyone.__version__; print('OK 1.19.0')"`
Expected: `fiftyone==1.19.0` + `OK 1.19.0` (설치본과 핀 일치 확인)

- [ ] **Step 4: Commit**

```bash
git add docker/analysis/requirements.txt
git commit -m "chore(analysis): fiftyone 1.19.0 핀 — 무핀 재빌드가 플러그인 3종+번들패치+brain_key 가정을 동시에 깨는 리스크(R8) 차단

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: 고정 색상 스킴 스크립트 (R3)

**Files:**
- Create: `docker/analysis/fiftyone_app_setup.py`

**Interfaces:**
- Consumes: 없음
- Produces: CLI `python fiftyone_app_setup.py colors [--datasets A,B,...]`, 상수 `CLASS_COLORS: dict[str,str]`, `OKABE_ITO: list[str]`, 함수 `apply_colors(ds) -> int`(적용한 field entry 수 반환). Task 3이 같은 파일에 `workspace` 서브커맨드를 추가한다.

- [ ] **Step 1: selftest부터 작성 (파일 하단에 먼저)**

`docker/analysis/fiftyone_app_setup.py` 생성 — 아래 뼈대에서 selftest만 먼저 완성:

```python
"""FiftyOne App 설정 정본화 — 색상 스킴(R3) + 워크스페이스.

정본: docker/analysis/fiftyone_app_setup.py (git). 컨테이너 실행:
  docker cp docker/analysis/fiftyone_app_setup.py docker-analysis-1:/workspace/
  docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py colors
설계 근거: docs/superpowers/specs/2026-08-07-viz-curation-platform-design.md §4 0-1
"""
import sys

# Okabe-Ito 색맹 안전 팔레트 (8색) + 회색
OKABE_ITO = ["#0072B2", "#E69F00", "#009E73", "#D55E00",
             "#CC79A7", "#56B4E9", "#F0E442", "#000000"]

# 클래스 → 고정색. 전 데이터셋·전 워크스페이스 동일 (스펙 §4 0-1).
CLASS_COLORS = {
    "fire":     "#D55E00",  # vermillion
    "smoke":    "#7F7F7F",  # grey
    "falldown": "#E69F00",  # orange
    "normal":   "#0072B2",  # blue
    "smoking":  "#CC79A7",
    "person":   "#009E73",
    "unknown":  "#BBBBBB",
    "none":     "#BBBBBB",
}

DEFAULT_DATASETS = ["sourcei", "sourcei-prompts", "source-h", "source-h-prompts"]
# 클래스 값을 담는 필드 후보 — 데이터셋에 존재하는 것만 적용
CLASS_FIELD_CANDIDATES = ["ground_truth", "category", "event_kind",
                          "pred_v1_0_8_0", "wave_pred_v1_0_8_0", "attached_bank"]


def _selftest():
    # 팔레트 위생: 중복 없음 + 유효 hex
    assert len(set(OKABE_ITO)) == len(OKABE_ITO)
    for c in list(OKABE_ITO) + list(CLASS_COLORS.values()):
        assert c.startswith("#") and len(c) == 7, c
    # 4클래스 핵심 색이 서로 다름 (fire/smoke/falldown/normal)
    core = [CLASS_COLORS[k] for k in ("fire", "smoke", "falldown", "normal")]
    assert len(set(core)) == 4
    print("selftest OK")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        _selftest()
    else:
        raise SystemExit("usage: fiftyone_app_setup.py selftest|colors|workspace")
```

- [ ] **Step 2: selftest 실행 (아직 colors 없음 → usage 에러도 확인)**

Run: `python3 docker/analysis/fiftyone_app_setup.py selftest && python3 docker/analysis/fiftyone_app_setup.py colors; echo "exit=$?"`
Expected: `selftest OK` 그리고 colors는 `usage:` SystemExit (exit=1) — 구현 전 실패 확인

- [ ] **Step 3: apply_colors 구현**

`_selftest` 위에 추가:

```python
def _field_entry(ds, path):
    """필드 타입에 맞는 ColorScheme fields 엔트리. Classification이면 .label 기준."""
    import fiftyone as fo
    field = ds.get_field(path)
    if field is None:
        return None
    value_colors = [{"value": v, "color": c} for v, c in CLASS_COLORS.items()]
    entry = {"path": path, "valueColors": value_colors}
    if isinstance(field, fo.EmbeddedDocumentField):  # Classification 계열
        entry["colorByAttribute"] = "label"
    return entry


def apply_colors(ds):
    """데이터셋에 고정 색상 스킴 적용. 적용된 field entry 수 반환."""
    import fiftyone as fo
    entries = [e for e in (_field_entry(ds, p) for p in CLASS_FIELD_CANDIDATES) if e]
    ds.app_config.color_scheme = fo.ColorScheme(
        color_by="value", color_pool=OKABE_ITO, opacity=0.9, fields=entries,
    )
    # active_fields allowlist 함정: 색칠 대상이 목록 밖이면 App이 죽는다 (스펙 §4 0-1)
    af = ds.app_config.active_fields
    if af is not None and getattr(af, "paths", None) is not None:
        for e in entries:
            if e["path"] not in af.paths:
                af.paths.append(e["path"])
    ds.save()
    return len(entries)


def cmd_colors(dataset_names):
    import fiftyone as fo
    for name in dataset_names:
        if not fo.dataset_exists(name):
            print(f"skip (없음): {name}")
            continue
        ds = fo.load_dataset(name)
        n = apply_colors(ds)
        assert ds.app_config.color_scheme is not None
        print(f"{name}: color_scheme 적용, field entries={n}")
```

`__main__`에 분기 추가:

```python
    elif sys.argv[1] == "colors":
        names = sys.argv[2].split(",") if len(sys.argv) > 2 else DEFAULT_DATASETS
        cmd_colors(names)
```

- [ ] **Step 4: 컨테이너에서 실행·검증**

```bash
docker cp docker/analysis/fiftyone_app_setup.py docker-analysis-1:/workspace/
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py selftest
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py colors
docker exec -i docker-analysis-1 python -c "
import fiftyone as fo
cs = fo.load_dataset('sourcei').app_config.color_scheme
assert cs is not None and cs.color_pool[0] == '#0072B2'
print('verify OK:', len(cs.fields), 'field entries')"
```
Expected: 4개 데이터셋 각각 `color_scheme 적용` + `verify OK`

- [ ] **Step 5: 브라우저 확인 (수동)**

`http://10.0.0.10:5153/datasets/sourcei` 열고 Color by=`ground_truth` — fire가 주황빨강(#D55E00), normal이 파랑(#0072B2)으로 나오는지, App 색상 설정 패널에서 값 편집이 되는지 확인. **편집이 dataset 기본으로 저장 안 되면**: README에 "커스텀은 `CLASS_COLORS` 수정 후 colors 재실행" 한 줄 추가 (Task 10에서).

- [ ] **Step 6: Commit**

```bash
git add docker/analysis/fiftyone_app_setup.py
git commit -m "feat(analysis): 고정 색상 스킴(R3) — Okabe-Ito + 클래스 고정색, 4개 데이터셋

color_scheme=None(랜덤 기본)이 시인성 불만의 직접 원인이었다. 클래스 색을
전 데이터셋·전 워크스페이스에서 동일하게 고정 — 색이 화면마다 바뀌면 비교가
무의미해진다. active_fields allowlist 함정도 함께 처리.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: rule_cross 불일치 워크스페이스 (R4 절반, 개발 0에 준함)

**Files:**
- Modify: `docker/analysis/fiftyone_app_setup.py` (workspace 서브커맨드 추가)

**Interfaces:**
- Consumes: Task 2의 파일 구조 (`__main__` 분기, selftest)
- Produces: CLI `python fiftyone_app_setup.py workspace` → `sourcei`에 워크스페이스 `rules` 저장

- [ ] **Step 1: 구현**

`cmd_colors` 아래 추가:

```python
def cmd_workspace():
    """sourcei에 'rules' 워크스페이스: Samples | Embeddings(emb_viz, Color by=rule_cross).

    rule_cross = argmax_k1/dist_iou 두 규칙이 갈리는 프레임 표식 (이미 존재하는 필드).
    """
    import fiftyone as fo
    ds = fo.load_dataset("sourcei")
    assert "rule_cross" in ds.get_field_schema(), "rule_cross 필드가 없다 — 스펙 §3 확인"
    space = fo.Space(
        children=[
            fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
            fo.Space(children=[fo.Panel(
                type="Embeddings",
                state=dict(brainResult="emb_viz", colorByField="rule_cross"),
            )]),
        ],
        orientation="horizontal",
    )
    ds.save_workspace("rules", space,
                      description="argmax_k1 vs dist_iou 불일치 프레임", overwrite=True)
    assert "rules" in ds.list_workspaces()
    print("workspace 'rules' 저장 완료")
```

`__main__`에 분기 추가:

```python
    elif sys.argv[1] == "workspace":
        cmd_workspace()
```

- [ ] **Step 2: 실행·검증**

```bash
docker cp docker/analysis/fiftyone_app_setup.py docker-analysis-1:/workspace/
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py workspace
```
Expected: `workspace 'rules' 저장 완료`

- [ ] **Step 3: 브라우저 확인 (수동)**

`:5153/datasets/sourcei` → 우상단 `⊞` 워크스페이스 셀렉터(F5 후 목록 갱신) → `rules` 선택.
Embeddings 패널 Color by가 `rule_cross`로 안 잡혀 있으면(state 키 `colorByField` 미지원 가능):
App에서 Color by를 `rule_cross`로 수동 선택 → 워크스페이스 `rules`로 **재저장** (덮어쓰기).
이 fallback을 썼는지 여부를 커밋 메시지에 남긴다.

- [ ] **Step 4: Commit**

```bash
git add docker/analysis/fiftyone_app_setup.py
git commit -m "feat(analysis): rules 워크스페이스 — 두 판정규칙이 갈리는 프레임을 한 화면에(R4 절반)

신규 필드 0개. 기존 rule_cross 필드에 Color by를 고정한 워크스페이스 저장만으로
argmax_k1/dist_iou 불일치 프레임 검토 화면이 생긴다.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: wave_gain/wave_role 프레임 필드 산출 경로 확인 (스펙 §4 0-4)

**Files:**
- Modify: `docker/analysis/README.md` (sourcei 절에 결론 1~2줄)

**Interfaces:**
- Consumes: 없음
- Produces: 판정 "프레임 쪽 `wave_gain`/`wave_role`은 문장 값의 복사본인가?" — Task 7이 이 판정에 따라 Panel의 읽기 소스를 결정한다 (복사본이면 prompts 쪽에서만 읽음)

- [ ] **Step 1: 산출 경로 추적**

Run: `grep -n "wave_gain\|wave_role" docker/analysis/prompt_geometry.py | head -30`
그리고 각 `set_values` 호출이 어느 데이터셋 객체(`ds`=프레임 vs 문장 샘플)에 걸리는지 해당 라인 ±20줄을 읽고 판정한다.

- [ ] **Step 2: 라이브 대조 (판정 검증)**

```bash
docker exec -i docker-analysis-1 python - <<'EOF'
import fiftyone as fo
f = fo.load_dataset("sourcei"); p = fo.load_dataset("sourcei-prompts")
schema_f = set(f.get_field_schema()); schema_p = set(p.get_field_schema())
print("frame has wave_gain:", "wave_gain" in schema_f, "| prompt has:", "wave_gain" in schema_p)
if "wave_gain" in schema_f:
    s = f.match(fo.ViewField("wave_gain") != None).first()
    print("sample frame wave_gain:", s["wave_gain"] if s else "전부 None")
EOF
```
Expected: 존재 여부와 실값 — grep 판정과 일치해야 한다

- [ ] **Step 3: README에 결론 기록**

`docker/analysis/README.md`의 `## source-i 실내 데이터셋` 절 끝에 결론을 2줄 이내로 추가.
예 (실측 결과에 맞게 수정): `- 프레임 필드 wave_gain/wave_role 은 승자 문장 값의 복사본(prompt_geometry.py:<라인>) — 문장 단위 양이므로 분석은 sourcei-prompts 쪽 필드를 정본으로 읽을 것.`

- [ ] **Step 4: Commit**

```bash
git add docker/analysis/README.md
git commit -m "docs(analysis): sourcei 프레임의 wave_gain/wave_role 산출 경로 판정 기록

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Panel 스파이크 — go/no-go 게이트 (스펙 §4 0-3)

**Files:**
- Create: `docker/analysis/plugins/user-prompt-compare/fiftyone.yml`
- Create: `docker/analysis/plugins/user-prompt-compare/__init__.py` (스파이크 버전 — Task 6+가 확장)

**Interfaces:**
- Consumes: 없음
- Produces: (1) go/no-go 판정 — **no-go면 Task 6~10을 중단**하고 스펙 §6(Phase 2)으로 우회 (2) 확정된 이벤트 훅 이름(`on_change_selected` / `on_change_extended_selection` 중 실제 발화하는 것) — Task 8이 사용 (3) `PromptComparePanel` 클래스명과 패널 name `"user_prompt_compare"` — 이후 태스크 공유

- [ ] **Step 1: fiftyone.yml 작성**

```yaml
name: "@user/prompt-compare"
description: "Cross-dataset prompt/frame embeddings comparison (spec 2026-08-07)"
version: "0.1.0"
fiftyone:
  version: ">=1.19,<1.20"
operators:
  - user_prompt_compare
```

- [ ] **Step 2: 스파이크 Panel 작성**

`__init__.py`:

```python
"""user-prompt-compare 스파이크 — go/no-go 검증 3항목 (스펙 §4 0-3).

① 네이티브 Embeddings lasso가 어느 훅으로 도달하는가
② 콜백에서 fo.load_dataset(B) + scattergl 12,480점 렌더 성립
③ 하이라이트 patch 왕복 체감 지연
정본: docker/analysis/plugins/user-prompt-compare/ (git)
배포: docker cp → /data/fiftyone/datasets/__plugins__/user-prompt-compare/
"""
import time

import fiftyone as fo
import fiftyone.operators as foo
import fiftyone.operators.types as types

PROMPTS_DATASET = "sourcei-prompts"
BRAIN_KEY = "emb_viz"          # 하드코딩 — App이 다른 키에서 죽는 실측 함정
LOG = "/tmp/pcmp_spike.log"


def _log(msg):
    with open(LOG, "a") as f:
        f.write(f"{time.strftime('%H:%M:%S')} {msg}\n")


def _load_prompt_points():
    """문장 emb_viz 좌표만 로드. embedding(1024-d)은 절대 읽지 않는다."""
    ds = fo.load_dataset(PROMPTS_DATASET)
    results = ds.load_brain_results(BRAIN_KEY)
    pts = results.points            # (N, 2) ndarray
    _log(f"loaded points: {pts.shape}")
    return pts


class PromptComparePanel(foo.Panel):
    @property
    def config(self):
        return foo.PanelConfig(name="user_prompt_compare",
                               label="Prompt Compare (spike)", surfaces="grid")

    def on_load(self, ctx):
        t0 = time.time()
        pts = _load_prompt_points()
        ctx.panel.set_data("scatter", {
            "data": [{"type": "scattergl", "mode": "markers",
                      "x": pts[:, 0].tolist(), "y": pts[:, 1].tolist(),
                      "marker": {"size": 3}}],
            "layout": {"title": f"{PROMPTS_DATASET} ({len(pts)} pts)"},
        })
        _log(f"on_load done in {time.time()-t0:.2f}s")

    # ── 스파이크 핵심: 어떤 훅이 lasso에 반응하는지 전부 계측 ──
    def on_change_selected(self, ctx):
        _log(f"HOOK on_change_selected: {len(ctx.selected or [])} ids")

    def on_change_extended_selection(self, ctx):
        ext = getattr(ctx, "extended_selection", None)
        _log(f"HOOK on_change_extended_selection: {type(ext)}")

    def on_change_view(self, ctx):
        _log("HOOK on_change_view")

    def render(self, ctx):
        panel = types.Object()
        panel.plot("scatter")
        return types.Property(panel, view=types.GridView())


def register(p):
    p.register(PromptComparePanel)
```

- [ ] **Step 3: 배포 + RSS 사전 측정**

```bash
docker exec docker-analysis-1 sh -c \
  "ps -o rss= -p \$(pgrep -f fiftyone_prod_launch | head -1) | awk '{print \"RSS before: \" \$1/1024 \" MB\"}'"
docker exec docker-analysis-1 mkdir -p /data/fiftyone/datasets/__plugins__/user-prompt-compare
docker cp docker/analysis/plugins/user-prompt-compare/. \
  docker-analysis-1:/data/fiftyone/datasets/__plugins__/user-prompt-compare/
docker exec docker-analysis-1 fiftyone plugins list
```
Expected: `@user/prompt-compare` 목록에 등장

- [ ] **Step 4: 브라우저 검증 (수동, 판정 기록)**

`:5153/datasets/sourcei` → `+` 패널 추가 → "Prompt Compare (spike)":
1. 12,480점 산점도가 뜨는가 (②)
2. 네이티브 Embeddings 패널에서 lasso → `docker exec docker-analysis-1 cat /tmp/pcmp_spike.log` 에 `HOOK` 줄이 찍히는가, 어느 훅인가 (①)
3. 훅 발화 → 재렌더 왕복이 체감 1초 이내인가 (③)
4. RSS 사후 측정 (Step 3 명령 재실행) — 증가분 ≤ 100MB (스펙 §5.5 예산)

- [ ] **Step 5: 판정 커밋 (go/no-go 명시)**

```bash
git add docker/analysis/plugins/user-prompt-compare/
git commit -m "feat(analysis): user-prompt-compare 스파이크 — cross-dataset Panel go/no-go

판정: <go|no-go>. 발화 훅: <훅 이름>. 12,480점 렌더 <성공|실패>,
RSS 증가 <N>MB. no-go면 Phase 1 중단, 스펙 §6(Phase 2)으로 우회한다.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**⚠️ GATE: 판정이 no-go면 여기서 이 계획을 중단한다.** 이하 태스크는 go 전제.

---

### Task 6: 데이터 계층 + selftest 불변식 (스펙 §5.6)

**Files:**
- Modify: `docker/analysis/plugins/user-prompt-compare/__init__.py`

**Interfaces:**
- Consumes: Task 5의 파일 구조, `PROMPTS_DATASET`/`BRAIN_KEY` 상수
- Produces:
  - `FRAMES_DATASET = "sourcei"`, `VTAG = "v080"`, `WINNER_FIELD = "winner_gidx_v080"`, `MAX_POINTS = 20_000`, `CACHE_CAP_BYTES = 64 * 2**20`
  - `load_prompt_bundle() -> dict` — keys: `xy`(N×2 float32), `gidx`(N int64), `text`, `category`, `adopted`(bool 배열), `wins`, `purity`, `n_cameras`, `wave_gain`, `wave_role` (전부 길이 N 배열, 캐시됨)
  - `frame_ids_to_gidx(frame_ids: list[str]) -> list[int]` (프레임 선택 → 문장 gidx 집합)
  - `gidx_to_frame_ids(g: int) -> list[str]` (문장 → 그 문장이 이긴 프레임 id들)
  - `stratified_subsample(labels: list, max_points: int, seed: int = 0) -> list[int]` (인덱스)
  - `selftest()` — 조인 불변식 3개 assert

- [ ] **Step 1: selftest를 먼저 작성** (파일 끝에 추가)

```python
def selftest():
    """조인 불변식 3개 (스펙 §5.6) + 데이터 계층 검증. App 불필요.

    FiftyOne 업그레이드 게이트로도 쓴다. 셋째가 깨지면 producer drift 의심.
    """
    import numpy as np
    b = load_prompt_bundle()
    frames = fo.load_dataset(FRAMES_DATASET)

    # 불변식 1: 완전분할 — 승수 총합 = 프레임 수
    assert int(np.sum(b["wins"])) == frames.count(), \
        (int(np.sum(b["wins"])), frames.count())
    # 불변식 2: 프레임의 승자 gidx ⊆ 문장 gidx
    winner = set(frames.values(WINNER_FIELD))
    winner.discard(None)
    assert winner <= set(int(g) for g in b["gidx"])
    # 불변식 3: 채택 ⟺ wins>0
    assert all((w > 0) == bool(a) for w, a in zip(b["wins"], b["adopted"]))

    # 조인 왕복: 임의 채택 문장 → 프레임들 → 도로 그 문장
    g = int(b["gidx"][np.argmax(b["wins"])])
    ids = gidx_to_frame_ids(g)
    assert ids and set(frame_ids_to_gidx(ids)) == {g}

    # 층화 서브샘플: 상한 준수 + 전 클래스 보존
    labs = ["a"] * 100 + ["b"] * 10
    idx = stratified_subsample(labs, 20)
    assert len(idx) <= 20 and {labs[i] for i in idx} == {"a", "b"}
    print("selftest OK")


if __name__ == "__main__":
    selftest()
```

- [ ] **Step 2: 실패 확인**

Run: `docker cp docker/analysis/plugins/user-prompt-compare/__init__.py docker-analysis-1:/data/fiftyone/datasets/__plugins__/user-prompt-compare/ && docker exec docker-analysis-1 python /data/fiftyone/datasets/__plugins__/user-prompt-compare/__init__.py`
Expected: `NameError: load_prompt_bundle is not defined`

- [ ] **Step 3: 데이터 계층 구현** (`_load_prompt_points` 를 대체)

```python
FRAMES_DATASET = "sourcei"
VTAG = "v080"
WINNER_FIELD = f"winner_gidx_{VTAG}"
MAX_POINTS = 20_000
CACHE_CAP_BYTES = 64 * 2**20

_CACHE = {}  # (dataset, brain_key, last_modified_at) -> bundle. 엔트리 1개 유지.

META_FIELDS = ["gidx", "text", "category", "adopted", "wins", "purity",
               "n_cameras", "wave_gain", "wave_role"]


def _bundle_nbytes(b):
    import numpy as np
    return sum(v.nbytes for v in b.values() if isinstance(v, np.ndarray))


def load_prompt_bundle():
    """문장 좌표+메타 로드. embedding(1024-d)은 절대 읽지 않는다 (스펙 §5.5)."""
    import numpy as np
    ds = fo.load_dataset(PROMPTS_DATASET)
    key = (PROMPTS_DATASET, BRAIN_KEY, str(ds.last_modified_at))
    if key in _CACHE:
        return _CACHE[key]
    xy = np.asarray(ds.load_brain_results(BRAIN_KEY).points, dtype="float32")
    b = {"xy": xy}
    schema = ds.get_field_schema()
    for f in META_FIELDS:
        if f not in schema:
            b[f] = None
            continue
        vals = ds.values(f)
        # Classification 필드(category/adopted 등)는 .label로
        if vals and hasattr(vals[0], "label"):
            vals = [v.label if v else None for v in vals]
        b[f] = np.asarray(vals, dtype=object) if isinstance(vals[0], str) \
            else np.asarray([0 if v is None else v for v in vals])
    if b.get("adopted") is not None and b["adopted"].dtype == object:
        b["adopted"] = np.asarray([v in (True, "채택", "true") for v in b["adopted"]])
    assert _bundle_nbytes(b) <= CACHE_CAP_BYTES, "캐시 예산 64MB 초과 — 스펙 §5.5"
    _CACHE.clear()          # 엔트리 1개만 유지
    _CACHE[key] = b
    return b


def frame_ids_to_gidx(frame_ids):
    frames = fo.load_dataset(FRAMES_DATASET)
    vals = frames.select(frame_ids).values(WINNER_FIELD)
    return sorted({int(v) for v in vals if v is not None})


def gidx_to_frame_ids(g):
    frames = fo.load_dataset(FRAMES_DATASET)
    return frames.match(fo.ViewField(WINNER_FIELD) == int(g)).values("id")


def stratified_subsample(labels, max_points, seed=0):
    """클래스 비례 서브샘플, 클래스당 최소 1점 보장. 인덱스 리스트 반환."""
    import numpy as np
    labels = list(labels)
    if len(labels) <= max_points:
        return list(range(len(labels)))
    rng = np.random.default_rng(seed)
    by_class = {}
    for i, lab in enumerate(labels):
        by_class.setdefault(lab, []).append(i)
    out = []
    for lab, idxs in by_class.items():
        k = max(1, int(round(len(idxs) / len(labels) * max_points)))
        out.extend(rng.choice(idxs, size=min(k, len(idxs)), replace=False).tolist())
    return sorted(out[:max_points])
```

- [ ] **Step 4: selftest 통과 확인**

Run: Step 2와 동일 명령
Expected: `selftest OK`

- [ ] **Step 5: Commit**

```bash
git add docker/analysis/plugins/user-prompt-compare/__init__.py
git commit -m "feat(analysis): prompt-compare 데이터 계층 — 조인 불변식 3개 selftest 포함

sum(wins)==frames.count() 완전분할이 이 화면의 성립 조건이라 selftest가
데이터 회귀 감지기를 겸한다. embedding 1024-d 미로드, 캐시 1엔트리 64MB 상한.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: 모드 A figure 빌더 — 규칙 토글·배너·미채택 회색 (스펙 §5.3, §5.4)

**Files:**
- Modify: `docker/analysis/plugins/user-prompt-compare/__init__.py`

**Interfaces:**
- Consumes: Task 6의 `load_prompt_bundle()`, `stratified_subsample()`, Task 2의 `CLASS_COLORS`(값 복사 — 플러그인은 fiftyone_app_setup을 import하지 않는다, 배포 단위가 다름)
- Produces:
  - `BANNER_RULE`, `BANNER_COORDS_A`, `BANNER_WAVE_NOCLICK`, `RESERVE_TEXT` (상수 문자열 — UI 계약)
  - `build_mode_a(bundle, rule: str, show_unadopted: bool, selected_gidx: set[int]) -> dict` — Plotly figure dict (`{"data": [...], "layout": {...}}`). trace 순서: [0]=미채택(회색), [1]=채택, [2]=하이라이트 오버레이. `customdata=[gidx,...]` 로 클릭 역참조

- [ ] **Step 1: selftest에 검증 추가** (기존 `selftest()` 끝, `print` 앞에)

```python
    # 모드 A figure: 규칙별 계약
    fig = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx={g})
    assert all(t["type"] == "scattergl" for t in fig["data"])           # scattergl 강제
    n_shown = sum(len(t["x"]) for t in fig["data"][:2])
    assert n_shown == len(b["gidx"])                                     # 12,480 전체 표시
    assert BANNER_RULE in fig["layout"]["title"]["text"]                 # 규칙 배너
    fig_w = build_mode_a(b, rule="dist_iou", show_unadopted=True, selected_gidx=set())
    assert BANNER_WAVE_NOCLICK in fig_w["layout"]["title"]["text"]       # 귀속 없음 안내
    fig_h = build_mode_a(b, rule="argmax_k1", show_unadopted=False, selected_gidx=set())
    assert sum(len(t["x"]) for t in fig_h["data"][:2]) == int(b["adopted"].sum())
```

- [ ] **Step 2: 실패 확인**

Run: Task 6 Step 2와 동일 명령
Expected: `NameError: build_mode_a is not defined`

- [ ] **Step 3: 구현**

```python
# ── UI 계약 문자열 (스펙 §5.4 — 임의 수정 금지) ──
BANNER_RULE = ("이 조인은 K=1 전역 argmax(argmax_k1) 승자 기준 — "
               "제품 판정규칙(topk_vote K=10 다수결, dist_iou)과 다른 값")
BANNER_COORDS_A = "좌우 UMAP은 독립 fit — 좌표 공간 비교 금지, 연결은 선택 하이라이트로만"
BANNER_WAVE_NOCLICK = "dist_iou에는 프레임 귀속이 없습니다 — 기여도는 전역 LOO(wave_gain)"
RESERVE_TEXT = "가져간 프레임 0 — 예비군 (새 카메라 승자의 66%가 여기서 나온다)"

GREY = "#CCCCCC"
CLASS_COLORS = {  # Task 2와 동일 값 (배포 단위가 달라 복사 유지 — 변경 시 양쪽 동기화)
    "fire": "#D55E00", "smoke": "#7F7F7F", "falldown": "#E69F00",
    "normal": "#0072B2", "smoking": "#CC79A7",
}


def _hover(b, i):
    return (f"[{b['gidx'][i]}] {str(b['text'][i])[:80]}<br>"
            f"class={b['category'][i]} wins={b['wins'][i]} "
            f"purity={b['purity'][i]} wave_gain={b['wave_gain'][i]}")


def build_mode_a(bundle, rule, show_unadopted, selected_gidx):
    """문장 산점도 (모드 A). trace: [0]미채택 [1]채택 [2]하이라이트."""
    import numpy as np
    b = bundle
    n = len(b["gidx"])
    adopted = b["adopted"].astype(bool)
    idx_all = np.arange(n)
    if n > MAX_POINTS:
        idx_all = np.asarray(stratified_subsample(list(b["category"]), MAX_POINTS))
        adopted = adopted[idx_all]

    def trace(mask, color, size, name, opacity):
        ii = idx_all[mask]
        return {
            "type": "scattergl", "mode": "markers", "name": name,
            "x": b["xy"][ii, 0].tolist(), "y": b["xy"][ii, 1].tolist(),
            "customdata": [int(b["gidx"][i]) for i in ii],
            "text": [_hover(b, i) for i in ii], "hoverinfo": "text",
            "marker": {"color": color, "size": size, "opacity": opacity},
        }

    if rule == "argmax_k1":
        colors = [CLASS_COLORS.get(str(b["category"][i]), "#999999") for i in idx_all[adopted]]
        sizes = [4 + min(10, int(b["wins"][i]) // 50) for i in idx_all[adopted]]
        banner = f"{BANNER_RULE}<br><sup>{BANNER_COORDS_A}</sup>"
    else:  # dist_iou — 색=wave_role, 크기 균일, 클릭 무효
        colors = [CLASS_COLORS.get(str(b["wave_role"][i]), "#999999") for i in idx_all[adopted]] \
            if b.get("wave_role") is not None else "#999999"
        sizes = 5
        banner = f"{BANNER_WAVE_NOCLICK}<br><sup>{BANNER_COORDS_A}</sup>"

    data = []
    if show_unadopted:
        data.append(trace(~adopted, GREY, 3, f"미채택 {int((~adopted).sum())} (예비군)", 0.35))
    else:
        data.append({"type": "scattergl", "mode": "markers", "x": [], "y": [],
                     "customdata": [], "name": "미채택 (숨김)", "marker": {}})
    t_adopt = trace(adopted, "#999999", 5, f"채택 {int(adopted.sum())}", 0.9)
    t_adopt["marker"] = {"color": colors, "size": sizes, "opacity": 0.9}
    data.append(t_adopt)

    sel = [i for i in range(len(idx_all))
           if int(b["gidx"][idx_all[i]]) in (selected_gidx or set())]
    hi = idx_all[sel]
    data.append({"type": "scattergl", "mode": "markers", "name": "선택",
                 "x": b["xy"][hi, 0].tolist(), "y": b["xy"][hi, 1].tolist(),
                 "customdata": [int(b["gidx"][i]) for i in hi],
                 "marker": {"color": "#000000", "size": 12, "symbol": "circle-open",
                            "line": {"width": 3}}})
    return {"data": data,
            "layout": {"title": {"text": banner, "font": {"size": 12}},
                       "showlegend": True, "dragmode": "pan",
                       "xaxis": {"visible": False}, "yaxis": {"visible": False}}}
```

- [ ] **Step 4: selftest 통과 확인**

Run: Task 6 Step 2와 동일 명령
Expected: `selftest OK`

- [ ] **Step 5: Commit**

```bash
git add docker/analysis/plugins/user-prompt-compare/__init__.py
git commit -m "feat(analysis): 모드 A figure 빌더 — 규칙 토글이 색·크기·클릭 가능성을 바꾼다

12,480 전체 표시(미채택 회색+토글) — 숨기면 문장 공간 밀도를 오독한다.
배너 3종은 상수 문자열 계약: argmax_k1/dist_iou 는 다른 종류의 규칙이고
dist_iou 에는 프레임 귀속이 존재하지 않는다.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Panel 배선 모드 A — 선택 연동 (스펙 §5.2)

**Files:**
- Modify: `docker/analysis/plugins/user-prompt-compare/__init__.py` (스파이크 Panel 본문 교체)

**Interfaces:**
- Consumes: Task 6 데이터 계층, Task 7 `build_mode_a`, Task 5가 확정한 발화 훅 이름
- Produces: 동작하는 모드 A Panel — 상태 키 `rule`("argmax_k1"|"dist_iou"), `show_unadopted`(bool), `selected_gidx`(list[int]), `top_table`(list[dict])

- [ ] **Step 1: Panel 클래스 본문 교체**

```python
class PromptComparePanel(foo.Panel):
    @property
    def config(self):
        return foo.PanelConfig(name="user_prompt_compare",
                               label="Prompt Compare", surfaces="grid")

    def on_load(self, ctx):
        ctx.panel.state.rule = "argmax_k1"
        ctx.panel.state.show_unadopted = True
        ctx.panel.state.selected_gidx = []
        self._refresh(ctx)

    def _refresh(self, ctx):
        b = load_prompt_bundle()
        sel = set(ctx.panel.state.selected_gidx or [])
        fig = build_mode_a(b, rule=ctx.panel.state.rule,
                           show_unadopted=ctx.panel.state.show_unadopted,
                           selected_gidx=sel)
        ctx.panel.set_data("scatter", fig)
        # 선택 프레임들의 승자 문장 상위 표 (프레임→문장 방향, 스펙 §5.2)
        rows = []
        if sel:
            import numpy as np
            for g in list(sel)[:20]:
                i = int(np.where(b["gidx"] == g)[0][0])
                rows.append({"gidx": g, "text": str(b["text"][i])[:60],
                             "wins": int(b["wins"][i]), "purity": float(b["purity"][i]),
                             "n_cameras": int(b["n_cameras"][i]),
                             "wave_gain": float(b["wave_gain"][i])})
        ctx.panel.state.top_table = rows

    # ── 프레임 → 문장 (훅 이름은 Task 5 스파이크가 확정한 것으로 교체) ──
    def on_change_selected(self, ctx):
        ids = ctx.selected or []
        ctx.panel.state.selected_gidx = frame_ids_to_gidx(ids) if ids else []
        self._refresh(ctx)

    # ── 문장 → 프레임 ──
    def on_plot_click(self, ctx):
        g = (ctx.params or {}).get("data", {}).get("customdata")
        if g is None:
            return
        if ctx.panel.state.rule != "argmax_k1":
            return  # dist_iou 모드: 귀속 없음 — 클릭 무효 (배너가 안내)
        ids = gidx_to_frame_ids(int(g))
        if ids:
            ctx.ops.set_extended_selection(ids)   # 뷰 보존 하이라이트 (기본)
        ctx.panel.state.selected_gidx = [int(g)]
        self._refresh(ctx)

    def on_toggle_rule(self, ctx):
        s = ctx.panel.state
        s.rule = "dist_iou" if s.rule == "argmax_k1" else "argmax_k1"
        self._refresh(ctx)

    def on_toggle_unadopted(self, ctx):
        ctx.panel.state.show_unadopted = not ctx.panel.state.show_unadopted
        self._refresh(ctx)

    def render(self, ctx):
        panel = types.Object()
        panel.btn("toggle_rule", label=f"규칙: {ctx.panel.state.rule or 'argmax_k1'}",
                  on_click=self.on_toggle_rule)
        panel.btn("toggle_unadopted", label="채택만 보기 ⇄ 전체",
                  on_click=self.on_toggle_unadopted)
        panel.plot("scatter", on_click=self.on_plot_click)
        if ctx.panel.state.top_table:
            panel.md("table_md", label="선택 프레임의 승자 문장",
                     description=str(ctx.panel.state.top_table))
        return types.Property(panel, view=types.GridView())
```

주의: `panel.md(...)` 표가 조잡하면 `types.TableView`로 교체 시도 — 없으면 md 유지 (표는 부속물, 산점도가 본체).

- [ ] **Step 2: selftest + 배포**

```bash
docker cp docker/analysis/plugins/user-prompt-compare/__init__.py \
  docker-analysis-1:/data/fiftyone/datasets/__plugins__/user-prompt-compare/
docker exec docker-analysis-1 python /data/fiftyone/datasets/__plugins__/user-prompt-compare/__init__.py
```
Expected: `selftest OK` (Panel 클래스는 import만 되고 실행 안 됨 — 순수 함수만 검증)

- [ ] **Step 3: 브라우저 검증 (수동 — 스펙 §5.2 계약 4개)**

1. 문장 클릭(argmax_k1) → 좌측 그리드에 그 문장이 이긴 프레임 하이라이트
2. 미채택 문장 클릭 → 하이라이트 없음 + 표에 wins=0 (예비군)
3. 규칙 토글 → dist_iou 에서 클릭이 무효가 되고 배너가 바뀜
4. 네이티브 Embeddings lasso → 우측 산점도에 승자 문장 하이라이트 + 표 갱신

- [ ] **Step 4: Commit**

```bash
git add docker/analysis/plugins/user-prompt-compare/__init__.py
git commit -m "feat(analysis): 모드 A 배선 — 문장↔프레임 양방향 선택 연동

문장→프레임은 set_extended_selection(뷰 보존), 프레임→문장은 selection 훅
→ winner_gidx 매핑 → 하이라이트 patch. dist_iou 모드에서는 클릭이 의도적으로
무효 — 귀속이 없는 규칙에 귀속 UX를 주지 않는다.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: 모드 B — 프로젝트별 embedding 비교 (스펙 §5.1b, R5-b)

**Files:**
- Modify: `docker/analysis/plugins/user-prompt-compare/__init__.py`

**Interfaces:**
- Consumes: Task 6 `stratified_subsample`, Task 8 Panel 구조
- Produces: `build_mode_b(ds_name, group_field, groups: list[str], brain_key) -> dict` (Plotly figure — 그룹당 1 trace, 같은 좌표계 overlay), Panel 상태 키 `mode`("A"|"B"), `group_field`, `groups`

- [ ] **Step 1: selftest 추가** (기존 selftest 끝에)

```python
    # 모드 B: sourcei를 gt 클래스 2그룹으로 갈라 같은 좌표계 overlay (구조 검증용)
    figb = build_mode_b(FRAMES_DATASET, "ground_truth", ["normal", "falldown"], BRAIN_KEY)
    assert len(figb["data"]) == 2 and all(t["type"] == "scattergl" for t in figb["data"])
    assert "같은 좌표계" in figb["layout"]["title"]["text"]
```

- [ ] **Step 2: 실패 확인**

Run: Task 6 Step 2와 동일 명령 → Expected: `NameError: build_mode_b is not defined`

- [ ] **Step 3: 구현**

```python
BANNER_COORDS_B = "같은 UMAP 좌표계 — 그룹 간 공간 비교 유효 (모드 A와 다름)"


def build_mode_b(ds_name, group_field, groups, brain_key=BRAIN_KEY):
    """같은 데이터셋의 그룹 슬라이스들을 하나의 emb_viz 좌표 위에 overlay.

    frames_captions(project 22개)이 본래 타깃 — 그룹당 1 trace, 같은 fit 공유라
    좌표 직접 비교가 정당하다 (스펙 §5.1b). 그룹 필드는 문자열/Classification 모두 허용.
    """
    import numpy as np
    ds = fo.load_dataset(ds_name)
    xy = np.asarray(ds.load_brain_results(brain_key).points, dtype="float32")
    labels = ds.values(group_field)
    if labels and hasattr(labels[0], "label"):
        labels = [v.label if v else None for v in labels]
    labels = np.asarray(labels, dtype=object)
    data = []
    per_group_cap = max(1, MAX_POINTS // max(1, len(groups)))
    for gi, grp in enumerate(groups):
        ii = np.where(labels == grp)[0]
        if len(ii) > per_group_cap:
            ii = ii[np.asarray(stratified_subsample([grp] * len(ii), per_group_cap, seed=gi))]
        data.append({
            "type": "scattergl", "mode": "markers",
            "name": f"{grp} ({len(ii)})",
            "x": xy[ii, 0].tolist(), "y": xy[ii, 1].tolist(),
            "marker": {"size": 4, "opacity": 0.55,
                       "color": OKABE_ITO_B[gi % len(OKABE_ITO_B)]},
        })
    return {"data": data,
            "layout": {"title": {"text": BANNER_COORDS_B, "font": {"size": 12}},
                       "showlegend": True,
                       "xaxis": {"visible": False}, "yaxis": {"visible": False}}}


OKABE_ITO_B = ["#0072B2", "#E69F00", "#009E73", "#D55E00",
               "#CC79A7", "#56B4E9", "#F0E442", "#000000"]
```

Panel 확장 — `render`에 모드 스위치, `on_load`에 기본값:

```python
    # on_load 에 추가:
        ctx.panel.state.mode = "A"
        ctx.panel.state.group_field = "project"
        ctx.panel.state.groups = ""

    # render 상단에 추가:
        panel.btn("toggle_mode", label=f"모드: {ctx.panel.state.mode or 'A'}",
                  on_click=self.on_toggle_mode)
        if ctx.panel.state.mode == "B":
            panel.str("group_field", label="그룹 필드 (기본 project)",
                      on_change=self.on_group_change)
            panel.str("groups", label="그룹들 (쉼표구분, 예: cohort-b,cohort-a)",
                      on_change=self.on_group_change)

    # 메서드 추가:
    def on_toggle_mode(self, ctx):
        ctx.panel.state.mode = "B" if ctx.panel.state.mode == "A" else "A"
        self._refresh(ctx)

    def on_group_change(self, ctx):
        p = ctx.params or {}
        if "group_field" in p:
            ctx.panel.state.group_field = p["group_field"]
        if "groups" in p:
            ctx.panel.state.groups = p["groups"]
        self._refresh(ctx)

    # _refresh 상단에 분기 추가:
        if ctx.panel.state.mode == "B":
            groups = [g.strip() for g in (ctx.panel.state.groups or "").split(",") if g.strip()]
            if groups:
                ctx.panel.set_data("scatter", build_mode_b(
                    ctx.dataset.name, ctx.panel.state.group_field or "project", groups))
            return
```

모드 B는 `ctx.dataset`(현재 세션 데이터셋)을 그린다 — frames_captions 세션에서 열면 project 비교가 된다. 모드 A는 sourcei 세션 전용(상수).

- [ ] **Step 4: selftest 통과 + 배포 + 브라우저 확인**

Run: Task 6 Step 2와 동일 → `selftest OK`.
브라우저: `:5153/datasets/frames_captions` → Panel 열고 모드 B → groups에 `cohort-b,cohort-a` 입력 → 두 project가 같은 좌표 위에 두 색으로 overlay 되는지, 상한 경고 동작(7.3만→서브샘플) 확인.

- [ ] **Step 5: Commit**

```bash
git add docker/analysis/plugins/user-prompt-compare/__init__.py
git commit -m "feat(analysis): 모드 B — 프로젝트별 embedding 비교(R5-b), 같은 좌표계 overlay

같은 데이터셋 슬라이스는 하나의 UMAP fit을 공유하므로 좌표 비교가 정당 —
모드 A(독립 fit, 비교 금지)와 배너부터 다르다. 네이티브 Embeddings 5,000점
상한 우회, 그룹당 MAX_POINTS/n 층화 서브샘플.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: compare 워크스페이스 + 배포 문서화 + RSS 실측 (스펙 §5.7)

**Files:**
- Modify: `docker/analysis/fiftyone_app_setup.py` (compare 워크스페이스 추가)
- Modify: `docker/analysis/README.md` (플러그인 절에 user-prompt-compare 추가)

**Interfaces:**
- Consumes: Task 3 `cmd_workspace` 패턴, Task 8 완성 Panel
- Produces: `sourcei` 워크스페이스 `compare` (Samples / Embeddings / user_prompt_compare 3-패널 — H1 확정안), README 배포·운영 절

- [ ] **Step 1: cmd_workspace 확장**

```python
def cmd_workspace_compare():
    """H1 확정안: Samples | Embeddings(emb_viz) | Prompt Compare 3-패널."""
    import fiftyone as fo
    ds = fo.load_dataset("sourcei")
    space = fo.Space(
        children=[
            fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
            fo.Space(children=[fo.Panel(type="Embeddings",
                                        state=dict(brainResult="emb_viz"))]),
            fo.Space(children=[fo.Panel(type="user_prompt_compare")]),
        ],
        orientation="horizontal",
    )
    ds.save_workspace("compare", space,
                      description="프레임↔문장 비교 (spec 2026-08-07 H1)", overwrite=True)
    assert "compare" in ds.list_workspaces()
    print("workspace 'compare' 저장 완료")
```

`__main__` 분기: `elif sys.argv[1] == "workspace-compare": cmd_workspace_compare()`

- [ ] **Step 2: 실행 + 브라우저 확인**

```bash
docker cp docker/analysis/fiftyone_app_setup.py docker-analysis-1:/workspace/
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py workspace-compare
```
브라우저 F5 → 워크스페이스 `compare` 선택 → 3-패널이 뜨고 Task 8의 연동 4개가 이 배치에서 동작하는지 재확인. 커스텀 패널 type 문자열이 안 맞으면 `fiftyone plugins list`의 operator uri로 교체.

- [ ] **Step 3: RSS 실측 (배포 조건 — 스펙 §5.5)**

Task 5 Step 3의 RSS 명령을 패널 열기 전/후 실행, 증가분 기록.
Expected: ≤ 100MB. 초과 시 `load_prompt_bundle`의 META_FIELDS를 줄이고 재측정.

- [ ] **Step 4: README 문서화**

`docker/analysis/README.md`의 플러그인 절에 추가 (기존 user-embeddings 패턴을 따라):

```markdown
### user-prompt-compare — 교차 데이터셋 비교 패널 (2026-08)

- 정본 `docker/analysis/plugins/user-prompt-compare/` → 배포:
  `docker cp docker/analysis/plugins/user-prompt-compare/. docker-analysis-1:/data/fiftyone/datasets/__plugins__/user-prompt-compare/`
- 워크스페이스 `compare`(sourcei): Samples | Embeddings | Prompt Compare.
  모드 A=프레임↔문장(argmax_k1 조인, dist_iou 모드는 클릭 무효), 모드 B=같은
  데이터셋 그룹 overlay(frames_captions에서 project 비교).
- selftest(조인 불변식 3개): `docker exec docker-analysis-1 python /data/fiftyone/datasets/__plugins__/user-prompt-compare/__init__.py`
  — FiftyOne 업그레이드 전 필수 게이트. 실패 시 producer drift 의심.
- 색상/워크스페이스 재설정: `python /workspace/fiftyone_app_setup.py colors|workspace|workspace-compare`
  (컨테이너 recreate 후 재실행 필요 — 이 디렉토리 전체가 그렇듯)
```

- [ ] **Step 5: Commit**

```bash
git add docker/analysis/fiftyone_app_setup.py docker/analysis/README.md
git commit -m "feat(analysis): compare 3-패널 워크스페이스(H1) + 배포 런북 — Phase 1 완결

RSS 실측 <N>MB (예산 100MB). 원 요구 'sourcei samples+embeddings 창에
sourcei-prompts embeddings 창을 split' 이 이 워크스페이스다.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Self-Review 결과 (계획 작성 시 수행)

- **스펙 커버리지**: Phase 0 4건(0-1→Task 2, 0-2→Task 3, 0-3→Task 5, 0-4→Task 4) + 핀 권고→Task 1. Phase 1: §5.1→Task 5/8, §5.1b→Task 9, §5.2→Task 6/8, §5.3→Task 7/8, §5.4→Task 7, §5.5→Task 6/7 상수+Task 10 RSS, §5.6→Task 6, §5.7→Task 10. Phase 1.5/2는 이 계획의 범위 밖 (스펙 §2 사다리 — Phase 1 실사용 관측 후 별도 계획).
- **자리표시자**: 없음. 단 두 곳의 **의도된 런타임 확정 지점**을 명시함 — (1) Task 5가 확정하는 훅 이름을 Task 8이 사용 (2) Task 3/10의 워크스페이스 state 키 fallback(수동 재저장). 둘 다 대체 경로가 계획 안에 있다.
- **타입/이름 일관성**: `user_prompt_compare`(패널 name), `load_prompt_bundle`/`frame_ids_to_gidx`/`gidx_to_frame_ids`/`stratified_subsample`/`build_mode_a`/`build_mode_b` — Task 간 참조 일치 확인.
