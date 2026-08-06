# frames_captions 프롬프트 뱅크 평가 확장 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** source-h 뱅크 기하 분석을 `--profile frames`로 FiftyOne `frames_captions`(프레임 187,994장)에 확장 — 뱅크·GT가 축적될수록 열리는 사다리 구조 + FiftyOne 시각화.

**Architecture:** 기존 `prompt_geometry.py`를 단일 파일 프로필 파라미터화하고, PG 조인은 신규 `frames_bank_ledger.py`가 source-h과 동일 포맷 `ledger.jsonl`+`embed.npz`로 생산한다(분석기는 DB를 모름). 채점은 도메인 샤드 단위 스트리밍 리덕션(유사도 행렬 미상주), FiftyOne에는 버전 중립 필드 6개만 publish.

**Tech Stack:** Python 3.10 (analysis 컨테이너), numpy fp32, FiftyOne 1.19, psycopg2 2.9.12, PyYAML, bash 래퍼(`docker cp`+`docker exec`).

**Spec:** `docs/superpowers/specs/2026-07-31-frames-captions-bank-eval-design.md` (승인됨)

## Global Constraints

- **fp32 필수** — margin 중앙값 ~0.01, fp16 분해능이 이를 먹는다. 집계 CI 만 fp64.
- **모달리티 필터 필수** — `frames_captions`는 frame 187,994 + caption 11,978(같은 `image_embedding` 필드에 **텍스트 벡터**). 원장은 `modality=='frame'`만.
- **FiftyOne 신규 필드 6개 상한, 버전 중립** — `bank_domain`/`bank_pred`/`bank_decision_margin`/`bank_shift`/`bank_gap`/`bank_gt`. 버전 정체성은 `ds.info["bank_run"]`+런 원장.
- **`stage_slim`은 source-h 전용** — frames_captions에서 실행 시 기존 필드·brain·뷰 21개 파괴. 코드 가드 필수.
- **fail-closed crosswalk** — 미등재 box category는 그 이미지 GT 제외. SAM3 `none`→`normal` 승격 금지. weak 라벨 지표명은 `concordance` 고정(`accuracy`/`recall` 금지), `bank_gt`에 SAM3 절대 미기입.
- **min-n 게이트** — GT 0→`no_gt`(0% 표시 금지) / 1~29→`counts_only` / 30~99→`exploratory` / ≥100→`reportable`.
- **커버리지 스탬프** — 모든 스테이지 첫 줄에 `[stamp] ...` 출력, 자격 미달 시 이유+hard-skip.
- **clear-then-set** — 매 score 런마다 6필드 전체 clear 후 현재 매핑 분만 set (stale 값 방지).
- **메모리** — `--mem-budget-gb`(기본 4) preflight: `MemAvailable < 2×budget`이면 시작 거부. 래퍼가 `OMP_NUM_THREADS=4` 강제. 공유 호스트(스왑 쓰래싱 이력).
- **source-h 무손상** — `--profile` 기본값 `sourceh`, 기존 `bank_eval.sh` 호출 무변경. Task 1 회귀로 증명.
- **운영 금기** — 호스트 src/ 수동 수정 금지(이 작업은 전부 `docker/analysis/`+`docs/`, git 경로로만). prod DB는 읽기 전용 쿼리만. `docker compose` 직접 호출 금지.
- **개발/실행 분리** — 코드는 호스트 repo(feature 브랜치)에서 편집·커밋, 실행은 래퍼가 `docker cp`로 컨테이너 반입 후 `docker exec`. ambient `/workspace` 의존 금지(필요 파일 전부 명시 복사).

## File Structure

| 파일 | 책임 |
|---|---|
| `docker/analysis/prompt_geometry.py` (수정) | 프로필 전환, 스트리밍 채점, frames 전용 스테이지(score/gap/viz/gtsync/report), selftest, slim 가드 |
| `docker/analysis/frames_bank_ledger.py` (신규) | FiftyOne+PG → `ledger.jsonl`/`embed.npz`/`gt_snapshot.json` 생산. 유일하게 DB를 아는 파일 |
| `docker/analysis/bank_domain_map.yaml` (신규) | project→도메인→뱅크쌍 + class crosswalk + unsupported (fail-closed 설정) |
| `docker/analysis/frames_bank_eval.sh` (신규) | 원커맨드: 파일 반입 → ledger → score → gap → viz → gtsync → report |
| `docker/analysis/README.md` (수정) | 운영 절차 추가 |

산출 데이터 루트(컨테이너): `/data/fiftyone/frames_bank/{work,work/geometry,report}` — source-h의 `sourceh_v2` 컨벤션 미러. 뱅크 npz는 `/data/fiftyone/sourceh/prompts/` **공유**(버전 전역 자원).

---

### Task 1: 프로필 기반 구조 + 메모리 preflight + slim 가드

**Files:**
- Modify: `docker/analysis/prompt_geometry.py:34-46` (상수부), `:831` (stage_slim), `:1007-1030` (main)

**Interfaces:**
- Produces: `PROFILES` dict, `set_profile(name)` (모듈 전역 `ROOT/WORK/GEO/REPORT_DIR/PROMPT_DIR/CLASS_NAMES/PROFILE` 재설정), `assert_mem_budget(budget_gb: float)`, main의 `--profile {sourceh,frames}` / `--mem-budget-gb` 인자. 이후 모든 Task가 이 전역을 사용.
- Consumes: 기존 상수·스테이지 (무변경 유지).

- [ ] **Step 1: 상수부를 프로필화**

`prompt_geometry.py:34-38`의 상수 5줄을 다음으로 교체 (기존 값은 `sourceh` 프로필로 이동, `VERSIONS`/`V0`/`V4`/`EVENT_CLASSES`/`SEEDS`는 그대로 둔다):

```python
PROFILES = {
    "sourceh": {
        "root": "/data/fiftyone/sourceh_v2",
        "dataset": "source-h",
        "prompt_dir": "/data/fiftyone/sourceh/prompts",
        "class_names": {0: "normal", 1: "falldown", 2: "fire", 3: "smoke"},
        "map_yaml": None,
    },
    "frames": {
        "root": "/data/fiftyone/frames_bank",
        "dataset": "frames_captions",
        "prompt_dir": "/data/fiftyone/sourceh/prompts",   # 뱅크 npz 는 버전 전역 자원 — 공유
        "class_names": {0: "normal", 1: "falldown", 2: "fire", 3: "smoke", 4: "smoking"},
        "map_yaml": os.environ.get("BANK_DOMAIN_MAP", "/workspace/bank_domain_map.yaml"),
    },
}
PROFILE = "sourceh"
ROOT = PROFILES["sourceh"]["root"]
WORK = f"{ROOT}/work"
GEO = f"{WORK}/geometry"
REPORT_DIR = f"{ROOT}/report"
PROMPT_DIR = PROFILES["sourceh"]["prompt_dir"]
EMBED_URL = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")


def set_profile(name: str) -> None:
    """모듈 전역 경로/클래스를 프로필로 전환 — 기존 900줄 수학은 전역만 보므로 무수정 재사용."""
    global PROFILE, ROOT, WORK, GEO, REPORT_DIR, PROMPT_DIR, CLASS_NAMES
    p = PROFILES[name]
    PROFILE = name
    ROOT = p["root"]
    WORK = f"{ROOT}/work"
    GEO = f"{WORK}/geometry"
    REPORT_DIR = f"{ROOT}/report"
    PROMPT_DIR = p["prompt_dir"]
    CLASS_NAMES = p["class_names"]


def assert_mem_budget(budget_gb: float) -> None:
    """공유 호스트 보호 — 2026-07 스왑 쓰래싱 사건 재발 방지. 부족하면 시작 자체를 거부."""
    avail_kb = 0
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                avail_kb = int(line.split()[1])
                break
    avail_gb = avail_kb / 1024 / 1024
    if avail_gb < 2 * budget_gb:
        raise SystemExit(f"메모리 부족: available {avail_gb:.1f}G < 2×budget {budget_gb:.0f}G — 시작 거부")
```

주의: `CLASS_NAMES = {0: "normal", ...}` 모듈 상수 줄(기존 :44)은 그대로 두고 `set_profile`이 덮어쓴다 (sourceh 기본값 유지).

- [ ] **Step 2: stage_slim 파괴 가드**

`stage_slim()` 함수(:831) 본문 첫 줄에 추가:

```python
    if PROFILES[PROFILE]["dataset"] != "source-h":
        raise SystemExit("slim 은 source-h 전용 — SLIM_DROP_* 하드코딩 리스트가 다른 데이터셋의 "
                         "필드/brain/뷰를 파괴한다 (스펙 §5-1). frames 프로필에서 영구 금지")
```

- [ ] **Step 3: main() 에 프로필 인자 + 스테이지 게이팅**

`main()`(:1007)을 다음으로 교체 (frames 스테이지 함수들은 Task 5~7에서 구현 — 이 시점엔 dict에 이름만 넣지 말고 sourceh 게이팅까지만):

```python
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["bank", "analyze", "ablate", "gap", "viz", "flips", "guide",
                                      "slim", "report", "all", "selftest",
                                      "score", "gtsync"])
    ap.add_argument("--profile", choices=list(PROFILES),
                    default=os.environ.get("BANK_PROFILE", "sourceh"))
    ap.add_argument("--csv", help="bank 스테이지: 프롬프트 CSV 경로")
    ap.add_argument("--version", help="bank 스테이지: 버전 이름 (npz 파일명)")
    ap.add_argument("--mem-budget-gb", type=float, default=4.0)
    args = ap.parse_args()
    set_profile(args.profile)
    assert_mem_budget(args.mem_budget_gb)
    os.makedirs(GEO, exist_ok=True)

    if PROFILE == "frames":
        sourceh_only = {"analyze", "ablate", "flips", "guide", "slim"}
        table = {"score": stage_score, "gap": stage_gap_frames, "viz": stage_viz_frames,
                 "gtsync": stage_gtsync, "report": stage_report_frames,
                 "selftest": stage_selftest}
        stages = ["score", "gap", "viz", "gtsync", "report"] if args.stage == "all" else [args.stage]
        for st in stages:
            log(f"───── stage: {st} (profile=frames) ─────")
            if st in sourceh_only:
                raise SystemExit(f"{st} 는 sourceh 프로필 전용 — frames 자격 미달 "
                                 "(팩토리얼=동일도메인 뱅크 2벌, guide/flips=GT 분모 필요. 스펙 §1)")
            if st == "bank":
                if not (args.csv and args.version):
                    raise SystemExit("bank 스테이지는 --csv 와 --version 이 필요하다")
                stage_bank(args.csv, args.version)
                continue
            table[st]()
        log("완료")
        return

    stages = ["analyze", "ablate", "gap", "viz", "flips", "guide", "slim", "report"] \
        if args.stage == "all" else [args.stage]
    for st in stages:
        log(f"───── stage: {st} ─────")
        if st == "selftest":
            stage_selftest()
            continue
        if st in ("score", "gtsync"):
            raise SystemExit(f"{st} 는 frames 프로필 전용")
        if st == "bank":
            if not (args.csv and args.version):
                raise SystemExit("bank 스테이지는 --csv 와 --version 이 필요하다")
            stage_bank(args.csv, args.version)
            continue
        {"analyze": stage_analyze, "ablate": stage_ablate, "gap": stage_gap, "viz": stage_viz,
         "flips": stage_flips, "guide": stage_guide, "slim": stage_slim,
         "report": stage_report}[st]()
    log("완료")
```

Task 1 시점에는 `stage_score` 등 4개 함수가 아직 없으므로, **임시 스텁**을 main 위에 추가해 파일이 import 가능하게 유지한다 (Task 5~7에서 실구현으로 교체):

```python
def stage_score() -> None:
    raise SystemExit("미구현 (Task 5)")


def stage_gap_frames() -> None:
    raise SystemExit("미구현 (Task 6)")


def stage_viz_frames() -> None:
    raise SystemExit("미구현 (Task 6)")


def stage_gtsync() -> None:
    raise SystemExit("미구현 (Task 7)")


def stage_report_frames() -> None:
    raise SystemExit("미구현 (Task 7)")


def stage_selftest() -> None:
    raise SystemExit("미구현 (Task 2)")
```

- [ ] **Step 4: source-h 회귀 검증 (무손상 증명)**

```bash
REPO=/home/user/work_p/Datapipeline-Data-data_pipeline
# 기준값 백업 후 재실행
docker exec docker-analysis-1 cp /data/fiftyone/sourceh_v2/work/geometry/geometry.json /tmp/geometry_before.json
docker cp $REPO/docker/analysis/prompt_geometry.py docker-analysis-1:/workspace/
docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py analyze
docker exec docker-analysis-1 python3 -c "
import json
a=json.load(open('/tmp/geometry_before.json')); b=json.load(open('/data/fiftyone/sourceh_v2/work/geometry/geometry.json'))
assert abs(a['full']['v1.0.8.0']['micro']-b['full']['v1.0.8.0']['micro'])<1e-9
assert abs(a['full']['v1.0.8.4']['micro']-b['full']['v1.0.8.4']['micro'])<1e-9
print('source-h 회귀 OK:', b['full']['v1.0.8.4']['micro'])"
```

Expected: `source-h 회귀 OK: 0.910...` (시드 고정이라 완전 일치)

- [ ] **Step 5: 게이팅 동작 확인**

```bash
docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py slim --profile frames; echo "exit=$?"
docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py score; echo "exit=$?"
```

Expected: 첫 줄 `slim 은 source-h 전용 ...` exit=1 / 둘째 `score 는 frames 프로필 전용` exit=1

- [ ] **Step 6: Commit**

```bash
git add docker/analysis/prompt_geometry.py
git commit -m "refactor(analysis): prompt_geometry 프로필 파라미터화 + slim 가드 + 메모리 preflight"
```

---

### Task 2: 스트리밍 채점 + min-n/crosswalk 헬퍼 + selftest

**Files:**
- Modify: `docker/analysis/prompt_geometry.py` (`class_sims` 아래에 추가; `class_sims` 자체는 sourceh `analyze`가 통계 재표집에 전체 행렬을 쓰므로 **삭제하지 않는다**)

**Interfaces:**
- Produces: `bank_best_stream(X, bank, batch=1024, block=2048) -> (best: dict[int, np.ndarray[N]], arg: dict[int, np.ndarray[N]])` — arg 는 뱅크 전역 프롬프트 인덱스. `crosswalk_class(cw: dict, category: str) -> str | None`. `minn_tier(n: int) -> str` (`no_gt|counts_only|exploratory|reportable`). `stage_selftest()` 실구현(스텁 교체).
- Consumes: Task 1의 전역.

- [ ] **Step 1: selftest 먼저 작성 (실패 확인용)** — `stage_selftest` 스텁을 실구현으로 교체:

```python
def stage_selftest() -> None:
    """데이터 불필요 자가검증 — 스트리밍 리덕션 == 순진 행렬곱, crosswalk fail-closed, min-n."""
    rng = np.random.default_rng(0)
    X = rng.normal(size=(500, 64)).astype(np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    V = rng.normal(size=(300, 64)).astype(np.float32)
    V /= np.linalg.norm(V, axis=1, keepdims=True)
    bank = {"vec": V, "cls": rng.integers(0, 4, 300).astype(np.int64),
            "prompt": [f"p{i}" for i in range(300)]}
    best, arg = bank_best_stream(X, bank, batch=64, block=32)   # 일부러 작은 배치로 경계 검증
    for c in sorted(set(bank["cls"].tolist())):
        idx = np.flatnonzero(bank["cls"] == c)
        S = X @ V[idx].T
        assert np.allclose(best[c], S.max(axis=1), atol=1e-6), f"best mismatch c={c}"
        # arg 는 뱅크 전역 인덱스 — 그 프롬프트와의 코사인이 곧 best 여야 한다
        recomputed = np.einsum("ij,ij->i", X, V[arg[c]])
        assert np.allclose(best[c], recomputed, atol=1e-6), f"arg 가 best 를 가리키지 않음 c={c}"
        assert np.isin(arg[c], idx).all(), f"arg 가 타 클래스 프롬프트를 가리킴 c={c}"
    cw = {"fire": "fire", "__no_box_finalized__": "normal"}
    assert crosswalk_class(cw, "fire") == "fire"
    assert crosswalk_class(cw, "patient") is None, "미등재 category 는 None(fail-closed)이어야 한다"
    assert crosswalk_class(cw, "__no_box_finalized__") == "normal"
    assert minn_tier(0) == "no_gt" and minn_tier(5) == "counts_only"
    assert minn_tier(30) == "exploratory" and minn_tier(99) == "exploratory"
    assert minn_tier(100) == "reportable"
    log("selftest OK")
```

- [ ] **Step 2: 실패 확인**

```bash
docker cp $REPO/docker/analysis/prompt_geometry.py docker-analysis-1:/workspace/
docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py selftest
```

Expected: `NameError: name 'bank_best_stream' is not defined` (구현 전)

- [ ] **Step 3: 구현** — `class_sims` 아래에 추가:

```python
def bank_best_stream(X: np.ndarray, bank: dict, batch: int = 1024,
                     block: int = 2048) -> tuple[dict, dict]:
    """클래스별 per-frame best cosine + argmax(뱅크 전역 인덱스) — 유사도 행렬 미상주.

    class_sims 는 [N, n_c] 전체를 할당한다(sourceh 13k 에선 OK, frames 200k 에선 12GB → 스왑
    쓰래싱). 여기선 [batch, block] 타일(8MB)만 만들고 즉시 running max 로 접는다. fp32 필수.
    """
    classes = sorted(set(bank["cls"].tolist()))
    n = X.shape[0]
    best = {c: np.full(n, -2.0, dtype=np.float32) for c in classes}
    arg = {c: np.zeros(n, dtype=np.int64) for c in classes}
    for c in classes:
        gidx = np.flatnonzero(bank["cls"] == c)
        V = bank["vec"][gidx]
        for q in range(0, V.shape[0], block):
            Vb = V[q:q + block]
            for s in range(0, n, batch):
                S = X[s:s + batch] @ Vb.T
                m = S.max(axis=1)
                a = S.argmax(axis=1)
                seg_best = best[c][s:s + batch]          # view — 제자리 갱신
                seg_arg = arg[c][s:s + batch]
                upd = m > seg_best
                seg_best[upd] = m[upd]
                seg_arg[upd] = gidx[q + a[upd]]
        best[c] = np.ascontiguousarray(best[c])
    return best, arg


def crosswalk_class(cw: dict, category: str) -> str | None:
    """box category → frame class. 미등재 = None = 그 이미지 GT 제외 (fail-closed)."""
    return cw.get(category)


def minn_tier(n: int) -> str:
    """min-n 게이트 (스펙 §7): 0=no_gt(0% 표시 금지) / <30=counts_only / <100=exploratory."""
    if n == 0:
        return "no_gt"
    if n < 30:
        return "counts_only"
    if n < 100:
        return "exploratory"
    return "reportable"
```

- [ ] **Step 4: selftest 통과 확인**

```bash
docker cp $REPO/docker/analysis/prompt_geometry.py docker-analysis-1:/workspace/
docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py selftest
docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py selftest --profile frames
```

Expected: 양쪽 다 `selftest OK`

- [ ] **Step 5: Commit**

```bash
git add docker/analysis/prompt_geometry.py
git commit -m "feat(analysis): 스트리밍 뱅크 채점 + crosswalk/min-n 헬퍼 + selftest 스테이지"
```

---

### Task 3: bank_domain_map.yaml + 로더

**Files:**
- Create: `docker/analysis/bank_domain_map.yaml`
- Modify: `docker/analysis/prompt_geometry.py` (로더 추가)

**Interfaces:**
- Produces: `load_domain_map() -> dict` — 키 `domains`(dict), `class_crosswalk`(dict), `unsupported_classes`(list), `project_to_domain`(dict, 파생), `crosswalk_version`. `NAME_TO_ID: dict[str, int]` 모듈 상수.
- Consumes: `PROFILES[PROFILE]["map_yaml"]`.

- [ ] **Step 1: YAML 작성** — `docker/analysis/bank_domain_map.yaml`:

```yaml
# frames_captions 프롬프트 뱅크 평가 — 도메인 매핑 (fail-closed)
# 스펙: docs/superpowers/specs/2026-07-31-frames-captions-bank-eval-design.md §5-4
#
# 시드 정본: 노션 "프롬프트 버전/관리 체계 구축" (도메인 번호·버전쌍 확정 후 기입).
# domains 가 비어 있으면 score 스테이지는 hard-skip 한다 (0단계: 스탬프만 — 정상 동작).
crosswalk_version: 1
domains: {}
# 시드 예시 (노션 대조 후 주석 해제):
# domains:
#   fire_smoke:
#     projects: [fire_smoke]     # frames_captions 의 project 필드 값들
#     bank_a: v1.0.5.7           # A 슬롯 = 기준(구) 버전 — /data/fiftyone/sourceh/prompts/<ver>.npz
#     bank_b: v2.0.5.3           # B 슬롯 = 신버전
class_crosswalk:
  # box category(image_label_annotations.category) → frame class. 미등재 = 그 이미지 GT 제외.
  fire: fire
  smoke: smoke
  __no_box_finalized__: normal   # 무박스 finalized = 사람이 "이벤트 없음" 확인 = normal
  # patient/person (vanguardhealthcarevhc 288장): frame class 사상을 결정하기 전까지
  # 의도적으로 미등재 — 등재 전에는 GT 축에서 제외된다 (스펙 §9-3).
unsupported_classes: [smoking]   # 뱅크에 프롬프트 0개 → status=unsupported ("0% recall" 표시 금지)
```

- [ ] **Step 2: 로더 + 상수 추가** — `prompt_geometry.py`의 `minn_tier` 아래:

```python
NAME_TO_ID = {"normal": 0, "falldown": 1, "fire": 2, "smoke": 3, "smoking": 4}


def load_domain_map() -> dict:
    import yaml

    path = PROFILES[PROFILE]["map_yaml"]
    with open(path, encoding="utf-8") as f:
        m = yaml.safe_load(f) or {}
    if not m.get("domains"):            # 미기재·null 모두 빈 dict 로 (0단계)
        m["domains"] = {}
    m.setdefault("class_crosswalk", {})
    m.setdefault("unsupported_classes", [])
    m["project_to_domain"] = {p: d for d, cfg in m["domains"].items()
                              for p in (cfg.get("projects") or [])}
    for d, cfg in m["domains"].items():
        for k in ("bank_a", "bank_b"):
            if not cfg.get(k):
                raise SystemExit(f"bank_domain_map.yaml: domains.{d}.{k} 누락 (fail-closed)")
    return m
```

- [ ] **Step 3: 로더 검증**

```bash
docker cp $REPO/docker/analysis/bank_domain_map.yaml docker-analysis-1:/workspace/
docker cp $REPO/docker/analysis/prompt_geometry.py docker-analysis-1:/workspace/
docker exec docker-analysis-1 python3 -c "
import sys; sys.path.insert(0,'/workspace')
import prompt_geometry as g
g.set_profile('frames')
m = g.load_domain_map()
assert m['domains'] == {} and m['project_to_domain'] == {}
assert m['class_crosswalk']['__no_box_finalized__'] == 'normal'
assert 'patient' not in m['class_crosswalk']
assert m['unsupported_classes'] == ['smoking']
print('map OK (0단계 상태)')"
```

Expected: `map OK (0단계 상태)`

- [ ] **Step 4: Commit**

```bash
git add docker/analysis/bank_domain_map.yaml docker/analysis/prompt_geometry.py
git commit -m "feat(analysis): 도메인 뱅크 매핑 YAML(fail-closed crosswalk) + 로더"
```

---

### Task 4: frames_bank_ledger.py — 원장 생산자 (유일하게 DB를 아는 파일)

**Files:**
- Create: `docker/analysis/frames_bank_ledger.py`

**Interfaces:**
- Produces (컨테이너 `/data/fiftyone/frames_bank/work/`):
  - `ledger.jsonl` — frame-modality 전 샘플 1행: `{key(=FiftyOne sample id), image_id, project, domain(str|null), src_video, gt_class(int, -1=GT없음), gt_source("ls_finalized"|null), gt_observed_at}`
  - `embed.npz` — **매핑된 도메인 프레임만** `{key, vec}` (미매핑 184k 를 안 담는 이유: 곱할 뱅크가 없고 루트 디스크 여유 30G 보호)
  - `gt_snapshot.json` — `{sha, n_images, n_boxes, excluded, crosswalk_version, gt_observed_at}`
- Consumes: `DATAOPS_POSTGRES_DSN` env(컨테이너 기본 존재), `BANK_DOMAIN_MAP` env, FiftyOne `frames_captions`.

- [ ] **Step 1: 전체 구현**

```python
#!/usr/bin/env python3
"""frames_captions → 뱅크 평가 원장 생산자.

분석기(prompt_geometry.py --profile frames)는 이 출력만 소비하고 DB 를 모른다 —
source-h 의 ledger.jsonl/embed.npz 데이터 계약을 그대로 미러 (스펙 §4·§5-2).

GT: image_id → image_labels(review_status='finalized') **좌조인** + annotations.category
    → crosswalk(fail-closed). 무박스 finalized = '__no_box_finalized__' → normal
    (inner join 이 이를 조용히 버리는 기존 QA 쿼리 함정 회피 — codex 지적).
    미등재 category 나 다중 이벤트 클래스 프레임은 gt_class=-1 + 사유 카운트.
SAM3 auto_generated 는 어떤 경우에도 GT 로 쓰지 않는다 (bank_gt 불변식, 스펙 §7).
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import time

import numpy as np
import psycopg2
import yaml

ROOT = "/data/fiftyone/frames_bank"
WORK = f"{ROOT}/work"
DSN = os.environ.get("DATAOPS_POSTGRES_DSN",
                     "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
MAP_YAML = os.environ.get("BANK_DOMAIN_MAP", "/workspace/bank_domain_map.yaml")
NAME_TO_ID = {"normal": 0, "falldown": 1, "fire": 2, "smoke": 3, "smoking": 4}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_finalized_gt(crosswalk: dict) -> tuple[dict, collections.Counter, int]:
    """image_id(str) → frame class 이름. 좌조인이라 무박스 finalized 도 잡힌다."""
    q = """
    SELECT il.image_id::text, ila.category
    FROM image_labels il
    LEFT JOIN image_label_annotations ila ON ila.image_label_id = il.image_label_id
    WHERE il.review_status = 'finalized'
    """
    cats: dict[str, set] = collections.defaultdict(set)
    n_boxes = 0
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(q)
        for image_id, category in cur.fetchall():
            cats[image_id].add(category)          # None = 무박스 finalized
            if category is not None:
                n_boxes += 1
    gt: dict[str, str] = {}
    excluded: collections.Counter = collections.Counter()
    for image_id, cs in cats.items():
        mapped: set[str] = set()
        bad = False
        for c in cs:
            key = "__no_box_finalized__" if c is None else c
            m = crosswalk.get(key)                # 미등재 = None = fail-closed
            if m is None:
                bad = True
                excluded[key] += 1
            elif m != "normal":
                mapped.add(m)
        if bad:
            continue
        if len(mapped) > 1:                       # 한 프레임 다중 이벤트 — frame 단일클래스 GT 불성립
            excluded["__multi_class__"] += 1
            continue
        gt[image_id] = mapped.pop() if mapped else "normal"
    return gt, excluded, n_boxes


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="파일 미기록, 스탬프만 출력")
    args = ap.parse_args()
    import fiftyone as fo
    from fiftyone import ViewField as F

    with open(MAP_YAML, encoding="utf-8") as f:
        m = yaml.safe_load(f) or {}
    crosswalk = m.get("class_crosswalk") or {}
    proj2dom = {p: d for d, cfg in (m.get("domains") or {}).items()
                for p in (cfg.get("projects") or [])}

    gt_by_image, excluded, n_boxes = fetch_finalized_gt(crosswalk)
    observed_at = time.strftime("%Y-%m-%dT%H:%M:%S")

    ds = fo.load_dataset("frames_captions")
    view = ds.match(F("modality") == "frame")     # 캡션 11,978 = 같은 필드의 텍스트 벡터 → 제외 (필수)
    ids = view.values("id")
    image_ids = view.values("image_id")
    projects = view.values("project")
    assets = view.values("asset_id")

    rows = []
    n_gt = 0
    for sid, iid, proj, asset in zip(ids, image_ids, projects, assets):
        g = gt_by_image.get(str(iid)) if iid else None
        if g is not None:
            n_gt += 1
        rows.append({
            "key": sid,
            "image_id": str(iid) if iid else None,
            "project": proj,
            "domain": proj2dom.get(proj),
            "src_video": asset or proj or "unknown",   # 부트스트랩 군집 단위 (iid 아님 방어)
            "gt_class": NAME_TO_ID[g] if g else -1,
            "gt_source": "ls_finalized" if g else None,
            "gt_observed_at": observed_at if g else None,
        })

    dom_counts = collections.Counter(r["domain"] for r in rows if r["domain"])
    log(f"[stamp] ledger: frame {len(rows):,} / 매핑 {dict(dom_counts) or '없음(0단계)'} / "
        f"GT 이미지 {n_gt} (box {n_boxes:,}) / crosswalk 제외 {dict(excluded) or '없음'}")
    if args.dry_run:
        return

    os.makedirs(WORK, exist_ok=True)
    with open(f"{WORK}/ledger.jsonl", "w", encoding="utf-8") as f:
        for r in rows:                            # 전량 재작성 — 원천이 DB/데이터셋이라 증분 불필요
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    scored = [r["key"] for r in rows if r["domain"]]
    if scored:
        vecs = view.select(scored, ordered=True).values("image_embedding")
        X = np.asarray(vecs, dtype=np.float32)
        np.savez_compressed(f"{WORK}/embed.npz", key=np.array(scored), vec=X)
        log(f"embed.npz: {X.shape}")
    elif os.path.exists(f"{WORK}/embed.npz"):
        os.remove(f"{WORK}/embed.npz")            # 매핑 제거 시 stale 임베딩도 제거

    gt_sha = hashlib.sha256(json.dumps(
        sorted([r["image_id"], r["gt_class"]] for r in rows if r["gt_class"] >= 0)
    ).encode()).hexdigest()[:16]
    with open(f"{WORK}/gt_snapshot.json", "w", encoding="utf-8") as f:
        json.dump({"sha": gt_sha, "n_images": n_gt, "n_boxes": n_boxes,
                   "excluded": dict(excluded),
                   "crosswalk_version": m.get("crosswalk_version"),
                   "gt_observed_at": observed_at}, f, ensure_ascii=False, indent=1)
    log(f"ledger {len(rows):,}행 / gt_snapshot sha={gt_sha}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: dry-run 검증 (prod 읽기 전용)**

```bash
docker cp $REPO/docker/analysis/frames_bank_ledger.py docker-analysis-1:/workspace/
docker exec -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml docker-analysis-1 \
  python3 /workspace/frames_bank_ledger.py --dry-run
```

Expected (오늘 데이터 기준): `[stamp] ledger: frame 187,994 / 매핑 없음(0단계) / GT 이미지 0 (box 1,558) / crosswalk 제외 {'patient': ..., 'person': ...}` — vanguard 288장이 patient/person **미등재**로 fail-closed 제외되는 것이 올바른 동작이다 (스펙 §9-3).

- [ ] **Step 3: 실기록 실행 + 파일 확인**

```bash
docker exec -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml docker-analysis-1 \
  python3 /workspace/frames_bank_ledger.py
docker exec docker-analysis-1 sh -c \
  "wc -l /data/fiftyone/frames_bank/work/ledger.jsonl && cat /data/fiftyone/frames_bank/work/gt_snapshot.json"
```

Expected: `187994 .../ledger.jsonl`, gt_snapshot에 `"n_images": 0, "n_boxes": 1558`, `embed.npz` 없음(매핑 0)

- [ ] **Step 4: Commit**

```bash
git add docker/analysis/frames_bank_ledger.py
git commit -m "feat(analysis): frames_captions 원장 생산자 — PG finalized 좌조인 + fail-closed crosswalk"
```

---

### Task 5: stage_score — 도메인 샤드 GT-free 채점 + 필드 publish

**Files:**
- Modify: `docker/analysis/prompt_geometry.py` (Task 1 스텁 `stage_score` 교체 + 헬퍼 추가)

**Interfaces:**
- Consumes: `bank_best_stream`, `predict`, `load_domain_map`, `WORK/ledger.jsonl`, `WORK/embed.npz`, `PROMPT_DIR/<ver>.npz`
- Produces: FiftyOne 필드 4개(`bank_domain`, `bank_pred`, `bank_decision_margin`, `bank_shift`) + `ds.info["bank_run"]`; 캐시 `GEO/<domain>_score.npz` (키 `key, pred_a, pred_b, margin, margin_a, best_b_<c>, arg_b_<c>`); 런 원장 `GEO/runs.jsonl` (`_append_run(run_id, domain, **kw)`); `_load_frames_ledger() -> list[dict]`. Task 6·7이 캐시와 `_append_run`을 소비.

- [ ] **Step 1: 헬퍼 + 본체 구현** (스텁 `stage_score` 교체):

```python
def _load_frames_ledger() -> list[dict]:
    return list(jsonl_load(f"{WORK}/ledger.jsonl").values())


def _append_run(run_id: str, domain: str, **kw) -> None:
    import resource

    os.makedirs(GEO, exist_ok=True)
    rec = {"run_id": run_id, "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "profile": PROFILE, "domain": domain,
           "mem_peak_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024, **kw}
    with open(f"{GEO}/runs.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


BANK_FIELDS = ("bank_domain", "bank_pred", "bank_decision_margin",
               "bank_shift", "bank_gap", "bank_gt")
# weak(SAM3 normalized_class) → 뱅크 클래스. none/person 은 어느 쪽으로도 주장 불가 → 미등재.
WEAK_TO_BANK = {"fall": "falldown", "fire": "fire", "smoke": "smoke"}


def stage_score() -> None:
    """frames: 도메인 샤드 GT-free 채점 → 필드 publish (clear-then-set) + 런 원장."""
    m = load_domain_map()
    rows = _load_frames_ledger()
    total = len(rows)
    by_dom: dict[str, list] = collections.defaultdict(list)
    for r in rows:
        if r.get("domain"):
            by_dom[r["domain"]].append(r)
    n_gt = sum(1 for r in rows if r.get("gt_class", -1) >= 0)
    log(f"[stamp] score: 전체 {total:,} / 매핑 {sum(map(len, by_dom.values())):,}"
        f" ({len(by_dom)}개 도메인) / GT {n_gt}")

    import fiftyone as fo

    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    run_id = f"score-{time.strftime('%Y%m%d-%H%M%S')}"

    # clear-then-set 은 hard-skip 판정보다 먼저 — 매핑이 비워진(축소된) 경우에도 이전 런의
    # stale 필드를 반드시 제거해야 한다 (스펙 §8: stale 값이 가장 악질적인 분석 거짓말)
    sch = ds.get_field_schema()
    for fld in BANK_FIELDS:
        if fld in sch:
            ds.clear_sample_field(fld)

    if not by_dom:
        ds.info["bank_run"] = {"run_id": run_id, "profile": PROFILE, "domains": {},
                               "n_gt": n_gt, "total": total,
                               "ts": time.strftime("%Y-%m-%d %H:%M")}
        ds.save()
        log("score: 매핑된 도메인 없음 → hard-skip (0단계). bank_domain_map.yaml 의 "
            "domains 를 노션 버전관리 페이지 기준으로 시드하면 열린다")
        return

    z = np.load(f"{WORK}/embed.npz", allow_pickle=True)
    key2i = {str(k): i for i, k in enumerate(z["key"])}
    Xall = z["vec"].astype(np.float32)
    Xall /= np.linalg.norm(Xall, axis=1, keepdims=True)

    for dom in sorted(by_dom):
        try:
            _score_domain(ds, m, dom, by_dom[dom], key2i, Xall, run_id)
        except Exception as exc:  # noqa: BLE001 — per-domain fail-forward (파이프라인 관례)
            log(f"score {dom}: 실패 {exc!r} — 다음 도메인 진행")
            _append_run(run_id, dom, error=repr(exc))
    ds.info["bank_run"] = {
        "run_id": run_id, "profile": PROFILE,
        "domains": {d: {"a": m["domains"][d]["bank_a"], "b": m["domains"][d]["bank_b"],
                        "n": len(by_dom[d])} for d in by_dom},
        "n_gt": n_gt, "total": total, "ts": time.strftime("%Y-%m-%d %H:%M"),
    }
    ds.save()
    log(f"score 완료: run={run_id}")


def _score_domain(ds, m: dict, dom: str, drows: list, key2i: dict,
                  Xall: np.ndarray, run_id: str) -> None:
    import fiftyone as fo

    cfg = m["domains"][dom]
    va, vb = cfg["bank_a"], cfg["bank_b"]
    banks = {}
    for v in (va, vb):
        path = f"{PROMPT_DIR}/{v}.npz"
        if not os.path.exists(path):
            raise FileNotFoundError(f"뱅크 npz 없음: {path} — 먼저 bank 스테이지로 생성")
        zb = np.load(path, allow_pickle=True)
        banks[v] = {"vec": zb["vec"].astype(np.float32), "cls": zb["cls"].astype(np.int64),
                    "prompt": [str(p) for p in zb["prompt"]]}
    keys = [r["key"] for r in drows if r["key"] in key2i]
    if not keys:
        log(f"[stamp] score {dom}: embed 교집합 0 → skip (ledger 재실행 필요?)")
        return
    X = Xall[[key2i[k] for k in keys]]

    best_a, _ = bank_best_stream(X, banks[va])
    best_b, arg_b = bank_best_stream(X, banks[vb])
    pred_a, pred_b = predict(best_a), predict(best_b)

    def dmargin(best: dict) -> np.ndarray:
        M = np.stack([best[c] for c in sorted(best)], axis=1)
        M.sort(axis=1)
        return (M[:, -1] - M[:, -2]).astype(np.float32)   # decision margin = top1−top2 (GT-free)

    margin_a, margin_b = dmargin(best_a), dmargin(best_b)

    ds.set_values("bank_domain", {k: dom for k in keys}, key_field="id")
    ds.set_values("bank_pred", {k: fo.Classification(label=CLASS_NAMES[int(p)])
                                for k, p in zip(keys, pred_b)}, key_field="id")
    ds.set_values("bank_decision_margin",
                  {k: float(v) for k, v in zip(keys, margin_b)}, key_field="id")
    ds.set_values("bank_shift", {
        k: fo.Classification(label=(f"{CLASS_NAMES[int(a)]}→{CLASS_NAMES[int(b)]}"
                                    if a != b else "unchanged"))
        for k, a, b in zip(keys, pred_a, pred_b)}, key_field="id")

    # weak concordance (참고 신호 — recall 아님, 스펙 §7)
    weak = ds.select(keys, ordered=True).values("normalized_class")
    wmask = [i for i, w in enumerate(weak) if WEAK_TO_BANK.get(w or "")]
    concordance = (float(np.mean([CLASS_NAMES[int(pred_b[i])] == WEAK_TO_BANK[weak[i]]
                                  for i in wmask])) if wmask else None)

    np.savez_compressed(f"{GEO}/{dom}_score.npz",
                        key=np.array(keys), pred_a=pred_a, pred_b=pred_b,
                        margin=margin_b, margin_a=margin_a,
                        **{f"best_b_{c}": best_b[c] for c in best_b},
                        **{f"arg_b_{c}": arg_b[c] for c in arg_b})
    n_shift = int((pred_a != pred_b).sum())
    log(f"score {dom}: n={len(keys):,} {va}→{vb} 예측변화 {n_shift:,} "
        f"({n_shift / len(keys):.1%}) / margin 중앙값 {np.median(margin_b):.4f}"
        f"{f' / concordance(weak,참고) {concordance:.1%} n={len(wmask)}' if concordance is not None else ''}")
    _append_run(run_id, dom, bank_a=va, bank_b=vb, n_scored=len(keys), n_shift=n_shift,
                margin_median=float(np.median(margin_b)),
                concordance_weak=concordance, n_weak=len(wmask))
```

- [ ] **Step 2: 0단계 hard-skip 검증**

```bash
docker cp $REPO/docker/analysis/prompt_geometry.py docker-analysis-1:/workspace/
docker exec -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml docker-analysis-1 \
  python3 /workspace/prompt_geometry.py score --profile frames
```

Expected: `[stamp] score: 전체 187,994 / 매핑 0 (0개 도메인) / GT 0` → `hard-skip (0단계)` — 예외 없이 정상 종료, FiftyOne 필드 미생성.

- [ ] **Step 3: 합성 매핑으로 실채점 검증** (실데이터 e2e — source-h 뱅크 npz는 이미 존재하므로 소형 프로젝트 하나를 임시 매핑):

```bash
docker exec docker-analysis-1 sh -c 'cat > /tmp/map_test.yaml <<EOF
crosswalk_version: 0
domains:
  _test:
    projects: [vanguardhealthcarevhc]
    bank_a: v1.0.8.0
    bank_b: v1.0.8.4
class_crosswalk: {__no_box_finalized__: normal}
unsupported_classes: [smoking]
EOF'
docker exec -e BANK_DOMAIN_MAP=/tmp/map_test.yaml docker-analysis-1 \
  python3 /workspace/frames_bank_ledger.py
docker exec -e BANK_DOMAIN_MAP=/tmp/map_test.yaml docker-analysis-1 \
  python3 /workspace/prompt_geometry.py score --profile frames
docker exec docker-analysis-1 python3 -c "
import fiftyone as fo
from fiftyone import ViewField as F
ds = fo.load_dataset('frames_captions')
n = len(ds.match(F('bank_domain') == '_test'))
assert n == 288, n
print('score e2e OK:', n, ds.info['bank_run']['run_id'])"
```

Expected: `score _test: n=288 ...` 로그 후 `score e2e OK: 288 score-...`

- [ ] **Step 4: clear-then-set 검증** (테스트 매핑 제거 후 재실행 → Step 3의 `_test` 필드가 비워지는가 — Step 1 코드가 clear를 hard-skip 판정보다 앞에 둔 이유):

```bash
docker exec -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml docker-analysis-1 \
  python3 /workspace/frames_bank_ledger.py
docker exec -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml docker-analysis-1 \
  python3 /workspace/prompt_geometry.py score --profile frames
docker exec docker-analysis-1 python3 -c "
import fiftyone as fo
from fiftyone import ViewField as F
ds = fo.load_dataset('frames_captions')
assert len(ds.match(F('bank_domain') != None)) == 0
print('clear-then-set OK (stale 없음)')"
```

Expected: `hard-skip (0단계)` 로그 후 `clear-then-set OK (stale 없음)`

- [ ] **Step 5: Commit**

```bash
git add docker/analysis/prompt_geometry.py
git commit -m "feat(analysis): frames score 스테이지 — 도메인 샤드 스트리밍 채점 + clear-then-set publish"
```

---

### Task 6: stage_gap_frames + stage_viz_frames — 공백지도·리뷰큐·시각화

**Files:**
- Modify: `docker/analysis/prompt_geometry.py` (Task 1 스텁 2개 교체)

**Interfaces:**
- Consumes: `GEO/<dom>_score.npz` (Task 5), `WORK/embed.npz`, `load_domain_map`, `WEAK_TO_BANK`
- Produces: 필드 `bank_gap`(int, 저확신 꼬리 군집); 뷰 `bank: <dom> review-queue`(select ordered), `bank: <dom> scored`, `bank: <dom> shifted`; brain `bank_margin_viz`(x=A margin, y=B margin — **확신도 비교, 정오 아님**); 워크스페이스 `bank-eval`; 사이드바 그룹 `⑥ 프롬프트뱅크` (`_sidebar_bank_group(ds)`).

- [ ] **Step 1: stage_gap_frames 구현**

```python
def stage_gap_frames() -> None:
    """도메인별 저확신 꼬리(margin 하위 10%) 군집 + 리뷰 큐(weak 불일치 × 저확신) 뷰."""
    from sklearn.cluster import KMeans

    import fiftyone as fo

    m = load_domain_map()
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    if not os.path.exists(f"{WORK}/embed.npz"):
        log("[stamp] gap: embed.npz 없음(매핑 0) → hard-skip")
        return
    z = np.load(f"{WORK}/embed.npz", allow_pickle=True)
    key2i = {str(k): i for i, k in enumerate(z["key"])}
    Xall = z["vec"].astype(np.float32)
    Xall /= np.linalg.norm(Xall, axis=1, keepdims=True)

    for dom in sorted(m["domains"]):
        sp = f"{GEO}/{dom}_score.npz"
        if not os.path.exists(sp):
            log(f"[stamp] gap {dom}: score 캐시 없음 → skip")
            continue
        sc = np.load(sp, allow_pickle=True)
        keys = [str(k) for k in sc["key"]]
        margin = sc["margin"]
        pred_b = sc["pred_b"]
        tail = np.flatnonzero(margin <= np.quantile(margin, 0.10))
        log(f"[stamp] gap {dom}: n={len(keys):,} / 저확신 꼬리 {len(tail)}")
        if len(tail) >= 40:
            k = max(2, min(6, len(tail) // 60))
            emb_idx = [key2i[keys[i]] for i in tail if keys[i] in key2i]
            km = KMeans(n_clusters=k, n_init=5, random_state=51).fit(Xall[emb_idx])
            ds.set_values("bank_gap",
                          {keys[i]: int(lab) for i, lab in zip(tail, km.labels_)},
                          key_field="id")
            log(f"gap {dom}: {k}군집 → bank_gap")
        # 리뷰 큐: 필드 추가 없이 ordered select 뷰 (스펙 §7 — LS 태스크 생성은 범위 밖)
        weak = ds.select(keys, ordered=True).values("normalized_class")

        def qkey(i: int) -> tuple:
            w = WEAK_TO_BANK.get(weak[i] or "")
            disagree = 1 if (w and CLASS_NAMES[int(pred_b[i])] != w) else 0
            return (-disagree, float(margin[i]))          # 불일치 우선, 저확신 오름차순

        order = sorted(range(len(keys)), key=qkey)[:500]
        qname = f"bank: {dom} review-queue"
        if qname in ds.list_saved_views():
            ds.delete_saved_view(qname)
        ds.save_view(qname, ds.select([keys[i] for i in order], ordered=True),
                     description="사람 검수 후보 — weak 불일치 × 저확신 (GT 축적 경로)")
        # report 상위 N 목록 + (선택) LS 반입용 원본 — 스펙 §7
        fps = ds.select([keys[i] for i in order], ordered=True).values("filepath")
        with open(f"{GEO}/{dom}_queue.json", "w", encoding="utf-8") as f:
            json.dump([{"key": keys[i], "filepath": fp, "margin": float(margin[i]),
                        "weak": weak[i], "pred": CLASS_NAMES[int(pred_b[i])]}
                       for i, fp in zip(order, fps)], f, ensure_ascii=False, indent=1)
        log(f"gap {dom}: 리뷰 큐 {len(order)} → 뷰 '{qname}' + {dom}_queue.json")
```

- [ ] **Step 2: stage_viz_frames + 사이드바 헬퍼 구현**

```python
def _sidebar_bank_group(ds) -> None:
    """기존 그룹 보존 + '⑥ 프롬프트뱅크' 그룹 append (멱등)."""
    import fiftyone as fo

    cur = ds.app_config.sidebar_groups
    if cur is None:
        cur = fo.DatasetAppConfig.default_sidebar_groups(ds)
    G = type(cur[0])
    universe = list(ds.get_field_schema(flat=True))
    paths = [p for p in BANK_FIELDS if p in universe]
    paths += [u for u in universe
              if any(u.startswith(p + ".") for p in paths) and u not in paths]
    groups = [g for g in cur if g.name != "⑥ 프롬프트뱅크"]
    for g in groups:
        g.paths = [p for p in g.paths if p not in paths]
    groups.append(G(name="⑥ 프롬프트뱅크", paths=paths, expanded=False))
    ds.app_config.sidebar_groups = groups
    ds.save()


def stage_viz_frames() -> None:
    """x=A margin, y=B margin 산점도(확신도 비교 — GT 아님) + 뷰/워크스페이스/사이드바."""
    import fiftyone as fo
    import fiftyone.brain as fob
    from fiftyone import ViewField as F

    m = load_domain_map()
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    scored = []
    for dom in sorted(m["domains"]):
        sp = f"{GEO}/{dom}_score.npz"
        if os.path.exists(sp):
            scored.append((dom, np.load(sp, allow_pickle=True)))
    if not scored:
        log("[stamp] viz: 채점 캐시 없음 → hard-skip")
        return
    keys = [str(k) for _, sc in scored for k in sc["key"]]
    ma = np.concatenate([sc["margin_a"] for _, sc in scored]).astype(np.float64)
    mb = np.concatenate([sc["margin"] for _, sc in scored]).astype(np.float64)

    bkey = "bank_margin_viz"
    if ds.has_brain_run(bkey):
        ds.delete_brain_run(bkey)
    fob.compute_visualization(ds.select(keys, ordered=True),   # ordered 필수 — points 정렬 일치
                              points=np.stack([ma, mb], axis=1), brain_key=bkey)

    for dom, _ in scored:
        for nm, view in ((f"bank: {dom} scored", ds.match(F("bank_domain") == dom)),
                         (f"bank: {dom} shifted",
                          ds.match(F("bank_domain") == dom)
                            .match(F("bank_shift.label") != "unchanged")
                            .sort_by("bank_decision_margin"))):
            if nm in ds.list_saved_views():
                ds.delete_saved_view(nm)
            ds.save_view(nm, view)

    ws = "bank-eval"                                # 워크스페이스명 ASCII (App slug 함정)
    space = fo.Space(children=[
        fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
        fo.Space(children=[fo.Panel(type="Embeddings",
                                    state={"brainResult": bkey,
                                           "colorByField": "bank_shift.label"})]),
    ], orientation="horizontal")
    if ws in ds.list_workspaces():
        ds.delete_workspace(ws)
    ds.save_workspace(ws, space,
                      description="x=A margin, y=B margin — 확신도 비교 (GT 정오 아님)")
    _sidebar_bank_group(ds)
    log(f"viz: brain {bkey} / 뷰 {2 * len(scored)}개 / 워크스페이스 {ws} / 사이드바 ⑥")
```

- [ ] **Step 3: 검증 — 캐시 없음 skip + 테스트 매핑 재사용 e2e**

```bash
# (a) 0단계: hard-skip
docker cp $REPO/docker/analysis/prompt_geometry.py docker-analysis-1:/workspace/
docker exec -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml docker-analysis-1 \
  python3 /workspace/prompt_geometry.py gap --profile frames
# (b) Task 5 Step 3 의 /tmp/map_test.yaml 로 ledger+score 재생성 후:
docker exec -e BANK_DOMAIN_MAP=/tmp/map_test.yaml docker-analysis-1 \
  sh -c "python3 /workspace/frames_bank_ledger.py && \
         python3 /workspace/prompt_geometry.py score --profile frames && \
         python3 /workspace/prompt_geometry.py gap --profile frames && \
         python3 /workspace/prompt_geometry.py viz --profile frames"
docker exec docker-analysis-1 python3 -c "
import fiftyone as fo
ds = fo.load_dataset('frames_captions')
assert 'bank: _test review-queue' in ds.list_saved_views()
assert ds.has_brain_run('bank_margin_viz') and 'bank-eval' in ds.list_workspaces()
print('gap+viz OK — 뷰/브레인/워크스페이스 생성됨')"
```

Expected: (a) `hard-skip`, (b) `gap+viz OK ...` + `http://10.0.0.10:5153/datasets/frames_captions`에서 워크스페이스 `bank-eval`·사이드바 `⑥ 프롬프트뱅크` 육안 확인.

- [ ] **Step 4: 테스트 흔적 정리 후 Commit** (정식 매핑으로 ledger+score 재실행 → clear-then-set이 `_test` 필드 제거, 뷰/브레인은 잔존하므로 명시 삭제):

```bash
docker exec -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml docker-analysis-1 \
  sh -c "python3 /workspace/frames_bank_ledger.py && \
         python3 /workspace/prompt_geometry.py score --profile frames"
docker exec docker-analysis-1 python3 -c "
import fiftyone as fo
ds = fo.load_dataset('frames_captions')
for v in [v for v in ds.list_saved_views() if v.startswith('bank: _test')]:
    ds.delete_saved_view(v)
print('테스트 뷰 정리 완료')"
git add docker/analysis/prompt_geometry.py
git commit -m "feat(analysis): frames gap/viz 스테이지 — 저확신 군집 + 리뷰큐 + margin 산점도"
```

---

### Task 7: stage_gtsync + stage_report_frames — GT 오버레이·min-n·리포트

**Files:**
- Modify: `docker/analysis/prompt_geometry.py` (Task 1 스텁 2개 교체)

**Interfaces:**
- Consumes: `GEO/<dom>_score.npz`, `WORK/ledger.jsonl`, `WORK/gt_snapshot.json`, `minn_tier`, `recalls`, `_append_run`
- Produces: 필드 `bank_gt`; `GEO/gt_eval_keys.jsonl`(교집합 델타용, `_append_gt_eval_keys`/`_last_gt_eval_keys`); `REPORT_DIR/bank_eval_report.md`.

- [ ] **Step 1: gtsync 구현**

```python
def _append_gt_eval_keys(run_id: str, domain: str, keys: list) -> None:
    with open(f"{GEO}/gt_eval_keys.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({"run_id": run_id, "domain": domain, "keys": keys}) + "\n")


def _last_gt_eval_keys() -> set:
    """직전 gtsync 가 평가에 쓴 GT 키 (도메인별 마지막 기록) — 교집합 델타의 기준."""
    path = f"{GEO}/gt_eval_keys.jsonl"
    if not os.path.exists(path):
        return set()
    last: dict[str, list] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            last[r["domain"]] = r["keys"]
    return {k for ks in last.values() for k in ks}


def stage_gtsync() -> None:
    """GT 오버레이 — 재채점 없이 캐시+원장으로 bank_gt/지표 갱신 (score_run 과 분리, 스펙 §8)."""
    import fiftyone as fo

    m = load_domain_map()
    rows = _load_frames_ledger()
    gt_by_key = {r["key"]: r["gt_class"] for r in rows if r.get("gt_class", -1) >= 0}
    snap = {}
    if os.path.exists(f"{WORK}/gt_snapshot.json"):
        snap = json.load(open(f"{WORK}/gt_snapshot.json", encoding="utf-8"))
    log(f"[stamp] gtsync: GT {len(gt_by_key)} (snapshot {snap.get('sha')}) / "
        f"crosswalk v{snap.get('crosswalk_version')}")
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    sch = ds.get_field_schema()
    if "bank_gt" in sch:
        ds.clear_sample_field("bank_gt")
    if gt_by_key:
        ds.set_values("bank_gt", {k: fo.Classification(label=CLASS_NAMES[c])
                                  for k, c in gt_by_key.items()}, key_field="id")

    run_id = f"gtsync-{time.strftime('%Y%m%d-%H%M%S')}"
    prev_keys = _last_gt_eval_keys()
    for dom in sorted(m["domains"]):
        sp = f"{GEO}/{dom}_score.npz"
        if not os.path.exists(sp):
            log(f"[stamp] gtsync {dom}: score 캐시 없음 → skip")
            continue
        sc = np.load(sp, allow_pickle=True)
        keys = [str(k) for k in sc["key"]]
        idx = [i for i, k in enumerate(keys) if k in gt_by_key]
        tier = minn_tier(len(idx))
        log(f"[stamp] gtsync {dom}: GT {len(idx)} / {len(keys):,} → tier={tier}")
        rec: dict = {"n_gt": len(idx), "tier": tier, "gt_snapshot": snap.get("sha")}
        if idx:
            gt = np.array([gt_by_key[keys[i]] for i in idx])
            if tier in ("exploratory", "reportable"):
                rec["recall_a"] = recalls(sc["pred_a"][idx], gt)
                rec["recall_b"] = recalls(sc["pred_b"][idx], gt)
                inter = [i for i in idx if keys[i] in prev_keys]
                if inter:                              # GT 성장 착시 차단 — 교집합 두 벌 보고
                    gti = np.array([gt_by_key[keys[i]] for i in inter])
                    rec["intersection_prev"] = {
                        "n": len(inter),
                        "micro_a": float((sc["pred_a"][inter] == gti).mean()),
                        "micro_b": float((sc["pred_b"][inter] == gti).mean()),
                    }
            else:                                      # counts_only — 백분율 표시 금지
                rec["counts"] = {"n": len(idx),
                                 "correct_b": int((sc["pred_b"][idx] == gt).sum())}
        _append_run(run_id, dom, **rec)
        _append_gt_eval_keys(run_id, dom, [keys[i] for i in idx])
    log(f"gtsync 완료: run={run_id}")
```

- [ ] **Step 2: report 구현**

```python
def stage_report_frames() -> None:
    rows = _load_frames_ledger()
    total = len(rows)
    by_dom = collections.Counter(r["domain"] for r in rows if r.get("domain"))
    n_gt = sum(1 for r in rows if r.get("gt_class", -1) >= 0)
    runs = []
    if os.path.exists(f"{GEO}/runs.jsonl"):
        with open(f"{GEO}/runs.jsonl", encoding="utf-8") as f:
            runs = [json.loads(x) for x in f if x.strip()]
    latest: dict[tuple, dict] = {}
    for r in runs:                                    # (종류, 도메인) 별 마지막 기록
        latest[(r["run_id"].split("-")[0], r["domain"])] = r

    L: list[str] = []
    A = L.append
    A("# frames_captions 프롬프트 뱅크 평가 리포트\n")
    A(f"- 생성: {time.strftime('%Y-%m-%d %H:%M')} | frame {total:,} (캡션 모달리티 제외)")
    A(f"- 커버리지: 뱅크 매핑 {sum(by_dom.values()):,}"
      f" ({dict(by_dom) if by_dom else '없음 — 0단계: bank_domain_map.yaml 시드 대기'})"
      f" / GT {n_gt} / 전체 {total:,}")
    A("- ⚠️ GT-free 축(pred/shift/margin)은 **확신도·변화**이지 정오가 아니다. "
      "recall 은 min-n tier(≥30) 통과 도메인만. concordance 는 SAM3 참고 신호(정확도 아님).\n")
    A("| 도메인 | 뱅크 A→B | 채점 n | 예측변화 | GT n | tier | recall B (micro) | 교집합 델타 |")
    A("|---|---|---|---|---|---|---|---|")
    for dom in sorted(by_dom):
        s = latest.get(("score", dom), {})
        g = latest.get(("gtsync", dom), {})
        rb = g.get("recall_b", {}).get("micro")
        ip = g.get("intersection_prev")
        rb_txt = f"{rb:.1%}" if rb is not None else "NA"
        ip_txt = f"n={ip['n']} B {ip['micro_b']:.1%}" if ip else "—"
        A(f"| {dom} | {s.get('bank_a', '?')}→{s.get('bank_b', '?')} "
          f"| {s.get('n_scored', 0):,} | {s.get('n_shift', 0):,} "
          f"| {g.get('n_gt', 0)} | {g.get('tier', 'no_gt')} | {rb_txt} | {ip_txt} |")
    if not by_dom:
        A("| — | — | 0 | — | 0 | no_gt | NA | — |")
    A("\n## 리뷰 큐 상위 (사람 검수 → GT 축적 경로)\n")
    for dom in sorted(by_dom):
        qp = f"{GEO}/{dom}_queue.json"
        if not os.path.exists(qp):
            continue
        q = json.load(open(qp, encoding="utf-8"))
        A(f"### {dom} (총 {len(q)}건 — 뷰 `bank: {dom} review-queue`)\n")
        for r in q[:5]:
            A(f"- `{os.path.basename(r['filepath'])}` margin={r['margin']:.4f} "
              f"weak={r['weak']} pred={r['pred']}")
        A("")
    A("\n## FiftyOne\n")
    A("- 워크스페이스 `bank-eval` (x=A margin, y=B margin — 확신도 비교)")
    A("- 뷰 `bank: <도메인> scored / shifted / review-queue` — 리뷰 큐가 GT 축적 경로다")
    A("- 사이드바 그룹 `⑥ 프롬프트뱅크`: " + ", ".join(BANK_FIELDS))
    os.makedirs(REPORT_DIR, exist_ok=True)
    out = f"{REPORT_DIR}/bank_eval_report.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log(f"report → {out}")
```

- [ ] **Step 3: 검증 (0단계 리포트)**

```bash
docker cp $REPO/docker/analysis/prompt_geometry.py docker-analysis-1:/workspace/
docker exec -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml docker-analysis-1 \
  sh -c "python3 /workspace/prompt_geometry.py gtsync --profile frames && \
         python3 /workspace/prompt_geometry.py report --profile frames && \
         cat /data/fiftyone/frames_bank/report/bank_eval_report.md"
```

Expected: `[stamp] gtsync: GT 0 ...` → 리포트에 `없음 — 0단계` + `no_gt`/`NA` 행 (0%가 아니라 NA — min-n 게이트 동작 증명)

- [ ] **Step 4: Commit**

```bash
git add docker/analysis/prompt_geometry.py
git commit -m "feat(analysis): frames gtsync/report — GT 오버레이 분리 + min-n 게이트 + 교집합 델타"
```

---

### Task 8: 원커맨드 래퍼 + README + 최종 e2e

**Files:**
- Create: `docker/analysis/frames_bank_eval.sh` (실행권한)
- Modify: `docker/analysis/README.md` (운영 절차 섹션 추가)

**Interfaces:**
- Consumes: Task 1~7 전부.
- Produces: `./docker/analysis/frames_bank_eval.sh [--bank <버전> <CSV>]` — 반입(4파일 명시 docker cp) → ledger → score → gap → viz → gtsync → report.

- [ ] **Step 1: 래퍼 작성**

```bash
#!/usr/bin/env bash
# frames_captions 프롬프트 뱅크 평가 — 원커맨드 (스펙 §5-3).
#
#   ./docker/analysis/frames_bank_eval.sh                           # 전체 사이클
#   ./docker/analysis/frames_bank_eval.sh --bank v1.0.9.0 /path/text_features_v1.0.9.0.csv
#
# 매핑(bank_domain_map.yaml)이 비어 있으면 채점은 hard-skip 되고 스탬프만 찍힌다 = 0단계 정상.
# GT 가 늘었을 때 재채점 없이 GT 만 갱신하려면: gtsync + report 두 스테이지만 재실행.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
C="${ANALYSIS_CONTAINER:-docker-analysis-1}"

# ambient /workspace 의존 금지 — 필요 파일 전부 명시 반입 (drift 차단, 스펙 §10 불채택 항목의 해소)
for f in prompt_geometry.py frames_bank_ledger.py bank_domain_map.yaml fiftyone_presentation.py; do
  docker cp "$REPO/docker/analysis/$f" "$C:/workspace/" >/dev/null
done

run() { docker exec -e OMP_NUM_THREADS=4 -e OPENBLAS_NUM_THREADS=4 \
        -e BANK_DOMAIN_MAP=/workspace/bank_domain_map.yaml "$C" python3 "$@"; }

if [[ "${1:-}" == "--bank" ]]; then
  VER="${2:?사용법: --bank <버전> <CSV경로>}"
  CSV="${3:?CSV 경로 필요}"
  docker cp "$CSV" "$C:/tmp/bank_new.csv"
  run /workspace/prompt_geometry.py bank --profile frames --csv /tmp/bank_new.csv --version "$VER"
fi

run /workspace/prompt_geometry.py selftest --profile frames
run /workspace/frames_bank_ledger.py
for st in score gap viz gtsync report; do
  run /workspace/prompt_geometry.py "$st" --profile frames
done

echo
echo "완료 — http://10.0.0.10:5153/datasets/frames_captions"
echo "  · 워크스페이스 bank-eval / 뷰 'bank: <도메인> …' / 사이드바 ⑥ 프롬프트뱅크"
echo "  · 리포트: docker exec $C cat /data/fiftyone/frames_bank/report/bank_eval_report.md"
```

- [ ] **Step 2: README 운영 절차 추가** — `docker/analysis/README.md`에 섹션 추가:

```markdown
## frames_captions 프롬프트 뱅크 평가 (frames_bank_eval.sh)

- 전체 사이클: `./docker/analysis/frames_bank_eval.sh` — 매핑이 비어 있으면 0단계(스탬프만)로
  정직하게 끝난다. 도메인을 열려면 `bank_domain_map.yaml` 의 `domains:` 를 노션
  "프롬프트 버전/관리 체계 구축" 페이지 기준으로 시드하고 뱅크 CSV 를 `--bank` 로 등록.
- GT(LS finalized)가 늘었을 때: 재채점 불필요 —
  `frames_bank_ledger.py` → `gtsync` → `report` 만 재실행 (래퍼 주석 참조).
- vanguardhealthcarevhc GT(patient/person)는 `class_crosswalk` 에 사상을 등재해야 GT 축에 편입된다.
- ⚠️ `slim` 스테이지는 source-h 전용(코드 가드 있음). frames_captions 의 필드 정리는 수동으로만.
- 산출: FiftyOne 필드 6개(bank_*), 뷰 `bank: <도메인> scored/shifted/review-queue`,
  워크스페이스 `bank-eval`, 리포트 `/data/fiftyone/frames_bank/report/bank_eval_report.md`,
  런 원장 `/data/fiftyone/frames_bank/work/geometry/runs.jsonl`.
```

- [ ] **Step 3: 최종 e2e (0단계 전체 사이클)**

```bash
chmod +x $REPO/docker/analysis/frames_bank_eval.sh
$REPO/docker/analysis/frames_bank_eval.sh
```

Expected: selftest OK → ledger 187,994 스탬프 → score/gap/viz `hard-skip (0단계)` → gtsync GT 0 → report 생성 → 완료 배너. 예외/트레이스백 0건.

- [ ] **Step 4: source-h 최종 회귀 (마지막 안전망)**

```bash
docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py selftest
docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py report
```

Expected: `selftest OK` + source-h 리포트 재생성 정상 (기존 geometry.json 소비 경로 무손상)

- [ ] **Step 5: Commit**

```bash
git add docker/analysis/frames_bank_eval.sh docker/analysis/README.md
git commit -m "feat(analysis): frames_bank_eval.sh 원커맨드 + 운영 README — 뱅크 평가 파이프라인 표준화"
```

---

## 이후 단계 (계획 밖 — 데이터 선행조건)

1. **도메인 시드** (사용자 협조): 노션 버전관리 페이지에서 frames_captions 실재 프로젝트(1차 후보 `fire_smoke` 3,464장)의 도메인 번호·버전쌍 확정 → `bank_domain_map.yaml` 기입 + `--bank`로 CSV 등록 → 래퍼 재실행 = 1단계 개통.
2. **vanguard GT 편입**: patient/person → frame class 사상 결정 후 crosswalk 등재 (`crosswalk_version` 증가) → ledger+gtsync 재실행.
3. `dev` 머지 시점에 `docker/analysis/`는 이미지 재빌드 트리거가 아니므로 배포 영향 없음 — 단 main push 자체는 dagster 재기동을 유발하므로 머지 타이밍은 라벨링 유휴 시간에.
