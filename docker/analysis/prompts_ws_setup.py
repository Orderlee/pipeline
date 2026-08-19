#!/usr/bin/env python3
"""`<X>-prompts` 데이터셋에 `compare` 워크스페이스를 **원본 그대로 복제**한다.

## 왜 필요한가

`sourcei-prompts` 에는 3분할 `compare` 워크스페이스가 있는데 `frames-prompts` 에는 없다
(2026-08-19 실측: sourcei-prompts = prompts/topk/wave/compare, frames-prompts =
prompts/topk/caption). 그래서 `frames-prompts` 를 열면 App 기본 화면(Samples 단독)이
뜨고, 문장·이미지 임베딩 패널을 손으로 붙여야 한다
(`user_default_workspace` 오퍼레이터가 찾는 이름이 정확히 `compare` 다).

`fiftyone_app_setup.py workspace-compare` 는 **코드로 구성을 재생성**하는 스크립트다
(`_compare_space()`). 이 스크립트는 반대로 **살아 있는 원본 워크스페이스를 읽어 그대로
미러링**한다 — 원본이 손으로 조정된 상태(분할 비율·active_child·패널 state)라도 그 값이
정본이기 때문이다. 둘은 상호 배타가 아니라 목적이 다르다.

## 실측한 원본 (sourcei-prompts / compare, 2026-08-19)

    Space(horizontal, sizes=[0.5, 0.5], active=좌)
      ├─ Space(vertical, sizes=[0.42, 0.58], active=Samples)
      │    ├─ Panel Samples            pinned=True   state={}
      │    └─ Panel sentence_embeddings              state={}
      └─ Space(active=image_embeddings)
           └─ Panel image_embeddings                 state={}

⚠️ 커스텀 패널 3종의 state 는 **전부 비어 있다** — `brainResult` 를 안 들고 있다.
   `sentence_embeddings` / `image_embeddings` 패널이 brain key 를 코드 상수
   (`BRAIN_KEY = "emb_viz"`)로 하드코딩하기 때문이다(gotchas §1: 네이티브 패널이 brain key
   를 데이터셋 간에 기억해 죽던 함정 회피). 따라서 워크스페이스 미러링에는 치환할
   brainResult 가 없다. 그래도 이 스크립트는 원본 state 에 `brainResult` 가 있으면
   **대상 데이터셋에 그 brain run 이 실재하는지 검사**하고, 없으면 `--brain-key` 로
   치환하거나 거부한다 (조용한 오참조 금지).

## emb_viz vs emb_viz_cap (frames-prompts 전용 결정)

`frames-prompts` 는 brain run 이 둘이다 (실측):

    emb_viz      603,318점 — 프롬프트 문장 행만
    emb_viz_cap  615,296점 — 문장 + **영상 캡션 11,978행** (`entity='caption'`)

`compare` 는 **문장 뱅크 ↔ 이미지** 비교 화면이므로 `emb_viz`(문장만)를 쓴다:
  · 캡션 행은 `gidx`/`bank_version` 이 NULL 이라 prompt DB 조인 대상이 아니고
    (호버 문장·채택 여부·purity 등 문장 축이 전부 비어 모집단이 섞인다),
  · sourcei-prompts 와 같은 모집단이라야 두 데이터셋의 화면을 나란히 읽을 수 있다.
캡션까지 보고 싶으면 기존 `caption` 워크스페이스를 쓰면 된다 (그쪽이 `emb_viz_cap` 용도).
현재 패널 상수가 `emb_viz` 고정이므로 이 선택은 워크스페이스가 아니라 **패널이** 강제한다.

## 사용

    python3 prompts_ws_setup.py                          # 전 <X>-prompts 대상 dry-run
    python3 prompts_ws_setup.py frames-prompts --apply   # 실제 저장
    python3 prompts_ws_setup.py frames-prompts --source sourcei-prompts --name compare
    python3 prompts_ws_setup.py --selftest               # fiftyone 없이 도는 순수부

컨테이너에서:
    docker cp docker/analysis/prompts_ws_setup.py docker-analysis-fiftyone-1:/workspace/
    docker exec docker-analysis-fiftyone-1 python /workspace/prompts_ws_setup.py frames-prompts

정본: docker/analysis/prompts_ws_setup.py
"""
from __future__ import annotations

import argparse
import sys

SOURCE_DATASET = "sourcei-prompts"
WORKSPACE = "compare"
PROMPTS_SUFFIX = "-prompts"
# 이 패널들은 brain key 를 코드에 하드코딩한다 — 워크스페이스 state 로 바꿀 수 없다.
CODE_PINNED_PANELS = {"sentence_embeddings": "emb_viz", "image_embeddings": "emb_viz"}


def log(m: str) -> None:
    print(m, flush=True)


# ── Space 트리 (fiftyone 없이도 도는 순수부) ─────────────────────────────────

def is_panel(node) -> bool:
    return not hasattr(node, "children")


def signature(node):
    """component_id(랜덤 uuid)를 뺀 구조 서명. **멱등 판정의 유일한 기준**이다.

    active_child 는 id 라 그대로 비교할 수 없다 → "몇 번째 자식이 active 인가"로 바꾼다.
    """
    if is_panel(node):
        return ("panel", node.type, bool(getattr(node, "pinned", False)),
                tuple(sorted((dict(getattr(node, "state", None) or {})).items())))
    kids = list(node.children)
    active = getattr(node, "active_child", None)
    return ("space", getattr(node, "orientation", None),
            tuple(round(float(x), 4) for x in (getattr(node, "sizes", None) or [])),
            next((i for i, c in enumerate(kids) if c.component_id == active), None),
            tuple(signature(c) for c in kids))


def describe(node, depth: int = 0) -> list[str]:
    pad = "  " * depth
    if is_panel(node):
        state = dict(getattr(node, "state", None) or {})
        pin = " pinned" if getattr(node, "pinned", False) else ""
        return [f"{pad}Panel {node.type}{pin}" + (f" state={state}" if state else "")]
    sizes = getattr(node, "sizes", None)
    head = f"{pad}Space {getattr(node, 'orientation', None)}" + (f" sizes={sizes}" if sizes else "")
    out = [head]
    for c in node.children:
        out += describe(c, depth + 1)
    return out


def panel_types(node) -> list[str]:
    if is_panel(node):
        return [node.type]
    out = []
    for c in node.children:
        out += panel_types(c)
    return out


def clone_space(fo, node, state_fix=None):
    """원본 트리를 **새 component_id 로** 복제. `state_fix(type, state) -> state` 로 치환.

    ⚠️ 리프 Panel 을 낱개 Space 로 감싸지 않는다 — 감싼 구조는 App resizeViews 가
    `reading 'minimumSize'` TypeError 로 죽어 화면이 통째로 빈다
    (fiftyone_app_setup._compare_space 의 Task 11 실측과 같은 계약).
    active_child 는 None 이면 안 된다 — 플러그인 패널의 on_load 가 발화하지 않아
    "클릭해야 나온다" 증상이 난다.
    """
    if is_panel(node):
        state = dict(getattr(node, "state", None) or {})
        if state_fix is not None:
            state = state_fix(node.type, state)
        return fo.Panel(type=node.type, pinned=bool(getattr(node, "pinned", False)),
                        state=state or None)
    kids = [clone_space(fo, c, state_fix) for c in node.children]
    active = getattr(node, "active_child", None)
    pos = next((i for i, c in enumerate(node.children) if c.component_id == active), 0)
    space = fo.Space(children=kids, orientation=getattr(node, "orientation", None),
                     active_child=kids[pos].component_id if kids else None)
    sizes = getattr(node, "sizes", None)
    if sizes:
        space.sizes = list(sizes)
    return space


# ── 대상 데이터셋 검사 ───────────────────────────────────────────────────────

def preflight(fo, name: str, types_used: list[str]) -> list[str]:
    """이 데이터셋에 워크스페이스를 얹어도 되는지. 반환 = 경고 목록 (빈 리스트면 정상).

    거부가 아니라 경고다 — 워크스페이스 저장 자체는 항상 가능하고, 여기서 잡는 건
    "저장은 됐는데 열면 빈 패널" 이 되는 조건이다.
    """
    ds = fo.load_dataset(name)
    warns = []
    runs = ds.list_brain_runs()
    for ptype, key in CODE_PINNED_PANELS.items():
        if ptype not in types_used:
            continue
        if ptype == "image_embeddings":
            # 이미지 패널은 **프레임 데이터셋**(`<name>` 에서 -prompts 제거)의 좌표를 그린다.
            frames = name[: -len(PROMPTS_SUFFIX)] if name.endswith(PROMPTS_SUFFIX) else name
            if not fo.dataset_exists(frames):
                warns.append(f"{ptype}: 프레임 데이터셋 `{frames}` 없음 — 우측 패널이 빈다")
            elif key not in fo.load_dataset(frames).list_brain_runs():
                warns.append(f"{ptype}: `{frames}` 에 brain run `{key}` 없음 — 우측 패널이 빈다")
        elif key not in runs:
            warns.append(f"{ptype}: `{name}` 에 brain run `{key}` 없음 — 좌하 패널이 빈다")
    if "sentence_embeddings" in types_used and "gidx" not in ds.get_field_schema():
        warns.append("sentence_embeddings: `gidx` 필드 없음 — 호버 문장 DB 조인 불가")
    return warns


def make_state_fix(fo, target_name: str, brain_key: str | None):
    """원본 패널 state 의 `brainResult` 를 대상 데이터셋에 맞게 검증/치환하는 함수.

    커스텀 패널 3종은 state 가 비어 있어 실제로는 아무것도 안 한다 — 원본에 네이티브
    `Embeddings` 패널이 섞여 들어올 때를 위한 안전망이다 (조용한 오참조 금지).
    """
    runs = set(fo.load_dataset(target_name).list_brain_runs())

    def fix(ptype, state):
        old = state.get("brainResult")
        if old is None:
            return state
        if brain_key:
            state = {**state, "brainResult": brain_key}
        elif old not in runs:
            raise SystemExit(
                f"❌ {ptype}: 원본의 brainResult=`{old}` 가 `{target_name}` 에 없습니다 "
                f"(있는 것: {sorted(runs)}). `--brain-key <키>` 로 치환하세요.")
        return state

    return fix


# ── 메인 ─────────────────────────────────────────────────────────────────────

def targets(fo, names: list[str] | None, source: str) -> list[str]:
    if names:
        return names
    return sorted(n for n in fo.list_datasets()
                  if n.endswith(PROMPTS_SUFFIX) and n != source)


def run(names, source, ws_name, brain_key, apply_) -> int:
    import fiftyone as fo

    if not fo.dataset_exists(source):
        log(f"❌ 원본 데이터셋 없음: {source}")
        return 1
    src_ds = fo.load_dataset(source)
    if ws_name not in src_ds.list_workspaces():
        log(f"❌ `{source}` 에 워크스페이스 `{ws_name}` 없음 (있는 것: {src_ds.list_workspaces()})")
        return 1
    src = src_ds.load_workspace(ws_name)
    src_desc = (src_ds.get_workspace_info(ws_name) or {}).get("description")
    src_sig = signature(src)
    used = panel_types(src)
    log(f"원본 {source}/{ws_name} — 패널 {used}")
    for line in describe(src):
        log("  " + line)
    if src_desc:
        log(f"  description: {src_desc}")
    log("")

    rc = 0
    for name in targets(fo, names, source):
        if not fo.dataset_exists(name):
            log(f"skip (데이터셋 없음): {name}")
            continue
        ds = fo.load_dataset(name)
        if ws_name in ds.list_workspaces() and signature(ds.load_workspace(ws_name)) == src_sig:
            log(f"{name}: 이미 동일한 `{ws_name}` — 변경 없음 (멱등)")
            continue
        for w in preflight(fo, name, used):
            log(f"  ⚠️ {name}: {w}")
        space = clone_space(fo, src, make_state_fix(fo, name, brain_key))
        assert signature(space) == src_sig, f"{name}: 복제 서명이 원본과 다르다"
        verb = "덮어씀" if ws_name in ds.list_workspaces() else "새로 만듦"
        if not apply_:
            log(f"{name}: `{ws_name}` {verb} 예정 (dry-run — 실제 저장은 `--apply`)")
            rc = max(rc, 2)
            continue
        ds.save_workspace(ws_name, space, description=src_desc, overwrite=True)
        saved = ds.load_workspace(ws_name)
        assert signature(saved) == src_sig, f"{name}: 저장 왕복 서명 불일치"
        log(f"{name}: `{ws_name}` {verb} ✅")
    return rc


def selftest() -> int:
    """fiftyone 없이 도는 순수부 — 서명·복제 계약."""

    class P:
        def __init__(self, type, pinned=False, state=None, cid=None):
            self.type, self.pinned, self.state = type, pinned, state
            self.component_id = cid or f"p-{type}-{id(self)}"

    class S:
        def __init__(self, children, orientation=None, sizes=None, active_child=None, cid=None):
            self.children, self.orientation, self.sizes = children, orientation, sizes
            self.active_child = active_child
            self.component_id = cid or f"s-{id(self)}"

    class FakeFO:
        Panel = staticmethod(lambda type, pinned=False, state=None: P(type, pinned, state))

        @staticmethod
        def Space(children, orientation=None, active_child=None):
            return S(children, orientation, None, active_child)

    samples = P("Samples", pinned=True)
    sent = P("sentence_embeddings")
    left = S([samples, sent], "vertical", [0.42, 0.58], samples.component_id)
    img = P("image_embeddings")
    right = S([img], None, None, img.component_id)
    root = S([left, right], "horizontal", [0.5, 0.5], left.component_id)

    assert panel_types(root) == ["Samples", "sentence_embeddings", "image_embeddings"]
    assert "Space horizontal sizes=[0.5, 0.5]" in describe(root)[0]

    # 복제는 **서명 동일 + component_id 전부 새것** 이어야 한다 (멱등 판정의 전제)
    copy = clone_space(FakeFO, root)
    assert signature(copy) == signature(root)
    old_ids = {root.component_id, left.component_id, right.component_id,
               samples.component_id, sent.component_id, img.component_id}
    new_ids = {copy.component_id, copy.children[0].component_id,
               copy.children[1].component_id,
               copy.children[0].children[0].component_id,
               copy.children[0].children[1].component_id,
               copy.children[1].children[0].component_id}
    assert not (old_ids & new_ids), "component_id 가 재사용됐다"
    # active_child 는 **새 id** 를 가리켜야 한다 (원본 id 를 남기면 App 이 활성 패널을 못 찾는다)
    assert copy.active_child == copy.children[0].component_id
    assert copy.children[0].active_child == copy.children[0].children[0].component_id
    assert copy.children[0].sizes == [0.42, 0.58] and copy.children[0].children[0].pinned

    # 구조가 달라지면 서명이 달라야 한다 (= 멱등 스킵이 잘못 걸리지 않는다)
    swapped = S([S([sent, samples], "vertical", [0.42, 0.58], sent.component_id), right],
                "horizontal", [0.5, 0.5], None)
    assert signature(swapped) != signature(root)
    resized = S([S([samples, sent], "vertical", [0.5, 0.5], samples.component_id), right],
                "horizontal", [0.5, 0.5], left.component_id)
    assert signature(resized) != signature(root)
    # state 차이도 잡아야 한다 (brainResult 치환이 조용히 묻히면 안 된다)
    assert signature(P("Embeddings", state={"brainResult": "emb_viz"})) != \
        signature(P("Embeddings", state={"brainResult": "emb_viz_cap"}))

    # state_fix 가 적용된 복제
    native = S([P("Embeddings", state={"brainResult": "emb_viz"})], None, None, None)
    fixed = clone_space(FakeFO, native, lambda t, s: {**s, "brainResult": "emb_viz_cap"})
    assert fixed.children[0].state == {"brainResult": "emb_viz_cap"}

    # 커스텀 패널은 brain key 를 코드에 박고 있다 — 이 표가 문서와 어긋나면 안 된다
    assert CODE_PINNED_PANELS["sentence_embeddings"] == "emb_viz"
    assert CODE_PINNED_PANELS["image_embeddings"] == "emb_viz"
    log("selftest OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("datasets", nargs="*",
                    help="대상 데이터셋. 생략하면 원본을 뺀 전 `<X>-prompts`")
    ap.add_argument("--source", default=SOURCE_DATASET, help="복제 원본 데이터셋")
    ap.add_argument("--name", default=WORKSPACE, help="워크스페이스 이름")
    ap.add_argument("--brain-key", default=None,
                    help="원본 state 의 brainResult 를 이 키로 치환 (커스텀 패널엔 무의미)")
    ap.add_argument("--apply", action="store_true", help="실제 저장 (기본 dry-run)")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        return selftest()
    return run(a.datasets, a.source, a.name, a.brain_key, a.apply)


if __name__ == "__main__":
    sys.exit(main())
