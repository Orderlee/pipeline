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
    "smoke":    "#56B4E9",  # sky blue — 회색(#7F7F7F)은 미채택 회색과 안 구분(2026-08-10 피드백)
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


def cmd_workspace():
    """sourcei에 'rules' 워크스페이스: Samples | Embeddings(emb_viz, Color by=rule_cross).

    rule_cross = argmax_k1/dist_iou 두 규칙이 갈리는 프레임 표식 (이미 존재하는 필드).
    """
    import fiftyone as fo
    ds = fo.load_dataset("sourcei")
    assert "rule_cross" in ds.get_field_schema(), "rule_cross 필드가 없다 — 스펙 §3 확인"
    # Panel을 Space로 한 번 더 감싸지 않는다 + active_child 필수 — Task 11 실측:
    # 감싼 구조는 App resizeViews TypeError로 빈 화면, active_child=None은 패널 활성 누락.
    samples = fo.Panel(type="Samples", pinned=True)
    embeddings = fo.Panel(
        type="Embeddings",
        state=dict(brainResult="emb_viz", colorByField="rule_cross"),
    )
    space = fo.Space(children=[samples, embeddings], orientation="horizontal",
                     active_child=samples.component_id)
    ds.save_workspace("rules", space,
                      description="argmax_k1 vs dist_iou 불일치 프레임", overwrite=True)
    assert "rules" in ds.list_workspaces()
    print("workspace 'rules' 저장 완료")


def cmd_workspace_compare():
    """H1 확정안 v2 (Task 11 실사용 피드백): 좌우 반반 — 좌=Samples/Embeddings 세로 스택, 우=Prompt Compare.

    v1(3-패널 가로 나열, Task 10)은 각 패널이 너무 좁다는 실사용 피드백으로 교체.
    ``sizes=[0.5, 0.5]``는 두 최상위 children(좌 스택 vs 우 패널) 사이의 폭 배분이다 — 좌 스택
    내부 리프가 2개(Samples/Embeddings)라도 바깥쪽 분할은 항상 이 outer Space의 children 수(2)
    기준이라 3-way 균등분할로 새지 않는다(fiftyone.core.odm.workspace.Space 문서 확인, "the
    ordered list of relative sizes for children of a space").
    """
    import fiftyone as fo
    ds = fo.load_dataset("sourcei")
    # 리프 Panel을 곧바로 children으로 둔다 (Panel을 낱개 Space로 한 번 더 감싸지 않음) —
    # 실측: outer(horizontal) > left(vertical) > Space > Panel 로 3단 중첩하면 App 번들의
    # 리사이즈 로직(recharts 기반, Ne$2.resizeViews)이 "Cannot read properties of undefined
    # (reading 'minimumSize')"로 크래시한다. Space.children은 "전부 같은 타입"만 허용하므로
    # (fiftyone.core.odm.workspace._validate_children) Panel 2개를 형제로 바로 두는 편이
    # 검증도 더 단순히 통과하고, default_workspace_factory()의 기본 패턴(Panel을 Space로
    # 안 감쌈)과도 일치한다.
    samples_panel = fo.Panel(type="Samples", pinned=True)
    embeddings_panel = fo.Panel(type="Embeddings", state=dict(brainResult="emb_viz"))
    prompt_compare_panel = fo.Panel(type="user_prompt_compare")
    left_stack = fo.Space(
        children=[samples_panel, embeddings_panel],
        orientation="vertical",
        active_child=samples_panel.component_id,
    )
    # active_child (Task 11 근본 원인, 실측 확정): 이걸 안 채우면 Space.active_child가
    # 기본값 None으로 저장된다. 이 상태의 워크스페이스를 로드하면 Samples/Embeddings(네이티브
    # 패널, on_load 오퍼레이터가 아예 없음)는 항상 즉시 렌더되지만, user_prompt_compare(플러그인
    # 패널)는 자기 on_load 오퍼레이터가 **한 번도 실행되지 않는다** — 네트워크 로그로 확인:
    # load_workspace 호출 이후 `/operators/execute`가 전혀 나가지 않다가, 사용자가 그 패널
    # 탭을 클릭한 순간에야 첫 execute가 발생한다("클릭해야 나온다"의 실제 정체). 즉 원인은
    # data/layout 렌더 경합이 아니라 **아예 on_load 트리거 누락**이었다 — active_child로
    # 각 Space가 자신의 리프를 "이미 활성"이라 표시해두면 워크스페이스 로드 즉시 on_load가
    # 발화한다(App 재기동 후 새 세션 3회 연속 실측 확인, task-11-report.md 참고).
    right_pane = fo.Space(children=[prompt_compare_panel],
                          active_child=prompt_compare_panel.component_id)
    space = fo.Space(
        children=[left_stack, right_pane],
        orientation="horizontal",
        sizes=[0.5, 0.5],
        active_child=left_stack.component_id,
    )
    ds.save_workspace("compare", space,
                      description="프레임↔문장 비교 (spec 2026-08-07 H1, v2 반반 스택)",
                      overwrite=True)
    assert "compare" in ds.list_workspaces()
    print("workspace 'compare' 저장 완료 (반반 스택)")


def _normalize_space(node, fo):
    """워크스페이스 Space 트리 정규화. (새 노드, changed) 반환.

    깨진 레거시 워크스페이스 일괄 수리 (Task 11 실측 근거 — task-11-report.md):
    1. 자식이 전부 '단일 Panel만 감싼 Space'면 Panel을 형제로 직접 둔다 —
       래핑 중첩은 App 번들 resizeViews가 TypeError로 죽어 화면 전체가 빈다.
    2. active_child=None 이면 첫 자식으로 채운다 — 플러그인 패널 on_load 미발화 방지.
    component_id를 보존해 두 번 돌리면 '변경 없음'이 되도록(멱등) 한다.
    """
    if not hasattr(node, "children"):        # Panel 리프
        return node, False
    changed = False
    kids = []
    for c in node.children:
        nc, ch = _normalize_space(c, fo)
        kids.append(nc)
        changed |= ch
    if kids and all(hasattr(k, "children") and len(k.children) == 1
                    and not hasattr(k.children[0], "children") for k in kids):
        kids = [k.children[0] for k in kids]  # 래퍼 Space 제거, Panel 직접 배치
        changed = True
    new = fo.Space(children=kids, orientation=node.orientation)
    new.component_id = node.component_id      # 보존 — active_child 참조/멱등성 유지
    if getattr(node, "sizes", None):
        new.sizes = node.sizes
    ids = [k.component_id for k in kids]
    ac = node.active_child if node.active_child in ids else (ids[0] if ids else None)
    changed |= (ac != node.active_child)
    new.active_child = ac
    return new, changed


def cmd_workspace_fix(dataset_names=None):
    """전 데이터셋의 저장된 워크스페이스를 일괄 정규화 (기본: 워크스페이스 가진 전부).

    실측(2026-08-07): compare 를 제외한 20개 워크스페이스 전부가 Space>Panel 래핑 +
    active_child=None 레거시 구조라 로드 시 빈 화면이었다. 멱등 — 재실행 시 '변경 없음'.
    """
    import fiftyone as fo
    names = dataset_names or fo.list_datasets()
    for name in names:
        if not fo.dataset_exists(name):
            print(f"skip (없음): {name}")
            continue
        ds = fo.load_dataset(name)
        for w in ds.list_workspaces():
            space = ds.load_workspace(w)
            new, changed = _normalize_space(space, fo)
            if not changed:
                print(f"{name}/{w}: 변경 없음")
                continue
            try:
                info = ds.get_workspace_info(w) or {}
            except Exception:
                info = {}
            ds.save_workspace(w, new, description=info.get("description"),
                              color=info.get("color"), overwrite=True)
            print(f"{name}/{w}: 정규화 저장 (평탄화/active_child)")


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
    args = sys.argv[1:]
    USAGE = "usage: fiftyone_app_setup.py selftest|colors|workspace|workspace-compare|workspace-fix"
    if not args:
        raise SystemExit(USAGE)
    elif args[0] == "selftest":
        _selftest()
    elif args[0] == "colors":
        names = args[1].split(",") if len(args) > 1 else DEFAULT_DATASETS
        cmd_colors(names)
    elif args[0] == "workspace":
        cmd_workspace()
    elif args[0] == "workspace-compare":
        cmd_workspace_compare()
    elif args[0] == "workspace-fix":
        cmd_workspace_fix(args[1].split(",") if len(args) > 1 else None)
    else:
        raise SystemExit(USAGE)
