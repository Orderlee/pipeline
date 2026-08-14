"""FiftyOne App 설정 정본화 — 색상 스킴(R3) + 워크스페이스 + 분석 필터 세트.

정본: docker/analysis/fiftyone_app_setup.py (git). 컨테이너 실행:
  docker cp docker/analysis/fiftyone_app_setup.py docker-analysis-1:/workspace/
  docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py colors
설계 근거: docs/superpowers/specs/2026-08-07-viz-curation-platform-design.md §4 0-1
         docs/superpowers/specs/2026-08-14-fiftyone-bank-filter-schema-design.md §3~§5

서브커맨드:
  selftest / colors / workspace / workspace-compare / workspace-fix   (기존)
  dump <ds>              app_config 스냅샷 → JSON (M0 롤백 아티팩트)
  restore <file>         덤프 되돌리기
  slots <ds> [--apply]   규칙별 예측 슬롯 `pred_<rule>_<a|b>` 생성 — **유일하게 필드를 쓴다**
  filters <ds> [--apply] 사이드바 그룹 + active_fields + 저장뷰 생성 (설정 전용, dry-run 기본)

⚠️ `filters` 는 **공유 호스트의 App 화면을 바꾼다**. 다른 세션이 같은 데이터셋을 보고
   있을 수 있으므로 `--apply` 전에 고지하고, `dump` 를 먼저 받아둘 것.
"""
import re
import sys
import time

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
# 클래스 값을 담는 필드 후보 중 **버전과 무관한** 것. 버전 접미 필드는 아래
# class_field_candidates() 가 bank_run.slots 에서 파생한다 (구: pred_v1_0_8_0 /
# wave_pred_v1_0_8_0 하드코딩 — 비교쌍이 바뀔 때마다 손으로 고쳐야 했다. 스펙 §1-4 5번).
CLASS_FIELD_BASE = ["ground_truth", "category", "event_kind", "attached_bank"]

# ── 분석 필터 세트 (스펙 §4-5) ───────────────────────────────────────────────
# `{A}`/`{B}` = `ds.info["bank_run"]["slots"]` 의 비교쌍. 실행 시 스키마에서 해석한다.
# 선정 근거는 스펙 §4-4 의 분별력 실측 — 상수·전량 null·한 통 쏠림(top1 ≥ 90%) 필드는
# 여기 없다(`rule_cross` 99.9% / `environment` 98.1% / `weather` 98.1% / `sam3*` null 100%).
# SAM3 축(`sam3_hit`/`sam3_n`)은 **다른 모델의 의견**이라 판정축에서 뺐다 (스펙 §4-3).
DEFAULT_SLOTS = ("v1.0.8.0", "v1.0.8.4")
# 규칙별 프레임 예측 슬롯 — 버전이 이름에 없고, 어느 뱅크인지는 `bank_run.slots` 와
# 사이드바 그룹 라벨이 말한다. `slots` 서브커맨드가 만든다.
#   pred_wave_*   분포 IoU (제품 판정 규칙)      ← wave_pred_<vt> 복사
#   pred_topk_*   top-K 다수결                  ← vote_<vt> 복사
#   pred_argmax_* argmax (K=1)                  ← winner_gidx_<vtag> → 문장 클래스 조인
# argmax 를 조인으로 유도하는 이유: `pred_<vt>` 는 v1.0.8.0 에만 존재해 버전 비교가
# 불가능했다. `winner_gidx_<vtag>` 는 29버전 전부 있고, 그 gidx 가 가리키는 문장의
# `category` 가 곧 argmax 예측이다 — 재계산 없이 조인만으로 복원된다.
# 검증(2026-08-14): v1.0.8.0 유도값이 기존 `pred_v1_0_8_0` 과 7,498/7,498 완전 일치.
PRED_SLOT_SOURCES = {"wave": "wave_pred_{v}", "topk": "vote_{v}", "argmax": None}
RULE_LABELS = {"wave": "분포 IoU (제품 판정 규칙)", "topk": "top-K 다수결",
               "argmax": "argmax (K=1)"}
FILTER_GROUPS = [
    # 그룹명에는 버전을 넣지 않는다 (2026-08-14 사용자 결정) — 버전 확인·교체는
    # `@user/bank-slots` 오퍼레이터가 담당하고, 사이드바에서의 출처 표시는 각 슬롯 필드의
    # **description**(`pred_wave_a` → "… · 뱅크 v1.0.8.0")이 맡는다.
    ("① 판정", True, [
        "ground_truth",
        "pred_wave_a", "pred_wave_b",
        "pred_argmax_a", "pred_argmax_b",
        "pred_topk_a", "pred_topk_b",
        "runner_up"]),
    ("② 층화", True, [
        "camera", "daynight", "person_count_bin", "source_unit", "src_video"]),
    ("③ 이벤트 맥락", True, [
        "event_kind", "event_index", "frame_in_event"]),
    ("④ 근거·심각도", False, [
        "close_call", "winner_ablate_role", "wave_gain",
        "wave_iou_falldown_{A}", "wave_iou_fire_{A}", "wave_iou_smoke_{A}",
        "wave_iou_falldown_{B}", "wave_iou_fire_{B}", "wave_iou_smoke_{B}",
        "wave_vs_topk_{A}", "wave_vs_topk_{B}"]),
]
# 나머지는 **접힌** 그룹 두 개로 몰아넣는다. FiftyOne 은 어느 그룹에도 없는 필드를 그냥
# 렌더하므로(그룹 미지정 = 숨김 아님), 루트 필드는 전부 명시적으로 배치해야 화면이 정리된다.
NOISE_GROUP = "⑨ 버전별 원자료 (도구 전용)"
OTHER_GROUP = "⑧ 기타"
# 덤프 그룹(⑨/⑧)에서는 서브경로를 `.label` 만 남긴다 — 사람이 값으로 필터하는 유일한
# 축이고, 나머지(`.confidence` 등)는 행 수만 늘린다. 큐레이션 그룹은 이 제한을 받지 않는다
# (2026-08-14 사용자 피드백: "⑨ 가 너무 난잡하다, confidence/id/label/tags 가 다 보인다").
DUMP_SUBPATH_KEEP = ("label",)
# `active_fields` 에 추가로 넣을 축 — 자주 Color-by 로 토글하지만 워크스페이스에는
# 안 박혀 있는 것. 여기 없는 필드로 색칠하려면 App 사이드바에서 켜면 된다.
# ⚠️ 숫자 필드는 넣지 말 것 — 카드마다 값이 찍혀 그리드가 난잡해진다 (2026-08-14 피드백).
ACTIVE_EXTRA = ("camera",)
# ⚠️ 위 FILTER_GROUPS 는 **sourcei 실측**으로 고른 목록이다. 분별력은 데이터의 성질이지
#    코드의 성질이 아니다 — `daynight` 은 sourcei 에선 night 27% 로 살아있지만 source-h 은
#    야간 프레임이 0장이라 죽은 축이고, 반대로 source-h 의 워크플로(flip 검수·사분면·프롬프트
#    품질·gap)에 필요한 축은 여기 없다. 다른 데이터셋에 그대로 쓰면 **틀린 화면**이 된다.
#    새 데이터셋은 스펙 §4-4 의 분별력 측정을 먼저 돌리고 목록을 다시 고를 것 (스펙 §6).
CURATED_DATASETS = {"sourcei"}
# 버전 접미사 — 세 세대 모두. `prompt_scores_export.suffixes()` 와 같은 규칙 (스펙 §3 D7).
VER_RE = re.compile(r"_(v\d+(?:_\d+)*(?:-[\w.]+)?)(?=$|\.)")
# `exclude_fields` 가 거부하는 FiftyOne 기본 필드 — 제외 목록에서 빼야 한다.
FO_DEFAULT_FIELDS = ("id", "filepath", "tags", "metadata",
                     "created_at", "last_modified_at")


def _resolve(schema, template, version):
    """버전 + 템플릿 → 스키마에 실존하는 필드명. 없으면 None.

    태그 해석은 `prompt_scores_export` 에 위임한다 — 규칙이 세 세대(vt/vtag/구)로 갈려
    있고, 사본을 또 만들면 그게 정확히 2026-08-14 사고의 재발이다 (스펙 §3 D7-1).
    `bank_tags_contract.py` 가 그 위임처를 감시한다.
    """
    from prompt_scores_export import resolve as pse_resolve
    return pse_resolve(schema, template, version)


def read_slots(ds):
    """비교쌍 = `ds.info["bank_run"]["slots"]`. 없으면 기본값."""
    br = (ds.info or {}).get("bank_run") or {}
    slots = br.get("slots") or {}
    a, b = slots.get("a"), slots.get("b")
    return (a, b) if a and b else DEFAULT_SLOTS


def class_field_candidates(ds):
    """색상 스킴 대상 필드 — 버전 접미 필드는 비교쌍에서 파생 (M3)."""
    schema = ds.get_field_schema()
    out = [f for f in CLASS_FIELD_BASE if f in schema]
    for ver in read_slots(ds):
        for tmpl in ("pred_{v}", "wave_pred_{v}"):
            f = _resolve(schema, tmpl, ver)
            if f and f not in out:
                out.append(f)
    return out


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
    entries = [e for e in (_field_entry(ds, p) for p in class_field_candidates(ds)) if e]
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


def _compare_space(fo, ds):
    """데이터셋별 'compare' Space 구성. (space, description) 반환.

    - 프롬프트 짝("<name>-prompts")이 있는 프레임 데이터셋(sourcei·source-h):
      H1 확정안 v2 — 좌우 반반, 좌=Samples/Embeddings 세로 스택, 우=Prompt Compare.
      ``sizes=[0.5, 0.5]``는 두 최상위 children 사이의 폭 배분 — 좌 스택 내부 리프가
      2개라도 바깥 분할은 outer children 수(2) 기준이라 3-way 로 새지 않는다.
    - "-prompts" 데이터셋·짝 없는 데이터셋(frames_captions 등): **같은 3분할**에서
      우측 Prompt Compare 자리만 **`user_image_embeddings` 패널**로 교체
      (2026-08-14 사용자 요청 4 + "3분할이 아닌데?" + "prompt compare 처럼 플러그인
      하면 되잖아" 피드백). 네이티브 Embeddings 패널을 그 자리에 넣지 않는 이유는
      플러그인 docstring 참고 — 요약하면 ① `<X>-prompts` 의 `emb_viz` 는 **문장**
      임베딩이라 이미지 공간이 아니고, 네이티브 패널은 크로스 데이터셋을 못 읽는다,
      ② 네이티브 패널은 brain key 를 데이터셋 간에 기억해 새 키를 만들면 다른
      데이터셋에서 죽는다, ③ 603,318 문장 샘플 전량 렌더에 110초가 걸린다(실측).
      우측 패널에 colorByField 는 안 박는다 — active_fields allowlist 밖 필드로
      Color by 하면 App 크래시 (reference_fiftyone_app_gotchas §5).

    구조 함정 (Task 11 실측 — 변경 금지):
    - 리프 Panel 을 곧바로 children 으로 둔다 (Panel 을 낱개 Space 로 한 번 더 감싸면
      App resizeViews 가 "reading 'minimumSize'" TypeError 로 크래시).
    - active_child 필수 — None 이면 플러그인 패널의 on_load 오퍼레이터가 워크스페이스
      로드 시 한 번도 실행되지 않아 "클릭해야 나온다" 증상 (task-11-report.md).
    """
    samples_panel = fo.Panel(type="Samples", pinned=True)
    has_pair = (not ds.name.endswith("-prompts")) \
        and fo.dataset_exists(f"{ds.name}-prompts")
    if has_pair:
        # 프레임 데이터셋의 emb_viz = 이미지 좌표 (sourcei 7,498 · source-h 13,144) — 가볍다.
        embeddings_panel = fo.Panel(type="Embeddings",
                                    state=dict(brainResult="emb_viz"))
    else:
        # ⚠️ `-prompts` 의 emb_viz 는 **문장** 좌표라 603,318 점이다. 네이티브 Embeddings 는
        #    ⓐ brainResult 를 비워두면 매번 손으로 brain key 를 골라야 하고("일일이 선택해야
        #    해" — 2026-08-14 사용자 지적), ⓑ emb_viz 를 박아두면 60만 점을 WebGL 로 그려
        #    렌더 110초 + Chrome 렌더러 크래시("Error code: 5")가 났다(실측).
        #    → 자체 패널로 교체한다. 층화 서브샘플 20,000 점을 **6.4초에 자동으로** 그리고
        #      배너가 표시/전체 비율까지 밝힌다. 고를 단계가 아예 없어진다.
        embeddings_panel = fo.Panel(type="sentence_embeddings")
    if has_pair:
        right_panel = fo.Panel(type="user_prompt_compare")
        desc = "프레임↔문장 비교 (spec 2026-08-07 H1, v2 반반 스택)"
    else:
        right_panel = fo.Panel(type="image_embeddings")
        desc = "3분할 — 좌하 문장(sentence_embeddings) · 우 이미지(image_embeddings)"
    left_stack = fo.Space(
        children=[samples_panel, embeddings_panel],
        orientation="vertical",
        # ⚠️ 세로 배분을 **명시**한다 (2026-08-14 사용자 지적: "변경할거면 분할 사이즈에 맞게").
        #    아래 칸이 산점도(문장/이미지 임베딩)일 때 기본 50:50 은 컨트롤+배너까지 얹히면
        #    플롯 아래가 칸 밖으로 잘렸다. 그리드는 스크롤로 더 볼 수 있지만 산점도는 잘리면
        #    분포 자체가 왜곡돼 보이므로 **아래를 더 준다**. 패널 쪽 PLOT_HEIGHT 예산과 짝이다.
        sizes=[0.42, 0.58],
        active_child=samples_panel.component_id,
    )
    right_pane = fo.Space(children=[right_panel],
                          active_child=right_panel.component_id)
    space = fo.Space(
        children=[left_stack, right_pane],
        orientation="horizontal",
        sizes=[0.5, 0.5],
        active_child=left_stack.component_id,
    )
    return space, desc


def cmd_workspace_compare(dataset_names=None):
    """전 데이터셋에 'compare' 워크스페이스 저장 (기본: emb_viz 브레인런 가진 전부).

    user_default_workspace 오퍼레이터(user-prompt-compare 플러그인, on_dataset_open)가
    이 이름을 찾아 데이터셋 열릴 때 기본으로 로드한다 — 여기 없으면 App 기본(Samples).
    """
    import fiftyone as fo
    names = dataset_names or fo.list_datasets()
    for name in names:
        if not fo.dataset_exists(name):
            print(f"skip (없음): {name}")
            continue
        ds = fo.load_dataset(name)
        if "emb_viz" not in ds.list_brain_runs():
            print(f"skip (emb_viz 없음): {name}")
            continue
        space, desc = _compare_space(fo, ds)
        ds.save_workspace("compare", space, description=desc, overwrite=True)
        assert "compare" in ds.list_workspaces()
        print(f"{name}: workspace 'compare' 저장 완료 ({desc})")


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


# ────────────────────── 분석 필터 세트 (M0~M4) ──────────────────────
def _expand(token, schema, slots):
    """`"wave_pred_{A}"` → 스키마에 실존하는 필드명. 버전 토큰이 없으면 그대로 검사."""
    for key, ver in (("{A}", slots[0]), ("{B}", slots[1])):
        if key in token:
            return _resolve(schema, token.replace(key, "{v}"), ver)
    return token if token in schema else None


# Classification 의 하위 5개 중 사람이 필터로 쓰는 것은 `.label` 뿐이다.
#   .id      ObjectId — "filter by id" 로 렌더되지만 값이 무의미하다 (2026-08-14 사용자 지적)
#   .tags    라벨 태그. 이 파이프라인에서 안 쓴다
#   .logits  항상 None
# `.confidence` 는 데이터셋마다 다르므로 denylist 가 아니라 **실측 비어있음**으로 거른다.
SUBPATH_NOISE = ("id", "tags", "logits")


def sidebar_subpaths(keep, universe):
    """사이드바 그룹에 넣을 서브경로 — **1단만**, 그리고 노이즈 제외.

    FiftyOne 1.19 App 의 `pullSidebarValue` 는 doc-list 분기에서 `keys[0]`/`keys[1]` 만
    본다. 부모가 ListField(EmbeddedDocument) 인 3단 경로를 그룹에 넣으면 모달을 열 때
    `.map is not a function` 으로 App 전체가 죽는다 (prompt_geometry.sidebar_subpaths
    와 같은 규칙 — 그쪽은 numpy 를 끌고 와서 여기 얇게 다시 둔다).
    """
    out = [u for u in universe
           if any(u.startswith(p + ".") and u.count(".") == p.count(".") + 1
                  for p in keep)]
    return [u for u in out if u.rsplit(".", 1)[-1] not in SUBPATH_NOISE]


def _is_categorical(ds, path):
    """Color-by 로 의미가 있는 축인가 — Classification 계열 또는 문자열.

    숫자형(float/int)은 제외한다. 연속값은 색으로 잘 안 읽히고, `active_fields` 에 넣으면
    **그리드 카드마다 숫자가 찍힌다** (2026-08-14 "samples 아래가 왜 변했냐" 피드백).
    """
    import fiftyone as fo
    f = ds.get_field(path)
    if f is None:
        return False
    return isinstance(f, (fo.EmbeddedDocumentField, fo.StringField))


def _workspace_color_roots(ds):
    """저장된 워크스페이스가 Color by 로 쓰는 필드 루트.

    `active_fields` 는 allowlist 이고 **여기 없는 필드로 Color by 하면 App 이 죽는다**.
    큐레이션 필터 세트가 아니라 **워크스페이스에서 파생**해야 하는 이유다 —
    실측(2026-08-14): sourcei 의 `rules` 는 `rule_cross`, `sam3` 는 `sam3_hit` 로 색칠하는데
    둘 다 분별력 기준(§4-4)으로는 필터 세트에서 빠진 필드다. 목록에서 빼면 그 두
    워크스페이스가 에러 화면이 된다.
    """
    roots = []

    def walk(node):
        if hasattr(node, "children"):
            for c in node.children:
                walk(c)
            return
        cb = (getattr(node, "state", None) or {}).get("colorByField")
        if cb:
            roots.append(cb.split(".")[0])

    for w in ds.list_workspaces():
        try:
            walk(ds.load_workspace(w))
        except Exception as exc:  # noqa: BLE001 — 깨진 워크스페이스가 계획을 막으면 안 된다
            print(f"    ⚠️  워크스페이스 {w} 읽기 실패 ({type(exc).__name__}) — 색 루트 누락 가능")
    return roots


def build_filter_plan(ds, slots=None):
    """사이드바 그룹 · active_fields · 뷰 제외목록을 계산한다 (저장하지 않음)."""
    import fiftyone as fo

    slots = slots or read_slots(ds)
    schema = ds.get_field_schema()
    universe_raw = list(ds.get_field_schema(flat=True))

    # 서브경로 청소 — **모든 그룹에 동일 적용**한다. 큐레이션 그룹에만 적용했더니
    # ⑨/⑧ 이 필드당 4~5행짜리 아코디언(⑨ 실측 488행)이 돼 "너무 난잡하다" 는
    # 피드백을 받았다 (2026-08-14).
    #   · 이름으로 거를 것: SUBPATH_NOISE (.id/.tags/.logits)
    #   · 실측으로 거를 것: 전량 null (`.confidence` 가 대표적이지만 데이터셋마다 다르다)
    # 배치 집계 사용 — 순차 `ds.count` 는 191경로에 8초, `aggregate` 는 0.75초 (실측).
    cand = [u for u in universe_raw
            if "." in u and u.rsplit(".", 1)[-1] not in SUBPATH_NOISE]
    counts = dict(zip(cand, ds.aggregate([fo.Count(p) for p in cand]))) if cand else {}

    def _usable(u):
        if "." not in u:
            return True
        return u.rsplit(".", 1)[-1] not in SUBPATH_NOISE and counts.get(u, 0) > 0

    universe = [u for u in universe_raw if _usable(u)]

    groups, keep_top, unresolved = [], [], []
    for name, expanded, tokens in FILTER_GROUPS:
        paths = []
        for t in tokens:
            f = _expand(t, schema, slots)
            (paths.append(f) if f else unresolved.append(t))
        keep_top += paths
        subs = [s for s in sidebar_subpaths(paths, universe) if s not in paths]
        groups.append((name.format(A=slots[0], B=slots[1]), expanded, paths + subs))

    assigned = {p for _, _, ps in groups for p in ps}
    rest = [u for u in universe if u not in assigned
            and ("." not in u or u.rsplit(".", 1)[-1] in DUMP_SUBPATH_KEEP)]
    noise = [u for u in rest if VER_RE.search(u)]
    other = [u for u in rest if u not in noise]
    groups.append((NOISE_GROUP, False, noise))
    groups.append((OTHER_GROUP, False, other))

    # active_fields 는 **그리드 카드에 값을 얹을 필드**다 (사이드바 체크박스 = 카드 표시).
    # 필터링은 이 목록과 무관하고 **Color-by 만** 이 목록을 요구한다 —
    # 밖의 필드로 색칠하면 App 이 죽는다 (prompt_geometry.py:3500-3504 실측).
    #
    # ⚠️ 초판은 큐레이션 필터 세트(`keep_top`)를 통째로 넣어 19→32 개가 됐고,
    #    `wave_iou_*` 같은 **숫자 필드가 카드마다 찍혀** "samples 아래가 왜 변했냐" 는
    #    피드백을 받았다 (2026-08-14). 필터 세트와 표시 세트는 다른 것이다.
    #
    # 가르는 기준은 "필터 세트냐"가 아니라 **범주형이냐 숫자형이냐**다.
    #   · 범주형(Classification/문자열) → Color-by 의 정당한 대상이고 카드에는 라벨 하나만 찍힌다
    #   · 숫자형(float/int)             → 색칠해도 연속값이라 잘 안 읽히고 카드에 숫자가 박힌다
    # 2026-08-14 2차 피드백: 처음엔 keep_top 을 통째로 넣어 숫자가 카드에 찍혔고,
    # 그걸 고치며 너무 좁혀 `close_call`/`runner_up` 등 **원래 색칠하던 축**까지 빠졌다.
    # 타입으로 가르면 둘 다 안 생긴다.
    # 이미 켜져 있던 축은 **뺏지 않는다** — 누군가 그걸로 색칠하고 있었을 수 있고,
    # 목록에서 빠지면 그 순간 App 이 죽는다. 큐레이션은 더하는 쪽으로만 작동한다.
    prior = list(getattr(ds.app_config.active_fields, "paths", None) or [])
    active = list(dict.fromkeys(
        _workspace_color_roots(ds)
        + [p for p in keep_top if _is_categorical(ds, p)]
        + list(ACTIVE_EXTRA) + class_field_candidates(ds)
        + [p for p in prior if _is_categorical(ds, p)]))
    active = [p for p in active if p in schema]

    drop = [f for f in schema if f not in keep_top and f not in FO_DEFAULT_FIELDS]
    return {"slots": slots, "groups": groups, "keep_top": keep_top,
            "active": active, "drop": drop, "unresolved": unresolved,
            "universe": len(universe), "universe_raw": len(universe_raw)}


GIDX_OFFSET = 100_000   # prompt_geometry.GIDX_OFFSET 와 같은 값이어야 한다


def bank_order(prompts_name):
    """gidx 블록에서 **정본 뱅크 순서**를 역산한다: `순번 = min(gidx) // GIDX_OFFSET`.

    왜 필요한가: `prompt_geometry` 는 `gidx = BANKS.index(version) * GIDX_OFFSET + 로컬인덱스`
    로 전역 id 를 만든다. 즉 **BANK_LIST 의 순서가 곧 데이터의 일부**인데 그 순서가 지금까지
    어디에도 기록돼 있지 않았다. 순서를 바꿔 재실행하면 기존 `winner_gidx_*` 전부가 다른
    문장을 가리킨다 — 조용히, 전 버전에서.

    ⚠️ semver 순이 아니다 (실측: v1.0.10.3 이 v1.0.8.4 **뒤**). 정렬로 재구성하면 어긋난다.
    반환: {"order": [버전...], "headroom": {버전: 문장수}, "overflow": [10만 초과 버전]}
    """
    import fiftyone as fo
    if not fo.dataset_exists(prompts_name):
        return {}
    p = fo.load_dataset(prompts_name)
    if "gidx" not in p.get_field_schema() or "bank_version" not in p.get_field_schema():
        return {}
    cur = p._sample_collection.aggregate(
        [{"$group": {"_id": "$bank_version.label", "mn": {"$min": "$gidx"},
                     "mx": {"$max": "$gidx"}, "n": {"$sum": 1}}}], allowDiskUse=True)
    rows = sorted((r["mn"] // GIDX_OFFSET, r["_id"], r["n"], r["mx"]) for r in cur
                  if r["_id"] is not None)
    return {
        "order": [v for _, v, _, _ in rows],
        "headroom": {v: n for _, v, n, _ in rows},
        # 한 버전이 두 블록에 걸치면 gidx 가 이미 이웃 버전을 침범한 것이다
        "overflow": [v for i, v, n, mx in rows if mx // GIDX_OFFSET != i],
    }


def write_bank_run(ds, slots):
    """M1 — 비교쌍 + 스키마 버전 태그 + **정본 뱅크 순서**를 기록."""
    tags = sorted({m.group(1) for m in
                   (VER_RE.search(f) for f in ds.get_field_schema()) if m})
    order = bank_order(f"{ds.name}-prompts")
    ds.info["bank_run"] = {
        "run_id": f"filters-{time.strftime('%Y%m%d-%H%M%S')}",
        "dataset": ds.name,
        "slots": {"a": slots[0], "b": slots[1]},
        "schema_tags": tags,
        "n_tags": len(tags),
        # 새 버전을 붙일 때 BANK_LIST 는 이 순서 그대로 + 끝에 append 여야 한다
        "bank_order": order.get("order", []),
        "gidx_offset": GIDX_OFFSET,
        "gidx_max_used": max(order.get("headroom", {0: 0}).values(), default=0),
        "gidx_overflow": order.get("overflow", []),
        "ts": time.strftime("%Y-%m-%d %H:%M"),
    }
    ds.save()
    if order.get("overflow"):
        print(f"  ❌ gidx 블록 침범 — 문장 수가 GIDX_OFFSET({GIDX_OFFSET:,})을 넘은 뱅크: "
              f"{order['overflow']}. winner_gidx 조인이 이미 오염됐을 수 있다")
    used = max(order.get("headroom", {0: 0}).values(), default=0)
    if used > GIDX_OFFSET * 0.7:
        print(f"  ⚠️  gidx 여유 부족: 최대 뱅크 {used:,}문장 / 오프셋 {GIDX_OFFSET:,} "
              f"({100 * used / GIDX_OFFSET:.0f}% 소진). 10만을 넘는 뱅크가 오면 블록이 충돌한다")
    return tags


def _gidx_to_class(prompts_name):
    """`<ds>-prompts` 의 gidx → 문장 클래스. argmax 예측 유도용."""
    import fiftyone as fo
    if not fo.dataset_exists(prompts_name):
        return {}
    p = fo.load_dataset(prompts_name)
    sch = p.get_field_schema()
    if "gidx" not in sch or "category" not in sch:
        return {}
    return dict(zip(p.values("gidx"), p.values("category.label")))


def cmd_slots(dataset_names, slots=None, apply=False, force=False):
    """규칙별 예측 슬롯 `pred_<rule>_<a|b>` 생성 — 버전 없는 이름으로 A/B 비교.

    ⚠️ 이 서브커맨드만 **필드를 쓴다** (`filters` 는 설정 전용이라 blast radius 가 다르다).
       되돌리기는 `ds.delete_sample_fields([...])` — 원본 `wave_pred_<vt>` /
       `winner_gidx_<vtag>` / `vote_<vt>` 는 손대지 않으므로 언제든 다시 만들 수 있다.
    """
    import fiftyone as fo

    for name in dataset_names:
        if not fo.dataset_exists(name):
            print(f"skip (없음): {name}")
            continue
        ds = fo.load_dataset(name)
        sl = tuple(slots) if slots else read_slots(ds)
        schema = ds.get_field_schema()
        g2c = _gidx_to_class(f"{name}-prompts")
        print(f"\n=== {name}  (A={sl[0]} B={sl[1]}) · prompts gidx 매핑 {len(g2c):,}")
        n = len(ds)
        writes = []
        for key, ver in (("a", sl[0]), ("b", sl[1])):
            for rule, tmpl in PRED_SLOT_SOURCES.items():
                target = f"pred_{rule}_{key}"
                if rule == "argmax":
                    src = _resolve(schema, "winner_gidx_{v}", ver)
                    if not src or not g2c:
                        print(f"  ⏭  {target}: winner_gidx 또는 prompts 매핑 없음 — 생략")
                        continue
                    labels = [g2c.get(g) if g is not None else None
                              for g in ds.values(src)]
                else:
                    src = _resolve(schema, tmpl, ver)
                    if not src:
                        print(f"  ⏭  {target}: {tmpl.format(v=ver)} 계열 필드 없음 — 생략")
                        continue
                    labels = ds.values(src + ".label")
                hit = sum(1 for x in labels if x)
                print(f"  {target:16s} ← {src:24s} 채움 {hit:,}/{n:,}")
                writes.append((target, labels, f"{RULE_LABELS[rule]} · 뱅크 {ver}"))

        if not apply:
            print("  (dry-run — 실제 생성은 --apply)")
            continue
        for target, labels, _ in writes:
            ds.set_values(target, [fo.Classification(label=x) if x else None
                                   for x in labels])
        ds.save()
        # 필드 설명 — 슬롯 이름에는 버전이 없으므로 **어느 뱅크인지 여기서 말한다**.
        # 그룹 라벨(`① 판정 (A=… · B=…)`)과 이중으로 남긴다: 그룹 헤더는 스크롤로 사라진다.
        for target, _, desc in writes:
            try:
                f = ds.get_field(target)
                f.description = desc
                f.save()
            except Exception as exc:  # noqa: BLE001 — 설명은 부가정보, 실패해도 슬롯은 유효
                print(f"    ⚠️  {target} description 설정 실패 ({type(exc).__name__})")
        # ⚠️ 출처 기록은 **여기서** 한다. 슬롯에 무엇이 들어갔는지 아는 것은 이 함수뿐이다.
        #    2026-08-14 실측 버그: `slots` 가 bank_run 을 안 쓰고 `filters` 가 stale 한
        #    값을 읽어, 데이터는 v1.0.8.4 인데 그룹 라벨은 v1.0.13.2 로 붙었다.
        #    출처 표시가 데이터와 어긋나는 것은 이 설계가 막으려던 실패 그 자체다.
        write_bank_run(ds, sl)
        print(f"  ✅ 슬롯 {len(writes)}개 생성: "
              + ", ".join(f"{t}({d.split('뱅크 ')[-1]})" for t, _, d in writes)
              + f" · bank_run.slots={{a:{sl[0]}, b:{sl[1]}}}")


def cmd_dump(dataset_names, path=None):
    """M0 — app_config 스냅샷. 이 파일이 `filters` 의 롤백 아티팩트 전체다.

    저장뷰·워크스페이스는 `filters` 가 **덮어쓰지 않으므로**(새 이름만 쓴다) 덤프 대상이
    아니다. 위험한 것은 sidebar_groups / active_fields / color_scheme 셋뿐이다.
    """
    import fiftyone as fo
    from bson import json_util

    path = path or f"/tmp/app_config-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out = {}
    for name in dataset_names:
        if not fo.dataset_exists(name):
            print(f"skip (없음): {name}")
            continue
        ds = fo.load_dataset(name)
        out[name] = ds.app_config.to_dict()
        sg = ds.app_config.sidebar_groups or []
        af = ds.app_config.active_fields
        print(f"{name}: sidebar_groups {len(sg)}개 / "
              f"active_fields {len(af.paths) if af else 0} / "
              f"color_scheme {'있음' if ds.app_config.color_scheme else '없음'}")
    with open(path, "w", encoding="utf-8") as f:
        f.write(json_util.dumps({"ts": time.strftime("%Y-%m-%d %H:%M"), "configs": out},
                                indent=2, ensure_ascii=False))
    print(f"→ {path}  ({len(out)}개 데이터셋)")
    return path


def cmd_restore(path):
    """덤프 되돌리기 — app_config 만 복원한다."""
    import fiftyone as fo
    from bson import json_util

    with open(path, encoding="utf-8") as f:
        blob = json_util.loads(f.read())
    for name, cfg in blob["configs"].items():
        if not fo.dataset_exists(name):
            print(f"skip (없음): {name}")
            continue
        ds = fo.load_dataset(name)
        ds.app_config = fo.DatasetAppConfig.from_dict(cfg)
        ds.save()
        print(f"{name}: app_config 복원 ({blob.get('ts')} 시점)")


def cmd_filters(dataset_names, slots=None, apply=False, force=False):
    """M1+M2+M4 — 사이드바 그룹 · active_fields · 저장뷰 생성. 기본 dry-run."""
    import fiftyone as fo
    from fiftyone import ViewField as F

    for name in dataset_names:
        if not fo.dataset_exists(name):
            print(f"skip (없음): {name}")
            continue
        if name not in CURATED_DATASETS and not force:
            print(f"skip ({name}): FILTER_GROUPS 는 sourcei 실측으로 고른 목록이라 다른 "
                  f"데이터셋에 쓰면 틀린 화면이 된다. 스펙 §4-4 분별력 측정을 먼저 돌려 "
                  f"목록을 다시 고르고 CURATED_DATASETS 에 추가할 것 (그래도 강행: --force)")
            continue
        ds = fo.load_dataset(name)
        plan = build_filter_plan(ds, slots)
        a, b = plan["slots"]
        print(f"\n=== {name}  (비교쌍 A={a} B={b})")
        # 슬롯 필드의 description 이 진실이다 (그것을 쓴 `cmd_slots` 가 붙였다).
        # 라벨로 쓸 비교쌍과 어긋나면 사용자에게 **틀린 출처**를 보여주게 된다.
        for key, ver in (("a", a), ("b", b)):
            fld = ds.get_field(f"pred_wave_{key}")
            desc = getattr(fld, "description", None) if fld is not None else None
            if desc and not desc.endswith(ver):
                print(f"  ❌ 슬롯 {key.upper()} 불일치: 필드는 '{desc}' 인데 라벨은 '{ver}' 로 "
                      f"붙는다. `slots --slots={a},{b} --apply` 를 먼저 실행할 것")
        if plan["unresolved"]:
            print(f"  ⚠️  스키마에서 못 찾은 토큰 {len(plan['unresolved'])}개: "
                  f"{plan['unresolved']} — 해당 필터는 생략된다")
        for gname, expanded, paths in plan["groups"]:
            mark = "▼" if expanded else "▶"
            head = ", ".join(p for p in paths if "." not in p)[:90]
            print(f"  {mark} {gname:24s} {len(paths):4d}경로  {head}")
        print(f"  active_fields {len(plan['active'])} · 뷰 제외 {len(plan['drop'])} top 필드 · "
              f"사이드바 경로 {plan['universe_raw']}→{plan['universe']} "
              f"(노이즈 서브경로 {plan['universe_raw'] - plan['universe']}개 미나열)")

        keep = plan["keep_top"]
        view_flat = len([u for u in ds.get_field_schema(flat=True)
                         if u.split(".")[0] in set(keep) | set(FO_DEFAULT_FIELDS)])
        print(f"  → `00_분석` 뷰 flat {view_flat} (현재 {plan['universe']})")

        if not apply:
            print("  (dry-run — 실제 적용은 --apply)")
            continue

        tags = write_bank_run(ds, plan["slots"])
        defaults = fo.DatasetAppConfig.default_sidebar_groups(ds)
        G = type(defaults[0])
        keep_default = [g for g in defaults if g.name in ("tags", "label tags")]
        groups = list(keep_default)
        assigned = {p for g in keep_default for p in g.paths}
        for gname, expanded, paths in plan["groups"]:
            paths = [p for p in paths if p not in assigned]
            if not paths:
                continue
            groups.append(G(name=gname, paths=paths, expanded=expanded))
            assigned.update(paths)
        ds.app_config.sidebar_groups = groups

        from fiftyone.core.odm.dataset import ActiveFields
        ds.app_config.active_fields = ActiveFields(paths=plan["active"], exclude=False)
        apply_colors(ds)          # color_scheme 을 비교쌍과 정합하게 재작성 (M3 경유)
        ds.save()

        base = ds.exclude_fields(plan["drop"])
        # 슬롯이 있으면 그것을, 없으면 버전 접미 원본을 쓴다 (`slots` 미실행 환경 폴백)
        sch2 = ds.get_field_schema()
        pred_a = ("pred_wave_a" if "pred_wave_a" in sch2
                  else _expand("wave_pred_{A}", sch2, plan["slots"]))
        pred_b = ("pred_wave_b" if "pred_wave_b" in sch2
                  else _expand("wave_pred_{B}", sch2, plan["slots"]))
        views = [("00_분석", base)]
        if pred_a:
            views.append(("01_오탐", base.match(
                (F("ground_truth.label") == "normal") & (F(pred_a + ".label") != "normal"))))
        if "event_kind" in ds.get_field_schema():
            views.append(("02_near_miss", base.match(F("event_kind.label") == "near_miss")))
        if pred_a and pred_b:
            views.append(("03_AB불일치", base.match(
                F(pred_a + ".label") != F(pred_b + ".label"))))
        views.append(("99_전체", ds.view()))
        for vname, v in views:
            if vname in ds.list_saved_views():
                ds.delete_saved_view(vname)
            ds.save_view(vname, v)
        print(f"  ✅ 적용: 그룹 {len(groups)} · active {len(plan['active'])} · "
              f"뷰 {[n for n, _ in views]} · bank_run 태그 {len(tags)}개")


def _selftest():
    # 팔레트 위생: 중복 없음 + 유효 hex
    assert len(set(OKABE_ITO)) == len(OKABE_ITO)
    for c in list(OKABE_ITO) + list(CLASS_COLORS.values()):
        assert c.startswith("#") and len(c) == 7, c
    # 4클래스 핵심 색이 서로 다름 (fire/smoke/falldown/normal)
    core = [CLASS_COLORS[k] for k in ("fire", "smoke", "falldown", "normal")]
    assert len(set(core)) == 4

    # ── 필터 세트 (fiftyone 불필요) ──
    slots = ("v1.0.8.0", "v1.0.8.4")
    schema = {"ground_truth", "wave_pred_v1_0_8_0", "wave_pred_v1_0_8_4",
              "wave_iou_fire_v1080", "wave_vs_topk_v1084", "camera"}
    assert _expand("wave_pred_{A}", schema, slots) == "wave_pred_v1_0_8_0"
    assert _expand("wave_pred_{B}", schema, slots) == "wave_pred_v1_0_8_4"
    # vtag 계열도 같은 토큰 문법으로 해석돼야 한다 (필드군마다 표기가 다르다)
    assert _expand("wave_iou_fire_{A}", schema, slots) == "wave_iou_fire_v1080"
    assert _expand("wave_vs_topk_{B}", schema, slots) == "wave_vs_topk_v1084"
    assert _expand("camera", schema, slots) == "camera"
    assert _expand("nope_{A}", schema, slots) is None      # 없으면 조용히 None
    assert _expand("nope", schema, slots) is None

    # 서브경로는 1단만 — 3단을 그룹에 넣으면 App 이 죽는다
    uni = ["a", "a.label", "a.label.x", "b", "b.id"]
    assert sidebar_subpaths(["a"], uni) == ["a.label"]
    # 노이즈 서브경로 제외 — `.id` 는 "filter by id" 로 렌더되지만 값이 무의미하다
    uni2 = ["p", "p.id", "p.tags", "p.label", "p.logits", "p.confidence"]
    assert sidebar_subpaths(["p"], uni2) == ["p.label", "p.confidence"], \
        sidebar_subpaths(["p"], uni2)
    # 규칙 라벨은 슬롯 이름에 없는 정보(어느 규칙인지)를 설명에 싣는다
    assert set(RULE_LABELS) == set(PRED_SLOT_SOURCES)
    # 덤프 그룹은 `.label` 만 — confidence 까지 나열하면 ⑨ 이 다시 난잡해진다
    assert DUMP_SUBPATH_KEEP == ("label",)
    assert not (set(DUMP_SUBPATH_KEEP) & set(SUBPATH_NOISE))
    # active_fields = 그리드 표시용. 숫자 축이 들어가면 카드마다 값이 찍힌다
    numeric_ish = ("wave_iou", "wave_gain", "event_index", "frame_in_event",
                   "wave_vs_topk", "_margin", "cos_best")
    assert not any(n in x for x in ACTIVE_EXTRA for n in numeric_ish), ACTIVE_EXTRA

    # 버전 접미사 인식 (세 세대 + 큐레이션 접미)
    for f, want in (("wave_pred_v1_0_8_0", "v1_0_8_0"), ("wave_iou_fire_v1080", "v1080"),
                    ("pred_margin_v080", "v080"), ("x_v1084-prune205", "v1084-prune205")):
        m = VER_RE.search(f)
        assert m and m.group(1) == want, (f, m and m.group(1))
    assert VER_RE.search("person_count_bin") is None
    assert VER_RE.search("ground_truth.label") is None

    # 필터 세트가 분별력 기준(§4-4)에서 탈락한 필드를 되들이지 않았는지
    tokens = {t for _, _, ts in FILTER_GROUPS for t in ts}
    banned = {"rule_cross", "environment", "weather", "sam3_hit", "sam3_n",
              "sam3", "category", "adopted", "attached_bank", "view_unit"}
    assert not (tokens & banned), tokens & banned
    assert all(("{A}" in t) or ("{B}" in t) or ("{" not in t) for t in tokens)
    # 그룹명에 버전을 넣지 않는다 (사용자 결정) — 출처는 필드 description 이 담당한다.
    # 포맷 자체는 안전해야 한다: 미치환 `{` 가 남으면 사이드바에 리터럴로 노출된다.
    names = [n.format(A=slots[0], B=slots[1]) for n, _, _ in FILTER_GROUPS]
    assert all("{" not in n for n in names), names
    assert not any(v in n for n in names for v in slots), \
        f"그룹명에 버전이 들어갔다 (출처는 description 담당): {names}"
    # 예측 슬롯은 규칙 × A/B — 버전이 이름에 없어야 한다 (그게 슬롯의 존재 이유)
    slot_names = [f"pred_{r}_{k}" for r in PRED_SLOT_SOURCES for k in ("a", "b")]
    assert all(VER_RE.search(s) is None for s in slot_names), slot_names
    assert set(slot_names) >= {"pred_wave_a", "pred_wave_b",
                               "pred_argmax_a", "pred_topk_b"}
    print("selftest OK")


if __name__ == "__main__":
    args = sys.argv[1:]
    USAGE = ("usage: fiftyone_app_setup.py "
             "selftest|colors|workspace|workspace-compare|workspace-fix"
             "|dump <ds>|restore <file>|slots <ds> [--apply]"
             "|filters <ds> [--apply] [--slots A,B]")
    flags = [a for a in args if a.startswith("--")]
    args = [a for a in args if not a.startswith("--")]
    if not args:
        raise SystemExit(USAGE)
    elif args[0] == "selftest":
        _selftest()
    elif args[0] == "dump":
        cmd_dump(args[1].split(",") if len(args) > 1 else DEFAULT_DATASETS,
                 args[2] if len(args) > 2 else None)
    elif args[0] == "restore":
        if len(args) < 2:
            raise SystemExit("restore 는 덤프 파일 경로가 필요하다")
        cmd_restore(args[1])
    elif args[0] == "slots":
        slot_flag = next((f for f in flags if f.startswith("--slots=")), None)
        cmd_slots(args[1].split(",") if len(args) > 1 else ["sourcei"],
                  tuple(slot_flag.split("=", 1)[1].split(",")) if slot_flag else None,
                  apply="--apply" in flags, force="--force" in flags)
    elif args[0] == "filters":
        slot_flag = next((f for f in flags if f.startswith("--slots=")), None)
        cmd_filters(args[1].split(",") if len(args) > 1 else ["sourcei"],
                    tuple(slot_flag.split("=", 1)[1].split(",")) if slot_flag else None,
                    apply="--apply" in flags, force="--force" in flags)
    elif args[0] == "colors":
        names = args[1].split(",") if len(args) > 1 else DEFAULT_DATASETS
        cmd_colors(names)
    elif args[0] == "workspace":
        cmd_workspace()
    elif args[0] == "workspace-compare":
        cmd_workspace_compare(args[1].split(",") if len(args) > 1 else None)
    elif args[0] == "workspace-fix":
        cmd_workspace_fix(args[1].split(",") if len(args) > 1 else None)
    else:
        raise SystemExit(USAGE)
