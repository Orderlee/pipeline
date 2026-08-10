"""user-prompt-compare — 문장(<세션 데이터셋>-prompts) ↔ 프레임(세션 데이터셋) 크로스 데이터셋
비교 Panel.

모드 A(스펙 §5.2): 판정규칙(argmax_k1|dist_iou)별 문장 산점도 + 프레임↔문장 양방향 선택 연동.
Task 12부터 프롬프트 데이터셋은 세션 데이터셋 이름에서 "<name>-prompts"로 자동 유도된다
(sourcei 세션 → sourcei-prompts, source-h 세션 → source-h-prompts) — 짝이 없으면 안내만 뜨고
크래시하지 않는다. 뱅크 버전이 여럿이면 "전체"/버전별 선택기로 산점도를 필터링한다.
정본: docker/analysis/plugins/user-prompt-compare/ (git)
배포: docker cp → /data/fiftyone/datasets/__plugins__/user-prompt-compare/
"""
import re

import fiftyone as fo
import fiftyone.operators as foo
import fiftyone.operators.types as types

# Task 12 이전 하드코딩 값 — 지금은 (a) selftest 오프라인 픽스처, (b) ctx.dataset 이 없을 때의
# fallback 기본값 두 용도로만 남는다. 런타임 조인은 _prompts_dataset_name(ctx)/
# _current_winner_field(ctx)가 세션 데이터셋에서 유도한 값을 쓴다 (아래 정의부 주석 참고).
PROMPTS_DATASET = "sourcei-prompts"
BRAIN_KEY = "emb_viz"          # 하드코딩 — App이 다른 키에서 죽는 실측 함정

FRAMES_DATASET = "sourcei"
VTAG = "v080"
WINNER_FIELD = f"winner_gidx_{VTAG}"
MAX_POINTS = 20_000
CACHE_CAP_BYTES = 64 * 2**20

_CACHE = {}  # (dataset_name, brain_key, last_modified_at) -> bundle. 엔트리 1개 유지.

META_FIELDS = ["gidx", "text", "category", "adopted", "wins", "purity",
               "n_cameras", "wave_gain", "wave_role", "bank_version"]

# Task 12 — 뱅크 버전 선택기 + 프롬프트 데이터셋 자동 유도.
ALL_VERSIONS_LABEL = "전체"
NO_PROMPTS_PAIR_TEXT = "이 데이터셋에는 프롬프트 짝이 없습니다"

# 표시 드롭다운 값 (2026-08-10 피드백: 토글 버튼 전부 드롭다운으로 통일)
SHOW_ALL_LABEL = "전체 (미채택 포함)"
SHOW_ADOPTED_LABEL = "채택만"


def _bundle_nbytes(b):
    import numpy as np
    return sum(v.nbytes for v in b.values() if isinstance(v, np.ndarray))


def load_prompt_bundle(dataset_name=PROMPTS_DATASET):
    """문장 좌표+메타 로드. embedding(1024-d)은 절대 읽지 않는다 (스펙 §5.5).

    Task 12: dataset_name 파라미터화 — source-h 세션은 "source-h-prompts", sourcei 세션은
    "sourcei-prompts"를 각자 넘긴다. 캐시 키에 dataset_name이 들어가므로 데이터셋을
    전환하면 재계산되지만, `_CACHE.clear()`가 항상 먼저 실행돼 1엔트리만 유지된다
    (스펙 §5.5 캐시 예산은 여전히 활성 엔트리 1개 기준).
    """
    import numpy as np
    ds = fo.load_dataset(dataset_name)
    key = (dataset_name, BRAIN_KEY, str(ds.last_modified_at))
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


def frame_ids_to_gidx(frame_ids, dataset_name=FRAMES_DATASET, winner_field=WINNER_FIELD):
    frames = fo.load_dataset(dataset_name)
    if winner_field not in frames.get_field_schema():
        return []   # Task 12: 조인 필드 없음 — 크래시 대신 빈 결과 (호출부가 계속 진행)
    vals = frames.select(frame_ids).values(winner_field)
    return sorted({int(v) for v in vals if v is not None})


def gidx_to_frame_ids(g, dataset_name=FRAMES_DATASET, winner_field=WINNER_FIELD):
    frames = fo.load_dataset(dataset_name)
    if winner_field not in frames.get_field_schema():
        return []   # Task 12: 조인 필드 없음 — 크래시 대신 빈 결과 (호출부가 계속 진행)
    return frames.match(fo.ViewField(winner_field) == int(g)).values("id")


def gidxes_to_frame_ids(gs, dataset_name=FRAMES_DATASET, winner_field=WINNER_FIELD):
    """복수 gidx → 프레임 id 일괄 조인 (lasso 다중선택용 — gidx당 쿼리 1회 대신 is_in 1회)."""
    frames = fo.load_dataset(dataset_name)
    if winner_field not in frames.get_field_schema():
        return []
    return frames.match(fo.ViewField(winner_field).is_in([int(g) for g in gs])).values("id")


# ── Task 12 — 뱅크 버전 → 조인 필드 매핑 + 프롬프트 데이터셋 자동 유도 ──

def version_to_winner_field(version):
    """버전 문자열의 숫자만 추출해 마지막 3자리로 winner_gidx_v<3자리> 필드명을 만든다.

    예: "v1.0.8.0" -> "1080" -> "080" -> "winner_gidx_v080"
        "v1.0.8.4" -> "1084" -> "084" -> "winner_gidx_v084"
    """
    digits = re.sub(r"\D", "", str(version))
    tail = digits[-3:].zfill(3) if digits else "000"
    return f"winner_gidx_v{tail}"


def _resolve_join_field(dataset, version):
    """버전 → 조인 필드, 단 세션 데이터셋 스키마에 실제로 없으면 None.

    실측(Task 12, 2026-08-07): source-h 프레임 데이터셋은 winner_gidx_v080만 갖고
    winner_gidx_v084는 없다(v084 관련 다른 필드 — rule_flip_v084/winner_loo_v084 등 —
    는 있지만 winner_gidx_v084 자체가 부재). 호출부는 None을 "조인 필드 없음" 안내로
    처리해야 하며 절대 KeyError/ValueError로 죽으면 안 된다.
    """
    if dataset is None or version is None:
        return None
    field = version_to_winner_field(version)
    try:
        schema = dataset.get_field_schema()
    except Exception:
        return None
    return field if field in schema else None


def _prompts_dataset_name(ctx):
    """모드 A 프롬프트 데이터셋 이름을 세션 데이터셋에서 유도: "<dataset>-prompts".

    ctx.dataset이 없는 호출(오프라인 selftest 등)은 레거시 PROMPTS_DATASET로 폴백한다.
    """
    ds = getattr(ctx, "dataset", None)
    if ds is not None:
        return f"{ds.name}-prompts"
    return PROMPTS_DATASET


def _current_winner_field(ctx):
    """프레임→문장 역방향 조인(그리드 체크박스/lasso)에 쓸 winner 필드.

    버전 필터가 특정 버전으로 잡혀 있으면 그 버전에서 유도, "전체"/미설정이면
    레거시 기본값(WINNER_FIELD=winner_gidx_v080)으로 폴백 — 기존 sourcei 기본 동작과
    바이트 단위로 동일하게 유지(회귀 방지).
    """
    filt = ctx.panel.state.bank_version_filter
    if filt and filt != ALL_VERSIONS_LABEL:
        return version_to_winner_field(filt)
    return WINNER_FIELD


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


# ── UI 계약 문자열 (스펙 §5.4 — 임의 수정 금지) ──
BANNER_RULE = ("이 조인은 K=1 전역 argmax(argmax_k1) 승자 기준 — "
               "제품 판정규칙(topk_vote K=10 다수결, dist_iou)과 다른 값")
BANNER_COORDS_A = "좌우 UMAP은 독립 fit — 좌표 공간 비교 금지, 연결은 선택 하이라이트로만"
BANNER_WAVE_NOCLICK = "dist_iou에는 프레임 귀속이 없습니다 — 기여도는 전역 LOO(wave_gain)"
RESERVE_TEXT = "가져간 프레임 0 — 예비군 (새 카메라 승자의 66%가 여기서 나온다)"

GREY = "#CCCCCC"
# 회색 계열 금지 (사용자 피드백 2026-08-10): 미채택(GREY)·중간(#999999)·smoke(#7F7F7F)가
# 전부 무채색이라 서로 안 구분됐다 — 채택 팔레트(CLASS/WAVE_ROLE)에는 유채색만 쓴다.
# smoke=하늘색(#56B4E9): person(#009E73 초록, fiftyone_app_setup.py)과 겹치지 않는 잔여
# Okabe-Ito 유채색. normal(#0072B2)과 같은 파랑 계열이지만 명도 차가 큰 공인 구분쌍.
CLASS_COLORS = {  # Task 2와 동일 값 (배포 단위가 달라 복사 유지 — 변경 시 양쪽 동기화)
    "fire": "#D55E00", "smoke": "#56B4E9", "falldown": "#E69F00",
    "normal": "#0072B2", "smoking": "#CC79A7",
}
WAVE_ROLE_COLORS = {  # dist_iou 전용 — CLASS_COLORS와 무교집합인 wave_role 값 색칠 (리뷰 fix)
    "유익 상위10%": "#009E73", "유해 하위10%": "#D55E00", "중간": "#0072B2",
}


def _hover(b, i):
    return (f"[{b['gidx'][i]}] {str(b['text'][i])[:80]}<br>"
            f"class={b['category'][i]} wins={b['wins'][i]} "
            f"purity={b['purity'][i]} wave_gain={b['wave_gain'][i]}")


def build_mode_a(bundle, rule, show_unadopted, selected_gidx, bank_version_filter=None):
    """문장 산점도 (모드 A). trace: [0]미채택 [1..k]채택(그룹별 1개) [마지막]하이라이트.

    채택점은 그룹(argmax_k1=클래스, dist_iou=wave_role)별로 trace를 쪼갠다 — Plotly 범례는
    trace 단위라, 단일 trace + per-point 색 배열이면 범례에 클래스→색 매핑이 아예 안 나온다
    (화면에 파랑 normal 240점이 있어도 범례엔 첫 점 색(주황) 글리프 하나 — 2026-08-10 실측 버그).

    bank_version_filter: None/"전체" -> 전 문장(기존 동작과 동일, 회귀 없음).
    특정 버전 문자열이면 그 버전 문장만 남기고 서브샘플/렌더 (Task 12).
    """
    import numpy as np
    b = bundle
    n = len(b["gidx"])
    idx_all = np.arange(n)
    bv = b.get("bank_version")
    if bank_version_filter and bank_version_filter != ALL_VERSIONS_LABEL and bv is not None:
        keep = np.asarray([str(bv[i]) == bank_version_filter for i in idx_all])
        idx_all = idx_all[keep]
    adopted = b["adopted"][idx_all].astype(bool)
    if len(idx_all) > MAX_POINTS:
        cats = [b["category"][i] for i in idx_all]
        sub_pos = np.asarray(stratified_subsample(cats, MAX_POINTS))
        idx_all = idx_all[sub_pos]
        adopted = b["adopted"][idx_all].astype(bool)

    def trace(mask, color, size, name, opacity):
        # "ids" (customdata 아님) — FiftyOne PlotlyView의 onClick 이벤트는 trace.ids[pointIndex]만
        # ctx.params["id"]로 전달한다 (App 번들 getIdForTrace 실측, 문서의 "data.customdata"는 오기).
        ii = idx_all[mask]
        return {
            "type": "scattergl", "mode": "markers", "name": name,
            "x": b["xy"][ii, 0].tolist(), "y": b["xy"][ii, 1].tolist(),
            "ids": [str(int(b["gidx"][i])) for i in ii],
            "text": [_hover(b, i) for i in ii], "hoverinfo": "text",
            "marker": {"color": color, "size": size, "opacity": opacity},
        }

    # Task 12: 배너에 현재 버전 필터 표기 (BANNER_RULE/BANNER_WAVE_NOCLICK/BANNER_COORDS_A는
    # `in` 검사로 selftest가 고정하므로 접미사 추가는 기존 assert를 깨지 않는다).
    vtxt = bank_version_filter or ALL_VERSIONS_LABEL
    sub = f"{BANNER_COORDS_A} · 버전: {vtxt}"
    if not show_unadopted:
        # 토글 피드백(사용자 피드백): 미채택 숨김 상태를 배너에도 굵게 명시 —
        # 버튼 라벨(render)과 이중으로 상태가 보이게 한다.
        sub += " · <b>표시: 채택만</b>"
    # 마커 시인성(사용자 피드백): App 테마 배경(mediaSpace, 다크)이 기본이라 작은/반투명
    # 점이 묻힌다 — 채택점은 크게 + 흰 테두리, 미채택은 한 단계 밝게. 팔레트 자체는 유지.
    if rule == "argmax_k1":
        banner = f"{BANNER_RULE}<br><sup>{sub}</sup>"
        groups_arr = np.asarray([str(b["category"][i]) for i in idx_all], dtype=object)
        palette = CLASS_COLORS
        def size_of(i):
            return 6 + min(10, int(b["wins"][i]) // 50)
    else:  # dist_iou — 색=wave_role, 크기 균일, 클릭 무효
        banner = f"{BANNER_WAVE_NOCLICK}<br><sup>{sub}</sup>"
        has_role = b.get("wave_role") is not None
        groups_arr = np.asarray([str(b["wave_role"][i]) if has_role else "채택"
                                 for i in idx_all], dtype=object)
        palette = WAVE_ROLE_COLORS if has_role else {}
        def size_of(i):
            return 7

    data = []
    if show_unadopted:
        # size 6 = 네이티브 Embeddings(emb_viz) 패널의 점 크기와 동일 (라이브 실측 — 사용자
        # 요청: 두 화면의 점 크기 체감이 같아야 비교가 편하다). 미채택 구분은 회색+반투명으로.
        data.append(trace(~adopted, GREY, 6, f"미채택 {int((~adopted).sum())} (예비군)", 0.45))
    else:
        # 빈 trace(x=[]) 대신 visible=False: 배열 길이를 유지한 채 플래그만 뒤집는다.
        # (빈 배열 방식은 클라이언트 patch 딥머지에서 옛 점을 못 지우는 문제가 있었다 —
        # _refresh의 set_data 금지 주석. visible=False trace는 범례에서도 빠진다 — 실측.)
        t_hidden = trace(~adopted, GREY, 6, f"미채택 {int((~adopted).sum())} (숨김)", 0.45)
        t_hidden["visible"] = False
        data.append(t_hidden)
    # 채택: 그룹별 trace 1개 (docstring 참고 — 범례에 그룹별 색+개수가 나오고, 범례 클릭으로
    # 그룹 토글도 된다). 팔레트 순서 → 팔레트 밖 그룹(사전순) 순으로 안정 정렬, 빈 그룹은 생략.
    order = [grp for grp in palette if (adopted & (groups_arr == grp)).any()]
    order += sorted(set(groups_arr[adopted]) - set(palette))
    for grp in order:
        m = adopted & (groups_arr == grp)
        t = trace(m, palette.get(grp, "#999999"), 5, f"{grp} {int(m.sum())}", 0.95)
        t["marker"] = {"color": palette.get(grp, "#999999"),
                       "size": [size_of(i) for i in idx_all[m]], "opacity": 0.95,
                       "line": {"width": 0.8, "color": "#FFFFFF"}}
        data.append(t)

    sel = [i for i in range(len(idx_all))
           if int(b["gidx"][idx_all[i]]) in (selected_gidx or set())]
    hi = idx_all[sel]
    data.append({"type": "scattergl", "mode": "markers", "name": "선택",
                 "x": b["xy"][hi, 0].tolist(), "y": b["xy"][hi, 1].tolist(),
                 "ids": [str(int(b["gidx"][i])) for i in hi],
                 # 다크 배경에서 #000000 링은 안 보인다 — Okabe-Ito 노랑(클래스 색과 무교집합)
                 "marker": {"color": "#F0E442", "size": 14, "symbol": "circle-open",
                            "line": {"width": 3}}})
    return {"data": data,
            "layout": {"title": {"text": banner, "font": {"size": 12}},
                       "showlegend": True, "dragmode": "pan",
                       "xaxis": {"visible": False}, "yaxis": {"visible": False},
                       # height 고정 금지 — PlotlyView는 style.height(=view의 height kwarg,
                       # 기본 "100%")를 따르므로 render()의 vh 기반 height가 실높이를 정한다
                       # (App 번들 실측: bo=Yn?.height||"100%"). autosize가 그 style을 추적.
                       "autosize": True,
                       "margin": {"l": 10, "r": 10, "t": 60, "b": 10}}}


# ── 모드 B (스펙 §5.1b, R5-b) — 같은 데이터셋 슬라이스를 하나의 emb_viz 좌표에 overlay ──
BANNER_COORDS_B = ("같은 좌표계(UMAP 공유 fit) — 그룹 간 공간 비교 유효 "
                    "(모드 A는 독립 fit이라 비교 금지, 모드 B는 비교 가능)")
OKABE_ITO_B = ["#0072B2", "#E69F00", "#009E73", "#D55E00",
               "#CC79A7", "#56B4E9", "#F0E442", "#000000"]


def build_mode_b(ds_name, group_field, groups, brain_key=BRAIN_KEY):
    """같은 데이터셋의 그룹 슬라이스들을 하나의 emb_viz 좌표 위에 overlay.

    frames_captions(project 22개)이 본래 타깃 — 그룹당 1 trace, 같은 UMAP fit을 공유하므로
    좌표 공간 비교가 정당하다 (스펙 §5.1b, 모드 A와 달리). 그룹 필드는 문자열/Classification
    모두 허용(카테고리 값이면 .label로 평탄화). Task 6 stratified_subsample로 그룹당
    MAX_POINTS/n 서브샘플 — 네이티브 Embeddings 패널의 5,000점 상한 우회.
    """
    import numpy as np
    ds = fo.load_dataset(ds_name)

    # 크래시 가드 (2026-08-10 실사용 오류): 기본 group_field="project"는 frames_captions
    # 용이라 sourcei 등 다른 데이터셋엔 없다 — 무방비 ds.values()가 ValueError로 패널을
    # 죽였다. 조인 필드 부재와 같은 규약: 크래시 대신 안내 배너만 그린다.
    def _notice(text):
        return {"data": [],
                "layout": {"title": {"text": f"{text}<br><sup>{BANNER_COORDS_B}</sup>",
                                     "font": {"size": 12}},
                           "xaxis": {"visible": False}, "yaxis": {"visible": False}}}

    try:
        field_missing = ds.get_field(group_field) is None
    except Exception:
        field_missing = True
    if field_missing:
        return _notice(f"이 데이터셋에는 그룹 필드 '{group_field}'가 없습니다")
    if brain_key not in ds.list_brain_runs():
        return _notice(f"이 데이터셋에는 brain run '{brain_key}'가 없습니다 — 좌표 없음")

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
            "marker": {"size": 6, "opacity": 0.75,
                       "line": {"width": 0.5, "color": "#FFFFFF"},
                       "color": OKABE_ITO_B[gi % len(OKABE_ITO_B)]},
        })
    return {"data": data,
            "layout": {"title": {"text": BANNER_COORDS_B, "font": {"size": 12}},
                       "showlegend": True,
                       "xaxis": {"visible": False}, "yaxis": {"visible": False}}}


def _dedup_guard(ctx, state_key, ids):
    """재발화 가드 (공용 헬퍼). 반환: 처리해야 하면 False, 중복(스킵)이면 True.

    ⚠️ `state_key`는 밑줄로 시작하면 안 된다 — 배포본 panel.py의 `PanelRefBase.__setattr__`
    는 `_`로 시작하는 속성을 `self.set()` 우회 경로(순수 파이썬 인스턴스 속성)로 처리해
    `ctx.panel_state`(실제 라운드트립되는 dict)에 반영되지 않는다(panel.py:223-235 실측).
    그 결과 매 훅 호출마다 리셋되어 `sig == []` 로 항상 붕괴 — 스퓨리어스 빈 payload
    재발화는 우연히 막히지만, 진짜 "전체 선택 해제"(ids=[] 로의 정상 전이)도 영구히
    삼켜 해제가 UI에 반영되지 않는 회귀가 났었다(Task 8 fix round). 밑줄 없는 키만
    `.set()`을 타 실제로 영속된다 — `rule`/`show_unadopted`/`selected_gidx`와 동일 경로.
    """
    sig = sorted(ids)
    if sig == (ctx.panel.state.get(state_key) or []):
        return True
    ctx.panel.state.set(state_key, sig)
    return False


def _rows_to_markdown(rows, join_field_missing=None, total=None):
    """선택 프레임의 승자 문장 표 (프레임→문장 방향, 스펙 §5.2). types.TableView 대신 md —
    Object.md(markdown, name=...)의 첫 인자가 실제 표시 내용이라 여기서 직접 조립한다.

    join_field_missing: Task 12 — 문장 클릭의 버전→조인 필드 매핑이 세션 데이터셋에
    없을 때 그 필드명. 표 위에 안내만 붙이고 표 자체(선택 문장 메타)는 그대로 보여준다
    (조인 실패 ≠ 문장 정보 없음).
    total: 전체 선택 문장 수 — rows가 상한으로 잘렸으면 표 위에 표기 (lasso 다중선택).
    """
    note = ""
    if join_field_missing:
        note = (f"*(조인 필드 없음: `{join_field_missing}` — 이 데이터셋에 해당 필드가 없어 "
                f"프레임 하이라이트를 건너뜁니다)*\n\n")
    if total and total > len(rows):
        note += f"*(선택 {total}개 중 상위 {len(rows)}개 표시)*\n\n"
    if not rows:
        return note + "*(선택된 프레임 없음)*"
    header = "| gidx | text | wins | purity | n_cameras | wave_gain |\n|---|---|---|---|---|---|\n"
    body = "".join(
        f"| {r['gidx']} | {str(r['text']).replace('|', chr(92) + '|')} | {r['wins']} | {r['purity']:.3f} | "
        f"{r['n_cameras']} | {r['wave_gain']:.3f} |\n"
        for r in rows
    )
    return note + header + body


class PromptComparePanel(foo.Panel):
    @property
    def config(self):
        return foo.PanelConfig(name="user_prompt_compare",
                               label="Prompt Compare", surfaces="grid")

    def on_load(self, ctx):
        ctx.panel.state.rule = "argmax_k1"
        ctx.panel.state.show_unadopted = True
        ctx.panel.state.selected_gidx = []
        ctx.panel.state.sel_total = 0
        ctx.panel.state.mode = "A"           # "A"|"B" — Task 9, 스펙 §5.1b
        ctx.panel.state.group_field = "project"
        ctx.panel.state.groups = ""
        # Task 12 — 뱅크 버전 선택기 + 프롬프트 데이터셋 자동 유도 상태.
        ctx.panel.state.bank_version_filter = ALL_VERSIONS_LABEL
        ctx.panel.state.bank_versions = []
        ctx.panel.state.prompts_available = True
        ctx.panel.state.join_field_missing = None
        self._refresh(ctx)

    def _sync_controls(self, ctx):
        """컨트롤 드롭다운 표시값을 서버 상태에서 밀어넣는다 (매 _refresh).

        컨트롤은 h_stack("controls") 아래 중첩 — 가로 한 줄 배치는 이 중첩만 동작한다
        (flat + view.space는 패널 오브젝트 렌더러가 무시, 2026-08-10 실측: 스키마 JSON에
        space가 실려도 select 4개가 각각 full-width 세로 스택). 중첩 property 는 state 도
        중첩 경로("controls.mode")에서 읽으므로, 여기서 정본(flat) 상태를 미러링해 준다 —
        서버가 매번 밀어넣으니 클라이언트 form 값과의 desync 도 함께 방지된다.
        """
        ctx.panel.state.set("controls", {
            "mode": ctx.panel.state.mode or "A",
            "rule": ctx.panel.state.rule or "argmax_k1",
            "show_mode": SHOW_ALL_LABEL if ctx.panel.state.show_unadopted else SHOW_ADOPTED_LABEL,
            "bank_version_filter": ctx.panel.state.bank_version_filter or ALL_VERSIONS_LABEL,
            "group_field": ctx.panel.state.group_field or "project",
            "groups": ctx.panel.state.groups or "",
        })

    def _refresh(self, ctx, update_plot=True):
        """update_plot=False = 성능 옵션: 플롯 상태(scatter_data/layout, 12k점)를 다시 쓰지
        않고 표(top_table)·컨트롤만 갱신한다. 선택 계열 훅은 뷰 변경이 어차피 재렌더를
        유발하므로 이중 재렌더를 피하려 False를 쓴다.

        ⚠️ 리로드 버그의 최종 진단(2026-08-10, on_change_extended_selection 주석 참고):
        emb_viz extendedSelection 파괴의 트리거는 재렌더도 상태 쓰기도 아니고 **이 패널에
        on_change 훅이 등록돼 있어 selection 변화 시 발생하는 훅 EXEC 왕복 그 자체**다
        (훅 바디 no-op이어도 파괴, 등록 제거 프로브만 생존). 한때 update_plot=False가
        파괴를 막는 것처럼 보였던 실측은 호스트 load 190 교란(파괴 측 서버 왕복이 부하로
        실패)이었다 — update_plot은 파괴 방지 수단이 아니다. 방어는 "받은 선택을 즉시
        뷰로 승격 + 빈 에코 무시" (_select_frames_view / on_change_extended_selection).
        """
        self._sync_controls(ctx)
        # 과거 set_data(patch_panel_data)로 심어진 패널 데이터가 세션에 영속되어, 있으면
        # 스키마 data를 영원히 가린다(App 번들 `mt||view.data` 우선순위). set_data를 안 쓰는
        # 지금도 옛 세션의 잔재가 남아 있으므로 매 갱신마다 비워 스키마 경로만 살린다.
        ctx.panel.data.clear()
        if ctx.panel.state.mode == "B":
            # 모드 B는 ctx.dataset(현재 세션 데이터셋)을 그린다 — sourcei(ground_truth 등)에서도
            # 열리지만 본용도는 frames_captions에서 project 간 비교.
            groups = [g.strip() for g in (ctx.panel.state.groups or "").split(",") if g.strip()]
            group_field = ctx.panel.state.group_field or "project"
            if groups and ctx.dataset is not None:
                fig = build_mode_b(ctx.dataset.name, group_field, groups)
            else:
                fig = {"data": [],
                       "layout": {"title": {"text": f"{BANNER_COORDS_B}<br><sup>"
                                                     "그룹을 쉼표로 구분해 입력하세요</sup>",
                                             "font": {"size": 12}},
                                  "xaxis": {"visible": False}, "yaxis": {"visible": False}}}
            # set_data 금지 — 아래 모드 A 쪽 주석 참고 (patch=딥머지라 줄어든 배열이 안 지워짐).
            ctx.panel.state.layout = fig["layout"]
            ctx.panel.state.scatter_data = fig["data"]
            ctx.panel.state.top_table = []
            ctx.panel.state.sel_total = 0
            return

        # Task 12: 프롬프트 데이터셋 자동 유도 — "<세션 데이터셋>-prompts". 없으면 크래시
        # 대신 안내 배너만 그리고 모드 A 컨트롤(규칙/미채택/버전)은 render()에서 숨긴다.
        prompts_name = _prompts_dataset_name(ctx)
        if not fo.dataset_exists(prompts_name):
            ctx.panel.state.prompts_available = False
            ctx.panel.state.bank_versions = []
            fig = {"data": [],
                   "layout": {"title": {"text": NO_PROMPTS_PAIR_TEXT, "font": {"size": 12}},
                              "xaxis": {"visible": False}, "yaxis": {"visible": False}}}
            ctx.panel.state.layout = fig["layout"]
            ctx.panel.state.scatter_data = fig["data"]
            ctx.panel.state.top_table = []
            ctx.panel.state.sel_total = 0
            return

        ctx.panel.state.prompts_available = True
        b = load_prompt_bundle(prompts_name)
        bv = b.get("bank_version")
        ctx.panel.state.bank_versions = sorted({str(v) for v in bv if v is not None}) \
            if bv is not None else []
        version_filter = ctx.panel.state.bank_version_filter or ALL_VERSIONS_LABEL
        sel = set(ctx.panel.state.selected_gidx or [])
        if update_plot:
            fig = build_mode_a(b, rule=ctx.panel.state.rule,
                               show_unadopted=ctx.panel.state.show_unadopted,
                               selected_gidx=sel, bank_version_filter=version_filter)
        # ⚠️ set_data 사용 금지 (사용자 피드백 라운드 실측, 2026-08-07): set_data →
        # patch_panel_data 는 클라이언트 패널 데이터 저장소에 **딥머지(patch)** 된다 —
        # 배열이 줄어드는 갱신(미채택 숨김: 12,166→0, 버전 필터: 전체→부분집합)에서 새
        # 짧은 배열이 옛 긴 배열의 꼬리를 못 지워 유령 점이 화면에 남는다(토글 후 trace0
        # n=12,279 잔존 실측). 반면 render()가 스키마에 굽는 data(아래 scatter_data)는
        # show_panel_output 마다 통째로 교체되므로, 데이터 갱신은 이 경로 하나만 쓴다.
        # (한 번이라도 set_data 를 부르면 클라이언트가 patched data 를 스키마 data 보다
        # 우선하므로 — App 번들 `wo=mergeData(mt||Lt?.view?.data,…)` — 부분 도입도 불가.)
        if update_plot:
            ctx.panel.state.layout = fig["layout"]
            ctx.panel.state.scatter_data = fig["data"]
        rows = []
        if sel:
            import numpy as np
            # 상한 50 (구 20): lasso 다중선택 도입으로 수십 개 선택이 일상 — 표가 내부 스크롤을
            # 갖게 돼(render의 maxHeight) 행이 늘어도 레이아웃을 밀지 않는다.
            # 정렬 = wins 내림차순: 넓은 box select는 미채택(wins 0)이 다수라 gidx순으로는
            # 승자 문장이 상한 밖으로 밀린다 (2026-08-10 실측: 7,716개 선택 중 대부분 미채택).
            idxs = np.nonzero(np.isin(b["gidx"], np.asarray(sorted(sel))))[0]
            for i in idxs[np.argsort(-b["wins"][idxs].astype(int), kind="stable")][:50]:
                i = int(i)
                rows.append({"gidx": int(b["gidx"][i]), "text": str(b["text"][i]),
                             "wins": int(b["wins"][i]), "purity": float(b["purity"][i]),
                             "n_cameras": int(b["n_cameras"][i]),
                             "wave_gain": float(b["wave_gain"][i])})
        ctx.panel.state.top_table = rows
        ctx.panel.state.sel_total = len(sel)

    # ── 프레임 → 문장 : Samples 그리드 체크박스 (Task 5 실측 확정 훅) ──
    #    (실측, Task 8) App은 패널 오퍼레이터가 하나라도 실행되면 등록된 on_change_* 훅
    #    전부를 "현재 값"으로 재발화한다 — 값이 실제로 안 바뀌어도 재발화됨. 마지막으로 처리한
    #    시그니처와 같으면 무시해야 다른 훅(on_plot_click 등)이 막 세팅한 상태를 덮어쓰지 않는다.
    #    가드 상태는 `_dedup_guard`로 위임 (밑줄 없는 상태 키 필수 — 헬퍼 docstring 참고).
    def on_change_selected(self, ctx):
        ids = ctx.selected or []
        if _dedup_guard(ctx, "sel_seen", ids):
            return
        ctx.panel.state.join_field_missing = None  # 이전 클릭 안내는 새 프레임 선택과 무관
        # Task 12: 프레임 데이터셋은 항상 "현재 세션 데이터셋"(ctx.dataset) — 예전처럼
        # 하드코딩된 FRAMES_DATASET("sourcei")로 고정하면 source-h 세션에서 오조인된다.
        frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
        winner_field = _current_winner_field(ctx)
        ctx.panel.state.selected_gidx = \
            frame_ids_to_gidx(ids, dataset_name=frames_name, winner_field=winner_field) if ids else []
        self._refresh(ctx, update_plot=False)   # 플롯 재쓰기 금지 — _refresh docstring(리로드 버그)

    # ── 프레임 → 문장 : 네이티브 Embeddings lasso (Task 5 실측 — on_change_selected는
    #    lasso에 반응하지 않는다, 0 ids 유지. lasso는 이 훅으로만 온다.
    #    payload = {"selection": [sample_id, ...], "scope": "global"|None, ...}) ──
    def on_change_extended_selection(self, ctx):
        ext = ctx.extended_selection or {}
        ids = ext.get("selection") or []
        if not ids:
            # 빈 전이 무시 (2026-08-10 리로드 버그 최종 진단): 이 패널에 on_change 훅이
            # 등록돼 있으면 **훅 EXEC 왕복 자체가** (바디가 no-op이어도) 네이티브 emb_viz의
            # extendedSelection을 수 초 내 소거한다 — 훅 등록을 지운 프로브만 생존, App
            # 버그라 Python에서 못 막는다. 그래서 ① 아래에서 받은 ids를 즉시 뷰(Select
            # stage)로 승격해 소거와 무관한 진실을 만들고 ② 소거가 만드는 빈 에코는 여기서
            # 삼킨다. 진짜 해제는 플롯 더블클릭·그리드 체크 해제 경로가 담당.
            return
        if _dedup_guard(ctx, "ext_sel_seen", ids):
            return
        ctx.panel.state.join_field_missing = None
        frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
        winner_field = _current_winner_field(ctx)
        ctx.panel.state.selected_gidx = \
            frame_ids_to_gidx(ids, dataset_name=frames_name, winner_field=winner_field)
        # 뷰 승격: extendedSelection은 이 훅의 EXEC 응답이 돌아올 즈음 App이 소거한다(위
        # 주석) — 같은 프레임 집합을 Select 뷰로 다시 걸면 그리드 필터·emb_viz 반영이
        # 안정적으로 유지된다 (역방향과 동일 종착 상태, 해제=더블클릭).
        self._select_frames_view(ctx, sorted(ids))
        self._refresh(ctx, update_plot=False)   # 성능: 표·컨트롤만 갱신 (뷰 변경이 재렌더 유발)

    # ── 문장 → 프레임 ──
    def on_plot_click(self, ctx):
        # ctx.params["id"] ← trace.ids[pointIndex] (App 번들 getIdForTrace 실측).
        # data.customdata 는 onClick 이벤트에 아예 실리지 않는다 — 브리프 원문의 가정은 틀렸다.
        if ctx.panel.state.mode != "A":
            return  # 모드 B trace는 ids를 싣지 않아 프레임 귀속이 없다 (교차 데이터셋 조인 아님)
        raw_id = (ctx.params or {}).get("id")
        if raw_id is None:
            return
        g = int(raw_id)
        if ctx.panel.state.rule != "argmax_k1":
            return  # dist_iou 모드: 귀속 없음 — 클릭 무효 (배너가 안내)
        # Task 12: 클릭된 "그 문장 row"의 bank_version에서 조인 필드를 유도한다 — 패널의
        # 전역 버전 필터가 아니라 그 문장 자체가 속한 버전 기준(요구사항 3). 필드가 세션
        # 데이터셋 스키마에 없으면 "조인 필드 없음" 안내로 무효 처리하고 크래시하지 않는다.
        prompts_name = _prompts_dataset_name(ctx)
        if not fo.dataset_exists(prompts_name):
            return
        import numpy as np
        b = load_prompt_bundle(prompts_name)
        idxs = np.where(b["gidx"] == g)[0]
        if len(idxs) == 0:
            return
        version_str = str(b["bank_version"][int(idxs[0])]) if b.get("bank_version") is not None else None
        join_field = _resolve_join_field(ctx.dataset, version_str)
        ctx.panel.state.selected_gidx = [g]
        if join_field is None:
            ctx.panel.state.join_field_missing = \
                version_to_winner_field(version_str) if version_str else "?"
        else:
            ctx.panel.state.join_field_missing = None
            frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
            ids = gidx_to_frame_ids(g, dataset_name=frames_name, winner_field=join_field)
            if ids:
                self._select_frames_view(ctx, ids)   # 뷰 기반 반영 — 헬퍼 docstring 참고
        # 전체 갱신(하이라이트 포함) — 이 방향은 뷰 기반이라 재렌더가 파괴할 extended
        # selection이 없다 (사용자 피드백: 선택했으면 시각적으로 표시돼야 한다).
        # emb_viz 선택을 소비하는 on_change_* 훅과 달리 update_plot=False가 불필요.
        self._refresh(ctx)

    # ── 문장 → 프레임 : Plotly lasso/box select (modebar 로 드래그 모드 전환) ──
    # 네이티브 Embeddings 패널의 g/s 단축키는 App 번들 React 컴포넌트 내부 하드코딩이라
    # Python 패널 API(on_load/on_startup 뿐, PanelConfig에 hotkey 없음 — 1.19.0 실측)로는
    # 재현 불가. 대신 modebar를 상시 표시(render의 displayModeBar)해 pan↔lasso↔box 전환은
    # 클라이언트에서 즉시 되게 하고, 선택 이벤트를 이 훅으로 받아 클릭과 동일하게 조인한다.
    def on_plot_selected(self, ctx):
        # PlotlyView onSelected: ctx.params["data"] = [{"trace","trace_idx","idx","id",...}]
        # — id = trace.ids[idx] (on_click과 같은 계약, PlotlyView docstring 실측).
        if ctx.panel.state.mode != "A" or ctx.panel.state.rule != "argmax_k1":
            return  # 모드 B/dist_iou: 프레임 귀속 없음 — 클릭과 동일하게 무효
        items = (ctx.params or {}).get("data") or []
        ids = sorted({int(d["id"]) for d in items if d.get("id") is not None})
        if not ids:
            # scattergl box select는 mouseup에 plotly_selected를 두 번 쏜다 — 점 있는 이벤트
            # 직후 빈 이벤트(실측 2026-08-10: selected 6666 → selected 0 연속). 빈 payload가
            # 방금 만든 선택을 지우면 box select가 "안 되는" 것처럼 보인다. 해제는
            # on_plot_double_click(플롯 더블클릭)과 그리드 선택 해제 경로가 담당.
            return
        # dedup 가드 없음 — on_click과 같은 plot 이벤트라 App 재발화(on_change_* 한정, Task 8)
        # 대상이 아니고, 가드를 걸면 클릭으로 상태가 바뀐 뒤 같은 영역 재-lasso가 삼켜진다.
        ctx.panel.state.selected_gidx = ids
        ctx.panel.state.join_field_missing = None
        prompts_name = _prompts_dataset_name(ctx)
        if ids and fo.dataset_exists(prompts_name):
            import numpy as np
            b = load_prompt_bundle(prompts_name)
            # 문장별 bank_version → 조인 필드로 버킷팅 (on_plot_click과 같은 per-문장 규칙).
            # 성능(codex 리뷰): lasso는 미채택 포함 최대 MAX_POINTS개 — gidx당 np.where 풀스캔
            # 대신 gidx→row 딕셔너리 1회 + 버전→조인필드 메모이즈로 O(n+k).
            row_of = {int(v): i for i, v in enumerate(b["gidx"])}
            jf_of_version = {}
            by_field = {}
            for g in ids:
                i = row_of.get(g)
                if i is None:
                    continue
                vs = str(b["bank_version"][i]) if b.get("bank_version") is not None else None
                if vs not in jf_of_version:
                    jf_of_version[vs] = _resolve_join_field(ctx.dataset, vs)
                jf = jf_of_version[vs]
                if jf is None:
                    ctx.panel.state.join_field_missing = \
                        version_to_winner_field(vs) if vs else "?"
                    continue
                by_field.setdefault(jf, []).append(g)
            frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
            frame_ids = []
            for jf, gs in by_field.items():
                frame_ids += gidxes_to_frame_ids(gs, dataset_name=frames_name, winner_field=jf)
            # 중복 제거(codex 리뷰): 같은 프레임이 버전별 winner 필드 양쪽의 승자면 버킷 두 개에서
            # 두 번 들어온다 — 중복이 남으면 클라이언트가 dedup해 돌려줄 때 에코 선점 비교
            # (sorted 리스트 동등)가 어긋나 진짜 처리로 오인된다.
            frame_ids = sorted(set(frame_ids))
            if frame_ids:
                self._select_frames_view(ctx, frame_ids)
        # 전체 갱신(하이라이트 포함) — 뷰 기반이라 재렌더가 파괴할 extended selection이
        # 없다 (사용자 피드백: box select 후 선택 표시가 보여야 한다). update_plot=False는
        # emb_viz의 extended selection을 소비하는 on_change_* 훅에만 필요 (_refresh docstring).
        self._refresh(ctx)

    def _select_frames_view(self, ctx, frame_ids):
        """문장→프레임 반영은 extended selection이 아니라 **뷰(Select stage)** 로 건다.

        extended selection은 scope를 "global"로 주든 emb_viz 자체 scope로 주든 네이티브
        Embeddings 패널의 내부 동기화 기계가 ~10초 뒤 스스로 지운다 (2026-08-10 실측:
        두 scope 모두 그리드 필터→복귀→선택 소멸; emb_viz 패널을 닫으면 안 지워짐 =
        그 패널이 소거 주체. App 번들이라 Python에서 수정 불가). 뷰 경로는 자가 소거
        기계가 없고, 그리드·Embeddings 패널 모두 선택 프레임만 보여주는 형태로 반영된다
        (사용자 요청: 관련 이미지가 samples/embeddings에 표시). 해제 = 플롯 더블클릭."""
        frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
        base = ctx.view if getattr(ctx, "view", None) is not None else fo.load_dataset(frames_name)
        ctx.ops.set_view(view=base.select(frame_ids))

    def on_plot_double_click(self, ctx):
        """플롯 더블클릭 = 선택 해제 (plotly 표준 UX) — 빈 plotly_selected는 무시하므로
        (on_plot_selected 주석) 명시적 해제 경로는 이 훅과 그리드 선택 해제 둘이다.
        clear_view는 커스텀 뷰까지 전체 해제한다 — Select stage만 제거하는 정교함은
        base view 직렬화 추적이 필요해 보류 (사용자 뷰는 view bar에서 복원 가능)."""
        if ctx.panel.state.mode != "A":
            return
        ctx.panel.state.selected_gidx = []
        ctx.panel.state.join_field_missing = None
        ctx.panel.state.set("ext_sel_seen", [])   # 빈 에코 선점 (그리드 해제와 동일 규약)
        ctx.ops.clear_view()
        self._refresh(ctx)   # 전체 갱신 — 하이라이트 링 즉시 제거 (뷰 기반이라 재렌더 무해)

    # ── 컨트롤 드롭다운 핸들러 (2026-08-10 피드백: 토글 버튼 → 드롭다운 통일).
    #    값은 ctx.params["value"] (아래 on_group_field_change 주석의 실측 계약과 동일).
    def on_mode_change(self, ctx):
        v = ctx.params.get("value")
        if v in ("A", "B"):
            ctx.panel.state.mode = v
            self._refresh(ctx)

    def on_rule_change(self, ctx):
        v = ctx.params.get("value")
        if v in ("argmax_k1", "dist_iou"):
            ctx.panel.state.rule = v
            self._refresh(ctx)

    def on_show_change(self, ctx):
        # 화이트리스트 (codex 3차 리뷰): 두 라벨 밖의 값이 조용히 "채택만"으로 폴백되면 안 된다
        values = {SHOW_ALL_LABEL: True, SHOW_ADOPTED_LABEL: False}
        v = (ctx.params or {}).get("value")
        if v not in values:
            return
        ctx.panel.state.show_unadopted = values[v]
        self._refresh(ctx)

    # 실측(fiftyone-plugins panel-examples InputsExample/DropdownMenuExample): Property-level
    # on_change 콜백은 바뀐 값을 ctx.params["value"]로만 전달한다 — 브리프 원문의
    # ctx.params["group_field"]/["groups"] 키 가정은 틀렸다(그 키들은 애초에 존재하지 않음).
    # 필드 2개가 같은 시그니처를 공유하므로 어느 쪽이 바뀌었는지 값만으로는 구분 불가 —
    # 필드별로 전용 핸들러를 둔다.
    def on_group_field_change(self, ctx):
        v = ctx.params.get("value")
        if v is not None:
            ctx.panel.state.group_field = v
        self._refresh(ctx)

    def on_groups_change(self, ctx):
        v = ctx.params.get("value")
        if v is not None:
            ctx.panel.state.groups = v
        self._refresh(ctx)

    # Task 12 — 뱅크 버전 드롭다운. Task 9 조사로 확정된 계약과 동일하게 값은
    # ctx.params["value"]로 온다(패널 예제 InputsExample/DropdownMenuExample 실측,
    # 필드명 키 가정 아님).
    def on_bank_version_change(self, ctx):
        v = ctx.params.get("value")
        if v is not None:
            ctx.panel.state.bank_version_filter = v
            ctx.panel.state.join_field_missing = None  # 버전 전환 시 이전 클릭 안내는 무효
        self._refresh(ctx)

    def render(self, ctx):
        panel = types.Object()
        # 컨트롤 = 드롭다운 4개를 h_stack 한 줄에 (2026-08-10 피드백 ×3: ① 세로 스택이 수직
        # 공간을 잡아먹어 하단 표가 뷰포트 밖으로 밀림, ② 버튼/드롭다운 혼재와 뱅크 버전의
        # 어색한 위치 → 라벨 있는 드롭다운으로 통일, ③ 폭은 글 크기에 맞게 — h_stack이 내용
        # 폭으로 잡아준다). flat + view.space 는 패널 렌더러가 무시(실측), h_stack 중첩의
        # state 바인딩 단절은 _sync_controls 의 중첩 경로 미러링으로 해결 — 그쪽 주석 참고.
        row = panel.h_stack("controls", gap=2, align_y="center")
        mode_choices = types.Choices()
        mode_choices.add_choice("A", label="A — 문장↔프레임")
        mode_choices.add_choice("B", label="B — 그룹 overlay")
        row.enum("mode", mode_choices.values(), label="모드", view=mode_choices,
                 on_change=self.on_mode_change)
        if ctx.panel.state.mode == "B":
            row.str("group_field", allow_empty=True,
                    label="그룹 필드 (기본 project)",
                    on_change=self.on_group_field_change)
            row.str("groups", allow_empty=True,
                    label="그룹들 (쉼표구분, 예: cohort-b,cohort-a)",
                    on_change=self.on_groups_change)
        elif ctx.panel.state.prompts_available:
            rule_choices = types.Choices()
            rule_choices.add_choice("argmax_k1", label="argmax_k1 — 클릭·lasso 조인")
            rule_choices.add_choice("dist_iou", label="dist_iou — wave 기여도")
            row.enum("rule", rule_choices.values(), label="규칙", view=rule_choices,
                     on_change=self.on_rule_change)
            show_choices = types.Choices()
            show_choices.add_choice(SHOW_ALL_LABEL, label=SHOW_ALL_LABEL)
            show_choices.add_choice(SHOW_ADOPTED_LABEL, label=SHOW_ADOPTED_LABEL)
            row.enum("show_mode", show_choices.values(), label="표시", view=show_choices,
                     on_change=self.on_show_change)
            # 뱅크 버전 선택기: "전체" + 실제 프롬프트 데이터셋의 distinct bank_version.
            # sourcei-prompts/source-h-prompts 둘 다 오늘(2026-08-07) 기준 단일 버전(v1.0.8.0)만
            # 갖고 있지만, 코드는 값 개수에 의존하지 않고 일반적으로 동작한다.
            choices = types.Choices()
            choices.add_choice(ALL_VERSIONS_LABEL, label=ALL_VERSIONS_LABEL)
            for v in (ctx.panel.state.bank_versions or []):
                choices.add_choice(v, label=v)
            row.enum("bank_version_filter", choices.values(), label="뱅크 버전",
                     view=choices, on_change=self.on_bank_version_change)
        else:
            row.md(NO_PROMPTS_PAIR_TEXT, name="no_prompts_notice")
        # data=... (Task 11 "클릭해야 나온다" 방어선 — 2차 안전망):
        # 실사용 버그의 **1차·확정 원인은 이 파일이 아니라 fiftyone_app_setup.py 쪽**이었다 —
        # cmd_workspace_compare()가 만드는 Space에 active_child를 안 채워서, 워크스페이스
        # 로드시 이 패널의 on_load 오퍼레이터 자체가 한 번도 실행되지 않았다(네트워크 로그로
        # 확인: load_workspace 이후 이 패널의 /operators/execute가 전혀 안 나가다가 패널 탭을
        # 클릭한 순간에야 첫 execute 발생). active_child 수정으로 그 근본 원인은 해결됐다.
        # 다만 on_load가 정상 발화하는 경우에도 잠재 경합이 하나 더 있다(실측, docker-analysis-1
        # 배포본 index-CFYL-qQX.js 역공학): set_data()는 patch_panel_data 오퍼레이터를 거쳐
        # `setTimeout(fn, 1)`로, render()가 만든 스키마는 show_panel_output 오퍼레이터를 거쳐
        # *또 다른* `setTimeout(fn, 1)`로 — 서로 다른 타이머로 큐잉되어 도착 순서가 항상
        # 보장되진 않는다(클릭 이후 재렌더는 패널이 이미 마운트된 상태라 이 경합이 안 걸림 —
        # Task 8 실측과 합치). PlotlyView는 `data ?? schema.view.data`로 폴백하므로(App 번들
        # PlotlyView 컴포넌트, `wo=mergeData(mt||Lt?.view?.data,...)`), show_panel_output
        # 스키마에 data를 직접 구워 넣으면 이 잠재 경합과도 무관하게 항상 채워진다.
        # 사용자 피드백 라운드(2026-08-07)부터 set_data는 아예 쓰지 않는다 — patch 딥머지가
        # 줄어든 배열을 못 지우는 문제까지 겹쳐, 스키마 data가 유일한 갱신 경로다(_refresh 주석).
        # height(사용자 피드백): 고정 800px는 큰 화면에서 아래 공간을 놀리고 작은 화면에선
        # 스크롤을 만든다. PlotlyView는 이 kwarg를 plotly div의 style.height로 그대로 쓰므로
        # (App 번들: bo=Yn?.height||"100%") vh 단위가 동작한다 — 뷰포트가 크면 크게, 작으면
        # 작게, 단독/분할(가로 분할이라 세로 공간 동일) 모두 자동 반응. 360px = 상단 컨트롤
        # +탭바+하단 표 예산, 480px = 최소 보장. config.responsive로 창 리사이즈도 추적.
        # 프로퍼티 키 "scatter_v2": 옛 배포가 set_data("scatter")로 세션 저장소에 영속시킨
        # patched data가 리로드 후에도 스키마 data를 가리는 문제(위 주석)의 결정적 우회 —
        # 키가 다르면 저장된 patch가 아예 매칭되지 않는다. data.clear()는 잔재 정리용 보조.
        # displayModeBar 상시 표시: pan↔lasso↔box 전환 버튼 (g/s 단축키 대체 — 단축키는
        # App 번들 전용이라 Python 패널에서 불가, on_plot_selected 주석 참고). 전환은
        # 클라이언트 즉시(서버 왕복 없음), 선택하면 on_selected로 문장→프레임 조인.
        # 높이 예산(사용자 피드백 2026-08-10): 구 360px 예산은 표를 뷰포트 밖으로 밀었다 —
        # 500px = 탭바+컨트롤 한 줄+하단 표(maxHeight 240px, 아래) 몫. 표가 항상 같이 보인다.
        panel.plot("scatter_v2", data=ctx.panel.state.scatter_data or [],
                   layout=ctx.panel.state.layout or {},
                   height="max(420px, calc(100vh - 500px))",
                   config={"responsive": True, "displayModeBar": True},
                   on_click=self.on_plot_click,
                   on_selected=self.on_plot_selected,
                   on_double_click=self.on_plot_double_click)
        if ctx.panel.state.mode != "B":
            # 표 내부 스크롤: 문장이 많아도(lasso 다중선택) 패널 전체가 아니라 표 안에서만
            # 스크롤한다 — componentsProps.container는 SchemaIO 표준 래퍼 prop(App 번들
            # getComponentProps(ctx,"container") 실측)이라 sx가 그대로 먹는다.
            panel.md(_rows_to_markdown(ctx.panel.state.top_table, ctx.panel.state.join_field_missing,
                                       total=ctx.panel.state.sel_total),
                     name="table_md", label="선택 프레임의 승자 문장",
                     componentsProps={"container": {
                         "sx": {"maxHeight": "240px", "overflowY": "auto"}}})
        return types.Property(panel, view=types.GridView())


def register(p):
    p.register(PromptComparePanel)


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
    # 불변식 4 (codex 3차 리뷰): gidx 전역 유일 — row_of 딕셔너리/np.where 단일행 전제.
    # 다중 bank_version 백필이 이걸 깨면 클릭·lasso 조인이 임의 행을 잡는다.
    assert len(b["gidx"]) == len({int(g) for g in b["gidx"]}), "gidx 전역 유일성 붕괴"

    # 조인 왕복: 임의 채택 문장 → 프레임들 → 도로 그 문장
    g = int(b["gidx"][np.argmax(b["wins"])])
    ids = gidx_to_frame_ids(g)
    assert ids and set(frame_ids_to_gidx(ids)) == {g}

    # 일괄 조인(lasso 다중선택 경로): 단건 조인의 합집합과 동일해야 한다
    top2 = [int(b["gidx"][i]) for i in np.argsort(b["wins"])[-2:]]
    assert set(gidxes_to_frame_ids(top2)) == \
        set(gidx_to_frame_ids(top2[0])) | set(gidx_to_frame_ids(top2[1]))

    # 회색 계열 금지 (사용자 피드백 2026-08-10): 미채택(GREY)·중간·smoke가 전부 무채색이라
    # 안 구분되던 회귀 방지 — 채택 팔레트(CLASS/WAVE_ROLE)는 유채색만, GREY 재사용 금지.
    def _greyish(c):
        r, gr, bl = (int(c[i:i + 2], 16) for i in (1, 3, 5))
        return max(r, gr, bl) - min(r, gr, bl) < 30
    for c in list(CLASS_COLORS.values()) + list(WAVE_ROLE_COLORS.values()):
        assert not _greyish(c), f"채택 팔레트에 회색 계열 색 {c}"
    assert GREY not in set(CLASS_COLORS.values()) | set(WAVE_ROLE_COLORS.values())
    assert len(set(CLASS_COLORS.values())) == len(CLASS_COLORS)          # 팔레트 내 중복 금지
    assert len(set(WAVE_ROLE_COLORS.values())) == len(WAVE_ROLE_COLORS)

    # 층화 서브샘플: 상한 준수 + 전 클래스 보존
    labs = ["a"] * 100 + ["b"] * 10
    idx = stratified_subsample(labs, 20)
    assert len(idx) <= 20 and {labs[i] for i in idx} == {"a", "b"}

    # 모드 A figure: 규칙별 계약 — trace 구조 [0]미채택 [1..k]채택(그룹별) [-1]선택
    fig = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx={g})
    assert all(t["type"] == "scattergl" for t in fig["data"])           # scattergl 강제
    n_shown = sum(len(t["x"]) for t in fig["data"][:-1])
    assert n_shown == len(b["gidx"])                                     # 12,480 전체 표시
    assert BANNER_RULE in fig["layout"]["title"]["text"]                 # 규칙 배너
    # 반응형 height 계약: layout에 height 고정 금지 — 실높이는 render()의 view height
    # (vh 기반 style)가 정한다. 고정값이 부활하면 큰 화면에서 아래 공간이 다시 논다.
    assert "height" not in fig["layout"] and fig["layout"]["autosize"] is True
    # 선택 하이라이트는 다크 배경에서 보이는 색이어야 한다 (#000000 회귀 방지)
    assert fig["data"][-1]["marker"]["color"] == "#F0E442"
    # 범례 회귀 가드 (2026-08-10 fix): 채택은 클래스별 trace — 색은 단일 문자열이어야 한다.
    # per-point 색 배열이 부활하면 범례에 클래스→색 매핑이 다시 사라진다(파랑 normal이
    # 화면에 있어도 범례엔 주황 글리프 하나뿐이던 버그).
    adopted_traces = fig["data"][1:-1]
    assert adopted_traces, "채택 trace 0개"
    assert all(isinstance(t["marker"]["color"], str) for t in adopted_traces), \
        "회귀: 채택 trace가 per-point 색 배열 — 범례에 클래스 매핑이 안 나온다"
    assert sum(len(t["x"]) for t in adopted_traces) == int(b["adopted"].astype(bool).sum())
    cats_adopted = {str(c) for c, a in zip(b["category"], b["adopted"].astype(bool)) if a}
    assert {t["name"].rsplit(" ", 1)[0] for t in adopted_traces} == cats_adopted, \
        "범례 이름(<클래스> <개수>)이 채택 클래스 집합과 불일치"
    assert all(t["marker"]["color"] == CLASS_COLORS.get(t["name"].rsplit(" ", 1)[0], "#999999")
               for t in adopted_traces)                                  # 범례 글리프 색 = 팔레트 색
    fig_w = build_mode_a(b, rule="dist_iou", show_unadopted=True, selected_gidx=set())
    assert BANNER_WAVE_NOCLICK in fig_w["layout"]["title"]["text"]       # 귀속 없음 안내
    w_traces = fig_w["data"][1:-1]
    assert all(isinstance(t["marker"]["color"], str) for t in w_traces)
    assert any(t["marker"]["color"] != "#999999" for t in w_traces), \
        "dist_iou 채택 trace 전체 회색 — wave_role 색 매핑 누락 의심"
    fig_h = build_mode_a(b, rule="argmax_k1", show_unadopted=False, selected_gidx=set())
    # 숨김 = visible:False (빈 배열 아님 — 클라이언트 patch 딥머지가 옛 점을 못 지운다).
    # 배열 길이는 전체 유지, 플래그만 뒤집혀야 한다.
    assert fig_h["data"][0]["visible"] is False
    assert len(fig_h["data"][0]["x"]) == int((~b["adopted"].astype(bool)).sum())
    assert sum(len(t["x"]) for t in fig_h["data"][1:-1]) == int(b["adopted"].sum())
    assert "표시: 채택만" in fig_h["layout"]["title"]["text"]            # 숨김 상태 배너 명시
    assert "(숨김)" in fig_h["data"][0]["name"]                          # 범례에도 상태 표기

    # ── Task 12: 버전 → 조인 필드 매핑 함수 단위 검증 (지시된 예시 그대로) ──
    assert version_to_winner_field("v1.0.8.0") == "winner_gidx_v080"
    assert version_to_winner_field("v1.0.8.4") == "winner_gidx_v084"
    assert version_to_winner_field("v1") == "winner_gidx_v001"   # 짧은 입력도 크래시 없이 zfill

    class _FakeSchemaDS:
        def __init__(self, fields):
            self._fields = set(fields)
        def get_field_schema(self):
            return {f: None for f in self._fields}

    fake_ds = _FakeSchemaDS(["winner_gidx_v080"])
    assert _resolve_join_field(fake_ds, "v1.0.8.0") == "winner_gidx_v080"
    assert _resolve_join_field(fake_ds, "v1.0.8.4") is None      # 필드 없음 → None(크래시 아님)
    assert _resolve_join_field(None, "v1.0.8.0") is None         # 데이터셋 없음 → None

    # 실측 검증(2026-08-07): sourcei/source-h 프레임 데이터셋 실제 스키마와 대조.
    # sourcei는 winner_gidx_v080만 갖는다(기존 VTAG 기본값과 일치).
    assert _resolve_join_field(frames, "v1.0.8.0") == WINNER_FIELD
    if fo.dataset_exists("source-h"):
        sourceh_frames = fo.load_dataset("source-h")
        sourceh_schema = sourceh_frames.get_field_schema()
        assert "winner_gidx_v080" in sourceh_schema
        assert _resolve_join_field(sourceh_frames, "v1.0.8.0") == "winner_gidx_v080"
        # ⚠️ 브리프 가정("source-h은 v080/v084 둘 다 있을 것")과 달리, 실측상 source-h 프레임
        # 스키마엔 winner_gidx_v084 필드 자체가 없다(v084 관련 다른 파생 필드는 있음 —
        # rule_flip_v084/winner_loo_v084/wave_iou_*_v084 등). 조인 가드가 크래시 대신
        # None을 반환해야 하는 정확히 그 케이스 — 회귀 가드로 고정한다.
        assert "winner_gidx_v084" not in sourceh_schema
        assert _resolve_join_field(sourceh_frames, "v1.0.8.4") is None

    # _prompts_dataset_name: ctx.dataset.name에서 유도, 없으면 레거시 PROMPTS_DATASET 폴백.
    class _FakeDataset:
        def __init__(self, name):
            self.name = name
    class _FakeCtxDS:
        def __init__(self, dataset):
            self.dataset = dataset
    assert _prompts_dataset_name(_FakeCtxDS(_FakeDataset("source-h"))) == "source-h-prompts"
    assert _prompts_dataset_name(_FakeCtxDS(_FakeDataset("sourcei"))) == "sourcei-prompts"
    assert _prompts_dataset_name(_FakeCtxDS(None)) == PROMPTS_DATASET

    # _current_winner_field: "전체"/미설정은 레거시 기본값(v080)으로 폴백 — 회귀 방지.
    class _FakeState2:
        def __init__(self, v):
            self.bank_version_filter = v
    class _FakePanel2:
        def __init__(self, v):
            self.state = _FakeState2(v)
    class _FakeCtx2:
        def __init__(self, v):
            self.panel = _FakePanel2(v)
    assert _current_winner_field(_FakeCtx2(ALL_VERSIONS_LABEL)) == WINNER_FIELD
    assert _current_winner_field(_FakeCtx2(None)) == WINNER_FIELD
    assert _current_winner_field(_FakeCtx2("v1.0.8.4")) == "winner_gidx_v084"

    # frame_ids_to_gidx/gidx_to_frame_ids: 존재하지 않는 조인 필드는 크래시 대신 빈 결과.
    assert gidx_to_frame_ids(g, dataset_name=FRAMES_DATASET, winner_field="winner_gidx_v999") == []
    assert frame_ids_to_gidx(ids, dataset_name=FRAMES_DATASET, winner_field="winner_gidx_v999") == []
    assert gidxes_to_frame_ids([g], dataset_name=FRAMES_DATASET, winner_field="winner_gidx_v999") == []

    # 뱅크 버전 필터: "전체"는 전 문장(기존 동작과 바이트 단위 동일), 특정 버전은 그 버전만.
    bank_versions = sorted({str(v) for v in b["bank_version"] if v is not None}) \
        if b.get("bank_version") is not None else []
    assert bank_versions, "sourcei-prompts에 bank_version 값이 없음 — 데이터 계층 회귀 의심"
    v0 = bank_versions[0]
    fig_all = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set(),
                            bank_version_filter=ALL_VERSIONS_LABEL)
    fig_v0 = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set(),
                           bank_version_filter=v0)
    n_all = sum(len(t["x"]) for t in fig_all["data"][:-1])
    n_v0 = sum(len(t["x"]) for t in fig_v0["data"][:-1])
    assert n_all == len(b["gidx"])            # "전체" = 기존 동작과 동일(회귀 없음)
    assert n_v0 <= n_all                       # 특정 버전은 부분집합
    assert f"버전: {v0}" in fig_v0["layout"]["title"]["text"]
    assert f"버전: {ALL_VERSIONS_LABEL}" in fig_all["layout"]["title"]["text"]
    # bank_version_filter 기본값(None)은 필터 없음과 동일해야 한다(하위호환 — 기존 호출부).
    fig_default = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set())
    assert sum(len(t["x"]) for t in fig_default["data"][:-1]) == n_all

    # load_prompt_bundle dataset_name 파라미터화 + 캐시 1엔트리 유지 검증.
    _CACHE.clear()
    load_prompt_bundle(PROMPTS_DATASET)
    assert len(_CACHE) == 1
    if fo.dataset_exists("source-h-prompts"):
        load_prompt_bundle("source-h-prompts")
        assert len(_CACHE) == 1, "회귀: dataset_name 전환 후에도 캐시 엔트리가 1개를 넘음"
        load_prompt_bundle(PROMPTS_DATASET)
        assert len(_CACHE) == 1

    # source-h-prompts 존재 시 bundle 로드/필터 스모크 (요구사항 4, 없으면 skip).
    if fo.dataset_exists("source-h-prompts"):
        b_sourceh = load_prompt_bundle("source-h-prompts")
        assert len(b_sourceh["gidx"]) > 0
        sourceh_versions = sorted({str(v) for v in b_sourceh["bank_version"] if v is not None}) \
            if b_sourceh.get("bank_version") is not None else []
        assert sourceh_versions, "source-h-prompts에 bank_version 값이 없음"
        fig_sourceh_all = build_mode_a(b_sourceh, rule="argmax_k1", show_unadopted=True,
                                     selected_gidx=set(), bank_version_filter=ALL_VERSIONS_LABEL)
        fig_sourceh_v0 = build_mode_a(b_sourceh, rule="argmax_k1", show_unadopted=True,
                                    selected_gidx=set(), bank_version_filter=sourceh_versions[0])
        assert all(t["type"] == "scattergl" for t in fig_sourceh_all["data"])
        n_sourceh_all = sum(len(t["x"]) for t in fig_sourceh_all["data"][:-1])
        n_sourceh_v0 = sum(len(t["x"]) for t in fig_sourceh_v0["data"][:-1])
        assert n_sourceh_all == len(b_sourceh["gidx"])
        assert n_sourceh_v0 <= n_sourceh_all
    else:
        print("source-h-prompts not found — skip smoke")

    # _rows_to_markdown join_field_missing 안내 (표 내용은 그대로 유지).
    row12 = {"gidx": g, "text": "hello12", "wins": 1, "purity": 0.5,
             "n_cameras": 1, "wave_gain": 0.1}
    md_missing = _rows_to_markdown([], "winner_gidx_v084")
    assert "조인 필드 없음" in md_missing and "winner_gidx_v084" in md_missing
    assert "선택된 프레임 없음" in md_missing
    md_missing_rows = _rows_to_markdown([row12], "winner_gidx_v084")
    assert "조인 필드 없음" in md_missing_rows and "| gidx |" in md_missing_rows

    # 클릭 매핑 계약: PlotlyView의 onClick은 trace.ids[pointIndex]만 ctx.params["id"]로 전달한다
    # (customdata 아님 — App 번들 getIdForTrace 실측, Task 8). 하이라이트 트레이스 ids로 역추적 가능해야 함.
    assert "ids" in fig["data"][-1] and set(int(x) for x in fig["data"][-1]["ids"]) == {g}
    assert all("ids" in t for t in fig["data"])

    # 승자 문장 표 마크다운: 빈 선택은 안내문, 채워진 선택은 헤더+행 포함
    assert "선택된 프레임 없음" in _rows_to_markdown([])
    row = {"gidx": g, "text": "hello", "wins": 1, "purity": 0.5,
           "n_cameras": 2, "wave_gain": 0.1}
    md = _rows_to_markdown([row])
    assert "| gidx |" in md and f"| {g} |" in md and "hello" in md
    # 표 셀 안 `|` 이스케이프 (원본 텍스트에 파이프가 있어도 열 정렬이 깨지면 안 됨)
    # 상한 잘림 표기 (lasso 다중선택): 전체 수 > 표시 행 수면 안내가 붙는다
    md_trunc = _rows_to_markdown([row], total=5)
    assert "선택 5개 중 상위 1개" in md_trunc and f"| {g} |" in md_trunc
    row_pipe = {**row, "text": "a|b|c"}
    md_pipe = _rows_to_markdown([row_pipe])
    assert "a\\|b\\|c" in md_pipe                    # 파이프가 이스케이프된 채 보존됨
    body_line = [ln for ln in md_pipe.splitlines() if ln.startswith(f"| {g} |")][0]
    cols = body_line.replace("\\|", "").split("|")   # 이스케이프 제거 후에도 6컬럼 유지돼야 함
    assert len(cols) == 8, cols                       # 양끝 빈 문자열 2 + 컬럼 6

    # dedup 가드 회귀 (Task 8 fix round): 밑줄 없는 상태 키만 실제로 영속되어야 하고,
    # 빈 payload 로의 "진짜 선택 해제" 전이는 스퓨리어스 재발화와 구별돼 절대 삼켜지면
    # 안 된다. 배포본 panel.py의 PanelRefBase.__setattr__가 `_` 시작 키를 self.set()
    # 우회(순수 인스턴스 속성, ctx.panel_state 라운드트립 밖)로 처리하는 걸 실측했으므로
    # (panel.py:223-235) 여기서는 실제 규약과 동일한 get/set 인터페이스의 fake로 검증한다.
    class _FakePanelState:
        def __init__(self):
            self._d = {}
        def get(self, k, default=None):
            return self._d.get(k, default)
        def set(self, k, v):
            self._d[k] = v
    class _FakeCtx:
        def __init__(self):
            self.panel = type("P", (), {"state": _FakePanelState()})()
    fctx = _FakeCtx()
    assert _dedup_guard(fctx, "sel_seen", ["a", "b"]) is False   # 최초 진입 → 처리
    assert _dedup_guard(fctx, "sel_seen", ["b", "a"]) is True    # 순서만 다른 재발화 → 스킵
    assert _dedup_guard(fctx, "sel_seen", []) is False, \
        "회귀: 실제 전체 선택 해제 전이가 삼켜짐"                  # 진짜 "전체 해제" → 반드시 처리
    assert _dedup_guard(fctx, "sel_seen", []) is True             # 그 다음 스퓨리어스 빈 재발화만 스킵

    # 모드 B (Task 9): sourcei를 ground_truth 2클래스로 갈라 같은 좌표계 overlay (구조 검증용).
    # frames_captions(project 22개)이 본용도지만 selftest는 App 없이 도는 sourcei로 검증한다.
    figb = build_mode_b(FRAMES_DATASET, "ground_truth", ["normal", "falldown"], BRAIN_KEY)
    assert len(figb["data"]) == 2 and all(t["type"] == "scattergl" for t in figb["data"])
    assert "같은 좌표계" in figb["layout"]["title"]["text"]
    # 크래시 가드 (2026-08-10 실사용 오류): 없는 그룹 필드는 ValueError 대신 안내 배너.
    # 기본값 "project"가 sourcei에 없어 on_groups_change가 패널을 죽였던 케이스 그대로.
    figb_nf = build_mode_b(FRAMES_DATASET, "project", ["cohort-b"], BRAIN_KEY)
    assert figb_nf["data"] == [] and "그룹 필드 'project'가 없습니다" in figb_nf["layout"]["title"]["text"]
    figb_nb = build_mode_b(FRAMES_DATASET, "ground_truth", ["normal"], "no_such_brain_key")
    assert figb_nb["data"] == [] and "brain run" in figb_nb["layout"]["title"]["text"]
    n_normal = int(np.sum(np.asarray(frames.values("ground_truth.label"), dtype=object) == "normal"))
    assert figb["data"][0]["name"] == f"normal ({min(n_normal, MAX_POINTS // 2)})"
    # 데이터 계약(모드 A와 동일): 스키마 PlotlyView.data에는 trace 리스트만 굽는다 —
    # {"data":...,"layout":...} 통짜를 넘기면 0점 렌더된다는 게 Task 5 스파이크 실측이므로,
    # 여기서도 fig 전체가 아니라 fig["data"]만 trace 스키마를 만족하는지 확인한다.
    assert all(set(t.keys()) >= {"type", "x", "y", "marker"} for t in figb["data"])

    # Task 11 회귀 가드 — "클릭해야 나온다" 2차 방어선(1차·확정 원인은 fiftyone_app_setup.py의
    # active_child 누락 — render()의 docstring/주석 참고): on_load 직후 render()가 만드는
    # 스키마의 PlotlyView.data가 (set_data 왕복 없이도) 즉시 비어있지 않아야 한다. set_data()는
    # patch_panel_data로, render()의 스키마는 show_panel_output으로 — 각각 독립된
    # setTimeout(fn,1)로 지연 적용되는 별도 채널이라 최초 마운트 시 도착 순서가 보장되지 않는다.
    # PlotlyView(data=...)로 스키마에 직접 구우면 이 잠재 경합과 무관하게 항상 채워진다.
    class _FakePanelStateAttr:
        def set(self, key, value=None):
            # 실제 PanelState.set 은 pydash 중첩 경로 — 여기선 최상위 키만 흉내내면 충분
            # (_sync_controls 가 "controls" 단일 키에 dict 를 통째로 넣는다)
            setattr(self, key.split(".")[0], value)
    class _FakePanelData:
        def __init__(self, calls):
            self._calls = calls
        def clear(self):
            self._calls.append("clear")
    class _FakePanelAttr:
        def __init__(self):
            self.state = _FakePanelStateAttr()
            self.data_calls = []
            self.data = _FakePanelData(self.data_calls)
        def set_data(self, name, value):
            self.data_calls.append((name, value))
    class _FakeCtxAttr:
        def __init__(self):
            self.panel = _FakePanelAttr()
            self.dataset = None
    render_ctx = _FakeCtxAttr()
    panel_instance = PromptComparePanel()
    panel_instance.on_load(render_ctx)               # 실제 마운트 시퀀스 그대로: on_load → _refresh
    schema = panel_instance.render(render_ctx)        # set_data 왕복(비동기) 없이 바로 render()
    scatter_view = schema.type.properties["scatter_v2"].view
    assert scatter_view.data, \
        "회귀: render() 스키마의 초기 data가 비어있음 — 최초 마운트 시 빈 산점도(Task 11) 재발"
    assert sum(len(t["x"]) for t in scatter_view.data[:-1]) == len(b["gidx"]), \
        "회귀: 스키마에 구운 data 포인트 수가 전체 gidx 수와 불일치"
    assert scatter_view.layout, "회귀: render() 스키마의 초기 layout이 비어있음"
    # 컨트롤 드롭다운 4개 (2026-08-10 피드백): h_stack("controls") 한 줄 + _sync_controls 미러링
    ctrl_props = schema.type.properties["controls"].type.properties
    assert {"mode", "rule", "show_mode", "bank_version_filter"} <= set(ctrl_props)
    # 미러링 회귀 가드: _refresh 가 controls.* 표시값을 서버 상태에서 밀어넣어야 한다
    assert render_ctx.panel.state.controls["mode"] == "A"
    assert render_ctx.panel.state.controls["rule"] == "argmax_k1"
    assert render_ctx.panel.state.controls["show_mode"] == SHOW_ALL_LABEL
    assert render_ctx.panel.state.controls["bank_version_filter"] == ALL_VERSIONS_LABEL

    # 컨트롤 핸들러 상태 전이 + 화이트리스트 (codex 3차 리뷰 (a)/(e)-1): 허용값만 반영,
    # 예상밖 값은 상태를 건드리지 않아야 한다 (조용한 폴백 금지).
    class _FakeCtxHandler(_FakeCtxAttr):
        def __init__(self):
            super().__init__()
            self.params = {}
    hctx = _FakeCtxHandler()
    panel_instance.on_load(hctx)
    hctx.params = {"value": "dist_iou"}
    panel_instance.on_rule_change(hctx)
    assert hctx.panel.state.rule == "dist_iou"
    assert hctx.panel.state.controls["rule"] == "dist_iou"          # 미러도 즉시 갱신
    hctx.params = {"value": "nonsense"}
    panel_instance.on_rule_change(hctx)
    assert hctx.panel.state.rule == "dist_iou"                       # 화이트리스트 밖 → 무시
    hctx.params = {"value": SHOW_ADOPTED_LABEL}
    panel_instance.on_show_change(hctx)
    assert hctx.panel.state.show_unadopted is False
    hctx.params = {"value": "nonsense"}
    panel_instance.on_show_change(hctx)
    assert hctx.panel.state.show_unadopted is False                  # 예상밖 값 → 상태 유지
    hctx.params = {"value": SHOW_ALL_LABEL}
    panel_instance.on_show_change(hctx)
    assert hctx.panel.state.show_unadopted is True
    hctx.params = {"value": "B"}
    panel_instance.on_mode_change(hctx)
    assert hctx.panel.state.mode == "B"
    hctx.params = {"value": "A"}
    panel_instance.on_mode_change(hctx)
    assert hctx.panel.state.mode == "A"

    # update_plot=False 계약 (성능): 플롯 상태를 다시 쓰지 않고 표만 갱신한다
    # (파괴 방지 수단 아님 — _refresh docstring의 최종 진단 참고).
    prev_scatter = render_ctx.panel.state.scatter_data
    render_ctx.panel.state.selected_gidx = [g]
    panel_instance._refresh(render_ctx, update_plot=False)
    assert render_ctx.panel.state.scatter_data is prev_scatter, \
        "회귀: update_plot=False인데 scatter_data가 교체됨 — emb_viz 선택 파괴 재발"
    assert render_ctx.panel.state.top_table and render_ctx.panel.state.top_table[0]["gidx"] == g, \
        "update_plot=False에서도 승자 문장 표는 갱신돼야 한다"
    render_ctx.panel.state.selected_gidx = []
    panel_instance._refresh(render_ctx)   # 전체 갱신 원복

    # box select 이중 발화 가드 (2026-08-10 실사용 오류): scattergl box select는 점 있는
    # plotly_selected 직후 빈 이벤트를 한 번 더 쏜다 — 빈 payload가 방금 선택을 지우면 안 됨.
    hctx.params = {"value": "argmax_k1"}
    panel_instance.on_rule_change(hctx)
    hctx.panel.state.selected_gidx = [999]
    hctx.params = {"data": []}
    panel_instance.on_plot_selected(hctx)
    assert hctx.panel.state.selected_gidx == [999], "회귀: 빈 plotly_selected가 선택을 지움"
    # 명시적 해제는 더블클릭 훅 — 문장 선택과 프레임 하이라이트(extended selection) 모두 비움
    class _FakeOps:
        def __init__(self):
            self.calls = []
        def clear_view(self):
            self.calls.append("clear_view")
        def set_view(self, view=None, name=None):
            self.calls.append(("set_view", view))
    hctx.ops = _FakeOps()
    panel_instance.on_plot_double_click(hctx)
    assert hctx.panel.state.selected_gidx == []
    assert hctx.ops.calls == ["clear_view"]   # 해제 = 뷰 초기화 (extended selection 안 씀)

    # 빈 extendedSelection 에코 무시 (리로드 버그 방어 — on_change_extended_selection 주석):
    # 훅 EXEC가 유발한 App의 extendedSelection 소거 잔향이 표/선택을 지우면 안 된다.
    hctx.panel.state.selected_gidx = [g]
    hctx.extended_selection = {"selection": []}
    panel_instance.on_change_extended_selection(hctx)
    assert hctx.panel.state.selected_gidx == [g], "회귀: 빈 extendedSelection 에코가 선택을 지움"

    # ── Task 12: 프롬프트 짝이 없는 데이터셋에서 모드 A가 크래시 대신 안내를 낸다 ──
    # frames_captions는 실측상 "frames_captions-prompts"가 없다(fo.list_datasets() 확인,
    # 2026-08-07) — 정확히 요구사항 1이 다루는 케이스를 실 데이터셋으로 검증한다.
    assert not fo.dataset_exists("frames_captions-prompts")
    if fo.dataset_exists("frames_captions"):
        class _FakeDatasetNP:
            def __init__(self, name):
                self.name = name
        class _FakeCtxNoPair(_FakeCtxAttr):
            def __init__(self, dataset_name):
                super().__init__()
                self.dataset = _FakeDatasetNP(dataset_name)
        nopair_ctx = _FakeCtxNoPair("frames_captions")
        panel_instance.on_load(nopair_ctx)
        assert nopair_ctx.panel.state.prompts_available is False, \
            "회귀: 프롬프트 짝 없는 데이터셋에서도 available=True로 남음"
        assert all(c == "clear" for c in nopair_ctx.panel.data_calls), \
            "회귀: set_data 호출됨 — patch 딥머지가 줄어든 배열을 못 지우므로 스키마 경로만 써야 한다"
        assert "clear" in nopair_ctx.panel.data_calls, \
            "회귀: data.clear() 미호출 — 옛 세션의 patched data가 스키마 data를 가린다"
        assert nopair_ctx.panel.state.scatter_data == [], \
            "회귀: 프롬프트 짝 없음인데 산점도에 데이터가 실림"
        assert NO_PROMPTS_PAIR_TEXT in nopair_ctx.panel.state.layout["title"]["text"]
        nopair_schema = panel_instance.render(nopair_ctx)
        # 모드 A 전용 컨트롤(규칙/표시/버전)이 비활성 — 안내 텍스트만 렌더.
        nopair_ctrls = nopair_schema.type.properties["controls"].type.properties
        assert "no_prompts_notice" in nopair_ctrls
        assert "bank_version_filter" not in nopair_ctrls
        assert "rule" not in nopair_ctrls and "show_mode" not in nopair_ctrls

    print("selftest OK")


if __name__ == "__main__":
    selftest()
