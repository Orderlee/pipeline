"""프롬프트 프로브 — App 안에서 후보 문장을 쓰고 **즉시** 채점한다.

## 왜 필요한가

지금 루프에는 구멍이 하나 있다. FiftyOne 에서 "이 군집이 안 잡힌다"를 **보고**,
문장을 **쓰고**, 점수를 **보는** 세 동작이 서로 다른 곳에 흩어져 있다 —
후보 문장을 하나 시험하려면 `prompt_geometry.py` 의 `PROBE_CANDIDATES` dict 에
손으로 써넣고 스테이지를 재실행해야 했다. 그 사이 "무엇을 보고 있었는지"가 날아간다.

이 오퍼레이터는 그 구멍만 메운다. 보던 화면 그대로에서 문장을 입력하면
`/embed_text`(7.5ms)로 임베딩해 **현재 뷰의 프레임들에 대해 판정 변화를 계산**한다.

## 어떻게 App 안에서 계산하나

뱅크(수만 문장 × 1024-d)는 App 프로세스에 못 올린다. 대신 `prompt_geometry.py probecache`
가 프레임마다 네 값을 미리 심어둔다 — 그것만 있으면 재채점이 **정확히** 재현된다.

    probe_bar_<tag>   top-K 마지막 코사인 = 진입 기준선
    probe_votes_<tag> 클래스별 현재 득표
    probe_topc_<tag>  클래스별 top-K 내 최고 코사인 (동표 해소)
    probe_out_<tag>   진입 시 밀려나는 문장의 클래스

후보 코사인 c 가 bar 를 넘으면 votes[cand]+1 / votes[out]−1, topc[cand]=max(topc, c) 로
갱신하고 `votes + (topc+2)/10` argmax — `bank_vote_stream` 과 같은 규칙이다.

## 무엇을 보고 판단하나

**진입률**만 높으면 안 된다. 배경을 서술한 문장도 진입률은 높다 — 그게 「배경 자석」이다.
그래서 세 가지를 함께 낸다: 진입률 / **순이득**(고친 수 − 망친 수) / **배경 코사인**.
배경 코사인은 같은 카메라의 `GT=normal` 프레임과의 평균 유사도다. 높으면 자석이다.

로직 검증: 컨테이너에서 `python __init__.py` (재채점 규칙 assert).
"""

import contextlib
import io
import os
import re
import sys
import threading

import numpy as np

import fiftyone as fo
import fiftyone.operators as foo
import fiftyone.operators.types as types

EMBED_URL = os.environ.get("EMBED_URL", "http://embedding-service:8003")
SUFFIX = "-prompts"          # 문장 데이터셋 접미사 (`stage_promptmap` 이 만드는 이름 규칙)
TAG_PREFIX = "bank:"         # 뱅크 버전 후보 문장에 붙는 표본 태그

# gidx 전역 유일성 오프셋 — 정본은 `prompt_geometry.GIDX_OFFSET`. 여기서 상수를 **다시 쓰지 않고**
# 그 모듈에서 읽는다(두 벌이 되면 조인이 조용히 깨진다). 뱅크 최대 크기(실측 49,140)보다 커야
# 나머지 연산이 wrap 하지 않는다.
def _gidx_offset():
    try:
        if "/workspace" not in sys.path:
            sys.path.insert(0, "/workspace")
        import prompt_geometry as pg

        return int(getattr(pg, "GIDX_OFFSET", 100_000)) or 100_000
    except Exception:
        return 100_000

# App 프로세스에서 도는 동기 연산이라 상한을 둔다. 13k 프레임 × 1024-d = 54MB 로
# 충분히 빠르지만(<1s), 20만 프레임 데이터셋에서 그대로 돌면 앱이 멈춘다.
MAX_FRAMES = 40_000
BATCH = 8_000


def _tags(dataset):
    """probecache 가 심어둔 뱅크 태그 목록."""
    return sorted(
        k[len("probe_bank_"):]
        for k in (dataset.info or {})
        if k.startswith("probe_bank_")
    )


# ⚠️ `resolve_placement` 는 **`ctx.dataset` 이 None 인 시점에도 호출된다** (데이터셋 목록 화면 등).
#    거기서 예외가 나면 그 오퍼레이터만 숨는 게 아니라 **배치 응답이 통째로 실패해 모든 플러그인
#    버튼이 함께 사라진다** (2026-08-12 실측: 툴바에서 프로브 버튼까지 같이 없어졌다).
#    그래서 배치 게이트는 전부 이 두 헬퍼로만 판정한다 — 절대 raise 하지 않는다.
def _has_field(ctx, name):
    try:
        return ctx.dataset is not None and name in ctx.dataset.get_field_schema()
    except Exception:
        return False


def _probe_tags_safe(ctx):
    try:
        return _tags(ctx.dataset) if ctx.dataset is not None else []
    except Exception:
        return []


# `resolve_input` 은 폼 입력마다 재평가된다 (dynamic=True). 문장 데이터셋이 603,318행으로
# 커진 뒤 `count_values("bank_version.label")` 이 0.5s 라 **글자 하나 칠 때마다** 그만큼 멈춘다.
# 표본 수로 키를 잡아 캐시한다 — 재빌드되면 개수가 바뀌므로 자동 무효화된다 (`count()` 는 0.00s).
_VER_CACHE = {}


def _bank_versions(dataset):
    key = (dataset.name, dataset.count())
    if key not in _VER_CACHE:
        _VER_CACHE.clear()                      # 한 데이터셋만 들고 있어 메모리 상한을 고정
        _VER_CACHE[key] = sorted(dataset.count_values("bank_version.label") or {})
    return _VER_CACHE[key]


def _bank_label(tag, bank, k):
    """드롭다운 표시명. `probecache BANK_ATTACH=all` 로 만든 합집합 캐시는 「전체」로 읽힌다."""
    return f"전체 ({bank}) k={k}" if tag == "all" else f"{bank} (k={k})"


def _meta(dataset, tag):
    info = dataset.info or {}
    return (
        info.get(f"probe_classes_{tag}") or [],
        int(info.get(f"probe_k_{tag}") or 10),
        info.get(f"probe_bank_{tag}") or "?",
    )


def _embed_text(text):
    import requests

    # 응답은 {"vector": [...], "dim": 1024, "model_name": ...} — 프레임 임베딩과 같은 인코더
    r = requests.post(f"{EMBED_URL}/embed_text", data={"text": text}, timeout=120)
    r.raise_for_status()
    v = np.asarray(r.json()["vector"], dtype="float32").ravel()
    n = np.linalg.norm(v)
    return v / n if n else v


def rescore(cos, bar, votes, topc, out_c, cand_c):
    """후보 문장 1개를 넣었을 때의 새 예측. 규칙은 `bank_vote_stream` 과 동일.

    cos[N] · bar[N] · votes[N,C] · topc[N,C] · out_c[N] · cand_c(int)
    반환 (new_pred[N], entered[N] bool)
    """
    entered = cos > bar
    v = votes.astype(np.int32).copy()
    t = topc.astype(np.float32).copy()
    idx = np.flatnonzero(entered)
    if len(idx):
        v[idx, cand_c] += 1
        # 밀려나는 자리가 후보와 같은 클래스면 표 수는 그대로다
        v[idx, out_c[idx]] -= 1
        t[idx, cand_c] = np.maximum(t[idx, cand_c], cos[idx])
    return (v + (t + 2.0) / 10.0).argmax(axis=1), entered


# ────────── 분석 모듈 재사용 ──────────
# 뱅크 확정·제품규칙 채점은 `/workspace/prompt_geometry.py` 에 이미 있다. 여기서 베끼면
# 클래스 사상·규칙이 두 벌이 되어 조용히 갈린다 (compare 플러그인의 `vtag` 복제가 그 선례).
# App 서버 자체가 `/workspace/fiftyone_relaunch.py` 로 뜨므로 이 경로 의존은 새 실패모드가 아니다.
_PG_LOCK = threading.Lock()


def _pg_profile(dataset_name):
    """`prompt_geometry` 모듈 + 이 데이터셋에 맞는 프로필 이름.

    ⚠️ 프로필 역매핑을 **반드시** 해야 한다. 프로필끼리 `class_names`·`prompt_dir` 이 같고
    `dataset` 만 다르므로, 어긋난 프로필로 돌리면 fail-closed 가 걸리지 않고 **엉뚱한
    데이터셋의 태그를 읽어 조용히 잘못된 뱅크**가 나온다.
    """
    if "/workspace" not in sys.path:
        sys.path.insert(0, "/workspace")
    import prompt_geometry as pg

    base = dataset_name[: -len(SUFFIX)] if dataset_name.endswith(SUFFIX) else dataset_name
    prof = next((k for k, v in pg.PROFILES.items() if v["dataset"] == base), None)
    if prof is None:
        raise ValueError(
            f"{dataset_name} 은 분석 프로필에 없습니다 — 대상: "
            + ", ".join(f"{v['dataset']}{SUFFIX}" for v in pg.PROFILES.values())
        )
    return pg, prof


def _run_pg(dataset_name, fn_name, *args):
    """전역 프로필을 lock 안에서만 만지고, `SystemExit` 을 모달에 보이는 오류로 바꾼다.

    ⚠️ `SystemExit` 은 `BaseException` 이라 FiftyOne executor 의 `except Exception` 에
    **안 걸린다** (설치판 `operators/executor.py` 의 except 절 = Exception·KeyError).
    변환하지 않으면 모달에 아무 메시지도 안 뜨고 스레드만 죽는다.
    반환값은 스테이지가 stdout 에 남긴 진행 로그 — 그대로 사용자에게 보여준다.
    """
    pg, prof = _pg_profile(dataset_name)
    buf = io.StringIO()
    with _PG_LOCK:
        pg.set_profile(prof)
        if f"{pg.PROFILES[pg.PROFILE]['dataset']}{SUFFIX}" != dataset_name:
            raise ValueError(f"프로필 정합 실패: {prof} ↔ {dataset_name}")
        try:
            with contextlib.redirect_stdout(buf):
                getattr(pg, fn_name)(*args)
        except SystemExit as e:
            raise ValueError(str(e) or "스테이지가 중단됐습니다") from None
        prompt_dir = pg.PROMPT_DIR
    return [ln for ln in buf.getvalue().splitlines() if ln.strip()], prompt_dir


def _ver_tags(version):
    """뱅크 버전 → 필드 접미 태그 후보. 표기가 두 세대 섞여 있다.

    신 표기 `v1080`(= 점 제거) / 구 표기 `v080`(= 뒤 두 성분). 29버전 재빌드가 태그 규칙을
    바꿨는데 기존 필드는 구 표기로 남아 있다 — 어느 쪽이 있든 찾아낸다.
    """
    parts = version.lstrip("v").split(".")
    return ["v" + "".join(parts), "v0" + "".join(parts[2:])]


def _pick_field(schema, pattern, version):
    """`pattern` 에 태그를 끼워 실제 존재하는 필드명을 돌려준다 (없으면 None)."""
    for t in _ver_tags(version):
        f = pattern.format(tag=t)
        if f in schema:
            return f
    return None


def _winner_field(frames_schema, version):
    return _pick_field(frames_schema, "winner_gidx_{tag}", version)


def _cos_columns(frames_view, rows, won_idx, classes, version, offset):
    """채택 근거 수치를 붙인다 — **프레임 필드만으로** 계산한다 (문장 임베딩 불필요).

    문장이 그 프레임을 top-1 로 이겼다면, 그 프레임의 `cos_best_<그 문장의 클래스>` 가 곧
    **그 문장의 코사인**이다 (클래스별 최고 코사인의 정의). 그래서 임베딩을 다시 안 곱한다.

      cos     이 문장이 가져간 프레임들에서의 평균 코사인 — "얼마나 강하게 끌어당기나"
      margin  같은 프레임에서 (자기 클래스 최고 − 다른 클래스 최고) 평균 — **판정을 뒤집은 여유폭**.
              실측 승리 margin 중앙값이 ~0.01 이라 fp16 로는 분해가 안 되는 크기다.
      p_iou   그 프레임들의 **제품 규칙**(분포 IoU) 평균. 낮을수록 탐지되는 쪽.
              ⚠️ 프레임의 성질이라 문장 개별 인과가 아니다 — "이 문장이 데려온 프레임들이
              제품 규칙에서 어디 서 있나" 로만 읽어야 한다.
    """
    sch = frames_view.get_field_schema()
    # 클래스당 values() 1회 → 필드 목록 1회씩 (위 배치 주석과 같은 이유)
    cb_keys = [c for c in classes if f"cos_best_{c}" in sch]
    cb = dict(zip(cb_keys, frames_view.values([f"cos_best_{c}" for c in cb_keys]))) \
        if cb_keys else {}
    iou_pairs = [(c, _pick_field(sch, "wave_iou_" + c + "_{tag}", version))
                 for c in classes]
    iou_keys = [c for c, f in iou_pairs if f]
    iou = dict(zip(iou_keys, frames_view.values(
        [f for _c, f in iou_pairs if f]))) if iou_keys else {}
    for r in rows:
        idxs = won_idx.get(int(r["gidx"]) % offset) or []
        c = r["cls"]
        if cb.get(c) and idxs:
            own = [cb[c][i] for i in idxs if cb[c][i] is not None]
            r["cos"] = round(sum(own) / len(own), 4) if own else None
            oth = [max((cb[o][i] for o in cb if o != c and cb[o][i] is not None), default=None)
                   for i in idxs]
            pair = [(cb[c][i], m) for i, m in zip(idxs, oth)
                    if m is not None and cb[c][i] is not None]
            r["margin"] = round(sum(a - b for a, b in pair) / len(pair), 4) if pair else None
        else:
            r["cos"] = r["margin"] = None
        if iou.get(c) and idxs:
            vv = [iou[c][i] for i in idxs if iou[c][i] is not None]
            r["p_iou"] = round(sum(vv) / len(vv), 4) if vv else None
        else:
            r["p_iou"] = None
    return rows


def _rank_by_project(frames_view, winner_fld, classes, gidx_list, texts, labels,
                     top_n, per_class, min_wins, sort_by):
    """이 뷰(=프로젝트로 자른 프레임)에서만 문장별 승수·정확도를 집계해 상위 N개를 고른다.

    · 승수 = 그 프레임들 중 이 문장이 top-1 로 이긴 수
    · 정확도 = 이긴 프레임 중 그 문장의 선언 클래스가 GT 와 같은 비율
    · 순이득 = 맞춘 수 − 틀린 수 (승수와 정확도를 한 축으로 합친 값)

    ⚠️ 이 순위는 **그 프로젝트의 GT 로 만든 값**이다 → 그 프로젝트에 적합(overfit)된 선택이고,
       다른 현장으로의 전이는 보장되지 않는다. 그래서 provenance 에 순위 조건을 박아 둔다.
    """
    wg = frames_view.values(winner_fld)
    gtl = frames_view.values("ground_truth.label")

    # ⚠️ gidx 오프셋 **세대 차이** 방어. 프레임의 `winner_gidx_*` 는 구 세대(뱅크-로컬 0~N)와
    #    신 세대(전역 = 버전순번×GIDX_OFFSET + 로컬)가 섞여 있다 (29버전 재빌드가 태그·오프셋을
    #    바꾸는 중). 그대로 등식 조인하면 **조용히 0건**이 되므로, 양쪽을 오프셋으로 나눈 나머지로
    #    맞춘다 — 어차피 뱅크 버전 하나로 이미 좁혀 놓았으니 나머지만으로 유일하다.
    off = _gidx_offset()

    def _norm(g):
        return int(g) % off

    cls_of = {_norm(g): lab for g, lab in zip(gidx_list, labels)}
    win, hit, won_idx = {}, {}, {}
    for i, (g, gt) in enumerate(zip(wg, gtl)):
        if g is None:
            continue
        g = _norm(g)
        win[g] = win.get(g, 0) + 1
        won_idx.setdefault(g, []).append(i)      # 채택 근거 수치를 이 위치들에서 계산한다
        if cls_of.get(g) is not None and cls_of[g] == gt:
            hit[g] = hit.get(g, 0) + 1

    rows = []
    for g, t, lab in zip(gidx_list, texts, labels):
        # ⚠️ 조회도 **정규화한 키**로 해야 한다. win/hit 의 키는 `_norm` 을 거쳤으므로
        #    원본 gidx(300,000+)로 찾으면 전부 0이 되어 "후보 0개"가 조용히 나온다 (실측 버그).
        k = _norm(g)
        n = win.get(k, 0)
        if n < min_wins:
            continue
        h = hit.get(k, 0)
        rows.append({"gidx": int(g), "text": t, "cls": lab, "wins": n,
                     "purity": round(h / n, 4), "net": 2 * h - n})

    key = {"purity": lambda r: (r["purity"], r["wins"]),
           "wins": lambda r: (r["wins"], r["purity"]),
           "net": lambda r: (r["net"], r["purity"])}[sort_by]
    rows.sort(key=key, reverse=True)
    if not per_class:
        return rows[:top_n], won_idx
    out, cnt = [], {}
    for r in rows:                       # 클래스별 쿼터 — 한 클래스가 상위를 독식하지 않게
        if cnt.get(r["cls"], 0) >= top_n:
            continue
        cnt[r["cls"]] = cnt.get(r["cls"], 0) + 1
        out.append(r)
    return out, won_idx


class ExportBankVersion(foo.Operator):
    """선택/뷰/태그 → 새 뱅크 버전 확정 + CSV + 원장. 문장 데이터셋(`<ds>-prompts`) 전용."""

    @property
    def config(self):
        return foo.OperatorConfig(
            name="export_bank_version",
            label="③ 뱅크 버전 만들기 — 선택한 문장 → CSV 내보내기",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        # 문장 데이터셋에서만 노출 — 프레임 데이터셋에 뜨면 오조작을 부른다.
        if not _has_field(ctx, "text"):
            return None
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(label="③ 뱅크 버전 만들기 — 선택한 문장 → CSV 내보내기", icon="download", prompt=True),
        )

    def _source_counts(self, ctx):
        """(선택 수, 뷰 수) — 라쏘는 `ctx.selected` 에 안 오고 `ctx.extended_selection` 으로만
        온다(compare 패널 실측). 그래서 세 경로를 다 본다."""
        sel = list(ctx.selected or [])
        if not sel:
            ext = ctx.extended_selection or {}
            sel = list(ext.get("selection") or []) if isinstance(ext, dict) else []
        view = ctx.view if ctx.view is not None else ctx.dataset.view()
        return sel, view

    def resolve_input(self, ctx):
        inputs = types.Object()
        if not _has_field(ctx, "text"):
            inputs.view("none", types.Error(
                label="문장 데이터셋이 아닙니다 — `<데이터셋>-prompts` 에서 실행하세요"))
            return types.Property(inputs)

        sel, view = self._source_counts(ctx)

        radio = types.RadioGroup()
        radio.add_choice("RANK", label="프로젝트 성능 상위 N개 (자동 선정 · 미리보기 가능)")
        if sel:
            radio.add_choice("SELECTED", label=f"선택한 문장 {len(sel):,}개")
        radio.add_choice("VIEW", label=f"현재 뷰 전체 {view.count():,}개")
        radio.add_choice("TAG", label="이미 붙여둔 태그")
        default = "SELECTED" if sel else "RANK"
        inputs.enum("source", radio.values(), default=default, required=True,
                    label="대상", view=radio)
        src = ctx.params.get("source") or default

        # 태그 목록은 **선택했을 때만** 읽는다 — 603k 행에서 0.35s 라 매 입력마다 부르면 폼이 끈다
        if src == "TAG":
            tags = sorted(t for t in (ctx.dataset.count_sample_tags() or {})
                          if t.startswith(TAG_PREFIX))
            if not tags:
                inputs.view("notag", types.Error(
                    label=f"`{TAG_PREFIX}*` 태그가 없습니다 — 그리드에서 태그를 붙이거나 "
                          "다른 대상을 고르세요"))
                return types.Property(inputs)
            dd = types.DropdownView()
            for t in tags:
                dd.add_choice(t, label=t)
            inputs.enum("tag", tags, default=tags[0], required=True, label="태그", view=dd)

        if src == "RANK":
            self._rank_inputs(ctx, inputs)

        inputs.str("version", required=True, label="새 버전 이름",
                   description="예: v1.0.8.4-esc-top200. 같은 이름이 이미 있으면 거부합니다",
                   view=types.TextFieldView())
        inputs.str("notes", label="왜 이 버전을 만드는가",
                   description="provenance 에 저장됩니다 (나중에 이 선택을 재현할 유일한 근거). "
                               "상위 N 선정 조건은 자동으로 함께 기록됩니다",
                   view=types.TextFieldView())
        inputs.view("warn", types.Warning(
            label="이 버전은 **미평가**로 기록됩니다. 홀드아웃 재채점 전에는 점수를 인용하지 마세요"))
        return types.Property(inputs, view=types.View(label="뱅크 버전 만들기"))

    def _rank_inputs(self, ctx, inputs):
        """상위 N 선정 폼. 프레임 데이터셋에서 카메라(프로젝트) 목록을 읽어 채운다."""
        vers = _bank_versions(ctx.dataset)
        if not vers:
            inputs.view("nov", types.Error(label="`bank_version` 이 없는 데이터셋입니다"))
            return
        vd = types.DropdownView()
        for v in vers:
            vd.add_choice(v, label=v)
        inputs.enum("rank_version", vers, default=vers[0], required=True,
                    label="원본 뱅크 버전", view=vd)

        frames_name = ctx.dataset.name[: -len(SUFFIX)]
        cams, err = [], None
        try:
            fds = fo.load_dataset(frames_name)
            if "camera" in fds.get_field_schema():
                cams = [c for c in sorted(fds.count_values("camera") or {}) if c]
        except Exception as e:                       # noqa: BLE001 — 폼은 절대 죽지 않게
            err = f"{type(e).__name__}: {e}"
        if err:
            inputs.view("camerr", types.Warning(label=f"{frames_name} 를 못 읽었습니다 — {err}"))
        cd = types.DropdownView()
        cd.add_choice("__ALL__", label=f"전체 ({frames_name} 프레임 전량)")
        for c in cams:
            cd.add_choice(c, label=c)
        inputs.enum("camera", ["__ALL__"] + cams, default="__ALL__", required=True,
                    label="프로젝트(카메라)",
                    description="이 프레임들에서만 문장별 성능을 집계합니다", view=cd)

        inputs.int("top_n", default=50, required=True, label="문장 개수",
                   description="아래 '클래스별로 N개' 가 켜져 있으면 **클래스마다** N개입니다")
        inputs.bool("per_class", default=True, label="클래스별로 N개",
                    description="끄면 전체 통합 상위 N개 — 한 클래스가 독식할 수 있습니다")
        inputs.int("min_wins", default=1, required=True, label="최소 승수",
                   description="이 프로젝트에서 최소 몇 장을 이겨야 후보로 볼지")
        sd = types.DropdownView()
        sd.add_choice("purity", label="정확도 우선 (이긴 프레임 중 정답 비율)")
        sd.add_choice("net", label="순이득 우선 (맞춘 수 − 틀린 수)")
        sd.add_choice("wins", label="승수 우선 (많이 가져가는 문장)")
        inputs.enum("sort_by", ["purity", "net", "wins"], default="net", required=True,
                    label="정렬 기준", view=sd)
        inputs.bool("dry_run", default=True, label="미리보기만 (아무것도 저장하지 않음)",
                    description="켜두면 고른 문장 표만 보여줍니다. 확인 후 끄고 다시 실행하세요")
        inputs.view("rankwarn", types.Warning(
            label="이 순위는 **그 프로젝트의 GT** 로 만든 값입니다 — 그 현장에 적합된 선택이고 "
                  "다른 현장으로의 전이는 보장되지 않습니다. 선정 조건은 provenance 에 기록됩니다"))

    def _rank_execute(self, ctx, version):
        """상위 N 선정 → (선정 표, 대상 뷰, 선정조건 문자열). dry-run 이면 뷰는 None."""
        rv = ctx.params["rank_version"]
        cam = ctx.params.get("camera") or "__ALL__"
        top_n = max(1, int(ctx.params.get("top_n") or 50))
        per_class = bool(ctx.params.get("per_class", True))
        min_wins = max(0, int(ctx.params.get("min_wins") or 1))
        sort_by = ctx.params.get("sort_by") or "net"

        frames_name = ctx.dataset.name[: -len(SUFFIX)]
        fds = fo.load_dataset(frames_name)
        fld = _winner_field(fds.get_field_schema(), rv)
        if not fld:
            have = sorted(f for f in fds.get_field_schema() if f.startswith("winner_gidx_"))
            raise ValueError(f"{frames_name} 에 {rv} 의 winner_gidx 필드가 없습니다 "
                             f"(있는 것: {have or '없음'}) — `attach` 스테이지를 먼저 돌리세요")
        fview = fds if cam == "__ALL__" else fds.match({"camera": cam})
        if not fview.count():
            raise ValueError(f"카메라 {cam} 프레임이 0장입니다")

        pview = ctx.dataset.match({"bank_version.label": rv})
        gidx, texts, labels = pview.values(["gidx", "text", "category.label"])
        classes = sorted({x for x in labels if x})
        picked, won_idx = _rank_by_project(fview, fld, classes, gidx, texts, labels,
                                           top_n, per_class, min_wins, sort_by)
        if not picked:
            raise ValueError(f"조건을 만족하는 문장이 0개입니다 (최소 승수 {min_wins} 를 낮춰보세요)")
        # 채택 근거 수치 — 코사인·마진·제품규칙 IoU (고른 문장에만 계산해 비용을 묶는다)
        _cos_columns(fview, picked, won_idx, classes, rv, _gidx_offset())

        # ⚠️ 카메라를 좁히면 그 현장에 없는 이벤트의 문장은 승수 0 → 전부 걸러진다.
        #    그대로 확정하면 **fire/smoke 문장이 하나도 없는 뱅크**가 조용히 만들어진다
        #    (실측: 상가 복도 카메라에서 normal 만 5개). 그래서 빠진 클래스를 명시한다.
        got = {r["cls"] for r in picked}
        missing = [c for c in classes if c not in got]
        self._missing = (
            f"⚠️ 이 선정에 {', '.join(missing)} 문장이 **0개**입니다 — 그 현장에 해당 이벤트 "
            "프레임이 없어 승수가 0이기 때문입니다. 이대로 확정하면 그 클래스를 절대 못 잡습니다. "
            "프로젝트를 「전체」로 하거나 최소 승수를 0으로 낮추세요"
        ) if missing else f"클래스 커버리지 OK ({', '.join(sorted(got))})"

        spec = (f"rank: bank={rv} camera={cam} top_n={top_n} "
                f"per_class={per_class} min_wins={min_wins} sort_by={sort_by} "
                f"frames={fview.count()} missing_classes={','.join(missing) or 'none'}")
        if ctx.params.get("dry_run", True):
            return picked, None, spec
        keep = {r["gidx"] for r in picked}
        ids = [i for i, g in zip(pview.values("id"), gidx) if g in keep]
        return picked, ctx.dataset.select(ids), spec

    def execute(self, ctx):
        version = str(ctx.params["version"]).strip()
        if not version:
            raise ValueError("버전 이름을 입력하세요")
        src = ctx.params.get("source") or "VIEW"
        sel, view = self._source_counts(ctx)
        picked, spec = None, None

        if src == "RANK":
            picked, target, spec = self._rank_execute(ctx, version)
            if target is None:                      # 미리보기 — 아무것도 쓰지 않는다
                cnt = {}
                for r in picked:
                    cnt[r["cls"]] = cnt.get(r["cls"], 0) + 1
                return {"version": version, "tag": "(미리보기 — 저장 안 함)",
                        "coverage": getattr(self, "_missing", ""),
                        "csv": "-", "host_csv": "-", "next": "-",
                        "spec": spec, "n_picked": len(picked), "class_counts": str(cnt),
                        "picked": picked,
                        "log": [{"line": "미리보기입니다. 확인 후 「미리보기만」을 끄고 "
                                         "다시 실행하면 CSV·원장이 생성됩니다"}]}
            tag = f"{TAG_PREFIX}{version}"
            target.tag_samples(tag)
        elif src == "TAG":
            tag = ctx.params["tag"]
        else:
            # 스테이지는 태그로 대상을 잡는다 (어느 선택 경로에서 왔든 태그는 살아남는다).
            # 선택/뷰를 고르면 여기서 태그를 붙여 provenance 로 남긴다 — 지우지 않는다.
            tag = f"{TAG_PREFIX}{version}"
            target = ctx.dataset.select(sel) if src == "SELECTED" else view
            if not target.count():
                raise ValueError("대상이 0개입니다")
            target.tag_samples(tag)

        notes = ctx.params.get("notes") or ""
        if spec:                                     # 선정 조건을 provenance 에 강제로 남긴다
            notes = (notes + " | " + spec).strip(" |")
        lines, prompt_dir = _run_pg(ctx.dataset.name, "stage_bankfrom", tag, version, notes)
        csv_path = f"{prompt_dir}/authored_{version}.csv"
        # ⚠️ 원장(Postgres 019)은 **fail-soft** 다 — DSN 미설정·DB 오류면 조용히 생략되고 CSV/JSON 만
        #    남는다. 결과 안내를 무조건 "원장까지 끝났습니다" 로 띄우면 사용자가 안 붙은 걸 붙은 줄 안다
        #    (codex 지적, 2026-08-12). 스테이지 로그로 실제 등록 여부를 판정해 그대로 보여준다.
        joined = " ".join(lines)
        ledger = ("등록됨" if "원장 등록" in joined
                  else "생략/실패 — CSV·JSON 만 생성됨 (아래 로그 확인)")
        out = {
            "version": version, "tag": tag, "csv": csv_path, "ledger": ledger,
            "host_csv": csv_path.replace("/data/fiftyone", "docker/data/fiftyone"),
            "next": (f"docker exec docker-analysis-1 python /workspace/prompt_geometry.py "
                     f"bank --csv {csv_path} --version {version}"),
            "log": [{"line": ln} for ln in lines],
        }
        if picked is not None:
            out.update(spec=spec, n_picked=len(picked), picked=picked,
                       coverage=getattr(self, "_missing", ""),
                       class_counts=str({r["cls"]: sum(1 for x in picked if x["cls"] == r["cls"])
                                         for r in picked}))
        return out

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("version", label="버전")
        outputs.str("tag", label="붙은 태그 (provenance)")
        outputs.str("ledger", label="Postgres 원장 (019)")
        outputs.str("spec", label="선정 조건 (provenance 에 기록됨)")
        outputs.int("n_picked", label="고른 문장 수")
        outputs.str("class_counts", label="클래스별 개수")
        outputs.str("coverage", label="클래스 커버리지 점검")
        pt = types.TableView()
        pt.add_column("text", label="문장")
        pt.add_column("cls", label="클래스")
        pt.add_column("wins", label="이긴 프레임")
        pt.add_column("purity", label="정답 비율")
        pt.add_column("net", label="순이득")
        pt.add_column("cos", label="코사인 (이미지↔문장)")
        pt.add_column("margin", label="마진 (2등 클래스와의 차)")
        pt.add_column("p_iou", label="제품규칙 IoU ↓")
        outputs.list("picked", types.Object(), label="고른 문장", view=pt)
        outputs.view("numhint", types.Notice(
            label="채택 근거 읽는 순서 — ① 마진이 0.01 미만이면 우연에 가깝다(실측 승리 마진 중앙값 "
                  "≈0.01) ② 코사인이 높아도 정답 비율이 낮으면 배경 자석 ③ 제품규칙 IoU 는 "
                  "낮을수록 탐지되는 쪽, 단 프레임의 성질이라 문장 개별 인과는 아니다"))
        outputs.str("csv", label="CSV (컨테이너)")
        outputs.str("host_csv", label="CSV (호스트, repo 기준 상대경로)")
        tbl = types.TableView()
        tbl.add_column("line", label="진행")
        outputs.list("log", types.Object(), label="스테이지 로그", view=tbl)
        outputs.str("next", label="다음 명령 — 벡터(npz) 만들기")
        outputs.view("hint", types.Notice(
            label="CSV·provenance JSON 은 생성됐습니다. 원장 등록 여부는 위 「Postgres 원장」 칸을 "
                  "확인하세요(DSN 미설정·DB 오류 시 생략됨). 벡터화는 문장 수에 비례해 오래 걸려 "
                  "App 을 막지 않도록 위 명령으로 따로 실행합니다"))
        return types.Property(outputs, view=types.View(label="뱅크 버전 결과"))


def _score_texts(view, tag, classes, cand_c, texts):
    """후보 문장 묶음을 현재 뷰에서 재채점 — 진입률·순이득·배경코사인.

    `ProbePrompt`(사람이 입력) 와 `GeneratePrompts`(LLM 이 생성) 가 **같은 채점부**를 쓴다.
    규칙을 두 벌로 두면 "생성기가 낸 점수"와 "프로브가 낸 점수"가 갈려 비교가 무의미해진다.
    """
    n = view.count()
    if n > MAX_FRAMES:
        raise ValueError(f"{n:,}장은 상한 {MAX_FRAMES:,} 초과 — 뷰를 좁히세요")

    need = ["embedding", f"probe_bar_{tag}", f"probe_votes_{tag}",
            f"probe_topc_{tag}", f"probe_out_{tag}", "ground_truth.label"]
    # ⚠️ 필드당 `values()` 를 따로 부르면 컬렉션을 필드 수만큼 **전체 순회**한다 —
    #    `values([...])` 는 한 번의 집계로 끝난다 (배열·순서·길이 동일). 형제 플러그인
    #    user-prompt-compare 가 603k 행에서 119.5s → 8.3s 로 줄인 것과 같은 처치
    #    (2026-08-14 감사 실측: 이 파일에서도 4곳 확인, 4~5배).
    emb, bar, votes, topc, out_c, gtl = view.values(need)
    E = np.asarray(emb, dtype="float32")
    E /= np.linalg.norm(E, axis=1, keepdims=True) + 1e-12
    bar = np.asarray(bar, dtype="float32")
    votes = np.asarray(votes, dtype="int32")
    topc = np.asarray(topc, dtype="float32")
    out_c = np.asarray(out_c, dtype="int64")
    gt = np.array([classes.index(g) if g in classes else -1 for g in gtl])

    base = (votes + (topc + 2.0) / 10.0).argmax(axis=1)
    base_ok = base == gt
    # 배경 코사인 — GT=normal 프레임과의 평균 유사도. 높으면 「배경 자석」
    ni = classes.index("normal") if "normal" in classes else 0
    bg_mask = gt == ni

    rows, cur_v, cur_t, cur_out = [], votes, topc, out_c
    prev_ok = base_ok            # 이 문장 **직전** 상태 — 행별 값은 한계효과여야 한다
    for txt in texts:
        e = _embed_text(txt)
        cos = E @ e
        new, entered = rescore(cos, bar, cur_v, cur_t, cur_out, cand_c)
        new_ok = new == gt
        # ⚠️ `base_ok` 와 비교하면 값이 **누적**이 되어 앞 문장의 이득이 뒤 문장에 복사된다
        #    (진입 0장인 문장이 "8장 고침"으로 표시되는 증상). 그러면 어느 문장이 일했는지
        #    알 수 없고, 나중에 무엇을 지울지 결정할 근거가 사라진다.
        fixed = int((~prev_ok & new_ok).sum())
        broke = int((prev_ok & ~new_ok).sum())
        rows.append({
            "text": txt[:90],
            "enter_rate": float(entered.mean()),
            "fixed": fixed,
            "broke": broke,
            "net": fixed - broke,
            "bg_cos": float(cos[bg_mask].mean()) if bg_mask.any() else 0.0,
            "max_cos": float(cos.max()),
        })
        # 묶음 평가: 앞 문장이 채택된 상태에서 다음 문장을 잰다
        idx = np.flatnonzero(entered)
        if len(idx):
            cur_v = cur_v.copy(); cur_t = cur_t.copy()
            cur_v[idx, cand_c] += 1
            cur_v[idx, cur_out[idx]] -= 1
            cur_t[idx, cand_c] = np.maximum(cur_t[idx, cand_c], cos[idx])
            prev_ok = new_ok     # 진입이 있었을 때만 상태가 실제로 전진한다

    final = (cur_v + (cur_t + 2.0) / 10.0).argmax(axis=1)
    return {
        "n": int(n),
        "base_acc": float(base_ok.mean()),
        "new_acc": float((final == gt).mean()),
        "total_net": int((final == gt).sum() - base_ok.sum()),
        "rows": rows,
    }


def _result_schema(outputs):
    """`ProbePrompt` / `GeneratePrompts` 공용 출력 스키마 — 같은 채점부니 같은 표로 읽는다."""
    outputs.int("n", label="평가 프레임")
    outputs.str("bank", label="뱅크")
    outputs.str("cls", label="선언 클래스")
    outputs.float("base_acc", label="현재 정확도")
    outputs.float("new_acc", label="후보 채택 시 정확도")
    outputs.int("total_net", label="순이득 (묶음 전체)")
    tbl = types.TableView()
    tbl.add_column("text", label="문장")
    tbl.add_column("enter_rate", label="top-k 진입률")
    tbl.add_column("fixed", label="고친 프레임")
    tbl.add_column("broke", label="망친 프레임")
    tbl.add_column("net", label="순이득")
    tbl.add_column("bg_cos", label="배경 코사인 ↓")
    tbl.add_column("max_cos", label="최고 코사인")
    outputs.list("rows", types.Object(), label="문장별", view=tbl)
    outputs.view("hint", types.Notice(
        label="배경 코사인이 높으면 「배경 자석」입니다 — 진입률이 높아도 채택하지 마세요"))
    return outputs


class ProbePrompt(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="probe_prompt",
            label="② 프롬프트 프로브 — 내가 쓴 문장 채점",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        # 그리드 툴바 — Embeddings 패널이 없는 상태에서도 항상 닿는다
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(label="② 프롬프트 프로브 — 내가 쓴 문장 채점", icon="science", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        tags = _tags(ctx.dataset)
        if not tags:
            inputs.view(
                "none",
                types.Error(
                    label="probe 캐시가 없습니다 — "
                    "`prompt_geometry.py probecache` 를 먼저 실행하세요"
                ),
            )
            return types.Property(inputs)

        dd = types.DropdownView()
        for t in tags:
            _, k, bank = _meta(ctx.dataset, t)
            dd.add_choice(t, label=_bank_label(t, bank, k))
        inputs.enum("tag", tags, default=tags[0], required=True, label="뱅크", view=dd)

        tag = ctx.params.get("tag") or tags[0]
        classes, k, bank = _meta(ctx.dataset, tag)

        inputs.str(
            "text",
            required=True,
            label="후보 문장",
            description="한 줄에 하나씩. 여러 개를 넣으면 **묶음으로** 평가합니다",
            view=types.TextFieldView(),
        )
        cd = types.DropdownView()
        for c in classes:
            cd.add_choice(c, label=c)
        inputs.enum(
            "cls", classes, required=True, label="선언 클래스",
            description="이 문장이 주장하는 클래스", view=cd,
        )

        radio = types.RadioGroup()
        radio.add_choice("CURRENT_VIEW", label="현재 뷰 (필터 적용분)")
        radio.add_choice("DATASET", label="전체 데이터셋")
        inputs.enum("target", radio.values(), default="CURRENT_VIEW",
                    required=True, label="대상", view=radio)

        view = ctx.view if ctx.view is not None else ctx.dataset.view()
        n = view.count() if ctx.params.get("target") != "DATASET" else ctx.dataset.count()
        if n > MAX_FRAMES:
            inputs.view("cap", types.Warning(
                label=f"{n:,}장 — 상한 {MAX_FRAMES:,} 초과. 뷰를 좁히세요"))
        else:
            inputs.view("info", types.Notice(
                label=f"{n:,}장에 대해 top-{k} 재채점 (뱅크 {bank})"))
        return types.Property(inputs, view=types.View(label="프롬프트 프로브"))

    def execute(self, ctx):
        tag = ctx.params["tag"]
        classes, k, bank = _meta(ctx.dataset, tag)
        cand_c = classes.index(ctx.params["cls"])
        texts = [t.strip() for t in str(ctx.params["text"]).splitlines() if t.strip()]
        if not texts:
            raise ValueError("문장을 입력하세요")

        view = ctx.dataset if ctx.params.get("target") == "DATASET" else (
            ctx.view if ctx.view is not None else ctx.dataset.view())
        out = _score_texts(view, tag, classes, cand_c, texts)
        out.update(bank=bank, k=k, cls=ctx.params["cls"])
        return out

    def resolve_output(self, ctx):
        return types.Property(_result_schema(types.Object()),
                              view=types.View(label="프로브 결과"))


# ────────── 문장 생성 (LLM) ──────────
# 백엔드 2종으로 끝난다: Vertex Gemini SDK 는 규격이 다르고, 나머지(로컬 vLLM·Ollama·상용)는
# 전부 OpenAI 호환 `/v1/chat/completions` 하나로 흡수된다. 신규 pip 의존성 0 —
# `google-genai` 는 이미지에 baked 이고 OpenAI 호환은 `requests` 로 충분하다.
GEN_BACKENDS = ("vertex", "openai_compat")
GEN_MODEL = os.environ.get("PROMPT_GEN_MODEL", "gemini-2.5-flash")
GEN_BASE_URL = os.environ.get("PROMPT_GEN_BASE_URL", "")      # 예: http://localhost:11434
GEN_MAX_IMAGES = 6

# 처방이 반대인 두 축. 섞으면 "오탐 고치려고 이벤트 문장을 추가"하는 정반대 동작이 나온다.
#   FP(오탐): 그 프레임을 훔친 이벤트 문장이 문제 → normal 대응자석 또는 삭제. 선언=normal
#   FN(미검출): 이벤트 자석이 없다 → 이벤트 문장 추가. 선언=그 이벤트
GEN_MODES = {
    "FP": ("오탐 줄이기 (GT normal → 이벤트로 오판)", "normal"),
    "FN": ("미검출 줄이기 (GT 이벤트 → normal 로 놓침)", None),
}


def _gen_instruction(mode, decl_cls, target_cls, scenes, state_sent, examples, stealing, attrs):
    """지시문 조립. 문법·장면어·예문은 분석 모듈에서 가져온다 (여기서 재정의하지 않는다)."""
    head = [
        "You write short English search sentences for a CCTV vision system.",
        "The system compares an image to each sentence by cosine similarity and answers with "
        "the class of the closest sentence. So every sentence is a MAGNET.",
        "",
        "GRAMMAR (follow exactly):  It is a {scene}. {state clause} {event clause}",
        f"  allowed scenes: {', '.join(scenes)}",
        f"  neutral state clause: {state_sent}",
        "",
        "RULES:",
        "- One sentence per line. No numbering, no quotes, no commentary.",
        "- Never mention specific objects (a red bag, a blue drum), positions (upper-right) or "
        "times (in the evening) — those become universal magnets that attract everything.",
        "- Describe the PHYSICAL CAUSE of the mistake, not a caption of the picture.",
    ]
    if mode == "FP":
        head += [
            "",
            f"TASK: the system wrongly calls these frames '{target_cls}' but they are normal.",
            f"Write sentences that declare class '{decl_cls}' and name the real cause of the "
            "false signal (reflection on the lens, vehicle headlights, steam, dust, a lens smudge).",
        ]
    else:
        head += [
            "",
            f"TASK: the system misses real '{target_cls}' frames and calls them normal.",
            f"Write sentences that declare class '{target_cls}' and describe the event itself.",
        ]
    if stealing:
        head += ["", "Sentences that currently win these frames (do NOT imitate them):"]
        head += [f"  - {s}" for s in stealing[:6]]
    if attrs:
        head += ["", f"Scene conditions of these frames: {attrs}"]
    if examples:
        head += ["", "Style examples (correct grammar):"] + [f"  - {e}" for e in examples[:3]]
    return "\n".join(head)


def _llm_generate(backend, model, instruction, images):
    """문장 생성 → 원문 텍스트. 실패는 그대로 올려서 모달에 보이게 한다."""
    if backend == "vertex":
        from google import genai
        from google.genai import types as gt

        client = genai.Client(
            vertexai=True,
            project=os.environ["GEMINI_PROJECT"],
            location=os.environ.get("GEMINI_LOCATION", "us-central1"),
        )
        parts = [instruction] + [gt.Part.from_bytes(data=b, mime_type="image/jpeg")
                                 for b in images]
        # thinking 을 끄지 않으면 같은 요청이 4배 느리다 (이미지 8장 3.1s → 12~15s 실측)
        cfg = gt.GenerateContentConfig(thinking_config=gt.ThinkingConfig(thinking_budget=0))
        return client.models.generate_content(model=model, contents=parts, config=cfg).text

    import base64

    import requests

    base = (GEN_BASE_URL or os.environ.get("PROMPT_GEN_BASE_URL") or "").rstrip("/")
    if not base:
        raise ValueError("PROMPT_GEN_BASE_URL 이 없습니다 — 로컬/외부 OpenAI 호환 엔드포인트를 "
                         "지정하거나 backend=vertex 를 쓰세요")
    content = [{"type": "text", "text": instruction}] + [
        {"type": "image_url",
         "image_url": {"url": "data:image/jpeg;base64," + base64.b64encode(b).decode()}}
        for b in images
    ]
    key = os.environ.get("PROMPT_GEN_API_KEY")
    r = requests.post(f"{base}/v1/chat/completions",
                      json={"model": model, "messages": [{"role": "user", "content": content}]},
                      headers={"Authorization": f"Bearer {key}"} if key else {}, timeout=180)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


_LEAD = re.compile(r'^[\s\-*•]*(?:\d+\s*[.)])?\s*["\']?')


def _parse_sentences(raw, limit):
    """번호·불릿·따옴표를 벗기고 문장만. LLM 이 서식을 지키지 않는 것을 전제로 한다."""
    out, seen = [], set()
    for ln in (raw or "").splitlines():
        s = _LEAD.sub("", ln).strip().rstrip('"').rstrip("'").strip()
        if len(s) < 15 or not s.endswith("."):      # 서술문이 아닌 줄(제목·설명)을 버린다
            continue
        k = " ".join(s.lower().split())
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
        if len(out) >= limit:
            break
    return out


class GeneratePrompts(foo.Operator):
    """오탐/미검출 코호트를 보고 LLM 이 보완 문장을 쓰고, 같은 화면에서 즉시 채점한다."""

    @property
    def config(self):
        return foo.OperatorConfig(
            name="generate_prompts",
            label="① 문장 생성 — 오탐/미검출 진단 + 초안",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        if not _probe_tags_safe(ctx):
            return None
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(label="① 문장 생성 — 오탐/미검출 진단 + 초안", icon="auto_awesome", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        tags = _probe_tags_safe(ctx)
        if not tags:
            inputs.view("none", types.Error(
                label="probe 캐시가 없습니다 — `prompt_geometry.py probecache` 를 먼저 실행하세요"))
            return types.Property(inputs)

        dd = types.DropdownView()
        for t in tags:
            _, k, bank = _meta(ctx.dataset, t)
            dd.add_choice(t, label=_bank_label(t, bank, k))
        inputs.enum("tag", tags, default=tags[0], required=True, label="뱅크", view=dd)
        tag = ctx.params.get("tag") or tags[0]
        classes, _k, _bank = _meta(ctx.dataset, tag)

        radio = types.RadioGroup()
        for m, (lab, _) in GEN_MODES.items():
            radio.add_choice(m, label=lab)
        inputs.enum("mode", radio.values(), default="FP", required=True,
                    label="무엇을 고치려는가", view=radio,
                    description="처방이 반대입니다 — 오탐은 normal 자석, 미검출은 이벤트 자석")

        events = [c for c in classes if c != "normal"]
        cd = types.DropdownView()
        for c in events:
            cd.add_choice(c, label=c)
        inputs.enum("target", events, default=events[0] if events else None, required=True,
                    label="대상 이벤트 클래스", view=cd)

        inputs.int("n", default=8, required=True, label="생성 문장 수")
        bd = types.DropdownView()
        bd.add_choice("vertex", label="Vertex Gemini (이미 배선됨)")
        bd.add_choice("openai_compat", label="OpenAI 호환 (로컬 LLM·외부 API)")
        inputs.enum("backend", list(GEN_BACKENDS), default="vertex", required=True,
                    label="모델 백엔드", view=bd)
        inputs.str("model", default=GEN_MODEL, required=True, label="모델명",
                   view=types.TextFieldView())
        inputs.bool("with_images", default=True, label="프레임 이미지를 함께 보내기",
                    description=f"최대 {GEN_MAX_IMAGES}장. 끄면 텍스트 조건만 사용")

        # 코호트 미리보기 — 0장이면 실행 전에 알려준다.
        # ⚠️ `resolve_input` 은 폼 입력마다 재평가되므로 여기서 `_cohort()` 를 부르면
        #    모델명 한 글자 칠 때마다 13k행 × 3컬럼을 다시 읽는다. 그래서 미리보기는
        #    **서버사이드 count** 로만 한다 (`stage_vote` 가 심어둔 필드가 있을 때).
        #    실행 경로는 그대로 probe 캐시로 정확히 재계산한다.
        n_prev = self._cohort_count_fast(ctx, tag, classes)
        if n_prev is not None:
            inputs.view("cohort", types.Notice(label=f"현재 뷰에서 대상 프레임 약 {n_prev:,}장"))
        inputs.view("warn", types.Warning(
            label="아래 점수는 **top-k 규칙**입니다. 제품 규칙(분포 IoU)과 상관이 −0.07 로 "
                  "측정됐고, 선례(사람 작성 5문장)는 top-k +3.53pp 인데 제품 +0.046pp 였습니다. "
                  "채택 판단은 `prompt_geometry.py wave` 재채점 후에 하세요"))
        return types.Property(inputs, view=types.View(label="문장 생성"))

    def _cohort_count_fast(self, ctx, tag, classes):
        """미리보기용 서버사이드 개수. `stage_vote` 가 심은 `vote_<ver>` 가 없으면 None.

        그 필드는 `bank_vote_stream` 결과이고 probe 캐시 재계산과 같은 규칙이라 개수가 맞는다
        (근사치로만 쓰므로 "약" 이라고 표기한다 — k 나 뱅크가 다르면 어긋날 수 있다).
        """
        _classes, _k, bank = _meta(ctx.dataset, tag)
        vt = bank.replace(".", "_")
        vt = vt if vt.startswith("v") else "v" + vt
        fld = f"vote_{vt}"
        if fld not in ctx.dataset.get_field_schema():
            return None
        target = ctx.params.get("target")
        if not target:
            return None
        view = ctx.view if ctx.view is not None else ctx.dataset.view()
        gt, pred = ("normal", target) if (ctx.params.get("mode") or "FP") == "FP" \
            else (target, "normal")
        try:
            return view.match({"ground_truth.label": gt, f"{fld}.label": pred}).count()
        except Exception:
            return None

    def _cohort(self, ctx, tag, classes):
        """(코호트 인덱스, 뷰, 필드값들) — 오탐/미검출 프레임만 골라낸다."""
        view = ctx.view if ctx.view is not None else ctx.dataset.view()
        mode = ctx.params.get("mode") or "FP"
        target = ctx.params.get("target") or next(c for c in classes if c != "normal")
        _v, _t, gtl = view.values([f"probe_votes_{tag}", f"probe_topc_{tag}",
                                   "ground_truth.label"])   # 배치 (위 주석)
        votes = np.asarray(_v, dtype="int32")
        topc = np.asarray(_t, dtype="float32")
        base = (votes + (topc + 2.0) / 10.0).argmax(axis=1)
        gt = np.array([classes.index(g) if g in classes else -1 for g in gtl])
        ni, ti = classes.index("normal"), classes.index(target)
        sel = ((gt == ni) & (base == ti)) if mode == "FP" else ((gt == ti) & (base == ni))
        return np.flatnonzero(sel), view, target

    def execute(self, ctx):
        tag = ctx.params["tag"]
        classes, k, bank = _meta(ctx.dataset, tag)
        mode = ctx.params.get("mode") or "FP"
        idx, view, target = self._cohort(ctx, tag, classes)
        if not len(idx):
            raise ValueError("대상 프레임이 0장입니다 — 뷰를 넓히거나 모드/클래스를 바꾸세요")

        decl = GEN_MODES[mode][1] or target
        # 이웃 프레임은 사실상 같은 그림이라 균등 간격으로 뽑는다 (stage_gen 의 중복제거와 같은 취지)
        pick = idx[np.linspace(0, len(idx) - 1, min(GEN_MAX_IMAGES, len(idx))).astype(int)]
        all_fp = view.values("filepath")     # 컬럼을 한 번만 읽는다 (pick 마다 읽으면 13k행 × N회)
        fps = [all_fp[i] for i in pick]

        # 대조 조건화 — 지금 이 프레임을 이기고 있는 문장. 개선 실측의 98.5%가 "나쁜 자석
        # 제거" 기여였으므로, 무엇이 훔치고 있는지가 이미지 캡션보다 중요한 입력이다.
        # 코호트 **전체**로 세어 삭제 후보 랭킹도 같이 낸다 — 한 문장이 수백 장을 독식하는
        # 경우가 실측된 지배 패턴이고, 그때 정답은 문장 추가가 아니라 그 문장 삭제다.
        vt = bank.replace(".", "_")
        vt = vt if vt.startswith("v") else "v" + vt
        stealing, steal_rank = [], []
        if f"top_prompt_{vt}" in ctx.dataset.get_field_schema():
            vals = view.values(f"top_prompt_{vt}")
            cnt = {}
            for i in idx:
                t = vals[i]
                if t:
                    cnt[t] = cnt.get(t, 0) + 1
            steal_rank = [{"text": t[:110], "n": n, "share": round(n / len(idx), 4)}
                          for t, n in sorted(cnt.items(), key=lambda kv: -kv[1])[:5]]
            # LLM 입력은 중복 제거 — 같은 문장을 6번 넣으면 문맥만 낭비되고 한 문장에 과가중된다
            stealing = list(dict.fromkeys(v for v in (vals[i] for i in pick) if v))
        attrs = []
        for ax in ("daynight", "environment", "person"):
            if ax in ctx.dataset.get_field_schema():
                vals = view.values(f"{ax}.label")
                got = sorted({vals[i] for i in pick if vals[i]})
                if got:
                    attrs.append(f"{ax}={'/'.join(got)}")

        pg, _prof = _pg_profile(ctx.dataset.name)
        instruction = _gen_instruction(
            mode, decl, target, pg.SCENE_WORDS, pg.STATE_SENT,
            (pg.PROBE_CANDIDATES or {}).get(target if mode == "FN" else "normal", []),
            stealing, ", ".join(attrs))

        images = []
        if ctx.params.get("with_images"):
            for fp in fps:
                try:
                    with open(fp, "rb") as f:
                        images.append(f.read())
                except OSError:
                    pass

        raw = _llm_generate(ctx.params["backend"], ctx.params["model"], instruction, images)
        texts = _parse_sentences(raw, int(ctx.params.get("n") or 8))
        if not texts:
            raise ValueError(f"생성 문장을 파싱하지 못했습니다 — 원문 앞부분: {(raw or '')[:200]}")

        out = _score_texts(view, tag, classes, classes.index(decl), texts)
        out.update(bank=bank, k=k, cls=decl, mode=GEN_MODES[mode][0],
                   n_cohort=int(len(idx)), n_images=len(images),
                   stealing=steal_rank, copy_block="\n".join(texts))
        return out

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("mode", label="처방")
        outputs.int("n_cohort", label="대상 프레임")
        outputs.int("n_images", label="모델에 보낸 이미지")

        # 삭제 후보를 생성 결과보다 **위에** 둔다. 실측상 개선의 98.5%가 "나쁜 자석 제거"
        # 기여였고, 한 문장이 코호트를 독식하면 문장 추가보다 그 문장 삭제가 정답이다.
        st = types.TableView()
        st.add_column("text", label="이 프레임들을 이기고 있는 문장")
        st.add_column("n", label="가져간 프레임")
        st.add_column("share", label="코호트 점유율")
        outputs.list("stealing", types.Object(), label="① 삭제 후보 (먼저 볼 것)", view=st)
        outputs.view("del_hint", types.Notice(
            label="점유율이 높은 문장 하나를 지우는 것이 새 문장을 넣는 것보다 이득이 큰 경우가 "
                  "지배적입니다 — 문장 데이터셋에서 그 문장을 제외하고 「뱅크 버전 만들기」로 "
                  "삭제본을 만드세요"))
        _result_schema(outputs)
        outputs.str("copy_block", label="문장 (프로브·태그로 넘길 때 복사)",
                    view=types.TextFieldView())
        outputs.view("gate", types.Warning(
            label="이 결과는 어디에도 저장되지 않습니다. 채택하려면 문장을 뱅크 CSV 로 넣고 "
                  "제품 규칙(`wave`)으로 재채점하세요 — 미채점 LLM 산출물이 원장에 들어가면 "
                  "다음 비교의 기준선이 오염됩니다"))
        return types.Property(outputs, view=types.View(label="생성 결과"))


def register(p):
    p.register(ProbePrompt)
    p.register(ExportBankVersion)
    p.register(GeneratePrompts)


def _self_check():
    """재채점 규칙만 검증 (App·임베딩 서비스 없이)."""
    C = 4
    # 프레임 3장: [0] 진입O·같은클래스 밀림, [1] 진입X, [2] 진입O·다른클래스 밀림
    bar = np.array([0.50, 0.90, 0.50], dtype="float32")
    cos = np.array([0.60, 0.10, 0.60], dtype="float32")
    votes = np.zeros((3, C), dtype="int32")
    votes[:, 0] = 6      # normal 6표
    votes[:, 2] = 4      # fire 4표
    topc = np.full((3, C), -2.0, dtype="float32")
    topc[:, 0] = 0.7
    topc[:, 2] = 0.55
    out_c = np.array([2, 0, 0], dtype="int64")   # 밀려날 자리
    new, entered = rescore(cos, bar, votes, topc, out_c, cand_c=2)

    assert entered.tolist() == [True, False, True], entered
    # [0] fire+1 / fire−1 → 6:4 그대로 normal
    assert new[0] == 0, new[0]
    # [1] 진입 실패 → 변화 없음
    assert new[1] == 0, new[1]
    # [2] fire+1 / normal−1 → 5:5 동표, topc fire 0.60 > normal 0.7? → normal 이 높다
    assert new[2] == 0, new[2]

    # 동표에서 후보 코사인이 더 높으면 뒤집힌다
    cos2 = np.array([0.60, 0.10, 0.95], dtype="float32")
    new2, _ = rescore(cos2, bar, votes, topc, out_c, cand_c=2)
    assert new2[2] == 2, new2[2]

    # 진입만 하고 아무것도 안 바뀌는 경우: 표차가 2 이상이면 1표로는 못 뒤집는다
    v3 = votes.copy(); v3[:, 0] = 8; v3[:, 2] = 2
    new3, _ = rescore(cos2, bar, v3, topc, out_c, cand_c=2)
    assert new3[2] == 0, new3[2]

    # LLM 응답 파싱 — 서식을 지키지 않는 것을 전제로 한 방어가 실제로 먹는지
    raw = ('Here are the sentences:\n'
           '1) It is a warehouse. The camera lens is dirty. Thin haze drifts upward.\n'
           '- "It is a parking lot. Vehicle headlights are shining. Bright glare fills the frame."\n'
           '2. It is a warehouse. The camera lens is dirty. Thin haze drifts upward.\n'   # 중복
           'too short.\n'
           'no trailing period here\n')
    got = _parse_sentences(raw, 8)
    assert len(got) == 2, got                                  # 헤더·짧은줄·마침표없음·중복 제거
    assert got[0].startswith("It is a warehouse."), got[0]      # 번호 접두 제거
    assert got[1].startswith("It is a parking lot."), got[1]    # 불릿+따옴표 제거
    assert got[1].endswith("frame."), got[1]                    # 끝 따옴표만 벗기고 마침표 보존
    assert _parse_sentences("", 8) == [] and _parse_sentences(None, 8) == []
    assert len(_parse_sentences(raw, 1)) == 1                   # limit 준수

    # 처방 2축이 섞이지 않는지 — FP 는 normal 선언 고정, FN 은 대상 이벤트 선언
    assert GEN_MODES["FP"][1] == "normal", GEN_MODES
    assert GEN_MODES["FN"][1] is None, GEN_MODES

    # ⚠️ 배치(placement)는 `ctx.dataset` 이 None 이어도 **예외 없이** None 을 돌려야 한다.
    #    하나라도 raise 하면 툴바의 모든 플러그인 버튼이 함께 사라진다 (2026-08-12 실측 회귀).
    class _NoDs:
        dataset = None
        params = {}
        selected = []
        extended_selection = None
        view = None

    for cls in (ProbePrompt, ExportBankVersion, GeneratePrompts):
        cls().resolve_placement(_NoDs())          # raise 하면 여기서 테스트가 깨진다
    assert ExportBankVersion().resolve_placement(_NoDs()) is None
    assert GeneratePrompts().resolve_placement(_NoDs()) is None
    assert ProbePrompt().resolve_placement(_NoDs()) is not None   # 얘는 조건 없이 항상 뜬다
    assert _has_field(_NoDs(), "text") is False
    assert _probe_tags_safe(_NoDs()) == []

    # winner_gidx 필드명 두 세대 표기
    sch = {"winner_gidx_v080": 1, "winner_gidx_v1084": 1}
    assert _winner_field(sch, "v1.0.8.0") == "winner_gidx_v080"      # 구 표기로 존재
    assert _winner_field(sch, "v1.0.8.4") == "winner_gidx_v1084"     # 신 표기로 존재
    assert _winner_field(sch, "v1.0.5.2") is None                    # 없으면 None (fail-closed)

    # 프로젝트 순위 집계 — 승수/정확도/순이득과 클래스 쿼터
    class _FV:
        def __init__(self, w, g):
            self._w, self._g = w, g
        def values(self, f):
            return self._w if f.startswith("winner_gidx") else self._g
    #  gidx 10=fire(3승 중 2정답) · 11=smoke(2승 0정답) · 12=fire(1승 1정답) · 13=미승리
    wg = [10, 10, 10, 11, 11, 12]
    gt = ["fire", "fire", "smoke", "fire", "normal", "fire"]
    G, T, L = [10, 11, 12, 13], ["a.", "b.", "c.", "d."], ["fire", "smoke", "fire", "fire"]
    r, _wi = _rank_by_project(_FV(wg, gt), "winner_gidx_x", ["fire", "smoke"], G, T, L,
                         top_n=10, per_class=False, min_wins=1, sort_by="net")
    by = {x["gidx"]: x for x in r}
    assert 13 not in by, r                                   # 승수 0 은 후보에서 빠진다
    assert by[10]["wins"] == 3 and by[10]["purity"] == round(2 / 3, 4), by[10]
    assert by[11]["wins"] == 2 and by[11]["purity"] == 0.0, by[11]
    assert by[10]["net"] == 1 and by[11]["net"] == -2 and by[12]["net"] == 1, r
    assert [x["gidx"] for x in r][:2] == [12, 10], r          # net 동률이면 정확도 높은 쪽 먼저
    # 클래스별 쿼터 1개 → fire 1 + smoke 1
    r2, _ = _rank_by_project(_FV(wg, gt), "winner_gidx_x", ["fire", "smoke"], G, T, L,
                          top_n=1, per_class=True, min_wins=1, sort_by="net")
    assert sorted(x["cls"] for x in r2) == ["fire", "smoke"], r2
    # 최소 승수 3 → gidx 10 만
    r3, _ = _rank_by_project(_FV(wg, gt), "winner_gidx_x", ["fire", "smoke"], G, T, L,
                          top_n=10, per_class=False, min_wins=3, sort_by="wins")
    assert [x["gidx"] for x in r3] == [10], r3

    # ⚠️ **오프셋이 붙은 gidx** 에서도 같은 결과가 나와야 한다. 정규화 키로 집계하면서 조회를
    #    원본으로 하면 전부 0승이 되어 "후보 0개"가 조용히 나온다 (2026-08-12 실측 버그).
    off = _gidx_offset()
    OG = [off * 3 + x for x in G]                     # 뱅크 순번 3번 → 300,000+
    owg = [off * 3 + x for x in wg]
    r4, _ = _rank_by_project(_FV(owg, gt), "winner_gidx_x", ["fire", "smoke"], OG, T, L,
                          top_n=10, per_class=False, min_wins=1, sort_by="net")
    assert [x["gidx"] for x in r4][:2] == [off * 3 + 12, off * 3 + 10], r4
    assert {x["wins"] for x in r4} == {3, 2, 1}, r4    # 오프셋 유무와 무관하게 같은 승수
    # 세대가 섞인 경우(프레임=구 로컬 표기, 문장=신 전역 표기)도 조인돼야 한다
    r5, _ = _rank_by_project(_FV(wg, gt), "winner_gidx_x", ["fire", "smoke"], OG, T, L,
                             top_n=10, per_class=False, min_wins=1, sort_by="net")
    assert len(r5) == 3, r5

    # 태그 후보 두 세대 + 필드 선택
    assert _ver_tags("v1.0.8.0") == ["v1080", "v080"], _ver_tags("v1.0.8.0")
    assert _pick_field({"wave_iou_fire_v084": 1}, "wave_iou_fire_{tag}", "v1.0.8.4") \
        == "wave_iou_fire_v084"
    assert _pick_field({}, "wave_iou_fire_{tag}", "v1.0.8.4") is None

    # 채택 근거 수치 — 이긴 프레임의 `cos_best_<클래스>` 가 그 문장의 코사인, 마진은 2등과의 차
    class _FV2:
        def __init__(self, cols):
            self._c = cols
        def get_field_schema(self):
            return dict.fromkeys(self._c, 1)
        def values(self, f):
            # 실제 FiftyOne 계약과 동일: 필드명 리스트를 주면 컬럼 리스트를 돌려준다
            # (2026-08-14 배치화 후 목이 낡아 TypeError 를 냈다 — 목이 API 를 따라야 한다).
            if isinstance(f, (list, tuple)):
                return [self._c[x] for x in f]
            return self._c[f]

    #  프레임 3장: fire 코사인 [.30,.40,.20] / normal [.10,.35,.25] / smoke 없음
    cols = {"cos_best_fire": [0.30, 0.40, 0.20], "cos_best_normal": [0.10, 0.35, 0.25],
            "wave_iou_fire_v1022": [0.10, 0.20, 0.30]}
    rows = [{"gidx": 5, "cls": "fire"}]
    _cos_columns(_FV2(cols), rows, {5: [0, 1]}, ["fire", "normal"], "v1.0.2.2", 100000)
    assert rows[0]["cos"] == round((0.30 + 0.40) / 2, 4), rows
    assert rows[0]["margin"] == round(((0.30 - 0.10) + (0.40 - 0.35)) / 2, 4), rows
    assert rows[0]["p_iou"] == round((0.10 + 0.20) / 2, 4), rows
    # 필드가 없으면 조용히 None (표에 빈칸) — 예외로 죽지 않는다
    rows2 = [{"gidx": 5, "cls": "fire"}]
    _cos_columns(_FV2({}), rows2, {5: [0]}, ["fire"], "v1.0.2.2", 100000)
    assert rows2[0]["cos"] is None and rows2[0]["p_iou"] is None, rows2

    # 행별 귀속이 **한계효과**인지 — 앞 문장이 고친 이득이 뒤 문장에 복사되면 안 된다.
    # 프레임 5장 GT=fire, 현재 예측 normal(오답). 문장 A 는 진입해 5장을 고치고,
    # 문장 B 는 아무 프레임에도 진입하지 않는다 → B 는 고침 0 이어야 한다.
    # (누적 귀속 버그에서는 B 가 A 의 5장을 그대로 보고했다 = night5 가 5문장에 464 를 복사한 것과 같은 오류)
    class _V:                                    # 최소 뷰 스텁 — App·임베딩 서비스 없이 검증
        def __init__(self, n):
            self._n = n

        def count(self):
            return self._n

        def values(self, f):
            # 실제 계약과 동일하게 필드명 리스트도 받는다 (2026-08-14 배치화)
            if isinstance(f, (list, tuple)):
                return [self.values(x) for x in f]
            if f == "embedding":
                return [[1.0] + [0.0] * 7 for _ in range(self._n)]
            if f.startswith("probe_bar"):
                return [0.5] * self._n
            if f.startswith("probe_votes"):
                return [[6, 0, 4, 0] for _ in range(self._n)]
            if f.startswith("probe_topc"):
                return [[0.7, -2.0, 0.55, -2.0] for _ in range(self._n)]
            if f.startswith("probe_out"):
                return [0] * self._n             # 진입 시 normal 이 밀려난다
            return ["fire"] * self._n            # ground_truth.label

    real_embed = globals()["_embed_text"]
    # A(cos 1.0 > bar 0.5) 는 진입, B(cos 0.0) 는 진입 못 한다
    globals()["_embed_text"] = lambda t: np.array(
        ([1.0] + [0.0] * 7) if t.startswith("A") else ([0.0, 1.0] + [0.0] * 6), dtype="float32")
    try:
        r = _score_texts(_V(5), "vX", ["normal", "falldown", "fire", "smoke"], 2, ["A.", "B."])
    finally:
        globals()["_embed_text"] = real_embed
    assert r["rows"][0]["enter_rate"] == 1.0 and r["rows"][0]["fixed"] == 5, r["rows"][0]
    assert r["rows"][1]["enter_rate"] == 0.0, r["rows"][1]
    assert r["rows"][1]["fixed"] == 0 and r["rows"][1]["broke"] == 0, r["rows"][1]
    assert r["total_net"] == 5, r                 # 묶음 총합은 그대로 5
    print("self-check OK")


if __name__ == "__main__":
    _self_check()
