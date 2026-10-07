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
import glob
import json
import io
import os
import re
import sys
import threading
import time

import numpy as np

import fiftyone as fo
import fiftyone.operators as foo
import fiftyone.operators.types as types

EMBED_URL = os.environ.get("EMBED_URL", "http://embedding-service:8003")
SUFFIX = "-prompts"          # 문장 데이터셋 접미사 (`stage_promptmap` 이 만드는 이름 규칙)
TAG_PREFIX = "bank:"         # 뱅크 버전 후보 문장에 붙는 표본 태그
# 라쏘 선택은 패널이 이 개수로 잘라 보낸다 — user-image-embeddings `SHOW_SAMPLES_CAP`
# (`show_samples(ids[:CAP], use_extended_selection=True)`). 오퍼레이터는 원래 라쏘 크기를 알 수
# 없으므로 "정확히 CAP 도달" 을 절단 의심 신호로 쓴다. 두 값이 어긋나면 이 가드가 빗나간다.
SELECTION_CAP = 500

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
# 온디맨드 채점의 `S = X @ V.T` 는 프레임 × 뱅크 문장 수로 자란다 — MAX_FRAMES 만으로는 못 막는다
# (40k 프레임 × 79,842문장 = 3.2G 셀 ≈ 50GB). 셀당 ≈12~16B(S + wave_iou Bi·임시배열).
# ponytail: 5억 셀 ≈ 6~8GB 과도 — 현행 sourcei·sourcei-OPT × DB 최대 뱅크(49,140문장, 3.7억 셀)는
# 통과시키고 frames 급 재앙만 막는 상한. 좌석 mem_limit 기준으로 조이려면 S 를 청크로 나눠 채점할 것.
MAX_SCORE_CELLS = 500_000_000


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
_CAM_CACHE = {}        # `_rank_inputs` 의 프레임 카메라 목록 — (frames_name, count) 키, 1항목


def _bank_versions(dataset):
    key = (dataset.name, dataset.count())
    if key not in _VER_CACHE:
        _VER_CACHE.clear()                      # 한 데이터셋만 들고 있어 메모리 상한을 고정
        _VER_CACHE[key] = sorted(dataset.count_values("bank_version.label") or {})
    return _VER_CACHE[key]


def _bank_label(tag, bank, k):
    """드롭다운 표시명. `probecache BANK_ATTACH=all` 로 만든 합집합 캐시는 「전체」로 읽힌다.

    ⚠️ 합집합 라벨에 버전을 다 이어 붙이면 안 된다 — 2뱅크 시절엔 짧았지만 31뱅크가 되니
       문자열이 300자를 넘어 드롭다운이 패널 폭을 밀어냈다(2026-08-28 실측). 개수만 쓴다.
    """
    if tag != "all":
        return f"{bank} (k={k})"
    n = bank.count("+") + 1 if bank and bank != "?" else 0
    return f"전체 ({n}뱅크 합집합) k={k}" if n > 2 else f"전체 ({bank}) k={k}"


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


_NEG_CLASS_CACHE = {}


def _negative_class(dataset_name, fallback_classes):
    """음성(정상) 클래스 이름 — **정본에서만** 가져온다("normal" 리터럴 금지, 2026-09-21
    표준화 지시: "다른 데이터셋이 추가되더라도 그 데이터에 맞게"). 반환 `(name, is_guess)`.

    우선순위(하나라도 성공하면 그 값을 쓴다 — 데이터셋 이름으로 캐시해 매번 재확인하지 않는다):
      1) `cohort.COHORTS[dataset_name]['negative_class']` — 이 repo 코호트 표준화 정본
         (`docker/analysis/cohort.py`, 2026-09-16 설계). 미등록·거부(`CohortRefused`)면 다음 단계.
      2) `prompt_geometry.PROFILES[*]['class_names'][0]` — 전 프로필이 "인덱스 0 = 음성"
         규약을 지킨다(sourceh/frames/sourcei/sitej 전부 실측: `{0: "normal", ...}`). `_pg_profile`
         로 이 데이터셋의 프로필을 역매핑한다(등록 안 된 데이터셋이면 ValueError → 다음 단계).
      3) 둘 다 없으면 `fallback_classes[0]` — 이때는 **조용히 넘어가지 않는다**: `is_guess=True`
         를 돌려줘서 호출부가 화면에 "추정했습니다" 를 띄우게 한다.

    두 정본이 등록한 코호트(sourcei/sitej_subway)는 현재 둘 다 "normal" 이라 실제로 갈릴 일이
    없었지만, cohort.py 를 우선하는 이유는 이 표준화의 명시적 정본이고 프로필 dict 보다
    최신(2026-09-16)이기 때문이다.
    """
    if dataset_name in _NEG_CLASS_CACHE:
        return _NEG_CLASS_CACHE[dataset_name]

    result = None
    try:
        if "/workspace" not in sys.path:
            sys.path.insert(0, "/workspace")
        import cohort as _cohort

        cfg = _cohort.resolve(dataset_name)
        result = (cfg["negative_class"], False)
    except Exception:                          # noqa: BLE001 — 미등록·거부 포함, 다음 단계로
        pass

    if result is None:
        try:
            pg, prof = _pg_profile(dataset_name)
            cn = pg.PROFILES[prof]["class_names"]
            result = (cn[0], False)
        except Exception:                      # noqa: BLE001 — 미등록 프로필 포함, 폴백으로
            pass

    if result is None:
        result = (fallback_classes[0], True)

    _NEG_CLASS_CACHE.clear()                    # `_VER_CACHE`(:106-113) 패턴과 동일 — 1개만 유지
    _NEG_CLASS_CACHE[dataset_name] = result
    return result


def _probe_cache_missing_notice(ctx):
    """probe 캐시 부재 안내 — 복붙 가능한 실제 명령으로 (Task 3, 요청 ②).

    `--profile` 은 `_pg_profile` 로 이 데이터셋에서 유도한다. 미등록 데이터셋(`sitej_subway` 등)
    이면 `_pg_profile` 이 ValueError 를 던지므로 **반드시** 감싼다 — 안내문을 만드는 함수 자체가
    죽으면 안 된다(이 함수는 raise 하지 않는다, ProbePrompt/GeneratePrompts/ExplainRule 세 곳이
    같은 메시지를 쓴다 — 문구가 갈리면 안 되므로 여기 한 곳에서만 만든다).
    """
    try:
        _pg, prof = _pg_profile(ctx.dataset.name)
    except Exception as e:                          # noqa: BLE001 — 안내문은 절대 죽지 않는다
        return (f"probe 캐시가 없습니다 — 게다가 이 데이터셋은 분석 프로필에 등록돼 있지 않아 "
                f"`probecache` 를 바로 돌릴 수도 없습니다 ({type(e).__name__}: {e}) — 새 프로필 "
                "등록이 선행돼야 합니다 (별건)")
    return (
        "probe 캐시가 없습니다. 아래를 호스트에서 실행하세요 (약 2~6분):\n\n"
        "```bash\n"
        "docker exec -e BANK_ATTACH=all docker-analysis-1 \\\n"
        "  sh -c \"cd /workspace && nice -n 19 python3 prompt_geometry.py "
        f"probecache --profile {prof}\"\n"
        "```\n\n"
        "완료 후 이 화면을 새로고침하면 캐시가 잡힙니다."
    )


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
    wg, gtl = frames_view.values([winner_fld, "ground_truth.label"])   # 한 번의 집계 (§_score_texts)

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
            # 삭제본. 실측상 개선의 98.5%가 "나쁜 자석 제거" 기여였고(이 파일 1195-1196),
            # 삭제집합은 수백 개 규모라 선택 상한(500) 안에 들어온다 — 유지집합으로 뒤집으면
            # 12,275개를 골라야 해서 원리적으로 표현이 안 됐다.
            radio.add_choice("DROP", label=f"선택한 문장 {len(sel):,}개를 뺀 나머지 (삭제본)")
        radio.add_choice("VIEW", label=f"현재 뷰 전체 {view.count():,}개")
        radio.add_choice("TAG", label="이미 붙여둔 태그")
        default = "SELECTED" if sel else "RANK"
        inputs.enum("source", radio.values(), default=default, required=True,
                    label="대상", view=radio)
        src = ctx.params.get("source") or default

        if src == "DROP":
            self._drop_inputs(ctx, inputs)

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

    def _drop_inputs(self, ctx, inputs):
        """삭제본 폼 — 원본 버전 하나를 고르고 그 안에서 선택 문장을 뺀다."""
        vers = _bank_versions(ctx.dataset)
        if not vers:
            inputs.view("nov_drop", types.Error(label="`bank_version` 이 없는 데이터셋입니다"))
            return
        vd = types.DropdownView()
        for v in vers:
            vd.add_choice(v, label=v)
        inputs.enum("drop_version", vers, default=vers[0], required=True,
                    label="원본 뱅크 버전",
                    description="이 버전에서 선택한 문장을 뺀 사본을 만듭니다. 선택 문장 중 "
                                "이 버전에 없는 것은 그냥 무시됩니다",
                    view=vd)
        inputs.view("dropwarn", types.Warning(
            label="⚠️ top-K 규칙에서는 **클래스별 문장 수가 곧 사전확률**입니다 — 한 클래스만 "
                  "많이 지우면 그 클래스를 통째로 못 잡게 됩니다(실측: 중복컷이 falldown 을 "
                  "16.9%만 남겨 손해의 69%를 만들었다). 실행 후 클래스별 잔존 수를 확인하세요"))

    def _drop_execute(self, ctx):
        """(대상 뷰, 선정조건, 클래스별 before→after, 남는 수, 경고). 삭제 0건이면 거부한다."""
        sel, _ = self._source_counts(ctx)
        if not sel:
            raise ValueError("선택된 문장이 0개입니다 — 지울 문장을 먼저 고르세요")
        base = ctx.params.get("drop_version")
        if not base:
            raise ValueError("원본 뱅크 버전을 고르세요 (`bank_version` 이 없는 데이터셋입니다)")
        bview = ctx.dataset.match({"bank_version.label": base})
        n0 = bview.count()
        if not n0:
            raise ValueError(f"{base} 문장이 0개입니다")
        # 선택 절단·자리표시자 검사는 execute 에서 모드 공통으로 한다 (SELECTED/VIEW/RANK 도 같은 함정)
        target = bview.exclude(sel)
        n1 = target.count()
        dropped = n0 - n1
        # 선택이 다른 버전 행을 가리켰으면 여기서 0 이 된다 — 조용히 원본 사본을 발행하는
        # 것이 최악이므로(계보에 "삭제본"으로 남는데 내용이 같다) 실패로 만든다.
        if not dropped:
            raise ValueError(
                f"{base} 에서 지워진 문장이 0개입니다 — 선택한 {len(sel):,}개가 이 버전에 "
                "없습니다. 패널·뷰에서 그 버전의 문장을 고르고 다시 실행하세요")
        if not n1:
            raise ValueError(f"{base} 문장을 전부 지웁니다 — 빈 뱅크는 만들 수 없습니다")

        before = bview.count_values("category.label") or {}
        after = target.count_values("category.label") or {}
        counts = " / ".join(
            f"{c}: {before.get(c, 0)}→{after.get(c, 0)}" for c in sorted(before))
        gone = [c for c in before if not after.get(c)]
        # `self._missing` 을 쓰지 않는다 — 오퍼레이터 인스턴스는 요청 간 공유돼서(설치판
        # fiftyone 의 `pctx.instances` + plugins cache) 동시 실행 시 남의 경고가 새어 나온다.
        warn = (f"⚠️ {', '.join(gone)} 문장이 **0개**가 됩니다 — 이 뱅크는 그 클래스를 절대 못 "
                "잡습니다" if gone else f"클래스 커버리지 OK ({counts})")
        spec = f"drop: base={base} dropped={dropped} kept={n1}/{n0} selected={len(sel)}"
        return target, spec, counts, n1, warn

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
            key = (frames_name, fds.count())        # `_VER_CACHE` 와 같은 키 — 글자마다 count_values 금지
            if key not in _CAM_CACHE:
                _CAM_CACHE.clear()
                _CAM_CACHE[key] = ([c for c in sorted(fds.count_values("camera") or {}) if c]
                                   if "camera" in fds.get_field_schema() else [])
            cams = _CAM_CACHE[key]
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
        """상위 N 선정 → (선정 표, 대상 뷰, 선정조건 문자열, 커버리지 경고). dry-run 이면 뷰는 None.
        경고는 `self._missing` 에 두지 않고 반환한다 — `_drop_execute` 와 같은 이유(인스턴스 공유)."""
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
        # "id" 도 같은 왕복에 싣는다 — 확정 경로에서 `values("id")` 를 또 부르면 컬렉션 재순회
        gidx, texts, labels, pids = pview.values(["gidx", "text", "category.label", "id"])
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
        coverage = (
            f"⚠️ 이 선정에 {', '.join(missing)} 문장이 **0개**입니다 — 그 현장에 해당 이벤트 "
            "프레임이 없어 승수가 0이기 때문입니다. 이대로 확정하면 그 클래스를 절대 못 잡습니다. "
            "프로젝트를 「전체」로 하거나 최소 승수를 0으로 낮추세요"
        ) if missing else f"클래스 커버리지 OK ({', '.join(sorted(got))})"

        spec = (f"rank: bank={rv} camera={cam} top_n={top_n} "
                f"per_class={per_class} min_wins={min_wins} sort_by={sort_by} "
                f"frames={fview.count()} missing_classes={','.join(missing) or 'none'}")
        if ctx.params.get("dry_run", True):
            return picked, None, spec, coverage
        keep = {r["gidx"] for r in picked}
        ids = [i for i, g in zip(pids, gidx) if g in keep]
        return picked, ctx.dataset.select(ids), spec, coverage

    def execute(self, ctx):
        version = str(ctx.params["version"]).strip()
        if not version:
            raise ValueError("버전 이름을 입력하세요")
        if not re.fullmatch(r"[A-Za-z0-9._-]+", version):     # 파일명(authored_<버전>.csv)이 된다
            raise ValueError(f"버전 이름 {version!r} — 영숫자·점·하이픈·밑줄만 쓸 수 있습니다")
        # 발행 **전에** 결정론적 실패를 거른다 — 부분 산출물이나 잘못된 마커를 남기지 않기 위해.
        # ① 프로필 밖 데이터셋(버튼 노출 조건은 `text` 필드뿐이라 sourcei-OPT-prompts 등에도 뜬다)
        # ② 이미 발행된 버전명 — 스테이지의 FileExistsError 메시지는 CSV 를 rm 하라고 안내하는데
        #    그 뒤 재발행은 원장을 갱신하지 않는다(`_bankfrom_ledger` ON CONFLICT). 이름을 바꾸라고만.
        pg, prof = _pg_profile(ctx.dataset.name)
        csv_pre = f"{pg.PROFILES[prof]['prompt_dir']}/authored_{version}.csv"
        if os.path.exists(csv_pre):
            raise ValueError(f"{version} 은 이미 발행됐습니다 ({csv_pre}) — 버전명을 바꾸세요")
        src = ctx.params.get("source") or "VIEW"
        sel, view = self._source_counts(ctx)
        # ③ 라쏘는 패널이 SELECTION_CAP 으로 잘라 보낸다 — 절단된 선택은 DROP 이면 부분 삭제본,
        #    SELECTED 면 작은 뱅크가 **조용히** 나온다. 정확히 CAP 개를 고른 정당한 선택도 막히는
        #    허위양성은 감수한다 (codex 리뷰 2026-09-08: SELECTED 도 같은 채널).
        if src in ("DROP", "SELECTED") and len(sel) >= SELECTION_CAP:
            raise ValueError(
                f"선택 {len(sel):,}개 = 패널 절단 상한({SELECTION_CAP}) — 라쏘가 더 컸다면 나머지는 "
                "오퍼레이터에 도달하지 않습니다. 라쏘를 더 작게 나누거나, 사이드바 필터로 남길 집합을 "
                "만들어 「현재 뷰 전체」로 발행하세요")
        picked, spec, drop, coverage = None, None, None, ""

        if src == "DROP":
            target, spec, drop_counts, n_kept, drop_warn = self._drop_execute(ctx)
            drop = (drop_counts, n_kept, drop_warn)
        elif src == "RANK":
            picked, target, spec, coverage = self._rank_execute(ctx, version)
            if target is None:                      # 미리보기 — 아무것도 쓰지 않는다
                cnt = {}
                for r in picked:
                    cnt[r["cls"]] = cnt.get(r["cls"], 0) + 1
                return {"version": version, "tag": "(미리보기 — 저장 안 함)",
                        "coverage": coverage,
                        "csv": "-", "host_csv": "-", "next": "-",
                        "spec": spec, "n_picked": len(picked), "class_counts": str(cnt),
                        "picked": picked,
                        "log": [{"line": "미리보기입니다. 확인 후 「미리보기만」을 끄고 "
                                         "다시 실행하면 CSV·원장이 생성됩니다"}]}
        elif src == "TAG":
            tag = ctx.params["tag"]
            target = None                            # 사용자 태그 — 스테이지가 태그로 모은다
        else:
            target = ctx.dataset.select(sel) if src == "SELECTED" else view
            if not target.count():
                raise ValueError("대상이 0개입니다")

        if target is not None:
            # ④ 벡터 전용(자리표시자) 문장이 섞이면 스테이지 `_bank_rows` 가 SystemExit 한다 — 8버전이
            #    그렇고 「현재 뷰 전체」는 거의 항상 섞인다. 발행 전에 같은 판정으로 거른다.
            n_ph = target.match({"text": {"$regex": "^\\s*" + re.escape(pg.PLACEHOLDER_PREFIX)}}).count()
            if n_ph:
                raise ValueError(
                    f"대상에 문장 텍스트가 없는 자리표시자가 {n_ph:,}개 있습니다 (벡터 전용 버전) — "
                    "사이드바 `rule_ok` 가 '미판정' 인 버전을 빼고 다시 고르세요")
            # ⑤ 태그는 **발행 성공 뒤에** 붙이는 provenance 마커다 — 스테이지는 target 뷰를 직접 읽으므로
            #    태그가 내용을 정하지 않는다. 동명 태그가 이미 있으면 지우지 않고 거부한다: 지우면 사용자
            #    수작업 태그가 파괴되고, 동시 실행에서는 CSV 는 A·태그는 B 로 어긋난다 (codex 2026-09-08).
            tag = f"{TAG_PREFIX}{version}"
            n_stale = ctx.dataset.match_tags(tag).count()
            if n_stale:
                raise ValueError(
                    f"태그 {tag} 가 이미 {n_stale:,}개 표본에 있습니다 — 옛 시도의 잔재면 그리드에서 그 태그를 "
                    "지우고 다시 실행하고, 손으로 만든 태그면 「이미 붙여둔 태그」로 발행하세요")

        notes = ctx.params.get("notes") or ""
        if spec:                                     # 선정 조건을 provenance 에 강제로 남긴다
            notes = (notes + " | " + spec).strip(" |")
        # 자기 뷰를 직접 넘긴다 — 스테이지가 전역 태그를 읽으면 같은 파라미터 재클릭(두 번째 실행의
        # untag→tag)만으로 읽기가 어긋나 클래스 뒤바뀐 CSV 가 나왔다 (적대 검증 재현). TAG 모드만 태그로.
        lines, prompt_dir = _run_pg(ctx.dataset.name, "stage_bankfrom", tag, version, notes, target)
        tag_note = tag
        if target is not None:
            try:
                target.tag_samples(tag)              # 발행에 성공한 문장에만 마커를 남긴다
            except Exception as e:                   # noqa: BLE001
                # 발행(CSV/JSON/원장)은 이미 끝났다. 마커 실패를 실패로 올리면 같은 버전 재시도가
                # CSV 사전검사에 막혀 복구 불가가 된다 — fail-soft 로 결과에 경고만 (codex 2026-09-08)
                tag_note = (f"{tag} ⚠️ 태깅 실패({type(e).__name__}: {e}) — CSV/JSON 은 정상, 원장 상태는 "
                            "아래 항목으로 확인. 필요하면 그리드에서 이 태그를 손으로 붙이세요")
        csv_path = f"{prompt_dir}/authored_{version}.csv"
        # ⚠️ 원장(Postgres 019)은 **fail-soft** 다 — DSN 미설정·DB 오류면 조용히 생략되고 CSV/JSON 만
        #    남는다. 결과 안내를 무조건 "원장까지 끝났습니다" 로 띄우면 사용자가 안 붙은 걸 붙은 줄 안다
        #    (codex 지적, 2026-08-12). 스테이지 로그로 실제 등록 여부를 판정해 그대로 보여준다.
        joined = " ".join(lines)
        # 성공줄 전용 토큰 — 실패줄도 "원장 등록 실패" 라 "원장 등록" 만 보면 실패를 등록됨으로 읽었다
        ledger = ("등록됨" if "원장 등록 bank_id=" in joined
                  else "생략/실패 — CSV·JSON 만 생성됨 (아래 로그 확인)")
        out = {
            "version": version, "tag": tag_note, "csv": csv_path, "ledger": ledger,
            "host_csv": csv_path.replace("/data/fiftyone", "docker/data/fiftyone"),
            "next": (f"docker exec docker-analysis-1 python /workspace/prompt_geometry.py "
                     f"bank --csv {csv_path} --version {version}"),
            "log": [{"line": ln} for ln in lines],
        }
        if drop is not None:
            out.update(spec=spec, n_picked=drop[1], class_counts=drop[0], coverage=drop[2])
        if picked is not None:
            out.update(spec=spec, n_picked=len(picked), picked=picked,
                       coverage=coverage,
                       class_counts=str({r["cls"]: sum(1 for x in picked if x["cls"] == r["cls"])
                                         for r in picked}))
        return out

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("version", label="버전")
        outputs.str("tag", label="붙은 태그 (provenance)")
        outputs.str("ledger", label="Postgres 원장 (019)")
        outputs.str("spec", label="선정 조건 (provenance 에 기록됨)")
        # 모드 중립 문구 — RANK 는 고른 수, DROP 은 남는 수를 넣는데 둘 다 "뱅크에 들어가는 문장" 이다.
        # 모드별 분기(ctx.params 의존)보다 문자열 1줄이 작고 어느 호출 경로에서도 같다.
        outputs.int("n_picked", label="뱅크에 들어가는 문장 수")
        outputs.str("class_counts", label="클래스별 개수 (삭제본은 원본→삭제 후)")
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
            inputs.view("none", types.Error(label=_probe_cache_missing_notice(ctx)))
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
# `plm` = 로컬 PLM(PE-Lang 비전타워 + Llama). embedding-service 의 /caption 을 쓴다.
# 외부 API·쿼터가 없고, 비전타워가 채점기(PE-Core-L14-336)와 같은 계보라는 게 유일한 차별점이다
# (PLM-3B config: vision_config.architecture = vit_pe_core_large_patch14_336).
# ⚠️ 3B 캡셔너라 긴 지시문 준수가 Gemini 보다 약하다 — 서술은 잘 하고 형식은 덜 지킨다.
GEN_BACKENDS = ("vertex", "openai_compat", "plm")
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
    head += _standard_constraints(decl_cls if mode == "FP" else target_cls)
    return "\n".join(head)


def _standard_constraints(cls):
    """측정된 규칙을 지시문 끝에 붙인다. **정본은 `/workspace/prompt_standard.py`** 이고
    여기서 문구를 새로 만들지 않는다 — 규칙이 두 곳에 있으면 반드시 드리프트한다.
    모듈을 못 읽으면 조용히 건너뛴다(플러그인이 죽는 것보다 낫다)."""
    try:
        import prompt_standard as ps
    except Exception:
        return []
    if cls not in ps.CLASSES:
        return []
    win = ps.WINNING_FORM[cls]
    sel = " ; ".join(f"{ps.FORMS[f]} = {v:.3f}" for f, v in sorted(ps.SELECTIVITY[cls].items(), key=lambda kv: -kv[1]))
    out = ["", "MEASURED CONSTRAINTS (sourcei GT, prompt_standard.py — follow exactly):",
           f"- Template selectivity for '{cls}': {sel}",
           f"- At least {int(ps.FORM_QUOTA * 100)}% of sentences must use: {ps.FORMS[win]}",
           f"- Content: {ps.MUST_DESCRIBE[cls]}",
           f"- Length {ps.LEN_MIN}-{ps.LEN_MAX} words, present tense, no numbers, no proper nouns.",
           "- Frame clusters carry PLACE 4x more than EVENT (NMI 0.586 vs 0.149) — at most one place phrase."]
    if cls == "normal":
        out.append("- CRITICAL: no posture/collapse/lying/fire/smoke vocabulary at all, not even negated. "
                   "A normal sentence containing posture words steals frames from falldown (measured: 52).")
    return out


def _llm_generate(backend, model, instruction, images, short_ask=None):
    """문장 생성 → 원문 텍스트. 실패는 그대로 올려서 모달에 보이게 한다.

    `short_ask` 는 **PLM 전용 짧은 지시문**이다. 3B 캡셔너는 조립 지시문(문법+장면어+예문+
    강탈문장)을 따라가지 못하고 **in-context 예문을 그대로 복사한다** — 2026-09-03 실측에서
    8문장 요청에 1문장이 나왔고 그게 입력으로 준 강탈 문장이었다("Scene description: A shopper
    squatting low at the station while cleaning"). 짧게 물으면 이미지를 보고 제대로 서술한다.
    Gemini 계열은 조립 지시문이 더 좋은 결과를 내므로 이 인자를 무시한다.
    """
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

    if backend == "plm":
        import requests

        if not images:
            raise ValueError("PLM 은 이미지 없이 쓸 수 없습니다 — "
                             "'프레임 이미지를 함께 보내기' 를 켜세요 (텍스트 전용은 vertex 를 쓰세요)")
        # /caption 은 1장씩 받는다. 장당 ~3s(3B) — GEN_MAX_IMAGES 6장이면 ~20s.
        # 모델은 서비스의 PLM_MODEL_ID 가 정한다(여기 model 인자는 무시된다).
        outs = []
        for b in images:
            r = requests.post(f"{EMBED_URL}/caption",
                              files={"file": ("f.jpg", b, "image/jpeg")},
                              data={"prompt": short_ask or instruction,
                                    "max_new_tokens": "96"}, timeout=300)
            if r.status_code == 503:
                raise ValueError(
                    "PLM 을 올릴 수 없습니다 — GPU1 은 SAM3(서빙) 우선이라 여유가 없으면 "
                    f"배치가 양보합니다. 잠시 뒤 다시 시도하세요. ({r.text[:160]})")
            r.raise_for_status()
            outs.append(r.json().get("text") or "")
        return "\n".join(outs)

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
# 생성기가 지시문을 되받아 쓴 문장은 뱅크에 들어가면 안 된다. 2026-09-03 실측:
# "…but the detector incorrectly identifies it as 'falldown'" 이 마진 게이트를 통과했다
# (마진은 장면 내용이 정하므로 메타 어구가 붙어 있어도 통과한다 → 텍스트 규칙이 따로 필요).
# "The frame shows a person …" 같은 액자 어구는 벗겨내면 쓸 만한 문장이 남는다.
_META = re.compile(r"\b(detector|detect(s|ed|ion)?|classif\w*|ground.?truth|incorrect\w*|"
                   r"wrong\w*|false (positive|alarm)|surveillance frame|this (frame|image|photo))\b", re.I)
_FRAME_LEAD = re.compile(r"^(the |this )?(frame|image|photo|picture|scene|video)\s+"
                         r"(shows|depicts|contains|displays)\s+", re.I)
# 모델이 지시문의 항목명을 그대로 붙여 오는 경우 ("Scene description: …", "Answer: …")
_LABEL_LEAD = re.compile(r"^[A-Za-z][A-Za-z ]{2,24}:\s+(?=[A-Z])")


def _parse_sentences(raw, limit):
    """번호·불릿·따옴표를 벗기고 문장만. LLM 이 서식을 지키지 않는 것을 전제로 한다."""
    out, seen = [], set()
    for ln in (raw or "").splitlines():
        s = _LEAD.sub("", ln).strip().rstrip('"').rstrip("'").strip()
        s = _LABEL_LEAD.sub("", s)      # "Scene description: …" 같은 지시문 라벨 되받기
        s = _FRAME_LEAD.sub("", s)
        if s and s[0].islower():
            s = s[0].upper() + s[1:]
        if len(s) < 15 or not s.endswith("."):      # 서술문이 아닌 줄(제목·설명)을 버린다
            continue
        if _META.search(s):                         # 지시문 되받기 — 버린다
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
        # ⚠️ 2026-09-21 Task 3: `_probe_tags_safe` 게이트를 지웠다 — 게이트가 있으면 캐시 0개인
        # 데이터셋에서 버튼 자체가 안 떠서 `resolve_input` 의 안내문(_probe_cache_missing_notice)
        # 에 도달할 길이 없었다. `ProbePrompt`(:832-837 근처)와 같은 무게이트 형태로 맞춘다.
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(label="① 문장 생성 — 오탐/미검출 진단 + 초안", icon="auto_awesome", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        tags = _probe_tags_safe(ctx)
        if not tags:
            inputs.view("none", types.Error(label=_probe_cache_missing_notice(ctx)))
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
        bd.add_choice("plm", label="로컬 PLM (embedding-service /caption, GPU1 여유 부족 시 503)")
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

        # PLM 은 장면을 **보고 서술**하는 데 강하고 규칙 준수는 약하다. 그래서 형식·금칙 규칙은
        # 아래 prompt_standard.validate 가 사후에 강제하고, 모델에는 짧게만 묻는다.
        subject = ("what the people and the space are doing" if decl == "normal"
                   else f"the visible evidence of {target}")
        short_ask = (f"Describe {subject} in ONE plain present-tense sentence. "
                     "Start with 'A person', 'People', or 'It is'. "
                     "Do not mention cameras, detectors, frames, images, or labels. "
                     "No place names, no speculation about intent.")
        raw = _llm_generate(ctx.params["backend"], ctx.params["model"], instruction, images,
                            short_ask=short_ask)
        texts = _parse_sentences(raw, int(ctx.params.get("n") or 8))
        if not texts:
            raise ValueError(f"생성 문장을 파싱하지 못했습니다 — 원문 앞부분: {(raw or '')[:200]}")

        # 표준 검증 — 금칙어·길이·숫자를 프로브 **전에** 거른다. 특히 normal 에 섞인 자세 어휘는
        # 그대로 두면 falldown 을 강탈하는 자석이 된다(§10 실측). 전량 기각되면 원본을 살려
        # 사람이 보고 판단하게 한다(조용히 빈 결과를 내지 않는다).
        gen_report, dropped = None, []
        try:
            import prompt_standard as ps
            kept, rej, gen_report = ps.validate(texts, decl, ps.ENVS.get("sourcei"))
            dropped = [f"{w} | {t}" for t, w in rej]
            if kept:
                texts = kept
        except Exception as e:
            gen_report = {"error": str(e)[:200]}

        out = _score_texts(view, tag, classes, classes.index(decl), texts)
        out.update(bank=bank, k=k, cls=decl, mode=GEN_MODES[mode][0],
                   n_cohort=int(len(idx)), n_images=len(images),
                   stealing=steal_rank, copy_block="\n".join(texts),
                   rule_report=json.dumps(gen_report, ensure_ascii=False) if gen_report else "",
                   rule_dropped="\n".join(dropped))
        return out

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("mode", label="처방")
        outputs.int("n_cohort", label="대상 프레임")
        outputs.int("n_images", label="모델에 보낸 이미지")
        outputs.str("rule_report", label="표준 규칙 검증 (prompt_standard)")
        outputs.str("rule_dropped", label="규칙 위반으로 버린 문장",
                    view=types.TextFieldView(read_only=True))

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


# ══════════════════════════════════════════════════════════════════════════
# ④ 이 프레임에 뭐가 찍혔나 — PLM 서술을 필드로 (오탐 자동 분해)
# ══════════════════════════════════════════════════════════════════════════
# 오탐 판독은 지금까지 사람이 썸네일을 보며 했다. 절반은 이미 자동이다 —
# `top_prompt_*` / `winner_gidx_*` 가 "어느 문장이 이 프레임을 가져갔나"를 알려준다.
# 나머지 절반이 "그런데 실제로는 뭐가 찍혔나"이고, 그걸 PLM 이 채운다.
# 둘이 나란히 사이드바에 뜨면 모달 하나로 판독이 끝난다.
#
# 필드 타입이 StringField 인 이유: 표본마다 유일한 서술문이라 Classification 으로 만들어도
# Color by 가 의미를 못 가진다(README 「Color by 는 .label 필수」의 반대 경우). 사이드바·모달
# 표시와 텍스트 검색이 목적이므로 문자열이 맞다.
PLM_FIELD = "plm_saw"
PLM_ASK = ("Describe in one plain present-tense sentence exactly what is visible. "
           "Do not mention cameras, detectors, frames, or labels. No speculation.")


class ExplainFrames(foo.Operator):
    """선택한 프레임을 PLM 에 보여 `plm_saw` 에 서술을 심는다."""

    @property
    def config(self):
        return foo.OperatorConfig(
            name="explain_frames",
            label="④ 이 프레임에 뭐가 찍혔나 — PLM 서술을 필드로",
            dynamic=True,
            icon="visibility",
        )

    def resolve_placement(self, ctx):
        # 프레임 데이터셋에서만 (문장 데이터셋 `-prompts` 에는 `text` 가 있다)
        if ctx.dataset is None or _has_field(ctx, "text"):
            return None
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(label="④ 이 프레임에 뭐가 찍혔나 — PLM 서술", icon="visibility", prompt=True),
        )

    def _source(self, ctx):
        """라쏘는 `ctx.selected` 에 안 오고 `extended_selection` 으로만 온다 (③ 과 같은 함정)."""
        sel = list(ctx.selected or [])
        if not sel:
            ext = ctx.extended_selection or {}
            sel = list(ext.get("selection") or []) if isinstance(ext, dict) else []
        view = ctx.view if ctx.view is not None else ctx.dataset.view()
        return sel, view

    def resolve_input(self, ctx):
        inputs = types.Object()
        sel, view = self._source(ctx)
        radio = types.RadioGroup()
        if sel:
            radio.add_choice("SELECTED", label=f"선택한 프레임 {len(sel):,}장")
        radio.add_choice("VIEW", label=f"현재 뷰 앞에서부터 (전체 {view.count():,}장)")
        inputs.enum("target", radio.values(), default="SELECTED" if sel else "VIEW",
                    required=True, label="대상", view=radio)
        inputs.int("n", default=10, required=True, label="최대 장수",
                   description="PLM 은 장당 약 3초다. 20장이면 1분, 오퍼레이터가 그동안 응답하지 않는다")
        inputs.bool("redo", default=False, label=f"이미 `{PLM_FIELD}` 가 있는 것도 다시",
                    description="끄면 비어 있는 것만 채운다 (이어서 돌리기)")
        inputs.view("cost", types.Warning(
            label="GPU1 은 SAM3(서빙) 우선입니다 — 여유가 없으면 PLM 이 양보하고 이 작업은 "
                  "503 으로 멈춥니다. 잠시 뒤 다시 실행하세요."))
        return types.Property(inputs, view=types.View(label="PLM 서술 심기"))

    def execute(self, ctx):
        import requests

        sel, view = self._source(ctx)
        ds = ctx.dataset
        n = max(1, int(ctx.params.get("n") or 10))
        redo = bool(ctx.params.get("redo"))
        if ctx.params.get("target") == "SELECTED" and sel:
            target = ds.select(sel)
        else:
            target = view

        if PLM_FIELD not in ds.get_field_schema():
            ds.add_sample_field(PLM_FIELD, fo.StringField)

        ids, fps, olds = target.values(["id", "filepath", PLM_FIELD])
        todo = [(i, f) for i, f, o in zip(ids, fps, olds) if redo or not o][:n]
        if not todo:
            return {"done": 0, "note": f"채울 것이 없습니다 — 이미 `{PLM_FIELD}` 가 있습니다 "
                                       "(다시 하려면 '이미 있는 것도 다시' 를 켜세요)"}

        wrote, fails = {}, []
        for sid, fp in todo:
            try:
                with open(fp, "rb") as fh:
                    blob = fh.read()
                r = requests.post(f"{EMBED_URL}/caption",
                                  files={"file": ("f.jpg", blob, "image/jpeg")},
                                  data={"prompt": PLM_ASK, "max_new_tokens": "64"}, timeout=300)
                if r.status_code == 503:
                    fails.append(f"GPU 양보(503) — {len(wrote)}장까지만 저장됨")
                    break
                r.raise_for_status()
                wrote[sid] = (r.json().get("text") or "").strip()
            except Exception as exc:                       # per-file fail-forward
                fails.append(f"{os.path.basename(str(fp))}: {exc}")

        if wrote:
            # ⚠️ 전체 데이터셋 순서로 주지 않고 select(ids) 로 쓴다 — 여기는 최대 수십 개라
            #    뷰 스테이지가 작다. 60만 개를 이렇게 쓰면 BSON 16MB 를 넘긴다(등록 스크립트 주석 참고).
            sub = ds.select(list(wrote))
            sub.set_values(PLM_FIELD, [wrote[i] for i in sub.values("id")])
            ds.save()

        prev = [f"{v[:90]}" for v in list(wrote.values())[:3]]
        return {"done": len(wrote), "field": PLM_FIELD,
                "preview": " / ".join(prev), "fails": "; ".join(fails[:3])}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.int("done", label="서술 심은 프레임")
        outputs.str("field", label="필드")
        outputs.str("preview", label="예시")
        outputs.str("fails", label="실패")
        outputs.str("note", label="비고")
        return types.Property(outputs, view=types.View(label="PLM 서술 결과"))


# ══════════════════════════════════════════════════════════════════════════
# ⑤ 판정규칙 실시간 조절 — thr·디바운스(W중M) (요청 ①③, 계획 2026-09-21)
# ══════════════════════════════════════════════════════════════════════════
# 제품 판정규칙은 top-k 가 아니라 **분포 IoU**(`project_pe_inference_dist_iou`) 다. `wave_iou_*`
# 필드는 `stage_wave` 가 이미 심어뒀으므로 여기서는 그 위에서 thr·디바운스만 다시 계산한다 —
# 신규 계산(히스토그램·IoU)은 없다. `dynamic=True` 라 폼 입력마다 `resolve_input` 이 재평가되고,
# 그게 곧 "슬라이더를 끌면 즉시 바뀐다"의 정체다(App 프로세스 안, 서버 왕복 없음, §C.4).
#
# ⚠️ 여기서 나오는 지표는 **top-k 규칙(①②)과 다른 규칙**이다 — top-k 상관 −0.07 로 측정됐으니
#    서로 바꿔 인용하지 말 것(F 함정 #10).
#
# ⚠️ 디바운스 의미는 **count_only** 다 — 제품 `PEEventStateManager.update()`
#    (`pe_inference/01_TuningFree_v2.py:140-146`) 를 그대로 따른다:
#        dq.append(1 if fired else 0); survive = sum(dq) >= threshold
#    "현재 프레임 자신이 발화했는가" 는 묻지 않는다 — 최근 W 개(현재 포함) 중 발화 횟수만 본다.
#    그래서 발화가 막 끊긴 프레임도 직전 창이 차 있으면 살아남는다(버스트 꼬리 래치, 제품 동작).
#    후보였던 `and_self`(자기 자신도 발화해야 함) 는 **오답**이다 — 독립 오라클이 120격자 중
#    96칸에서 두 해석이 갈리는 것을 실측했다(2026-09-21 사양 교정 1/2).
#
# ⚠️ 디바운스 묶음 키는 **비디오가 아니라 이벤트창** `(src_video, event_index)` + `frame_in_event`
#    오름차순이다(2026-09-21 사양 교정 2/2). `sourcei` 6,032장은 105개 비디오를 연속 디코드한
#    게 아니라 **780개 이벤트창**을 2fps 로 뽑은 것 — 같은 비디오의 다른 이벤트창은 실제로 수 분
#    이상 떨어져 있어, 비디오로만 묶으면 그 불연속을 가로질러 디바운스가 샌다. `t_sec` 동률도
#    여기서 나온다(한 비디오에서 서로 다른 35개 이벤트의 첫 프레임이 전부 t=46.01). 두 필드가
#    없는 데이터셋은 `(src_video,)`+`t_sec` 로 낮춘다 — 그 사실을 폼에 알린다(조용히 다른
#    규칙으로 계산하지 않는다). 그조차 없으면 디바운스 컨트롤 자체를 접는다.


def _rule_predict(iou, thr, *, seq=None, window=1, need=1):
    """분포 IoU 판정 재계산 — thr·디바운스(count_only, W중M)만 다시 센다. 의미·묶음 규칙은
    위 섹션 헤더 주석 참조(여기서 반복하지 않는다 — 적대적 리뷰 정리 지적, 세 곳 반복 제거).

    iou[N, n_ev] (결측은 호출자가 +inf 로 채워 절대 발화하지 않게 한다) 열 = 이벤트 클래스
    (정상 제외), 순서는 호출자가 정한다. seq=(group_id[N], order_key[N]) 가 있으면 group_id 가
    같은 행끼리만 order_key 오름차순(stable, 동률은 원래 행 순서 유지)으로 묶어 디바운스한다.
    seq 가 None 이면 디바운스 없음(그대로 반환).

    ⚠️ **`window>1` 로 진입 조건을 걸지 않는다** — trailing cumsum 은 0-프리필 deque 와 모든
    (W,M) 조합에서 수학적으로 동치라 `seq is not None` 만으로 충분하고, `window>1`/`need>1` 식의
    부분 게이트는 W=1 이나 M=1 을 낀 경계 조합에서 **조용히 다른 값**을 낸다(적대적 리뷰 실측:
    현행 `window>1` 21/121 불일치, `window>1 or need>1` 도 3/121 잔존 — `seq is not None` 만이
    4,356격자 전수 0불일치). window/need 는 진입 후 음수 클램프(`max(int(x),0)`) — 안 하면
    `lo=i+1-window` 가 음의 window 에서 배열 경계를 넘어 `cs[lo]` 가 IndexError 를 낸다.
    반환 pred[N] — 살아남은 열 중 **iou 최소**(로컬 0-base) 또는 -1(전부 죽음=normal).
    ⚠️ 반드시 `fired` **안에서만** argmin 한다 — 전역 argmin 은 디바운스로 죽은 열을 고를 수 있다.
    """
    fired = iou < thr
    if seq is not None:
        window, need = max(int(window), 0), max(int(need), 0)
        group_id, order_key = np.asarray(seq[0]), np.asarray(seq[1])
        alive = fired.copy()
        for g in np.unique(group_id):
            m = np.flatnonzero(group_id == g)
            idx = m[np.argsort(order_key[m], kind="stable")]
            f = fired[idx].astype(np.int64)
            cs = np.concatenate([np.zeros((1, f.shape[1]), dtype=np.int64), f.cumsum(0)])
            lo = np.maximum(np.arange(len(idx)) + 1 - window, 0)
            alive[idx] = (cs[np.arange(len(idx)) + 1] - cs[lo]) >= need
        fired = alive
    return np.where(fired.any(1), np.where(fired, iou, np.inf).argmin(1), -1)


def _f1_row(gt, pred, cidx):
    """클래스 하나의 (F1, support, tp, fp, fn, precision, recall) — `analysis_standard.f1_per_class`
    와 같은 정의(tp/fp/fn→precision/recall→F1). 이 App 프로세스는 그 모듈을 새로 import 하지
    않으므로 같은 공식을 여기서 다시 쓴다 — 정의를 바꿀 땐 두 곳을 같이 봐야 한다.

    ⚠️ `f1` 은 **이전과 완전히 같은 식**(반올림 없는 pr/rc 로 계산)에서 나온다 — 회귀 0 요건
    (2026-09-21, "fp/tp 값 노출" 요청). pr/rc 는 **반올림하지 않고** 그대로 돌려준다 — 표시용
    3자리 반올림은 렌더링 쪽(`_f1_table_md`)에서만 한다(F1 재계산에 반올림된 pr/rc 를 쓰면
    바이트 동일 보장이 깨진다).
    """
    tp = int(((pred == cidx) & (gt == cidx)).sum())
    fp = int(((pred == cidx) & (gt != cidx)).sum())
    fn = int(((pred != cidx) & (gt == cidx)).sum())
    pr, rc = tp / max(tp + fp, 1), tp / max(tp + fn, 1)
    f1 = round(2 * pr * rc / max(pr + rc, 1e-12), 4)
    return f1, int(tp + fn), tp, fp, fn, pr, rc


def _scores_from_pred(pred, gt, classes, ev):
    """(fp_normal 또는 None, rows[{cls,f1,support,tp,fp,fn,precision,recall}]) — `pred` 가 이미
    **`classes` 기준 전역 인덱스**로 계산된 뒤 공용으로 쓰는 꼬리. dist_iou(`_rule_metrics`)와
    top-k(`_topk_metrics`, 요청③)가 이 꼬리만 공유한다 — `_rule_metrics` 자체의 dist_iou
    계산부는 건드리지 않는다(회귀 0 요건, 두 규칙이 갈리면 사용자가 화면에서 본 숫자와 실행
    결과가 달라진다).

    ⚠️ 2026-09-21 확장("fp/tp 값 노출" 요청): `rows` 끝에 **normal(음성) 1-vs-rest 요약 행**을
    덧붙인다(n_normal>0 일 때만). F1 이 정밀도·재현율 중 무엇이 낮아서인지 화면만 봐서는
    안 보이던 문제 + 오탐이 스칼라 비율(`fp_normal`)로만 보이던 문제, 둘을 원시 카운트로 푼다.
    normal 행은 tp/fn/precision/recall/f1 을 **None** 으로 비운다 — 1-vs-rest 로 다 정의는
    되지만(`fn`=이 오판 수와 같은 값, `fp`=반대 방향인 "이벤트를 놓치고 normal 이라 한 수") 이
    표의 목적은 「오판한 수」하나를 절대 수로 드러내는 것뿐이라 나머지는 표시 목적과 무관하다.
    그래서 그 칸에 `fp_normal` 의 **분자**(GT=normal 인데 이벤트로 오판한 프레임 수)를 `fp`
    필드에 싣는다 — 코드가 이미 이 값을 `fp_normal`(false positive)이라 불러온 것과 같은
    명명(정상 프레임에 대해 시스템이 오경보를 낸 수)이라 「FP」 칸이 맞다.
    """
    # ⚠️ 리터럴 "normal" 금지(2026-09-21 표준화 지시) — `classes[0]` 은 `_rule_arrays`/
    # `_rule_arrays_ondemand` 가 항상 `[neg_class] + ev` 로 짓기 때문에 **어떤 이름이든** 음성
    # 클래스는 정의상 인덱스 0 이다. 이름으로 찾는 것보다 이게 더 정확하다(이름이 다른
    # 코호트에서도 그대로 맞는다).
    normal_idx = 0
    normal_mask = gt == normal_idx
    n_normal = int(normal_mask.sum())
    # ⚠️ 이 두 줄은 예전 `fp_normal` 식을 **그대로** 보존한다(회귀 0) — 같은 불리언 배열에서
    # `.mean()`(비율)과 `.sum()`(분자, 절대 수)을 같이 뽑으므로 둘은 항상 정합한다.
    wrong_normal = (pred[normal_mask] != normal_idx) if n_normal else None
    fp_normal = float(wrong_normal.mean()) if n_normal else None
    fp_normal_count = int(wrong_normal.sum()) if n_normal else None

    rows = []
    for c in ev:
        ci = classes.index(c)
        if (gt == ci).sum() == 0:                    # GT 에 없는 클래스는 표에서 뺀다 (§C.3/G5)
            continue
        f1, support, tp, fp, fn, precision, recall = _f1_row(gt, pred, ci)
        rows.append({"cls": c, "f1": f1, "support": support, "tp": tp, "fp": fp, "fn": fn,
                     "precision": precision, "recall": recall})
    if n_normal:
        rows.append({"cls": classes[0], "f1": None, "support": n_normal, "tp": None,
                     "fp": fp_normal_count, "fn": None, "precision": None, "recall": None})
    return fp_normal, rows


def _rule_metrics(iou, gt, classes, ev, thr, window, need, group_id, order_key):
    """(pred[N] 전역 클래스 인덱스, fp_normal 또는 None, rows[{cls,f1,support}]) — 폼 실시간
    미리보기(`resolve_input`)와 `execute()` 가 **같은 계산**을 쓰도록 공유한다(둘이 갈리면
    사용자가 화면에서 본 숫자와 실행 결과가 달라진다). dist_iou 전용 — top-k 는 `_topk_metrics`."""
    normal_idx = 0                          # classes[0] 은 항상 음성 클래스 (§표준화, 리터럴 금지)
    ev_idx = np.array([classes.index(c) for c in ev])
    # ⚠️ `window>1` 로 게이트하지 않는다 — `_rule_predict` 와 같은 이유(적대적 리뷰 #2). 이
    # 가드가 `_rule_predict` 내부와 **따로** 있어서, 저기만 고치고 여기를 안 고치면 `seq=None`
    # 이 넘어가 버그가 그대로 산다(실측 지적) — 진입 조건은 오직 `group_id is not None`.
    seq = (group_id, order_key) if group_id is not None else None
    pred_local = _rule_predict(iou, thr, seq=seq, window=window, need=need)
    pred = np.where(pred_local == -1, normal_idx, ev_idx[np.clip(pred_local, 0, None)])
    fp_normal, rows = _scores_from_pred(pred, gt, classes, ev)
    return pred, fp_normal, rows


def _topk_predict(ord_v, ord_c, k, n_cls):
    """`prompt_cos_db.topk_vote` 와 동일한 판정식 — 미리 만든 상위 `K_MAX` 순위표(`ord_v`,
    `ord_c`, `_rule_arrays_ondemand` 가 캐시)에서 앞 `k` 개만 슬라이스해 재계산한다(재조회·
    재argpartition 없음, 요청③). `ord_c` 는 `range(n_cls)` 기준 지역 색인이어야 한다
    (`_bank_cls_to_local` 참조) — 반환 pred[N] 도 같은 색인 체계다.

    ⚠️ **동점 경계 위험**: 코사인이 정확히 같은 값이 k 번째 경계에 걸리면, `topk_vote` 의 단발
    `argpartition(kg-1)` 과 이 함수(고정 top-`K_MAX` 를 먼저 뽑아 정렬한 뒤 자르는 방식)가
    **다른 원소를 top-k 집합에 넣을 수 있다** — `prompt_geometry.py` 의 `vote_topk` 가 이미 같은
    종류의 위험을 문서화했다(선택 집합의 kind 를 바꾸면 동점 주입 200프레임 중 28프레임에서
    pred 가 갈린다는 실측). 부동소수 코사인에서는 사실상 발생하지 않지만, 자체테스트가 인위적
    동점으로 이 경계를 스트레스 테스트한다 — 거기서 갈리면 **`topk_vote` 가 정본이다**(고치지
    않는다, 계획 지시).
    """
    kk = min(int(k), ord_c.shape[1])
    sub_c = ord_c[:, :kk]
    sub_v = ord_v[:, :kk].astype("float32")
    votes = np.stack([(sub_c == c).sum(1) for c in range(n_cls)], 1)
    topc = np.stack([np.where(sub_c == c, sub_v, -2.0).max(1) for c in range(n_cls)], 1)
    return (votes + (topc + 2.0) / 10.0).argmax(1)


def _topk_metrics(ord_v, ord_c, gt, classes, ev, k):
    """(pred[N] 전역(=`classes`) 인덱스, fp_normal, rows) — top-k 전용, `_rule_metrics` 와
    같은 꼬리(`_scores_from_pred`)를 공유한다. dist_iou 와 달리 `_topk_predict` 가 이미
    `classes` 기준 전역 인덱스를 직접 내므로 `ev_idx[...]` 변환이 필요 없다."""
    pred = _topk_predict(ord_v, ord_c, k, len(classes))
    fp_normal, rows = _scores_from_pred(pred, gt, classes, ev)
    return pred, fp_normal, rows


def _rule_view_name(tag, thr, window, need):
    """`thr030_w5m3_<tag>` — 파라미터를 이름에 박아 좌석 공유 중 덮어쓰기 사고를 줄인다(Task 2).
    온디맨드 경로의 뱅크 버전(예: `vGEN.2026.08.28`)은 점을 포함하므로 영숫자 외 문자를
    `_` 로 바꾼다(필드 경로 태그는 이미 영숫자뿐이라 이 치환이 no-op)."""
    safe_tag = re.sub(r"[^A-Za-z0-9]+", "_", tag).strip("_")
    return f"thr{int(round(thr * 100)):03d}_w{window}m{need}_{safe_tag}"


def _topk_view_name(tag, k):
    """`topk_k10_<tag>` — `_rule_view_name` 의 top-k 짝(요청③⑨). 같은 좌석 공유 덮어쓰기
    방지 목적도 동일(Task 2)."""
    safe_tag = re.sub(r"[^A-Za-z0-9]+", "_", tag).strip("_")
    return f"topk_k{int(k)}_{safe_tag}"


def _view_fingerprint(view):
    """뷰 지문 — 캐시 키에 쓴다(적대적 리뷰 #3, blocker).

    `view.count()` 만으로는 표본 수가 같은 **서로 다른** 필터가 충돌한다 — 리뷰어가 재현:
    `ds.match(gt=='falldown')`(599장) vs `ds.select(ids[:599])`(599장, 전혀 다른 표본)가 같은
    캐시 항목을 공유해, `execute()` 가 캐시된 pred/gt 를 **현재 뷰의 id** 와 zip 해서 행 대응이
    무의미한 뷰를 `thr030_w5m3_<tag>` 이름으로 저장 — 좌석 공유라 모두에게 보인다(되돌리기
    어려운 유일한 결함이라고 지적받음). `_VER_CACHE`(:106-113) 선례는 `dataset.count()`(거의
    불변) 키에 **읽기 전용**이라 이 위험이 없었다 — 여긴 `view.count()`(필터마다 변함)에
    **쓰기**라 다르다.

    `view._serialize()` 의 스테이지 목록(클래스+kwargs)을 해시한다. **`_uuid` 는 의도적으로
    뺀다** — 실측상 논리적으로 같은 필터를 다시 만들어도(`ds.match(...)` 두 번) `_uuid` 는
    매번 다르다. 포함시키면 서버가 매 `resolve_input` 재평가마다 뷰를 새로 역직렬화할 때 같은
    필터에도 캐시가 안 맞아 `dynamic=True` 폼이 다시 버벅인다(§C.2, 0.20s/키입력). 비용은
    무시할 만하다(실측 20회 <0.2ms).
    """
    stages = [{k: v for k, v in st.items() if k != "_uuid"} for st in view._serialize()]
    return hash(json.dumps(stages, sort_keys=True, default=str))


def _content_fingerprint(dataset):
    """데이터셋 최종 수정 시각 — 캐시 키에 넣어 **개수는 같은데 내용만 바뀐** 경우(재라벨링,
    태그 편집 등)도 잡는다(2026-09-21 지시② — "새로고침해도 캐시가 그대로다").

    ⚠️ `dataset.last_modified_at` 만으로는 부족하다 — FiftyOne 1.19 는 이 값을 **데이터셋 메타
    (info·필드 스키마 등) 편집에만** 올리고 표본 편집(재라벨링·태그)에는 안 올린다. 그래서
    표본별 `last_modified_at` 의 최댓값을 함께 넣는다. 예전 문서화는 이걸 "뷰 전체를 훑는 O(N)
    Mongo 조회(0.20s)" 라고 봤는데 실측이 다르다 — `last_modified_at` 은 색인 필드라
    `dataset.max()` 집계가 sourcei 6,032장에서 2ms. 둘 중 큰 값을 쓴다(어느 축이 바뀌어도 반응).
    """
    try:
        v = dataset.last_modified_at
        ts = v.timestamp() if v else 0
    except Exception:                      # noqa: BLE001 — 캐시 키 계산은 절대 죽지 않게
        ts = 0
    try:
        m = dataset.max("last_modified_at")
        ts = max(ts, m.timestamp() if m else 0)
    except Exception:                      # noqa: BLE001 — 자체검증 _FakeDS 등 max() 없는 객체
        pass
    return ts


def _bank_fingerprint(version):
    """뱅크 npz 의 `(mtime_ns, size)` — 온디맨드 캐시 키에 넣어 뱅크 재빌드(프롬프트 추가·
    삭제로 새 버전이 같은 파일명에 다시 쓰였을 가능성 포함)를 잡는다(2026-09-21 지시②).
    파일이 없으면(방어적) 0 — 캐시가 무효화되지 않을 뿐 크래시는 없다(그 경우 이후
    `load_bank` 가 어차피 못 찾아 명시적으로 죽는다)."""
    try:
        pg = _pg_module()
        st = os.stat(f"{pg.PROMPT_DIR}/{version}.npz")
        return (st.st_mtime_ns, st.st_size)
    except Exception:                      # noqa: BLE001
        return 0


def _field_cache_key(dataset, tag, view):
    """`_rule_arrays` 필드 경로 캐시 키 — 한 곳에서만 짓는다(호출부가 여러 곳이라 모양이
    갈리면 조용히 다른 캐시 항목을 찾거나 KeyError 가 난다)."""
    return ("field", dataset.name, tag, view.count(), _view_fingerprint(view),
            _content_fingerprint(dataset))


def _ondemand_cache_key(dataset, bank_version, view):
    """`_rule_arrays_ondemand`/`_TOPK_CACHE` 공용 캐시 키 — **세 곳**(만드는 쪽 1 + 읽는 쪽 2,
    top-k 미리보기·top-k 실행)이 정확히 같은 모양을 내야 한다. 여기 한 곳만 고치면 셋 다
    맞는다(2026-09-21 지시② — 뱅크 재빌드까지 반영)."""
    return ("ondemand", dataset.name, bank_version, view.count(), _view_fingerprint(view),
            _content_fingerprint(dataset), _bank_fingerprint(bank_version))


def _wave_ev_fields(schema, tag):
    """`tag` 로 끝나는 `wave_iou_<cls>_<tag>` 필드를 스캔해 (클래스, 필드명) 쌍을 뽑는다.

    클래스명에 밑줄이 없다는 현재 명명 관례를 전제로 마지막 `_<tag>` 접미만 벗긴다(클래스가
    `falldown`/`fire`/`smoke` 처럼 단일 토큰인 동안만 성립 — 복합 클래스명이 생기면 재검토).
    """
    prefix, suffix = "wave_iou_", f"_{tag}"
    out = []
    for f in schema:
        if f.startswith(prefix) and f.endswith(suffix):
            cls = f[len(prefix):-len(suffix)]
            if cls:
                out.append((cls, f))
    return sorted(out)


def _wave_iou_tags(schema):
    """`wave_iou_*` 필드가 있는 태그 전부 — **필드 존재 자체**로 판정한다(적대적 리뷰 #4,
    blocker). 예전엔 `_probe_tags_safe`(probe 캐시 태그, `stage_probecache` 산출물)를 썼는데,
    이 오퍼레이터가 실제로 읽는 건 `stage_wave` 산출물이라 probe 캐시 유무와 무관해야 한다.
    probe 캐시 태그로 게이트하면 ① 정렬 1위가 `all`(31뱅크 합집합, `stage_wave` 가 합집합에는
    필드를 안 만든다)이라 **최초 진입이 100% 에러 화면**이었고 ② `wave_iou` 는 있는데 probe
    캐시가 없는 코호트에서 "probecache 를 실행하라"는 **틀린 처방**이 나왔다(실측 지적).
    """
    tags = set()
    for f in schema:
        if f.startswith("wave_iou_"):
            tag = f.rsplit("_", 1)[-1]           # wave_iou_<cls>_<tag> — 위 밑줄 전제와 동일
            if tag:
                tags.add(tag)
    return sorted(tags)


def _wave_tag_label(dataset, tag):
    """드롭다운 표시명 — probe 캐시가 **어쩌다** 같은 태그에 있으면 `_bank_label` 로 풍성하게,
    없으면 태그 그대로 보여준다. 이 오퍼레이터의 존재 판정은 `wave_iou_*` 필드만 보므로(위
    `_wave_iou_tags`) probe 캐시는 라벨을 예쁘게 하는 보너스일 뿐 없어도 동작해야 한다."""
    _classes, k, bank = _meta(dataset, tag)
    return _bank_label(tag, bank, k) if bank != "?" else tag


_WAVE_DS_CACHE = {}


def _datasets_with_wave_iou():
    """`wave_iou_*` 를 가진 데이터셋 이름 전부 — 전체 데이터셋을 스캔한다(코호트 이름을
    하드코딩하지 않는다 — 후속 지시. 새 데이터셋이 붙어도 자동으로 잡힌다).

    `fo.list_datasets()` 자체는 가볍지만 데이터셋 수만큼 `get_field_schema()` 를 부른다(실측
    10개에서 0.38s — 데이터셋마다 `fo.load_dataset()` 오버헤드가 실측 비용의 대부분). 이 함수는
    "wave_iou 가 아예 없다"는 에러 화면(3·4단계)에서만 부르므로 `dynamic=True` 의 매 키입력
    경로에는 안 들어가지만, 그 안에서도 반복 호출을 막기 위해 `_VER_CACHE`(:106-113) 패턴으로
    캐시한다 — 데이터셋 목록 자체를 키로 써서 추가/삭제 시 자동 무효화된다."""
    key = tuple(sorted(fo.list_datasets()))
    if key not in _WAVE_DS_CACHE:
        _WAVE_DS_CACHE.clear()
        found = []
        for name in key:
            try:
                if _wave_iou_tags(fo.load_dataset(name).get_field_schema()):
                    found.append(name)
            except Exception:                # noqa: BLE001 — 목록 만들기는 절대 죽지 않는다
                continue
        _WAVE_DS_CACHE[key] = sorted(found)
    return _WAVE_DS_CACHE[key]


def _wave_dataset_hint():
    """에러 문구에 덧붙이는 안내 한 줄 — 사용자가 지금 당장 어디로 가면 되는지."""
    found = _datasets_with_wave_iou()
    return (f"현재 `wave_iou_*` 를 가진 데이터셋: {', '.join(found)}" if found
           else "현재 `wave_iou_*` 를 가진 데이터셋이 하나도 없습니다 — `stage_wave` 를 먼저 "
                "실행해야 합니다")


def _resolve_wave_target(ctx):
    """오퍼레이터가 실제로 읽을 (dataset, view) 를 4단으로 해석한다 — 코호트 이름 하드코딩 없이
    이미 있는 규약(`SUFFIX`(:50), `_pg_profile`(:174))만 재사용한다(후속 지시, 2026-09-21).

    실사용 계기: 사용자가 문장 데이터셋(`<이름>-prompts`, `stage_promptmap` 산출물)에서 이
    오퍼레이터를 열었는데 "`stage_wave` 를 돌리세요" 라는 **틀린 처방**이 나왔다 — 문장
    데이터셋엔 애초에 `stage_wave` 를 돌릴 수 없다(프레임이 아니라 문장이 표본이다). 프레임
    데이터셋 역해석은 `ExportBankVersion`(`frames_name = ctx.dataset.name[: -len(SUFFIX)]`,
    `_rank_inputs`/`_rank_execute` 두 곳 선례)과 같은 패턴이다.

    반환 (dataset, view, note) — 성공(1·2단계)이면 `dataset`/`view` 가 실제 값이고 `note` 는
    2단계일 때만 사용자에게 보여줄 안내(1단계는 None, 조용히 그대로). 실패(3·4단계)면
    `(None, None, error_message)` — `resolve_input`/`execute` 는 `dataset is None` 으로 성패를
    가른다. 이 함수 자체는 raise 하지 않는다(내부에서 무엇이 터지든 잡아서 문자열로 돌려준다 —
    호출부마다 try/except 를 반복하지 않기 위해).

    1) 현재 데이터셋/뷰에 `wave_iou_*` 있음 → 그대로(변경 없음).
    2) 현재 이름이 `SUFFIX` 로 끝나고, 베이스 프레임 데이터셋이 존재 + `wave_iou_*` 보유
       → 그 데이터셋 **전량**을 쓴다. 문장 뷰의 사이드바 필터는 프레임에 대응시킬 수 없으므로
       **조용히 바꾸지 않고** 안내를 반환한다.
    3) 어느 쪽도 없지만 `_pg_profile` 로 프로필이 잡힘 → 실제 프로필 이름을 박은 안내(예전엔
       `<profile>` 리터럴이 그대로 나가 틀린 명령이 됐다).
    4) 프로필도 없음(미등록 코호트) → `_pg_profile` 의 ValueError 메시지를 그대로 쓴다(이미
       등록된 후보 목록을 담고 있다 — 새로 안 만든다).
    3)·4) 모두 `_datasets_with_wave_iou()` 목록을 덧붙인다 — 어디로 가면 되는지 바로 보이게.
    """
    try:
        ds = ctx.dataset
        view = ctx.view if ctx.view is not None else ds.view()
        if _wave_iou_tags(view.get_field_schema()):
            return ds, view, None                                        # 1) 그대로

        if ds.name.endswith(SUFFIX):
            base_name = ds.name[: -len(SUFFIX)]
            if fo.dataset_exists(base_name):
                base_ds = fo.load_dataset(base_name)
                if _wave_iou_tags(base_ds.get_field_schema()):
                    base_view = base_ds.view()
                    note = (f"ℹ️ 문장 데이터셋에서 실행 중이라 프레임 데이터셋 `{base_name}` "
                            f"전량({base_view.count():,}장)으로 채점합니다 — 현재 사이드바 "
                            "필터는 반영되지 않습니다.")
                    return base_ds, base_view, note                      # 2) 프레임 데이터셋 전량

        try:
            _pg, prof = _pg_profile(ds.name)
        except ValueError as e:
            return None, None, str(e) + "\n\n" + _wave_dataset_hint()    # 4) 프로필도 없음

        return None, None, (                                             # 3) 프로필은 있는데 없음
            "이 데이터셋/뷰에 `wave_iou_*` 필드가 없습니다 — 호스트에서 "
            "`docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py wave "
            f"--profile {prof}` 을 먼저 실행하세요 (probe 캐시와는 무관합니다)\n\n"
            + _wave_dataset_hint())
    except Exception as e:                       # noqa: BLE001 — 폼은 절대 죽지 않게 (:505 선례)
        return None, None, f"{type(e).__name__}: {e}"


#: 디바운스 묶음 축 후보 (앞이 우선). **새 코호트 편입 = 여기 이름 한 줄.**
#: sourcei=`src_video` / sitej_subway=`video_stem`. `filepath` 는 최후 수단 —
#: 한 폴더에 여러 영상이 섞이면 잘못 묶이므로 진짜 영상 축이 없을 때만 쓴다.
VIDEO_FIELD_CANDIDATES = ("src_video", "video_stem", "video", "source_video")
#: 디바운스 정렬 축 후보 (앞이 우선). 단조 증가하는 수치면 된다(초/프레임번호 무관).
TIME_FIELD_CANDIDATES = ("t_sec", "timestamp_sec", "timestamp", "frame_index")


def _debounce_grouping(sch, view):
    """디바운스 묶음 키(group_id/order_key)+창 길이 통계 — `_rule_arrays`(필드 경로)와
    `_rule_arrays_ondemand`(온디맨드 경로) 공용(후속 지시 — 두 벌로 쪼개면 반드시 갈린다).

    이벤트창 `(src_video,event_index)`+`frame_in_event` 오름차순이 정본, 없으면
    `(src_video,)`+`t_sec` 로 낮춘다 — 자세한 사유는 섹션 헤더 주석 참조(여기서 반복하지 않는다).
    반환 (group_id, order_key, group_sizes, group_label, note) — 아무 필드도 없으면
    앞 넷은 None 이고 note 가 사유 문자열이다.
    """
    # ⚠️ 필드명을 하드코딩하지 않는다 — 코호트마다 다르다 (sourcei `src_video` / sitej_subway
    #    `video_stem`). **새 현장 편입은 아래 후보에 이름 한 줄 추가**이지 코드 수정이 아니다
    #    (2026-09-21 사용자 요구: "다른 데이터셋이 추가되어도 문제없이"). 앞에 있는 것이 우선.
    vid_f = next((f for f in VIDEO_FIELD_CANDIDATES if f in sch), None)
    tim_f = next((f for f in TIME_FIELD_CANDIDATES if f in sch), None)
    has_event = vid_f and "event_index" in sch and "frame_in_event" in sch
    has_fallback = vid_f and tim_f
    if has_event:
        grp_fields, note, label = [vid_f, "event_index", "frame_in_event"], None, "이벤트 창"
    elif has_fallback:
        grp_fields, note, label = [vid_f, tim_f], (
            f"⚠️ `event_index`/`frame_in_event` 가 없어 비디오 단위(`{vid_f}`+`{tim_f}`)로 낮춰 "
            "디바운스합니다 — 같은 비디오의 다른 이벤트창을 가로질러 셀 수 있어 부정확할 수 있습니다"), "비디오"
    else:
        missing = "묶음 축" if not vid_f else "시간 축"
        return None, None, None, None, (
            f"{missing}이 없어 디바운스를 계산할 수 없습니다 — thr 만 조절됩니다 "
            f"(찾은 필드: 묶음={vid_f or '없음'} / 시간={tim_f or '없음'}. "
            f"후보: {', '.join(VIDEO_FIELD_CANDIDATES)} × {', '.join(TIME_FIELD_CANDIDATES)})")

    cols = view.values(grp_fields)
    if has_event:
        vid, event_index, frame_in_event = cols
        group_id = np.array([f"{v}\x00{e}" for v, e in zip(vid, event_index)])
        order_key = np.array([np.inf if v is None else v for v in frame_in_event],
                             dtype="float64")
    else:
        vid, tim = cols
        group_id = np.array([str(v) for v in vid])
        order_key = np.array([np.inf if v is None else v for v in tim], dtype="float64")
    # 창 길이 통계 — 이미 읽은 group_id 에서 뽑는다(추가 Mongo 조회 없음, 2026-09-21 지시)
    _, group_sizes = np.unique(group_id, return_counts=True)
    return group_id, order_key, group_sizes, label, note


#: `_rule_arrays`(필드 경로) 전용 1-항목 캐시. **`_RULE_CACHE`(온디맨드)와 분리돼 있다** —
#: 2026-09-21① 자동전환 도입 전에는 한 `resolve_input` 렌더가 필드/온디맨드 중 **한쪽만**
#: 불렀으므로 하나의 dict 를 접두("field"/"ondemand")로만 나눠 공유해도 안전했다. 자동전환은
#: 매 렌더마다 **필드를 먼저 확인**(`_wave_field_reliable`)하고 못 믿으면 **온디맨드도** 부르므로,
#: 하나의 dict 를 공유하면 두 호출이 서로의 항목을 지워 매 렌더 온디맨드가 캐시 미스로
#: 풀재계산되는 핑퐁이 났다(실측: sourcei 6,032×16,125 재현 — 스테일 상태에서 슬라이더를
#: 움직일 때마다 십수 초씩 걸림, "캐시 히트면 즉시 반응" 요건 위반). 별도 dict 로 완전히 분리해
#: 해결한다 — 두 캐시 다 1-항목 상한은 그대로(`_VER_CACHE`(:106-113) 패턴).
_FIELD_RULE_CACHE = {}

_RULE_CACHE = {}
#: (ord_v, ord_c, n_sentences) 캐시 — `_RULE_CACHE` 와 **같은 키**로 `_rule_arrays_ondemand` 가
#: 동시에 채운다(요청③). 별도 dict 인 이유: `_RULE_CACHE[key]` 의 13-튜플 모양(2026-09-21
#: 확장 — 9-튜플 → 12-튜플(`gt_label`/`n_missing`/`missing_breakdown`) → 13-튜플(`sample_ids`,
#: save_view 정합성 수정) 순으로 늘었다, 기존 소비자 다수)을
#: 그대로 두고 싶어서 — 여기에 얹으면 필드 경로(`_rule_arrays`)까지 모양을 맞춰야
#: 하는데, 필드 경로는 애초에 top-k 를 지원하지 않는다(순위표를 만들 임베딩·뱅크 벡터가 없다).
#: `n_sentences`(2026-09-21②)는 온디맨드 소요시간 안내에 실측 문장 수를 보여주기 위함 —
#: 뱅크를 또 열 필요 없이 이미 로드한 `V.shape[0]` 을 같이 싣는다. **`_RULE_CACHE` 자체는
#: 온디맨드 전용**이다(위 `_FIELD_RULE_CACHE` 참조 — 필드 경로와 더는 안 섞인다).
_TOPK_CACHE = {}


def _rule_arrays(dataset, tag, view):
    """(ev, iou[N,n_ev], gt[N], classes, group_id, order_key, debounce_note, group_sizes,
    group_label, gt_label[N], n_missing, missing_breakdown, sample_ids[N]) — 한 번만 읽어 캐시한다.

    ⚠️ 2026-09-21 확장 두 건(회귀 0 대상 밖 — 이 함수 자체가 지시①②의 수정 대상이다):
    - `classes[0]` 은 더 이상 하드코딩 "normal" 이 아니다 — `_negative_class()` 가 코호트
      레지스트리/프로필에서 이 데이터셋의 실제 음성 클래스 이름을 찾는다(§표준화).
    - `wave_iou_*` 값이 **없는**(`None`) 프레임은 예전엔 `+inf` 로 채워 "절대 발화 안 함" =
      조용히 음성 예측으로 잡혔다(배치 `stage_wave` 이후 추가된 이미지가 정확히 이 상태다).
      이제 그런 프레임은 **N 에서 통째로 제외**한다 — `gt`/`iou`/`group_id`/`order_key` 는
      전부 이 제외를 반영한 뒤의 길이다. 제외된 수·GT 분포는 `n_missing`/`missing_breakdown`
      로 별도 보고한다(호출부가 경고 + 혼동행렬 (결측) 행을 만든다).

    `gt_label`(길이 N, 문자열)은 혼동행렬이 **코호트 밖 GT 클래스**(`gt=-1` 인 이유가 된
    원본 라벨, 예: sitej 의 `intrustion`)를 실제 이름으로 행에 넣기 위해 필요하다 — `gt`
    정수 배열만으로는 그 라벨들이 전부 -1 로 뭉개져 서로 구분이 안 된다.

    `sample_ids`(길이 N, 문자열, 2026-09-21 새 지시 — save_view 정합성 수정)는 `gt`/`iou`
    와 **정확히 같은 순서·같은 결측 제외**를 거친 FiftyOne sample id 다. `execute()`의
    `save_view` 가 예전엔 `view.values("id")`(결측 제외 **전** 전체 N) 를 `pred != gt`
    (결측 제외 **후** N_kept)와 그냥 `zip()` 했다 — `n_missing>0` 이면 zip 이 짧은 쪽에서
    끊겨 **id 와 예측이 서로 다른 프레임을 가리키는 채로 저장 뷰가 만들어졌다**(결측이 뷰
    맨 끝에 몰려있지 않는 한 어긋난다). 이제 호출부가 이 `sample_ids` 로 직접 인덱싱하므로
    그 위험이 구조적으로 없어진다. 조회 비용은 0 — 어차피 읽던 `view.values(fields + [...])`
    호출의 컬럼 목록에 `"id"` 하나를 얹었을 뿐 추가 Mongo 왕복이 없다.

    `group_sizes` = 묶음(이벤트창 또는 폴백 비디오)별 프레임 수 배열 — **추가 조회 없이**
    이미 읽은 `group_id` 에서 바로 뽑는다(`np.unique(..., return_counts=True)`, 결측 제외 전
    전체 기준 — 창 길이는 어떤 프레임이 채점 가능한지와 무관한 데이터 구조상의 성질이라
    더 정확하다는 판단). 중앙값 대비 M 이 크면 "대부분의 창에서 도달 불가" 경고를
    `resolve_input` 이 그때그때 계산한다(재조회 없음, 2026-09-21 추가 지시).

    캐시 키는 `_field_cache_key()` 한 곳에서만 짓는다 — `view.count()`+`_view_fingerprint`
    (적대적 리뷰 #3, 서로 다른 뷰의 동일 개수 충돌 방지) 에 `_content_fingerprint`(데이터셋
    최종수정시각, 2026-09-21 지시② — 새로고침해도 캐시가 그대로인 문제)를 더했다.
    `_VER_CACHE`(:106-113)와 같은 형태로 한 항목만 들고 있어 메모리 상한을 고정한다.

    필드 존재는 `_wave_iou_tags`/`_wave_ev_fields`(필드명 직접 스캔)로 판정한다 — probe 캐시
    (`_meta`)에 의존하지 않는다(적대적 리뷰 #4). 없으면 ValueError — `resolve_input`/`execute`
    가 그대로 사유 한 줄로 보여준다(raise 는 여기선 안전하다 — `resolve_placement` 가 아니다).
    """
    # "field" 접두는 유지한다(키 모양은 `_ondemand_cache_key` 와 여전히 다르다 — 자리 수가
    # 다르다) — 하지만 저장소는 `_FIELD_RULE_CACHE` **전용**이다(2026-09-21① 자동전환이
    # 온디맨드와 캐시를 공유하면 핑퐁이 나서 분리했다, 위 선언부 주석 참조).
    key = _field_cache_key(dataset, tag, view)
    if key not in _FIELD_RULE_CACHE:
        if view.count() > MAX_FRAMES:
            raise ValueError(f"{view.count():,}장 — 상한 {MAX_FRAMES:,} 초과. 뷰를 좁히세요")
        sch = view.get_field_schema()
        ev_fields = _wave_ev_fields(sch, tag)
        if not ev_fields:
            raise ValueError(
                f"태그 {tag} 에 `wave_iou_*` 필드가 없습니다 — 호스트에서 "
                "`docker exec docker-analysis-1 python3 /workspace/prompt_geometry.py wave "
                "--profile <profile>` 을 먼저 실행하세요")
        ev = [c for c, _f in ev_fields]
        fields = [f for _c, f in ev_fields]
        neg_class, neg_is_guess = _negative_class(dataset.name, ["normal"])
        classes = [neg_class] + ev           # 음성은 항상 인덱스 0 — 기존 관례와 동일, 이름만 동적

        cols = view.values(fields + ["ground_truth.label", "id"])
        raw_iou_cols = cols[:len(ev)]
        gtl_all = cols[len(ev)]
        ids_all = cols[len(ev) + 1]              # save_view 정합성 수정(2026-09-21) — 추가 조회 없음
        n_all = len(gtl_all)
        # ⚠️ 2026-09-21 지시① — 결측(None) 프레임은 예측 자체를 만들지 않는다(위 docstring).
        # `any` 이유: 한 프레임의 `wave_iou_*` 열이 부분적으로만 None 인 것은 정상 배치 산출물
        # 형태가 아니다 — 섞여 있으면 보수적으로 전부 결측 취급한다(조용한 오답보다 안전).
        missing_mask = (np.array([any(v is None for v in row) for row in zip(*raw_iou_cols)])
                        if raw_iou_cols else np.zeros(n_all, dtype=bool))
        n_missing = int(missing_mask.sum())
        missing_breakdown = {}
        if n_missing:
            for i in np.flatnonzero(missing_mask):
                lab = gtl_all[i] or "(GT 없음)"
                missing_breakdown[lab] = missing_breakdown.get(lab, 0) + 1
        keep = ~missing_mask

        # 결측 자리는 0.0 으로 채워 배열 구성만 통과시킨다 — 어차피 `[keep]` 로 즉시 버려진다.
        iou_full = np.array([[0.0 if v is None else float(v) for v in c] for c in raw_iou_cols],
                            dtype="float32").T
        iou = iou_full[keep]
        gtl = [g for g, k in zip(gtl_all, keep.tolist()) if k]
        gt = np.array([classes.index(g) if g in classes else -1 for g in gtl])
        ids = [i for i, k in zip(ids_all, keep.tolist()) if k]

        group_id_all, order_key_all, group_sizes, group_label, note = _debounce_grouping(sch, view)
        group_id = group_id_all[keep] if group_id_all is not None else None
        order_key = order_key_all[keep] if order_key_all is not None else None

        if neg_is_guess:
            g_note = f"ℹ️ 음성 클래스를 추정했습니다({neg_class}) — cohort.py 에 등록하면 명시됩니다."
            note = (note + "\n\n" + g_note) if note else g_note
        if n_missing:
            m_note = (f"⚠️ {n_missing:,}장은 `wave_iou` 값이 없어 채점되지 않았습니다(배치 "
                     "`stage_wave` 이후 추가된 이미지로 보입니다). 아래 지표의 모수에서 "
                     "제외했습니다 — 반영하려면 `stage_wave` 재실행이 필요합니다.")
            note = (note + "\n\n" + m_note) if note else m_note

        _FIELD_RULE_CACHE.clear()                # 한 항목만 — 메모리 상한 고정 (_VER_CACHE 와 동형)
        _FIELD_RULE_CACHE[key] = (ev, iou, gt, classes, group_id, order_key, note,
                                  group_sizes, group_label, gtl, n_missing, missing_breakdown, ids)
    return _FIELD_RULE_CACHE[key]


# ────────── 온디맨드 계산 (배치 `stage_wave` 없는 코호트) ──────────
# 실사용 계기(2026-09-21): `sitej_subway` 는 `wave_iou_*` 도 없고 `PROFILES` 등록도 없어 위
# 4단 해석이 "등록이 필요합니다"로 끝났다. 사용자 결정은 "배치(다른 에이전트가 처리)와
# 온디맨드 둘 다" + "상한 없음(대신 예상 소요를 먼저 보여준다)". 새 계산 로직을 만들지 않는다
# — `prompt_cos_db.wave_iou`(기존 공용 원시)를 그대로 쓴다.
def _pg_module():
    """`prompt_geometry` 지연 임포트 — `_gidx_offset`/`_pg_profile` 과 같은 관용구."""
    if "/workspace" not in sys.path:
        sys.path.insert(0, "/workspace")
    import prompt_geometry as pg
    return pg


def _find_embedding_field(view, dim=1024):
    """1024-d 임베딩 후보 필드 — **이름을 하드코딩하지 않는다**(sitej=`embedding`,
    frames=`image_embedding` 로 이름이 다르다, 사용자 지시). 스키마에서 `ListField(FloatField)`
    를 후보로 추리고, 표본 몇 개의 **실제 길이**로 확인한다(스키마만으로는 리스트 길이를 모른다).
    여러 후보가 있으면(`frames` 는 4개) 표본에서 1024 로 확인되는 **첫 후보**(스키마 순서—
    같은 데이터셋에서는 항상 같은 필드가 뽑힌다)를 쓴다. 없으면 None.
    """
    sch = view.get_field_schema()
    cands = [f for f, t in sch.items()
            if type(t).__name__ == "ListField" and type(getattr(t, "field", None)).__name__ == "FloatField"]
    if not cands:
        return None
    sample_cols = view.limit(5).values(cands)          # [[col1],[col2],...] — 순서 = cands
    for f, col in zip(cands, sample_cols):
        if any(v is not None and len(v) == dim for v in col):
            return f
    return None


_BANK_NPZ_CACHE = {}


def _ondemand_bank_candidates():
    """`PROMPT_DIR` 의 `*.npz` 를 스캔해 (version, tag) 후보를 뽑는다 — **`attached_bank`
    필드는 믿지 않는다**(sitej 실측: `vGEN.2026.09.04` 를 가리키는데 그 버전 npz 가
    `PROMPT_DIR` 에 없다). 파일명 자체가 `load_bank()` 의 `version` 인자와 같은 문자열이다.
    `vtag()` 로 표시용 태그를 만든다(필드 기반 경로의 태그와 같은 규칙 — `_wave_iou_tags` 참조).
    디렉토리 목록이 자주 안 바뀌므로 목록 자체를 키로 캐시한다(`_VER_CACHE` 패턴)."""
    pg = _pg_module()
    names = tuple(sorted(os.path.splitext(os.path.basename(p))[0]
                         for p in glob.glob(f"{pg.PROMPT_DIR}/*.npz")))
    if names not in _BANK_NPZ_CACHE:
        _BANK_NPZ_CACHE.clear()
        _BANK_NPZ_CACHE[names] = [(v, pg.vtag(v)) for v in names]
    return _BANK_NPZ_CACHE[names]


def _wave_staleness_note(dataset_name, tag):
    """뱅크 npz 가 그 태그의 `stage_wave` 산출물보다 최신이면 경고 한 줄(2026-09-21 지시③) —
    필드 경로(`wave_iou_*`)는 **배치 산출물**이라 프롬프트가 바뀌어도 새로고침으로는 반영되지
    않는다. 이걸 숨기지 않고 정직하게 알린다. 뱅크→태그 역매핑은 `_ondemand_bank_candidates`
    가 이미 계산해두는 `vtag()` 를 재사용한다(새 계산 로직을 만들지 않는다).

    파일/프로필 부재는 **조용히** None 을 돌려준다(크래시 금지 — 이 파일의 일관된 원칙,
    `_resolve_wave_target` 등과 같은 fail-soft)."""
    try:
        pg, prof = _pg_profile(dataset_name)
        wave_path = f"{pg.PROFILES[prof]['root']}/work/geometry/wave_{tag}.npz"
        if not os.path.exists(wave_path):
            return None
        wave_mtime = os.path.getmtime(wave_path)
        bank_mtimes = [os.path.getmtime(f"{pg.PROMPT_DIR}/{version}.npz")
                      for version, t in _ondemand_bank_candidates()
                      if t == tag and os.path.exists(f"{pg.PROMPT_DIR}/{version}.npz")]
        if not bank_mtimes:
            return None
        newest_bank = max(bank_mtimes)
        if newest_bank <= wave_mtime:
            return None
        fmt = lambda t: __import__("datetime").datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M")
        return (f"⚠️ 뱅크가 wave_iou 필드보다 최신입니다(뱅크 {fmt(newest_bank)} > "
               f"필드 {fmt(wave_mtime)}) — 프롬프트 변경이 아직 반영되지 않았습니다. "
               "반영하려면 stage_wave 재실행, 또는 top-k 처럼 온디맨드 계산을 쓰세요.")
    except Exception:                      # noqa: BLE001 — 폼은 절대 죽지 않게
        return None


def _ondemand_feasibility(dataset, view):
    """온디맨드 wave_iou 계산의 3단 전제(임베딩 필드 → 뱅크 npz 존재 → 뒤는 뱅크별로 다르니
    실제 계산 시점에 확인)를 앞 두 개만 먼저 본다. 반환 dict:
    `{"ok": bool, "reason": str|None, "embed_field": str|None, "banks": [(version,tag),...]}`.
    이 함수는 raise 하지 않는다(폼은 절대 죽지 않게, `_resolve_wave_target` 과 같은 원칙)."""
    try:
        embed_field = _find_embedding_field(view)
        if not embed_field:
            return {"ok": False, "reason": "1024-d 임베딩 필드를 찾지 못했습니다",
                    "embed_field": None, "banks": []}
        banks = _ondemand_bank_candidates()
        if not banks:
            return {"ok": False, "reason": "뱅크 npz 파일을 하나도 찾지 못했습니다",
                    "embed_field": embed_field, "banks": []}
        return {"ok": True, "reason": None, "embed_field": embed_field, "banks": banks}
    except Exception as e:                       # noqa: BLE001 — 폼은 절대 죽지 않게 (:505 선례)
        return {"ok": False, "reason": f"{type(e).__name__}: {e}", "embed_field": None, "banks": []}


def _bank_version_for_tag(tag):
    """`tag` 에 대응하는 뱅크 버전 후보 중 가장 최신(mtime) 것 — 자동전환(2026-09-21 지시①)
    시 온디맨드 드롭다운 기본값으로 쓴다. "지금 필드가 못 믿는 바로 그 뱅크 계열"을 기본으로
    잡는 것이 임의의 첫 후보(`banks[0]`)보다 사용자 의도에 가깝다 — 결측이든 스테일이든, 그
    태그가 원래 가리키던 뱅크를 이어가는 편이 자연스럽다(다른 태그의 뱅크로 튀지 않는다).
    후보가 없으면 None — 호출부가 `banks[0]` 로 폴백한다(fail-soft, `_wave_staleness_note` 와
    같은 원칙: 이 함수는 절대 안 죽는다).
    """
    try:
        pg = _pg_module()
        cands = [(v, t) for v, t in _ondemand_bank_candidates() if t == tag]
        if not cands:
            return None
        return max(cands, key=lambda vt: os.path.getmtime(f"{pg.PROMPT_DIR}/{vt[0]}.npz"))[0]
    except Exception:                          # noqa: BLE001 — 폼은 절대 죽지 않게
        return None


def _wave_field_reliable(rdataset, tag, rview):
    """필드 경로(`wave_iou_*`)를 그대로 믿어도 되는지 — **`resolve_input`/`execute()` 가
    반드시 같은 답을 내야 한다**(2026-09-21 지시① 자동전환 조건. 두 벌로 쪼개면 미리보기와
    실행 결과가 갈린다 — 이 파일의 반복 원칙, `_rule_view_name`/`thr` 클램프 공유 등 선례와 동일).

    믿을 수 없는 두 조건(하나라도 참이면 전환 대상):
      · `n_missing > 0` — 배치 `stage_wave` 이후 추가된 이미지가 있다.
      · `_wave_staleness_note(...)` 가 not None — 뱅크(프롬프트)가 필드보다 최신이다.

    반환 (reliable: bool, rule_arrays: `_rule_arrays` 의 12-튜플, stale: str|None,
    reasons: list[str]). `rule_arrays` 를 함께 돌려주는 이유는 이 함수가 이미 `_rule_arrays`
    를 호출했기 때문(캐시 히트라 재호출 비용은 없다) — 판정에 쓴 데이터와 호출부가 이어서
    쓸 데이터가 같은 호출에서 나와야 시점이 어긋나지 않는다. `reasons` 는 신뢰 못 하는 사유
    조각(해당하는 것 전부, "신규 이미지 N장"/"뱅크가 더 최신")이다 — 안내 문구 조립은
    호출부(`resolve_input`) 몫이고, `execute()` 는 `reliable` 만 보면 된다.
    """
    rule_arrays = _rule_arrays(rdataset, tag, rview)
    n_missing = rule_arrays[10]
    stale = _wave_staleness_note(rdataset.name, tag)
    reasons = []
    if n_missing:
        reasons.append(f"신규 이미지 {n_missing:,}장")
    if stale:
        reasons.append("뱅크가 더 최신")
    return (not reasons), rule_arrays, stale, reasons


#: top-k 순위표 상한 — 요청③⑥. 폼의 k 슬라이더 상한(1~50)과 같아야 한다(`_render_topk_form`).
#: sourcei 실측: 생성 1.48s / 0.9MB, 이후 k 1회 슬라이스 3ms. sitej 는 0.06s/0.4MB.
K_MAX = 50


def _bank_cls_to_local(bank_cls_global, class_names_map, classes):
    """전역 클래스 id(`class_names_map`, 예: `pg.CLASS_NAMES`) → **지역** `classes`
    (`["normal"] + ev`, 이 오퍼레이터 전용 순번) 색인. 요청③ 핵심 경고: 결과는 반드시
    `classes` 리스트의 인덱스여야 한다 — `class_names_map` 의 키(전역 id)를 그대로 쓰면 지역
    리스트의 순서/구성이 다를 때 **조용히 다른 클래스로 잘못 채점된다** (self-check 4번 항목이
    바로 이 함정을 재현한다).

    지역 목록에 없는 전역 클래스(예: 이 코호트/뱅크에서 안 잡힌 클래스)는 -1(무판정) —
    GT 쪽 `-1=무판정` 관례(`_rule_arrays_ondemand` 의 `uncovered`)와 같은 선상.
    """
    g = np.asarray(bank_cls_global)
    name_to_local = {name: i for i, name in enumerate(classes)}
    size = int(g.max()) + 1 if g.size else 1
    lut = np.full(size, -1, dtype="int64")
    for gid, name in class_names_map.items():
        if name in name_to_local and 0 <= gid < lut.size:
            lut[gid] = name_to_local[name]
    return lut[g]


def _rule_arrays_ondemand(dataset, view, embed_field, bank_version):
    """`_rule_arrays` 와 **같은 13-튜플 모양**을 온디맨드로 만든다(2026-09-21 확장 — 9-튜플 →
    12-튜플(`gt_label`/`n_missing`/`missing_breakdown`) → 13-튜플(`sample_ids`, save_view
    정합성 수정) 순으로 늘었다) — `_rule_metrics` 가 출처를 몰라도 되게(필드 경로/온디맨드
    경로가 이후 파이프라인을 공유한다).

    계산은 기존 공용 원시만 쓴다(새로 짜지 않는다, 사용자 지시): `prompt_geometry.load_bank`
    (벡터 정본은 npz) + `prompt_cos_db.wave_iou`(제품과 같은 적응적 binning IoU). `members` 는
    `prompt_geometry.CLASS_NAMES`(전역 클래스: 음성+이벤트) 기준으로 뱅크 문장의 클래스
    인덱스를 모은 것 — **뱅크에 없는 GT 클래스**(예: sitej 의 `intrustion`)는 `classes` 에도
    안 들어가 조용히 -1(무판정) 처리되므로, 그 사실을 `note` 에 명시한다.

    ⚠️ 2026-09-21 지시① — 이 임베딩 필드에 값이 없는(`None`) 프레임은 예전엔
    `np.asarray(emb_col, dtype="float32")` 가 구성 자체에서 크래시하거나(리스트 길이가
    섞이면) 조용히 이상한 배열이 됐다. 필드 경로(`_rule_arrays`)와 같은 원칙으로 **N 에서
    통째로 제외**한다 — 임베딩·GT·그룹·id 축을 전부 같은 `keep` 마스크로 한 번에 자른다(먼저
    자르고 나머지 계산은 기존 그대로, 요청③이 만든 top-k 순위표까지 자동으로 결측이 빠진
    행렬 위에서 계산된다). `sample_ids` 는 `_rule_arrays`(필드 경로)와 같은 이유로 붙였다 —
    save_view 가 `pred`/`gt`(결측 제외 후 N_kept)와 `view.values("id")`(제외 전 전체 N)를
    그냥 zip 해 어긋나던 문제를 없앤다(자세한 사유는 `_rule_arrays` 문서 참조, 두 벌 반복 안 함).

    캐시 키는 `_ondemand_cache_key()` 한 곳에서만 짓는다(2026-09-21 지시② — `_content_fingerprint`
    로 뷰 내용 변화, `_bank_fingerprint` 로 뱅크 npz 재빌드를 잡는다). 저장소는 `_RULE_CACHE` —
    ⚠️ **`_rule_arrays`(필드 경로)의 `_FIELD_RULE_CACHE` 와 더는 같은 dict 를 안 쓴다**
    (2026-09-21① 자동전환 도입 후 분리 — 한 렌더가 필드 확인(`_wave_field_reliable`)과 온디맨드
    계산을 **둘 다** 부를 수 있게 되면서, 하나의 dict 를 공유하면 서로 캐시를 지워 매 렌더
    온디맨드가 풀재계산되는 핑퐁이 났다). top-k 순위표는 `S`(`X @ V.T`)를 버리기 전에 같은
    키로 `_TOPK_CACHE` 에 **별도** 저장한다(요청③, 아래 참조).
    """
    key = _ondemand_cache_key(dataset, bank_version, view)
    if key not in _RULE_CACHE:
        if view.count() > MAX_FRAMES:
            raise ValueError(f"{view.count():,}장 — 상한 {MAX_FRAMES:,} 초과. 뷰를 좁히세요")
        pg = _pg_module()
        bank = pg.load_bank(bank_version)
        n_cells = view.count() * len(bank["vec"])
        if n_cells > MAX_SCORE_CELLS:
            raise ValueError(f"{view.count():,}장 × 문장 {len(bank['vec']):,}개 = {n_cells:,} 셀 — "
                             f"상한 {MAX_SCORE_CELLS:,} 초과. 뷰를 좁히거나 작은 뱅크를 고르세요")
        neg_class, neg_is_guess = _negative_class(dataset.name, ["normal"])
        members = {}
        for i, name in pg.CLASS_NAMES.items():
            idx = np.flatnonzero(bank["cls"] == i)
            if len(idx):
                members[name] = idx
        if neg_class not in members:
            raise ValueError(f"뱅크 {bank_version} 에 {neg_class} 문장이 없어 분포 IoU 기준 "
                             "분포를 잡을 수 없습니다 — 다른 버전을 고르세요")
        if len(members) < 2:
            raise ValueError(f"뱅크 {bank_version} 에 이벤트 클래스 문장이 없습니다")

        # 한 호출로 묶는다 — "id" 를 따로 조회하면 왕복이 하나 더 는다(2026-09-21 save_view
        # 정합성 수정, `_rule_arrays` 와 같은 원칙).
        gtl_all, emb_col, ids_all = view.values(["ground_truth.label", embed_field, "id"])
        missing_mask = np.array([v is None for v in emb_col])
        n_missing = int(missing_mask.sum())
        missing_breakdown = {}
        if n_missing:
            for i in np.flatnonzero(missing_mask):
                lab = gtl_all[i] or "(GT 없음)"
                missing_breakdown[lab] = missing_breakdown.get(lab, 0) + 1
        keep = ~missing_mask

        # ⚠️ 배치(`load_all`/`wave_stream`)와 **정규화를 정확히 맞춘다** — X 는 epsilon 없이
        # 나누고, V(뱅크 벡터)는 npz 값을 그대로 쓴다(재정규화 안 함). 처음엔 안전하게 둘 다
        # epsilon+재정규화를 넣었더니 sourcei 교차검증에서 IoU 가 최대 5.6e-4 어긋났다 —
        # npz 벡터가 이미 거의 단위(노름 0.9999999~1.0000001)라 재정규화가 그 잔차만큼
        # 값을 미세하게 밀고, 적응적 binning 이 경계값에서 그 미세한 밀림을 증폭한 것이다.
        # 최종 등급(F1/fp_normal)은 그때도 동일했지만, "같은 뱅크·같은 bins 면 소수점까지
        # 같아야 한다"는 대조 기준을 맞추려 배치와 완전히 같은 산식으로 고쳤다.
        emb_kept = [v for v, k in zip(emb_col, keep.tolist()) if k]
        # ⚠️ 전부 결측이면(len(emb_kept)==0) 빈 배열끼리의 나눗셈이라 실행 자체는 안전하지만
        # (원소가 없어 0/0 이 실제로 계산되지 않는다), 그 뒤 `wave_iou`/뱅크 매칭이 빈 입력에서
        # 무엇을 하는지는 검증하지 않았다 — 이 경로는 self-check 대상이 아니다(문서화만).
        X = np.asarray(emb_kept, dtype="float32") if emb_kept else np.zeros((0, 1024), "float32")
        X /= np.linalg.norm(X, axis=1, keepdims=True)
        V = np.asarray(bank["vec"], dtype="float32")

        import prompt_cos_db as pcdb                # /workspace 형제 모듈(이미 sys.path 에 있음)
        S = X @ V.T                                  # [N_kept, n_sentences] — top-k 순위표도 이 행렬에서 만든다
        io = pcdb.wave_iou(S, members, bins=pcdb.WAVE_BINS)
        ev = sorted(io.keys())
        iou = np.stack([io[c] for c in ev], axis=1).astype("float32")
        classes = [neg_class] + ev

        gtl = [g for g, k in zip(gtl_all, keep.tolist()) if k]
        gt = np.array([classes.index(g) if g in classes else -1 for g in gtl])
        ids = [i for i, k in zip(ids_all, keep.tolist()) if k]
        uncovered = sorted({g for g in gtl if g and g not in classes})

        sch = view.get_field_schema()
        group_id_all, order_key_all, group_sizes, group_label, note = _debounce_grouping(sch, view)
        group_id = group_id_all[keep] if group_id_all is not None else None
        order_key = order_key_all[keep] if order_key_all is not None else None
        if uncovered:
            extra = (f"⚠️ 뱅크 {bank_version} 에 {', '.join(uncovered)} 문장이 없어 이 "
                    "클래스는 채점에서 제외됩니다")
            note = (note + "\n\n" + extra) if note else extra
        if neg_is_guess:
            g_note = f"ℹ️ 음성 클래스를 추정했습니다({neg_class}) — cohort.py 에 등록하면 명시됩니다."
            note = (note + "\n\n" + g_note) if note else g_note
        if n_missing:
            m_note = (f"⚠️ {n_missing:,}장은 임베딩이 없어 채점되지 않았습니다. 아래 지표의 "
                     "모수에서 제외했습니다.")
            note = (note + "\n\n" + m_note) if note else m_note

        # ── top-k 순위표(K_MAX, 요청③) — S 를 버리기 전에 만든다. ord_c 는 `classes`(지역,
        # 음성=0) 색인이어야 한다(`pg.CLASS_NAMES` 전역 색인이 아니다 — `_bank_cls_to_local`
        # 참조. 여기가 어긋나면 조용한 오답이다).
        bank_cls_local = _bank_cls_to_local(bank["cls"], pg.CLASS_NAMES, classes)
        KM = min(K_MAX, S.shape[1])
        # `-S` 는 S 전체 사본(N×M float32)을 하나 더 만든다 — 뒤에서 어차피 내림차순 재정렬하므로
        # 부호를 뒤집지 않고 큰 쪽 꼬리를 뽑는다(선별 집합 동일, ord_v/ord_c 불변).
        part = np.argpartition(S, S.shape[1] - KM, axis=1)[:, -KM:]
        vpart = np.take_along_axis(S, part, 1)
        order = np.argsort(-vpart, axis=1)
        # ⚠️ **float32 로 저장한다.** 처음 사양은 float16 이었는데(메모리 절감), 그러면
        #    `prompt_cos_db.topk_vote` 와 **조용히 갈린다** — 실측 24,000건 중 2건(0.008%).
        #    기전: votes 가 동률일 때 판정식 `votes + (topc+2)/10` 의 승부가 클래스별 top
        #    코사인 차이로 갈리는데, 그 차이가 float16 분해능(~5e-4)보다 작으면 저장 반올림이
        #    타이브레이크를 뒤집는다. 알고리즘 자체는 무결하다(float32 로는 k 전 구간 완전 일치).
        #    절감액은 sourcei 6,032×50 기준 0.6MB 뿐이고, 정본(`topk_vote`)과의 불일치를
        #    그 값에 파는 건 손해다 — 이 repo 의 반복 버그 형태(조용한 오답)이기도 하다.
        ord_v = np.take_along_axis(vpart, order, 1).astype("float32")
        ord_c = bank_cls_local[np.take_along_axis(part, order, 1)].astype("int8")

        _RULE_CACHE.clear()                 # 한 항목만 — 필드 경로와 같은 상한(:106-113 동형)
        _RULE_CACHE[key] = (ev, iou, gt, classes, group_id, order_key, note,
                            group_sizes, group_label, gtl, n_missing, missing_breakdown, ids)
        _TOPK_CACHE.clear()                 # `_RULE_CACHE` 와 같은 1-항목 상한, 같은 키로 동기화
        # 문장 수(V.shape[0])도 같이 싣는다 — 소요시간 안내(2026-09-21②)가 실측 문장 수를
        # 보여주려면 뱅크를 또 열 필요 없이 여기서 얻는 게 정확하다(같은 로드의 결과이므로).
        _TOPK_CACHE[key] = (ord_v, ord_c, int(V.shape[0]))
    return _RULE_CACHE[key]


def _f1_table_md(rows, n_unscored=0, n_total=0):
    """`rows`(`_scores_from_pred` 반환) → 마크다운 표 문자열 — `_render_rule_form`/
    `_render_topk_form`/`ExplainRule.resolve_output` 이 공유한다(드리프트 방지, "fp/tp 값
    노출" 요청 2026-09-21). `types.Notice` 의 label 은 `remark-gfm` 붙은 ReactMarkdown 으로
    렌더된다(검증됨) — 마크다운 표가 동작한다. 소수는 컬럼 폭 때문에 **3자리**로 줄인다
    (`f1` 필드 자체는 4자리를 유지 — self-check(live) 오라클 대조가 그 정밀도를 쓴다. 표시만
    3자리다, F1 재계산은 하지 않으므로 반올림 손실이 없다).

    음성 클래스 행은 `_scores_from_pred` 가 tp/fn/precision/recall/f1 을 None 으로 비워
    보낸다 — `r["tp"] is None` 으로 그 행을 판별한다(클래스 이름이 "normal" 이 아닐 수도
    있어 이름 비교는 쓰지 않는다, §표준화). 그 칸은 '—' 로 찍고, `fp` 칸(=GT=음성인데
    이벤트로 오판한 절대 수, 기존 `fp_normal` 비율의 분자)만 값을 보여준다(요청 원문:
    "오판한 수가 한눈에 보이는 게 목적").
    """
    if not rows:
        return "(GT 에 존재하는 이벤트 클래스가 없어 클래스별 표를 낼 수 없습니다)"

    def _f(v):
        return "—" if v is None else f"{v:.3f}"

    def _i(v):
        return "—" if v is None else f"{v:,}"

    lines = ["| 클래스 | TP | FP | FN | 정밀도 | 재현율 | F1 | GT |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        label = f"{r['cls']}(음성)" if r["tp"] is None else r["cls"]
        lines.append(f"| {label} | {_i(r['tp'])} | {_i(r['fp'])} | {_i(r['fn'])} | "
                     f"{_f(r['precision'])} | {_f(r['recall'])} | {_f(r['f1'])} | "
                     f"{r['support']:,} |")
    md = "\n".join(lines)
    if n_unscored:
        # ⚠️ "빠졌습니다" 로만 쓰면 부정확하다 — TP/FN/GT 에서는 빠지지만 **이 프레임이 이벤트로
        #    예측되면 FP 로는 계산된다**(GT 가 무엇이든 실제 오경보이므로 세는 게 맞다. `_f1_row`
        #    의 기존 정의이기도 하다). 2026-09-21 브라우저 실측에서 sitej 의 `intrustion` 375장
        #    중 1장이 fire 로 예측돼 fire FP=1 에 들어간 것이 이 경우다 — 표가 그걸 설명해야 한다.
        md += (f"\n\n미채점(gt<0) {n_unscored:,}/{n_total:,}장은 **TP·FN·GT 열에서 빠졌습니다**"
               "(사유는 위 경고). 다만 이 프레임이 이벤트로 예측되면 **FP 로는 계산됩니다** — "
               "GT 를 못 세도 오경보는 실제로 발생한 것이기 때문입니다. "
               "어디로 예측됐는지는 혼동행렬의 `(미채점)` 행에서 볼 수 있습니다.")
    return md


def _confusion_matrix_md(gt_label, pred_idx, classes, missing_breakdown=None, n_view_total=None):
    """혼동행렬(행=GT, 열=예측) 마크다운 — 2026-09-21 지시 변경으로 N×N 전체를 그린다("폼이
    좁다"던 이전 판단은 철회됨, 사용자 명시 요청 "confusion matrix 값이 나오게 해줘").

    열 = `classes`(예측 가능 클래스, 음성이 먼저 — 이미 그 순서로 들어온다, §표준화 지시).
    행 = **데이터에 실제 존재하는 GT 값 전부**(정렬, 음성 먼저) — 하드코딩 목록이 아니라
    `gt_label`/`missing_breakdown` 을 스캔해서 만든다(새 코호트가 새 클래스를 들고 와도
    자동). `classes` 안에 있으면 보통 행, 없으면 `(미채점)` 행(예측은 있지만 GT 가 뱅크/
    코호트 밖 — 대각선 없음. 이게 sitej 의 `intrustion` 375장이 **어디로 오분류되는지**
    보여준다). `missing_breakdown` 은 `(결측)` 행 — GT 는 있지만 IoU/임베딩이 없어 예측
    자체가 없는 프레임(행 안이 전부 '—', 합계에만 더한다 — "같은 방식" 지시를 이렇게 해석:
    예측이 아예 없으므로 열 분해가 구조적으로 불가능하다).

    대각선은 `**굵게**` 로 표시한다(색은 못 쓴다, 요청 사양). 열이 많아지는 코호트(예:
    8클래스)에서도 **자르지 않는다** — 자르면 행 합계가 안 맞는다(요청 사양, 표가 옆으로
    길어지는 쪽을 택한다).
    """
    neg = classes[0]
    gt_arr = np.asarray(gt_label, dtype=object)
    gt_vals = sorted(set(gt_label) | (set(missing_breakdown) if missing_breakdown else set()))
    ordered = ([neg] if neg in gt_vals else []) + sorted(v for v in gt_vals if v != neg)

    header = "| GT \\ 예측 | " + " | ".join(classes) + " | 합계 |"
    sep = "|---|" + "---:|" * (len(classes) + 1)
    lines = [header, sep]

    col_sums = [0] * len(classes)
    grand_total = 0
    for lab in ordered:
        row_mask = gt_arr == lab
        row_total = int(row_mask.sum())
        cells = []
        for ci, cname in enumerate(classes):
            n = int(((pred_idx == ci) & row_mask).sum())
            col_sums[ci] += n
            cells.append(f"**{n}**" if cname == lab else str(n))
        rowlabel = lab if lab in classes else f"{lab}(미채점)"
        lines.append(f"| {rowlabel} | " + " | ".join(cells) + f" | {row_total} |")
        grand_total += row_total

    if missing_breakdown:
        for lab in sorted(missing_breakdown):
            n = missing_breakdown[lab]
            cells = ["—"] * len(classes)
            lines.append(f"| {lab}(결측) | " + " | ".join(cells) + f" | {n} |")
            grand_total += n

    lines.append("| **합계** | " + " | ".join(str(c) for c in col_sums) + f" | {grand_total} |")
    note = ("\n\n대각선(**굵게**)이 정답입니다. `(미채점)` 행은 예측 가능 클래스 밖의 GT — "
           "합계 열/행에 포함됩니다. `(결측)` 행은 IoU/임베딩이 없어 예측 자체가 없는 "
           "프레임 — 행 안이 전부 '—'이고 합계에만 더해집니다.")
    if n_view_total is not None and n_view_total != grand_total:
        note += (f"\n\n⚠️ 표 합계({grand_total:,})가 뷰 전체({n_view_total:,})와 다릅니다 — "
                "내부 불일치이니 보고하세요.")
    return "\n".join(lines) + note


def _render_rule_form(inputs, ctx, ev, iou, gt, classes, group_id, order_key, note,
                      group_sizes, group_label, source_label, gt_label, n_missing,
                      missing_breakdown):
    """thr·W·M 입력 + 실시간 지표 — **필드 경로/온디맨드 경로가 데이터를 얻은 뒤 공유**한다
    (후속 지시로 경로가 둘이 됐지만 렌더링은 한 곳이어야 드리프트가 안 난다).

    `source_label` 은 화면에 항상 보이는 출처 표시(`📊 필드` / `⚡ 온디맨드`) — 사용자 지시:
    "어느 쪽 숫자인지 모르면 비교가 무의미해진다".
    """
    inputs.float("thr", default=0.15, min=0.05, max=0.60, required=True,
                 label="thr (분포 IoU 임계값)",
                 description="낮을수록 엄격(적게 잡음) — 제품 기본값 0.15",
                 view=types.SliderView(view_multiple_of=0.01))

    can_debounce = group_id is not None
    if can_debounce:
        inputs.int("window", default=5, min=1, max=9, required=True,
                   label="디바운스 창 W (프레임)",
                   description="최근 W 프레임(현재 포함, count_only) — 1 이면 디바운스 없음",
                   view=types.SliderView(view_multiple_of=1))
        inputs.int("need", default=3, min=1, max=9, required=True,
                   label="필요 발화 M (W중M)",
                   description="W 프레임 중 M 회 이상 발화해야 산다. M>W 면 항상 normal",
                   view=types.SliderView(view_multiple_of=1))
        if note:
            inputs.view("fallback", types.Warning(label=note))
    else:
        inputs.view("nodebounce", types.Warning(label=note))

    # ⚠️ 값 읽기 — `.get(k, default)` 는 키가 **없을 때만** default 를 쓴다. 값이 명시적으로
    # None 이면(적대적 리뷰 #6 실측) 그대로 통과해 `float(None)` 이 TypeError 를 낸다 —
    # 그래서 `or` 로 받는다. 서버 스키마 min/max 는 제출 시에만 강제되므로 미리보기 단계
    # 에서도 직접 clamp 한다(리뷰 #7 — 클램프 없인 thr=0.9·W=40 이 그대로 미리보기에
    # 반영된다). can_debounce 가 False 면 W/M 입력 자체가 폼에 없으니 5/3 기본값을 그대로
    # 쓰면 "적용된 것처럼" 새어 보인다(리뷰 #5) — 그때는 1/1(디바운스 없음)로 고정한다.
    thr = min(max(float(ctx.params.get("thr") or 0.15), 0.05), 0.60)
    if can_debounce:
        window = min(max(int(ctx.params.get("window") or 5), 1), 9)
        need = min(max(int(ctx.params.get("need") or 3), 1), 9)
    else:
        window = need = 1
    # ⚠️ 뱅크에 문장이 없는 GT 클래스는 `_rule_arrays` 가 -1 로 남긴다 — 그대로 두면 그 프레임이
    #    **어느 F1 행에도 안 나타나 조용히 사라진다**(sitej_subway 실측: `intrustion` 375장 =
    #    전체의 13.7%. 뱅크 vGEN/vOPT 는 4클래스 × 500문장뿐이라 구조적으로 판정 불가).
    #    이 repo 의 반복 버그 형태(크래시 대신 조용한 오답)라 **모수에서 빠졌다는 사실을 박는다.**
    n_unscored = int((gt < 0).sum())
    if n_unscored:
        inputs.view("unscored", types.Warning(
            label=f"⚠️ GT 가 있으나 **뱅크에 해당 클래스 문장이 없어 채점에서 빠진 프레임 "
                  f"{n_unscored:,}장** ({n_unscored / max(len(gt), 1):.1%})이 있습니다. "
                  f"아래 지표의 모수는 나머지 {len(gt) - n_unscored:,}장입니다 — "
                  f"채점 가능한 클래스: {', '.join(ev)}. 그 클래스를 보려면 해당 문장을 가진 "
                  "뱅크가 필요합니다."))

    defaults_ok = thr == 0.15 and (not can_debounce or (window, need) == (5, 3))
    if not defaults_ok:
        inputs.view("diff", types.Warning(
            label=f"제품 기본값(thr=0.15, 5중3)과 다릅니다 — 지금: thr={thr:.2f}, "
                  f"{window}중{need}. 화면 재현일 뿐 배포 상수를 바꾸지 않습니다"))

    # ⚠️ 구조적 경고(2026-09-21 추가 지시) — M 이 이 코호트 창 길이 중앙값을 넘으면 결과가
    # 판정규칙이 아니라 창 길이 자체를 측정한다(예: 9중5 는 780개 이벤트창 중 484개인
    # 3프레임 창에서 M=5 에 원리적으로 도달 못 한다). **막지 않는다 — 알림만**.
    if can_debounce and group_sizes is not None and len(group_sizes):
        median_len = float(np.median(group_sizes))
        if need > median_len:
            n_at_median = int((group_sizes == median_len).sum())
            frac_unreach = float((group_sizes < need).mean())
            inputs.view("winlen", types.Warning(label=(
                f"⚠️ 이 코호트의 {group_label} 길이 중앙값은 {median_len:.0f}프레임입니다 "
                f"({len(group_sizes):,}개 중 {n_at_median:,}개가 {median_len:.0f}프레임). "
                f"M={need} 는 {group_label} 의 {frac_unreach:.0%} 에서 도달 자체가 불가능해, "
                "결과가 판정규칙이 아니라 창 길이를 측정합니다")))

    # ── 실시간 지표 — resolve_input 안에서 직접 계산해야 "끌면 바뀐다"가 성립한다
    # (기존 `_score_texts` 는 execute() 에서 도는데(:902) 그러면 슬라이더가 죽은 채로 보인다).
    pred, fp_normal, rows = _rule_metrics(iou, gt, classes, ev, thr, window, need,
                                          group_id, order_key)
    debounce_note = "" if can_debounce else " (디바운스 미적용)"
    head = f"**thr={thr:.2f} · {window}중{need}{debounce_note} · {len(gt):,}장** — {source_label}"
    neg = classes[0]                        # §표준화 — 라벨 문구는 실제 음성 클래스 이름을 쓴다
    body = (f"오탐(fp_{neg}): GT={neg} 중 오판 비율 {fp_normal:.4f}" if fp_normal is not None
           else f"오탐(fp_{neg}): GT={neg} 프레임이 0장이라 계산할 수 없습니다")
    summary = head + "\n\n" + body + "\n\n" + _f1_table_md(rows, n_unscored, len(gt))
    inputs.view("live", types.Notice(label=summary))

    # ── 혼동행렬(2026-09-21 지시 변경 — N×N 허용, 이전 "그리지 마라" 판단 철회됨) ──
    # 행=GT 전부(코호트 밖·결측 포함), 열=예측. `n_view_total` 은 결측 제외 전 전체(§검증②).
    cm_md = _confusion_matrix_md(gt_label, pred, classes, missing_breakdown,
                                 n_view_total=len(gt_label) + n_missing)
    inputs.view("confmat", types.Notice(label=cm_md))
    inputs.bool("save_view", default=False, label="결과를 뷰로 저장 (판정≠GT 프레임)",
                description=f"켜면 실행 시 `thr{int(round(thr * 100)):03d}_w{window}m{need}_<태그>` "
                            "이름으로 저장합니다 — 동일 파라미터 재실행은 덮어씁니다"
                            "(좌석 공유 주의, Task 2)")
    # (2026-09-22) "이 컨트롤은 분포 IoU 전용" 안내도 함께 제거 — 규칙 선택 라디오가 위에
    #    이미 있어 무엇을 조절 중인지 화면에 드러난다. 같은 말이 세 곳에서 반복되던 것을 줄인다.
    return thr, window, need


def _render_topk_form(inputs, ctx, gt, classes, ev, ord_v, ord_c, source_label, gt_label,
                      n_missing, missing_breakdown):
    """k 슬라이더 + 실시간 지표 — top-k 전용(요청③). `_render_rule_form`(dist_iou)과 형태는
    맞추되(head/summary/save_view 구조 동일) **디바운스는 없다**(설계 5) — 제품 디바운스는
    "클래스별 발화(IoU<thr)" 위에 정의된 장치라 top-k 다수결 위에 얹는 의미가 정의된 적이
    없다(임의로 만들지 않는다, 사용자 지시).
    """
    k_max = min(K_MAX, ord_c.shape[1])
    inputs.int("k", default=min(10, k_max), min=1, max=k_max, required=True,
               label="k (top-k)",
               description="상위 k 문장으로 판정 — 제품 기본값 10",
               view=types.SliderView(view_multiple_of=1))
    inputs.view("nodebounce_topk", types.Notice(
        label="디바운스는 분포 IoU 의 클래스별 발화 위에 정의된 장치라 top-k 에는 적용하지 "
              "않습니다"))

    # ⚠️ `.get(k, default)` 는 값이 명시적 None 일 때 그대로 통과한다(`_render_rule_form` 의
    # 같은 함정, :2009-2014 주석 참조) — `or` 로 받고 직접 clamp 한다.
    k = min(max(int(ctx.params.get("k") or 10), 1), k_max)
    if k != 10:
        inputs.view("diff_topk", types.Warning(
            label=f"제품 기본값(k=10)과 다릅니다 — 지금: k={k}. 화면 재현일 뿐 배포 상수를 "
                  "바꾸지 않습니다"))

    # ⚠️ 여기 있던 "top-k 는 제품 판정규칙이 아닙니다" 경고 배너는 2026-09-22 사용자 지시로
    #    제거했다. **사실 자체는 유효하다** — 제품 규칙은 분포 IoU 이고 두 규칙은 29버전 비교에서
    #    순위 상관 ρ≈0 이라 top-k 숫자를 제품 성능으로 읽으면 안 된다(`project_pe_inference_dist_iou`).
    #    화면 문구만 뺀 것이니, 되살릴 때 이 주석을 근거로 쓰면 된다.

    n_unscored = int((gt < 0).sum())        # dist_iou 와 같은 판정(§C.3) — topk 엔 사전 경고가
    if n_unscored:                          # 없었으니 여기서 처음이자 유일하게 알린다
        inputs.view("unscored_topk", types.Warning(
            label=f"⚠️ GT 가 있으나 **뱅크에 해당 클래스 문장이 없어 채점에서 빠진 프레임 "
                  f"{n_unscored:,}장** ({n_unscored / max(len(gt), 1):.1%})이 있습니다. "
                  f"아래 지표의 모수는 나머지 {len(gt) - n_unscored:,}장입니다 — "
                  f"채점 가능한 클래스: {', '.join(ev)}."))

    pred, fp_normal, rows = _topk_metrics(ord_v, ord_c, gt, classes, ev, k)
    head = f"**k={k} · {len(gt):,}장** — {source_label}"
    neg = classes[0]                        # §표준화 — 라벨 문구는 실제 음성 클래스 이름을 쓴다
    body = (f"오탐(fp_{neg}): GT={neg} 중 오판 비율 {fp_normal:.4f}" if fp_normal is not None
           else f"오탐(fp_{neg}): GT={neg} 프레임이 0장이라 계산할 수 없습니다")
    summary = head + "\n\n" + body + "\n\n" + _f1_table_md(rows, n_unscored, len(gt))
    inputs.view("live_topk", types.Notice(label=summary))

    # ── 혼동행렬(2026-09-21 지시 변경) — dist_iou 와 같은 렌더 함수를 공유한다(드리프트 방지).
    cm_md = _confusion_matrix_md(gt_label, pred, classes, missing_breakdown,
                                 n_view_total=len(gt_label) + n_missing)
    inputs.view("confmat_topk", types.Notice(label=cm_md))
    inputs.bool("save_view", default=False, label="결과를 뷰로 저장 (판정≠GT 프레임)",
                description=f"켜면 실행 시 `topk_k{k}_<태그>` 이름으로 저장합니다 — 동일 "
                            "파라미터 재실행은 덮어씁니다(좌석 공유 주의, Task 2)")
    return k


class ExplainRule(foo.Operator):
    """판정규칙(분포 IoU 또는 top-k, 요청③)의 파라미터를 폼에서 조절하면 지표가 즉시 갱신된다.

    dist_iou 는 `wave_iou_*` 필드 코호트(`sourcei`/`sourcei-OPT`, 그 외 `stage_wave` 를 돌린
    코호트)면 그 필드를, 아니면 온디맨드로 계산한다(§C.1). top-k 는 순위표가 필요해 필드
    경로가 아예 없다 — **항상 온디맨드**다(wave_iou 필드 보유 여부와 무관).
    """

    @property
    def config(self):
        return foo.OperatorConfig(
            name="explain_rule",
            label="⑤ 판정규칙 실시간 조절 — thr·디바운스",
            dynamic=True,
            icon="tune",
        )

    def resolve_placement(self, ctx):
        # `ProbePrompt`(:832-837 근처)와 같은 무게이트 형태 — `wave_iou_*` 존재는 스키마를
        # 뒤져야 알 수 있어 `resolve_placement`(raise 금지, :85-88)에서 하기엔 무겁고 위험하다.
        # 없으면 버튼은 뜨되 resolve_input 이 사유를 한 줄로 보여준다(Task1 Step1).
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(label="⑤ 판정규칙 실시간 조절 — thr·디바운스", icon="tune", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()

        # ── ①③ 규칙 선택 — dist_iou(기본, 이하 회귀 0) / topk(항상 온디맨드) ──
        radio = types.RadioGroup()
        # 라벨에 "제품 판정규칙"/"참고용 — 제품 규칙 아님" 수식어를 달지 않는다
        # (2026-09-22 사용자 지시). 규칙 이름만 둔다. "다수결"도 같은 지시로 뺐다.
        # ⚠️ 표시 순서는 topk 가 먼저지만 **기본 선택은 dist_iou 그대로다**(사용자는 순서만
        #    요청했다). 기본을 topk 로 바꾸면 모달을 열 때마다 온디맨드 계산이 강제되고
        #    (필드에서 k 를 되돌릴 수 없으므로) sourcei 기준 14~19초가 매번 든다.
        radio.add_choice("topk", label="top-k")
        radio.add_choice("dist_iou", label="분포 IoU")
        # ⚠️ 기본 선택 = topk (2026-09-22 사용자 지시). 비용을 알고 고른 것이다 — topk 는
        #    사전계산 `probe_votes` 가 k=10 집계값뿐이라 필드에서 k 를 되돌릴 수 없어
        #    **항상 온디맨드**이고, sourcei 기준 첫 계산 14~19초가 모달을 열 때마다 든다
        #    (캐시 히트면 즉시). dist_iou 로 바꾸면 필드 경로라 비용 0 이다.
        inputs.enum("rule", radio.values(), default="topk", required=True,
                    label="판정규칙", view=radio)
        rule = ctx.params.get("rule") or "topk"

        if rule == "topk":
            # top-k 는 순위표가 필요해 **항상** 온디맨드다(설계②) — wave_iou 필드가 있는
            # sourcei 에서 골라도 필드 경로로 새지 않는다. 기존 온디맨드 흐름(뱅크 선택 →
            # 즉시 계산 → 캐시, 2026-09-21② 이후 클릭 게이트 없음)을 그대로 탄다(사용자 지시).
            return self._resolve_input_ondemand(
                ctx, inputs, rule="topk",
                header_note="ℹ️ top-k 규칙은 순위표가 필요해 온디맨드로만 계산합니다 "
                           "(이 데이터셋의 `wave_iou` 필드 보유 여부와 무관합니다).")

        # ── 이하 dist_iou — 기존 로직 그대로 (회귀 0) ──
        # ⚠️ probe 캐시 안내(`_probe_cache_missing_notice`)를 재사용하지 않는다 — 이 오퍼레이터는
        # probe 캐시가 아니라 `stage_wave` 산출물을 읽으므로 "probecache 를 실행하라"는 처방은
        # 틀린 처방이다(적대적 리뷰 #4). 대상 데이터셋/뷰 자체도 4단으로 해석해야 한다(후속
        # 지시 — 문장 데이터셋에서 열면 "stage_wave 를 돌려라"가 또 다른 틀린 처방이 된다).
        rdataset, rview, rnote = _resolve_wave_target(ctx)
        if rdataset is None:
            # 3·4단계 실패 — 온디맨드 계산이 가능한지 먼저 본다(sitej_subway 실사용 계기,
            # 2026-09-21 사양 확대). 필드가 없다고 바로 포기하지 않는다.
            return self._resolve_input_ondemand(ctx, inputs, rule="dist_iou", header_note=rnote)
        if rnote:
            inputs.view("resolved", types.Notice(label=rnote))          # 2단계 — 전환 사실 고지

        tags = _wave_iou_tags(rview.get_field_schema())
        if not tags:
            # 이론상 도달 불가 — `_resolve_wave_target` 이 이미 wave_iou 보유를 확인했다.
            # 그래도 방어적으로 남긴다(폼은 절대 죽지 않게).
            inputs.view("none2", types.Error(label="`wave_iou_*` 필드를 다시 찾지 못했습니다 — "
                                                    "새로고침 후 다시 시도하세요"))
            return types.Property(inputs)

        # ⚠️ 태그 드롭다운은 **아래에서(필드 경로로 실제로 렌더링을 마칠 때)만** 등록한다 —
        # 2026-09-21 지시① 자동전환이 걸리면 이 컨트롤은 아무 효과가 없는 죽은 UI가 되므로
        # (전환된 뒤 계산은 `ondemand_bank` 를 쓰지 `tag` 를 안 본다) 애초에 안 보여준다.
        # `tag` 값 자체는 지금 읽어둔다 — 판정에 필요하다(등록 시점과 무관, 폼 스키마는
        # `inputs.enum` 호출로만 만들어지고 여기선 아직 안 부른다).
        tag = ctx.params.get("tag") or tags[0]

        def _register_tag_dropdown():
            dd = types.DropdownView()
            for t in tags:
                dd.add_choice(t, label=_wave_tag_label(rdataset, t))
            inputs.enum("tag", tags, default=tags[0], required=True, label="뱅크 태그", view=dd)

        try:
            reliable, rule_arrays, stale, reasons = _wave_field_reliable(rdataset, tag, rview)
        except Exception as e:                      # noqa: BLE001 — 폼은 절대 죽지 않게 (:505 선례)
            _register_tag_dropdown()                 # 실패해도 다른 태그를 골라볼 수 있게 남긴다
            inputs.view("nofield", types.Error(label=f"{type(e).__name__}: {e}"))
            return types.Property(inputs, view=types.View(label="판정규칙 조절"))

        (ev, iou, gt, classes, group_id, order_key, note, group_sizes, group_label,
         gt_label, n_missing, missing_breakdown, _sample_ids) = rule_arrays

        # ⚠️ 2026-09-21 지시① — 필드가 못 믿을 상태(결측 또는 스테일)면 **조용히 넘어가지
        # 않고** 온디맨드로 전환한다. 온디맨드가 애초에 불가능하면(임베딩 필드·뱅크 npz 없음)
        # 전환하지 않고 기존 필드 결과를 그대로 보여주되 그 사실을 덧붙인다.
        fallback_notice = None
        if not reliable:
            feas = _ondemand_feasibility(rdataset, rview)
            if feas["ok"]:
                switch_note = (
                    f"ℹ️ wave_iou 필드가 현재 데이터와 어긋나 있어({' / '.join(reasons)}) "
                    "**온디맨드로 다시 계산했습니다** — 지금 보이는 숫자는 현재 상태 기준입니다.")
                return self._resolve_input_ondemand(
                    ctx, inputs, rule="dist_iou", header_note=switch_note,
                    target_ds=rdataset, target_view=rview,
                    preferred_bank_version=_bank_version_for_tag(tag),
                    always_show_reason=True)
            fallback_notice = f"⚠️ 온디맨드로도 못 고칩니다: {feas['reason']}"

        _register_tag_dropdown()
        if stale:
            inputs.view("stale", types.Warning(label=stale))
        if fallback_notice:
            inputs.view("ondemand_unavailable", types.Warning(label=fallback_notice))

        source_label = f"📊 출처: `wave_iou` 필드(배치 `stage_wave` 산출, 태그 `{tag}`)"
        _render_rule_form(inputs, ctx, ev, iou, gt, classes, group_id, order_key, note,
                          group_sizes, group_label, source_label, gt_label, n_missing,
                          missing_breakdown)
        return types.Property(inputs, view=types.View(label="판정규칙 조절"))

    def _resolve_input_ondemand(self, ctx, inputs, rule, header_note,
                                target_ds=None, target_view=None,
                                preferred_bank_version=None, always_show_reason=False):
        """온디맨드 계산 UX 공용 — dist_iou(필드 없는 코호트, 또는 있어도 못 믿을 상태라
        전환된 경우) 와 top-k(요청③ — 필드 경로 자체가 없어 **항상** 이 경로) 가 공유한다.

        **2026-09-21 재지시로 `compute_now` 체크박스를 없앴다** — 폼이 열리면(또는 무관한
        파라미터가 바뀌어 `resolve_input` 이 재평가돼도) 그 즉시 계산한다("추가 클릭 없이"가
        사용자 요구). `_rule_arrays_ondemand` 는 (데이터셋·뷰·뱅크) 가 안 바뀌면 캐시를 그대로
        돌려주므로, thr/k 슬라이더 등 무관한 값이 바뀌어 이 함수가 다시 불려도 비용이 없다 —
        그래서 소요시간도 그때는 0에 가깝게 찍힌다(그 자체가 "캐시됐다"는 증거). 소요시간은
        **예상치가 아니라 이 호출을 감싼 실측**이다(사용자 지시: "예상치가 아니라 실측").

        `target_ds`/`target_view` 는 계산 대상을 명시로 고정한다(생략하면 `ctx.dataset`/
        `ctx.view` — 기존 stage3·4·top-k 호출부와 동일, 회귀 0). 2026-09-21 지시① 자동전환이
        이 값을 넘긴다 — 문장 데이터셋에서 프레임 데이터셋으로 2단계 전환된 세션에서
        자동전환이 걸리면 온디맨드도 **프레임 쪽**에서 계산해야 한다(`ctx.dataset` 을 쓰면
        임베딩이 없는 문장 데이터셋을 본다).

        `preferred_bank_version` 은 드롭다운 기본값 힌트 — 자동전환은 "지금 못 믿는 바로 그
        태그"에 대응하는 뱅크를 기본으로 잡는 게 임의의 첫 후보보다 사용자 의도에 가깝다.
        후보 목록에 없으면 조용히 무시하고 `banks[0]` 로 돌아간다.

        `always_show_reason` 이 False(기본, 기존 두 호출부와 동일)면 `header_note` 는 top-k
        에서만 보인다(dist_iou 의 3·4단계 사유는 실패시 error 문구 안에 접혀 들어갈 뿐 정상
        경로에선 안 보였다 — 기존 동작, 회귀 0). 자동전환 안내는 **정상 경로에서 반드시 보여야
        하는 사실 고지**라(요청①: "조용히 바꾸지 마라") 호출부가 True 로 켠다.
        """
        target_ds = target_ds if target_ds is not None else ctx.dataset
        target_view = target_view if target_view is not None else (
            ctx.view if ctx.view is not None else target_ds.view())
        feas = _ondemand_feasibility(target_ds, target_view)
        if not feas["ok"]:
            prefix = f"{header_note}\n\n" if header_note else ""
            inputs.view("none", types.Error(
                label=f"{prefix}온디맨드 계산이 불가능합니다: {feas['reason']}"))
            return types.Property(inputs)

        if header_note and (always_show_reason or rule == "topk"):
            inputs.view("ondemand_reason", types.Notice(label=header_note))

        banks = feas["banks"]
        bank_names = [v for v, _t in banks]
        default_bank = (preferred_bank_version if preferred_bank_version in bank_names
                        else banks[0][0])
        dd = types.DropdownView()
        for version, tag in banks:
            dd.add_choice(version, label=f"{tag} ({version})")
        inputs.enum("ondemand_bank", bank_names, default=default_bank, required=True,
                    label="뱅크 버전(온디맨드 계산용)", view=dd)
        bank_version = ctx.params.get("ondemand_bank") or default_bank

        n_frames = target_view.count()
        try:
            t0 = time.perf_counter()
            (ev, iou, gt, classes, group_id, order_key, note, group_sizes, group_label,
             gt_label, n_missing, missing_breakdown, _sample_ids) = _rule_arrays_ondemand(
                target_ds, target_view, feas["embed_field"], bank_version)
            elapsed = time.perf_counter() - t0
        except Exception as e:                       # noqa: BLE001 — 폼은 절대 죽지 않게 (:505 선례)
            inputs.view("ondemand_err", types.Error(label=f"{type(e).__name__}: {e}"))
            return types.Property(inputs, view=types.View(label="판정규칙 조절 (온디맨드)"))

        key = _ondemand_cache_key(target_ds, bank_version, target_view)
        ord_v, ord_c, n_sentences = _TOPK_CACHE[key]
        sent_note = f"{n_sentences:,}문장" if n_sentences else "문장 수 확인 실패"
        cost_note = f"⚡ 온디맨드 계산 {elapsed:.1f}초 ({n_frames:,}장 × {sent_note})"
        if elapsed > 20:
            # ⚠️ 사후 고지(요청②) — 막지 않는다. 계산은 이미 끝났고, 공유 좌석의 다른 세션이
            # 그 시간만큼 멈췄다는 사실만 알린다.
            cost_note += f"\n\n⚠️ 계산에 {elapsed:.1f}초 걸렸습니다 — 뷰를 좁히면 빨라집니다"
        inputs.view("cost", types.Notice(label=cost_note))

        if rule == "topk":
            source_label = (f"⚡ 출처: 온디맨드 계산 (뱅크 `{bank_version}`, top-k 순위표 "
                           f"K_MAX={ord_c.shape[1]})")
            _render_topk_form(inputs, ctx, gt, classes, ev, ord_v, ord_c, source_label,
                              gt_label, n_missing, missing_breakdown)
        else:
            source_label = f"⚡ 출처: 온디맨드 계산 (뱅크 `{bank_version}`, bins=80)"
            _render_rule_form(inputs, ctx, ev, iou, gt, classes, group_id, order_key, note,
                              group_sizes, group_label, source_label, gt_label, n_missing,
                              missing_breakdown)
        return types.Property(inputs, view=types.View(label="판정규칙 조절 (온디맨드)"))

    def execute(self, ctx):
        rule = ctx.params.get("rule") or "topk"   # ⚠️ resolve_input 과 같은 기본값이어야 한다
        if rule == "topk":
            return self._execute_topk(ctx)

        # ── 이하 dist_iou — 기존 계산은 완전히 그대로(회귀 0). 필드/온디맨드 판정만 바꿨다:
        # 예전엔 `ondemand_bank` 파라미터 **존재**로 갈랐는데, 같은 모달에서 `rule` 을
        # topk → dist_iou 로 되돌리면 topk 가 남긴 `ondemand_bank` 가 ctx.params 에 남아있을
        # 수 있어(요청③이 만든 새 위험) 필드 경로인데 온디맨드로 잘못 샐 수 있다.
        # `_resolve_wave_target` 은 순수 스키마 조회라 resolve_input 때와 항상 같은 값을
        # 내므로(부작용 없음), 파라미터 존재가 아니라 **이걸로** 갈라도 순수 dist_iou 세션의
        # 결과는 바이트 단위로 동일하다.
        rdataset, rview, _rnote = _resolve_wave_target(ctx)
        if rdataset is None:
            target_ds = ctx.dataset
            target_view = ctx.view if ctx.view is not None else target_ds.view()
            bank_version = ctx.params["ondemand_bank"]
            feas = _ondemand_feasibility(target_ds, target_view)
            if not feas["ok"]:
                raise ValueError(f"온디맨드 계산이 불가능합니다: {feas['reason']}")
            (ev, iou, gt, classes, group_id, order_key, _note, _group_sizes, _group_label,
             gt_label, n_missing, missing_breakdown, sample_ids) = _rule_arrays_ondemand(
                target_ds, target_view, feas["embed_field"], bank_version)
            rdataset, rview = target_ds, target_view
            tag_label, bank_display = bank_version, f"온디맨드:{bank_version}"
        else:
            # ⚠️ 2026-09-21 지시① — 필드가 있어도(`rdataset is not None`) resolve_input 과
            # **반드시 같은 결정**을 다시 내려야 한다(`_wave_field_reliable` 한 곳에서만 판정 —
            # 두 벌로 쪼개면 미리보기와 실행 결과가 갈린다). `tag` 기본값도 resolve_input 과
            # 똑같이 유도한다 — 자동전환된 세션은 애초에 `tag` 폼 필드를 렌더링하지 않았으므로
            # `ctx.params["tag"]` 가 없을 수 있다(그래서 `.get` — 예전엔 `[...]` 라 KeyError 였다).
            tags = _wave_iou_tags(rview.get_field_schema())
            tag = ctx.params.get("tag") or (tags[0] if tags else None)
            if tag is None:
                raise ValueError("`wave_iou_*` 필드를 다시 찾지 못했습니다 — 새로고침 후 다시 시도하세요")
            reliable, rule_arrays, _stale, _reasons = _wave_field_reliable(rdataset, tag, rview)
            (ev, iou, gt, classes, group_id, order_key, _note, _group_sizes, _group_label,
             gt_label, n_missing, missing_breakdown, sample_ids) = rule_arrays
            tag_label, bank_display = tag, _wave_tag_label(rdataset, tag)

            if not reliable:
                # resolve_input 과 같은 두 갈래: 온디맨드가 가능하면 전환, 아니면 방금 구한
                # 필드 결과(`rule_arrays`)를 그대로 쓴다(폴백 — "온디맨드로도 못 고칩니다").
                feas = _ondemand_feasibility(rdataset, rview)
                if feas["ok"]:
                    bank_version = (ctx.params.get("ondemand_bank")
                                   or _bank_version_for_tag(tag)
                                   or feas["banks"][0][0])
                    (ev, iou, gt, classes, group_id, order_key, _note, _group_sizes, _group_label,
                     gt_label, n_missing, missing_breakdown, sample_ids) = _rule_arrays_ondemand(
                        rdataset, rview, feas["embed_field"], bank_version)
                    tag_label, bank_display = bank_version, f"온디맨드:{bank_version}"

        can_debounce = group_id is not None
        # 값 읽기·클램프는 resolve_input 과 동일해야 한다(둘이 갈리면 미리보기와 실행 결과가
        # 달라진다) — 리뷰 #5·#6·#7 이 동일하게 적용된다.
        thr = min(max(float(ctx.params.get("thr") or 0.15), 0.05), 0.60)
        if can_debounce:
            window = min(max(int(ctx.params.get("window") or 5), 1), 9)
            need = min(max(int(ctx.params.get("need") or 3), 1), 9)
        else:
            window = need = 1
        pred, fp_normal, rows = _rule_metrics(iou, gt, classes, ev, thr, window, need,
                                              group_id, order_key)

        # ⚠️ 저장 뷰는 **해석된(resolved) 데이터셋**에 저장한다 — `ctx.dataset` 에 저장하면
        # 2단계(문장→프레임 전환)·온디맨드(같은 데이터셋이라 무해하나 일관성 위해 동일 처리)
        # 양쪽에서 엉뚱한 데이터셋에 뷰가 생긴다(사용자 지시로 명시 확인).
        view_name = "(저장 안 함)"
        if ctx.params.get("save_view"):
            # ⚠️ 2026-09-21 정합성 수정 — 예전엔 `rview.values("id")`(결측 제외 **전** 전체 N)
            # 를 `pred != gt`(결측 제외 **후** N_kept)와 그냥 zip 했다. `n_missing>0` 이면
            # zip 이 짧은 쪽에서 끊겨 id 와 예측이 서로 다른 프레임을 가리켰다(결측이 뷰 맨
            # 끝에 몰려있지 않는 한 어긋난다) — 그 상태로 `thr030_w5m3_<태그>` 이름의 **공유**
            # 저장 뷰가 만들어지면 아무도 틀렸다는 걸 알 방법이 없다. `sample_ids` 는 `gt`/
            # `pred` 와 **같은 호출·같은 keep 마스크**에서 나온 배열이라 길이·순서가 항상
            # 맞는다(`_rule_arrays`/`_rule_arrays_ondemand` 문서 참조).
            assert len(sample_ids) == len(pred) == len(gt), (
                len(sample_ids), len(pred), len(gt))               # 불변식 — 어긋나면 여기서 바로 죽는다
            nm = _rule_view_name(tag_label, thr, window, need)
            mism_ids = [sid for sid, keep in zip(sample_ids, (pred != gt).tolist()) if keep]
            mism = rview.select(mism_ids)
            if nm in rdataset.list_saved_views():
                rdataset.delete_saved_view(nm)
            rdataset.save_view(nm, mism, description=(
                f"ExplainRule thr={thr} {window}중{need} bank={bank_display} — "
                f"판정≠GT {mism.count():,}장"))
            view_name = nm

        confusion_md = _confusion_matrix_md(gt_label, pred, classes, missing_breakdown,
                                            n_view_total=len(gt_label) + n_missing)
        return {"tag": tag_label, "bank": bank_display, "n": int(len(gt)),
               "rule": "dist_iou", "thr": thr, "window": window, "need": need,
               "fp_normal": fp_normal, "neg_class": classes[0], "class_f1": rows,
               "confusion_md": confusion_md, "view_saved": view_name}

    def _execute_topk(self, ctx):
        """top-k 실행 — 항상 온디맨드(설계②). 값 읽기·클램프는 `_render_topk_form` 과
        동일해야 한다(미리보기와 실행 결과가 갈리면 안 된다, dist_iou 쪽 리뷰 #5·#6·#7 과 같은
        이유)."""
        target_ds = ctx.dataset
        target_view = ctx.view if ctx.view is not None else target_ds.view()
        bank_version = ctx.params.get("ondemand_bank")
        if not bank_version:
            # 2026-09-21② `compute_now` 체크박스 제거 이후 이 분기는 사실상 도달 불가에
            # 가깝다(폼이 열리자마자 `ondemand_bank` 기본값이 채워진다) — 그래도 방어적으로
            # 남긴다(폼이 아니라 API로 직접 execute 를 호출하는 경로 등 예외적 상황 대비).
            raise ValueError("뱅크 버전을 고르세요 — 온디맨드 계산이 준비되지 않았습니다")
        feas = _ondemand_feasibility(target_ds, target_view)
        if not feas["ok"]:
            raise ValueError(f"온디맨드 계산이 불가능합니다: {feas['reason']}")
        (ev, _iou, gt, classes, _group_id, _order_key, _note, _group_sizes, _group_label,
         gt_label, n_missing, missing_breakdown, sample_ids) = _rule_arrays_ondemand(
            target_ds, target_view, feas["embed_field"], bank_version)
        key = _ondemand_cache_key(target_ds, bank_version, target_view)
        ord_v, ord_c, _n_sentences = _TOPK_CACHE[key]     # 문장 수는 미리보기 안내 전용, 실행 결과엔 안 쓴다

        k_max = min(K_MAX, ord_c.shape[1])
        k = min(max(int(ctx.params.get("k") or 10), 1), k_max)
        pred, fp_normal, rows = _topk_metrics(ord_v, ord_c, gt, classes, ev, k)

        view_name = "(저장 안 함)"
        if ctx.params.get("save_view"):
            # 2026-09-21 정합성 수정 — dist_iou 의 execute() 와 같은 이유(위 주석 참조):
            # `target_view.values("id")` 는 결측 제외 전 전체 N 이라 `pred`/`gt`(제외 후
            # N_kept)와 그냥 zip 하면 n_missing>0 일 때 어긋난다. `sample_ids` 로 대체.
            assert len(sample_ids) == len(pred) == len(gt), (
                len(sample_ids), len(pred), len(gt))
            nm = _topk_view_name(bank_version, k)
            mism_ids = [sid for sid, keep in zip(sample_ids, (pred != gt).tolist())
                        if keep]
            mism = target_view.select(mism_ids)
            if nm in target_ds.list_saved_views():
                target_ds.delete_saved_view(nm)
            target_ds.save_view(nm, mism, description=(
                f"ExplainRule topk k={k} bank={bank_version} — 판정≠GT {mism.count():,}장"))
            view_name = nm

        confusion_md = _confusion_matrix_md(gt_label, pred, classes, missing_breakdown,
                                            n_view_total=len(gt_label) + n_missing)
        return {"tag": bank_version, "bank": f"온디맨드:{bank_version}", "n": int(len(gt)),
               "rule": "topk", "k": k, "fp_normal": fp_normal, "neg_class": classes[0],
               "class_f1": rows, "confusion_md": confusion_md, "view_saved": view_name}

    def resolve_output(self, ctx):
        # `ctx.results` = execute() 가 돌려준 dict(FiftyOne `ExecutionContext.results` 계약) —
        # 음성 클래스 이름을 라벨 문구에 동적으로 넣으려면(§표준화 지시) 여기서 읽어야 한다.
        # 실패해도 죽지 않게 기본값 "normal" 로 fallback(이전 정적 문구와 동일하게 낮춘다).
        neg = (ctx.results or {}).get("neg_class") or "normal"
        cm_md = (ctx.results or {}).get("confusion_md") or "(혼동행렬 없음)"

        outputs = types.Object()
        outputs.str("tag", label="뱅크 태그")
        outputs.str("bank", label="뱅크 버전")
        outputs.int("n", label="평가 프레임")
        outputs.str("rule", label="판정규칙")                     # 신규 — dist_iou/topk 구분(요청③⑦)
        outputs.float("thr", label="thr (dist_iou)")
        outputs.int("window", label="디바운스 창 W (dist_iou)")
        outputs.int("need", label="필요 발화 M (dist_iou)")
        outputs.int("k", label="k (top-k)")                       # 신규
        outputs.float("fp_normal", label=f"오탐 (GT={neg} 중 오판 비율)")
        tbl = types.TableView()                                   # 2026-09-21: tp/fp/fn 원시 카운트 노출
        tbl.add_column("cls", label="클래스")
        tbl.add_column("tp", label="TP")
        tbl.add_column("fp", label="FP")
        tbl.add_column("fn", label="FN")
        tbl.add_column("precision", label="정밀도")
        tbl.add_column("recall", label="재현율")
        tbl.add_column("f1", label="F1")
        tbl.add_column("support", label="GT 표본 수")
        outputs.list("class_f1", types.Object(),
                     label=f"클래스별 TP/FP/FN (GT 존재 클래스만, {neg}(음성) 1-vs-rest 포함)",
                     view=tbl)
        outputs.view("confmat", types.Notice(label=cm_md))         # 2026-09-21: 혼동행렬(N×N)
        outputs.str("view_saved", label="저장된 뷰")
        outputs.view("hint", types.Notice(
            label="이 지표는 위 「판정규칙」 값 전용입니다(dist_iou 또는 top-k). 프레임 단위 "
                  "macro F1 은 의도적으로 표시하지 않습니다 — 디바운스는 재현율을 내주는 장치라 "
                  # (2026-09-22) "top-k 는 제품 판정규칙이 아닙니다" 문구 제거 — 사용자 지시.
                  "프레임 F1 만 보면 손해로 보입니다(계획서 §C.3). "
                  f"{neg}(음성) 행은 tp/fn/정밀도/재현율/F1 이 비어 있습니다(1-vs-rest 로 정의는 "
                  "되지만 이 표의 목적과 무관) — fp 칸만 「GT=" + neg + " 인데 이벤트로 오판한 "
                  "절대 수」입니다(= 위 「오탐」 비율의 분자)."))
        return types.Property(outputs, view=types.View(label="판정규칙 조절 결과"))


def register(p):
    p.register(ProbePrompt)
    p.register(ExportBankVersion)
    p.register(GeneratePrompts)
    p.register(ExplainFrames)
    p.register(ExplainRule)


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

    for cls in (ProbePrompt, ExportBankVersion, GeneratePrompts, ExplainRule):
        cls().resolve_placement(_NoDs())          # raise 하면 여기서 테스트가 깨진다
    assert ExportBankVersion().resolve_placement(_NoDs()) is None
    # ⚠️ 2026-09-21 Task 3: GeneratePrompts 는 `_probe_tags_safe` 게이트를 지웠다 — 이제
    #    ProbePrompt/ExplainRule 과 같은 무게이트 형태라 이 assert 방향이 뒤집혔다(옛 assert 는
    #    `is None` 이었다. 그대로 뒀으면 이 self-check 가 여기서 깨져 아래 나머지 테스트가
    #    전부 미실행이 됐을 것).
    assert GeneratePrompts().resolve_placement(_NoDs()) is not None
    assert ProbePrompt().resolve_placement(_NoDs()) is not None   # 얘는 조건 없이 항상 뜬다
    assert ExplainRule().resolve_placement(_NoDs()) is not None   # 얘도 무게이트(Task1 Step1)
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
            if isinstance(f, (list, tuple)):          # 실제 계약과 동일하게 필드명 리스트도 받는다
                return [self.values(x) for x in f]
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

    # ── ⑤ _rule_predict — thr·디바운스(count_only) (계획 §D Task1 Step7, 2026-09-21 사양 확정) ──
    # T1: window=1,need=1 은 (아무 seq 를 줘도) 디바운스 없음과 완전히 동일 — **수학적으로**
    #     window=1 에서 트레일링 창은 "자기 자신 한 칸"뿐이라 count_only 도 raw fired 와
    #     항상 같다(코드가 그 경우를 건너뛰어서가 아니다 — 적대적 리뷰 #2 이후로는 seq 가
    #     있으면 window 값과 무관하게 항상 루프를 탄다. 아래로 진입 조건 자체가 바뀌었다).
    I1 = np.array([[0.05, 0.30], [0.40, 0.02], [0.50, 0.60]], dtype="float32")
    weird_seq = (np.array(["g0", "g1", "g0"]), np.array([9.0, 1.0, 0.0]))
    p_plain = _rule_predict(I1, 0.15)
    p_w1m1 = _rule_predict(I1, 0.15, seq=weird_seq, window=1, need=1)
    assert p_plain.tolist() == [0, 1, -1], p_plain           # 0=col0 발화, 1=col1 발화, 2=무발화
    assert np.array_equal(p_plain, p_w1m1), (p_plain, p_w1m1)

    # T2: need > window 면 발화율이 90% 여도 전부 normal(-1) — 창이 아무리 차 있어도 M 에 못 미친다
    I2 = np.full((5, 1), 0.01, dtype="float32")               # thr=0.5 기준 5장 전부 발화
    seq2 = (np.array(["g"] * 5), np.arange(5, dtype="float64"))
    p2 = _rule_predict(I2, 0.5, seq=seq2, window=3, need=5)
    assert np.all(p2 == -1), p2

    # T3: 그룹 경계를 넘어 디바운스가 새지 않는다 — group "A" 뒤에 바로 "B" 가 오도록 이어붙여
    #     "새는" 구현이면 B 의 첫 프레임이 A 의 꼬리를 빌려 살아남는다.
    #     A: 3프레임 전부 발화(thr=0.5 기준 0.1<0.5) / B: [발화, 미발화]
    grp3 = np.array(["A", "A", "A", "B", "B"])
    ord3 = np.array([0.0, 1.0, 2.0, 0.0, 1.0])
    I3 = np.array([[0.1], [0.1], [0.1], [0.1], [0.6]], dtype="float32")
    p3 = _rule_predict(I3, 0.5, seq=(grp3, ord3), window=3, need=2)
    # A: f0 count=1(<2,죽음) f1 count=2(살음) f2 count=3(살음) / B: f0 count=1(<2,죽음,새면 3이 돼 살아남았을 것)
    #    f1 count=1(<2,죽음)
    assert p3.tolist() == [-1, 0, 0, -1, -1], p3

    # T4: thr=0.15·디바운스 없음이 기존 배치(`analysis_standard:252-253`) 공식과 동일 (배치 등가 고정)
    I4 = np.array([[0.10, 0.20], [0.50, 0.50], [0.05, 0.01], [0.90, 0.80]], dtype="float32")
    p4 = _rule_predict(I4, 0.15)
    manual = np.where((I4 < 0.15).any(1), I4.argmin(1), -1)
    assert np.array_equal(p4, manual), (p4, manual)

    # T5: argmin 마스킹 회귀 — 디바운스로 죽은 열이 전역 최소 IoU 를 갖고 있어도 뽑히면 안 된다.
    #     col0(A) 는 raw IoU 최솟값(0.05)을 갖지만 3프레임 중 2번만 발화(need=3 미달) → 죽는다.
    #     col1(B) 는 3프레임 전부 발화 → 산다. 살아있는 col1 이 뽑혀야 한다.
    I5 = np.array([[0.10, 0.10], [0.60, 0.10], [0.05, 0.40]], dtype="float32")
    grp5 = np.array(["v", "v", "v"])
    ord5 = np.array([0.0, 1.0, 2.0])
    p5 = _rule_predict(I5, 0.5, seq=(grp5, ord5), window=3, need=3)
    assert p5.tolist() == [-1, -1, 1], p5
    naive_argmin = int(I5[2].argmin())           # 마스킹 없이 전역으로 골랐다면 나왔을 (틀린) 값
    assert naive_argmin == 0 and p5[2] != naive_argmin, (naive_argmin, p5[2])

    # T6: ⚠️ 적대적 리뷰 #2 회귀 가드 — 예전 `window > 1` 가드는 W=1 이면 M 과 무관하게
    #     디바운스를 건너뛰고 raw fired 를 그대로 썼다(4,356격자 전수 대조 21/121 불일치의
    #     근원). W=1,M>=2 는 "1프레임 창에서 2회 이상 발화"가 원리적으로 불가능하므로 발화율이
    #     100%(thr=0.5 기준 전부 발화)여도 전부 normal(-1) 이어야 한다.
    I6 = np.array([[0.05, 0.20], [0.30, 0.02], [0.10, 0.10]], dtype="float32")
    seq6 = (np.array(["g", "g", "g"]), np.arange(3, dtype="float64"))
    for m in (2, 3, 9):
        p6 = _rule_predict(I6, 0.5, seq=seq6, window=1, need=m)
        assert np.all(p6 == -1), (m, p6)

    # T7: window=0 (빈 창) — count_only 공식대로 누적합은 항상 0. need=0 이면 `0>=0` 이 항상
    #     참이라 모든 열이 "산다" = 사실상 무필터 raw argmin, need>=1 이면 항상 normal. 둘 다
    #     크래시 없이 정의된 값을 내야 한다(예전엔 `window>1` 이 아니라 `window>1 or need>1`
    #     같은 부분 게이트를 썼어도 W<0 에서 `cs[lo]` IndexError 였다 — 그래서 음수도 같이 본다).
    p7_m0 = _rule_predict(I6, 0.5, seq=seq6, window=0, need=0)
    p7_m1 = _rule_predict(I6, 0.5, seq=seq6, window=0, need=1)
    manual_argmin = np.where((I6 < 0.5).any(1), I6.argmin(1), -1)
    assert np.array_equal(p7_m0, manual_argmin), (p7_m0, manual_argmin)   # need=0 → 사실상 무필터
    assert np.all(p7_m1 == -1), p7_m1

    # T8: 음수 window/need 는 크래시 없이 0으로 클램프된 것과 동일해야 한다(리뷰 #2 지적).
    p8 = _rule_predict(I6, 0.5, seq=seq6, window=-3, need=1)
    assert np.array_equal(p8, p7_m1), (p8, p7_m1)
    p8b = _rule_predict(I6, 0.5, seq=seq6, window=2, need=-5)
    assert np.array_equal(p8b, manual_argmin), (p8b, manual_argmin)      # need<0 도 사실상 무필터

    # ── ⑥ top-k 규칙 — 순위표(K_MAX) 캐시가 `prompt_cos_db.topk_vote` 와 일치하는지 (요청③,
    #    2026-09-21) ──
    if "/workspace" not in sys.path:
        sys.path.insert(0, "/workspace")
    import prompt_cos_db as _pcdb

    def _build_rank_table(S, lab, km, dtype="float32"):
        part = np.argpartition(S, S.shape[1] - km, axis=1)[:, -km:]   # 운영 코드와 동일 산식
        vpart = np.take_along_axis(S, part, 1)
        order = np.argsort(-vpart, axis=1)
        ov = np.take_along_axis(vpart, order, 1).astype(dtype)
        oc = lab[np.take_along_axis(part, order, 1)].astype("int8")
        return ov, oc

    # T9: 순위표 **알고리즘**(top-K_MAX 고정 선별 후 정렬해 자르기) ≡ topk_vote(개별 k 마다
    #     새로 argpartition) — 무작위 S 여러 개 × k ∈ {1,3,5,10,20,50} 전수 **완전 일치**.
    #     이게 이 기능에서 **가장 중요한 테스트**다. dtype 은 **운영 코드와 동일한 float32**.
    rng = np.random.default_rng(20260921)
    for trial in range(5):
        n_frames, n_sent, n_cls = 200, 300, 4
        S = rng.standard_normal((n_frames, n_sent)).astype(np.float32)
        lab = rng.integers(0, n_cls, n_sent).astype(np.int64)
        ord_v32, ord_c32 = _build_rank_table(S, lab, 50, dtype="float32")
        for k in (1, 3, 5, 10, 20, 50):
            got = _topk_predict(ord_v32, ord_c32, k, n_cls)
            want = _pcdb.topk_vote(S, lab, n_cls, k=k)
            assert np.array_equal(got, want), (trial, k, int((got != want).sum()))

    # T9b: **float16 을 쓰지 않는 이유를 실측으로 박아두는 테스트**다(운영은 float32 — 위 T9).
    #     초기 사양이 메모리 절감을 노려 float16 이었는데 그러면 정본과 조용히 갈린다.
    #     실측(2026-09-21, 이 시드): float32(T9) 는 전수 불일치 0,
    #     float16 은 24,000건 중 2건(8.3e-5). 원인은 SET 선택(T9 가 이미 증명한 알고리즘)이
    #     아니라 **저장값 반올림이 votes 동률의 타이브레이크(topc)를 흔드는 것** — votes 가
    #     동률이고 두 클래스의 top 코사인 차이가 float16 분해능(~5e-4, 0.3 부근)보다 작을
    #     때만 갈린다. 아래 상한은 실측의 60배 여유(알고리즘 자체가 틀렸다면 수백~수천 단위로
    #     터진다 — 상한을 크게 넘겨야만 진짜 회귀).
    n_checked = n_mismatch16 = 0
    for trial in range(5):
        n_frames, n_sent, n_cls = 200, 300, 4
        S = rng.standard_normal((n_frames, n_sent)).astype(np.float32)
        lab = rng.integers(0, n_cls, n_sent).astype(np.int64)
        ord_v16, ord_c16 = _build_rank_table(S, lab, 50, dtype="float16")
        for k in (1, 3, 5, 10, 20, 50):
            got = _topk_predict(ord_v16, ord_c16, k, n_cls)
            want = _pcdb.topk_vote(S, lab, n_cls, k=k)
            n_mismatch16 += int((got != want).sum())
            n_checked += len(got)
    rate16 = n_mismatch16 / max(n_checked, 1)
    assert rate16 < 0.005, (n_mismatch16, n_checked, rate16)
    print(f"self-check topk: 운영 dtype=float32 는 정본과 전수 일치(T9). "
          f"float16 이었다면 불일치 {n_mismatch16}/{n_checked} ({rate16:.4%}) — 그래서 안 쓴다(T9b). "
          f"⚠️ 이 값은 시드 의존이라 0 이어도 위험이 사라진 게 아니다(별도 20-trial 실측 2/24,000)")

    # T10: k=1 은 argmax(top-1 코사인의 클래스) 와 같다 — k=1 은 선택 원소가 항상 1개뿐이라
    #     votes 동률 자체가 성립할 수 없다(표가 갈릴 두 번째 후보가 없다). 그래서 float16
    #     저장(운영과 동일 dtype)에서도 **항상** 정확히 일치해야 한다.
    S1 = rng.standard_normal((50, 80)).astype(np.float32)
    lab1 = rng.integers(0, 3, 80).astype(np.int64)
    ordv1, ordc1 = _build_rank_table(S1, lab1, 50)
    got_k1 = _topk_predict(ordv1, ordc1, 1, 3)
    want_k1 = lab1[S1.argmax(1)]
    assert np.array_equal(got_k1, want_k1), (got_k1, want_k1)

    # T11: 동점 주입 회귀 스트레스 — 계획 경고(prompt_geometry.py:674-678, 동점 주입 200프레임
    #     중 28프레임 pred 갈림)와 같은 위험군. 여기서 갈리면 **고치지 않는다** — `topk_vote`
    #     가 정본이고, 순위표(top-K_MAX 고정 후 슬라이스) 방식이 개별 k 마다 새로 argpartition
    #     하는 원본과 동점 경계에서 다른 원소를 고를 수 있다는 사실을 알리는 것 자체가 목적이다.
    St = rng.standard_normal((200, 60)).astype(np.float32)
    St[:, :20] = 0.5                                    # 앞 20열을 완전 동점으로 만든다
    labt = rng.integers(0, 4, 60).astype(np.int64)
    ordvt, ordct = _build_rank_table(St, labt, 50)
    tie_mismatches = {}
    for k in (1, 3, 5, 10, 20, 50):
        got = _topk_predict(ordvt, ordct, k, 4)
        want = _pcdb.topk_vote(St, labt, 4, k=k)
        n_mismatch = int((got != want).sum())
        if n_mismatch:
            tie_mismatches[k] = n_mismatch
    if tie_mismatches:
        print(f"⚠️ self-check topk 동점 스트레스: 불일치 {tie_mismatches} — 동점 경계에서 "
              "순위표 방식이 topk_vote 원본과 갈릴 수 있다는 기지 위험(계획 경고와 같은 "
              "종류). topk_vote 가 정본이다 — 순위표 쪽을 고치지 않는다(보고 대상).")
    else:
        print("self-check topk 동점 스트레스: 불일치 0 (이 시드·이 동점 패턴에서는 일치)")

    # T12: ord_c 색인 체계 = classes(지역) 기준, pg.CLASS_NAMES(전역) 기준이 아니다 — 순서가
    #     다른 로컬 리스트에서 검증한다(전역·지역이 우연히 같은 순서면 이 버그를 못 잡는다).
    fake_global = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke"}
    local_classes = ["normal", "fire", "smoke"]         # falldown 이 로컬엔 없다(뱅크에 없는 코호트)
    bank_g = np.array([0, 1, 2, 3, 2, 0], dtype="int64")
    got_local = _bank_cls_to_local(bank_g, fake_global, local_classes)
    # 전역 인덱스를 그대로 썼다면 fire=2, smoke=3 으로 나왔을 것(둘 다 틀림) —
    # 로컬 인덱스는 normal=0, falldown(로컬에 없음)=-1, fire=1, smoke=2
    assert got_local.tolist() == [0, -1, 1, 2, 1, 0], got_local
    print("self-check topk OK — 알고리즘≡topk_vote(T9,float32) / float16 저장오차 실측(T9b) / "
          "k=1(T10) / 동점 스트레스(T11) / ord_c 지역색인(T12)")

    # 🔴 적대적 리뷰 #1 회귀 가드 — `fiftyone.yml` 의 `operators:` 는 FiftyOne 1.19.0 의
    # **하드 게이트**다(`plugins/definitions.py:201` → `name in self.operators`). `register(p)`
    # 에 `p.register(NewOp)` 를 넣어도 yml 목록에 이름이 없으면 **조용히 미등록**된다(에러도
    # 경고도 없다 — 실측: 좌석 3개, 등록된 오퍼레이터 4종, `errors=[]`, `ExplainRule` 이 그
    # 상태로 459줄이 데드코드였다). `_self_check()` 는 `register()` 를 부르지 않아 이 계층을
    # 구조적으로 못 잡았던 게 원인이었다 — 그래서 여기서 실제 `register()` 를 호출해 yml 과
    # 대조한다(정본을 새로 만들지 않는다 — 둘 다 이미 있는 정본을 서로 비춘다).
    import yaml as _yaml

    class _OpCollector:
        def __init__(self):
            self.names = set()

        def register(self, cls):
            self.names.add(cls().config.name)

    col = _OpCollector()
    register(col)
    yml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fiftyone.yml")
    with open(yml_path) as f:
        yml_ops = set(_yaml.safe_load(f).get("operators") or [])
    assert yml_ops == col.names, (
        f"fiftyone.yml operators({sorted(yml_ops)}) != register() 등록 이름"
        f"({sorted(col.names)}) — 둘 중 하나가 최신화되지 않았다")

    # ══════════════════════════════════════════════════════════════════
    # ⑦ TP/FP/FN 원시 카운트 + 혼동행렬 (요청: "fp/tp 값 노출" + "confusion matrix",
    #    2026-09-21) — `_scores_from_pred`/`_f1_table_md`/`_confusion_matrix_md` 공용 검증.
    # ══════════════════════════════════════════════════════════════════
    # 합성 스텁: 음성 4장(정답2·오판2) / falldown GT 3장(정답2·미검출1) / fire GT 2장
    # (예측 0건 — 분모 0 가드 확인) / smoke GT 0장(표에서 빠짐 확인) / 코호트 밖 GT 2종
    # (smoking 1장, loitering 1장 — 혼동행렬 (미채점) 행, "여러 클래스면 각각 한 행씩").
    classes7 = ["normal", "falldown", "fire", "smoke"]
    ev7 = ["falldown", "fire", "smoke"]
    gt_label7 = (["normal"] * 4 + ["falldown"] * 3 + ["fire"] * 2 + ["smoking", "loitering"])
    pred7 = [0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0]
    gt7 = np.array([classes7.index(g) if g in classes7 else -1 for g in gt_label7])
    pred7_arr = np.array(pred7)
    assert gt7.tolist() == [0, 0, 0, 0, 1, 1, 1, 2, 2, -1, -1], gt7.tolist()

    fp_normal7, rows7 = _scores_from_pred(pred7_arr, gt7, classes7, ev7)
    by7 = {r["cls"]: r for r in rows7}
    assert "smoke" not in by7, rows7          # GT 0 인 클래스는 여전히 표에서 빠진다(§C.3/G5 불변)
    assert set(by7) == {"falldown", "fire", "normal"}, rows7

    # 5-1) tp+fn == support, 클래스별(전 클래스 — normal 은 직접 `_f1_row` 로 대조)
    for c in ("falldown", "fire"):
        r = by7[c]
        assert r["tp"] + r["fn"] == r["support"], (c, r)
    f1_n, support_n, tp_n, fp_n_raw, fn_n, pr_n, rc_n = _f1_row(gt7, pred7_arr, 0)
    assert tp_n + fn_n == support_n, (tp_n, fn_n, support_n)
    assert tp_n == 2 and fn_n == 2 and support_n == 4, (tp_n, fn_n, support_n)

    # 5-2) Σ_classes(tp+fn) == 채점 프레임 수(미채점 gt<0 제외) — 항등식 유도:
    #      support_k = tp_k+fn_k(정의) 이고 Σ_k support_k = Σ_k count(gt==k) = count(gt>=0)
    #      (classes 가 유효 gt 값 0..len(classes)-1 를 정확히 분할하므로). fp 는 미채점 프레임의
    #      예측도 섞여 들어가 이 항등식에 없다(기존 `_f1_row` 그대로 — 회귀 0, 고치지 않는다).
    n_scored7 = int((gt7 >= 0).sum())
    total_tp_fn = (tp_n + fn_n) + sum(by7[c]["tp"] + by7[c]["fn"] for c in ("falldown", "fire"))
    assert total_tp_fn == n_scored7 == 9, (total_tp_fn, n_scored7)

    # 5-3) precision/recall/F1 공식이 `_f1_row`(기존 F1) 과 바이트 동일
    for c in ("falldown", "fire"):
        r = by7[c]
        pr = r["tp"] / max(r["tp"] + r["fp"], 1)
        rc = r["tp"] / max(r["tp"] + r["fn"], 1)
        assert r["precision"] == pr and r["recall"] == rc, (c, r, pr, rc)
        assert r["f1"] == round(2 * pr * rc / max(pr + rc, 1e-12), 4), (c, r)

    # 5-4) fp_normal 비율 == normal 행의 「오판한 수」 / normal support(=GT=normal 전체)
    assert by7["normal"]["fp"] == fn_n, (by7["normal"], fn_n)     # 오판한 수 = fn_normal(1-vs-rest)
    assert abs(fp_normal7 - fn_n / support_n) < 1e-9, (fp_normal7, fn_n, support_n)
    assert by7["normal"]["tp"] is None and by7["normal"]["fn"] is None \
        and by7["normal"]["f1"] is None, by7["normal"]           # 표시 목적과 무관한 칸은 비운다

    # 5-5) 분모 0(그 클래스 예측이 하나도 없음) 에서 크래시하지 않고 0 을 낸다 — fire 는 예측 0건
    assert by7["fire"]["tp"] == 0 and by7["fire"]["fp"] == 0, by7["fire"]
    assert by7["fire"]["precision"] == 0.0 and by7["fire"]["f1"] == 0.0, by7["fire"]

    md7 = _f1_table_md(rows7, n_unscored=2, n_total=len(gt7))
    # ⚠️ 전체 문자열의 `—` 개수를 세지 않는다 — 표 **아래 산문**이 바뀔 때마다 깨진다(2026-09-21
    #    실측: 미채점 안내 문구에 `—` 를 넣자 이 assert 가 터졌다. 로직이 아니라 테스트가 취약했다).
    #    의도는 "음성 행의 표시 안 하는 칸이 정확히 5개"이므로 **그 행만** 본다.
    neg_row7 = next(ln for ln in md7.splitlines() if "(음성)" in ln)
    assert neg_row7.count("—") == 5, neg_row7   # 음성 행의 tp/fn/precision/recall/f1 = 5칸만
    assert "normal(음성)" in md7 and "0.000" in md7, md7   # dash 행 + fire precision(분모0→0.000)
    assert "미채점" in md7, md7                 # n_unscored>0 이면 아래 한 줄이 붙는다

    # ── 혼동행렬(N×N, 2026-09-21 지시 변경 — "폼이 좁다"던 이전 판단 철회) ──
    missing_breakdown7 = {"fire": 3, "normal": 1}
    cm7 = _confusion_matrix_md(gt_label7, pred7_arr, classes7, missing_breakdown7,
                               n_view_total=len(gt_label7) + 4)
    # 대각선 굵게 + 행/열 값 — 손으로 검산한 값과 줄 단위로 완전 대조(부분 문자열 충돌 방지)
    assert "| normal | **2** | 2 | 0 | 0 | 4 |" in cm7, cm7
    assert "| falldown | 1 | **2** | 0 | 0 | 3 |" in cm7, cm7
    assert "| fire | 2 | 0 | **0** | 0 | 2 |" in cm7, cm7
    assert "| loitering(미채점) | 1 | 0 | 0 | 0 | 1 |" in cm7, cm7      # 코호트 밖 GT, 대각선 없음
    assert "| smoking(미채점) | 0 | 1 | 0 | 0 | 1 |" in cm7, cm7        # "여러 클래스면 각각 한 행씩"
    assert "| fire(결측) | — | — | — | — | 3 |" in cm7, cm7             # 결측 행 — 전부 '—'
    assert "| normal(결측) | — | — | — | — | 1 |" in cm7, cm7
    assert "| **합계** | 6 | 5 | 0 | 0 | 15 |" in cm7, cm7               # 열 합계 == 예측된 총수
    assert "다릅니다" not in cm7, cm7            # 15(scored+미채점+결측) == n_view_total(15) 일치
    # 열 합계 교차검증 — pred7 값 직접 카운트와 일치해야 한다
    assert sum(1 for p in pred7 if p == 0) == 6 and sum(1 for p in pred7 if p == 1) == 5

    print("self-check tp/fp/fn+혼동행렬 OK — support 항등식·byte-identical F1·fp_normal 정합·"
          "분모0 무크래시·행/열/전체 합계 일치")

    # ══════════════════════════════════════════════════════════════════
    # ⑧ 음성 클래스 이름 표준화 — "normal" 리터럴 금지 (2026-09-21 지시, "핵심 테스트":
    #    음성 클래스 이름이 normal 이 아닌 인공 코호트에서도 전 경로가 정상 동작해야 한다.
    # ══════════════════════════════════════════════════════════════════
    if "/workspace" not in sys.path:
        sys.path.insert(0, "/workspace")
    import cohort as _cohort_mod

    _FAKE_COHORT = "UnitTestFakeCohort_bg"
    _cohort_mod.COHORTS[_FAKE_COHORT] = dict(
        prompts=f"{_FAKE_COHORT}-prompts", gt_field="ground_truth", group="camera",
        negative_class="background", target_classes=["falldown", "fire"])
    try:
        neg_name, is_guess = _negative_class(_FAKE_COHORT, ["normal"])
        assert neg_name == "background" and is_guess is False, (neg_name, is_guess)

        classes_bg = [neg_name, "falldown", "fire"]
        gt_bg = np.array([0, 0, 1, 1, 2, -1])
        pred_bg = np.array([0, 1, 1, 0, 0, 1])
        fp_bg, rows_bg = _scores_from_pred(pred_bg, gt_bg, classes_bg, ["falldown", "fire"])
        by_bg = {r["cls"]: r for r in rows_bg}
        assert neg_name in by_bg, rows_bg           # "normal" 이 아니라 "background" 로 행이 생긴다
        assert by_bg[neg_name]["tp"] is None and by_bg[neg_name]["fp"] == 1, by_bg[neg_name]
        assert fp_bg == 0.5, fp_bg                  # background(gt==0) 2장 중 1장 오판

        md_bg = _f1_table_md(rows_bg, n_unscored=1, n_total=6)
        assert "background(음성)" in md_bg and "normal(음성)" not in md_bg, md_bg

        gt_label_bg = ["background", "background", "falldown", "falldown", "fire", "loitering"]
        cm_bg = _confusion_matrix_md(gt_label_bg, pred_bg, classes_bg, None, n_view_total=6)
        assert "| background |" in cm_bg and "loitering(미채점)" in cm_bg, cm_bg
        assert "normal" not in cm_bg, cm_bg          # 리터럴 "normal" 이 새어 들어오지 않는다
    finally:
        del _cohort_mod.COHORTS[_FAKE_COHORT]
        _NEG_CLASS_CACHE.pop(_FAKE_COHORT, None)
    print("self-check §표준화 OK — 음성 클래스 이름이 'normal' 이 아닌 코호트에서도 전 경로가 "
          "그 이름으로 동작한다(리터럴 하드코딩 없음)")

    # ══════════════════════════════════════════════════════════════════
    # ⑨ wave_iou 결측 프레임 제외 + 캐시 키 내용변화 감지 (2026-09-21 지시①②)
    # ══════════════════════════════════════════════════════════════════
    import datetime as _dt

    class _FakeDS:
        def __init__(self, name, lm=None):
            self.name = name
            self.last_modified_at = lm if lm is not None else _dt.datetime(2026, 1, 1)

    class _FakeView:
        def __init__(self, n, data, schema):
            self._n, self._data, self._schema = n, data, schema

        def count(self):
            return self._n

        def get_field_schema(self):
            return dict.fromkeys(self._schema, 1)

        def values(self, f):
            return [self._data[x] for x in f] if isinstance(f, (list, tuple)) else self._data[f]

        def _serialize(self):
            return [{"n": self._n}]

    # 결측(None) 프레임이 N 에서 통째로 제외되는지 — idx2(fire GT)·idx3(normal GT) 가 결측
    fake_ds_m = _FakeDS("UnitTestFakeDS_missing")
    fake_data_m = {
        "wave_iou_falldown_vtest": [0.1, 0.05, None, 0.2, 0.15, 0.02],
        "wave_iou_fire_vtest": [0.2, 0.3, 0.05, None, 0.25, 0.4],
        "ground_truth.label": ["normal", "falldown", "fire", "normal", "intrustion", "falldown"],
        "id": ["m0", "m1", "m2", "m3", "m4", "m5"],
    }
    fake_view_m = _FakeView(6, fake_data_m, {"wave_iou_falldown_vtest", "wave_iou_fire_vtest"})
    (ev_m, iou_m, gt_m, classes_m, gid_m, ok_m, note_m, gs_m, gl_m,
     gtlab_m, n_miss_m, mb_m, ids_m) = _rule_arrays(fake_ds_m, "vtest", fake_view_m)

    assert n_miss_m == 2, n_miss_m
    assert mb_m == {"fire": 1, "normal": 1}, mb_m
    assert len(gt_m) == 4 and len(gtlab_m) == 4, (gt_m, gtlab_m)
    assert gtlab_m == ["normal", "falldown", "intrustion", "falldown"], gtlab_m
    assert gt_m.tolist() == [0, 1, -1, 1], gt_m.tolist()      # intrustion → -1 (코호트 밖 GT)
    assert classes_m == ["normal", "falldown", "fire"], classes_m
    assert iou_m.shape == (4, 2), iou_m.shape
    # 결측이 지표 모수에서 실제로 빠졌는지 — (채점+코호트밖) + 결측 == 뷰 전체
    assert len(gt_m) + n_miss_m == fake_view_m.count() == 6
    assert note_m and "wave_iou" in note_m and "재실행" in note_m, note_m
    # sample_ids(2026-09-21 save_view 정합성 수정) — 결측(idx2·3)이 빠진 나머지 4개만, 순서 보존
    assert ids_m == ["m0", "m1", "m4", "m5"], ids_m
    assert len(ids_m) == len(gt_m) == len(gtlab_m), (ids_m, gt_m, gtlab_m)

    print("self-check 결측처리 OK — wave_iou 결측 프레임이 N 에서 제외되고 breakdown 이 맞다")

    # 캐시 키 — 같은 개수·같은 뷰 정의, `last_modified_at` 만 다른 두 데이터셋이 다른 키를 낸다
    # (2026-09-21 지시② — "새로고침해도 캐시가 그대로다" 문제. 슬라이더 드래그 경로에 O(N)
    # Mongo 조회를 새로 넣지 않기 위해 데이터셋 단일 타임스탬프를 쓴 선택의 근거는
    # `_content_fingerprint` 문서화 참조).
    ds_a = _FakeDS("UnitTestFakeDS_cache", _dt.datetime(2026, 1, 1))
    ds_b = _FakeDS("UnitTestFakeDS_cache", _dt.datetime(2026, 1, 2))
    same_view = _FakeView(3, {"x": [1, 2, 3]}, {"x"})
    key_a = _field_cache_key(ds_a, "vtest", same_view)
    key_b = _field_cache_key(ds_b, "vtest", same_view)
    assert key_a[:5] == key_b[:5], (key_a, key_b)     # 개수·태그·뷰정의(스테이지 목록)는 동일
    assert key_a[5] != key_b[5], (key_a, key_b)       # 내용(최종수정시각)만 다르면 이 자리가 다르다
    assert _bank_fingerprint("__no_such_bank_version_ever__") == 0   # 파일 없음 → fail-soft 0

    print("self-check 캐시키 OK — 개수 동일·내용(last_modified_at) 변화가 캐시 키를 바꾼다")

    # ══════════════════════════════════════════════════════════════════
    # ⑩ 자동전환 판정(`_wave_field_reliable`) — 결측·스테일 각각 단독으로 전환을 유발하고,
    #    신선하면(결측 0·스테일 없음) 그대로 필드를 믿는다(2026-09-21 새 지시①, 회귀 조건).
    #    `resolve_input`/`execute()` 가 공유하는 판정 함수 자체를 직접 검증한다 — 렌더링
    #    (Notice/Warning 문구)까지는 아래 ⑪이 실제 ctx 로 한 번 더 확인한다.
    # ══════════════════════════════════════════════════════════════════
    fake_ds_fresh = _FakeDS("UnitTestFakeDS_fresh")
    fake_data_fresh = {
        "wave_iou_falldown_vtest": [0.1, 0.05, 0.3],
        "wave_iou_fire_vtest": [0.2, 0.3, 0.1],
        "ground_truth.label": ["normal", "falldown", "fire"],
        "id": ["f0", "f1", "f2"],
    }
    fake_view_fresh = _FakeView(3, fake_data_fresh,
                                {"wave_iou_falldown_vtest", "wave_iou_fire_vtest"})

    reliable_fresh, ra_fresh, stale_fresh, reasons_fresh = _wave_field_reliable(
        fake_ds_fresh, "vtest", fake_view_fresh)
    assert reliable_fresh is True and reasons_fresh == [] and stale_fresh is None, (
        reliable_fresh, reasons_fresh, stale_fresh)
    assert ra_fresh[10] == 0, ra_fresh[10]                       # n_missing
    assert ra_fresh[12] == ["f0", "f1", "f2"], ra_fresh[12]      # sample_ids — 결측 0 이라 전량 보존

    # `fake_ds_m`/`fake_view_m` 재사용(위 ⑨) — n_missing=2, 스테일은 아니다(미등록 데이터셋이라
    # `_wave_staleness_note` 가 fail-soft 로 None).
    reliable_miss, _ra_miss, stale_miss, reasons_miss = _wave_field_reliable(
        fake_ds_m, "vtest", fake_view_m)
    assert reliable_miss is False, reliable_miss
    assert reasons_miss == ["신규 이미지 2장"], reasons_miss
    assert stale_miss is None, stale_miss

    # 뱅크가 최신인 경우 — 라이브 프로필 등록 없이 재현하려고 `_wave_staleness_note` 자체를
    # 잠깐 스텁으로 갈아 끼운다(위 ⑧의 `cohort.COHORTS` 몽키패치와 같은 급의 임시 치환 —
    # 이 self-check 는 라이브 프로필에 의존하지 않는다는 원칙을 지킨다).
    _orig_staleness_fn = globals()["_wave_staleness_note"]
    globals()["_wave_staleness_note"] = lambda *_a, **_k: "⚠️ 뱅크가 wave_iou 필드보다 최신입니다(가짜)"
    try:
        reliable_stale, _ra_stale, stale_stale, reasons_stale = _wave_field_reliable(
            fake_ds_fresh, "vtest", fake_view_fresh)
    finally:
        globals()["_wave_staleness_note"] = _orig_staleness_fn
    assert reliable_stale is False and reasons_stale == ["뱅크가 더 최신"], (
        reliable_stale, reasons_stale)
    assert stale_stale is not None, stale_stale

    print("self-check 자동전환 판정 OK — 결측·스테일이 각각 단독으로 전환을 유발하고, "
          "둘 다 없으면 필드를 그대로 믿는다")

    # ══════════════════════════════════════════════════════════════════
    # ⑪ 온디맨드 캐시 무효화 — 뱅크 mtime·데이터셋 내용 변화가 실제 재계산을 트리거하는지
    #    (2026-09-21 새 지시③ — 키만 다른 게 아니라 `_rule_arrays_ondemand` 가 **실제로
    #    다시 돈다**는 것까지 고정한다). DB·FiftyOne·pgvector 없이: `prompt_geometry`
    #    의존은 `sys.modules` 치환으로 걷어낸다(위 ⑧과 같은 원칙, 대상만 모듈 전체로 넓어진다).
    #    `prompt_cos_db`(코사인·wave_iou 커널)는 순수 numpy 라 그대로 실제 모듈을 쓴다.
    # ══════════════════════════════════════════════════════════════════
    import shutil
    import tempfile

    tmp_dir = tempfile.mkdtemp(prefix="ondemand_selfcheck_")
    calls = {"n": 0}

    class _FakePG:
        PROMPT_DIR = tmp_dir
        PROFILES = {}                            # 미등록 → `_pg_profile` 이 ValueError(§표준화 폴백 유도)
        CLASS_NAMES = {0: "normal", 1: "falldown"}

        @staticmethod
        def load_bank(_version):
            calls["n"] += 1
            # ⚠️ `prompt_cos_db.wave_iou` 는 "normal" 을 리터럴로 찾는다(그 파일 자체의 기존
            #    설계다 — 이 플러그인의 §표준화 대상이 아니라 손대지 않는다) — 뱅크는 반드시
            #    "normal" 클래스를 포함해야 `members["normal"]` 조회가 성립한다.
            return {"vec": np.array([[1.0, 0.0], [0.0, 1.0]], dtype="float32"),
                    "cls": np.array([0, 1], dtype="int64")}

    bank_npz = os.path.join(tmp_dir, "FakeBankV1.npz")
    with open(bank_npz, "wb") as f:            # 내용은 무관 — `_bank_fingerprint` 는 mtime/size 만 본다
        f.write(b"placeholder")

    real_pg_mod = sys.modules.get("prompt_geometry")
    sys.modules["prompt_geometry"] = _FakePG()
    try:
        fake_ds_od1 = _FakeDS("UnitTestOndemandDS", _dt.datetime(2026, 1, 1))
        fake_ds_od2 = _FakeDS("UnitTestOndemandDS", _dt.datetime(2026, 1, 2))
        ids_od = ["o0", "o1", "o2", "o3"]
        fake_view_od = _FakeView(4, {
            "emb": [[1.0, 0.0], [0.0, 1.0], [1.0, 0.0], [0.0, 1.0]],
            "ground_truth.label": ["normal", "falldown", "normal", "falldown"],
            "id": ids_od,
        }, {"emb"})

        key1 = _ondemand_cache_key(fake_ds_od1, "FakeBankV1", fake_view_od)
        r1 = _rule_arrays_ondemand(fake_ds_od1, fake_view_od, "emb", "FakeBankV1")
        assert calls["n"] == 1, calls                            # 최초 호출 — 실제 계산됨
        # sample_ids(2026-09-21 save_view 정합성 수정) — 결측 0 이라 전량·순서 보존
        assert r1[-1] == ids_od, r1[-1]
        assert len(r1[-1]) == len(r1[2]), (r1[-1], r1[2])         # len(sample_ids) == len(gt)

        # 같은 (데이터셋·뷰·뱅크) — 캐시 히트, 재계산 없음(같은 객체가 그대로 반환된다)
        key1b = _ondemand_cache_key(fake_ds_od1, "FakeBankV1", fake_view_od)
        assert key1 == key1b, (key1, key1b)
        r1b = _rule_arrays_ondemand(fake_ds_od1, fake_view_od, "emb", "FakeBankV1")
        assert calls["n"] == 1, calls                             # ⚠️ 안 늘어나야 한다
        assert r1b is r1, "캐시 히트인데 다른 객체 — 재계산됐을 가능성"

        # ── dataset.last_modified_at 변화 → 키가 바뀌고 실제 재계산된다 ──
        key2 = _ondemand_cache_key(fake_ds_od2, "FakeBankV1", fake_view_od)
        assert key2 != key1, (key1, key2)
        r2 = _rule_arrays_ondemand(fake_ds_od2, fake_view_od, "emb", "FakeBankV1")
        assert calls["n"] == 2, calls                             # 재계산됨(캐시 반환이 아니라)
        assert r2 is not r1

        # ── 뱅크 npz mtime 변화 → 같은 데이터셋·뷰라도 키가 바뀌고 재계산된다 ──
        newer = os.path.getmtime(bank_npz) + 3600
        os.utime(bank_npz, (newer, newer))
        key3 = _ondemand_cache_key(fake_ds_od2, "FakeBankV1", fake_view_od)
        assert key3 != key2, (key2, key3)
        r3 = _rule_arrays_ondemand(fake_ds_od2, fake_view_od, "emb", "FakeBankV1")
        assert calls["n"] == 3, calls                             # 또 재계산됨
        assert r3 is not r2

        # ── sample_ids 정합성 — 온디맨드 경로도 결측을 **중간에** 섞어 검증한다(요청 원문:
        # "결측을 맨 끝에 두면 버그가 안 드러나니 반드시 중간에 섞어라"). 6장 중 idx2·4(3·5번째)
        # 임베딩 결측 — 끝(idx5)도 아니고 시작도 아닌 위치를 골라 절단 방향 버그까지 배제한다.
        ids_od_miss = [f"od{i}" for i in range(6)]
        fake_view_od_miss = _FakeView(6, {
            "emb": [[1.0, 0.0], [0.0, 1.0], None, [1.0, 0.0], None, [0.0, 1.0]],
            "ground_truth.label": ["normal", "falldown", "normal", "normal", "falldown", "falldown"],
            "id": ids_od_miss,
        }, {"emb"})
        r_miss = _rule_arrays_ondemand(fake_ds_od1, fake_view_od_miss, "emb", "FakeBankV1")
        n_missing_odm, ids_odm = r_miss[10], r_miss[-1]
        assert n_missing_odm == 2, n_missing_odm
        assert ids_odm == ["od0", "od1", "od3", "od5"], ids_odm   # idx2·4 만 빠지고 순서 보존
        assert len(ids_odm) == len(r_miss[2]) == 4, (ids_odm, r_miss[2])
    finally:
        sys.modules.pop("prompt_geometry", None)
        if real_pg_mod is not None:
            sys.modules["prompt_geometry"] = real_pg_mod
        _NEG_CLASS_CACHE.pop("UnitTestOndemandDS", None)
        shutil.rmtree(tmp_dir, ignore_errors=True)

    print("self-check 온디맨드 캐시 OK — dataset.last_modified_at·뱅크 npz mtime 변화가 "
          "캐시 키를 바꾸고 `_rule_arrays_ondemand` 가 실제로 재계산한다(캐시 반환이 아니라)")

    # ══════════════════════════════════════════════════════════════════
    # ⑫ save_view id 정합성 — 결측을 **중간**에 섞어 재현(코디네이터 지시, 2026-09-21 후속).
    #    `execute()` 가 실제로 쓰는 zip 로직(`zip(sample_ids, (pred != gt).tolist())`)을 그대로
    #    복제해 손으로 계산한 정답과 정확히 일치하는지 확인하고, 옛 버그(`zip(view.values("id"),
    #    ...)` — "id" 는 결측 제외 **전** 전체 N, `pred`/`gt` 는 결측 제외 **후** N_kept)가
    #    실제로 **다른(틀린) 프레임**을 가리켰다는 것까지 대조한다. 결측을 맨 끝에 두면 이
    #    버그가 절대 안 드러난다(짧은 쪽에서 끊기는 zip 이 우연히 맞아떨어진다) — 그래서
    #    10장 중 3·7번째(0-index 2·6)로 **중간에** 둔다.
    # ══════════════════════════════════════════════════════════════════
    n_sv = 10
    miss_idx_sv = {2, 6}
    ids_sv = [f"id{i}" for i in range(n_sv)]
    fake_ds_sv = _FakeDS("UnitTestFakeDS_saveview")
    fake_data_sv = {
        "wave_iou_falldown_vtest": [None if i in miss_idx_sv else 0.1 for i in range(n_sv)],
        "wave_iou_fire_vtest": [None if i in miss_idx_sv else 0.2 for i in range(n_sv)],
        "ground_truth.label": ["normal"] * n_sv,
        "id": ids_sv,
    }
    fake_view_sv = _FakeView(n_sv, fake_data_sv,
                             {"wave_iou_falldown_vtest", "wave_iou_fire_vtest"})
    ra_sv = _rule_arrays(fake_ds_sv, "vtest", fake_view_sv)
    n_missing_sv, sample_ids_sv, gt_sv = ra_sv[10], ra_sv[-1], ra_sv[2]

    expected_ids_sv = [f"id{i}" for i in range(n_sv) if i not in miss_idx_sv]   # 손으로 계산한 정답
    assert n_missing_sv == 2, n_missing_sv
    assert sample_ids_sv == expected_ids_sv, (sample_ids_sv, expected_ids_sv)
    assert len(sample_ids_sv) == len(gt_sv) == 8, (sample_ids_sv, gt_sv)        # 불변식(요청 원문)

    # `execute()` 의 실제 save_view 로직을 그대로 복제 — 축소된(N_kept=8) 배열 기준 위치
    # 1·5 에서 예측이 틀렸다고 가정한다(reduced idx 1→원본 id1, reduced idx 5→원본 id7 —
    # miss_idx_sv={2,6} 을 건너뛴 순서이므로 reduced=[0,1,3,4,5,7,8,9]).
    pred_sv = np.array([0, 1, 0, 0, 0, 1, 0, 0])
    mism_ids_fixed = [sid for sid, keep in zip(sample_ids_sv, (pred_sv != gt_sv).tolist()) if keep]
    assert mism_ids_fixed == ["id1", "id7"], mism_ids_fixed

    # 옛 버그 재현 — `view.values("id")`(결측 제외 **전** 전체 10개)를 축소 배열(8개, boolean)
    # 과 그냥 zip 했다면 무엇이 나왔을지. zip 은 짧은 쪽(8)에서 끊기므로 ids_sv[0:8] 과 짝지어진다.
    old_buggy_ids = ids_sv                        # = (수정 전) `view.values("id")` 였을 값
    old_buggy_mism = [sid for sid, keep in zip(old_buggy_ids, (pred_sv != gt_sv).tolist()) if keep]
    assert old_buggy_mism == ["id1", "id5"], old_buggy_mism      # id5 는 틀렸다 — 정답은 id7
    assert old_buggy_mism != mism_ids_fixed, (
        "버그 재현 실패 — 수정 전/후 결과가 같으면 이 테스트가 아무것도 증명하지 못한다")

    print("self-check save_view id 정합성 OK — 결측 중간 섞기(idx2·6/10)에서 sample_ids 가 "
          "손으로 계산한 정답과 일치하고, 옛 zip(view.values('id')) 방식이 실제로 다른(틀린) "
          "프레임을 가리켰음을 대조 확인했다")

    print("self-check OK")


def _self_check_live():
    """`_rule_predict`(count_only, 이벤트창 묶음) + 실제 `sourcei` 데이터 대조.

    독립 오라클이 확정한 값(2026-09-21 사양 교정 2/2)과 소수 4자리까지 일치해야 한다 — 두 독립
    구현이 일치한다는 증거. `_self_check()` 본체(App·임베딩 서비스 없이)와 달리 이 함수는 살아있는
    FiftyOne+Mongo 접근이 필요하다(App 자체나 embedding-service 는 필요 없다) — 그래서 별도
    함수로 분리했다. `sourcei` 를 못 읽으면 **건너뛴다**(실패로 치지 않는다, 환경 문제와 코드
    회귀를 구분한다). 읽었는데 수치가 다르면 그건 진짜 회귀이므로 그대로 raise 한다.
    """
    try:
        ds = fo.load_dataset("sourcei")
        view = ds.view()
        (ev, iou, gt, classes, group_id, order_key, _note, _group_sizes, _group_label,
         gt_label, n_missing, missing_breakdown, sample_ids) = _rule_arrays(ds, "v1084", view)
    except Exception as e:                     # noqa: BLE001 — 라이브 데이터가 없으면 건너뛴다
        print(f"self-check(live) SKIP — {type(e).__name__}: {e}")
        return

    # ⚠️ 2026-09-21 지시③ 회귀 가드 — sourcei/sitej 는 결측 0 이라고 확인된 전제(코디네이터
    # 지시 원문)다. 0 이 아니면 그 전제가 깨진 것이므로 **여기서 바로 보고**해야 한다 —
    # 결측 프레임이 있으면 `len(gt)` 가 줄어 아래 오라클 4점 비교 자체가 무의미해진다.
    assert n_missing == 0, (
        f"sourcei 결측 {n_missing}장 — 회귀 전제 위반, self-check(live) 오라클을 재확인할 것")

    # ⚠️ save_view 정합성 수정(2026-09-21 새 지시) 라이브 확인 — 길이 불변식 + 반환된
    # `sample_ids` 가 실제로 이 데이터셋의 유효한 표본 id 인지(라이브 Mongo 조회로 실증,
    # `_self_check()`(순수 함수)에서는 확인할 수 없는 부분).
    assert len(sample_ids) == len(gt) == len(gt_label), (
        len(sample_ids), len(gt), len(gt_label))
    n_selected = view.select(sample_ids).count()
    assert n_selected == len(sample_ids), (
        f"sample_ids 중 {len(sample_ids) - n_selected}개가 이 뷰에서 select 되지 않았습니다 "
        "— id 가 어긋났을 가능성")

    # 표: thr, W, M, (fp_normal, falldown, fire, smoke) — 독립 오라클 v2, group=event 묶음.
    # (0.15/0.30, W=1,M=1) 은 디바운스 없음이라 묶음 방식과 무관 — v1 오라클과도 일치했다.
    cases = [
        (0.15, 5, 3, dict(fp_normal=0.0030, falldown=0.6972, fire=0.2570, smoke=0.2969)),
        (0.30, 5, 3, dict(fp_normal=0.0118, falldown=0.7599, fire=0.6902, smoke=0.6601)),
        (0.15, 1, 1, dict(fp_normal=0.0015, falldown=0.8083, fire=0.4264, smoke=0.3440)),
        (0.30, 1, 1, dict(fp_normal=0.0110, falldown=0.8820, fire=0.8061, smoke=0.7290)),
    ]
    for thr, window, need, expected in cases:
        pred, fp_normal, rows = _rule_metrics(iou, gt, classes, ev, thr, window, need,
                                              group_id, order_key)
        got = {"fp_normal": round(fp_normal, 4) if fp_normal is not None else None}
        got.update({r["cls"]: r["f1"] for r in rows})
        for k, v in expected.items():
            assert got.get(k) is not None and abs(got[k] - v) < 1e-4, \
                (thr, window, need, k, got, expected)

        # ── 혼동행렬 교차검증(2026-09-21 지시 — "가장 중요한" 검증) — 대각선 == TP(위 표) ──
        by_cls = {r["cls"]: r for r in rows}
        cm_md = _confusion_matrix_md(gt_label, pred, classes, missing_breakdown,
                                     n_view_total=len(gt_label) + n_missing)
        for c in ev:
            ci = classes.index(c)
            diag = int(((pred == ci) & (np.asarray(gt_label, dtype=object) == c)).sum())
            assert diag == by_cls[c]["tp"], (thr, window, need, c, diag, by_cls[c]["tp"], cm_md)
        # 행 합계(음성 클래스, GT=classes[0]) == 그 클래스의 GT 수(support) — F1 표의 normal 행
        neg = classes[0]
        neg_support = int((np.asarray(gt_label, dtype=object) == neg).sum())
        assert neg_support == by_cls[neg]["support"], (thr, window, need, neg_support, by_cls[neg])
    print("self-check(live) OK — 2026-09-21 오라클 4점 그리드 일치 (count_only, 이벤트창 묶음) "
          "+ 혼동행렬 대각선=TP 교차검증")


if __name__ == "__main__":
    _self_check()
    _self_check_live()
