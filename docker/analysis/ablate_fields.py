#!/usr/bin/env python3
"""절/구/단어 절제로 **어느 부분이 최고 코사인과 배경 코사인을 만드는지** 측정해
FiftyOne 문장 데이터셋(`<ds>-prompts`)과 프레임 데이터셋에 필드로 올린다.

왜 절제인가: `/embed_text` 는 문장당 1024-d 벡터 **하나**만 준다. 토큰별 기여도를 내부에서
꺼낼 경로가 없으므로 "이 단어가 코사인의 몇 %" 는 원리적으로 계산 불가다. 대신 그 부분을
**빼고 다시 임베딩해** 코사인이 얼마나 떨어지는지 재는 것이 유일하게 정직한 측정이다.

지표 정의는 프로브 플러그인(`user-prompt-probe/__init__.py:196,214,249`)과 동일:
  · `max_cos` = 전체 프레임 중 최대   · `bg_cos` = **GT=normal 프레임과의 평균**

절제 단위 — **뱅크 문장 구조에 따라 자동 선택**한다 (codex 리뷰 §1):
  · v1.0.8.4 형 `It is a X. Y. Z.`  → **clause**(마침표 경계). 3절 15,750 / 2절 375
  · v1.0.8.0 형 `A small fire blazes in the center in daylight.` → **phrase**
    (전치사·쉼표 경계). 실측: 마침표 1개 11,740 + 0개 740 = **전부 단일 절**이라
    clause 절제가 no-op 이었다. 중앙 11단어라 구 단위가 유일한 중간 해상도다.
  · word — 두 경우 모두. 단 `a`/`is` 같은 기능어는 빼면 문장이 깨져서 코사인이 떨어지는데
    그건 "그 단어가 중요"한 게 아니다 → `*_is_stopword` 플래그로 표시한다.

⚠️ **배경 코사인은 문장만의 속성이 아니라 문장×데이터셋의 속성이다.** 같은 문장이 source-h
   에서는 자석이 아니었는데 백화점에서는 자석일 수 있다 → `ablate_dataset` 필드로 남긴다.
⚠️ 뱅크 간 절대 코사인 비교 금지 (가산 오프셋). 여기 값은 **한 뱅크 내부** 비교용이다.
⚠️ `d_max` 는 "이 조각을 빼면 **달성 가능한 천장**이 얼마나 낮아지나" 다. base 와 variant 의
   argmax 프레임이 다를 수 있어 특정 이미지를 가리키지 않는다 (플러그인 `max_cos` 도 동일).

env: AB_PROFILE(sourcei) AB_TOPN(400) AB_WORDS(1) AB_RETRY(3)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time

import numpy as np
import requests

PROFILES = {
    "sourcei": ("/data/fiftyone/sourcei", "sourcei"),
    "sourceh": ("/data/fiftyone/sourceh_v2", "source-h"),
}
BANK_NPZ = "/data/fiftyone/sourceh/prompts/{v}.npz"
URL = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")
EPS = 1e-5                    # 이보다 작은 하락은 "무효과" — 부호 없는 argmax 금지(codex §5)
# `prompt_geometry.GIDX_OFFSET` 와 **같은 값**. -prompts 의 `gidx` 는 2026-08-11 다중뱅크
# 리빌드부터 `뱅크순번 × GIDX_OFFSET + 뱅크-로컬` 인 전역 id 다 — npz 행 인덱스로 쓰려면
# `% GIDX_OFFSET` 이 필요하다 (안 하면 뱅크순번 0 이외 전 버전에서 IndexError).
GIDX_OFFSET = 100_000
STOP = {"a", "an", "the", "is", "are", "was", "were", "in", "on", "at", "of", "to",
        "and", "or", "it", "its", "with", "by", "for", "there", "has", "have", "be"}
# 구 경계 — 전치사/접속 앞에서 자른다. 쉼표도 경계다.
PHRASE_RX = re.compile(r"(?=\b(?:in|on|at|of|to|with|near|behind|beside|under|over|"
                       r"through|across|around|between|inside|outside|during|while|"
                       r"and|but|as)\b)|(?<=,)")


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def vtag(version: str) -> str:
    parts = version.lstrip("v").split(".")
    return "v" + "".join(parts[-3:] if len(parts) >= 3 else parts)


def clauses_of(text: str) -> list[str]:
    parts = [s.strip() for s in text.strip().rstrip(".").split(".") if s.strip()]
    return [p + "." for p in parts]


def phrases_of(text: str) -> list[str]:
    """전치사/쉼표 경계로 구 분할. 단일 절 뱅크(v1.0.8.0)의 중간 해상도."""
    body = text.strip()
    parts = [p.strip() for p in PHRASE_RX.split(body) if p and p.strip()]
    return [p for p in parts if len(re.findall(r"[A-Za-z']+", p)) >= 1]


def word_spans(text: str) -> list[tuple[str, int, int]]:
    """(단어, start, end). **원문 구두점을 보존**하며 제거하려면 span 이 필요하다(codex §4).
    `" ".join(words)` 방식은 마침표를 전부 날려 모든 단어 변형에 동일한 구조손실이
    섞여 들어가 순위를 오염시킨다."""
    return [(m.group(0), m.start(), m.end()) for m in re.finditer(r"[A-Za-z']+", text)]


def drop_span(text: str, s: int, e: int) -> str:
    return re.sub(r"\s{2,}", " ", (text[:s] + text[e:])).strip()


def embed_many(texts: list[str], retry: int) -> np.ndarray:
    """실패 1건이 전 구간을 날리지 않게 재시도한다 (codex §7 — embedding-service 는
    GPU 정비 창에서 503 을 낼 수 있다)."""
    sess = requests.Session()
    out = np.empty((len(texts), 1024), dtype=np.float32)
    for i, t in enumerate(texts):
        for k in range(retry):
            try:
                r = sess.post(f"{URL}/embed_text", data={"text": t}, timeout=180)
                r.raise_for_status()
                v = np.asarray(r.json()["vector"], dtype=np.float32)
                out[i] = v / np.linalg.norm(v)
                break
            except Exception as exc:                                  # noqa: BLE001
                if k == retry - 1:
                    raise SystemExit(f"embed 실패({i}/{len(texts)}): {exc!r}") from exc
                time.sleep(0.5 * (k + 1))
        if (i + 1) % 1000 == 0:
            log(f"  embed {i + 1}/{len(texts)}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default=os.environ.get("AB_PROFILE", "sourcei"),
                    choices=list(PROFILES))
    ap.add_argument("--bank", default=os.environ.get("BANK_A", "v1.0.8.0"))
    ap.add_argument("--topn", type=int, default=int(os.environ.get("AB_TOPN", "400")))
    ap.add_argument("--words", type=int, default=int(os.environ.get("AB_WORDS", "1")))
    ap.add_argument("--retry", type=int, default=int(os.environ.get("AB_RETRY", "3")))
    a = ap.parse_args()
    root, dsname = PROFILES[a.profile]

    import fiftyone as fo
    d = np.load(f"{root}/work/embed.npz", allow_pickle=True)
    keys = [str(k) for k in d["key"]]
    X = d["vec"].astype(np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    led = {r["key"]: r for r in (json.loads(x) for x in
                                open(f"{root}/work/ledger.jsonl", encoding="utf-8"))}
    gt = np.array([led[k]["gt_class"] for k in keys], dtype=np.int64)
    bg = gt == 0
    log(f"{dsname}: 프레임 {len(keys):,} / 배경 모수(GT=normal) {int(bg.sum()):,}")
    if not bg.any():
        raise SystemExit("GT=normal 프레임 없음 — 배경 코사인 미정의")

    pds = fo.load_dataset(f"{dsname}-prompts")
    sid, gidx, texts, wins, bver = (pds.values(f) for f in
                                    ("id", "gidx", "text", "wins", "bank_version.label"))
    # ⚠️ gidx 는 **전역** id 다 (뱅크순번 × GIDX_OFFSET + 로컬). npz 행 인덱스로 쓸 때만
    #    `% GIDX_OFFSET` 로 로컬 환산하고, 프레임 `winner_gidx_*` 조인은 전역 그대로 쓴다.
    #    버전을 안 걸면 다른 버전 문장의 벡터를 base 로 집어와 전 수치가 조용히 오염된다 (codex §2).
    cand = [i for i in range(len(sid)) if bver[i] == a.bank]
    if len(cand) < len(sid):
        log(f"주의: bank_version != {a.bank} 인 문장 {len(sid) - len(cand)}개 제외")
    cand = [i for i in cand if (wins[i] or 0) > 0]        # 미채택은 의미 없음 (codex §12)
    order = sorted(cand, key=lambda i: -(wins[i] or 0))[:a.topn]
    if not order:
        raise SystemExit(f"{a.bank} 에서 wins>0 문장이 없다")
    log(f"대상 문장 {len(order)} (wins {wins[order[0]]}~{wins[order[-1]]})")

    V = np.load(BANK_NPZ.format(v=a.bank), allow_pickle=True)["vec"].astype(np.float32)

    # 절제 단위 자동 선택
    n_multi = sum(1 for i in order if len(clauses_of(texts[i])) >= 2)
    unit = "clause" if n_multi >= 0.5 * len(order) else "phrase"
    splitter = clauses_of if unit == "clause" else phrases_of
    log(f"구조 단위 = **{unit}** (다절 문장 {n_multi}/{len(order)})")

    jobs, meta = [], []
    n_struct = 0
    for i in order:
        t = texts[i]
        ps = splitter(t)
        if len(ps) >= 2:
            n_struct += 1
            for j in range(len(ps)):
                rest = ps[:j] + ps[j + 1:]
                jobs.append(" ".join(rest) if unit == "clause" else " ".join(rest))
                meta.append((i, unit, ps[j]))
        if a.words:
            sp = word_spans(t)
            if len(sp) >= 4:
                for w, s, e in sp:
                    jobs.append(drop_span(t, s, e))
                    meta.append((i, "word", w))
    if n_struct == 0:
        log(f"⚠️ {unit} 절제가 0건 — 문장이 전부 단일 조각이다. 단어 단위만 산출된다")
    log(f"절제 변형 {len(jobs):,}개 ({unit} {n_struct}문장분 + word) — 임베딩 시작")
    t0 = time.time()
    E = embed_many(jobs, a.retry) if jobs else np.zeros((0, 1024), dtype=np.float32)
    log(f"임베딩 완료 {time.time() - t0:.0f}s")

    base = V[[int(gidx[i]) % GIDX_OFFSET for i in order]]      # 전역 gidx → npz 행
    bmax = (X @ base.T).max(axis=0)
    bbg = (X[bg] @ base.T).mean(axis=0)
    pos = {i: p for p, i in enumerate(order)}
    vmax = np.empty(len(jobs), dtype=np.float32)
    vbg = np.empty(len(jobs), dtype=np.float32)
    for s in range(0, len(jobs), 512):
        S = X @ E[s:s + 512].T
        vmax[s:s + 512] = S.max(axis=0)
        vbg[s:s + 512] = S[bg].mean(axis=0)
        del S

    agg: dict[int, dict] = {i: {unit: [], "word": []} for i in order}
    for k, (i, kind, piece) in enumerate(meta):
        p = pos[i]
        agg[i][kind].append({"piece": piece,
                             "d_max": float(bmax[p] - vmax[k]),
                             "d_bg": float(bbg[p] - vbg[k])})

    # bg_ratio 는 **코호트 백분위**로 구간화한다. 절대컷은 뱅크 성질이라 버전 간 의미가
    # 달라진다 — 이 파일과 같은 실수를 prompt_geometry 가 이미 한 번 고쳤다 (codex §6).
    ratio = np.array([bbg[pos[i]] / bmax[pos[i]] if bmax[pos[i]] > 0 else np.nan
                      for i in order], dtype=np.float64)
    fin = ratio[np.isfinite(ratio)]
    q = np.percentile(fin, [25, 50, 75, 90]) if len(fin) else np.zeros(4)
    qlab = [f"하위25%(≤{q[0]:.3f})", f"25-50%(≤{q[1]:.3f})", f"50-75%(≤{q[2]:.3f})",
            f"75-90%(≤{q[3]:.3f})", f"상위10%(>{q[3]:.3f}) 자석 후보"]
    log(f"bg_ratio 백분위 경계 {np.round(q, 4).tolist()}")

    upd: dict[str, dict] = {}
    for i in order:
        r: dict = {}
        for kind in (unit, "word"):
            L = agg[i][kind]
            if not L:
                continue
            pk = max(L, key=lambda x: x["d_max"])
            bgp = max(L, key=lambda x: x["d_bg"])
            # 전 조각이 음수면 "빼면 오히려 올라간다" 는 뜻 → 주도 조각이 없다
            r[f"peak_{kind}"] = pk["piece"] if pk["d_max"] > EPS else "(무효과)"
            r[f"peak_{kind}_drop"] = round(pk["d_max"], 5)
            r[f"bg_{kind}"] = bgp["piece"] if bgp["d_bg"] > EPS else "(무효과)"
            r[f"bg_{kind}_drop"] = round(bgp["d_bg"], 5)
            if kind == "word":
                r["peak_word_stopword"] = "기능어(해석주의)" if pk["piece"].lower() in STOP \
                    else "내용어"
                r["bg_word_stopword"] = "기능어(해석주의)" if bgp["piece"].lower() in STOP \
                    else "내용어"
        rt = ratio[pos[i]]
        r["bg_ratio"] = None if not np.isfinite(rt) else round(float(rt), 4)
        r["bg_ratio_tier"] = "미정의" if not np.isfinite(rt) else \
            qlab[int(np.searchsorted(q, rt, side="left"))]
        # 역할 — 구조 단위가 있을 때만. 마지막 조각이 이벤트절인지는 3절 이상에서만 유효
        ps = splitter(texts[i])
        if r.get(f"peak_{unit}") and len(ps) >= 2:
            same = r[f"peak_{unit}"] == r[f"bg_{unit}"]
            hot = r["bg_ratio_tier"].startswith("상위10%")
            if r[f"peak_{unit}"] == "(무효과)":
                role = "주도 조각 없음"
            elif same and hot:
                role = "배경 자석 (같은 조각이 둘 다 주도)"
            elif hot:
                role = "배경 편향"
            elif unit == "clause" and len(ps) >= 3 and r[f"peak_{unit}"] == ps[-1]:
                role = "이벤트절 주도"
            else:
                role = "구조 조각 주도"
            r["ablate_role"] = role
        r["max_cos"] = round(float(bmax[pos[i]]), 5)
        r["bg_cos"] = round(float(bbg[pos[i]]), 5)
        r["ablate_unit"] = unit
        r["ablate_dataset"] = dsname
        upd[sid[i]] = r

    CLS = {"ablate_role", "ablate_dataset", "ablate_unit", "bg_ratio_tier",
           "peak_word_stopword", "bg_word_stopword"}
    flds = sorted({k for v in upd.values() for k in v})
    for f in flds:
        vals = {s: v[f] for s, v in upd.items() if v.get(f) is not None}
        if f in CLS:
            vals = {s: fo.Classification(label=str(x)) for s, x in vals.items()}
        pds.set_values(f, vals, key_field="id")
    from fiftyone.core.odm.dataset import ActiveFields
    af = pds.app_config.active_fields
    paths = list(af.paths) if af else ["ground_truth", "category", "adopted", "wave_role"]
    for p in sorted(CLS):
        if p not in paths and p in pds.get_field_schema():
            paths.append(p)
    pds.app_config.active_fields = ActiveFields(paths=paths, exclude=False)
    pds.save()
    log(f"{dsname}-prompts: 필드 {len(flds)}종 / {len(upd)}문장 → {flds}")
    for f in ("ablate_role", "bg_ratio_tier", "peak_word_stopword"):
        if f in pds.get_field_schema():
            log(f"  {f}: {pds.count_values(f + '.label')}")

    # 프레임으로 내리기 — 필드명을 **버전에서 파생**한다 (prefix 매칭은 다른 버전을
    # 집어올 수 있다, codex §3)
    fds = fo.load_dataset(dsname)
    wg = f"winner_gidx_{vtag(a.bank)}"
    if wg in fds.get_field_schema():
        # 여기는 **전역 gidx 그대로** — `winner_gidx_<tag>` 도 오프셋 포함 값이라 양쪽이 맞는다
        g2 = {int(gidx[i]): upd[sid[i]] for i in order}
        pc, rl = {}, {}
        for fid, g in zip(fds.values("id"), fds.values(wg)):
            r = g2.get(int(g)) if g is not None else None
            if not r:
                continue
            if r.get(f"peak_{unit}"):
                pc[fid] = r[f"peak_{unit}"]
            if r.get("ablate_role"):
                rl[fid] = fo.Classification(label=r["ablate_role"])
        fds.set_values("winner_peak_piece", pc, key_field="id")
        if rl:
            fds.set_values("winner_ablate_role", rl, key_field="id")
        af = fds.app_config.active_fields
        paths = list(af.paths) if af else []
        if rl and "winner_ablate_role" not in paths:
            paths.append("winner_ablate_role")
            fds.app_config.active_fields = ActiveFields(paths=paths, exclude=False)
        fds.save()
        log(f"{dsname}: winner_peak_piece {len(pc):,} · winner_ablate_role {len(rl):,}")
        if rl:
            log("  winner_ablate_role: " + str(fds.count_values("winner_ablate_role.label")))
    else:
        log(f"{dsname}: {wg} 없음 — 프레임 전파 생략 (attach 먼저)")
    log("완료")


if __name__ == "__main__":
    sys.exit(main())
