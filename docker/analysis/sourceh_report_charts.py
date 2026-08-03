#!/usr/bin/env python3
"""보고서 차트 재생성 — `docs/img/source-h-report/c*.png`.

원래 차트는 애드혹으로 만들어져 스크립트가 없었다. GT 재라벨(프레임 클래스 폴더 이동)이
일어나면 수치가 바뀌므로, 재현 가능하게 고정해둔다. **입력은 전부 기존 산출물**이라
분석을 다시 돌리지 않는다 (필요하면 `sourceh_prompt_geometry.py analyze/flips` 를 먼저).

    c1_recall_by_class  ← geometry.json(full) + ledger(클래스별 n)
    c2_fire_curve       ← geometry.json(marginal_curves.fire)
    c3_flip_reasons     ← flips.json(fixed_reasons)
    c4_sentence_prune   ← prune.json(sentences) — 문장별 삭제 판단 산점도

matplotlib 은 analysis 컨테이너에만 있다. docs/ 는 컨테이너에 마운트돼 있지 않으므로
`/data/fiftyone/sourceh_v2/report/` 에 쓰고 호스트로 꺼낸다:

    docker cp docker/analysis/sourceh_report_charts.py docker-analysis-1:/tmp/
    docker exec docker-analysis-1 python /tmp/sourceh_report_charts.py c1 c3
    docker cp docker-analysis-1:/data/fiftyone/sourceh_v2/report/c1_recall_by_class.png \\
        docs/img/source-h-report/
"""
from __future__ import annotations

import collections
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

WORK = "/data/fiftyone/sourceh_v2/work"
GEO = f"{WORK}/geometry"
OUT = "/data/fiftyone/sourceh_v2/report"
V0, V4 = "v1.0.8.0", "v1.0.8.4"
VERSIONS_ORDER = (V0, V4)
BLUE, ORANGE, GREEN = "#1f77b4", "#ff7f0e", "#21b384"
BG = "#fafafa"
CLASSES = ("normal", "falldown", "fire", "smoke")


def style(ax, spines=("top", "right")):
    for s in spines:
        ax.spines[s].set_visible(False)
    ax.set_facecolor(BG)
    ax.tick_params(colors="#444", labelsize=13)
    for t in ax.get_xticklabels() + ax.get_yticklabels():
        t.set_color("#444")


def geometry():
    with open(f"{GEO}/geometry.json", encoding="utf-8") as f:
        return json.load(f)


def class_counts() -> dict[str, int]:
    """클래스별 프레임 수. **원장이 아니라 scores.json** 을 센다 — 원장에는 임베딩이 없어
    채점에서 빠진 행이 남아 있어(13,146 vs 13,144) 분모가 어긋난다(실측)."""
    with open(f"{WORK}/scores.json", encoding="utf-8") as f:
        return collections.Counter(r["folder"] for r in json.load(f))


def c1() -> str:
    g = geometry()
    n = class_counts()
    total = g["n_frames"]
    a = [100 * g["full"][V0]["per_class"][c] for c in CLASSES]
    b = [100 * g["full"][V4]["per_class"][c] for c in CLASSES]
    x = range(len(CLASSES))
    fig, ax = plt.subplots(figsize=(12.92, 6.9), dpi=100, facecolor=BG)
    w = 0.42
    ax.bar([i - w / 2 for i in x], a, w, color=BLUE, label=V0)
    ax.bar([i + w / 2 for i in x], b, w, color=ORANGE, label=V4)
    for i, (p, q) in enumerate(zip(a, b)):
        ax.text(i - w / 2, p + 1.2, f"{p:.1f}%", ha="center", fontsize=13, color="#555")
        ax.text(i + w / 2, q + 1.2, f"{q:.1f}%", ha="center", fontsize=13, color="#555")
        ax.text(i, max(p, q) + 6.5, f"{q - p:+.1f}pp", ha="center", fontsize=14,
                fontweight="bold", color="#222")
    ax.set_title(f"Per-class recall — {V0} vs {V4} ({total:,} relabeled frames)",
                 fontsize=17, color="#222", loc="left", pad=18)
    ax.set_xticks(list(x))
    ax.set_xticklabels([f"{c}\n(n={n[c]:,})" for c in CLASSES], fontsize=14)
    ax.set_yticks(range(0, 101, 20))
    ax.set_yticklabels([f"{v}%" for v in range(0, 101, 20)])
    ax.set_ylim(0, 112)
    ax.grid(axis="y", color="#e6e6e6", zorder=0)
    ax.set_axisbelow(True)
    # 막대 위에 겹쳐도 읽히게 배경 있는 프레임 (frameon=False 면 파란 라벨이 파란 막대에 묻힘)
    ax.legend(loc="lower left", fontsize=14, frameon=True, facecolor="white",
              edgecolor="#dddddd", framealpha=0.95, labelcolor=[BLUE, ORANGE])
    style(ax)
    return save(fig, "c1_recall_by_class.png")


def c2() -> str:
    g = geometry()
    cur = g["marginal_curves"]["fire"]
    full0 = 100 * g["full"][V0]["per_class"]["fire"]
    fig, ax = plt.subplots(figsize=(12.92, 6.9), dpi=100, facecolor=BG)
    # ⚠️ x 는 **로그 스케일 + 실제 개수**. 인덱스 위치로 그리면 두 버전의 size 목록이 달라
    #    (v080 은 573 에서 끝남) 마지막 점이 엉뚱한 눈금(800)에 붙는다.
    ax.set_xscale("log")
    for v, col in ((V0, BLUE), (V4, ORANGE)):
        pts = cur[v]
        xs = [p["size"] for p in pts]
        ys = [100 * p["recall_mean"] for p in pts]
        sd = [100 * p["recall_std"] for p in pts]
        ax.plot(xs, ys, "o-", color=col, label=v, lw=2.5, ms=7)
        ax.fill_between(xs, [y - s for y, s in zip(ys, sd)],
                        [y + s for y, s in zip(ys, sd)], color=col, alpha=0.15)
        ax.text(xs[-1] * 1.03, ys[-1], v, color=col, fontsize=15, fontweight="bold",
                va="center")
    sizes = sorted({p["size"] for v in VERSIONS_ORDER for p in cur[v]})
    ax.axhline(full0, ls="--", lw=1.2, color=BLUE, alpha=0.7)
    ax.text(sizes[0] * 1.05, full0 + 1.5,
            f"{V0} full bank ({cur[V0][-1]['size']} prompts) = {full0:.1f}%",
            fontsize=13, color="#555")
    ax.set_xticks(sizes)
    ax.set_xticklabels([f"{s:,}" if s >= 1000 else str(s) for s in sizes], fontsize=13)
    ax.minorticks_off()
    ax.set_title(f"Fire recall vs prompt count — {cur[V4][0]['size']} new sentences "
                 f"beat {cur[V0][-1]['size']} old ones", fontsize=17, color="#222",
                 loc="left", pad=18)
    ax.set_xlabel("fire prompts in bank (others fixed, 10 seeds ± sd)", fontsize=14,
                  color="#444")
    ax.set_ylabel("fire recall", fontsize=13, color="#444")
    ax.set_yticks(range(0, 81, 10))
    ax.set_yticklabels([f"{v}%" for v in range(0, 81, 10)])
    ax.grid(color="#e6e6e6")
    ax.set_axisbelow(True)
    ax.legend(loc="center right", fontsize=14, frameon=False)
    style(ax)
    return save(fig, "c2_fire_curve.png")


def c3() -> str:
    with open(f"{GEO}/flips.json", encoding="utf-8") as f:
        fl = json.load(f)
    label_en = {
        "자기접근+경쟁소거": "own approach + rival removed",
        "경쟁문장 소거": "rival sentence removed",
        "자기문장 접근": "own sentence approached",
        "재배열(미세)": "micro reordering",
    }
    items = sorted(fl["fixed_reasons"].items(), key=lambda kv: kv[1], reverse=True)
    names = [label_en.get(k, k) for k, _ in items]
    vals = [v for _, v in items]
    fig, ax = plt.subplots(figsize=(12.92, 5.2), dpi=100, facecolor=BG)
    y = range(len(names))
    ax.barh(list(y), vals, height=0.55, color=GREEN)
    for i, v in enumerate(vals):
        ax.text(v + max(vals) * 0.012, i, f"{v:,}", va="center", fontsize=15, color="#222")
    ax.set_yticks(list(y))
    ax.set_yticklabels(names, fontsize=15)
    ax.invert_yaxis()
    ax.set_xlim(0, max(vals) * 1.15)
    ax.set_title(f"Why {fl['counts']['오탐→정탐']:,} frames flipped wrong→correct "
                 "(rel-score decomposition)", fontsize=17, color="#222", loc="right", pad=18)
    ax.grid(axis="x", color="#e6e6e6")
    ax.set_axisbelow(True)
    style(ax, spines=("top", "right", "left"))
    return save(fig, "c3_flip_reasons.png")


def c4() -> str:
    """문장 단위 산점도 — x=승수(log), y=선언클래스 순도, 크기=|LOO 제거이득|.

    이미지 UMAP 을 승자 문장으로 칠하는 안은 실측으로 기각됐다(공간 분산 ↔ 제거이득
    spearman +0.13/−0.10 무상관, 나쁜 문장이 오히려 조밀). 판단해야 할 두 축을 그대로 그린다.
    **우하단(많이 가져가는데 선언클래스가 틀림) = 우선 삭제 후보.**
    """
    with open(f"{GEO}/prune.json", encoding="utf-8") as f:
        pr = json.load(f)
    versions = [v for v in VERSIONS_ORDER if v in pr] or list(pr)
    col = dict(zip(CLASSES, ("#8c8c8c", "#9467bd", "#d62728", ORANGE)))
    fig, axes = plt.subplots(1, len(versions), figsize=(12.92, 6.0), dpi=100,
                             facecolor=BG, sharey=True)
    axes = axes if len(versions) > 1 else [axes]
    for ax, v in zip(axes, versions):
        rows = pr[v]["sentences"]
        ax.set_xscale("log")
        for cname in CLASSES:
            rs = [r for r in rows if r["cls_name"] == cname]
            if not rs:
                continue
            ax.scatter([r["wins"] for r in rs], [100 * r["purity"] for r in rs],
                       s=[18 + 9 * abs(r["loo_gain"]) for r in rs], alpha=0.55,
                       color=col[cname], edgecolors="none", label=cname)
        # 삭제 상위 2개만 문장을 붙인다 (3개 이상이면 서로 겹쳐 못 읽는다).
        # 점이 오른쪽에 있으면 라벨을 왼쪽으로 뻗어 패널 밖으로 나가지 않게 한다.
        for k, r in enumerate(sorted(rows, key=lambda r: -r["loo_gain"])[:2]):
            if r["loo_gain"] <= 0:
                break
            right = r["wins"] > 60
            ax.annotate(f"+{r['loo_gain']} {r['text'][:30]}…",
                        (r["wins"], 100 * r["purity"]), fontsize=10, color="#222",
                        ha="right" if right else "left",
                        xytext=(-8 if right else 8, 14 if k == 0 else -18),
                        textcoords="offset points",
                        bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="#ddd", alpha=0.9))
        ax.axhspan(0, 50, color="#d62728", alpha=0.05)
        # ⚠️ 컨테이너 matplotlib 에 한글 글리프가 없다 (DejaVu Sans) — 차트 안 문자열은 영문만
        h = pr[v].get("holdout", {})
        held = (f", held-out {h['b_gain_pp']:+.1f}pp" if "b_gain_pp" in h else "")
        ax.set_title(f"{v} — {pr[v]['n_winners']} winners, {pr[v]['n_harmful']} net-harmful\n"
                     f"greedy prune {pr[v]['n_dropped']} sents → {pr[v]['total_gain']:+,} frames"
                     f"{held}",
                     fontsize=13, color="#222", loc="left", pad=10)
        ax.set_xlabel("frames won (log)", fontsize=13, color="#444")
        ax.grid(color="#e6e6e6")
        ax.set_axisbelow(True)
        style(ax)
    axes[0].set_ylabel("declared-class purity", fontsize=13, color="#444")
    axes[0].set_ylim(-6, 106)
    axes[0].set_yticks(range(0, 101, 20))
    axes[0].set_yticklabels([f"{v}%" for v in range(0, 101, 20)])
    # 범례는 좌패널 중앙-왼쪽 빈 구역 (승수 1~5 · 순도 20~90% 는 거의 비어 있다)
    axes[0].legend(loc="center left", fontsize=12, frameon=True, facecolor="white",
                   edgecolor="#dddddd", framealpha=0.92, markerscale=0.55,
                   labelspacing=0.35, borderpad=0.5)
    fig.suptitle("Which sentences to delete — bottom-right = takes many frames, declares wrong class",
                 fontsize=16, color="#222", x=0.006, ha="left", y=0.995)
    return save(fig, "c4_sentence_prune.png")


# ───────────────── s1~s5: FiftyOne 화면 대체 렌더 ─────────────────
# 원래 s1~s4 는 FiftyOne 앱 스크린샷이었다. 앱 상태는 URL 로 주소지정되지 않아
# (`?workspace=`/`?view=` 는 1.19 에서 TypeError, 한글 뷰명은 slug 가 비어 깨진다)
# 브라우저 캡처가 GT 변경마다 재현되지 않았다. 같은 정보를 산출물에서 직접 그린다.
FRAMES = "/data/fiftyone/sourceh_v2/frames"
GT_COL = {"normal": "#8c8c8c", "falldown": "#9467bd", "fire": "#d62728", "smoke": ORANGE}


def scores() -> list[dict]:
    with open(f"{WORK}/scores.json", encoding="utf-8") as f:
        return json.load(f)


def margin_gt(rec: dict, ver: str) -> float:
    """정답 클래스 기준 마진 = cos[GT] − max(cos[다른 클래스]). >0 이면 그 버전이 맞춘 것.

    FiftyOne 의 `margin_v080/v084` 와 같은 양이며, 부호가 곧 정오답이라 사분면이 판정이 된다.
    """
    cb = rec[ver]["class_best"]
    g = str(rec["gt_class"])
    other = max(v["cos"] for k, v in cb.items() if k != g)
    return cb[g]["cos"] - other


def flip_of(rec: dict) -> str:
    ok0, ok4 = rec[V0]["correct"], rec[V4]["correct"]
    return ("both correct" if ok0 and ok4 else "wrong→correct" if ok4
            else "correct→wrong" if ok0 else "both wrong")


def _grid(recs: list[dict], cols: int, rows: int, title: str, name: str,
          caption=None) -> str:
    """프레임 썸네일 격자. caption(rec) 이 있으면 각 칸 아래 한 줄 (영문만)."""
    from PIL import Image

    recs = recs[:cols * rows]
    fig, axes = plt.subplots(rows, cols, figsize=(cols * 2.6, rows * 1.72), dpi=110,
                             facecolor=BG)
    for ax, rec in zip(axes.ravel(), recs):
        im = Image.open(f"{FRAMES}/{rec['folder']}/{rec['name']}")
        im.thumbnail((420, 420))
        ax.imshow(im)
        if caption:
            ax.set_title(caption(rec), fontsize=7.5, color="#444", pad=2)
    for ax in axes.ravel():
        ax.set_xticks([])
        ax.set_yticks([])
        for s in ax.spines.values():
            s.set_color("#dddddd")
    for ax in axes.ravel()[len(recs):]:
        ax.set_visible(False)
    fig.suptitle(title, fontsize=15, color="#222", x=0.006, ha="left", y=0.998)
    return save(fig, name)


def s1() -> str:
    """판정 사분면 — x=v080 정답마진, y=v084 정답마진. 사분면이 곧 판정."""
    S = scores()
    pts = collections.defaultdict(lambda: ([], []))
    for r in S:
        xs, ys = pts[flip_of(r)]
        xs.append(margin_gt(r, V0))
        ys.append(margin_gt(r, V4))
    order = (("both correct", "#c9c9c9", 3), ("both wrong", "#6b6b6b", 5),
             ("wrong→correct", GREEN, 7), ("correct→wrong", "#d62728", 7))
    fig, ax = plt.subplots(figsize=(9.6, 9.0), dpi=110, facecolor=BG)
    for lab, col, sz in order:
        xs, ys = pts[lab]
        ax.scatter(xs, ys, s=sz, c=col, alpha=0.55, edgecolors="none",
                   label=f"{lab} ({len(xs):,})")
    ax.axhline(0, color="#888", lw=1)
    ax.axvline(0, color="#888", lw=1)
    lim = max(abs(v) for xs, ys in pts.values() for v in xs + ys) * 1.05
    ax.set_xlim(-lim, lim)
    ax.set_ylim(-lim, lim)
    # 아래쪽 라벨은 축 바로 밑에 붙인다 — 사분면 밑단은 범례가 차지한다
    for tx, ty, t in ((-lim * 0.97, lim * 0.93, "v084 only → wrong→correct"),
                      (lim * 0.03, lim * 0.93, "both correct"),
                      (-lim * 0.97, -lim * 0.06, "both wrong"),
                      (lim * 0.03, -lim * 0.06, "v080 only → correct→wrong")):
        ax.text(tx, ty, t, fontsize=11, color="#666")
    net = len(pts["wrong→correct"][0]) - len(pts["correct→wrong"][0])
    ax.set_title(f"Verdict quadrant — GT-class margin per bank (net {net:+,} frames)",
                 fontsize=16, color="#222", loc="left", pad=14)
    ax.set_xlabel(f"{V0} margin  (cos[GT] − best other)", fontsize=13, color="#444")
    ax.set_ylabel(f"{V4} margin  (cos[GT] − best other)", fontsize=13, color="#444")
    ax.grid(color="#ececec")
    ax.set_axisbelow(True)
    ax.legend(loc="lower right", fontsize=12, frameon=True, facecolor="white",
              edgecolor="#dddddd", framealpha=0.95, markerscale=2.4)
    style(ax)
    return save(fig, "s1_margin_quadrant.png")


def s2() -> str:
    """오탐→정탐 프레임 — 개선 폭 큰 순 (뷰 30_fixed 에 대응)."""
    S = [r for r in scores() if flip_of(r) == "wrong→correct"]
    S.sort(key=lambda r: -(margin_gt(r, V4) - margin_gt(r, V0)))
    return _grid(S, 8, 5, f"wrong→correct ({len(S):,} frames) — top 40 by margin gain",
                 "s2_fixed_grid.png",
                 caption=lambda r: f"{r['folder']}  +{margin_gt(r, V4) - margin_gt(r, V0):.3f}")


def s3() -> str:
    """smoke 미검출 — 가장 아깝게 놓친 순 (뷰 07_gap_smoke 에 대응)."""
    S = [r for r in scores() if r["folder"] == "smoke" and not r[V4]["correct"]]
    S.sort(key=lambda r: -margin_gt(r, V4))   # 마진이 0 에 가까운 = 아깝게 놓친
    return _grid(S, 8, 5, f"smoke missed by {V4} ({len(S):,} frames) — top 40 nearest misses",
                 "s3_gap_smoke.png",
                 caption=lambda r: f"→{CLASSES[r[V4]['pred']]}  {margin_gt(r, V4):.3f}")


def s4() -> str:
    """전체 프레임 임베딩 지도 (GT 색). emb_viz brain run 의 2D 좌표를 그대로 쓴다."""
    import fiftyone as fo
    import fiftyone.brain  # noqa: F401  (brain run 로드에 필요)

    ds = fo.load_dataset("source-h")
    res = ds.load_brain_results("emb_viz")
    labels = ds.values("ground_truth.label")
    xy = res.points
    fig, ax = plt.subplots(figsize=(10.4, 9.0), dpi=110, facecolor=BG)
    for cname in CLASSES:
        idx = [i for i, lb in enumerate(labels) if lb == cname]
        if not idx:
            continue
        ax.scatter([xy[i][0] for i in idx], [xy[i][1] for i in idx], s=4,
                   c=GT_COL[cname], alpha=0.55, edgecolors="none",
                   label=f"{cname} ({len(idx):,})")
    ax.set_title(f"Embedding map of all {len(labels):,} frames (colour = corrected GT)",
                 fontsize=16, color="#222", loc="left", pad=14)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.legend(loc="upper right", fontsize=12, frameon=True, facecolor="white",
              edgecolor="#dddddd", framealpha=0.95, markerscale=3.2)
    style(ax, spines=("top", "right", "left", "bottom"))
    return save(fig, "s4_data_map.png")


def s5() -> str:
    """falldown 오탐 코호트 — 정정 GT 로 처음 보이는 축 (뷰 32_falldown_fp)."""
    S = [r for r in scores() if r[V4]["pred"] == 1 and r["gt_class"] != 1]
    S.sort(key=lambda r: margin_gt(r, V4))
    return _grid(S, 5, 3,
                 f"{V4} falldown false positives ({len(S)} frames) — GT is not falldown",
                 "s5_falldown_fp.png",
                 caption=lambda r: f"GT {r['folder']}  margin {margin_gt(r, V4):.3f}")


def save(fig, name: str) -> str:
    os.makedirs(OUT, exist_ok=True)
    p = f"{OUT}/{name}"
    fig.tight_layout()
    fig.savefig(p, facecolor=BG)
    plt.close(fig)
    print(f"[chart] {p}")
    return p


def demo() -> None:
    """margin_gt 의 부호가 정오답과 일치해야 한다 — 사분면 해석의 전제."""
    rec = {"gt_class": 1, V0: {"class_best": {"0": {"cos": 0.30}, "1": {"cos": 0.32},
                                              "2": {"cos": 0.10}, "3": {"cos": 0.10}}},
           V4: {"class_best": {"0": {"cos": 0.33}, "1": {"cos": 0.31},
                               "2": {"cos": 0.10}, "3": {"cos": 0.10}}}}
    assert abs(margin_gt(rec, V0) - 0.02) < 1e-9, margin_gt(rec, V0)
    assert margin_gt(rec, V4) < 0, "GT 가 1위가 아니면 마진은 음수여야 한다"
    rec[V0]["correct"], rec[V4]["correct"] = True, False
    assert flip_of(rec) == "correct→wrong"
    print("demo ok")


if __name__ == "__main__":
    table = {"c1": c1, "c2": c2, "c3": c3, "c4": c4,
             "s1": s1, "s2": s2, "s3": s3, "s4": s4, "s5": s5}
    args = sys.argv[1:]
    if "--selftest" in args:
        demo()
    else:
        for k in (args or list(table)):
            table[k]()
