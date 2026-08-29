#!/usr/bin/env python3
"""sourcei GT 검증에 쓴 값들을 전부 CSV 로 내보낸다 (사람이 눈으로 보고 재분석하라고).

차트가 보여준 수치의 원본이다. 차트 스크립트(sourcei_gt_charts.py)와 **같은 식**으로 다시 계산하고,
끝에서 summary.json 과 대조해 어긋나면 죽는다 — 표와 그림이 다른 숫자를 말하는 사고를 막는 유일한 장치다.

인코딩은 utf-8-sig: Excel 이 BOM 없으면 한글 헤더를 깨뜨린다.
헤더는 `이름(한글)` 이중 표기 — 스크립트로 다시 읽을 때는 이름 부분만 쓰면 된다.
"""
import csv, json, os, glob
import numpy as np

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CSVD = f"{OUT}/csv"; os.makedirs(CSVD, exist_ok=True)
CLASSES = ["normal", "falldown", "fire", "smoke"]
RULES = ["argmax", "topk", "wave"]
RN = {"argmax": "argmax(top-1)", "topk": "top-K투표(K=10)", "wave": "분포-IoU(제품)"}

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
m = json.load(open(f"{OUT}/metrics.json"))
S = json.load(open(f"{OUT}/summary.json"))
meta = np.load(f"{OUT}/frame_meta.npz", allow_pickle=True)
sent = json.load(open(f"{OUT}/sentences.json"))
L = np.load(f"{OUT}/sentence_ledger.npz", allow_pickle=True)
gt, cam, src, unit, ids = d["gt"], d["camera"], d["gt_source"], d["unit"], d["ids"]
fie = meta["frame_in_event"].astype(int); ekind = meta["event_kind"]


def vkey(b):
    return tuple(int(x) for x in b.lstrip("vV").split("."))


banks = sorted([b for b in d["banks"] if not b.startswith("v2.")], key=vkey)
extra = [b for b in banks if len(m["banks"][b]["classes"]) != 4]   # 이름이 아니라 classes 필드로 판정
FILES = []


def macro_f1(pred, g, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = ((pred == c) & (g == c)).sum(); fp = ((pred == c) & (g != c)).sum(); fn = ((pred != c) & (g == c)).sum()
        p = tp / max(tp + fp, 1); r = tp / max(tp + fn, 1); f.append(2 * p * r / max(p + r, 1e-12))
    return float(np.mean(f))


def w(name, desc, header, rows, highlight=""):
    with open(f"{CSVD}/{name}", "w", newline="", encoding="utf-8-sig") as f:
        cw = csv.writer(f); cw.writerow(header); cw.writerows(rows)
    FILES.append((name, desc, len(rows), highlight))
    print(f"  {name}: {len(rows)}행")


def r4(x):
    return "" if x is None or (isinstance(x, float) and np.isnan(x)) else round(float(x), 4)


# ── 01 뱅크 × 규칙 요약 ────────────────────────────────────────────────
hdr = ["bank(뱅크)", "rule(규칙)", "n_sentences(문장수)", "n_classes(클래스수)", "classes(클래스목록)",
       "accuracy(정확도)", "macro_f1_event(이벤트3클래스macroF1)", "macro_f1_all(4클래스macroF1)",
       "agree_topk_wave(topK↔IoU일치율)", "agree_topk_argmax(topK↔argmax)", "agree_wave_argmax(IoU↔argmax)"]
for c in CLASSES:
    hdr += [f"{c}_precision(정밀도)", f"{c}_recall(재현율)", f"{c}_f1", f"{c}_n_pred(예측수)", f"{c}_n_gt(GT수)"]
rows = []
for b in banks:
    for r in RULES:
        e = m["banks"][b]["rules"][r]; a = m["banks"][b]["agree"]
        cls = m["banks"][b]["classes"]
        row = [b, RN[r], m["banks"][b]["n_sent"], len(cls), ",".join(cls),
               r4(e["acc"]), r4(e["macro_f1_ev"]), r4(e["macro_f1_all"]),
               r4(a["tw"]), r4(a["ta"]), r4(a["wa"])]
        for c in CLASSES:
            pc = e["per_class"][c]
            row += [r4(pc["p"]), r4(pc["r"]), r4(pc["f1"]), pc["n_pred"], pc["n_gt"]]
        rows.append(row)
w("01_bank_rule_summary.csv", "뱅크 31종 × 규칙 3종 전체 지표 (그림 1·2·6의 원본)", hdr, rows,
  "규칙별 macro-F1 상위3 초록 / 하위3 빨강, 재현율<0.15 빨강, 기준선 v1.0.8.0 볼드")

# ── 02 클래스 재현율 wide (그림 2b) ────────────────────────────────────
hdr = ["bank(뱅크)"] + [f"{r}_{c}" for r in RULES for c in CLASSES]
rows = [[b] + [r4(m["banks"][b]["rules"][r]["per_class"][c]["r"]) for r in RULES for c in CLASSES] for b in banks]
w("02_bank_class_recall.csv", "뱅크 × (규칙×클래스) 재현율 — 그림 2b 히트맵 원본", hdr, rows,
  "0.15 미만 빨강, 0.5 이상 초록 (분포-IoU smoke 열이 통째로 빨강인지 확인용)")

# ── 03 혼동 비율 (그림 5) ──────────────────────────────────────────────
cols = [(1, 0, "falldown→normal"), (2, 0, "fire→normal"), (3, 0, "smoke→normal"),
        (2, 3, "fire→smoke"), (3, 2, "smoke→fire")]
hdr = ["bank(뱅크)", "rule(규칙)"] + [c[2] for c in cols] + ["normal→event(정상오탐)"]
rows = []
for b in banks:
    for r in RULES:
        p = d[f"{r}__{b}"]
        rows.append([b, RN[r]] + [r4((p[gt == g] == q).mean()) for g, q, _ in cols] + [r4((p[gt == 0] > 0).mean())])
w("03_confusion_rates.csv", "행 정규화 혼동 비율 — 그림 5 원본 (오류가 어디로 새는지)", hdr, rows,
  "→normal 누락 0.8 이상 빨강, normal 오탐 0.1 이상 주황")

# ── 04 IoU 임계 스윕 (그림 8) ──────────────────────────────────────────
thrs = np.round(np.arange(0.05, 0.61, 0.025), 3)
Smf = np.zeros((len(banks), len(thrs))); Sacc = np.zeros_like(Smf)
for i, b in enumerate(banks):
    I = d[f"iou__{b}"].astype(np.float32)
    for j, t in enumerate(thrs):
        pred = np.where((I < t).any(1), I.argmin(1) + 1, 0)
        Smf[i, j] = macro_f1(pred, gt); Sacc[i, j] = (pred == gt).mean()
best_j = Smf.argmax(1)
hdr = (["bank(뱅크)", "best_thr(최적임계)", "best_macro_f1(최적macroF1)", "macro_f1_at_0.15(제품임계)",
        "topk_macro_f1(비교:topK)", "beats_topk(최적임계가topK를넘나)"] +
       [f"mf1@{t:g}" for t in thrs] + [f"acc@{t:g}" for t in thrs])
rows = []
for i, b in enumerate(banks):
    tk = m["banks"][b]["rules"]["topk"]["macro_f1_ev"]
    rows.append([b, float(thrs[best_j[i]]), r4(Smf[i, best_j[i]]), r4(Smf[i, list(thrs).index(0.15)]), r4(tk),
                 "Y" if Smf[i].max() > tk else "N"] +
                [r4(x) for x in Smf[i]] + [r4(x) for x in Sacc[i]])
w("04_iou_threshold_sweep.csv", "분포-IoU 임계 0.05~0.60 스윕 — 그림 8 원본 (제품 0.15 가 틀린 상수임을 보이는 표)", hdr, rows,
  "뱅크별 최고 mf1 셀 초록, mf1@0.15 열 빨강 배경, beats_topk=N 인 뱅크 주황")

# ── 05 카메라 홀드아웃 (그림 8b) ───────────────────────────────────────
cams_all = np.unique(cam)
hdr = ["bank(뱅크)", "fold(폴드)", "n_test(테스트프레임)", "thr_from_train(학습카메라에서고른임계)",
       "iou_tuned(튠임계macroF1)", "iou_at_0.15(제품임계)", "topk", "argmax", "tuned_beats_topk"]
rows = []
for b in banks:
    I = d[f"iou__{b}"].astype(np.float32)
    for fold in (0, 1):
        tr = np.isin(cam, cams_all[fold::2]); te = ~tr
        f1_at = lambda t, s: macro_f1(np.where((I[s] < t).any(1), I[s].argmin(1) + 1, 0), gt[s])
        tb = float(thrs[int(np.argmax([f1_at(t, tr) for t in thrs]))])
        tuned, tk = f1_at(tb, te), macro_f1(d[f"topk__{b}"][te], gt[te])
        rows.append([b, fold, int(te.sum()), tb, r4(tuned), r4(f1_at(0.15, te)), r4(tk),
                     r4(macro_f1(d[f"argmax__{b}"][te], gt[te])), "Y" if tuned > tk else "N"])
w("05_iou_holdout.csv", "카메라 반분 홀드아웃 — 그림 8b 원본 (임계를 GT 로 고른 낙관 편향 제거)", hdr, rows,
  "tuned_beats_topk=Y 초록 / N 빨강")

# ── 06 클래스 오프셋 α 스윕 (그림 9) ───────────────────────────────────
alphas = np.round(np.linspace(0, 1, 11), 2)
pbanks = [b for b in banks if os.path.exists(f"{OUT}/percls_{b}.npy")]
hdr = ["bank(뱅크)", "alpha(오프셋강도)", "macro_f1", "accuracy(정확도)",
       "fire_recall(fire재현율)", "normal_recall(normal재현율)", "is_best_alpha(뱅크별최적)"]
rows = []
for b in pbanks:
    per = np.load(f"{OUT}/percls_{b}.npy"); vals = []
    for a in alphas:
        f1s, accs, fr, nr = [], [], [], []
        for fold in (0, 1):
            tr = np.isin(cam, cams_all[fold::2]); te = ~tr
            off = per[tr].mean(0) - per[tr].mean(0)[0]
            pred = (per[te] - a * off).argmax(1)
            f1s.append(macro_f1(pred, gt[te])); accs.append((pred == gt[te]).mean())
            fr.append((pred[gt[te] == 2] == 2).mean()); nr.append((pred[gt[te] == 0] == 0).mean())
        vals.append([np.mean(f1s), np.mean(accs), np.mean(fr), np.mean(nr)])
    bi = int(np.argmax([v[0] for v in vals]))
    for j, a in enumerate(alphas):
        rows.append([b, float(a)] + [r4(x) for x in vals[j]] + ["Y" if j == bi else ""])
w("06_offset_alpha_sweep.csv", "클래스 오프셋(z-보정) 강도 α 스윕 — 그림 9 원본 (α=1 전량 차감이 왜 위험한지)", hdr, rows,
  "is_best_alpha=Y 초록, α=1 행 빨강(정확도 붕괴 확인)")

# ── 07 argmax 마진 구간 (그림 7) ───────────────────────────────────────
edges = [0, .005, .01, .02, .03, .05, .08, .2]
bins = [f"{lo:g}~{hi:g}" for lo, hi in zip(edges[:-1], edges[1:])]
hdr = ["bank(뱅크)"] + [f"acc[{x}]" for x in bins] + [f"n[{x}]" for x in bins]
rows = []
for b in banks:
    mg = d[f"margin__{b}"].astype(float); ok = d[f"argmax__{b}"] == gt
    accs, ns = [], []
    for lo, hi in zip(edges[:-1], edges[1:]):
        sel = (mg >= lo) & (mg < hi); ns.append(int(sel.sum()))
        accs.append(r4(ok[sel].mean()) if sel.sum() >= 30 else "")
    rows.append([b] + accs + ns)
w("07_margin_bins.csv", "argmax 결정 마진 구간별 정확도 — 그림 7 원본 (마진이 신뢰도 게이트가 아님)", hdr, rows,
  "행 최저 정확도 빨강, 노션 기준 0.02~0.03 열 볼드(이 열이 최저면 비단조)")

# ── 08 카메라 × 뱅크 (그림 11) ─────────────────────────────────────────
elig = [c for c in cams_all if ((cam == c).sum() >= 100) and len(np.unique(gt[cam == c])) >= 2]
hdr = ["camera(카메라)", "n_frames(프레임수)"] + [f"gt_{c}" for c in CLASSES] + ["best_bank(1위뱅크)"] + banks
rows = []
for c in elig:
    s = cam == c; cls = tuple(int(x) for x in np.unique(gt[s]) if x > 0)
    vals = [macro_f1(d[f"topk__{b}"][s], gt[s], classes=cls) for b in banks]
    rows.append([c, int(s.sum())] + [int((gt[s] == i).sum()) for i in range(4)] +
                [banks[int(np.argmax(vals))]] + [r4(v) for v in vals])
w("08_camera_bank_matrix.csv", "카메라 × 뱅크 top-K macro-F1 — 그림 11 원본 (현장 특이성)", hdr, rows,
  "행별 최고 초록·최저 빨강 (카메라마다 1위 뱅크가 다른지 확인)")

# ── 09 frames(GT 없음) vs sourcei GT (그림 3·12) ───────────────────────
fe = {}
for l in open("/workspace/.cron_logs/frames_rule_env.tsv"):
    p = l.strip().split("|")
    fe[p[0]] = dict(n=int(p[1]), topk=int(p[2]), wave=int(p[3]), argmax=int(p[4]),
                    tw=float(p[5]) / 100, ta=float(p[6]) / 100, wa=float(p[7]) / 100)
hdr = (["bank(뱅크)", "n_sentences(문장수)", "frames_n(21현장프레임)"] +
       [f"frames_eventrate_{r}(발화율%)" for r in RULES] +
       [f"gt_macro_f1_{r}" for r in RULES] + [f"gt_acc_{r}" for r in RULES] +
       ["frames_agree_tw", "sourcei_agree_tw", "frames_agree_wa", "sourcei_agree_wa"])
rows = []
for b in banks:
    f = fe[b]; a = m["banks"][b]["agree"]
    rows.append([b, m["banks"][b]["n_sent"], f["n"]] +
                [r4(f[r] / f["n"] * 100) for r in RULES] +
                [r4(m["banks"][b]["rules"][r]["macro_f1_ev"]) for r in RULES] +
                [r4(m["banks"][b]["rules"][r]["acc"]) for r in RULES] +
                [r4(f["tw"]), r4(a["tw"]), r4(f["wa"]), r4(a["wa"])])
w("09_frames_vs_gt.csv", "GT 없는 frames 발화율 vs sourcei GT 성능 — 그림 3·12 원본 (발화율이 역지표인 증거)", hdr, rows,
  "발화율 상위5 주황, GT macro-F1 상위3 초록·하위3 빨강 (둘이 엇갈리는지)")

# ── 10 GT 출처·이벤트 내 위치 (그림 10) ────────────────────────────────
srcs = ["caption", "filename", "folder", "none"]
pbins = [1, 2, 3, 5, 10, 20, 50, 100, 1000]
plab = [f"{lo}~{hi - 1}" if hi - 1 > lo else f"{lo}" for lo, hi in zip(pbins[:-1], pbins[1:])]
hdr = ["bank(뱅크)"] + [f"acc_{s}(n={int((src == s).sum())})" for s in srcs] + [f"falldown_recall[{x}]" for x in plab]
rows = []
sel_f = (gt == 1) & (fie > 0)
for b in banks:
    p = d[f"topk__{b}"]; rec = []
    for lo, hi in zip(pbins[:-1], pbins[1:]):
        s = sel_f & (fie >= lo) & (fie < hi)
        rec.append(r4((p[s] == 1).mean()) if s.sum() >= 20 else "")
    rows.append([b] + [r4(m["banks"][b]["rules"]["topk"]["per_gt_source"][s]) for s in srcs] + rec)
w("10_gt_source_and_position.csv", "GT 출처별 정확도 + 낙상 이벤트 내 프레임 순번별 재현율 — 그림 10 원본 (GT 잡음 진단)", hdr, rows,
  "folder 출처 열 빨강(0.2 미만), 순번 50~99 열 빨강 (윈도우 라벨 증거)")

# ── 11 문장 원장 (그림 13) ─────────────────────────────────────────────
hit, trap, lab, ncam, txt = L["hit"], L["trap"], L["lab"], L["n_cams"], L["text"]
tbg = L["trap_by_gt"]
act = np.where((hit + trap) > 0)[0]
order = act[np.lexsort((-(hit[act] + trap[act]), lab[act]))]
hdr = (["class(문장클래스)", "text(문장)", "hit(정답프레임끌어당김)", "trap(다른클래스가로챔)",
        "selectivity(선택도)", "appear(등장수)", "n_cameras(카메라수)"] +
       [f"trap_from_{c}(이클래스를가로챔)" for c in CLASSES])
rows = [[CLASSES[lab[j]], str(txt[j]), int(hit[j]), int(trap[j]),
         r4(hit[j] / max(hit[j] + trap[j], 1)), int(hit[j] + trap[j]), int(ncam[j])] +
        [int(tbg[j, g]) for g in range(4)] for j in order]
w("11_sentences_hit_trap.csv", "문장 단위 hit/trap 원장 (top-10 에 한 번이라도 든 문장 전부) — 그림 13 원본", hdr, rows,
  "선택도 0.9 이상 & hit≥50 초록(쓸 문장), 선택도 0.3 미만 & trap≥50 빨강(뺄 문장)")

# ── 12 구문 대조 (그림 14) ─────────────────────────────────────────────
hdr = ["class(클래스)", "phrase(구문)", "hit", "trap", "selectivity(선택도)",
       "class_base(클래스기준선)", "delta(기준선대비)", "verdict(판정)"]
rows = []
for c in CLASSES:
    P = sent["phrases"][c]; base = P["base_sel"]
    seen = set()
    for g, h, t, s in P["white"] + P["black"]:
        if g in seen: continue
        seen.add(g)
        rows.append([c, g, int(h), int(t), r4(s), r4(base), r4(s - base), "넣을구문" if s >= base else "피할구문"])
w("12_phrase_contrast.csv", "구문(1~3-gram) 선택도 대조 — 그림 14 원본 (프롬프트 화이트/블랙리스트 근거)", hdr, rows,
  "delta 양수 초록 / 음수 빨강, |delta|>0.3 볼드")

# ── 13 템플릿 유형 + 문장 집중도 ───────────────────────────────────────
ss = json.load(open(f"{OUT}/sentence_summary.json"))
hdr = ["class(클래스)", "template(템플릿유형)", "selectivity(선택도)", "n_sentences(문장수)"]
rows = [[c, k, (r4(v) if v is not None else ""), ss["template_n"][c][k]] for c, kv in ss["template_sel"].items() for k, v in kv.items()]   # n=0 → 공란 (0.000 이 아니다)
w("13_template_selectivity.csv", "문장 템플릿 유형별 선택도 — 그림 15 우측 원본 (어떤 문형을 쓸지)", hdr, rows,
  "클래스별 최고 초록 / 최저 빨강 (normal=카메라서술, falldown=인물선행이 이기는지)")
hdr = ["class(클래스)", "gt_frames(GT프레임)", "n_active(등장문장)", "n_for_50pct_hits(hit절반을내는문장수)",
       "n_for_90pct_hits(hit90%)", "share_50pct(절반을내는비율%)"]
rows = [[c, sent["per_class"][c]["gt_frames"], v["n_active"], v["n_for_50pct_hits"], v["n_for_90pct_hits"],
         r4(100 * v["n_for_50pct_hits"] / max(v["n_active"], 1))] for c, v in ss["hit_concentration"].items()]
w("13b_hit_concentration.csv", "클래스별 문장 집중도 — 'hit 의 절반을 몇 문장이 내는가' (R7 근거)", hdr, rows,
  "share_50pct 10% 미만 주황 (소수 문장이 다 한다는 뜻)")

# ── 14 프레임 단위 예측 (전 뱅크 top-K) ────────────────────────────────
hdr = (["frame_id", "camera(카메라)", "source_unit", "gt(정답)", "gt_source(GT출처)",
        "frame_in_event(이벤트내순번)", "event_kind"] + [f"topk_{b}" for b in banks] +
       ["n_banks_correct(맞힌뱅크수)"])
rows = []
P = np.stack([d[f"topk__{b}"] for b in banks], 1)
ncorr = (P == gt[:, None]).sum(1)
for i in range(len(ids)):
    rows.append([str(ids[i]), str(cam[i]), str(unit[i]), CLASSES[gt[i]], str(src[i]), int(fie[i]), str(ekind[i])] +
                [CLASSES[x] if x >= 0 else "기타" for x in P[i]] + [int(ncorr[i])])
w("14_frame_predictions.csv", "프레임 7,498 × 전 뱅크 top-K 예측 (틀린 프레임 직접 보기)", hdr, rows,
  "XLSX 미수록(행 많음) — n_banks_correct=0 인 프레임이 '전 뱅크가 놓친' 프레임")

# ── 15 프레임 단위 마진·IoU (핵심 3뱅크) ───────────────────────────────
KEY = ["v1.0.8.0", "v1.0.8.1", "v1.0.8.4"]
hdr = ["frame_id", "camera(카메라)", "gt(정답)", "gt_source(GT출처)", "frame_in_event"]
for b in KEY:
    hdr += [f"{b}_argmax", f"{b}_topk", f"{b}_wave", f"{b}_margin(1등-2등)",
            f"{b}_iou_falldown", f"{b}_iou_fire", f"{b}_iou_smoke"]
rows = []
for i in range(len(ids)):
    row = [str(ids[i]), str(cam[i]), CLASSES[gt[i]], str(src[i]), int(fie[i])]
    for b in KEY:
        I = d[f"iou__{b}"]
        row += [CLASSES[d[f"argmax__{b}"][i]] if d[f"argmax__{b}"][i] >= 0 else "기타",
                CLASSES[d[f"topk__{b}"][i]] if d[f"topk__{b}"][i] >= 0 else "기타",
                CLASSES[d[f"wave__{b}"][i]] if d[f"wave__{b}"][i] >= 0 else "기타",
                r4(d[f"margin__{b}"][i])] + [r4(x) for x in I[i]]
    rows.append(row)
w("15_frame_margin_iou.csv", "프레임 7,498 × 핵심 3뱅크 마진·클래스별 IoU (임계를 손으로 바꿔볼 때 쓰는 원자료)", hdr, rows,
  "XLSX 미수록(행 많음) — iou 값이 임계보다 작으면 그 클래스로 발화")

# ── 00 인덱스 ──────────────────────────────────────────────────────────
w("00_index.csv", "파일 안내", ["file(파일)", "content(내용)", "rows(행수)", "highlight(XLSX강조규칙)"],
  [[a, b, c, h] for a, b, c, h in FILES])

# ── 자기검증: 차트 요약(summary.json)과 어긋나면 죽는다 ────────────────
assert abs(S["wave_thr_best_median"] - float(np.median(thrs[best_j]))) < 1e-9, "임계 스윕 불일치"
assert S["wave_beats_topk_at_best"] == int(sum(Smf[i].max() > m["banks"][b]["rules"]["topk"]["macro_f1_ev"]
                                               for i, b in enumerate(banks))), "beats_topk 불일치"
assert S["camera_elig"] == list(elig), "카메라 목록 불일치"
assert S["n_banks"] == len(banks) == 31, "뱅크 수 불일치"
print(f"자기검증 통과 — CSV {len(FILES)}개 → {CSVD}")
