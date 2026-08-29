#!/usr/bin/env python3
"""전량 생성 뱅크 빌드·평가 — 공급 문장 0, 생성 문장만.

§23 의 혼합 뱅크(공급 75%)는 성능 1위였지만 규칙을 클래스마다 다르게 어겼다
(normal 14.0% · smoke 3.6% · falldown 38.2% · fire 69.8%, 하한 70%). 원인은 공급 문장이
그 규칙으로 쓰인 적이 없기 때문. 전량 생성이면 규칙은 정의상 지켜진다 — 질문은 **성능과
분포-IoU 가 버티는가**다 (§17 에서 생성 단독은 균질해서 PR-AUC 0.382 로 무너졌다).

**설정을 탐색하지 않는다.** §23 이 고른 노브(클래스당 500 · 중복 0.97 · 전역평균 제거 ·
품질키 특이도)를 **그대로 사전 고정**해서 쓴다 — 같은 조건에서 출처만 바꾼 대조 실험이고,
설정 선택 과적합이 끼어들 여지를 없앤다.
"""
import os, sys, json, csv, glob, collections, time, urllib.parse, urllib.request
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
from prompt_cos_db import load_sentence_vectors, load_banks, topk_vote, wave_iou
import numpy as np, psycopg2, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.metrics import average_precision_score
import fiftyone as fo
import prompt_standard as ps

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"; BANKDIR = f"{OUT}/optbank"
GENF = "/workspace/gen_full.json"
CACHE = f"{BANKDIR}/genfull_vectors.npz"
EMB = os.environ.get("EMBED_URL", "http://embedding-service:8003/embed_text")
K_PER_CLASS, DEDUP, CENTERED = 500, 0.97, True
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]; EVENTS = CLASSES[1:]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
RNG = np.random.default_rng(0); T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

gen = json.load(open(GENF))
sent = gen["sentences"]
log("생성 문장 " + " ".join(f"{c} {len(v):,}" for c, v in sent.items()))
texts, labs = [], []
for i, c in enumerate(CLASSES):
    for t in sent.get(c, []): texts.append(t); labs.append(i)
labs = np.array(labs)
log(f"총 {len(texts):,}")

# ── 인코딩 (캐시) ────────────────────────────────────────────────────
cached = {}
if os.path.exists(CACHE):
    z = np.load(CACHE, allow_pickle=True)
    cached = {str(t): v for t, v in zip(z["texts"], z["vecs"])}
need = [t for t in texts if t not in cached]
log(f"인코딩 캐시 {len(texts)-len(need):,} · 신규 {len(need):,}")
for i, t in enumerate(need):
    body = urllib.parse.urlencode({"text": t}).encode()
    r = json.loads(urllib.request.urlopen(urllib.request.Request(EMB, data=body), timeout=300).read())
    v = np.asarray(r["vector"], dtype=np.float32); cached[t] = v / np.linalg.norm(v)
    if (i + 1) % 400 == 0: log(f"  {i+1:,}/{len(need):,}")
np.savez_compressed(CACHE, texts=np.array(list(cached)), vecs=np.stack([cached[t] for t in cached]))
G = np.stack([cached[t] for t in texts]).astype(np.float32)
log(f"벡터 {G.shape}")

# ── 라벨-free 지표 ───────────────────────────────────────────────────
conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"); cur = conn.cursor()
cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster WHERE method='kmeans64'")
e2k = dict(cur.fetchall()); NK = 64; NG = len(texts)
Ak = np.zeros((NG, NK), np.float64); cnt = np.zeros(NK, np.int64); msum = np.zeros(NG, np.float64); ntot = 0
buf_v, buf_k = [], []
def flush():
    global ntot
    if not buf_v: return
    X = np.vstack(buf_v); X /= np.linalg.norm(X, axis=1, keepdims=True)
    S = X @ G.T
    msum[:] += S.sum(0); ntot += len(buf_k); kk = np.asarray(buf_k)
    for k0 in np.unique(kk):
        mm = kk == k0; Ak[:, k0] += S[mm].sum(0); cnt[k0] += int(mm.sum())
    buf_v.clear(); buf_k.clear()
with conn.cursor(name="fr9") as c2:
    c2.itersize = 4000
    c2.execute("SELECT entity_id, embedding::text FROM image_embeddings WHERE entity_type='frame'")
    for eid, vt in c2:
        k = e2k.get(eid)
        if k is None: continue
        buf_v.append(np.fromstring(vt.strip("[]"), sep=",", dtype=np.float32)); buf_k.append(k)
        if len(buf_v) >= 4000: flush()
flush()
assert ntot == 90084, ntot
g_ms = (msum / ntot).astype(np.float32)
Ak = (Ak / np.maximum(cnt, 1)).astype(np.float32)
g_sd = (Ak - Ak.mean(1, keepdims=True)).std(1); del Ak
log(f"라벨-free — m_s {g_ms.mean():.4f} · 특이도 {g_sd.mean():.5f}")

# ── 선택 (사전 고정 노브) ────────────────────────────────────────────
mu_g = G.mean(0); mu_g /= np.linalg.norm(mu_g)
GC = G - (G @ mu_g)[:, None] * mu_g[None, :]
GC = (GC / np.maximum(np.linalg.norm(GC, axis=1, keepdims=True), 1e-8)).astype(np.float32)
USE = GC if CENTERED else G
sel_all = []
for i, c in enumerate(CLASSES):
    idx = np.where(labs == i)[0]
    q = g_sd[idx] * (1.0 - (g_ms[idx] - g_ms[idx].min()) / (np.ptp(g_ms[idx]) + 1e-9) * 0.5)
    order = idx[np.argsort(g_ms[idx])]
    V = G[order]; keep, kept = [], []
    for j in range(len(order)):
        if kept and float(np.max(V[j] @ V[kept].T)) > DEDUP: continue
        kept.append(j); keep.append(order[j])
    keep = np.array(keep)
    qq = g_sd[keep] * (1.0 - (g_ms[keep] - g_ms[keep].min()) / (np.ptp(g_ms[keep]) + 1e-9) * 0.5)
    take = keep[np.argsort(-qq)[:min(K_PER_CLASS, len(keep))]]
    sel_all.append(take)
    log(f"  {c}: 후보 {len(idx):,} → 중복컷 {len(keep):,} → 선택 {len(take):,}")
sel = np.concatenate(sel_all); slab = labs[sel]
log(f"최종 뱅크 {len(sel):,}문장")

# ── 채점 ────────────────────────────────────────────────────────────
d = np.load(f"{OUT}/preds.npz", allow_pickle=True); gt, cam = d["gt"], d["camera"]
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == list(d["ids"])
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
def f1s(t, p):
    o = {}
    for i, c in enumerate(CLASSES):
        tp = int(((p == i) & (t == i)).sum()); fp = int(((p == i) & (t != i)).sum()); fn = int(((p != i) & (t == i)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1); o[c] = 2 * pr * rc / max(pr + rc, 1e-12)
    return o
S = FH @ USE[sel].T
pred = topk_vote(S, slab, 4)
mem = {c: np.where(slab == i)[0] for i, c in enumerate(CLASSES)}
io = wave_iou(S, mem)
f = f1s(gt, pred); all4 = [f[c] for c in CLASSES]
aps = [float(average_precision_score((gt == CLASSES.index(c)).astype(int), -io[c])) for c in EVENTS]
row = dict(bank="sourcei-GEN (전량 생성)", n=len(sel), acc=round(float((pred == gt).mean()), 4),
           macro_f1_4cls=round(float(np.mean(all4)), 4),
           macro_f1_event=round(float(np.mean([f[c] for c in EVENTS])), 4),
           prauc=round(float(np.mean(aps)), 4), balance=round(float(min(all4) / np.mean(all4)), 4),
           fp_normal=round(float((pred[gt == 0] > 0).mean()), 4),
           **{f"f1_{c}": round(f[c], 4) for c in CLASSES})
log(f"전량 생성 뱅크 — 4클래스 mF1 {row['macro_f1_4cls']} · PR-AUC {row['prauc']} · 균형 {row['balance']} · 오탐 {row['fp_normal']}")
log("  클래스별 F1 " + " ".join(f"{c} {f[c]:.3f}" for c in CLASSES))

# ── 규칙 준수 ────────────────────────────────────────────────────────
rules = []
for i, c in enumerate(CLASSES):
    ii = [j for j in sel if labs[j] == i]
    tt = [texts[j] for j in ii]
    kept, rej, rep = ps.validate(tt, c, ps.sourcei)
    rules.append(dict(cls=c, n=len(tt), winning_form=rep["winning_form"],
                      winning_share=round(rep["winning_share"], 3),
                      quota_ok="Y" if rep["quota_ok"] else "N", violations=len(rej)))
    log(f"  규칙 {c:<9} 승리형태 {rep['winning_share']:.1%} ({'통과' if rep['quota_ok'] else '미달'}) · 위반 {len(rej)}")

# ── 비교표 (기존 32행에 추가) ────────────────────────────────────────
prev = list(csv.DictReader(open(f"{OUT}/csv/54_optbank_vs_all.csv", encoding="utf-8-sig")))
rows = [{k: (float(v) if k not in ("rank", "bank", "n") else v) for k, v in r.items()} for r in prev]
for r in rows: r["n"] = int(r["n"])
rows = [r for r in rows if r["bank"] != row["bank"]]
rows.append({**row})
rows.sort(key=lambda r: -float(r["macro_f1_4cls"]))
for i, r in enumerate(rows, 1): r["rank"] = i
with open(f"{OUT}/csv/55_genfull_vs_all.csv", "w", newline="", encoding="utf-8-sig") as fh:
    w = csv.DictWriter(fh, fieldnames=["rank", "bank", "n", "acc", "macro_f1_4cls", "macro_f1_event",
                                       "prauc", "balance", "fp_normal"] + [f"f1_{c}" for c in CLASSES])
    w.writeheader()
    for r in rows: w.writerow({k: r.get(k, "") for k in w.fieldnames})
with open(f"{OUT}/csv/56_genfull_rulecheck.csv", "w", newline="", encoding="utf-8-sig") as fh:
    w = csv.DictWriter(fh, fieldnames=["cls", "n", "winning_form", "winning_share", "quota_ok", "violations"])
    w.writeheader()
    for r in rules: w.writerow(r)
with open(f"{OUT}/csv/57_genfull_sentences.csv", "w", newline="", encoding="utf-8-sig") as fh:
    w = csv.writer(fh); w.writerow(["class", "text", "m_s", "spec_sd", "form"])
    for j in sel: w.writerow([CLASSES[labs[j]], texts[j], round(float(g_ms[j]), 5),
                              round(float(g_sd[j]), 5), ps._form_of(texts[j])])
gi = next(i for i, r in enumerate(rows) if r["bank"] == row["bank"])
oi = next((i for i, r in enumerate(rows) if str(r["bank"]).startswith("sourcei-OPT")), None)
log(f"순위 — 전량생성 {gi+1}/{len(rows)} · 혼합(OPT) {oi+1 if oi is not None else '?'}/{len(rows)}")
np.savez_compressed(f"{BANKDIR}/genfull_bank.npz", vecs=USE[sel], text=np.array([texts[j] for j in sel]),
                    cls=np.array([CLASSES[labs[j]] for j in sel]), pred=pred,
                    iou=np.stack([io[c] for c in EVENTS], 1))
json.dump(dict(row=row, rules=rules, n_candidates={c: len(v) for c, v in sent.items()},
               knobs=dict(k=K_PER_CLASS, dedup=DEDUP, centered=CENTERED),
               rank=dict(genfull=gi + 1, mixed=(oi + 1 if oi is not None else None), total=len(rows))),
          open(f"{BANKDIR}/genfull.json", "w"), ensure_ascii=False, indent=1)

# ── 그림 ────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(21, 6.8))
ax = axes[0]
top = rows[:12]; y = np.arange(len(top))
col = ["#2a78d6" if str(r["bank"]).startswith("sourcei-GEN") else
       ("#1baf7a" if str(r["bank"]).startswith("sourcei-OPT") else "#c3c2b7") for r in top]
ax.barh(y, [float(r["macro_f1_4cls"]) for r in top], color=col)
for i, r in enumerate(top): ax.text(float(r["macro_f1_4cls"]) + .004, i, f"{float(r['macro_f1_4cls']):.3f}", va="center", fontsize=8)
ax.set_yticks(y); ax.set_yticklabels([f"{r['bank']} ({int(r['n']):,})" for r in top], fontsize=7.6); ax.invert_yaxis()
ax.set_xlabel("4클래스 macro-F1"); ax.set_xlim(0, max(float(r["macro_f1_4cls"]) for r in top) * 1.16)
ax.set_title(f"① 상위 12 — 전량생성 {gi+1}위 · 혼합 {oi+1 if oi is not None else '?'}위 / {len(rows)}", loc="left", fontsize=11)
ax = axes[1]
x = np.arange(4); w2 = 0.38
gen_share = [r["winning_share"] for r in rules]
mix = {r["class(클래스)"]: float(r["winning_share(비율)"]) for r in
       csv.DictReader(open(f"{OUT}/csv/53_optbank_rulecheck.csv", encoding="utf-8-sig"))}
ax.bar(x - w2 / 2, [mix[c] for c in CLASSES], w2, color="#1baf7a", label="혼합 뱅크(공급 75%)")
ax.bar(x + w2 / 2, gen_share, w2, color="#2a78d6", label="전량 생성")
for i, c in enumerate(CLASSES):
    ax.text(i - w2 / 2, mix[c] + .02, f"{mix[c]:.0%}", ha="center", fontsize=8.5)
    ax.text(i + w2 / 2, gen_share[i] + .02, f"{gen_share[i]:.0%}", ha="center", fontsize=8.5)
ax.axhline(ps.FORM_QUOTA, color="#e34948", ls="--", lw=1.2)
ax.text(-0.45, ps.FORM_QUOTA + .02, "규칙 하한 70%", color="#e34948", fontsize=9)
ax.set_xticks(x); ax.set_xticklabels(CLASSES); ax.set_ylim(0, 1.15); ax.legend(frameon=False, fontsize=9)
ax.set_ylabel("§10 승리 템플릿 형태 비율")
ax.set_title("② 규칙 준수 — 출처를 바꾸면 지켜지나", loc="left", fontsize=11)
ax = axes[2]
for k, (key, lab_, col2) in enumerate([("macro_f1_4cls", "4클래스 mF1", "#1baf7a"),
                                       ("prauc", "PR-AUC", "#2a78d6"), ("balance", "균형", "#eda100"),
                                       ("fp_normal", "정상 오탐", "#0b0b0b")]):
    names = ["sourcei-GEN (전량 생성)", "sourcei-OPT", "v1.0.8.1"]
    vals = []
    for nm in names:
        r = next((r for r in rows if str(r["bank"]).startswith(nm.split(" (")[0])), None)
        vals.append(float(r[key]) if r else 0.0)
    ax.bar(np.arange(3) + (k - 1.5) * 0.2, vals, 0.19, color=col2, label=lab_)
ax.set_xticks(range(3)); ax.set_xticklabels(["전량 생성\n(공급 0)", "혼합\n(공급 75%)", "v1.0.8.1\n(공급 100%)"], fontsize=8.5)
ax.legend(frameon=False, fontsize=9, ncol=2)
ax.set_title("③ 출처 구성별 — 성능·랭킹·균형·오탐", loc="left", fontsize=11)
fig.suptitle("전량 생성 뱅크 — 공급 문장 0, 규칙만으로 만든 뱅크가 어디까지 가나\n"
             f"후보 {len(texts):,}문장(배치 생성·다양성 축 회전) → 중복컷 0.97 → 클래스당 {K_PER_CLASS} · "
             "노브는 §23 과 동일하게 사전 고정(탐색 없음)", x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f56_genfull.png", dpi=150); plt.close(fig)
log("saved f56 → csv/55~57")
print("DONE")
