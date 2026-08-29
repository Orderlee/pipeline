#!/usr/bin/env python3
"""전량 생성 뱅크 `vGEN.2026.08.28` 를 `sourcei-prompts` 에 등록 — compare 패널에서 보이게.

§23 의 혼합 뱅크는 문장의 69%가 이미 데이터셋에 있어 좌표를 **그대로 복사**할 수 있었다.
전량 생성본은 **전부 새 문장이라 복사할 좌표가 0개**다. NaN 으로 두면 산점도가 비어
시각화가 성립하지 않는다.

그래서 **out-of-sample 확장**을 쓴다 (표준 Nyström 식):
    새 문장의 좌표 = 임베딩 공간에서 가장 가까운 기존 문장 5개의 좌표를 코사인 softmax 가중평균
UMAP transform 이 하는 일과 같은 성격이고, 결과가 **근사**라는 사실을 숨기지 않는다:
  · 샘플 필드 `coord_method` 에 `knn5-extension` 을 박아 둔다 (기존 문장은 `original`)
  · 보고서에도 명시한다 — 이 산점도는 **정확한 투영이 아니라 이웃 기반 배치**다
정확한 좌표가 필요하면 전 문장 임베딩으로 UMAP 을 다시 적합해야 하는데, `sourcei-prompts` 는
임베딩 필드가 없고(좌표가 외부 계산본) 자리표시자가 43% 라 전량 재적합은 별건 작업이다.

실행 전 emb_viz 를 백업한다. ⚠️ `ds.select(60만 id)` 로 쓰면 뷰 스테이지만 14.5MB 라 BSON
16MB 를 넘긴다 — **전체 데이터셋 순서**로 points 를 준다(2026-08-28 실측).
"""
import os, sys, json, glob, time
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
from prompt_cos_db import load_sentence_vectors
import numpy as np, psycopg2, fiftyone as fo, fiftyone.brain as fob

BANKDIR = "/data/fiftyone/frames_bank/report/sourcei_gt/optbank"
VERSION = "vGEN.2026.08.28"; GIDX0 = 3100000
WFIELD = "winner_gidx_v" + "".join(VERSION.lstrip("vV").split("."))
CLASSES = ["normal", "falldown", "fire", "smoke"]
K = 5
APPLY = "--apply" in sys.argv
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)
log(f"버전 {VERSION} · 조인 필드 {WFIELD} · gidx {GIDX0:,} · {'APPLY' if APPLY else 'DRY-RUN'}")

z = np.load(f"{BANKDIR}/genfull_bank.npz", allow_pickle=True)
text = [str(x) for x in z["text"]]; cls = [str(x) for x in z["cls"]]
V = z["vecs"].astype(np.float32)                     # 전역평균 제거판 (뱅크가 쓰는 벡터)
pred = z["pred"]
N = len(text)
log(f"뱅크 {N}문장 · 구성 {dict(zip(*np.unique(cls, return_counts=True)))}")

ds = fo.load_dataset("sourcei-prompts")
res = ds.load_brain_results("emb_viz")
old_ids = [str(i) for i in res.sample_ids]; old_xy = np.asarray(res.points, dtype="float32")
assert len(old_ids) == len(ds), f"시작부터 불일치 {len(old_ids)} vs {len(ds)}"
bak = f"{BANKDIR}/emb_viz_backup_{time.strftime('%Y%m%d_%H%M%S')}.npz"
np.savez_compressed(bak, sample_ids=np.array(old_ids), points=old_xy)
log(f"백업 {bak} · 샘플 {len(ds):,} · emb_viz {len(old_xy):,}")

# ── 이웃 색인: 좌표를 가진 기존 문장 중 **벡터를 알 수 있는 것**만 ──────
cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
h2c, SENT = load_sentence_vectors(cur)
cur.execute("SELECT MIN(text), content_hash FROM bank_sentences GROUP BY content_hash")
t2h = {t: h for t, h in cur.fetchall() if t}
sid2xy = {i: old_xy[k] for k, i in enumerate(old_ids)}
ids_all, txt_all = ds.values(["id", "text"])
idx_vec, idx_xy, seen = [], [], set()
for i, t in zip(ids_all, txt_all):
    if not t or t in seen: continue
    h = t2h.get(t)
    if h is None or h not in h2c: continue
    xy = sid2xy.get(i)
    if xy is None or not np.isfinite(xy).all(): continue
    seen.add(t); idx_vec.append(h2c[h]); idx_xy.append(xy)
IDX = SENT[np.array(idx_vec)]; IXY = np.stack(idx_xy).astype(np.float32)
log(f"이웃 색인 {len(IDX):,}문장 (좌표+벡터를 모두 아는 것)")

# ── kNN out-of-sample 확장 ─────────────────────────────────────────
# 뱅크 벡터는 전역평균 제거판이라 색인과 공간이 다르다 → 원본 벡터를 다시 만들어 비교한다
gv = np.load(f"{BANKDIR}/genfull_vectors.npz", allow_pickle=True)
raw = {str(t): v for t, v in zip(gv["texts"], gv["vecs"])}
VR = np.stack([raw[t] for t in text]).astype(np.float32)
new_xy = np.empty((N, 2), np.float32)
for s0 in range(0, N, 256):
    S = VR[s0:s0 + 256] @ IDX.T
    top = np.argpartition(-S, K - 1, axis=1)[:, :K]
    for r in range(S.shape[0]):
        t_ = top[r]; w = S[r, t_]
        w = np.exp((w - w.max()) * 20.0); w /= w.sum()          # 코사인 softmax (온도 1/20)
        new_xy[s0 + r] = (IXY[t_] * w[:, None]).sum(0)
sim_top1 = float(np.mean([np.max(VR[i] @ IDX.T) for i in range(0, N, 50)]))
log(f"좌표 확장 완료 · 최근접 코사인 평균(표본) {sim_top1:.3f}")

if not APPLY:
    log("DRY-RUN — --apply 로 실제 등록"); print("DONE"); sys.exit(0)

ex = ds.match(fo.ViewField("bank_version.label") == VERSION)
if len(ex): ds.delete_samples(ex.values("id")); log(f"기존 {VERSION} {len(ex):,} 삭제")

hy = fo.load_dataset("sourcei")
fp0 = hy.first().filepath
samples = []
for i in range(N):
    s = fo.Sample(filepath=fp0)
    s["text"] = text[i]
    s["category"] = fo.Classification(label=cls[i])
    s["bank_version"] = fo.Classification(label=VERSION)
    s["adopted"] = fo.Classification(label="채택")
    s["gidx"] = GIDX0 + i
    s["coord_method"] = fo.Classification(label="knn5-extension")
    samples.append(s)
new_ids = ds.add_samples(samples)
log(f"{len(new_ids):,} 추가 → 총 {len(ds):,}")

t2new = {text[i]: new_xy[i] for i in range(N)}
cur_ids, cur_txt = ds.values(["id", "text"])
pts = np.full((len(cur_ids), 2), np.nan, dtype="float32"); miss = 0
for k, (i, t) in enumerate(zip(cur_ids, cur_txt)):
    if i in sid2xy: pts[k] = sid2xy[i]
    elif t in t2new: pts[k] = t2new[t]
    else: miss += 1
log(f"좌표 {len(pts):,} · 미해결 {miss}")
ds.delete_brain_run("emb_viz")
fob.compute_visualization(ds, points=pts, brain_key="emb_viz")
chk = ds.load_brain_results("emb_viz")
assert len(chk.points) == len(ds)
log(f"emb_viz 재작성 {len(chk.points):,} · NaN {int(np.isnan(np.asarray(chk.points)).any(1).sum())}")

# ── 프레임 조인 필드 ────────────────────────────────────────────────
d = np.load("/data/fiftyone/frames_bank/report/sourcei_gt/preds.npz", allow_pickle=True)
hid, hemb = hy.values(["id", "embedding"])
assert hid == list(d["ids"])
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
lab_s = np.array([CLASSES.index(c) for c in cls])
win = np.empty(len(pred), np.int64)
for s0 in range(0, len(FH), 1500):
    S = FH[s0:s0 + 1500] @ V.T
    for r in range(S.shape[0]):
        m = np.where(lab_s == int(pred[s0 + r]))[0]
        win[s0 + r] = GIDX0 + int(m[np.argmax(S[r, m])])
hy.set_values(WFIELD, win.tolist()); hy.save()
log(f"프레임 조인 필드 {WFIELD} 기록 ({len(win):,})")
json.dump(dict(version=VERSION, n=N, winner_field=WFIELD, gidx0=GIDX0,
               coord_method="knn5-extension", knn=K, index_size=len(IDX),
               mean_top1_cos=round(sim_top1, 4), backup=bak, total=len(ds)),
          open(f"{BANKDIR}/genfull_register.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
