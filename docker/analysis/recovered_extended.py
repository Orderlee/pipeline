#!/usr/bin/env python3
"""회수 뱅크 7벌에 **벡터만으로 되는** 분석 4종을 확장한다 (§27).

`score_recovered_banks.py` 가 3규칙 채점(§5)을 끝냈다. 여기서는 나머지를 붙인다:

  A. 배경 1패스   — 군집별 평균코사인 A_k(n×64) → 배경평균 m_s · 군집 특이도 SD
  B. §1  프로토타입 채점 (클래스 중심벡터) vs top-K
  C. §13 임계 무관 랭킹 PR-AUC — max코사인 / 차점수 / 분포-IoU 3종
  D. §3  허브니스 — 배경 20,000프레임 top-10 슬롯 점유
  E. §15 프루닝 3컷 — 특이도컷 · 주효과컷 · 중복컷, 카메라 군집 부트스트랩 비열등

⛔ **하지 않는 것**: §18 구문 β, §17 문장 생성. 이 7벌은 텍스트가 자리표시자
   `(텍스트 없음 #N)` 라 낱말이 없다. 벡터만으로 되는 분석에만 들어갈 수 있다.

공유 서버 예의 — BLAS 4스레드 · nice 10 · 뱅크당 체크포인트(G8). 뱅크 하나가
끝날 때마다 JSON 을 떨어뜨리므로 중간에 죽어도 이어서 돌린다.

실행:
    docker exec -d docker-analysis-1 sh -c 'cd /workspace && \
      OMP_NUM_THREADS=4 OPENBLAS_NUM_THREADS=4 MKL_NUM_THREADS=4 \
      nohup nice -n 10 python3 recovered_extended.py > /data/.../ext.log 2>&1'
"""
import os

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_v, "4")

import sys, json, time, argparse
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

BASE = "/data/fiftyone/frames_bank/report/sourcei_gt"
VEC = f"{BASE}/vecbanks"
OUT = f"{BASE}/recovered"
DSN = "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
CLASSES = ["normal", "falldown", "fire", "smoke"]
EVENTS = ["falldown", "fire", "smoke"]
NK = 64
BANKS = ["v1.0.5.7", "v1.0.5.6", "v1.0.5.5", "v1.0.5.4",
         "v1.0.13.1", "v1.0.13.0", "v1.0.13.2"]
T0 = time.time()


def log(m):
    print(f"[{time.time() - T0:6.0f}s] {m}", flush=True)


def rss():
    try:
        return int(open("/proc/self/status").read().split("VmRSS:")[1].split()[0]) / 1e6
    except Exception:
        return 0.0


# ── 입력 ────────────────────────────────────────────────────────────────
def load_background():
    """군집 배정이 있는 배경 프레임 90,084 — pgvector 에서 직접. FiftyOne ODM 우회."""
    import psycopg2
    cur = psycopg2.connect(DSN).cursor()
    cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster WHERE method='kmeans64'")
    e2k = dict(cur.fetchall())
    log(f"군집 배정 {len(e2k):,}")
    cur.execute("""SELECT e.entity_id, e.embedding FROM image_embeddings e
                   JOIN analysis.frame_cluster c ON c.entity_id = e.entity_id
                   WHERE c.method='kmeans64' AND e.entity_type='frame'""")
    ids, vecs = [], []
    for eid, emb in cur:
        ids.append(eid)
        vecs.append(np.fromstring(emb.strip("[]"), sep=",", dtype=np.float32)
                    if isinstance(emb, str) else np.asarray(emb, np.float32))
    B = np.stack(vecs)
    B /= np.maximum(np.linalg.norm(B, axis=1, keepdims=True), 1e-9)
    k = np.array([e2k[i] for i in ids], np.int32)
    log(f"배경 프레임 {B.shape} · 군집 {len(np.unique(k))} · RSS {rss():.2f}GB")
    return B, k


def load_sourcei():
    import fiftyone as fo
    d = np.load(f"{BASE}/preds.npz", allow_pickle=True)
    ds = fo.load_dataset("sourcei")
    hid, hemb = ds.values(["id", "embedding"])
    assert hid == list(d["ids"])
    F = np.asarray(hemb, np.float32)
    F /= np.maximum(np.linalg.norm(F, axis=1, keepdims=True), 1e-9)
    return F, d["gt"], d["camera"], d["topk__v1.0.8.1"]


def load_bank(v):
    z = np.load(f"{VEC}/{v}.npz")
    lab = z["cls"].astype(np.int32)
    V = z["vecs"].astype(np.float32)
    V /= np.maximum(np.linalg.norm(V, axis=1, keepdims=True), 1e-9)
    return lab, V


# ── 지표 ────────────────────────────────────────────────────────────────
def mf1(gt, p, idx=None):
    t, pp = (gt, p) if idx is None else (gt[idx], p[idx])
    out = []
    for c in EVENTS:
        i = CLASSES.index(c)
        if (t == i).sum() == 0:
            continue
        tp = ((pp == i) & (t == i)).sum(); fp = ((pp == i) & (t != i)).sum()
        fn = ((pp != i) & (t == i)).sum()
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        out.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(out)) if out else 0.0


def topk_vote(S, lab):
    """정본을 그대로 쓴다 — 동표를 클래스 최고 코사인으로 깨는 규칙(votes+(topc+2)/10)을
    재구현하면 값이 어긋난다(실제로 0.5178 vs 0.5138 로 어긋났다)."""
    from prompt_cos_db import topk_vote as _canon
    return _canon(S, lab, len(CLASSES))


def wave_iou(S, mem, bins=80):
    lo = S.min(1); hi = S.max(1); w = np.maximum(hi - lo, 1e-6)
    B = np.clip(((S - lo[:, None]) / w[:, None] * bins).astype(np.int16), 0, bins - 1)
    f = S.shape[0]; fi = np.arange(f, dtype=np.int64); h = {}
    for c, idx in mem.items():
        acc = np.zeros((f, bins), np.float32)
        for j0 in range(0, len(idx), 4000):
            sub = B[:, idx[j0:j0 + 4000]].astype(np.int64)
            flat = (fi[:, None] * bins + sub).ravel()
            acc += np.bincount(flat, minlength=f * bins).reshape(f, bins).astype(np.float32)
        h[c] = acc / len(idx)
    return {c: np.minimum(h["normal"], h[c]).sum(1) / np.maximum(
        np.maximum(h["normal"], h[c]).sum(1), 1e-9) for c in mem if c != "normal"}


def boot_ci(fn, cam, n=2000, seed=0):
    rng = np.random.default_rng(seed)
    cams = np.unique(cam); by = {c: np.where(cam == c)[0] for c in cams}
    out = []
    for _ in range(n):
        pick = rng.choice(len(cams), len(cams), replace=True)
        out.append(fn(np.concatenate([by[cams[k]] for k in pick])))
    a = np.array(out)
    return float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5)), float((a > 0).mean())


# ── 본체 ────────────────────────────────────────────────────────────────
def run_bank(v, BG, BK, F, gt, cam, base_pred, sub_bg):
    lab, V = load_bank(v)
    n = len(lab)
    mem = {c: np.where(lab == i)[0] for i, c in enumerate(CLASSES) if (lab == i).any()}
    res = {"bank": v, "n_sentences": int(n),
           "class_counts": {c: int((lab == CLASSES.index(c)).sum()) for c in CLASSES}}

    # ── A. 배경 1패스 → A_k, m_s, 특이도 SD
    Ak = np.zeros((n, NK), np.float32); cnt = np.bincount(BK, minlength=NK).astype(np.float32)
    for i0 in range(0, BG.shape[0], 1000):
        blk = BG[i0:i0 + 1000] @ V.T                      # (1000, n)
        kk = BK[i0:i0 + 1000]
        for k in np.unique(kk):
            Ak[:, k] += blk[kk == k].sum(0)
        del blk
    Ak /= np.maximum(cnt, 1)[None, :]
    m_s = (Ak * (cnt / cnt.sum())).sum(1)
    spec_sd = (Ak - Ak.mean(1, keepdims=True)).std(1)
    res["m_s_mean"] = float(m_s.mean()); res["spec_sd_mean"] = float(spec_sd.mean())
    log(f"  A 배경1패스 m_s {m_s.mean():.4f} · 특이도SD {spec_sd.mean():.5f} · RSS {rss():.2f}GB")

    # ── sourcei 점수행렬
    S = np.empty((F.shape[0], n), np.float32)
    for i0 in range(0, F.shape[0], 1500):
        S[i0:i0 + 1500] = F[i0:i0 + 1500] @ V.T
    per = np.stack([np.where(lab == i, S, -2.0).max(1) for i in range(len(CLASSES))], 1)

    # ── B. §1 프로토타입 (클래스 중심벡터)
    mu = np.stack([V[mem[c]].mean(0) if c in mem else np.zeros(V.shape[1], np.float32)
                   for c in CLASSES])
    mu /= np.maximum(np.linalg.norm(mu, axis=1, keepdims=True), 1e-9)
    p_proto = (F @ mu.T).argmax(1)
    p_topk = topk_vote(S, lab)
    res["proto"] = {"mf1": round(mf1(gt, p_proto), 4), "acc": round(float((p_proto == gt).mean()), 4),
                    "fp": round(float((p_proto[gt == 0] > 0).mean()), 4),
                    "coherence": {c: round(float(np.linalg.norm(V[mem[c]].mean(0))), 4)
                                  for c in mem}}
    res["topk"] = {"mf1": round(mf1(gt, p_topk), 4), "acc": round(float((p_topk == gt).mean()), 4),
                   "fp": round(float((p_topk[gt == 0] > 0).mean()), 4)}
    d_pt = mf1(gt, p_proto) - mf1(gt, p_topk)
    lo, hi, pg = boot_ci(lambda ix: mf1(gt, p_proto, ix) - mf1(gt, p_topk, ix), cam)
    res["proto"]["delta_vs_topk"] = [round(d_pt, 4), round(lo, 4), round(hi, 4), round(pg, 3)]
    log(f"  B 프로토타입 mF1 {res['proto']['mf1']:.4f} vs topK {res['topk']['mf1']:.4f} "
        f"Δ{d_pt:+.4f} [{lo:+.3f},{hi:+.3f}]")

    # ── C. §13 PR-AUC 3 점수함수
    from sklearn.metrics import average_precision_score
    io = wave_iou(S, mem) if "normal" in mem and len(mem) > 1 else {}
    pr = {}
    for c in EVENTS:
        i = CLASSES.index(c)
        if (gt == i).sum() == 0 or c not in mem:
            continue
        y = (gt == i).astype(int)
        pr[c] = {
            "maxcos": round(float(average_precision_score(y, per[:, i])), 4),
            "diff": round(float(average_precision_score(y, per[:, i] - per[:, 0])), 4),
            "iou": round(float(average_precision_score(y, -io[c])), 4) if c in io else None,
        }
    res["prauc"] = pr
    log(f"  C PR-AUC {json.dumps(pr, ensure_ascii=False)}")

    # ── D. §3 허브니스 (배경 20,000 비이벤트 프레임 top-10)
    Nk = np.zeros(n, np.int64)
    for i0 in range(0, sub_bg.shape[0], 2000):
        blk = sub_bg[i0:i0 + 2000] @ V.T
        top = np.argpartition(-blk, 10, axis=1)[:, :10]
        Nk += np.bincount(top.ravel(), minlength=n)
        del blk
    slots = Nk.sum()
    order = np.argsort(-Nk)
    top1p = max(1, n // 100)
    inv_h = (Nk.sum() ** 2) / max(float((Nk.astype(np.float64) ** 2).sum()), 1e-9)
    res["hubness"] = {
        "frames": int(sub_bg.shape[0]), "slots": int(slots),
        "never_selected_pct": round(float((Nk == 0).mean()) * 100, 1),
        "top1pct_slot_share": round(float(Nk[order[:top1p]].sum() / slots) * 100, 1),
        "top100_slot_share": round(float(Nk[order[:100]].sum() / slots) * 100, 1),
        "effective_sentence_pct": round(float(inv_h / n) * 100, 2),
        "top100_class_mix": {c: int((lab[order[:100]] == CLASSES.index(c)).sum()) for c in CLASSES},
        "skew": round(float(((Nk - Nk.mean()) ** 3).mean() / max(Nk.std() ** 3, 1e-9)), 2),
    }
    log(f"  D 허브니스 미선택 {res['hubness']['never_selected_pct']}% · "
        f"상위1% 슬롯 {res['hubness']['top1pct_slot_share']}% · "
        f"유효문장 {res['hubness']['effective_sentence_pct']}% · "
        f"상위100 구성 {res['hubness']['top100_class_mix']}")

    # ── E. §15 프루닝 3컷 (클래스 내 분위)
    def keep_mask(kind, q):
        m = np.ones(n, bool)
        for c, idx in mem.items():
            if kind == "spec":                       # 특이도 하위 q 제거
                thr = np.quantile(spec_sd[idx], q); m[idx[spec_sd[idx] <= thr]] = False
            elif kind == "main":                     # 주효과(배경평균) 상위 q 제거
                thr = np.quantile(m_s[idx], 1 - q); m[idx[m_s[idx] >= thr]] = False
        return m

    cuts = {}
    for kind, q in [("spec", 0.25), ("spec", 0.10), ("main", 0.25), ("main", 0.10)]:
        km = keep_mask(kind, q)
        kidx = np.where(km)[0]
        p2 = topk_vote(S[:, kidx], lab[kidx])
        d = mf1(gt, p2) - mf1(gt, p_topk)
        lo, hi, pg = boot_ci(lambda ix: mf1(gt, p2, ix) - mf1(gt, p_topk, ix), cam)
        cuts[f"{kind}{int(q*100)}"] = {
            "keep_pct": round(float(km.mean()) * 100, 1), "delta": round(d, 4),
            "ci": [round(lo, 4), round(hi, 4)], "noninferior": bool(lo > -0.02),
            "fp": round(float((p2[gt == 0] > 0).mean()), 4)}
        log(f"  E {kind}{int(q*100)}컷 유지 {cuts[f'{kind}{int(q*100)}']['keep_pct']}% "
            f"Δ{d:+.4f} [{lo:+.3f},{hi:+.3f}] 비열등={cuts[f'{kind}{int(q*100)}']['noninferior']}")
    res["pruning"] = cuts

    # 중복컷(cos>0.95) — 클래스 내 블록 탐욕. 정확중복 0건이었으므로 근접중복만 본다
    keep = np.ones(n, bool)
    for c, idx in mem.items():
        alive = list(idx)
        gone = set()
        for j0 in range(0, len(alive), 2000):
            blk = alive[j0:j0 + 2000]
            Sim = V[blk] @ V[alive].T
            for a, gi in enumerate(blk):
                if gi in gone:
                    continue
                dup = np.where(Sim[a] > 0.95)[0]
                for b in dup:
                    if alive[b] != gi:
                        gone.add(alive[b])
            del Sim
        keep[list(gone)] = False
    kidx = np.where(keep)[0]
    p2 = topk_vote(S[:, kidx], lab[kidx])
    d = mf1(gt, p2) - mf1(gt, p_topk)
    lo, hi, pg = boot_ci(lambda ix: mf1(gt, p2, ix) - mf1(gt, p_topk, ix), cam)
    res["pruning"]["dup95"] = {
        "keep_pct": round(float(keep.mean()) * 100, 1), "delta": round(d, 4),
        "ci": [round(lo, 4), round(hi, 4)], "noninferior": bool(lo > -0.02),
        "keep_by_class": {c: round(float(keep[mem[c]].mean()) * 100, 1) for c in mem}}
    log(f"  E 중복컷 유지 {res['pruning']['dup95']['keep_pct']}% Δ{d:+.4f} "
        f"클래스별 {res['pruning']['dup95']['keep_by_class']}")

    np.savez_compressed(f"{OUT}/{v}_ledger.npz", m_s=m_s, spec_sd=spec_sd,
                        Nk=Nk, lab=lab)
    with open(f"{OUT}/{v}_ext.json", "w") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    log(f"  ✓ {v} 저장 · RSS {rss():.2f}GB")
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--banks", default=",".join(BANKS))
    a = ap.parse_args()
    todo = [b for b in a.banks.split(",") if not os.path.exists(f"{OUT}/{b}_ext.json")]
    log(f"대상 {len(todo)}벌 (완료분 건너뜀): {todo}")
    if not todo:
        return
    BG, BK = load_background()
    F, gt, cam, base = load_sourcei()
    z = np.load(f"{BASE}/frames_sub_24792.npz")
    FF, ref = z["FF"].astype(np.float32), z["ref"]
    FF /= np.maximum(np.linalg.norm(FF, axis=1, keepdims=True), 1e-9)
    sub_bg = FF[~np.isin(ref, ["fire", "smoke"])]      # 비이벤트 배경 20,000
    log(f"배경 표본 {sub_bg.shape[0]:,} (frames 표본에서 fire/smoke 제외)")
    for i, b in enumerate(todo, 1):
        log(f"===== [{i}/{len(todo)}] {b}")
        try:
            run_bank(b, BG, BK, F, gt, cam, base, sub_bg)
        except Exception as e:
            import traceback
            log(f"  ✗ {b} 실패: {e}")
            traceback.print_exc()
    log("배치 끝")


if __name__ == "__main__":
    main()
