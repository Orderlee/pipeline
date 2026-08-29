#!/usr/bin/env python3
"""목적 1 — image embedding 과 prompt embedding 은 어떻게 연결돼 있나 (라벨 불필요).

프레임×문장 코사인 c(i,j) 를 **이원 분산분해**한다:
    c(i,j) = 전체평균 + 프레임효과(i) + 문장효과(j) + 상호작용(i,j)
- 프레임효과 = "이 프레임은 모든 문장과 잘 붙는다"(밝기·구도·텍스처). **판별 정보 없음**.
- 문장효과 = "이 문장은 모든 프레임과 잘 붙는다"(일반적 문장). **판별 정보 없음**. max 풀링이 이걸 증폭한다.
- **상호작용만이 클래스를 가른다.** 이 셋의 비율이 프롬프트 방식의 상한을 규정한다.

이어서 두 가지 라벨-free 정규화를 실측한다 (CLIP zero-shot 문헌의 표준 처치):
  ① 문장 센터링 s' = s − mean_i(x_i)  … 프레임 배경 방향 제거
  ② 문장별 z-정규화 c'(i,j) = (c(i,j) − μ_j) / σ_j  … "목소리 큰 문장" 억제
효과는 SAM3 fire/smoke 를 약참조로 측정(GT 아님, 상대 비교).
"""
import os, json, collections
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ.setdefault(_v, "6")
import numpy as np, psycopg2, sys
sys.path.insert(0, "/workspace")
from prompt_cos_db import load_sentence_vectors, load_banks
import fiftyone as fo
from fiftyone import ViewField as F

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CLASSES = ["normal", "falldown", "fire", "smoke"]
RNG = np.random.default_rng(0)
res = {}

ds = fo.load_dataset("frames"); fr = ds.match(F("modality") == "frame")
ncls_raw, emb, proj = fr.values(["normalized_class", "image_embedding", "project"])
ncls = np.array([x or "none" for x in ncls_raw]); proj = np.array(proj)
fire_i = np.where(ncls == "fire")[0]; smoke_i = np.where(ncls == "smoke")[0]
neg_i = np.where(np.isin(ncls, ["none", "person"]))[0]
bg_i = RNG.choice(neg_i, size=20000, replace=False)                    # 배경 통계용(정규화 학습)
ev_i = np.concatenate([fire_i, smoke_i, RNG.choice(np.setdiff1d(neg_i, bg_i), size=10000, replace=False)])
def mat(idx):
    X = np.asarray([emb[i] for i in idx], dtype=np.float32); X /= np.linalg.norm(X, axis=1, keepdims=True); return X
BG, EV = mat(bg_i), mat(ev_i)
ref = np.array([{"fire": 2, "smoke": 3}.get(ncls[i], 0) for i in ev_i], dtype=np.int8)
print(f"배경 {len(BG):,} / 평가 {len(EV):,} (fire {len(fire_i):,} smoke {len(smoke_i):,})")

cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
h2c, SENT = load_sentence_vectors(cur)
print(f"문장 {SENT.shape}")

# ── 1) 이원 분산분해 (스트리밍: 행합·열합·제곱합만 있으면 된다) ──────────
n_f, n_s = len(BG), SENT.shape[0]
row_sum = np.zeros(n_f); col_sum = np.zeros(n_s); tot = 0.0; sq = 0.0
for s in range(0, n_f, 2000):
    C = BG[s:s + 2000] @ SENT.T
    row_sum[s:s + 2000] = C.sum(1); col_sum += C.sum(0); tot += C.sum(); sq += float((C.astype(np.float64) ** 2).sum())
N = n_f * n_s; gm = tot / N
ss_tot = sq - N * gm ** 2
ss_frame = n_s * float(((row_sum / n_s - gm) ** 2).sum())
ss_sent = n_f * float(((col_sum / n_f - gm) ** 2).sum())
ss_int = ss_tot - ss_frame - ss_sent
res["anova"] = dict(mean_cos=float(gm), frame=ss_frame / ss_tot, sentence=ss_sent / ss_tot, interaction=ss_int / ss_tot,
                    sd_total=float(np.sqrt(ss_tot / N)), sd_frame=float(np.sqrt(ss_frame / N)), sd_sent=float(np.sqrt(ss_sent / N)))
print(f"\n=== 이원 분산분해 (배경 {n_f:,} × 문장 {n_s:,} = {N/1e9:.1f}G 셀) ===")
print(f"  전체 평균 코사인 {gm:.4f}, 표준편차 {np.sqrt(ss_tot/N):.4f}")
print(f"  프레임 주효과 {ss_frame/ss_tot:6.1%}   문장 주효과 {ss_sent/ss_tot:6.1%}   **상호작용 {ss_int/ss_tot:6.1%}**")
print(f"  → 판별에 쓸 수 있는 신호는 상호작용 {ss_int/ss_tot:.1%} 뿐. 나머지는 '이 프레임/문장이 원래 세다'")

# ── 2) 모달리티 갭 ──────────────────────────────────────────────────────
si = RNG.choice(n_s, 20000, replace=False)
ii = RNG.choice(n_f, 4000, replace=False)
it = (BG[ii] @ SENT[si].T).ravel()
ii2 = BG[ii][:2000] @ BG[ii][2000:].T
ss2 = SENT[si][:5000] @ SENT[si][5000:10000].T
mu_i = BG.mean(0); mu_i /= np.linalg.norm(mu_i); mu_s = SENT.mean(0); mu_s /= np.linalg.norm(mu_s)
res["gap"] = dict(image_text=float(it.mean()), image_image=float(ii2.mean()), text_text=float(ss2.mean()),
                  centroid_cos=float(mu_i @ mu_s))
print(f"\n=== 모달리티 갭 ===\n  이미지↔문장 {it.mean():.3f} | 이미지↔이미지 {ii2.mean():.3f} | 문장↔문장 {ss2.mean():.3f} | 중심벡터 간 {mu_i@mu_s:.3f}")

# ── 3) 라벨-free 정규화 두 가지의 효과 ──────────────────────────────────
mu_j = col_sum / n_f                                    # 문장별 배경 평균
sd_j = np.zeros(n_s)                                    # 문장별 배경 표준편차(2패스)
acc = np.zeros(n_s)
for s in range(0, n_f, 2000):
    C = BG[s:s + 2000] @ SENT.T
    acc += ((C - mu_j) ** 2).sum(0)
sd_j = np.sqrt(acc / n_f) + 1e-6
banks = {b["version"]: b for b in load_banks(cur, ["v1.0.8.0", "v1.0.8.1"])}
def cls_cols(bank):
    m = {}
    for chash, cls, _g in banks[bank]["rows"]:
        if chash in h2c: m.setdefault(cls, []).append(h2c[chash])
    return {c: np.asarray(v) for c, v in m.items() if c in CLASSES}

print("\n=== 라벨-free 정규화 — SAM3 fire 재현율 / 비화재 오탐 (argmax, 뱅크 v1.0.8.0·v1.0.8.1) ===")
res["norm"] = {}
for bank in ["v1.0.8.0", "v1.0.8.1"]:
    mem = cls_cols(bank); cs = [c for c in CLASSES if c in mem]
    S_raw = np.zeros((len(EV), len(cs)), np.float32); S_z = np.zeros_like(S_raw); S_ct = np.zeros_like(S_raw)
    SENT_ct = SENT - (SENT @ mu_i)[:, None] * mu_i[None, :]      # 이미지 중심방향 성분 제거
    SENT_ct /= np.linalg.norm(SENT_ct, axis=1, keepdims=True)
    for s in range(0, len(EV), 2000):
        C = EV[s:s + 2000] @ SENT.T
        Z = (C - mu_j) / sd_j
        Ct = EV[s:s + 2000] @ SENT_ct.T
        for k, c in enumerate(cs):
            S_raw[s:s + 2000, k] = C[:, mem[c]].max(1); S_z[s:s + 2000, k] = Z[:, mem[c]].max(1); S_ct[s:s + 2000, k] = Ct[:, mem[c]].max(1)
    fi = cs.index("fire")
    for name, S in (("원본", S_raw), ("문장 z-정규화", S_z), ("이미지중심 제거", S_ct)):
        p = np.array([cs.index(CLASSES[x]) if CLASSES[x] in cs else 0 for x in [0]] * 0)  # placeholder
        pred = S.argmax(1)
        rec = float((pred[ref == 2] == fi).mean()); fp = float((pred[ref == 0] == fi).mean())
        sm = float((pred[ref == 3] == fi).mean())
        res["norm"].setdefault(bank, {})[name] = dict(fire_recall=rec, fp=fp, smoke_to_fire=sm)
        print(f"  {bank} {name:<14} fire 재현율 {rec:.3f}  비화재 오탐 {fp:.3%}  smoke→fire {sm:.3f}")
json.dump(res, open(f"{OUT}/embed_geometry.json", "w"), ensure_ascii=False, indent=1)
