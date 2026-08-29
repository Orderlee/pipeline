#!/usr/bin/env python3
"""`sourcei-prompts` 전 문장 **진짜 UMAP 재투영** — kNN 근사를 없앤다.

지금까지의 문제: 새 뱅크 문장은 좌표가 없어 out-of-sample kNN 확장으로 배치했다(§25-1).
근사라는 걸 명시했지만, 가중평균이 분산을 줄여 산점도를 오독하기 쉽다.

이제 전 문장의 벡터를 모을 수 있다:
  · 텍스트 있는 문장 → DB `image_embeddings`(entity_type='prompt') by content_hash
  · `(텍스트 없음 #N)` 자리표시자 → NAS 공급 원본에서 회수한 npz (`vecbanks/<버전>.npz`)
    매핑: 자리표시자 `#N` ↔ 공급 `ID` ↔ 그 버전 gidx 의 N 번째 (실측 확인)

투영은 **PCA(1024→50) → UMAP**. 두 가지 모드가 있고 runner 가 full → landmark 순으로 시도한다:
  · `full`     — 60만 전량 직접 fit. 근사가 전혀 없다. ⚠️ 기본 `init="spectral"` 은 60만 노드
                 희소행렬 고유분해라 공유 호스트(62GB, 타 사용자 8GB 상주)에서 OOM-kill 됐다
                 → `init="random"` + `n_neighbors=10` 로 낮춘다.
  · `landmark` — full 이 또 죽으면 층화표본 15만을 fit 하고 나머지는 **PCA-50 공간에서 직접
                 kNN 배치**한다. umap 의 `transform` 은 쓰지 않는다(아래 참조).
⚠️ sample-fit + `transform` 은 쓰지 않는다. 2026-08-28 실측: 벡터전용 뱅크 4종(191,862문장)이
   1차 transform 에서 전량 NaN 이 됐고, 작은 배치 재시도가 NaN 을 없애긴 했지만 좌표가
   **고유값 1,020개로 붕괴**했다(벡터는 100% 고유인데). NaN 검사는 통과하고 좌표만 쓰레기가 되는
   조용한 오답이라, 배치 transform 경로 자체를 폐기한다.
⚠️ `ds.select(...)` 로 쓰지 말 것 — 뷰 스테이지에 60만 ObjectId 가 들어가 BSON 16MB 를 넘긴다.
   **전체 데이터셋 순서**로 points 를 준다.
⚠️ 실행 전 emb_viz 를 백업한다. 좌표계가 바뀌므로 이전 절의 캡처와 배치가 달라진다(값은 불변).

기본 DRY-RUN. `--apply` 로 실제 반영.
"""
import os, sys, json, re, time, collections
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "3")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMBA_NUM_THREADS"): os.environ[_v] = THR
import numpy as np, psycopg2, fiftyone as fo, fiftyone.brain as fob
from prompt_cos_db import load_sentence_vectors

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
VECB = f"{OUT}/vecbanks"
KNN = int(os.environ.get("RP_KNN", "10"))
SHRINK = None
PCA_D = int(os.environ.get("RP_PCA", "50"))
MODE = os.environ.get("RP_MODE", "full")          # full | landmark
NNB = int(os.environ.get("RP_NN", "10"))
LAND = int(os.environ.get("RP_LAND", "150000"))
APPLY = "--apply" in sys.argv
T0 = time.time(); RNG = np.random.default_rng(0)
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

ds = fo.load_dataset("sourcei-prompts")
ids, txts, vers, gidx = ds.values(["id", "text", "bank_version.label", "gidx"])
N = len(ids)
log(f"샘플 {N:,}")

cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
h2c, SENT = load_sentence_vectors(cur)
cur.execute("SELECT MIN(text), content_hash FROM bank_sentences GROUP BY content_hash")
t2h = {t: h for t, h in cur.fetchall() if t}
log(f"DB prompt 벡터 {SENT.shape}")

# 자리표시자 → 공급 npz. 버전별 gidx 최솟값을 기준으로 N 번째를 찾는다.
PH = re.compile(r"^\(텍스트 없음 #(\d+)\)$")
base = {}
for v, g in zip(vers, gidx):
    if g is None: continue
    if v not in base or g < base[v]: base[v] = g
vb = {}
for f in sorted(os.listdir(VECB)) if os.path.isdir(VECB) else []:
    if not f.endswith(".npz"): continue
    tag = f[:-4]
    z = np.load(f"{VECB}/{f}")
    V = z["vecs"].astype(np.float32)
    vb[tag] = dict(vecs=V, ids=z["ids"])
    log(f"  공급 회수 {tag}: {V.shape}")

X = np.zeros((N, 1024), np.float32)
src = np.zeros(N, np.int8)          # 0=없음 1=DB 2=공급회수
miss = collections.Counter()
for i, (t, v) in enumerate(zip(txts, vers)):
    h = t2h.get(t)
    if h is not None and h in h2c:
        X[i] = SENT[h2c[h]]; src[i] = 1; continue
    m = PH.match(t or "")
    if m and v in vb:
        k = int(m.group(1))
        Z = vb[v]
        if 0 <= k < len(Z["vecs"]):
            X[i] = Z["vecs"][k]; src[i] = 2; continue
    miss[v] += 1
n1, n2 = int((src == 1).sum()), int((src == 2).sum())
log(f"벡터 확보 {n1+n2:,}/{N:,} ({(n1+n2)/N:.1%}) — DB {n1:,} · 공급회수 {n2:,} · 미확보 {N-n1-n2:,}")
if miss: log("  미확보 버전: " + ", ".join(f"{k} {v:,}" for k, v in miss.most_common(6)))

if not APPLY:
    log("DRY-RUN — --apply 로 재투영"); print("DONE"); sys.exit(0)

have = np.where(src > 0)[0]
# cosine 은 L2 정규화 후 euclidean 과 순서가 같다. 정규화해 두면 PCA 후에도 관계가 보존되고
# UMAP 을 훨씬 싼 euclidean 으로 돌릴 수 있다.
X /= np.maximum(np.linalg.norm(X, axis=1, keepdims=True), 1e-9)
from sklearn.decomposition import PCA
log(f"PCA 1024→{PCA_D} (randomized)")
Z = PCA(n_components=PCA_D, svd_solver="randomized", random_state=0).fit_transform(X[have]).astype(np.float32)
del X
log(f"PCA 완료 {Z.shape} · {Z.nbytes/2**20:.0f} MiB")
import umap
def _umap(k):
    return umap.UMAP(n_components=2, n_neighbors=k, min_dist=0.1, metric="euclidean",
                     init="random", random_state=0, low_memory=True, verbose=True)
if MODE == "full":
    log(f"UMAP 전량 적합 (n={len(Z):,}, n_neighbors={NNB}, init=random)")
    emb = _umap(NNB).fit_transform(Z).astype(np.float32)
else:
    # 층화 랜드마크: 뱅크 버전별로 비례 배분해 어느 뱅크도 표본에서 빠지지 않게 한다.
    vv = np.array([vers[i] or "" for i in have])
    pick = []
    for v in np.unique(vv):
        idx = np.where(vv == v)[0]
        n = max(200, int(round(LAND * len(idx) / len(vv))))
        pick.append(RNG.choice(idx, min(n, len(idx)), replace=False))
    L = np.sort(np.concatenate(pick))
    log(f"landmark {len(L):,}/{len(Z):,} 적합 (n_neighbors={NNB}, init=random)")
    LE = _umap(NNB).fit_transform(Z[L]).astype(np.float32)
    emb = np.empty((len(Z), 2), np.float32); emb[L] = LE
    rest = np.setdiff1d(np.arange(len(Z)), L)
    ZL = Z[L]
    log(f"나머지 {len(rest):,} 를 PCA-50 kNN({KNN})으로 배치")
    for s0 in range(0, len(rest), 4096):
        sel = rest[s0:s0 + 4096]
        D = ((Z[sel] ** 2).sum(1)[:, None] - 2 * Z[sel] @ ZL.T + (ZL ** 2).sum(1)[None, :])
        top = np.argpartition(D, KNN - 1, axis=1)[:, :KNN]
        d = np.take_along_axis(D, top, 1)
        w = np.exp(-d / np.maximum(d[:, :1], 1e-6)); w /= w.sum(1, keepdims=True)
        emb[sel] = (LE[top] * w[:, :, None]).sum(1)
    # 근사의 대가를 숨기지 않는다 — 랜드마크 대비 배치점의 산포 축소를 재서 기록한다.
    SHRINK = float(emb[rest].std(0).mean() / LE.std(0).mean())
    log(f"landmark 배치 완료 · 산포비(배치/랜드마크) {SHRINK:.3f}")
log(f"UMAP 완료 {emb.shape} · mode={MODE}")
pts = np.full((N, 2), np.nan, np.float32)
pts[have] = emb
FALLBACK = set()

# 조용한 오답 방지: **고유 좌표 수를 고유 입력 벡터 수와 비교**한다.
# ⚠️ 절대 비율로 재면 안 된다 — 같은 문장이 여러 뱅크에 중복돼 입력 자체가 52~63%만 고유하다.
#    (2026-08-28: 절대 90% 가드가 정상 전량 fit 을 52.3% 라고 잘못 막았다.)
#    붕괴의 실제 지문은 45,840행 → 고유 좌표 1,020개(2.2%) 처럼 **입력 고유수보다 훨씬 적은** 것.
uniq_in = len(np.unique(Z, axis=0)) / len(Z)
uniq = len(np.unique(np.round(emb, 3), axis=0)) / len(emb)
log(f"고유 좌표 {uniq:.1%} vs 고유 입력벡터 {uniq_in:.1%} → 보존율 {uniq/uniq_in:.1%}")
assert uniq > 0.80 * uniq_in, f"좌표 붕괴 의심 {uniq:.1%} (입력 {uniq_in:.1%}) — 반영 중단"

res = ds.load_brain_results("emb_viz")
bak = f"{OUT}/optbank/emb_viz_backup_{time.strftime('%Y%m%d_%H%M%S')}.npz"
np.savez_compressed(bak, sample_ids=np.array([str(i) for i in res.sample_ids]),
                    points=np.asarray(res.points, dtype="float32"))
log(f"기존 좌표 백업 {bak}")
ds.delete_brain_run("emb_viz")
fob.compute_visualization(ds, points=pts, brain_key="emb_viz")
chk = ds.load_brain_results("emb_viz")
nan = int(np.isnan(np.asarray(chk.points)).any(1).sum())
log(f"emb_viz 재작성 {len(chk.points):,} (샘플 {len(ds):,}) · NaN {nan:,}")
assert len(chk.points) == len(ds)
ds.set_values("coord_method", [fo.Classification(
    label=("knn5-fallback" if i in FALLBACK else ("umap-true" if s > 0 else "none")))
    for i, s in enumerate(src)])
ds.save()
json.dump(dict(n=N, db=n1, recovered=n2, missing=int(N - n1 - n2), nan=nan,
               mode=MODE, n_neighbors=NNB, pca_dim=PCA_D, landmark_shrink=SHRINK,
               uniq_coord=round(float(uniq), 4), uniq_input=round(float(uniq_in), 4), backup=bak,
               missing_by_version={k: int(v) for k, v in miss.items()}),
          open(f"{OUT}/optbank/reproject.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
