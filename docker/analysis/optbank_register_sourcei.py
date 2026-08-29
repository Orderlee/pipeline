#!/usr/bin/env python3
"""sourcei-prompts 에 새 뱅크 등록 + emb_viz 확장 — `sourcei` 의 compare 패널에서 보이게.

앞선 실패의 진짜 원인은 좌표 용량이 아니라 **`ds.select(ids)` 뷰 스테이지에 60만 ObjectId 가
직렬화되는 것**이었다(그것만 14.5MB). 전체 데이터셋 순서로 주면 좌표는 바이너리로 들어가 넉넉하다.
 · 좌표를 아는 문장(같은 text 가 이미 있음) → 그 좌표 그대로
 · 모르는 문장(새로 생성한 것) → **NaN**. 날조 대신 '좌표 없음'을 명시한다(산점도에서 빠진다)
실행 전 백업하고, 끝나면 점 수·NaN 수를 검증한다.
"""
import os, sys, json, glob, time
sys.path.insert(0, "/workspace")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = "2"
import numpy as np, fiftyone as fo, fiftyone.brain as fob

BANKDIR = "/data/fiftyone/frames_bank/report/sourcei_gt/optbank"
VERSION = "vOPT.2026.08.28"; GIDX0 = 2900000
CLASSES = ["normal", "falldown", "fire", "smoke"]
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

bank = np.load(f"{BANKDIR}/optbank_vectors.npz", allow_pickle=True)
text = [str(x) for x in bank["text"]]; cls = [str(x) for x in bank["cls"]]; src = [str(x) for x in bank["src"]]
N = len(text)
ds = fo.load_dataset("sourcei-prompts")
ex = ds.match(fo.ViewField("bank_version.label") == VERSION)
if len(ex): ds.delete_samples(ex.values("id")); log(f"기존 {VERSION} {len(ex):,} 삭제")

res = ds.load_brain_results("emb_viz")
old_ids = [str(i) for i in res.sample_ids]; old_xy = np.asarray(res.points, dtype="float32")
bak = f"{BANKDIR}/emb_viz_backup_{time.strftime('%Y%m%d_%H%M%S')}.npz"
np.savez_compressed(bak, sample_ids=np.array(old_ids), points=old_xy)
log(f"백업 {bak} ({len(old_ids):,}점) · 샘플 {len(ds):,}")
assert len(old_ids) == len(ds), "시작 상태부터 불일치 — 중단"
sid2xy = {i: old_xy[k] for k, i in enumerate(old_ids)}
ids_all, texts_all = ds.values(["id", "text"])
text2xy = {}
for i, t in zip(ids_all, texts_all):
    if t and t not in text2xy and i in sid2xy: text2xy[t] = sid2xy[i]

opt = fo.load_dataset("sourcei-OPT-prompts")
ov = {s["text"]: s for s in opt}
samples = []
for i in range(N):
    o = ov.get(text[i])
    s = fo.Sample(filepath=o.filepath if o else fo.load_dataset("sourcei").first().filepath)
    s["text"] = text[i]
    s["category"] = fo.Classification(label=cls[i])
    s["bank_version"] = fo.Classification(label=VERSION)
    s["adopted"] = fo.Classification(label="채택")
    s["gidx"] = GIDX0 + i
    if o is not None:
        s["wins"] = int(o["wins"]); s["purity"] = o["purity"]
        s["wave_role"] = fo.Classification(label=str(o["src"].label))
    samples.append(s)
new_ids = ds.add_samples(samples)
log(f"{len(new_ids):,} 추가 → 총 {len(ds):,}")

cur_ids, cur_txt = ds.values(["id", "text"])
pts = np.full((len(cur_ids), 2), np.nan, dtype="float32")
nan_n = 0
for k, (i, t) in enumerate(zip(cur_ids, cur_txt)):
    if i in sid2xy: pts[k] = sid2xy[i]
    elif t in text2xy: pts[k] = text2xy[t]
    else: nan_n += 1
log(f"좌표 구성 {len(pts):,} · 좌표 없음(NaN) {nan_n}")
ds.delete_brain_run("emb_viz")
fob.compute_visualization(ds, points=pts, brain_key="emb_viz")
chk = ds.load_brain_results("emb_viz")
log(f"emb_viz 재작성 {len(chk.points):,}점 (샘플 {len(ds):,}) · NaN {int(np.isnan(np.asarray(chk.points)).any(1).sum())}")
assert len(chk.points) == len(ds)
json.dump(dict(version=VERSION, n=N, total=len(ds), nan=nan_n, backup=bak),
          open(f"{BANKDIR}/register_sourcei.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
