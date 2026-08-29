#!/usr/bin/env python3
"""sourcei-OPT 뱅크를 `sourcei-prompts` 에 등록 — 패널의 뱅크 버전 선택기에 뜨게 한다.

안전 근거 (코드로 확인함): `user-prompt-compare` 는 좌표↔메타를 **brain result 의 sample_ids
기준으로 명시 정렬**한다(플러그인 주석 §build bundle). 따라서 좌표 없는 샘플을 추가해도
기존 점이 밀리거나 크래시하지 않는다 — 새 샘플은 산점도에서 빠질 뿐이다.

⚠️ **emb_viz 는 건드리지 않는다.** 이 brain run 은 이미 MongoDB BSON 16MB 한도에 닿아 있다
   (603,318점 = 16.79MB, 한도 16.79MB). 2,000점을 더하면 문서가 한도를 넘어 **쓰기가 실패**하고,
   그 전에 `delete_brain_run` 을 하면 남의 패널이 죽는다 — 2026-08-28 실측으로 한 번 그렇게 됐고
   백업에서 복원했다. 게다가 `ds.select(ids)` 로 만들면 뷰 스테이지에 60만 ObjectId 가 들어가
   그것만으로도 14.5MB 라 한도를 넘는다(원본 run 이 전체 데이터셋 기준인 이유).
   → 새 문장은 **산점도에 안 찍히지만** 뱅크 버전 선택기·표·필터에는 정상적으로 나온다.
     플러그인이 좌표↔메타를 sample_ids 로 정렬하므로 기존 점은 밀리지 않는다(코드 확인).
그래도 실행 전 좌표를 npz 로 **백업**한다.

프레임 쪽 역방향 조인 필드도 같이 쓴다: `winner_gidx_vOPT20260828`
  (플러그인 `version_to_winner_field("vOPT.2026.08.28")` 와 문자 단위 동일)
"""
import os, sys, json, glob, collections, time
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
import numpy as np
import fiftyone as fo, fiftyone.brain as fob

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
BANKDIR = f"{OUT}/optbank"
VERSION = "vOPT.2026.08.28"
WFIELD = "winner_gidx_v" + "".join(VERSION.lstrip("vV").split("."))
GIDX0 = 2900000                       # 기존 최대 gidx 2,879,841 위 — 대역 충돌 없음
APPLY = "--apply" in sys.argv
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)
log(f"버전 {VERSION} · 조인 필드 {WFIELD} · gidx 시작 {GIDX0:,} · {'APPLY' if APPLY else 'DRY-RUN'}")

bank = np.load(f"{BANKDIR}/optbank_vectors.npz", allow_pickle=True)
text = [str(x) for x in bank["text"]]; cls = [str(x) for x in bank["cls"]]; src = [str(x) for x in bank["src"]]
N = len(text)
rc = json.load(open(f"{BANKDIR}/rulecheck.json"))

ds = fo.load_dataset("sourcei-prompts")
log(f"sourcei-prompts 샘플 {len(ds):,} · 버전 {len(ds.distinct('bank_version.label'))}종")
if VERSION in ds.distinct("bank_version.label"):
    log(f"⚠️ 이미 {VERSION} 이 있다 — 기존 샘플 삭제 후 재등록")
    if APPLY:
        old = ds.match(fo.ViewField("bank_version.label") == VERSION)
        ds.delete_samples(old.values("id")); log(f"   {len(old):,} 삭제")

# ── 기존 좌표 백업 + text→좌표 맵 ───────────────────────────────────
res = ds.load_brain_results("emb_viz")
old_ids = [str(i) for i in res.sample_ids]
old_xy = np.asarray(res.points, dtype="float32")
bak = f"{BANKDIR}/emb_viz_backup_{time.strftime('%Y%m%d_%H%M%S')}.npz"
np.savez_compressed(bak, sample_ids=np.array(old_ids), points=old_xy)
log(f"기존 emb_viz 백업 → {bak} ({len(old_ids):,}점)")
sid2xy = {i: old_xy[k] for k, i in enumerate(old_ids)}
ids_all, texts_all = ds.values(["id", "text"])
text2xy = {}
for i, t in zip(ids_all, texts_all):
    if t and i in sid2xy and t not in text2xy: text2xy[t] = sid2xy[i]
log(f"text→좌표 맵 {len(text2xy):,}")
matched = sum(1 for t in text if t in text2xy)
log(f"새 문장 {N} 중 좌표 재사용 가능 {matched} ({matched/N:.0%}) · 좌표 없음 {N-matched}")

if not APPLY:
    log("DRY-RUN — --apply 를 주면 실제로 등록한다"); print("DONE"); sys.exit(0)

# ── 새 샘플 추가 ────────────────────────────────────────────────────
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
log(f"샘플 {len(new_ids):,} 추가 → 총 {len(ds):,}")

# ── emb_viz 는 그대로 둔다 (위 주석의 16MB 한도) ─────────────────────
chk = ds.load_brain_results("emb_viz")
log(f"emb_viz 유지 {len(chk.points):,}점 — 새 문장 {N}개는 산점도 미표시(선택기·표는 정상)")
assert len(chk.points) == len(old_ids), "emb_viz 가 변경됐다 — 백업에서 복원할 것"

# ── 프레임 쪽 조인 필드 ─────────────────────────────────────────────
dh = fo.load_dataset("sourcei")
pred_npz = np.load(f"{BANKDIR}/optbank_sourcei_pred.npz", allow_pickle=True)
V = bank["vecs"].astype(np.float32)
hid, hemb = dh.values(["id", "embedding"])
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
CLASSES = ["normal", "falldown", "fire", "smoke"]
lab_s = np.array([CLASSES.index(c) for c in cls]); pred = pred_npz["pred"]
win = np.empty(len(pred), np.int64)
for s0 in range(0, len(FH), 1500):
    S = FH[s0:s0 + 1500] @ V.T
    for r in range(S.shape[0]):
        m = np.where(lab_s == int(pred[s0 + r]))[0]
        win[s0 + r] = GIDX0 + int(m[np.argmax(S[r, m])])
dh.set_values(WFIELD, win.tolist())
dh.save()
log(f"프레임 조인 필드 {WFIELD} 기록 ({len(win):,})")
json.dump(dict(version=VERSION, winner_field=WFIELD, gidx0=GIDX0, n=N,
               emb_viz="유지(16MB 한도로 확장 불가) — 새 문장은 산점도 미표시", backup=bak),
          open(f"{BANKDIR}/register.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
