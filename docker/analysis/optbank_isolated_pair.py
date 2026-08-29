#!/usr/bin/env python3
"""compare 패널에서 새 뱅크를 보기 위한 **격리 짝 데이터셋**.

왜 이 방법인가 (2026-08-28 실측으로 두 길이 막혔다):
 1) `sourcei-prompts` 에 문장을 추가해도 **패널 버전 목록에 안 뜬다** — 패널은 번들을
    `emb_viz` 의 `sample_ids` 기준으로 만들어서, 좌표 없는 샘플은 메타까지 통째로 빠진다.
 2) 그럼 emb_viz 를 늘리면 되는데 **MongoDB BSON 16MB 한도**에 이미 닿아 있다
    (603,318점 = 16.79MB). 2,000점을 더하면 쓰기가 실패하고, 그 전에 delete 하면 남의 패널이
    죽는다(실제로 한 번 그렇게 됐고 백업에서 복원했다).

→ 그래서 건드리지 않고 **격리**한다. 패널은 프롬프트 데이터셋을 `<dataset>-prompts` 로 유도하므로
   `sourcei-OPT`(프레임 클론) + `sourcei-OPT-prompts`(문장 2,000, 자체 emb_viz) 짝을 만들면
   그 안에서 새 뱅크만 깨끗하게 보인다. 기존 데이터셋은 원본 그대로 남는다.
"""
import os, sys, json, time
sys.path.insert(0, "/workspace")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = "2"
import numpy as np, fiftyone as fo, fiftyone.brain as fob

BANKDIR = "/data/fiftyone/frames_bank/report/sourcei_gt/optbank"
VERSION = "vOPT.2026.08.28"; GIDX0 = 2900000
WFIELD = "winner_gidx_v" + "".join(VERSION.lstrip("vV").split("."))
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

# 1) sourcei-prompts 에 넣었던 샘플 되돌리기 — 보이지도 않는데 남겨두면 혼란만 준다
hp = fo.load_dataset("sourcei-prompts")
ex = hp.match(fo.ViewField("bank_version.label") == VERSION)
if len(ex):
    hp.delete_samples(ex.values("id")); log(f"sourcei-prompts 에서 {len(ex):,} 되돌림 → {len(hp):,}")
r = hp.load_brain_results("emb_viz")
assert len(r.points) == len(hp), f"emb_viz {len(r.points)} vs 샘플 {len(hp)} — 불일치"
log(f"sourcei-prompts 원상 확인: 샘플 {len(hp):,} · emb_viz {len(r.points):,}")

# 2) 문장 데이터셋 gidx 를 프레임 조인 필드와 맞춘다
op = fo.load_dataset("sourcei-OPT-prompts")
bank = np.load(f"{BANKDIR}/optbank_vectors.npz", allow_pickle=True)
text = [str(x) for x in bank["text"]]
t2i = {t: i for i, t in enumerate(text)}
ids, txts = op.values(["id", "text"])
op.set_values("gidx", [GIDX0 + t2i[t] for t in txts])
op.save()
log(f"sourcei-OPT-prompts gidx 재설정 (시작 {GIDX0:,}) · 샘플 {len(op):,} · emb_viz {len(op.load_brain_results('emb_viz').points):,}")

# 3) 프레임 클론
if fo.dataset_exists("sourcei-OPT"): fo.delete_dataset("sourcei-OPT")
src = fo.load_dataset("sourcei")
clone = src.clone("sourcei-OPT", persistent=True)
log(f"sourcei-OPT 클론 {len(clone):,} · 조인 필드 있음: {WFIELD in clone.get_field_schema()}")
clone.info = dict(clone.info or {}, optbank_pair=dict(
    prompts="sourcei-OPT-prompts", version=VERSION, winner_field=WFIELD,
    note="compare 패널 전용 격리 짝. 원본 sourcei/sourcei-prompts 는 손대지 않았다."))
clone.save()
json.dump(dict(frames="sourcei-OPT", prompts="sourcei-OPT-prompts", version=VERSION,
               winner_field=WFIELD, gidx0=GIDX0), open(f"{BANKDIR}/pair.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
