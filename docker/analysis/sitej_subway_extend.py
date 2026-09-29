#!/usr/bin/env python3
"""기존 `sitej_subway`(373) 에 certbody 원본 프레임을 **겹치지 않는 것만** 추가한다.

덮어쓰지 않는 이유: 이 데이터셋에는 `emb_viz` UMAP 과 프롬프트 뱅크 평가 2회분
(`pred_vGEN_2026_08_28` / `_09_04`, `cos_best_*`, `attached_bank`)이 붙어 있다.

⚠️ **두 라벨링 규약이 한 데이터셋에 섞인다.** 겹치는 285장에서 기존 라벨과 이 투영의
일치는 **79.3%**(타이브레이크를 smoke 우선으로 맞춰도 84.6%) 다. 원인 분해:
  - 38건 = fire·smoke 중첩 구간의 타이브레이크 차이 (오류 아님)
  - 21건 = 설명 안 됨 → 기존 373장은 이 JSON 의 순수 구간투영으로 재현되지 않는다
그래서 `label_rule` 필드로 출처를 명시하고, **섞어서 GT 로 쓰지 못하게** 한다.

⚠️ `intrusion` 은 기존 데이터셋 철자 `intrustion` 에 맞춘다. 올바른 철자를 쓰면 한 클래스가
둘로 쪼개져 `cos_best_intrustion` 등 기존 필드와의 연결도 끊긴다 — 조용한 오답이 된다.

⚠️ 새로 추가된 표본에는 `emb_viz` UMAP 좌표가 없다 (recompute_viz.py 별도 실행 필요).
"""
import json
import os
import unicodedata as u

import fiftyone as fo
import numpy as np

SRC = "/data/fiftyone/uploads/sitej_certbody"
NPZ = "/data/fiftyone/frames_bank/report/sourcea/sitej_certbody.npz"
SPELL = {"intrusion": "intrustion"}  # 기존 데이터셋 철자에 맞춘다(클래스 분열 방지)
N = lambda s: u.normalize("NFC", s)  # noqa: E731


def main():
    ds = fo.load_dataset("sitej_subway")
    have = {N(os.path.basename(f)) for f in ds.values("filepath")}
    print(f"기존 {len(ds)} 샘플")

    d = np.load(NPZ, allow_pickle=True)
    vec = {N(str(k)): v for k, v in zip(d["ids"], d["vectors"])}
    rows = [json.loads(l) for l in open(f"{SRC}/manifest.jsonl", encoding="utf-8")]

    # 기존 표본에 출처를 먼저 박는다 — 안 박으면 나중에 구분이 불가능해진다
    if "label_rule" not in ds.get_field_schema():
        ds.add_sample_field("label_rule", fo.StringField)
    ds.set_values("label_rule", ["original"] * len(ds))

    new, skip = [], 0
    for r in rows:
        key = N(r["key"])
        base = os.path.basename(key)
        if base in have:
            skip += 1
            continue
        v = vec.get(key)
        if v is None:
            skip += 1
            continue
        lab = SPELL.get(r["cls"], r["cls"])
        s = fo.Sample(filepath=f"{SRC}/images/{key}")
        s["ground_truth"] = fo.Classification(label=lab)
        s["embedding"] = [float(x) for x in v]
        s["camera"] = N(r["camera"])
        s["session"] = r["session"]          # ← 홀드아웃 단위. 카메라로 나누면 누수다
        s["video_stem"] = N(r["video_stem"])
        s["t_sec"] = float(r["t_sec"])
        s["ambiguous"] = bool(r["ambiguous"])
        s["boundary"] = bool(r["boundary"])
        s["n_passes"] = int(r["n_passes"])
        s["label_rule"] = "interval_projection_v1"
        new.append(s)

    print(f"추가 {len(new)} · 기존과 겹쳐 건너뜀 {skip}")
    if new:
        ds.add_samples(new)
    ds.save()

    import collections
    gts, rules = ds.values(["ground_truth.label", "label_rule"])
    print(f"\n최종 {len(ds)} 샘플")
    print("  클래스:", dict(collections.Counter(gts)))
    print("  라벨규약:", dict(collections.Counter(rules)))
    ses = [s for s in ds.values("session") if s]
    print(f"  세션 필드 보유 {len(ses)} · distinct {len(set(ses))}")


if __name__ == "__main__":
    main()
