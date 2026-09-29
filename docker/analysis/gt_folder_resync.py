#!/usr/bin/env python3
"""폴더 기준 ground_truth 재동기화 — 어떤 경로로 옮겼든 폴더↔GT 불일치를 맞춘다.

왜 필요한가: 이 데이터셋에서 **폴더는 정식 GT 출처**다 (`gt_source` 실측 2026-08-31:
caption 4,356 · filename 1,695 · **folder 1,288** · none 42). 그런데 App 의
`미디어 파일 이동`(user-embeddings `move_media`) 은 파일과 `filepath` 만 옮기고 라벨은
다시 유도하지 않는다 — 그 오퍼레이터의 docstring 이 "예: frames/falldown → frames/normal
**오분류 정정**" 이라고 밝히는 용도인데도 그렇다. 그래서 옮긴 뒤 GT 가 옛 클래스로 남는다
(사용자 리포트: "normal 폴더에서 fire 폴더로 이동했는데 아직 GT 가 normal").

⚠️ **허용 클래스를 하드코딩하지 않는다.** 데이터셋에 실재하는 `ground_truth` 라벨 집합을
   그대로 쓴다. 정본은 `src/vlm_pipeline/data/label_ontology.json` 인데 이 컨테이너에
   마운트되지 않고, 목록을 여기 베끼면 정본의 6번째 복사본이 된다(드리프트 이력 있음).
   부작용: 아직 한 장도 없는 새 클래스 폴더는 건너뛴다 — 조용히 넘기지 않고 리포트한다.

⚠️ **GT 를 바꾸면 파생 필드가 stale 된다** (`pred_*`·`probe_*`·`optbank_*`·`cos_best_*`·
   `close_call`·`runner_up`·`match` 등 179개 — 전부 옛 GT 기준 계산값). 바꾼 장수를
   리포트하니 규모를 보고 `prompt_geometry.py` attach 재실행을 판단할 것.

사용:
    python3 gt_folder_resync.py sourcei                 # dry-run (기본)
    python3 gt_folder_resync.py sourcei --apply         # 적용 + 되돌리기 파일 기록
    python3 gt_folder_resync.py sourcei --revert <file> # 되돌리기
"""
import argparse
import collections
import json
import os
import sys
import time

import fiftyone as fo

GT_FIELD = "ground_truth"
SRC_FIELD = "gt_source"
SRC_LABEL = "folder"


def folder_of(path):
    return os.path.basename(os.path.dirname(str(path)))


def scan(ds):
    """(불일치 목록, 허용 클래스, 클래스 아닌 폴더 카운트)."""
    ids = [str(i) for i in ds.values("id")]
    fps = [str(p) for p in ds.values("filepath")]
    gts = [None if g is None else str(g) for g in ds.values(f"{GT_FIELD}.label")]
    allowed = {g for g in gts if g}
    rows, skipped = [], collections.Counter()
    for i, p, g in zip(ids, fps, gts):
        f = folder_of(p)
        if f == g:
            continue
        if f not in allowed:
            skipped[f] += 1          # 클래스 폴더가 아님 — 정리용 이동으로 보고 건드리지 않는다
            continue
        rows.append({"id": i, "file": os.path.basename(p), "from": g, "to": f})
    return rows, allowed, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dataset")
    ap.add_argument("--apply", action="store_true", help="실제로 반영 (기본은 dry-run)")
    ap.add_argument("--revert", metavar="FILE", help="이 파일로 되돌린다")
    a = ap.parse_args()

    ds = fo.load_dataset(a.dataset)

    if a.revert:
        recs = json.load(open(a.revert, encoding="utf-8"))["changes"]
        for r in recs:
            s = ds[r["id"]]
            s[GT_FIELD] = fo.Classification(label=r["from"])
            if r.get("src_from") is not None:
                s[SRC_FIELD] = fo.Classification(label=r["src_from"])
            s.save()
        print(f"되돌림: {len(recs):,}장")
        return

    rows, allowed, skipped = scan(ds)
    print(f"데이터셋 {a.dataset}: {ds.count():,}장 · 허용 클래스(기존 GT 라벨) {sorted(allowed)}")
    if skipped:
        print(f"클래스 아닌 폴더는 건너뜀: {dict(skipped)}")
    if not rows:
        print("폴더↔GT 불일치 없음 — 할 일 없음")
        return

    print(f"\n불일치 {len(rows):,}장:")
    for (a_, b_), n in collections.Counter((r["from"], r["to"]) for r in rows).most_common():
        print(f"  {a_:10s} → {b_:10s} {n:,}장")
    for r in rows[:5]:
        print(f"    · {r['file'][:52]}  {r['from']} → {r['to']}")
    if len(rows) > 5:
        print(f"    … 외 {len(rows)-5:,}장")

    if not a.apply:
        print("\n[dry-run] 반영하려면 --apply")
        return

    out = f"/workspace/_gt_resync_revert_{a.dataset}_{int(time.time())}.json"
    for r in rows:
        s = ds[r["id"]]
        cur = s[SRC_FIELD]
        r["src_from"] = getattr(cur, "label", None) if cur is not None else None
        s[GT_FIELD] = fo.Classification(label=r["to"])
        s[SRC_FIELD] = fo.Classification(label=SRC_LABEL)   # 출처를 남긴다 (caption/filename 과 구분)
        s.save()
    json.dump({"dataset": a.dataset, "changes": rows}, open(out, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(f"\n적용 {len(rows):,}장 · gt_source='{SRC_LABEL}' 기록")
    print(f"되돌리기: python3 gt_folder_resync.py {a.dataset} --revert {out}")
    print(f"⚠️ GT 기준 파생 필드({len(rows):,}장분)가 stale — 규모가 크면 prompt_geometry attach 재실행")


if __name__ == "__main__":
    sys.exit(main())
