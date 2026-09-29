#!/usr/bin/env python3
"""원장(`ledger.jsonl`) + `embed.npz` 를 FiftyOne 의 현재 폴더/GT 에 맞춘다.

`gt_folder_resync.py` 는 FiftyOne 의 `ground_truth` 만 고친다. 하지만
`prompt_geometry.py` 의 모든 스테이지는 GT 를 **원장에서** 받으므로
(`load_all()` → `jsonl_load(f"{WORK}/ledger.jsonl")`), 원장을 같이 옮기지 않으면
`wave`/`promptmap` 이 옛 GT·옛 모집단으로 문장 지표를 다시 굽는다.

원장 키 = `<부모폴더>/<파일명>` 이라 **폴더 이동이 키를 바꾼다.** 그래서 이동은
"삭제 + 신규" 로 보이고, 신규 키에는 벡터가 없다 → 조용히 표본에서 빠진다.
여기서 하는 일:

  · 이동 (basename 짝) → 키를 새 폴더로 갈고 `gt_class` 를 폴더에서 다시 딴다.
                          벡터는 이미지 함수라 안 변하므로 `embed.npz` 키만 같이 간다.
  · 삭제 (짝 없음)     → 원장 행과 embed.npz 행을 같이 뺀다.
  · 잔류               → GT 를 FiftyOne 현재값으로 맞춘다 (보통 이미 같다).

basename 이 양쪽에서 유일할 때만 이동으로 본다 — 겹치면 어느 쪽으로 옮겼는지
결정할 수 없어서 삭제로 떨어뜨린다 (조용한 오귀속보다 낫다).

기본 dry-run. `--apply` 는 두 파일을 `.bak.<ts>` 로 먼저 복사한다.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import shutil
import sys
import time

import numpy as np

CLASS_IDS = {"normal": 0, "falldown": 1, "fire": 2, "smoke": 3}


def frame_key(path: str) -> str:
    return f"{os.path.basename(os.path.dirname(path))}/{os.path.basename(path)}"


def unique_basename_map(keys) -> dict[str, str]:
    """basename → key, **유일한 것만**. 중복 basename 은 빼서 오귀속을 막는다."""
    seen = collections.Counter(os.path.basename(k) for k in keys)
    return {os.path.basename(k): k for k in keys if seen[os.path.basename(k)] == 1}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("dataset", nargs="?", default="sourcei")
    ap.add_argument("--work", default=None, help="기본: /data/fiftyone/<소문자 dataset>/work")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    work = args.work or f"/data/fiftyone/{args.dataset.lower()}/work"
    led_path, npz_path = f"{work}/ledger.jsonl", f"{work}/embed.npz"
    for p in (led_path, npz_path):
        if not os.path.exists(p):
            print(f"없음: {p}", file=sys.stderr)
            return 2

    import fiftyone as fo
    ds = fo.load_dataset(args.dataset)
    fps, gts = ds.values(["filepath", "ground_truth.label"])
    cur: dict[str, str] = {}
    for p, g in zip(fps, gts):
        if g:
            cur[frame_key(str(p))] = str(g)

    rows = []
    with open(led_path) as fh:
        for ln in fh:
            ln = ln.strip()
            if ln:
                rows.append(json.loads(ln))
    led = {r["key"]: r for r in rows}

    only_led = set(led) - set(cur)
    only_cur = set(cur) - set(led)
    bl, bc = unique_basename_map(only_led), unique_basename_map(only_cur)
    moved = {bl[b]: bc[b] for b in (set(bl) & set(bc))}          # 옛 키 → 새 키
    dropped = only_led - set(moved)
    unmatched_new = only_cur - set(moved.values())

    # 새 원장 조립 — 입력 순서 보존
    out, n_gt_fix = [], 0
    for r in rows:
        k = r["key"]
        if k in dropped:
            continue
        nk = moved.get(k, k)
        lab = cur.get(nk)
        if lab is None:                                          # 방어: 짝은 맞았는데 GT 없음
            continue
        r = dict(r)
        r["key"] = nk
        want = CLASS_IDS.get(lab)
        if want is not None and r.get("gt_class") != want:
            r["gt_class"] = want
            r["gt_source"] = "folder"                            # 근거가 폴더로 바뀜
            n_gt_fix += 1
        out.append(r)

    z = np.load(npz_path, allow_pickle=True)
    zkeys = [str(k) for k in z["key"]]
    keep = [i for i, k in enumerate(zkeys) if k not in dropped]
    new_zkeys = [moved.get(zkeys[i], zkeys[i]) for i in keep]

    print(f"원장 {len(rows):,} → {len(out):,}   ·   embed.npz {len(zkeys):,} → {len(keep):,}")
    print(f"  이동(키 재작성) {len(moved):,}   삭제 {len(dropped):,}   GT 값 정정 {n_gt_fix:,}")
    if moved:
        c = collections.Counter((k.split("/")[0], v.split("/")[0]) for k, v in moved.items())
        for (a, b), n in c.most_common():
            print(f"     {a:9s} → {b:9s} {n:>5,}")
    if unmatched_new:
        print(f"  ⚠️ 원장에 없고 짝도 못 찾은 FiftyOne 표본 {len(unmatched_new):,} — 분석 표본 밖으로 남는다")
    covered = set(new_zkeys) & {r["key"] for r in out}
    print(f"  결과 커버리지: 원장 {len(out):,} 중 벡터 있는 것 {len(covered):,}")
    if len(covered) != len(out):
        print(f"  ⚠️ 벡터 없는 원장 행 {len(out) - len(covered):,} — load_all 이 조용히 뺀다")

    if not args.apply:
        print("\n(dry-run — 반영하려면 --apply)")
        return 0

    # 변경이 없으면 쓰지 않는다. 예전엔 무조건 써서 `.bak.<ts>` 가 실행마다 쌓였고,
    # 30분 타이머(`refresh_sourcei_chain.sh`)에 걸면 하루 48개 × 14MB 씩 늘어난다
    # (실측: 손실행 10회로 이미 143MB). 원장 지문도 안 바뀌므로 체인의 빠른 경로가
    # 계속 성립한다.
    if not moved and not dropped and not n_gt_fix and len(out) == len(rows):
        print("변경 없음 — 쓰지 않는다 (백업도 만들지 않음)")
        return 0

    ts = int(time.time())
    shutil.copy2(led_path, f"{led_path}.bak.{ts}")
    shutil.copy2(npz_path, f"{npz_path}.bak.{ts}")
    tmp = f"{led_path}.tmp"
    with open(tmp, "w") as fh:
        for r in out:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, led_path)
    payload = {k: z[k] for k in z.files}
    payload["key"] = np.array(new_zkeys, dtype=object)
    for f in z.files:
        if f != "key" and getattr(z[f], "shape", (0,))[:1] == (len(zkeys),):
            payload[f] = z[f][keep]
    np.savez(npz_path, **payload)
    print(f"\n반영 완료 · 백업 {led_path}.bak.{ts} / {npz_path}.bak.{ts}")
    return 0


def demo() -> None:
    """ponytail: basename 유일성 가드와 이동/삭제 분류만 검증 — 나머지는 파일 IO 다."""
    assert unique_basename_map(["a/x.jpg", "b/y.jpg"]) == {"x.jpg": "a/x.jpg", "y.jpg": "b/y.jpg"}
    # 같은 basename 이 두 폴더에 있으면 둘 다 빠진다
    assert unique_basename_map(["a/x.jpg", "b/x.jpg"]) == {}
    assert frame_key("/n/fire/z.jpg") == "fire/z.jpg"
    print("demo OK")


if __name__ == "__main__":
    sys.exit(demo() if "--demo" in sys.argv else main())
