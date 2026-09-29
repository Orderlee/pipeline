#!/usr/bin/env python3
"""`<dataset>-prompts` 의 **문장 지표만** 현 원장 기준으로 제자리 갱신한다.

`prompt_geometry.stage_promptmap` 이 정본이지만 그건 `fo.Dataset(name, overwrite=True)`
로 데이터셋을 통째로 재생성한다 — emb_viz 좌표(60만 행), 별도 스크립트로 등록된
vOPT/vGEN 문장, 저장뷰가 같이 날아간다. 게다가 `rebuild_sourcei.py` 의 semantic sort 는
현 PROMPT_DIR 에서 `int("OPT")` 로 죽는다.

여기서는 GT·모집단에 의존하는 필드만 다시 계산해 덮는다:
    wins / purity / purity_tier / n_cameras / adopted

`wave_gain`·`nearest_*`·`match` 는 건드리지 않는다 — 앞의 둘은 `wave` npz 재계산이,
`match` 는 최근접 프레임 재탐색이 필요하고 그건 promptmap 의 일이다. 갱신 안 한 필드를
조용히 남기지 않도록 마지막에 어긋난 필드를 출력한다.

⚠️ gidx 오프셋은 env(`BANK_LIST`) 순서에 의존한다 — 실행마다 달라지는 값이라
   추측하지 않고 **기존 표본의 gidx 에서 버전별 블록을 역산**한다
   (memory: gidx 오프셋 세대 버그). 조인은 (버전, gidx % GIDX_OFFSET) 쌍으로만 한다.
"""
from __future__ import annotations

import argparse
import collections
import os
import resource
import sys

import numpy as np

resource.setrlimit(resource.RLIMIT_AS, (16 * 2**30, 16 * 2**30))
sys.path.insert(0, "/workspace")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="sourcei")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    import fiftyone as fo

    pds_name = None
    # 어떤 버전이 실제로 데이터셋에 있는지 먼저 읽어 BANK_LIST 를 만든다 (load_bank 대상).
    import prompt_geometry as _pg0
    pds_name = f"{_pg0.PROFILES[args.profile]['dataset']}-prompts"
    pds = fo.load_dataset(pds_name)
    ids, gidx_all, ver_all = pds.values(["id", "gidx", "bank_version.label"])
    present = sorted({v for v in ver_all if v})
    os.environ["BANK_LIST"] = ",".join(present)

    import importlib
    pg = importlib.reload(_pg0)
    pg.set_profile(args.profile)
    OFF = pg.GIDX_OFFSET

    # 버전별 gidx 블록을 데이터에서 역산 — env 순서와 무관해진다.
    blocks: dict[str, set[int]] = collections.defaultdict(set)
    for g, v in zip(gidx_all, ver_all):
        if g is not None and v:
            blocks[v].add(int(g) // OFF)
    bad = {v: sorted(b) for v, b in blocks.items() if len(b) != 1}
    if bad:
        print(f"⚠️ 한 버전이 여러 gidx 블록에 걸쳐 있다 — 조인 불가: {bad}", file=sys.stderr)
        return 2
    block = {v: next(iter(b)) for v, b in blocks.items()}

    keys, X, gt, src, banks = pg.load_all()
    cam = pg.load_cameras(keys)
    print(f"원장 프레임 {len(keys):,} · 뱅크 {len(banks)}종 · {pds_name} {len(ids):,}행")

    # (버전, 로컬 gidx) → 표본 id
    slot: dict[tuple[str, int], str] = {}
    for sid, g, v in zip(ids, gidx_all, ver_all):
        if g is not None and v:
            slot[(v, int(g) % OFF)] = sid

    upd = {f: {} for f in ("wins", "purity", "n_cameras")}
    cls_upd = {f: {} for f in ("purity_tier", "adopted")}
    n_missing = 0
    for v in present:
        bank = banks.get(v)
        if bank is None:
            print(f"  {v}: 뱅크 npz 없음 — 건너뜀")
            continue
        cls = bank["cls"]
        classes = sorted(set(cls.tolist()))
        b1, _, a1 = pg.bank_top2_stream(X, bank)
        M = np.stack([b1[c] for c in classes], axis=1)
        pred = np.array(classes)[M.argmax(axis=1)]
        gmap = {c: np.flatnonzero(cls == c) for c in classes}
        win_g = np.array([gmap[int(c)][a1[int(c)][i]] for i, c in enumerate(pred)])
        won: dict[int, list[int]] = collections.defaultdict(list)
        for i, g in enumerate(win_g.tolist()):
            won[g].append(i)
        for g in range(len(cls)):
            sid = slot.get((v, g))
            if sid is None:
                n_missing += 1
                continue
            fr = won.get(g, [])
            upd["wins"][sid] = len(fr)
            if fr:
                p = float((gt[fr] == int(cls[g])).mean())
                upd["purity"][sid] = round(p, 4)
                upd["n_cameras"][sid] = len(set(cam[fr].tolist()))
                cls_upd["purity_tier"][sid] = fo.Classification(label=pg.purity_bin(p))
            else:
                upd["purity"][sid] = None
                upd["n_cameras"][sid] = None
                cls_upd["purity_tier"][sid] = None
            cls_upd["adopted"][sid] = fo.Classification(label="채택" if fr else "미채택")
        print(f"  {v:22s} 문장 {len(cls):>6,} · 채택 {len(won):>5,} · 슬롯 {sum(1 for g in range(len(cls)) if (v,g) in slot):>6,}")

    tot = sum(v for v in upd["wins"].values())
    print(f"\nsum(wins)={tot:,}  (버전수 × 원장 프레임 = {len(present)}×{len(keys):,}={len(present)*len(keys):,})")
    if n_missing:
        print(f"⚠️ 데이터셋에 슬롯이 없는 (버전,gidx) {n_missing:,} — promptmap 재빌드 대상")
    print("갱신 안 한 GT 의존 필드: wave_gain / wave_role / match / nearest_* "
          "(promptmap 전용 — 필요하면 별도 결정)")

    if not args.apply:
        print("\n(dry-run — 반영하려면 --apply)")
        return 0
    for f, d in upd.items():
        pds.set_values(f, d, key_field="id")
        print(f"  set {f} {len(d):,}행")
    for f, d in cls_upd.items():
        pds.set_values(f, d, key_field="id")
        print(f"  set {f} {len(d):,}행")
    pds.save()
    print("반영 완료")
    return 0


if __name__ == "__main__":
    sys.exit(main())
