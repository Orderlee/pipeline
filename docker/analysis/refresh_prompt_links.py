#!/usr/bin/env python3
"""`<dataset>-prompts` 의 **최근접 프레임 연결과 wave 축**을 제자리 갱신한다.

`refresh_sentence_metrics.py` 가 top-k 축(`wins`/`purity`/`n_cameras`/`adopted`)을 맡고,
이 스크립트가 나머지 두 축을 맡는다.

  · 최근접 프레임 — `filepath`(썸네일) · `nearest_key` · `nearest_gt` · `match` ·
    `nearest_{environment,daynight,person}`
  · wave 축 — `wave_gain` · `wave_role`  (`stage_wave` 가 만든 npz 를 읽는다)

둘 다 정본은 `stage_promptmap` 이지만 그건 `fo.Dataset(name, overwrite=True)` 라 데이터셋을
통째로 재생성한다 — emb_viz 60만 좌표, 별도 스크립트로 등록된 vOPT/vGEN 문장, 저장뷰가 같이
날아간다. 그래서 같은 계산을 하고 값만 덮는다.

**왜 필요한가**: 프레임이 지워지면 그 프레임을 가리키던 문장의 썸네일이 깨진다
(실측 2026-09-03: 607,318 문장 중 **173,683(28.6%)** 가 삭제된 프레임을 가리키고 있었다).
GT 가 바뀌면 `nearest_gt`·`match` 도 같이 틀어진다.

⚠️ gidx 오프셋은 런타임 env 의존이라 추측하지 않고 **기존 표본의 gidx 에서 버전별 블록을
역산**한다 (`refresh_sentence_metrics.py` 와 같은 계약).
"""
from __future__ import annotations

import argparse
import collections
import glob
import os
import resource
import sys

import numpy as np

resource.setrlimit(resource.RLIMIT_AS, (16 * 2**30, 16 * 2**30))
sys.path.insert(0, "/workspace")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, os.environ.get("COS_THREADS", "3"))

ATTR_AXES = ("environment", "daynight", "person")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="sourcei")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--no-filepath", action="store_true",
                    help="썸네일 경로는 건드리지 않는다 (기본은 갱신)")
    a = ap.parse_args()

    import fiftyone as fo
    import prompt_geometry as _pg0

    pds_name = f"{_pg0.PROFILES[a.profile]['dataset']}-prompts"
    pds = fo.load_dataset(pds_name)
    ids, gidx_all, ver_all = pds.values(["id", "gidx", "bank_version.label"])
    present = sorted({v for v in ver_all if v})
    os.environ["BANK_LIST"] = ",".join(present)

    import importlib
    pg = importlib.reload(_pg0)
    pg.set_profile(a.profile)
    OFF, CN = pg.GIDX_OFFSET, pg.CLASS_NAMES

    blocks: dict[str, set[int]] = collections.defaultdict(set)
    for g, v in zip(gidx_all, ver_all):
        if g is not None and v:
            blocks[v].add(int(g) // OFF)
    bad = {v: sorted(b) for v, b in blocks.items() if len(b) != 1}
    if bad:
        print(f"⚠️ 한 버전이 여러 gidx 블록에 걸쳐 있다 — 조인 불가: {bad}", file=sys.stderr)
        return 2
    slot: dict[tuple[str, int], str] = {}
    for sid, g, v in zip(ids, gidx_all, ver_all):
        if g is not None and v:
            slot[(v, int(g) % OFF)] = sid

    keys, X, gt, src, banks = pg.load_all()
    fds = fo.load_dataset(pg.PROFILES[a.profile]["dataset"])
    sch = fds.get_field_schema()
    fps = fds.values("filepath")
    fkeys = [f"{os.path.basename(os.path.dirname(p))}/{os.path.basename(p)}" for p in map(str, fps)]
    key2fp = dict(zip(fkeys, map(str, fps)))
    key2attr: dict[str, dict] = {}
    for ax in ATTR_AXES:
        fld = f"db_{ax}" if f"db_{ax}" in sch else (ax if ax in sch else None)
        if not fld:
            continue
        for k, lab in zip(fkeys, fds.values(f"{fld}.label")):
            if lab and k:
                key2attr.setdefault(k, {})[ax] = str(lab)
    print(f"{pds_name} {len(ids):,}행 · 버전 {len(present)}종 · 원장 프레임 {len(keys):,} "
          f"· 씬 속성 {len(key2attr):,}장분")

    upd: dict[str, dict] = {f: {} for f in ("filepath", "nearest_key", "wave_gain")}
    cls_upd: dict[str, dict] = {f: {} for f in
                                ["nearest_gt", "match", "wave_role"] + [f"nearest_{x}" for x in ATTR_AXES]}
    n_missing = n_nowave = 0
    for v in present:
        bank = banks.get(v)
        if bank is None:
            print(f"  {v}: 뱅크 npz 없음 — 건너뜀")
            continue
        P, cls = bank["vec"], bank["cls"]
        ncos, nidx = pg.nearest_frame_stream(X, P)

        wpath = f"{pg.GEO}/wave_{pg.vtag(v)}.npz"
        wgain = wrole = None
        if os.path.exists(wpath):
            z = np.load(wpath)
            wgain = z["gain"]
            if len(wgain) != len(cls):
                print(f"  {v}: wave npz 길이 {len(wgain):,} != 문장 {len(cls):,} — wave 축 생략")
                wgain = None
        if wgain is None:
            n_nowave += 1
        else:
            # promptmap 과 같은 규칙: normal 은 부호를 뒤집고 클래스 내 백분위로 층화
            signed = np.where(cls == 0, -wgain, wgain)
            wrole = np.full(len(cls), "중간", dtype=object)
            for c in sorted(set(cls.tolist())):
                gi = np.flatnonzero(cls == c)
                lo_q, hi_q = np.percentile(signed[gi], [10, 90])
                wrole[gi[(signed[gi] >= hi_q) & (signed[gi] > 0)]] = "유익 상위10%"
                wrole[gi[(signed[gi] <= lo_q) & (signed[gi] < 0)]] = "유해 하위10%"

        hit = 0
        for g in range(len(cls)):
            sid = slot.get((v, g))
            if sid is None:
                n_missing += 1
                continue
            hit += 1
            k = keys[int(nidx[g])]
            fp = key2fp.get(k)
            c = int(cls[g])
            ngt = int(gt[int(nidx[g])])
            if fp and not a.no_filepath:
                upd["filepath"][sid] = fp
            upd["nearest_key"][sid] = k
            cls_upd["nearest_gt"][sid] = fo.Classification(
                label=CN.get(ngt, "no_gt"), confidence=float(ncos[g]))
            cls_upd["match"][sid] = fo.Classification(
                label=("no_gt" if ngt < 0 else ("hit" if ngt == c else "miss")))
            for ax, lab in (key2attr.get(k) or {}).items():
                cls_upd[f"nearest_{ax}"][sid] = fo.Classification(label=lab)
            if wgain is not None:
                upd["wave_gain"][sid] = float(wgain[g])
                cls_upd["wave_role"][sid] = fo.Classification(label=str(wrole[g]))
        print(f"  {v:22s} 문장 {len(cls):>6,} · 슬롯 {hit:>6,} · "
              f"최근접 cos 중앙 {float(np.median(ncos)):.4f} · wave {'있음' if wgain is not None else '없음'}")

    if n_missing:
        print(f"⚠️ 데이터셋에 슬롯이 없는 (버전,gidx) {n_missing:,}")
    if n_nowave:
        print(f"⚠️ wave npz 가 없거나 길이가 안 맞는 버전 {n_nowave}종 — `stage_wave` 먼저 실행")
    tot = len(upd["nearest_key"])
    print(f"\n갱신 대상 {tot:,}행 · filepath {len(upd['filepath']):,} · wave_gain {len(upd['wave_gain']):,}")
    if not a.apply:
        print("(dry-run — 반영하려면 --apply)")
        return 0
    for f, d in upd.items():
        if d:
            pds.set_values(f, d, key_field="id")
            print(f"  set {f} {len(d):,}행")
    for f, d in cls_upd.items():
        if d:
            pds.set_values(f, d, key_field="id")
            print(f"  set {f} {len(d):,}행")
    pds.save()
    print("반영 완료")
    return 0


if __name__ == "__main__":
    sys.exit(main())
