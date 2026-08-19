#!/usr/bin/env python3
"""`<dataset>-prompts` 문장행에 wave 축(wave_gain/wave_role)을 **재빌드 없이** 부착.

promptmap 은 wave_<tag>.npz 가 있으면 빌드 시점에 wave 축을 붙이지만, 없이 빌드된
데이터셋(frames-prompts, 2026-08-19 — 당시 wave 조인 버그로 npz 0개)에 축을 더하려고
전체 재빌드(promptmap 재실행 → 캡션 enrich → compare ws 재생성 체인)를 도는 건 낭비다.
이 스크립트는 기존 문장행에 set_values 로 두 필드만 얹는다.

수학은 `stage_promptmap` 의 wave 블록을 **문자 그대로 미러**한다 (그쪽이 정본):
    signed = where(cls==0, -gain, gain)      # normal 은 부호 반대 — 원값 정렬 금지
    클래스 내 10/90 백분위 → "유해 하위10%" / "중간" / "유익 상위10%"

키: (bank_version.label, gidx % GIDX_OFFSET) → wave_<tag>.npz 의 bank-local 행.
캡션 행(entity=caption, gidx 없음)은 대상 아님 — wave 는 뱅크 문장의 속성이다.

사용 (analysis 컨테이너):
    python3 wave_prompts_attach.py frames-prompts            # dry-run
    python3 wave_prompts_attach.py frames-prompts --apply
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import numpy as np

GIDX_OFFSET = 100_000                     # prompt_geometry.GIDX_OFFSET 미러 (사본 동기화)
PROMPT_DIR = os.environ.get("PROMPT_DIR", "/data/fiftyone/sourceh/prompts")
GEO = os.environ.get("WAVE_GEO_DIR", "/data/fiftyone/frames_bank/work/geometry")
BATCH = 20_000


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def vtag(version: str) -> str:
    """prompt_geometry.vtag 미러 — 전 파트 조인 (v1.0.8.0 → v1080)."""
    return "v" + "".join(p for p in version.lstrip("vV").split("."))


def roles_for(version: str) -> tuple[np.ndarray, np.ndarray] | None:
    """(gain, role) — stage_promptmap 의 wave 블록과 문자 단위 동일 수식."""
    wpath = f"{GEO}/wave_{vtag(version)}.npz"
    npz_path = f"{PROMPT_DIR}/{version}.npz"
    if not os.path.exists(wpath):
        return None
    cls = np.load(npz_path, allow_pickle=True)["cls"].astype(np.int64)
    if len(cls) > GIDX_OFFSET:            # 가드 사본 (372bd8b 계약)
        raise SystemExit(f"뱅크 {version} 문장 {len(cls):,} > GIDX_OFFSET — gidx 블록 충돌")
    gain = np.load(wpath)["gain"]
    if len(gain) != len(cls):
        raise SystemExit(f"{wpath}: gain {len(gain):,} ≠ 뱅크 {len(cls):,} — 정체성 불일치. "
                         "wave 를 그 버전 npz 로 다시 돌려라")
    signed = np.where(cls == 0, -gain, gain)
    role = np.full(len(cls), "중간", dtype=object)
    for c in np.unique(cls):
        g = np.flatnonzero(cls == c)
        lo_q, hi_q = np.percentile(signed[g], [10, 90])
        # 부호 실재 조건 — stage_promptmap 과 동일 (사본 동기화). gain=0 동점 퇴화 방지.
        role[g[(signed[g] >= hi_q) & (signed[g] > 0)]] = "유익 상위10%"
        role[g[(signed[g] <= lo_q) & (signed[g] < 0)]] = "유해 하위10%"
    return gain, role


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("dataset")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    import fiftyone as fo

    tds = fo.load_dataset(args.dataset)
    ids = tds.values("id")
    vers = tds.values("bank_version.label")
    gidxs = tds.values("gidx")

    per_ver: dict[str, list[tuple[str, int]]] = {}
    for sid, v, g in zip(ids, vers, gidxs):
        if v and g is not None:                      # 캡션 행(gidx 없음)은 제외
            per_ver.setdefault(v, []).append((sid, int(g) % GIDX_OFFSET))

    plan, missing = [], []
    for v in sorted(per_ver):
        rr = roles_for(v)
        (plan if rr else missing).append(v)
    log(f"{args.dataset}: 문장 보유 {len(per_ver)}버전 — wave npz 보유 {len(plan)} / 미보유 {len(missing)}")
    if missing:
        log(f"  미보유(생략): {', '.join(missing)}")
    if not plan:
        raise SystemExit("부착할 wave npz 가 하나도 없다 — `wave` 스테이지 먼저")
    if not args.apply:
        n = sum(len(per_ver[v]) for v in plan)
        log(f"dry-run — {len(plan)}버전 × 문장 {n:,}행에 wave_gain/wave_role 부착 예정. --apply 로 실행")
        return 0

    for v in plan:
        gain, role = roles_for(v)
        rows = per_ver[v]
        for s in range(0, len(rows), BATCH):
            chunk = rows[s:s + BATCH]
            tds.set_values("wave_gain", {sid: float(gain[loc]) for sid, loc in chunk},
                           key_field="id")
            tds.set_values("wave_role", {sid: fo.Classification(label=str(role[loc]))
                                         for sid, loc in chunk}, key_field="id")
        log(f"wave 축 부착 {v}: {len(rows):,}행")

    # 워크스페이스 — sourcei-prompts 'wave' 실측 미러 (Samples | Embeddings·color=wave_role)
    space = fo.Space(children=[
        fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
        fo.Space(children=[fo.Panel(type="Embeddings",
                                    state={"brainResult": "emb_viz",
                                           "colorByField": "wave_role.label"})]),
    ], orientation="horizontal")
    if "wave" in tds.list_workspaces():
        tds.delete_workspace("wave")
    tds.save_workspace("wave", space, description="문장 UMAP (색: wave_role) — 분포 IoU 기여도")
    tds.save()
    log(f"완료 — ws {tds.list_workspaces()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
