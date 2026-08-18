"""-prompts 데이터셋 2버전(v080+v084) 리빌드 러너.

sourceh: winner_gidx_v084(lite attach, 오프셋) → promptmap 재빌드
sourcei: wave(v084 캐시 생성 포함) → winner_gidx_v084 → promptmap 재빌드
lite attach = stage_attach 의 winner_gidx_<tag> 한 필드만 (버전중립 6필드 덮어쓰기 부작용 회피).
"""
import os
import sys

sys.path.insert(0, "/workspace")
import numpy as np
import prompt_geometry as pg
import fiftyone as fo


def lite_attach(version: str) -> None:
    keys, X, gt, src, banks = pg.load_all()
    bank = banks[version]
    classes = sorted(set(bank["cls"].tolist()))
    b1, _, a1 = pg.bank_top2_stream(X, bank)
    M = np.stack([b1[c] for c in classes], axis=1)
    pred = np.array(classes)[M.argmax(axis=1)]
    gidx = {c: np.flatnonzero(bank["cls"] == c) for c in classes}
    win_g = np.array([gidx[int(c)][a1[int(c)][i]] for i, c in enumerate(pred)])
    goff = pg.BANKS.index(version) * pg.GIDX_OFFSET
    ds = fo.load_dataset(pg.PROFILES[pg.PROFILE]["dataset"])
    key_to_id = {}
    for s in ds.select_fields(["id", "filepath"]):
        key_to_id[f"{os.path.basename(os.path.dirname(s.filepath))}/"
                  f"{os.path.basename(s.filepath)}"] = s.id
    ids = [key_to_id.get(k) for k in keys]
    ok = [i for i, x in enumerate(ids) if x]
    tag = pg.vtag(version)
    ds.set_values(f"winner_gidx_{tag}", {ids[i]: int(win_g[i]) + goff for i in ok},
                  key_field="id")
    ds.save()
    pg.log(f"lite-attach {version}: winner_gidx_{tag} {len(ok):,}행 (goff={goff})")


def main():
    pg.log(f"BANKS={pg.BANKS} GIDX_OFFSET={pg.GIDX_OFFSET}")

    pg.set_profile("sourceh")
    pg.log("=== sourceh: lite-attach v1.0.8.4 ===")
    lite_attach("v1.0.8.4")
    pg.log("=== sourceh: promptmap 재빌드 (2버전) ===")
    pg.stage_promptmap()

    pg.set_profile("sourcei")
    pg.log("=== sourcei: wave (v080 재계산 + v084 신규) ===")
    pg.stage_wave()
    pg.log("=== sourcei: lite-attach v1.0.8.4 ===")
    lite_attach("v1.0.8.4")
    pg.log("=== sourcei: promptmap 재빌드 (2버전) ===")
    pg.stage_promptmap()
    pg.log("=== 완료 ===")


if __name__ == "__main__":
    main()
