"""-prompts 데이터셋 전 버전 리빌드 러너 (userwatch 전량, 2026-08-11).

PROMPT_DIR 의 모든 v*.npz 를 semantic sort 로 BANK_LIST 에 올리고
sourceh → sourcei 순서로 wave → lite-attach(전 버전) → promptmap 을 돈다.
구 3자리 태그 필드(winner_gidx_v080/v084)는 삭제 (신 태그 v1080/v1084 로 재백필).

메모리 주의: 공유 호스트 가용 ~8GB — load_all 1회/프로필로 재사용하고
데이터셋 순차 처리로 피크를 1개분으로 유지.
"""
import glob
import os
import resource
import sys

# 자기보호: 공유 호스트(가용 ~8GB, oom_kill 이력)에서 폭주하느니 MemoryError 로 죽는다
resource.setrlimit(resource.RLIMIT_AS, (16 * 2**30, 16 * 2**30))

sys.path.insert(0, "/workspace")

_npzs = glob.glob("/data/fiftyone/sourceh/prompts/v*.npz")
_vers = [os.path.basename(p)[:-4] for p in _npzs]
_vers.sort(key=lambda v: tuple(int(x) for x in v.lstrip("v").split(".")))
os.environ["BANK_LIST"] = ",".join(_vers)

import fiftyone as fo
import numpy as np
import prompt_geometry as pg

OLD_FIELDS = ("winner_gidx_v080", "winner_gidx_v084")


def attach_all(profile: str) -> None:
    """전 버전 lite-attach — load_all 1회, 버전 루프에서 winner_gidx_<tag> 만 세팅."""
    pg.set_profile(profile)
    keys, X, gt, src, banks = pg.load_all()
    ds = fo.load_dataset(pg.PROFILES[profile]["dataset"])
    key_to_id = {}
    for s in ds.select_fields(["id", "filepath"]):
        key_to_id[f"{os.path.basename(os.path.dirname(s.filepath))}/"
                  f"{os.path.basename(s.filepath)}"] = s.id
    ids = [key_to_id.get(k) for k in keys]
    ok = [i for i, x in enumerate(ids) if x]
    for v in pg.BANKS:
        bank = banks[v]
        classes = sorted(set(bank["cls"].tolist()))
        b1, _, a1 = pg.bank_top2_stream(X, bank)
        M = np.stack([b1[c] for c in classes], axis=1)
        pred = np.array(classes)[M.argmax(axis=1)]
        gidx = {c: np.flatnonzero(bank["cls"] == c) for c in classes}
        win_g = np.array([gidx[int(c)][a1[int(c)][i]] for i, c in enumerate(pred)])
        goff = pg.BANKS.index(v) * pg.GIDX_OFFSET
        tag = pg.vtag(v)
        ds.set_values(f"winner_gidx_{tag}", {ids[i]: int(win_g[i]) + goff for i in ok},
                      key_field="id")
        pg.log(f"lite-attach {v}: winner_gidx_{tag} {len(ok):,}행 (goff={goff})")
    for f in OLD_FIELDS:
        try:
            ds.delete_sample_field(f)
            pg.log(f"구 필드 삭제: {f}")
        except Exception:
            pass
    ds.save()


def main():
    pg.log(f"BANKS n={len(pg.BANKS)} GIDX_OFFSET={pg.GIDX_OFFSET}")
    pg.log(f"BANK_LIST={os.environ['BANK_LIST']}")
    for profile in ("sourcei",):
        pg.set_profile(profile)
        pg.log(f"=== {profile}: wave (전 버전) ===")
        pg.stage_wave()
        pg.log(f"=== {profile}: lite-attach (전 버전) ===")
        attach_all(profile)
        pg.log(f"=== {profile}: promptmap 재빌드 ({len(pg.BANKS)}버전) ===")
        pg.stage_promptmap()
    pg.log("=== 완료 ===")


if __name__ == "__main__":
    main()
