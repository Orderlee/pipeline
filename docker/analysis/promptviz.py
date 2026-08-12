#!/usr/bin/env python3
"""`emb_viz` 의 **프롬프트 버전** — `source-h` 안에서 프롬프트 공간 좌표를 보는 brain key 2종.

`promptmap` 은 축을 뒤집어 문장을 표본으로 만든 **별도 데이터셋**(`source-h-prompts`)이라 URL 이
갈린다. Embeddings 패널은 표본당 점 1개를 그리므로 13,144 프레임 데이터셋 안에 문장 28,605
개를 점으로 넣을 방법은 없다 (넣으려면 표본을 추가해야 하고, 그러면 `count()`·GT 통계·기존
뷰의 **분모가 전부 조용히 바뀐다**). 대신 **표본은 프레임 그대로 두고 좌표만 프롬프트 공간
에서** 가져오면 같은 URL 에서 프롬프트 기하를 본다.

  `prompt_viz_<vtag>` — 프레임을 **그 프레임의 승자 문장 벡터** 위치에 놓는다.
    · 승자가 같은 프레임은 좌표가 완전히 겹친다 (v080 승자 201개 → 201덩이). 그 축약이
      곧 메시지다 — 13,144 프레임이 문장 201개에 몰려 있다. 덩이를 라쏘로 잡으면 그 문장이
      가져간 프레임 전체가 선택된다.
    · ⚠️ **Color by 를 `camera` 로 먼저 보라** (기본값으로 깔아둔 이유). 승자문장→카메라
      예측력이 82~87% 라, 그림이 카메라 지도와 닮으면 그 그림은 프롬프트에 대해 아무것도
      말하지 않는다 (slim 워크스페이스 `prompt` 와 같은 함정).
    · 승자 정의는 **argmax(K=1)** — `promptmap`/`prompt_frames_*.csv` 의 wins·purity 와 같은
      정의다. 데이터셋 필드 `winner_gidx_*` 는 prune 이 RULE=topk(K=10) 다수결로 쓴 값이라
      일부 프레임에서 다르다. 섞어 읽지 말 것 (`--selftest` 가 이 차이를 분리해 검증한다).

  `bank_resp_viz` — 프레임을 **뱅크 반응 프로필**(클래스별 best cos × 뱅크 2벌 = 8축) 위치에
    놓는다. 좌표가 겹치지 않고 판정규칙이 실제로 보는 양이라, `emb_viz` 와 번갈아 보면
    "이미지 공간의 클래스 구조가 뱅크 반응에도 남는가" 가 바로 읽힌다. 축별 표준화로 뱅크
    간 **가산 오프셋을 상쇄**한다 (절대 코사인 산점도 `cover_viz` 를 폐기한 사유와 같다).

UMAP 은 prompt_viz 의 경우 **고유 승자 벡터만**(201개) 적합하고 프레임에 좌표를 복사한다.
중복행 13,144개를 그대로 먹이면 거리 0 쌍이 대량 생겨 임베딩이 불안정하고 65배 느리다 —
결과는 정의상 동일하다.

`source-h` 에 추가하는 것은 **brain run + 워크스페이스뿐**이다. 표본·필드 불변 → 기존 분모와
뷰가 그대로 산다. 입력은 전부 읽기 전용 (`sourceh_v2/work/{embed.npz,ledger.jsonl}`,
`geometry/cache.npz`, `sourceh/prompts/*.npz`) — `analyze` 스테이지가 먼저 돌아 있어야 한다.

    docker cp docker/analysis/promptviz.py docker-analysis-1:/workspace/promptviz.py
    docker exec docker-analysis-1 nice -n 10 python /workspace/promptviz.py
    docker exec docker-analysis-1 nice -n 10 python /workspace/promptviz.py --selftest

env: BANK_A/BANK_B(뱅크 버전), BANK_PROFILE(sourceh|frames) — prompt_geometry 와 공유.
"""

from __future__ import annotations

import argparse
import os
import sys

# 스레드 캡을 numpy/umap import 보다 먼저 — 공유 호스트의 병목은 RAM/CPU 경합이다.
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "NUMBA_NUM_THREADS"):
    os.environ.setdefault(_v, str(max(1, (os.cpu_count() or 4) // 4)))

import numpy as np  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import prompt_geometry as pg  # noqa: E402 — 로더·상수 재사용 (VERSIONS/CLASS_NAMES/GEO/load_all)

import fiftyone as fo  # noqa: E402


def frame_ids(ds, keys: list[str]) -> tuple[list, np.ndarray]:
    """원장 key(`<folder>/<name>`) → 데이터셋 sample id + 매칭된 순번."""
    k2i = {f"{os.path.basename(os.path.dirname(fp))}/{os.path.basename(fp)}": i
           for i, fp in zip(ds.values("id"), ds.values("filepath"))}
    ids = [k2i.get(k) for k in keys]
    ok = np.array([i for i, x in enumerate(ids) if x], dtype=np.int64)
    if not len(ok):
        raise SystemExit("원장 key ↔ 데이터셋 filepath 매칭 0 — 프로필/데이터셋 확인")
    return ids, ok


def winners(cache, bank: dict, version: str) -> tuple[np.ndarray, np.ndarray]:
    """프레임별 (판정 클래스, 승자 문장의 **뱅크 전역 인덱스**) — argmax(K=1) 정의.

    `arg_*` 는 **클래스-로컬** 번호라 `flatnonzero(cls == c)` 로 되돌려야 한다
    (`bank_top2_stream` 도크스트링). 이 사상이 틀리면 엉뚱한 문장 벡터 위에 프레임을 놓게
    되는데 그림은 그럴싸하게 나온다 — 그래서 `--selftest` 가 다른 코드 경로와 대조한다.
    """
    tag = version.replace(".", "_")
    classes = sorted(pg.CLASS_NAMES)
    best = np.stack([cache[f"best_{tag}_{c}"] for c in classes], axis=1)
    pred = np.array(classes)[best.argmax(axis=1)]
    gmap = {c: np.flatnonzero(bank["cls"] == c) for c in classes}
    arg = {c: cache[f"arg_{tag}_{c}"] for c in classes}
    gidx = np.array([gmap[int(c)][arg[int(c)][i]] for i, c in enumerate(pred)])
    return pred, gidx


def umap2(M: np.ndarray, **kw) -> np.ndarray:
    import umap

    return umap.UMAP(n_components=2, random_state=42, low_memory=True,
                     **kw).fit_transform(M).astype(np.float64)


def register(ds, view, bkey: str, pts: np.ndarray) -> None:
    import fiftyone.brain as fob

    if ds.has_brain_run(bkey):
        ds.delete_brain_run(bkey)
    fob.compute_visualization(view, points=pts, brain_key=bkey)
    pg.log(f"{bkey} 등록 {pts.shape}")


def workspace(ds, name: str, bkey: str, color: str) -> None:
    """Samples ↔ Embeddings 분할. `brainResult` 를 못 박는 이유: 패널이 마지막 키를 기억해서
    데이터셋에 brain key 가 여럿이면 엉뚱한 투영이 열린다 (Color by 까지 죽는 함정)."""
    space = fo.Space(children=[
        fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
        fo.Space(children=[fo.Panel(type="Embeddings",
                                    state={"brainResult": bkey, "colorByField": color})]),
    ], orientation="horizontal")
    if name in ds.list_workspaces():
        ds.delete_workspace(name)
    ds.save_workspace(name, space, description=f"{bkey} (색: {color})")


def build() -> int:
    keys, _X, _gt, _src, banks = pg.load_all()
    cache = np.load(f"{pg.GEO}/cache.npz", allow_pickle=True)
    ds = fo.load_dataset(pg.PROFILES[pg.PROFILE]["dataset"])
    ids, ok = frame_ids(ds, keys)
    # points 는 뷰의 표본 순서에 정렬돼야 한다 (brain 에 sample_ids 인자가 없다) →
    # 데이터셋 기본 순서에 기대지 않고 ordered select 로 순서를 못 박는다.
    view = ds.select([ids[i] for i in ok], ordered=True)
    pg.log(f"promptviz: 프레임 {len(ok):,}/{len(keys):,} 매칭 → {ds.name}")

    for v in pg.BANKS:
        tag = pg.vtag(v)
        _pred, w = winners(cache, banks[v], v)
        uniq, inv = np.unique(w, return_inverse=True)
        pu = umap2(banks[v]["vec"][uniq], metric="cosine",
                   n_neighbors=min(15, max(2, len(uniq) - 1)))
        pts = pu[inv][ok]
        assert len(np.unique(pts, axis=0)) == len(np.unique(w[ok])), "좌표 복사 붕괴"
        register(ds, view, f"prompt_viz_{tag}", pts)
        pg.log(f"prompt_viz_{tag}: 승자 문장 {len(uniq):,}개 위에 프레임 {len(ok):,}장 "
               f"(문장당 평균 {len(ok) / len(uniq):.1f}장)")
        workspace(ds, f"prompt-{tag}", f"prompt_viz_{tag}", "camera")

    R = np.stack([cache[f"best_{v.replace('.', '_')}_{c}"]
                  for v in pg.BANKS for c in sorted(pg.CLASS_NAMES)], axis=1)[ok]
    Z = (R - R.mean(axis=0)) / (R.std(axis=0) + 1e-9)
    register(ds, view, "bank_resp_viz", umap2(Z, metric="euclidean", n_neighbors=15))
    workspace(ds, "bank-resp", "bank_resp_viz", "ground_truth.label")

    ds.save()
    pg.log(f"promptviz 완료 — brain {ds.list_brain_runs()} / ws {ds.list_workspaces()}")
    return 0


def selftest() -> int:
    """cache.npz 사상(클래스-로컬 `arg` → 전역 gidx)을 **다른 코드 경로**와 대조한다.

    필드 `winner_gidx_*` 는 prune 이 RULE=topk 로 쓴 값이라 판정 **클래스**가 다를 수 있다.
    클래스가 일치하는 프레임에서는 두 경로가 같은 식(`gidx[c][a1[c][i]]`)을 쓰므로 gidx 가
    **정확히** 같아야 한다 — 어긋나면 사상이 깨진 것이다.
    """
    keys, _X, _gt, _src, banks = pg.load_all()
    cache = np.load(f"{pg.GEO}/cache.npz", allow_pickle=True)
    ds = fo.load_dataset(pg.PROFILES[pg.PROFILE]["dataset"])
    ids, ok = frame_ids(ds, keys)
    bad = 0
    for v in pg.BANKS:
        field = f"winner_gidx_{pg.vtag(v)}"
        if field not in ds.get_field_schema():
            pg.log(f"selftest {v}: {field} 없음 — 대조 생략 (prune 미실행)")
            continue
        # 다중뱅크 gidx 오프셋 보정 (pg.GIDX_OFFSET 주석) — 필드 값은 전역 gidx,
        # cache/뱅크 인덱스는 뱅크-로컬이라 오프셋을 벗겨 비교한다.
        goff = pg.BANKS.index(v) * getattr(pg, "GIDX_OFFSET", 0)
        got = {k: (None if x is None else int(x) - goff)
               for k, x in zip(ds.values("id"), ds.values(field))}
        pred, w = winners(cache, banks[v], v)
        cls = banks[v]["cls"]
        cmp_ = [(i, got[ids[i]]) for i in ok if got.get(ids[i]) is not None]
        same = [(i, g) for i, g in cmp_ if int(cls[g]) == int(pred[i])]
        mis = [(i, g) for i, g in same if int(g) != int(w[i])]
        pg.log(f"selftest {v}: 비교 {len(cmp_):,} · 판정클래스 일치 {len(same):,} "
               f"({len(same) / max(1, len(cmp_)):.1%}, 나머지는 topk↔argmax 차이) · "
               f"gidx 불일치 {len(mis)}")
        if mis:
            i, g = mis[0]
            pg.log(f"  예: key={keys[i]} field={g}(cls {cls[g]}) "
                   f"derived={w[i]}(cls {cls[w[i]]})")
        bad += len(mis)
    print("SELFTEST", "FAIL" if bad else "PASS")
    return 1 if bad else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--profile", choices=list(pg.PROFILES),
                    default=os.environ.get("BANK_PROFILE", "sourceh"))
    ap.add_argument("--selftest", action="store_true",
                    help="brain run 을 쓰지 않고 승자 사상만 검증")
    ap.add_argument("--mem-budget-gb", type=float, default=2.0)
    args = ap.parse_args()
    pg.set_profile(args.profile)
    pg.assert_mem_budget(args.mem_budget_gb)
    return selftest() if args.selftest else build()


if __name__ == "__main__":
    sys.exit(main())
