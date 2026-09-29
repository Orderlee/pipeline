#!/usr/bin/env python3
"""SourceA 알람 썸네일 → PE-Core 임베딩. dagster 컨테이너에서 실행(/nas/data 보유).

왜 썸네일인가: sourcea 영상 1,751개가 DB 에 있지만 **라벨이 0건**이었다. 그런데
`by_category/<class>/thumbnail/<date>/<camera>_sourcea_db_<epoch>.jpg` 는 경로 하나에
**클래스 + 카메라 + 날짜**를 전부 담고 있다 — 영상↔썸네일 매핑(현재 복구 불가)이 없어도
그 자체로 완결된 라벨+그룹 데이터셋이다. sourcei·sitej_subway 와 같은 프로브를 돌릴 수 있다.

⚠️ 이 클래스는 사람 검수가 아니라 **현장 배포 탐지기의 알람 카테고리**로 보인다
(smoke 가 94%). AL 후보 랭킹·커버리지에는 쓰되 학습 GT 로는 쓰지 않는다(자기학습 금지).

산출: /data/fiftyone/frames_bank/report/sourcea/thumb_embeddings.npz
      {ids, vectors(1024), cls, camera, date, epoch}
"""

import os
import sys
import time

sys.path.insert(0, "/src/vlm")

import numpy as np  # noqa: E402

from vlm_pipeline.lib.embedding import EmbeddingClient  # noqa: E402

ROOT = "/nas/data/sourcea/by_category"
OUT_DIR = "/data/fiftyone/frames_bank/report/sourcea"
OUT = f"{OUT_DIR}/thumb_embeddings.npz"


def parse(path):
    """.../by_category/<cls>/thumbnail/<date>/<cam>_sourcea_db_<epoch>.jpg → 메타.

    경로가 곧 라벨이므로 여기가 틀리면 전부 오라벨된다 — selftest 로 못박는다.
    """
    parts = path.split("/")
    base = parts[-1]
    cls, date = parts[-4], parts[-2]
    cam = base.split("_", 1)[0]
    epoch = base.rsplit("_", 1)[-1].rsplit(".", 1)[0]
    return cls, cam, date, epoch


def selftest():
    p = "/nas/data/sourcea/by_category/fire/thumbnail/20260907/45_sourcea_db_1788779184812.jpg"
    assert parse(p) == ("fire", "45", "20260907", "1788779184812"), parse(p)
    p2 = "/nas/data/sourcea/by_category/smoke/thumbnail/20260810/7_sourcea_db_1786000000000.jpg"
    assert parse(p2) == ("smoke", "7", "20260810", "1786000000000"), parse(p2)
    print("selftest 통과 (경로→클래스/카메라/날짜 파싱)")


def walk():
    out = []
    for dirpath, _, files in os.walk(ROOT):
        if os.path.basename(os.path.dirname(dirpath)) != "thumbnail":
            continue
        for f in files:
            if f.lower().endswith((".jpg", ".jpeg")) and not f.startswith("._"):
                out.append(os.path.join(dirpath, f))
    return sorted(out)


def main():
    selftest()
    os.makedirs(OUT_DIR, exist_ok=True)
    paths = walk()
    print(f"썸네일 {len(paths):,}장", flush=True)

    done = {}
    if os.path.exists(OUT):
        d = np.load(OUT, allow_pickle=True)
        for i, v in zip(d["ids"], d["vectors"]):
            done[str(i)] = v
        print(f"  체크포인트 {len(done):,} 적재", flush=True)

    client = EmbeddingClient()
    if not client.wait_until_ready(max_wait_sec=600):
        raise SystemExit("embedding-service 준비 실패")

    rows, t0, fail = [], time.time(), 0
    for n, p in enumerate(paths, 1):
        cls, cam, date, epoch = parse(p)
        key = f"{cls}/{date}/{os.path.basename(p)}"
        if key in done:
            rows.append((key, done[key], cls, cam, date, epoch))
            continue
        try:
            with open(p, "rb") as fh:
                v = client.embed(fh.read())
            rows.append((key, np.asarray(v, dtype=np.float32), cls, cam, date, epoch))
        except Exception as exc:  # noqa: BLE001 — per-file fail-forward
            fail += 1
            print(f"  실패 {key}: {type(exc).__name__} {exc}", flush=True)
        if n % 300 == 0:
            el = time.time() - t0
            print(f"  {n}/{len(paths)} · {el:.0f}s · {el / n:.2f}s/장 · 실패 {fail}", flush=True)

    ids = [r[0] for r in rows]
    np.savez_compressed(
        OUT,
        ids=np.array(ids),
        vectors=np.stack([r[1] for r in rows]),
        cls=np.array([r[2] for r in rows]),
        camera=np.array([r[3] for r in rows]),
        date=np.array([r[4] for r in rows]),
        epoch=np.array([r[5] for r in rows]),
    )
    import collections

    print(f"\n완료 {len(rows):,} · 실패 {fail}")
    print("  클래스:", dict(collections.Counter(r[2] for r in rows)))
    print("  카메라:", len({r[3] for r in rows}), "대")
    print("  저장:", OUT)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
