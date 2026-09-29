#!/usr/bin/env python3
"""sitej_certbody 프레임 → PE-Core 임베딩 npz. analysis 컨테이너에서 실행.

`camera_confound_probe.py` 가 바로 먹을 수 있는 형태로 쓴다:
  cls / camera / group(=session)  — **그룹키는 세션**이다(카메라 홀드아웃은 누수, 스크립트 주석 참조).
ambiguous(멀티라벨)·boundary 는 별도 배열로 남겨 채점에서 제외할 수 있게 한다.
"""
import io, json, os, sys, time
import numpy as np, requests

SRC = "/data/fiftyone/uploads/sitej_certbody"
OUT = "/data/fiftyone/frames_bank/report/sourcea/sitej_certbody.npz"
EMB = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")


def main():
    rows = [json.loads(l) for l in open(f"{SRC}/manifest.jsonl", encoding="utf-8")]
    print(f"매니페스트 {len(rows):,}행", flush=True)
    done = {}
    if os.path.exists(OUT):
        d = np.load(OUT, allow_pickle=True)
        done = {str(k): v for k, v in zip(d["ids"], d["vectors"])}
        print(f"  체크포인트 {len(done):,}", flush=True)
    s = requests.Session()
    keep, fail, t0 = [], 0, time.time()
    for n, r in enumerate(rows, 1):
        k = r["key"]
        if k in done:
            keep.append((r, done[k])); continue
        p = f"{SRC}/images/{k}"
        try:
            with open(p, "rb") as fh:
                resp = s.post(f"{EMB}/embed", files={"file": ("i.jpg", fh.read(), "image/jpeg")}, timeout=120)
            resp.raise_for_status()
            keep.append((r, np.asarray(resp.json()["vector"], dtype=np.float32)))
        except Exception as exc:  # noqa: BLE001
            fail += 1
            if fail <= 5:
                print(f"  실패 {k}: {type(exc).__name__}", flush=True)
        if n % 500 == 0:
            el = time.time() - t0
            print(f"  {n}/{len(rows)} · {el:.0f}s · {el/n:.2f}s/장 · 실패 {fail}", flush=True)
    np.savez_compressed(
        OUT,
        ids=np.array([r["key"] for r, _ in keep]),
        vectors=np.stack([v for _, v in keep]),
        cls=np.array([r["cls"] for r, _ in keep]),
        camera=np.array([r["camera"] for r, _ in keep]),
        group=np.array([r["session"] for r, _ in keep]),          # ← 세션 = 홀드아웃 단위
        video=np.array([r["video_stem"] for r, _ in keep]),
        ambiguous=np.array([bool(r["ambiguous"]) for r, _ in keep]),
        boundary=np.array([bool(r["boundary"]) for r, _ in keep]),
    )
    import collections
    print(f"\n완료 {len(keep):,} · 실패 {fail}")
    print("  클래스:", dict(collections.Counter(r['cls'] for r, _ in keep)))
    print("  세션", len({r['session'] for r, _ in keep}), "· 카메라", len({r['camera'] for r, _ in keep}))
    print("  저장", OUT)


if __name__ == "__main__":
    main()
