#!/usr/bin/env python3
"""벡터전용 공급 뱅크(`prompt:null`)의 feature 벡터를 NAS JSON 에서 회수한다.

`sourcei-prompts` 의 벡터 미보유 261,247 샘플은 텍스트가 `(텍스트 없음 #N)` 자리표시자라
**텍스트에서 만들 수 없다**. 공급 원본 JSON 에는 feature 가 들어 있으므로 거기서 꺼낸다.

⚠️ 파일이 크다(최대 2.8GB). `json.load` 는 수 GB 를 잡으므로 **증분 파싱**한다 —
   버퍼를 밀면서 `JSONDecoder.raw_decode` 로 객체 하나씩 떼어낸다(의존성 0).
⚠️ 호스트에서 실행한다. NAS(`/home/user/mou/userwatch`)는 컨테이너에 마운트돼 있지 않다.

출력: 버전별 npz (ids, cls, vecs float32) → 이후 좌표 재계산이 전 문장을 덮을 수 있다.
"""
import os, sys, json, time, argparse
import numpy as np

ROOT = "/home/user/mou/userwatch/prompts"
OUT = "/home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/fiftyone/frames_bank/report/sourcei_gt/vecbanks"
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)


def stream_objects(path, chunk=1 << 22):
    """최상위 배열의 객체를 하나씩 내놓는다. 메모리는 버퍼 크기로 묶인다."""
    dec = json.JSONDecoder()
    with open(path, "r", encoding="utf-8") as f:
        buf = f.read(chunk)
        i = buf.find("[")
        if i < 0: raise ValueError("배열 시작을 못 찾음")
        buf = buf[i + 1:]
        while True:
            buf = buf.lstrip()
            if buf.startswith(","): buf = buf[1:].lstrip()
            if buf.startswith("]") or buf == "":
                more = f.read(chunk)
                if not more: return
                buf += more; continue
            try:
                obj, end = dec.raw_decode(buf)
            except ValueError:
                more = f.read(chunk)
                if not more:
                    return
                buf += more; continue
            yield obj
            buf = buf[end:]


def recover(version, path, limit=None):
    ids, cls, vecs = [], [], []
    n = 0
    for o in stream_objects(path):
        f = o.get("feature")
        if f is None: continue
        ids.append(int(o.get("ID", n))); cls.append(int(o.get("class", -1)))
        vecs.append(np.asarray(f, dtype=np.float32))
        n += 1
        if n % 20000 == 0: log(f"  {version}: {n:,}")
        if limit and n >= limit: break
    V = np.stack(vecs) if vecs else np.zeros((0, 1024), np.float32)
    V /= np.maximum(np.linalg.norm(V, axis=1, keepdims=True), 1e-9)
    return np.array(ids), np.array(cls), V


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--versions", default="")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    cand = []
    for d in sorted(os.listdir(ROOT)):
        p = os.path.join(ROOT, d)
        if not os.path.isdir(p): continue
        js = [x for x in os.listdir(p) if x.startswith("text_features") and x.endswith(".json")]
        if not js: continue
        tag = d if d.startswith("v") else "v" + d
        cand.append((tag, os.path.join(p, js[0]), os.path.getsize(os.path.join(p, js[0]))))
    want = set(x for x in a.versions.split(",") if x)
    todo = [c for c in cand if not want or c[0] in want]
    log(f"대상 {len(todo)}종 / 총 {sum(c[2] for c in todo)/2**30:.2f} GiB")
    for tag, path, size in todo:
        out = f"{OUT}/{tag}.npz"
        if os.path.exists(out):
            z = np.load(out); log(f"{tag}: 이미 있음 {z['vecs'].shape}"); continue
        t0 = time.time()
        ids, cls, V = recover(tag, path, a.limit or None)
        np.savez_compressed(out, ids=ids, cls=cls, vecs=V)
        log(f"{tag}: {V.shape} · {size/2**20:.0f} MiB · {time.time()-t0:.0f}s → {out}")


if __name__ == "__main__":
    main()
