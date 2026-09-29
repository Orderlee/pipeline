#!/usr/bin/env python3
"""certbody sitej 원본 15초 영상 → 1fps 프레임 + JSON 구간 라벨 투영. **호스트에서 실행.**

왜 호스트인가: `/nas_secondary/certbody/` 는 어느 컨테이너에도 마운트돼 있지 않다(`nas_secondary/datasets`
만 붙어 있고 certbody 는 그 형제다). 컴포즈를 건드리는 대신 호스트 ffmpeg 로 뽑아
`docker/data/fiftyone/`(= 컨테이너 `/data/fiftyone`) 에 쓴다.

왜 클립이 아니라 원본인가: 영상이 전부 정확히 15.0s 이고 이벤트 점유율 중앙 83% 라
클립만 쓰면 **normal 이 한 장도 안 나온다.** 기존 `sitej_subway` 373장도 원본에서
1fps(`_t001`~`_t015`)로 뽑고 구간을 투영한 것이다 — 그 규약을 그대로 따른다.

⚠️ **그룹키는 세션(`<datetime>`)이다. 카메라로 나누면 안 된다.**
한 세션을 카메라 최대 14대가 동시 녹화한 연출 코퍼스라, 카메라 홀드아웃은 같은 물리
이벤트를 학습에 남긴다(기존 373장에서 28% 실측). 세션 홀드아웃은 카메라가 train/test
양쪽에 남지만, 배포 시 카메라는 고정이므로 그건 누수가 아니라 배포 조건이다.
blocked(세션+카메라 동시) 홀드아웃은 폴드당 테스트 영상이 2~7개라 실현 불가 — 실측 후 폐기.

⚠️ 같은 stem 이 두 클래스 폴더에 있으면 **서로 다른 어노테이션 패스**다(22개).
구간이 어긋나므로 합집합으로 투영하되 불일치를 `n_passes`/`ambiguous` 로 남긴다.
"""

import glob
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict

SRC = "/home/user/mou/nas_secondary/certbody/cert-eval_재인코딩"
DST = "/home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/fiftyone/uploads/sitej_certbody"
FFMPEG = "/home/user/anaconda3/bin/ffmpeg"
FPS = 1
MARGIN = 0.25  # 구간 경계 ±0.25s 는 프레임 라벨이 흔들린다 → boundary 표시 후 채점 제외


def label_at(t, events, margin=MARGIN):
    """시각 t 의 라벨. 반환 (classes, boundary).

    여기가 이 스크립트의 전부다 — 틀리면 전량 오라벨이라 selftest 로 못박는다.
    """
    cls, boundary = set(), False
    for cat, a, b in events:
        if a <= t <= b:
            cls.add(cat)
        if abs(t - a) <= margin or abs(t - b) <= margin:
            boundary = True
    return cls, boundary


def selftest():
    ev = [("fire", 1.5, 15.042), ("smoke", 2.292, 15.042)]
    assert label_at(0.0, ev)[0] == set(), "이벤트 전인데 라벨이 붙음"
    assert label_at(2.0, ev)[0] == {"fire"}, label_at(2.0, ev)
    assert label_at(5.0, ev)[0] == {"fire", "smoke"}, "중첩 구간이 멀티라벨이 아님"
    assert label_at(1.5, ev)[1] is True, "구간 시작이 boundary 로 안 잡힘"
    assert label_at(5.0, ev)[1] is False, "구간 한복판이 boundary 로 잡힘"
    # margin 은 양끝 모두
    assert label_at(15.0, ev)[1] is True
    print("selftest 통과 (구간→프레임 라벨 투영)")


def collect():
    """distinct stem → (events 합집합, 패스별 events, 메타)."""
    by = defaultdict(list)
    for p in sorted(glob.glob(f"{SRC}/*/*.json")):
        by[os.path.basename(p)[:-5]].append(p)
    out = {}
    for stem, paths in by.items():
        passes = []
        for p in paths:
            d = json.load(open(p, encoding="utf-8"))
            passes.append([(e["category"], float(e["timestamp"][0]), float(e["timestamp"][1]))
                           for e in d.get("events", [])])
        merged = sorted({e for ps in passes for e in ps})
        parts = stem.split("__")
        out[stem] = dict(
            events=merged, n_passes=len(paths),
            camera=parts[0], session=parts[1] if len(parts) > 1 else "?",
            take=parts[2] if len(parts) > 2 else "?",
            mp4=next((p[:-5] + ".mp4" for p in paths if os.path.exists(p[:-5] + ".mp4")), None),
        )
    return out


def main():
    selftest()
    items = collect()
    print(f"distinct stem {len(items)} · mp4 없음 {sum(1 for v in items.values() if not v['mp4'])}")
    os.makedirs(f"{DST}/images", exist_ok=True)
    tmp = f"{DST}/_tmp"
    os.makedirs(tmp, exist_ok=True)

    man = open(f"{DST}/manifest.jsonl", "w", encoding="utf-8")
    stats, nfail = Counter(), 0
    for n, (stem, v) in enumerate(sorted(items.items()), 1):
        if not v["mp4"]:
            nfail += 1
            continue
        for f in glob.glob(f"{tmp}/*.jpg"):
            os.remove(f)
        r = subprocess.run(
            [FFMPEG, "-y", "-v", "error", "-i", v["mp4"], "-vf", f"fps={FPS}", "-q:v", "3",
             f"{tmp}/t%03d.jpg"], capture_output=True, timeout=300)
        frames = sorted(glob.glob(f"{tmp}/t*.jpg"))
        if not frames:
            nfail += 1
            print(f"  추출 실패 {stem}: {r.stderr.decode()[:120]}", flush=True)
            continue
        for f in frames:
            idx = int(os.path.basename(f)[1:4])
            t = (idx - 1) / FPS
            cls, boundary = label_at(t, v["events"])
            amb = len(cls) > 1
            label = "normal" if not cls else (sorted(cls)[0] if not amb else sorted(cls)[0])
            d = f"{DST}/images/{label}"
            os.makedirs(d, exist_ok=True)
            out = f"{d}/{stem}_t{idx:03d}.jpg"
            os.replace(f, out)
            man.write(json.dumps(dict(
                filepath=out.replace(DST, "/data/fiftyone/uploads/sitej_certbody"),
                key=f"{label}/{stem}_t{idx:03d}.jpg", cls=label,
                classes=sorted(cls), ambiguous=amb, boundary=boundary,
                camera=v["camera"], session=v["session"], take=v["take"],
                video_stem=stem, t_sec=t, n_passes=v["n_passes"],
            ), ensure_ascii=False) + "\n")
            stats[label] += 1
            stats["__ambiguous"] += amb
            stats["__boundary"] += boundary
        if n % 50 == 0:
            print(f"  {n}/{len(items)}", flush=True)
    man.close()
    for f in glob.glob(f"{tmp}/*.jpg"):
        os.remove(f)
    os.rmdir(tmp)
    print(f"\n완료 · 실패 {nfail}")
    print("  라벨:", {k: v for k, v in stats.items() if not k.startswith("__")})
    print(f"  멀티라벨(ambiguous) {stats['__ambiguous']} · 경계 {stats['__boundary']}")
    print(f"  매니페스트 {DST}/manifest.jsonl")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
