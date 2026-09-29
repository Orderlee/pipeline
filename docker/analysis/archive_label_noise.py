"""아카이브 확정 라벨의 노이즈 바닥 — 같은 영상이 두 번 라벨링된 쌍에서 잰다.

`/nas_primary/archive` 의 PoC export 에는 **동일 콘텐츠가 서로 다른 파일명으로 두 벌** 들어 있다
(`FILE...F.MOV` ↔ `FILE...F-008.MOV`, export batch `-0176` ↔ `-0177`). 각 벌에 독립적으로
구간 라벨이 붙어 있으므로, 두 벌을 대조하면 **annotator agreement 를 공짜로 얻는다** —
이 프로젝트가 한 번도 가져본 적 없는 수치다.

이 바닥이 왜 필요한가: 이 코퍼스로 만든 어떤 지표든(구간 IoU, 프레임 투영 정확도, 게이트
오탈락률) 두 사람이 같은 영상에 대해 만들어내는 불일치보다 작은 차이는 **해석할 수 없다.**
그 하한을 모르면 개선을 주장할 수 없다.

중복 판정: 파일 크기 그룹핑 → 선두 4MB md5. 327GB 전체 해시는 CIFS 에서 비싸고, 크기+선두
4MB 로 이미 17쌍이 잡힌다. 오탐이 의심되면 그때 전체 해시로 올린다.

이벤트 정렬: 같은 category 안에서 |Δstart|+|Δend| 최소인 쌍을 greedy 매칭. 한쪽에만 있는
이벤트는 unmatched 로 센다(이게 개수 불일치의 정의).

실행 — **호스트에서 돌린다.** analysis 컨테이너에는 /nas/data 마운트가 없다:
    /usr/bin/python3 docker/analysis/archive_label_noise.py
"""

import hashlib
import json
import os
import statistics as st
from collections import defaultdict
from pathlib import Path

ARCHIVE = Path(os.environ.get("ARCHIVE_ROOT", "/home/user/mou/nas_primary/archive"))
OUT = Path(os.environ.get("NOISE_OUT", "docker/data/fiftyone/frames_bank/report/archive/label_noise_floor.json"))
HEAD_BYTES = 4 << 20  # 선두 4MB — 크기 충돌 그룹 안에서만 계산한다
VIDEO_EXT = {".mp4", ".mov", ".MOV", ".MP4"}


def _head_md5(path: Path) -> str:
    with path.open("rb") as fh:
        return hashlib.md5(fh.read(HEAD_BYTES)).hexdigest()


def find_duplicate_groups(videos: list[Path]) -> list[list[Path]]:
    """크기 그룹핑 → 선두 4MB md5 로 동일 콘텐츠 그룹을 만든다."""
    by_size: dict[int, list[Path]] = defaultdict(list)
    for p in videos:
        by_size[p.stat().st_size].append(p)

    groups: list[list[Path]] = []
    for same_size in by_size.values():
        if len(same_size) < 2:
            continue
        by_hash: dict[str, list[Path]] = defaultdict(list)
        for p in same_size:
            by_hash[_head_md5(p)].append(p)
        groups.extend(g for g in by_hash.values() if len(g) > 1)
    return groups


def load_events(video: Path) -> list[dict]:
    sidecar = video.with_suffix(".json")
    if not sidecar.exists():
        return []
    return json.load(sidecar.open(encoding="utf-8")).get("events") or []


def align(a: list[dict], b: list[dict]) -> tuple[list[tuple[dict, dict]], int]:
    """같은 category 안에서 |Δstart|+|Δend| 최소인 쌍을 greedy 매칭.

    Returns: (matched pairs, unmatched count)
    """
    remaining = list(range(len(b)))
    matched: list[tuple[dict, dict]] = []
    for ev_a in a:
        best_j, best_cost = None, None
        for j in remaining:
            ev_b = b[j]
            if ev_a.get("category") != ev_b.get("category"):
                continue
            ta, tb = ev_a["timestamp"], ev_b["timestamp"]
            cost = abs(ta[0] - tb[0]) + abs(ta[1] - tb[1])
            if best_cost is None or cost < best_cost:
                best_j, best_cost = j, cost
        if best_j is not None:
            remaining.remove(best_j)
            matched.append((ev_a, b[best_j]))
    unmatched = (len(a) - len(matched)) + len(remaining)
    return matched, unmatched


def main() -> None:
    videos = [p for p in ARCHIVE.glob("*/*/*") if p.suffix in VIDEO_EXT]
    groups = find_duplicate_groups(videos)
    print(f"영상 {len(videos)} · 동일 콘텐츠 그룹 {len(groups)} "
          f"· distinct {len(videos) - sum(len(g) - 1 for g in groups)}", flush=True)

    d_start: list[float] = []
    d_end: list[float] = []
    unmatched_total = 0
    pair_detail: list[dict] = []

    for g in groups:
        a_path, b_path = sorted(g)[:2]  # 3벌 이상이면 앞의 두 벌만 — 실측상 전부 2벌이다
        matched, unmatched = align(load_events(a_path), load_events(b_path))
        unmatched_total += unmatched
        ds = [abs(x["timestamp"][0] - y["timestamp"][0]) for x, y in matched]
        de = [abs(x["timestamp"][1] - y["timestamp"][1]) for x, y in matched]
        d_start += ds
        d_end += de
        pair_detail.append({
            "a": str(a_path.relative_to(ARCHIVE)),
            "b": str(b_path.relative_to(ARCHIVE)),
            "size_bytes": a_path.stat().st_size,
            "matched": len(matched),
            "unmatched": unmatched,
            "max_d_start": round(max(ds), 3) if ds else None,
            "max_d_end": round(max(de), 3) if de else None,
        })

    def summarize(v: list[float]) -> dict:
        if not v:
            return {}
        return {
            "n": len(v),
            "median": round(st.median(v), 4),
            "mean": round(st.mean(v), 4),
            "max": round(max(v), 4),
            "exact_match": sum(1 for x in v if x < 1e-6),
        }

    report = {
        "archive_root": str(ARCHIVE),
        "videos_total": len(videos),
        "duplicate_groups": len(groups),
        "distinct_videos": len(videos) - sum(len(g) - 1 for g in groups),
        "matched_event_pairs": len(d_start),
        "unmatched_events": unmatched_total,
        "delta_start_sec": summarize(d_start),
        "delta_end_sec": summarize(d_end),
        "pairs": pair_detail,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"정렬된 이벤트 쌍 {len(d_start)} · 한쪽에만 있는 이벤트 {unmatched_total}")
    if d_start:
        print(f"|Δstart| median {st.median(d_start):.3f}s  mean {st.mean(d_start):.3f}s  max {max(d_start):.3f}s")
        print(f"|Δend|   median {st.median(d_end):.3f}s  mean {st.mean(d_end):.3f}s  max {max(d_end):.3f}s")
        print(f"정확일치 start {sum(1 for x in d_start if x < 1e-6)}/{len(d_start)} "
              f"· end {sum(1 for x in d_end if x < 1e-6)}/{len(d_end)}")
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
