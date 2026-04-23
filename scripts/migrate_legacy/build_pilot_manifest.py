#!/usr/bin/env python3
"""레거시 NAS 에서 10GB 파일럿 샘플 manifest 생성 (파일 미복사, 인덱싱만).

프로젝트별 확장자 × 크기 버킷 × 파일명 특이속성 다양성을 고려해
프로젝트당 ~300MB (기본), 전체 10GB 이내로 선택한다.

manifest 는 src(NAS) 경로와 dest 상대경로만 담는다. 실제 복사는 별도 스크립트에서
DEST_ROOT 를 staging incoming (/home/pia/mou/staging/incoming) 으로 지정해 수행.

산출물:
    pilot_manifest.csv  — project,src_path,size_bytes,ext,dest_rel,tags
    pilot_stats.json    — 프로젝트별 선택 통계 (ext/size/tag 분포)

실행:
    python3 build_pilot_manifest.py
    TARGET_MB_PER_PROJECT=500 MAX_GB_TOTAL=12 python3 build_pilot_manifest.py
"""

from __future__ import annotations

import csv
import json
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

NAS_ROOT = Path(os.environ.get("NAS_ROOT", "/home/pia/mou/nas_192tb/datasets/projects"))
OUT_DIR = Path(__file__).parent
MANIFEST_OUT = OUT_DIR / "pilot_manifest.csv"
STATS_OUT = OUT_DIR / "pilot_stats.json"

TARGET_MB_PER_PROJECT = int(os.environ.get("TARGET_MB_PER_PROJECT", "300"))
MAX_GB_TOTAL = float(os.environ.get("MAX_GB_TOTAL", "10"))

# lib/validator.py 와 동기화
ALLOWED_EXTS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp", ".heic",
    ".mp4", ".avi", ".mov", ".mkv", ".webm", ".mpeg", ".mpg", ".m4v", ".mts", ".m2ts",
}
LARGE_BYTES = 450 * 1024 * 1024  # Vertex preview 재인코딩 임계
SIZE_BUCKETS = [
    ("xs", 0, 10 * 1024 * 1024),
    ("sm", 10 * 1024 * 1024, 100 * 1024 * 1024),
    ("md", 100 * 1024 * 1024, LARGE_BYTES),
    ("lg", LARGE_BYTES, float("inf")),
]
HANGUL_RE = re.compile(r"[가-힣]")
SPECIAL_RE = re.compile(r"[^\w\s가-힣.\-/]")

random.seed(20260422)


def size_bucket(size: int) -> str:
    for name, lo, hi in SIZE_BUCKETS:
        if lo <= size < hi:
            return name
    return "lg"


def compute_tags(rel_path: str, size: int, orig_ext: str) -> list[str]:
    tags: list[str] = []
    if HANGUL_RE.search(rel_path):
        tags.append("hangul")
    if " " in rel_path:
        tags.append("space")
    if "&" in rel_path:
        tags.append("ampersand")
    if SPECIAL_RE.search(rel_path):
        tags.append("special")
    if orig_ext and orig_ext != orig_ext.lower():
        tags.append("uppercase_ext")
    if size >= LARGE_BYTES:
        tags.append("large")
    return tags


def index_project(project_dir: Path) -> tuple[list[dict], str | None]:
    files: list[dict] = []
    try:
        for root, dirs, fnames in os.walk(project_dir):
            dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__MACOSX"]
            for fname in fnames:
                if fname.startswith(".") or fname.startswith("._"):
                    continue
                orig_ext = os.path.splitext(fname)[1]
                ext_lower = orig_ext.lower()
                if ext_lower not in ALLOWED_EXTS:
                    continue
                full = Path(root) / fname
                try:
                    size = full.stat().st_size
                except OSError:
                    continue
                if size == 0:
                    continue
                rel_str = str(full.relative_to(project_dir))
                files.append(
                    {
                        "src_path": str(full),
                        "rel_path": rel_str,
                        "size": size,
                        "ext": ext_lower,
                        "tags": compute_tags(rel_str, size, orig_ext),
                    }
                )
    except Exception as exc:  # noqa: BLE001
        return [], f"{type(exc).__name__}: {exc}"
    return files, None


def select_diverse(files: list[dict], target_bytes: int) -> list[dict]:
    """ext × size_bucket 그룹 라운드로빈, tag 많은 파일 우선.

    Pass1: 그룹당 1개씩 (다양성 확보, 단일 파일이 target*2 초과하면 스킵)
    Pass2: target 근접까지 추가 (total + size > target*1.3 면 스킵)
    """
    if not files:
        return []

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for f in files:
        groups[(f["ext"], size_bucket(f["size"]))].append(f)

    for g in groups.values():
        g.sort(key=lambda f: (-len(f["tags"]), random.random()))

    group_keys = list(groups.keys())
    random.shuffle(group_keys)

    selected: list[dict] = []
    total = 0

    # Pass 1: diversity
    for key in group_keys:
        bucket = groups[key]
        if not bucket:
            continue
        cand = bucket[0]
        if cand["size"] > target_bytes * 2 and selected:
            continue
        bucket.pop(0)
        selected.append(cand)
        total += cand["size"]

    # Pass 2: top-up
    progressing = True
    while progressing and total < target_bytes:
        progressing = False
        for key in group_keys:
            if total >= target_bytes:
                break
            bucket = groups[key]
            if not bucket:
                continue
            cand = bucket[0]
            if total + cand["size"] > target_bytes * 1.3:
                continue
            bucket.pop(0)
            selected.append(cand)
            total += cand["size"]
            progressing = True

    return selected


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def main() -> int:
    if not NAS_ROOT.exists():
        print(f"ERROR: NAS_ROOT not found: {NAS_ROOT}", file=sys.stderr)
        return 1

    projects = sorted(p for p in NAS_ROOT.iterdir() if p.is_dir())
    target_per_project = TARGET_MB_PER_PROJECT * 1024 * 1024
    max_total_bytes = int(MAX_GB_TOTAL * 1024**3)

    print(
        f"=== {len(projects)} projects under {NAS_ROOT}"
        f" | target {TARGET_MB_PER_PROJECT}MB/project"
        f" | cap {MAX_GB_TOTAL}GB ==="
    )

    all_selected: list[dict] = []
    per_project: list[dict] = []
    total_bytes = 0
    t0 = time.time()

    for i, project in enumerate(projects, 1):
        t1 = time.time()
        print(f"[{i:>2}/{len(projects)}] {project.name} ... ", end="", flush=True)

        remaining = max_total_bytes - total_bytes
        if remaining <= 0:
            print("skip (budget exhausted)")
            per_project.append(
                {"project": project.name, "status": "skipped_budget", "selected_bytes": 0}
            )
            continue

        files, err = index_project(project)
        if err:
            print(f"ERROR ({time.time() - t1:.1f}s): {err}")
            per_project.append(
                {"project": project.name, "status": "error", "error": err, "selected_bytes": 0}
            )
            continue

        if not files:
            print(f"no allowed files ({time.time() - t1:.1f}s)")
            per_project.append(
                {"project": project.name, "status": "empty", "selected_bytes": 0}
            )
            continue

        project_target = min(target_per_project, remaining)
        selected = select_diverse(files, project_target)
        sel_bytes = sum(f["size"] for f in selected)

        if sel_bytes > remaining:
            selected.sort(key=lambda f: f["size"])
            trimmed: list[dict] = []
            running = 0
            for f in selected:
                if running + f["size"] > remaining:
                    continue
                trimmed.append(f)
                running += f["size"]
            selected = trimmed
            sel_bytes = running

        for f in selected:
            f["project"] = project.name
            all_selected.append(f)
        total_bytes += sel_bytes

        ext_cnt: Counter[str] = Counter(f["ext"] for f in selected)
        bucket_cnt: Counter[str] = Counter(size_bucket(f["size"]) for f in selected)
        tag_cnt: Counter[str] = Counter()
        for f in selected:
            tag_cnt.update(f["tags"])

        per_project.append(
            {
                "project": project.name,
                "status": "ok",
                "indexed_file_count": len(files),
                "selected_file_count": len(selected),
                "selected_bytes": sel_bytes,
                "selected_bytes_human": human_bytes(sel_bytes),
                "ext_counts": dict(ext_cnt),
                "size_bucket_counts": dict(bucket_cnt),
                "tag_counts": dict(tag_cnt),
                "scan_seconds": round(time.time() - t1, 2),
            }
        )

        print(
            f"{len(selected)}/{len(files)} files, "
            f"{human_bytes(sel_bytes)} "
            f"(total {human_bytes(total_bytes)}, {time.time() - t1:.1f}s)"
        )

    with MANIFEST_OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["project", "src_path", "size_bytes", "ext", "dest_rel", "tags"])
        for s in all_selected:
            w.writerow(
                [
                    s["project"],
                    s["src_path"],
                    s["size"],
                    s["ext"],
                    s["rel_path"],
                    ",".join(s["tags"]),
                ]
            )

    g_tags: Counter[str] = Counter()
    g_exts: Counter[str] = Counter()
    g_buckets: Counter[str] = Counter()
    for s in all_selected:
        g_tags.update(s["tags"])
        g_exts[s["ext"]] += 1
        g_buckets[size_bucket(s["size"])] += 1

    summary = {
        "scan_root": str(NAS_ROOT),
        "scanned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_seconds": round(time.time() - t0, 1),
        "target_mb_per_project": TARGET_MB_PER_PROJECT,
        "max_gb_total": MAX_GB_TOTAL,
        "project_count": len(projects),
        "selected_file_count": len(all_selected),
        "selected_bytes": total_bytes,
        "selected_bytes_human": human_bytes(total_bytes),
        "global_ext_counts": dict(g_exts.most_common()),
        "global_size_bucket_counts": dict(g_buckets),
        "global_tag_counts": dict(g_tags.most_common()),
        "per_project": per_project,
    }
    STATS_OUT.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print(f"→ {MANIFEST_OUT} ({len(all_selected)} files)")
    print(f"→ {STATS_OUT}")
    print(
        f"TOTAL: {human_bytes(total_bytes)} / "
        f"{MAX_GB_TOTAL}GB cap in {summary['total_seconds']}s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
