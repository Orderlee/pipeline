#!/usr/bin/env python3
"""Legacy NAS 데이터 프로파일링 — read-only 스캔.

NAS_ROOT 아래 각 최상위 프로젝트 폴더를 os.walk + os.stat 로 스캔해
파이프라인 투입 전 리스크 평가에 필요한 통계를 생성한다.

산출물:
    legacy_profile.json       — 프로젝트별 상세 (기계 처리용)
    legacy_profile_summary.md — 사람 읽기용 리포트

실행:
    python3 profile.py
    NAS_ROOT=/custom/path python3 profile.py
"""

from __future__ import annotations

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
JSON_OUT = OUT_DIR / "legacy_profile.json"
MD_OUT = OUT_DIR / "legacy_profile_summary.md"

# lib/validator.py 와 1:1 동기화
ALLOWED_EXTS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp", ".heic",
    ".mp4", ".avi", ".mov", ".mkv", ".webm", ".mpeg", ".mpg", ".m4v", ".mts", ".m2ts",
}
LARGE_FILE_BYTES = 450 * 1024 * 1024  # Vertex preview 재인코딩 임계
SAMPLE_PATH_COUNT = 3

HANGUL_RE = re.compile(r"[가-힣]")
SPECIAL_RE = re.compile(r"[^\w\s가-힣.\-/]")  # 공백/한글/영숫자/./-// 제외

random.seed(20260422)


def percentile(sorted_values: list[int], p: float) -> int:
    if not sorted_values:
        return 0
    k = int(round((len(sorted_values) - 1) * p))
    return sorted_values[k]


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def profile_project(project_dir: Path) -> dict:
    start = time.time()
    file_count = 0
    total_bytes = 0
    sizes: list[int] = []
    ext_counts: Counter[str] = Counter()
    has_hangul = has_space = has_ampersand = has_special = uppercase_ext = 0
    non_allowed = large_file = 0
    max_depth = 0
    folders_with_files = 0
    max_files_in_folder = 0
    folders_over_100 = 0
    dup_key: dict[tuple[int, str], int] = defaultdict(int)
    all_rel_paths: list[str] = []
    error: str | None = None

    try:
        for root, dirs, files in os.walk(project_dir):
            # 숨김/macOS 메타 디렉토리 제외
            dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__MACOSX"]
            rel_root = Path(root).relative_to(project_dir)
            depth = 0 if str(rel_root) == "." else len(rel_root.parts)

            folder_file_count = 0
            for fname in files:
                # macOS AppleDouble / 숨김 파일 제외
                if fname.startswith(".") or fname.startswith("._"):
                    continue
                full = Path(root) / fname
                try:
                    size = full.stat().st_size
                except OSError:
                    continue

                rel_str = str(rel_root / fname) if str(rel_root) != "." else fname

                file_count += 1
                total_bytes += size
                sizes.append(size)
                folder_file_count += 1
                all_rel_paths.append(rel_str)

                # Extension
                ext = os.path.splitext(fname)[1]
                ext_lower = ext.lower()
                ext_counts[ext_lower] += 1
                if ext_lower not in ALLOWED_EXTS:
                    non_allowed += 1
                if ext and ext != ext_lower:
                    uppercase_ext += 1

                # Name characteristics — rel_str(경로 전체) 기준
                if HANGUL_RE.search(rel_str):
                    has_hangul += 1
                if " " in rel_str:
                    has_space += 1
                if "&" in rel_str:
                    has_ampersand += 1
                if SPECIAL_RE.search(rel_str):
                    has_special += 1

                if size >= LARGE_FILE_BYTES:
                    large_file += 1

                dup_key[(size, fname.lower())] += 1

            if folder_file_count > 0:
                folders_with_files += 1
                if folder_file_count > max_files_in_folder:
                    max_files_in_folder = folder_file_count
                if folder_file_count > 100:
                    folders_over_100 += 1

            if depth > max_depth:
                max_depth = depth

    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"

    sizes.sort()
    dup_groups = [n for n in dup_key.values() if n > 1]

    return {
        "project": project_dir.name,
        "error": error,
        "scan_seconds": round(time.time() - start, 2),
        "file_count": file_count,
        "total_bytes": total_bytes,
        "ext_counts": dict(ext_counts.most_common()),
        "size_p50": percentile(sizes, 0.5),
        "size_p90": percentile(sizes, 0.9),
        "size_p95": percentile(sizes, 0.95),
        "size_p99": percentile(sizes, 0.99),
        "size_max": sizes[-1] if sizes else 0,
        "folder_count": folders_with_files,
        "max_depth": max_depth,
        "max_files_in_folder": max_files_in_folder,
        "folders_over_100_files": folders_over_100,
        "has_hangul_count": has_hangul,
        "has_space_count": has_space,
        "has_ampersand_count": has_ampersand,
        "has_special_count": has_special,
        "uppercase_ext_count": uppercase_ext,
        "non_allowed_ext_count": non_allowed,
        "large_file_count": large_file,
        "dup_candidate_groups": len(dup_groups),
        "dup_candidate_files": sum(dup_groups),
        "sample_paths": (
            random.sample(all_rel_paths, min(SAMPLE_PATH_COUNT, len(all_rel_paths)))
            if all_rel_paths
            else []
        ),
    }


def build_markdown(agg: dict, results: list[dict]) -> str:
    lines: list[str] = []
    lines.append("# Legacy NAS Profile")
    lines.append("")
    lines.append(f"- **Scan root**: `{agg['scan_root']}`")
    lines.append(f"- **Scanned at**: {agg['scanned_at']}")
    lines.append(f"- **Scan duration**: {agg['total_seconds']}s")
    lines.append(
        f"- **Projects**: {agg['project_count']} "
        f"(errors: {agg['project_count_with_errors']})"
    )
    lines.append(f"- **Total files**: {agg['total_file_count']:,}")
    lines.append(f"- **Total size**: {agg['total_bytes_human']}")
    lines.append("")
    lines.append("## Extension distribution (top 20)")
    lines.append("")
    lines.append("| ext | count |")
    lines.append("|---|---:|")
    for ext, n in agg["extension_distribution_top20"].items():
        lines.append(f"| `{ext or '(none)'}` | {n:,} |")
    lines.append("")
    lines.append("## Pipeline blocker aggregates")
    lines.append("")
    lines.append(f"- **Non-allowed extensions**: {agg['non_allowed_ext_files']:,}")
    lines.append(
        f"- **Files ≥ 450MB** (Vertex preview 재인코딩 필요): "
        f"{agg['large_files_over_450mb']:,}"
    )
    nr = agg["name_risk_counts"]
    lines.append(f"- **Hangul in path**: {nr['has_hangul']:,}")
    lines.append(f"- **Space in path**: {nr['has_space']:,}")
    lines.append(f"- **Ampersand (&)**: {nr['has_ampersand']:,}")
    lines.append(f"- **Other special chars**: {nr['has_special']:,}")
    lines.append(f"- **Uppercase ext (.MOV 등)**: {nr['uppercase_ext']:,}")
    lines.append("")
    lines.append("## Per-project summary")
    lines.append("")
    header = (
        "| project | files | size | max_depth | folders>100 | dup_groups "
        "| p95_size | non_allowed | ≥450MB | 한글 | 공백 | 특수 | err |"
    )
    sep = "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    lines.append(header)
    lines.append(sep)
    for r in results:
        err_mark = "⚠️" if r.get("error") else ""
        lines.append(
            f"| {r['project']} | {r['file_count']:,} | "
            f"{human_bytes(r['total_bytes'])} | {r['max_depth']} | "
            f"{r['folders_over_100_files']} | {r['dup_candidate_groups']} | "
            f"{human_bytes(r['size_p95'])} | {r['non_allowed_ext_count']} | "
            f"{r['large_file_count']} | {r['has_hangul_count']} | "
            f"{r['has_space_count']} | {r['has_special_count']} | {err_mark} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    if not NAS_ROOT.exists():
        print(f"ERROR: NAS_ROOT not found: {NAS_ROOT}", file=sys.stderr)
        return 1

    projects = sorted(p for p in NAS_ROOT.iterdir() if p.is_dir())
    print(f"=== Profiling {len(projects)} projects under {NAS_ROOT} ===\n")

    t0 = time.time()
    results: list[dict] = []
    for i, project in enumerate(projects, 1):
        print(f"[{i:>2}/{len(projects)}] {project.name} ... ", end="", flush=True)
        info = profile_project(project)
        if info.get("error"):
            print(f"ERROR ({info['scan_seconds']}s): {info['error']}")
        else:
            print(
                f"{info['file_count']:,} files, "
                f"{human_bytes(info['total_bytes'])} "
                f"({info['scan_seconds']}s)"
            )
        results.append(info)

    ext_union: Counter[str] = Counter()
    for r in results:
        for ext, n in r["ext_counts"].items():
            ext_union[ext] += n

    aggregate = {
        "scan_root": str(NAS_ROOT),
        "scanned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_seconds": round(time.time() - t0, 1),
        "project_count": len(projects),
        "project_count_with_errors": sum(1 for r in results if r.get("error")),
        "total_file_count": sum(r["file_count"] for r in results),
        "total_bytes": sum(r["total_bytes"] for r in results),
        "extension_distribution_top20": dict(ext_union.most_common(20)),
        "non_allowed_ext_files": sum(r["non_allowed_ext_count"] for r in results),
        "large_files_over_450mb": sum(r["large_file_count"] for r in results),
        "name_risk_counts": {
            "has_hangul": sum(r["has_hangul_count"] for r in results),
            "has_space": sum(r["has_space_count"] for r in results),
            "has_ampersand": sum(r["has_ampersand_count"] for r in results),
            "has_special": sum(r["has_special_count"] for r in results),
            "uppercase_ext": sum(r["uppercase_ext_count"] for r in results),
        },
    }
    aggregate["total_bytes_human"] = human_bytes(aggregate["total_bytes"])

    out = {"aggregate": aggregate, "projects": results}
    JSON_OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    MD_OUT.write_text(build_markdown(aggregate, results), encoding="utf-8")

    print()
    print(f"→ {JSON_OUT} ({JSON_OUT.stat().st_size / 1024:.1f} KB)")
    print(f"→ {MD_OUT}")
    print()
    print(
        f"TOTAL: {aggregate['total_file_count']:,} files, "
        f"{aggregate['total_bytes_human']}, "
        f"{aggregate['total_seconds']}s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
