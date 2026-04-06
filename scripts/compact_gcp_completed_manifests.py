#!/usr/bin/env python3
"""Compact completed GCP auto-bootstrap manifests into one summary per source unit/signature."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from vlm_pipeline.defs.ingest.compaction import (  # noqa: E402
    collect_processed_manifest_groups,
    compact_completed_manifest_records,
    discover_compactable_manifest_groups,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compact processed GCP manifests into completed summaries.",
    )
    parser.add_argument(
        "--manifest-dir",
        default="/nas/incoming/.manifests",
        help="Manifest root directory. Example: /nas/incoming/.manifests",
    )
    parser.add_argument(
        "--archive-dir",
        default="/nas/archive",
        help="Archive root directory. Example: /nas/archive",
    )
    parser.add_argument(
        "--source-unit-like",
        default="",
        help="Optional substring filter for source_unit_name/source_unit_path.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply compaction. Dry-run by default.",
    )
    return parser.parse_args()


def _matches_filter(report: dict, needle: str) -> bool:
    if not needle:
        return True
    candidate = " ".join(
        [
            str(report.get("source_unit_name", "")),
            str(report.get("source_unit_path", "")),
        ]
    ).lower()
    return needle.lower() in candidate


def main() -> int:
    args = _parse_args()
    manifest_dir = Path(args.manifest_dir)
    archive_dir = Path(args.archive_dir)

    reports = [
        report
        for report in discover_compactable_manifest_groups(
            manifest_dir=manifest_dir,
            archive_dir=archive_dir,
        )
        if _matches_filter(report, args.source_unit_like)
    ]

    if not args.yes:
        print(
            json.dumps(
                {
                    "mode": "dry-run",
                    "manifest_dir": str(manifest_dir),
                    "archive_dir": str(archive_dir),
                    "candidate_count": len(reports),
                    "candidates": reports[:50],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    processed_groups = collect_processed_manifest_groups(manifest_dir / "processed")
    applied: list[dict] = []
    for report in reports:
        key = (
            str(report.get("source_unit_path", "")),
            str(report.get("stable_signature", "")),
        )
        applied.append(
            compact_completed_manifest_records(
                manifest_dir=manifest_dir,
                archive_dir=archive_dir,
                records=processed_groups.get(key, []),
                source_unit_path=key[0],
                stable_signature=key[1],
                apply=True,
            )
        )

    print(
        json.dumps(
            {
                "mode": "apply",
                "manifest_dir": str(manifest_dir),
                "archive_dir": str(archive_dir),
                "candidate_count": len(reports),
                "applied_count": len(applied),
                "results": applied[:50],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
