#!/usr/bin/env python3
"""dataset pull — resolve a pinned DVC dataset version from dataset_catalog and `dvc get` it.

  python scripts/dataset_pull.py --task sam3_detection --alias current --dest /data/pull/fire

Resolves task+alias → catalog row (git_rev + dvc_out_path + dvc_md5) → `dvc get` from the
bare repo (bytes stream from MinIO via the repo's .dvc/config) → verify md5. DRY-RUN by
default (no dvc invoked) — scaffolding only. Real pull = operation time with --no-dry-run.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vlm_pipeline.lib.dvc_pull import build_dvc_get_argv, verify_pulled_md5  # noqa: E402


def _repo_path() -> str:
    return os.environ.get("DVC_DATA_REPO_PATH", "/srv/data-repos/dvc-datasets.git")


def _open_db():
    """Open the pipeline PG resource (psycopg2-direct, like scripts/promote_model.py)."""
    from vlm_pipeline.resources.postgres import PostgresResource  # lazy: keep import cheap for --help

    dsn = os.environ.get("DATAOPS_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("DATAOPS_POSTGRES_DSN not set")
    return PostgresResource(dsn=dsn)


def _run_dvc_get(argv: list[str]) -> None:
    subprocess.run(argv, check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pull a pinned DVC dataset version from dataset_catalog.")
    parser.add_argument("--task", required=True)
    parser.add_argument("--alias", default="current")
    parser.add_argument("--dest", required=True)
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false")
    parser.set_defaults(dry_run=True)
    args = parser.parse_args(argv)

    db = _open_db()
    row = db.get_catalog_by_alias(args.task, args.alias)
    if row is None:
        print(f"[dataset-pull] no '{args.alias}' alias pinned for task={args.task}", file=sys.stderr)
        return 2

    repo = _repo_path()
    get_argv = build_dvc_get_argv(repo, row["dvc_out_path"], row["git_rev"], args.dest)
    print(
        f"[dataset-pull] resolved task={args.task} alias={args.alias} "
        f"git_rev={row['git_rev']} out={row['dvc_out_path']} subject={row.get('commit_subject')!r}"
    )
    print(f"[dataset-pull] argv: {' '.join(get_argv)}")

    if args.dry_run:
        print("[dataset-pull] DRY-RUN — not invoking dvc get (use --no-dry-run to actually pull)")
        return 0

    _run_dvc_get(get_argv)
    # md5 verify is a best-effort post-step at operation time (dvc writes <dest>.dvc with the md5).
    if not verify_pulled_md5(row.get("dvc_md5"), _computed_md5(args.dest)):
        print("[dataset-pull] md5 mismatch — pulled bytes do not match catalog dvc_md5", file=sys.stderr)
        return 3
    print(f"[dataset-pull] OK → {args.dest}")
    return 0


def _computed_md5(dest: str) -> str | None:
    """Operation-time md5 of the pulled tree (None in scaffolding — real impl reads <dest>.dvc)."""
    return None


if __name__ == "__main__":
    raise SystemExit(main())
