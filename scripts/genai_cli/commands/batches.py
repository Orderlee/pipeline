"""`genai-cli batches list / show`."""

from __future__ import annotations

import argparse

from genai_cli.client import HTTPClient
from genai_cli.output import auto_format, emit


def add_arguments(parser: argparse.ArgumentParser) -> None:
    from genai_cli.cli import COMMON
    sub = parser.add_subparsers(dest="batches_cmd", required=True)

    p_list = sub.add_parser("list", help="batch 목록", parents=[COMMON])
    p_list.add_argument("--status",
                        help="필터: pending|running|succeeded|partial_success|failed|cancelled")
    p_list.add_argument("--engine", help="필터: kling|higgsfield|veo|nanobanana|gpt_image")
    p_list.add_argument("--limit", type=int, default=50)

    p_show = sub.add_parser("show", help="batch + jobs 상세", parents=[COMMON])
    p_show.add_argument("batch_id")


def run(args, client: HTTPClient) -> int:
    fmt = auto_format(args.format)
    if args.batches_cmd == "list":
        rows = client.list_batches(
            status=args.status, engine=args.engine, limit=args.limit,
        )
        cols = ["batch_id", "engine", "output_media", "status",
                "n_total", "n_succeeded", "n_failed", "submitted_at"]
        emit(rows, fmt=fmt, columns=cols, empty_msg="batch 없음")
        return 0
    if args.batches_cmd == "show":
        batch = client.get_batch(args.batch_id)
        emit(batch, fmt=fmt)
        return 0
    return 1
