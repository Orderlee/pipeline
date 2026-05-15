"""`genai-cli costs --range day|week|month`."""

from __future__ import annotations

import argparse

from genai_cli.client import HTTPClient
from genai_cli.output import auto_format, emit


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--range", dest="range_", default="week",
                        choices=["day", "week", "month"])


def run(args, client: HTTPClient) -> int:
    fmt = auto_format(args.format)
    summary = client.costs(range_=args.range_)
    emit(summary, fmt=fmt)
    return 0
