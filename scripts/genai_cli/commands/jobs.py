"""`genai-cli jobs retry <job_id>`."""

from __future__ import annotations

import argparse

from genai_cli.client import HTTPClient
from genai_cli.output import auto_format, emit


def add_arguments(parser: argparse.ArgumentParser) -> None:
    from genai_cli.cli import COMMON
    sub = parser.add_subparsers(dest="jobs_cmd", required=True)
    p_retry = sub.add_parser("retry", help="실패 job 재시도", parents=[COMMON])
    p_retry.add_argument("job_id")


def run(args, client: HTTPClient) -> int:
    if args.jobs_cmd != "retry":
        return 1
    fmt = auto_format(args.format)
    result = client.retry_job(args.job_id)
    emit(result, fmt=fmt)
    return 0
