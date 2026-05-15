"""`genai-cli wait <batch_id>` — done/failed/partial 까지 polling."""

from __future__ import annotations

import argparse
import sys
import time

from genai_cli.client import APIError, HTTPClient
from genai_cli.output import auto_format, emit


_TERMINAL = {"succeeded", "failed", "partial_success", "cancelled"}


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("batch_id")
    parser.add_argument("--timeout", type=int, default=900,
                        help="대기 한도 초 (default 900)")
    parser.add_argument("--interval", type=int, default=10,
                        help="polling 주기 초 (default 10)")


def wait_for_batch(client: HTTPClient, batch_id: str, *,
                   timeout: int, interval: int, fmt: str = "table") -> int:
    """polling 후 종료 코드 반환.
      0 = succeeded
      2 = failed
      3 = partial_success
      4 = timeout
    """
    start = time.monotonic()
    last_status = None
    while True:
        try:
            batch = client.get_batch(batch_id)
        except APIError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        status = batch.get("status")
        if status != last_status:
            elapsed = int(time.monotonic() - start)
            print(
                f"[{elapsed:>4}s] {batch_id} · {status} "
                f"({batch.get('n_succeeded',0)}/{batch.get('n_failed',0)}/{batch.get('n_total',0)})",
                file=sys.stderr,
            )
            last_status = status
        if status in _TERMINAL:
            emit(batch, fmt=fmt)
            return {
                "succeeded": 0,
                "partial_success": 3,
                "failed": 2,
                "cancelled": 2,
            }.get(status, 1)
        if time.monotonic() - start > timeout:
            print(f"timeout: batch {batch_id} 가 {timeout}s 안에 끝나지 않음",
                  file=sys.stderr)
            emit(batch, fmt=fmt)
            return 4
        time.sleep(interval)


def run(args, client: HTTPClient) -> int:
    fmt = auto_format(args.format)
    return wait_for_batch(
        client, args.batch_id,
        timeout=args.timeout, interval=args.interval, fmt=fmt,
    )
