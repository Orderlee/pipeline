"""`genai-cli engines` — /healthz 의 engines 목록."""

from __future__ import annotations

import argparse

from genai_cli.client import HTTPClient
from genai_cli.output import auto_format, emit


def add_arguments(parser: argparse.ArgumentParser) -> None:
    pass


def run(args, client: HTTPClient) -> int:
    fmt = auto_format(args.format)
    data = client.healthz()
    if fmt == "table":
        engines = data.get("engines", [])
        print(f"base: {client.cfg.base}")
        print(f"status: {data.get('status')}")
        print("engines:")
        for e in engines:
            print(f"  - {e}")
    else:
        emit(data, fmt=fmt)
    return 0
