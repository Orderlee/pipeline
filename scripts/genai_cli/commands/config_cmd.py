"""`genai-cli config init / show`."""

from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path

from genai_cli.config import (
    CONFIG_PATH,
    DEFAULT_BASE,
    _read_config_file,
    write_config,
)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    from genai_cli.cli import COMMON
    sub = parser.add_subparsers(dest="config_cmd", required=True)
    sub.add_parser("init", help="config 파일 인터랙티브 생성", parents=[COMMON])
    sub.add_parser("show", help="현재 config 위치/내용 표시 (비밀번호 마스킹)", parents=[COMMON])
    sub.add_parser("path", help="config 파일 경로 출력", parents=[COMMON])


def run(args, _client=None) -> int:
    if args.config_cmd == "path":
        print(CONFIG_PATH)
        return 0
    if args.config_cmd == "show":
        if not CONFIG_PATH.exists():
            print(f"config 파일 없음: {CONFIG_PATH}", file=sys.stderr)
            return 1
        cfg = _read_config_file().get("default", {})
        print(f"path: {CONFIG_PATH}")
        print(f"base: {cfg.get('base', DEFAULT_BASE)}")
        print(f"user: {cfg.get('user', '—')}")
        pw = cfg.get("password")
        print(f"password: {'*' * 4 + pw[-2:] if pw else '—'}")
        return 0
    if args.config_cmd == "init":
        base = input(f"base URL [{DEFAULT_BASE}]: ").strip() or DEFAULT_BASE
        user = input("username [user]: ").strip() or "user"
        password = getpass.getpass(f"password for {user}@{base}: ")
        if not password:
            print("password 가 비어있어 저장 안 함.", file=sys.stderr)
            return 1
        path = write_config(base=base, user=user, password=password)
        print(f"saved: {path} (mode 0600)")
        return 0
    return 1
