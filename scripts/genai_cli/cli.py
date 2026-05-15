"""argparse 라우터 — `genai-cli <subcommand> ...`."""

from __future__ import annotations

import argparse
import sys

from genai_cli import __version__
from genai_cli.client import APIError, HTTPClient
from genai_cli.config import load_config
from genai_cli.commands import (
    batches as cmd_batches,
    bulk_submit as cmd_bulk_submit,
    config_cmd,
    costs as cmd_costs,
    engines as cmd_engines,
    jobs as cmd_jobs,
    submit as cmd_submit,
    wait as cmd_wait,
)


def make_common_parent() -> argparse.ArgumentParser:
    """모든 subparser 가 parents=[...] 로 상속할 공통 옵션."""
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--base", help="API base URL override")
    parent.add_argument("--user", help="username override")
    parent.add_argument("--no-prompt", action="store_true",
                        help="누락 자격증명에 대해 prompt 띄우지 않음 (CI)")
    fmt = parent.add_mutually_exclusive_group()
    fmt.add_argument("--json", dest="format", action="store_const", const="json",
                     help="JSON 출력 강제")
    fmt.add_argument("--table", dest="format", action="store_const", const="table",
                     help="table 출력 강제")
    parent.set_defaults(format=None)
    return parent


COMMON = make_common_parent()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="genai-cli",
        description="GenAI Studio CLI (thin HTTP client)",
        parents=[COMMON],
    )
    p.add_argument("--version", action="version", version=__version__)

    sub = p.add_subparsers(dest="cmd", required=True)

    def _mk(name, help_):
        return sub.add_parser(name, help=help_, parents=[COMMON])

    cmd_submit.add_arguments(_mk("submit", "batch 제출"))
    cmd_bulk_submit.add_arguments(_mk("bulk-submit", "대량 (이미지×프롬프트) bulk 제출"))
    cmd_batches.add_arguments(_mk("batches", "batch 목록 / 상세"))
    cmd_wait.add_arguments(_mk("wait", "batch 종료까지 polling"))
    cmd_jobs.add_arguments(_mk("jobs", "개별 job retry"))
    cmd_costs.add_arguments(_mk("costs", "비용 / 활동 집계"))
    cmd_engines.add_arguments(_mk("engines", "활성 엔진 / health"))
    config_cmd.add_arguments(_mk("config", "config 파일 init / show / path"))

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    # config 명령은 server 호출 불필요
    if args.cmd == "config":
        return config_cmd.run(args)

    cfg = load_config(base_override=args.base, interactive=not args.no_prompt)
    if args.user:
        cfg.user = args.user
    if not cfg.auth:
        # 자격증명 없는데 prompt 도 막혔으면 CI 가 fail-loud 하게.
        # health/engines 처럼 일부 auth 우회 가능한 endpoint 가 있다면 그건 시도해도 됨.
        if args.cmd not in ("engines",):
            print("error: 인증 정보 누락. GENAI_CLI_USER/PASS env 또는 config init.",
                  file=sys.stderr)
            return 1

    client = HTTPClient(cfg)

    try:
        if args.cmd == "submit":
            return cmd_submit.run(args, client)
        if args.cmd == "bulk-submit":
            return cmd_bulk_submit.run(args, client)
        if args.cmd == "batches":
            return cmd_batches.run(args, client)
        if args.cmd == "wait":
            return cmd_wait.run(args, client)
        if args.cmd == "jobs":
            return cmd_jobs.run(args, client)
        if args.cmd == "costs":
            return cmd_costs.run(args, client)
        if args.cmd == "engines":
            return cmd_engines.run(args, client)
    except APIError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
