"""`genai-cli submit <engine> ...`."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from genai_cli.client import HTTPClient
from genai_cli.output import auto_format, emit
from genai_cli.commands.wait import wait_for_batch


_ALLOWED_EXT = {".png", ".jpg", ".jpeg", ".webp"}


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("engine", help="kling | higgsfield | veo | nanobanana | gpt_image")
    # image / images-from-dir / text-only — 셋 중 하나. text-only 는 veo 전용 (txt2video).
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--image", help="단일 이미지 파일 경로")
    src.add_argument("--images-from-dir", dest="images_dir",
                     help="디렉토리 안 모든 이미지 (alphabetical)")
    src.add_argument("--text-only", action="store_true",
                     help="이미지 없이 프롬프트만 (veo txt2video 전용)")
    prompt_src = parser.add_mutually_exclusive_group(required=True)
    prompt_src.add_argument("--prompt", help="프롬프트 문자열")
    prompt_src.add_argument("--prompt-file", help="프롬프트가 들어있는 텍스트 파일")

    # engine 별 옵션 — 미설정 시 서버 default
    parser.add_argument("--model", dest="model_name", help="Kling/Veo model_name")
    parser.add_argument("--mode",
                        help="Kling mode (std|pro|master). --text-only 시 자동 'txt2video'")
    parser.add_argument("--duration", help="duration 초 (Kling: 3-15, >10초는 kling-v3만 실동작; Veo: 4|6|8)")
    parser.add_argument("--aspect-ratio", dest="aspect_ratio",
                        help="16:9 | 9:16 | 1:1 (Veo 는 1:1 미지원)")

    parser.add_argument("--max-batch", type=int, default=20,
                        help="dir 입력 시 한 batch 당 최대 장수 (default 20)")
    parser.add_argument("--wait", action="store_true",
                        help="결과 도착까지 대기 (default: 즉시 반환)")
    parser.add_argument("--timeout", type=int, default=900,
                        help="--wait 시 대기 한도 초 (default 900)")
    parser.add_argument("--interval", type=int, default=10,
                        help="--wait polling 주기 초 (default 10)")


def _collect_files(args) -> list[Path]:
    if getattr(args, "text_only", False):
        if args.engine != "veo":
            print(
                f"error: --text-only 는 veo 전용 (engine={args.engine!r})",
                file=sys.stderr,
            )
            sys.exit(1)
        return []
    if args.image:
        p = Path(args.image).expanduser()
        if not p.exists():
            print(f"error: image not found: {p}", file=sys.stderr)
            sys.exit(1)
        if p.suffix.lower() not in _ALLOWED_EXT:
            print(f"error: unsupported ext {p.suffix} (allowed {sorted(_ALLOWED_EXT)})",
                  file=sys.stderr)
            sys.exit(1)
        return [p]
    # images-from-dir
    d = Path(args.images_dir).expanduser()
    if not d.is_dir():
        print(f"error: not a directory: {d}", file=sys.stderr)
        sys.exit(1)
    files = sorted(
        p for p in d.iterdir()
        if p.is_file() and p.suffix.lower() in _ALLOWED_EXT
    )
    if not files:
        print(f"error: no image files in {d} (allowed {sorted(_ALLOWED_EXT)})",
              file=sys.stderr)
        sys.exit(1)
    if len(files) > args.max_batch:
        print(
            f"error: {len(files)} files > --max-batch={args.max_batch}. "
            f"디렉토리를 분할하거나 --max-batch 조정.",
            file=sys.stderr,
        )
        sys.exit(1)
    return files


def _resolve_prompt(args) -> str:
    if args.prompt:
        return args.prompt
    p = Path(args.prompt_file).expanduser()
    if not p.exists():
        print(f"error: prompt-file not found: {p}", file=sys.stderr)
        sys.exit(1)
    return p.read_text(encoding="utf-8").strip()


def run(args, client: HTTPClient) -> int:
    files = _collect_files(args)
    prompt = _resolve_prompt(args)
    # --text-only 면 자동으로 mode='txt2video' 주입 (사용자가 명시 --mode 한 경우 우선)
    mode = args.mode
    if getattr(args, "text_only", False) and not mode:
        mode = "txt2video"
    options = {
        "model_name": args.model_name,
        "mode": mode,
        "duration": args.duration,
        "aspect_ratio": args.aspect_ratio,
    }
    options = {k: v for k, v in options.items() if v is not None}

    result = client.submit_batch(
        engine=args.engine, prompt=prompt, files=files, options=options,
    )

    # batch_id 는 stdout 첫 줄에 항상 노출 (Codex R2: CI 가 jq 로 파싱 가능)
    batch_id = result.get("batch_id") if isinstance(result, dict) else None
    fmt = auto_format(args.format)
    emit(result, fmt=fmt)

    if args.wait and batch_id:
        print("", file=sys.stderr)  # 구분 줄
        return wait_for_batch(client, batch_id,
                              timeout=args.timeout, interval=args.interval,
                              fmt=fmt)
    return 0
