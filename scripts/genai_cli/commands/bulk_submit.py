"""`genai-cli bulk-submit` — 대량 (이미지×프롬프트) batch 제출.

설계 (Codex 합의안: Design C on top of A):
  - 사용자 입력: canonical manifest JSON 또는 편의 입력(prompts file + images dir)
  - 내부 동작: prompt 별로 group 화 → MAX_FILES_PER_BATCH 단위 chunk → 기존
    POST /genai/batches 를 여러 번 호출 (스키마 변경 없음)
  - 모든 batch 는 동일 bulk_group_id 를 options_json 으로 송신 → UI 그룹 필터.

안전장치:
  - --dry-run 으로 카운트/비용 미리보기 → --confirm 안 붙이면 제출 불가
  - 제출 전 GET /genai/limits 사전 체크 (PG 장애시 500 받아 거부)
  - --sleep 으로 batch 간 throttle (default 0.5s) — Vertex/Kling rate-limit 회피

종료 코드:
  0  성공 (또는 dry-run)
  1  사용자 오류 / 사전 체크 실패
  2  일부/전체 batch 제출 실패 (partial)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Iterable

from genai_cli.client import APIError, HTTPClient
from genai_cli.output import auto_format, emit


_ALLOWED_EXT = {".png", ".jpg", ".jpeg", ".webp"}
_VALID_ENGINES = {"kling", "higgsfield", "veo", "nanobanana", "gpt_image"}


def add_arguments(parser: argparse.ArgumentParser) -> None:
    # 입력 — manifest OR convenience (둘 다 가능 — manifest 우선)
    src = parser.add_argument_group("입력 (manifest 또는 편의)")
    src.add_argument("--file", dest="manifest_file",
                     help="canonical bulk manifest JSON 경로 (--prompts-file 보다 우선)")
    src.add_argument("--prompts-file",
                     help="프롬프트 JSON. list[str] 또는 {'prompts':[...]}")
    src.add_argument("--images-from-dir", dest="images_dir",
                     help="이미지 디렉토리 (편의 입력 — paired/cartesian 모드)")
    src.add_argument("--text-only", action="store_true",
                     help="이미지 없이 프롬프트만 (veo txt2video 전용)")
    src.add_argument("--engine", help="엔진 (편의 입력시 필수, manifest 면 override)")

    # pairing
    parser.add_argument(
        "--pair-mode", choices=["paired", "cartesian"], default="paired",
        help="paired (1:1, N==M 필요, default) | cartesian (N×M 모든 조합)",
    )

    # 엔진 옵션 shorthand (manifest 면 override)
    opt = parser.add_argument_group("엔진 옵션 (manifest options 와 병합, CLI 우선)")
    opt.add_argument("--model", dest="model_name", help="Kling/Veo model_name")
    opt.add_argument("--mode", help="Kling mode (std|pro|master)")
    opt.add_argument("--duration", help="duration 초 (Kling: 5|10, Veo: 4|6|8)")
    opt.add_argument("--aspect-ratio", dest="aspect_ratio",
                     help="16:9 | 9:16 | 1:1 (Veo 는 1:1 미지원)")

    # group / 안전장치
    parser.add_argument("--group-id", dest="bulk_group_id",
                        help="bulk_group_id 명시 (영숫자/_/-/. 1~64자). 미지정시 자동 생성")
    parser.add_argument("--dry-run", action="store_true",
                        help="제출 없이 계획만 출력 (default)")
    parser.add_argument("--confirm", action="store_true",
                        help="실제 제출. --dry-run 미지정이어도 --confirm 없으면 dry-run.")
    parser.add_argument("--sleep", type=float, default=0.5,
                        help="batch 간 대기 (초, default 0.5)")
    parser.add_argument("--continue-on-error", action="store_true",
                        help="batch 1개 실패시 중단 대신 계속 진행")


# ----------------------------------------------------------------------
# manifest 로드 / 편의 입력 → canonical manifest 변환

def _load_manifest_file(path: Path) -> dict:
    if not path.exists():
        _die(f"manifest 파일 없음: {path}")
    try:
        m = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        _die(f"manifest JSON 파싱 오류: {exc}")
    if not isinstance(m, dict) or "pairs" not in m or "engine" not in m:
        _die("manifest 형식 오류: {engine, options?, pairs[]} 필요")
    return m


def _load_prompts_file(path: Path) -> list[str]:
    if not path.exists():
        _die(f"prompts 파일 없음: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        _die(f"prompts JSON 파싱 오류: {exc}")
    if isinstance(data, list):
        prompts = data
    elif isinstance(data, dict) and "prompts" in data:
        prompts = data["prompts"]
    else:
        _die("prompts 파일 형식: list[str] 또는 {'prompts': [...]}")
    prompts = [str(p).strip() for p in prompts if str(p).strip()]
    if not prompts:
        _die("prompts 파일에 유효한 프롬프트 없음")
    return prompts


def _collect_images(images_dir: Path) -> list[Path]:
    if not images_dir.is_dir():
        _die(f"images-from-dir 디렉토리 아님: {images_dir}")
    imgs = sorted(
        p for p in images_dir.iterdir()
        if p.is_file() and p.suffix.lower() in _ALLOWED_EXT
    )
    if not imgs:
        _die(f"이미지 없음: {images_dir} (.png/.jpg/.jpeg/.webp)")
    return imgs


def _build_manifest_from_convenience(args) -> dict:
    if not args.engine:
        _die("--engine 필수 (편의 입력 모드)")
    if args.engine not in _VALID_ENGINES:
        _die(f"알 수 없는 engine: {args.engine!r}")
    if not args.prompts_file:
        _die("--prompts-file 필수")
    prompts = _load_prompts_file(Path(args.prompts_file).expanduser())

    options = _engine_options_from_args(args)

    pairs: list[dict] = []
    if args.text_only:
        if args.engine != "veo":
            _die(f"--text-only 는 veo 전용 (engine={args.engine!r})")
        if args.images_dir:
            _die("--text-only 와 --images-from-dir 동시 사용 불가")
        # txt2video: image 없이 prompts 개수만큼
        options.setdefault("mode", "txt2video")
        for p in prompts:
            pairs.append({"image": None, "prompt": p})
    else:
        if not args.images_dir:
            _die("--images-from-dir 또는 --text-only 필요")
        imgs = _collect_images(Path(args.images_dir).expanduser())
        if args.pair_mode == "paired":
            if len(imgs) != len(prompts):
                _die(
                    f"paired 모드는 N==M 필요 (images={len(imgs)}, prompts={len(prompts)}). "
                    "--pair-mode cartesian 로 cartesian product 가능"
                )
            for img, p in zip(imgs, prompts):
                pairs.append({"image": str(img), "prompt": p})
        else:  # cartesian
            for img in imgs:
                for p in prompts:
                    pairs.append({"image": str(img), "prompt": p})

    return {
        "engine": args.engine,
        "options": options,
        "pairs": pairs,
    }


def _engine_options_from_args(args) -> dict:
    return {
        k: v for k, v in {
            "model_name": args.model_name,
            "mode": args.mode,
            "duration": args.duration,
            "aspect_ratio": args.aspect_ratio,
        }.items()
        if v is not None
    }


def _resolve_manifest(args) -> dict:
    """--file XOR 편의 입력. CLI override 는 manifest options 위에 병합."""
    # 두 입력 동시 → 의도 모호. 명시 거부 (silent 우선은 footgun).
    if args.manifest_file and args.prompts_file:
        _die("--file 과 --prompts-file 동시 사용 불가 — 하나만 지정")
    # text-only 일관성: 명시 --mode 가 txt2video 아닌 값이면 거부
    if args.text_only and args.mode and args.mode.strip().lower() != "txt2video":
        _die(f"--text-only 와 --mode={args.mode!r} 충돌 (txt2video 만 허용)")
    if args.manifest_file:
        m = _load_manifest_file(Path(args.manifest_file).expanduser())
        # CLI override 가 manifest 값을 덮는다 (운영자 의도 우선)
        if args.engine:
            m["engine"] = args.engine
        m.setdefault("options", {})
        m["options"].update(_engine_options_from_args(args))
        # text-only 플래그도 mode 로 반영 (setdefault 라 명시 --mode 가 우선)
        if args.text_only:
            m["options"].setdefault("mode", "txt2video")
        return m
    return _build_manifest_from_convenience(args)


# ----------------------------------------------------------------------
# pairs → submission plan
#   같은 prompt 끼리 묶고, 이미지 개수가 MAX_FILES_PER_BATCH 초과면 chunk.
#   text-only (image=None) 는 prompt 당 1 batch (n_jobs=1).

def _plan_batches(manifest: dict, max_files_per_batch: int) -> list[dict]:
    by_prompt: dict[str, list] = {}
    for pair in manifest["pairs"]:
        prompt = (pair.get("prompt") or "").strip()
        if not prompt:
            _die(f"pair 에 prompt 비어있음: {pair!r}")
        img = pair.get("image")
        by_prompt.setdefault(prompt, []).append(img)

    plan: list[dict] = []
    for prompt, images in by_prompt.items():
        if all(i is None for i in images):
            plan.append({"prompt": prompt, "images": []})  # text-only
            continue
        # None 섞여있으면 오류 — 한 prompt 안에서 mode 가 일관해야 함
        if any(i is None for i in images):
            _die(f"prompt={prompt!r} 안에 image=None 과 image=path 혼재 — 분리 필요")
        # chunk
        files = [Path(i).expanduser() for i in images]
        for chunk in _chunked(files, max_files_per_batch):
            plan.append({"prompt": prompt, "images": list(chunk)})
    return plan


def _chunked(seq: list, n: int) -> Iterable[list]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ----------------------------------------------------------------------
# 사전 체크 — limits + image 존재성

def _precheck(plan: list[dict], limits_resp: dict) -> None:
    n_batches = len(plan)
    n_jobs = sum(max(1, len(b["images"])) for b in plan)
    daily_batches_remaining = (
        limits_resp.get("usage", {}).get("daily_batches", {}).get("remaining")
    )
    daily_bytes_remaining = (
        limits_resp.get("usage", {}).get("daily_bytes", {}).get("remaining")
    )
    max_files = limits_resp.get("per_batch", {}).get("max_files", 20)
    max_bytes = limits_resp.get("per_batch", {}).get("max_bytes_per_file", 50 * 1024 * 1024)

    # batch 한도
    if daily_batches_remaining is not None and n_batches > daily_batches_remaining:
        _die(
            f"일별 배치 한도 초과: 제출 예정 {n_batches} > 잔여 {daily_batches_remaining}",
            code=1,
        )

    # 이미지 파일 존재 + 크기
    total_bytes = 0
    for b in plan:
        for img in b["images"]:
            if not img.exists():
                _die(f"이미지 파일 없음: {img}")
            sz = img.stat().st_size
            if sz > max_bytes:
                _die(f"이미지 너무 큼 ({sz} > {max_bytes}): {img}")
            total_bytes += sz
            if img.suffix.lower() not in _ALLOWED_EXT:
                _die(f"지원 안 하는 확장자 ({img.suffix}): {img}")
        if len(b["images"]) > max_files:
            _die(f"batch 당 파일 수 초과 ({len(b['images'])} > {max_files})")

    # 바이트 한도
    if daily_bytes_remaining is not None and total_bytes > daily_bytes_remaining:
        _die(
            f"일별 입력 bytes 한도 초과: 제출 {total_bytes:,} > 잔여 {daily_bytes_remaining:,}",
            code=1,
        )


def _format_preview(manifest: dict, plan: list[dict], bgi: str, limits_resp: dict) -> str:
    n_batches = len(plan)
    n_jobs = sum(max(1, len(b["images"])) for b in plan)
    total_bytes = sum(img.stat().st_size for b in plan for img in b["images"])

    lines = [
        f"engine        : {manifest['engine']}",
        f"options       : {json.dumps(manifest.get('options', {}), ensure_ascii=False)}",
        f"bulk_group_id : {bgi}",
        f"batches       : {n_batches}",
        f"jobs (total)  : {n_jobs}",
        f"input bytes   : {total_bytes:,}",
        "",
        "limits / headroom:",
    ]
    rl = limits_resp.get("usage", {})
    for k in ("daily_batches", "daily_bytes"):
        info = rl.get(k, {})
        lim = info.get("limit") or 0
        used = info.get("used_24h") or 0
        rem = info.get("remaining")
        rem_s = "∞" if rem is None else str(rem)
        lines.append(f"  {k:<14} limit={lim:>10}  used_24h={used:>10}  remaining={rem_s}")

    lines.append("")
    lines.append("first 3 batches:")
    for i, b in enumerate(plan[:3], 1):
        p = b["prompt"]
        p_short = (p[:60] + "…") if len(p) > 60 else p
        lines.append(f"  [{i:>3}] prompt='{p_short}' images={len(b['images'])}")
    if n_batches > 3:
        lines.append(f"  ... ({n_batches - 3} more)")
    return "\n".join(lines)


# ----------------------------------------------------------------------
# run

def run(args, client: HTTPClient) -> int:
    # 1) manifest 확보
    manifest = _resolve_manifest(args)
    engine = manifest["engine"]
    options = dict(manifest.get("options") or {})

    # 2) limits 사전 조회 — PG 장애시 5xx → APIError → 호출자에서 1 반환
    try:
        limits_resp = client.get_limits()
    except APIError as exc:
        _die(f"GET /genai/limits 실패: {exc} (서버 PG 장애 가능성)")

    # 3) plan
    max_files = limits_resp.get("per_batch", {}).get("max_files", 20)
    plan = _plan_batches(manifest, max_files_per_batch=max_files)
    if not plan:
        _die("plan 이 비었음 (pairs 가 비었거나 prompt 가 모두 비어있음)")

    # 4) precheck
    _precheck(plan, limits_resp)

    # 5) bulk_group_id 결정
    bgi = args.bulk_group_id or f"bgi-{uuid.uuid4().hex[:12]}"

    # 6) dry-run preview (default behavior — --confirm 없으면 dry-run)
    preview = _format_preview(manifest, plan, bgi, limits_resp)
    if args.dry_run or not args.confirm:
        print(preview)
        print()
        if not args.confirm:
            print("dry-run only. 실제 제출하려면 --confirm 추가.")
        return 0

    # 7) 실제 제출
    print(preview)
    print()
    print(f"submitting {len(plan)} batches with bulk_group_id={bgi} ...")
    print(f"(sleep {args.sleep}s between batches)")
    print()

    submitted: list[dict] = []
    failed: list[dict] = []
    fmt = auto_format(getattr(args, "format", None))

    for i, b in enumerate(plan, 1):
        # APIError 뿐 아니라 ConnectionError / 파일 IO 등 모든 transient 오류를
        # batch 단위 실패로 흡수해 submitted[] 정보 손실 방지. KeyboardInterrupt /
        # SystemExit 만 그대로 propagate (사용자 의도 종료).
        try:
            sub_opts = dict(options)  # per-batch copy
            res = client.submit_batch(
                engine=engine, prompt=b["prompt"],
                files=b["images"],
                options=sub_opts,
                bulk_group_id=bgi,
            )
            batch_id = res.get("batch_id") if isinstance(res, dict) else None
            submitted.append({"i": i, "batch_id": batch_id, "n_images": len(b["images"])})
            print(f"  [{i:>3}/{len(plan)}] batch_id={batch_id} ✓  (images={len(b['images'])})")
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            failed.append({"i": i, "error": str(exc), "type": type(exc).__name__})
            print(f"  [{i:>3}/{len(plan)}] FAILED ({type(exc).__name__}) — {exc}",
                  file=sys.stderr)
            if not args.continue_on_error:
                print(f"\n중단 (--continue-on-error 로 계속 진행 가능). "
                      f"submitted={len(submitted)} failed={len(failed)} remaining={len(plan) - i}",
                      file=sys.stderr)
                break
        if i < len(plan) and args.sleep > 0:
            time.sleep(args.sleep)

    # 8) 결과
    result = {
        "bulk_group_id": bgi,
        "engine": engine,
        "total_planned": len(plan),
        "submitted": submitted,
        "failed": failed,
    }
    emit(result, fmt=fmt)
    if failed:
        return 2
    return 0


def _die(msg: str, code: int = 1) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(code)
