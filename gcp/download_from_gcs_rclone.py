#!/usr/bin/env python3
"""
Unified GCS downloader.

Modes:
- date-folders: download date-named top-level folders (YYYYMMDD / YYYY-MM-DD)
- legacy-range: legacy folder/date-pattern based download flow

Backend:
- auto   : prefer rclone, fallback to gcloud
- rclone : force rclone
- gcloud : force gcloud
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Sequence

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - runtime fallback
    tqdm = None


DEFAULT_BUCKET_NAME = "khon-kaen-rtsp-bucket"
DEFAULT_DOWNLOAD_DIR = "/media/pia/DOCK_DISK02"
DEFAULT_RCLONE_REMOTE = "gcs"
DEFAULT_BACKEND = "auto"
DEFAULT_MODE = "legacy-range"

DATE_FOLDER_FORMATS = ("%Y%m%d", "%Y-%m-%d")

DEFAULT_FOLDER_START = 1
DEFAULT_FOLDER_END = 63
DEFAULT_START_DATE = "2025-09-19"
DEFAULT_END_DATE = "2025-09-26"
DEFAULT_LEGACY_FOLDERS = [
    2,
    3,
    4,
    5,
    6,
    7,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    37,
    38,
    39,
    40,
    41,
    42,
    43,
    44,
    45,
    46,
]
DEFAULT_LEGACY_PATTERNS = ["*_14-*.mp4", "*_12-*.mp4"]

DEFAULT_STALL_SECONDS = 300
DEFAULT_MAX_RESTARTS = 3
DONE_MARKER_FILENAME = "_DONE"
PARTIAL_DIR_PREFIX = ".partial__"

NO_MATCH_MESSAGES = (
    "no urls matched",
    "did not match any objects",
    "matched no objects",
    "no objects matched",
)


@dataclass
class DownloadSummary:
    attempted: int = 0
    succeeded: int = 0
    skipped: int = 0
    failed: int = 0


class _NoOpProgress:
    def update(self, _n: int = 1) -> None:
        pass

    def set_postfix_str(self, _s: str, refresh: bool = False) -> None:
        pass

    def close(self) -> None:
        pass


_TQDM_WARNED = False


def _env_list(name: str, cast=str) -> List:
    raw = os.environ.get(name)
    if not raw:
        return []
    parts = raw.split()
    return [cast(p) for p in parts]


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got '{raw}'") from exc


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_str(name: str, default: str) -> str:
    val = os.environ.get(name)
    return val if val is not None and val != "" else default


def _resolve_remote(remote: str, bucket: str) -> str:
    remote = remote.strip()
    bucket = bucket.strip()

    if ":" in remote:
        if remote.endswith(":") and bucket:
            return f"{remote}{bucket}"
        return remote.rstrip("/")

    if not bucket:
        raise ValueError(
            "BUCKET_NAME is empty but remote has no ':' path. "
            "Set BUCKET_NAME or use a full remote path."
        )

    return f"{remote}:{bucket}"


def _parse_date(raw: str) -> date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date '{raw}': expected YYYY-MM-DD") from exc


def _build_folder_range(start: int, end: int) -> List[int]:
    if start > end:
        raise ValueError(f"folder range is invalid: start ({start}) must be <= end ({end})")
    return list(range(start, end + 1))


def _build_date_range(start: date, end: date) -> List[str]:
    if start > end:
        raise ValueError(f"date range is invalid: start ({start}) must be <= end ({end})")
    items: List[str] = []
    cur = start
    while cur <= end:
        items.append(cur.isoformat())
        cur += timedelta(days=1)
    return items


def _build_date_patterns(start: date, end: date) -> List[str]:
    return [f"{d}_*.mp4" for d in _build_date_range(start, end)]


def _normalize_bucket(bucket: str) -> str:
    out = bucket.strip()
    if out.startswith("gs://"):
        out = out[len("gs://") :]
    out = out.strip("/")
    if not out:
        raise ValueError("bucket is empty")
    return out


def _is_tool_available(name: str) -> bool:
    return shutil.which(name) is not None


def _choose_backend(preferred: str) -> str:
    if preferred == "rclone":
        if not _is_tool_available("rclone"):
            raise ValueError("backend 'rclone' requested, but rclone is not installed.")
        return "rclone"

    if preferred == "gcloud":
        if not _is_tool_available("gcloud"):
            raise ValueError("backend 'gcloud' requested, but gcloud is not installed.")
        return "gcloud"

    if preferred != "auto":
        raise ValueError(f"unsupported backend: {preferred}")

    if _is_tool_available("rclone"):
        return "rclone"
    if _is_tool_available("gcloud"):
        return "gcloud"
    raise ValueError("no supported backend found. Install rclone or gcloud.")


def _format_cmd(cmd: Sequence[str]) -> str:
    return " ".join(shlex.quote(c) for c in cmd)


def _make_progress(total: int, desc: str, unit: str = "task"):
    global _TQDM_WARNED
    if tqdm is None:
        if not _TQDM_WARNED:
            print(
                "[WARN] tqdm is not installed. Install with `pip install tqdm` for progress bar.",
                file=sys.stderr,
            )
            _TQDM_WARNED = True
        return _NoOpProgress()
    return tqdm(total=total, desc=desc, unit=unit, dynamic_ncols=True, leave=True)


def _drain_stream(
    stream,
    sink: List[str],
    *,
    is_stderr: bool,
    emit_output: bool,
    last_activity: List[float],
    lock: threading.Lock,
) -> None:
    for line in iter(stream.readline, ""):
        sink.append(line)
        with lock:
            last_activity[0] = time.monotonic()
        if emit_output:
            if is_stderr:
                print(line, end="", file=sys.stderr, flush=True)
            else:
                print(line, end="", flush=True)
    stream.close()


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _run_cmd_once(
    cmd: Sequence[str],
    *,
    dry_run: bool,
    capture_output: bool,
    stall_seconds: int,
    echo_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    print("[CMD]", _format_cmd(cmd))
    if dry_run:
        return subprocess.CompletedProcess(args=list(cmd), returncode=0, stdout="", stderr="")

    if not capture_output:
        result = subprocess.run(list(cmd), text=True)
        return subprocess.CompletedProcess(
            args=list(cmd),
            returncode=result.returncode,
            stdout="",
            stderr="",
        )

    process = subprocess.Popen(
        list(cmd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
    )
    assert process.stdout is not None
    assert process.stderr is not None

    stdout_lines: List[str] = []
    stderr_lines: List[str] = []
    lock = threading.Lock()
    last_activity = [time.monotonic()]

    stdout_thread = threading.Thread(
        target=_drain_stream,
        args=(process.stdout, stdout_lines),
        kwargs={
            "is_stderr": False,
            "emit_output": echo_output,
            "last_activity": last_activity,
            "lock": lock,
        },
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_drain_stream,
        args=(process.stderr, stderr_lines),
        kwargs={
            "is_stderr": True,
            "emit_output": echo_output,
            "last_activity": last_activity,
            "lock": lock,
        },
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    stalled = False
    while process.poll() is None:
        if stall_seconds > 0:
            with lock:
                idle_for = time.monotonic() - last_activity[0]
            if idle_for >= stall_seconds:
                stalled = True
                warn_msg = f"[WARN] No output for {stall_seconds}s. Terminating stalled command..."
                print(warn_msg, file=sys.stderr, flush=True)
                stderr_lines.append(warn_msg + "\n")
                _terminate_process(process)
                break
        time.sleep(1)

    returncode = process.wait()
    stdout_thread.join()
    stderr_thread.join()

    if stalled:
        returncode = 124
        stderr_lines.append(f"[STALL] command terminated after {stall_seconds}s without output\n")

    return subprocess.CompletedProcess(
        args=list(cmd),
        returncode=returncode,
        stdout="".join(stdout_lines),
        stderr="".join(stderr_lines),
    )


def _run_cmd_with_restarts(
    cmd: Sequence[str],
    *,
    dry_run: bool,
    capture_output: bool,
    stall_seconds: int,
    max_restarts: int,
    label: str,
    echo_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    attempts = max_restarts + 1
    last_result: subprocess.CompletedProcess[str] | None = None

    for attempt in range(1, attempts + 1):
        if attempt > 1:
            print(f"[WARN] Restarting stalled task ({attempt}/{attempts}): {label}")

        result = _run_cmd_once(
            cmd,
            dry_run=dry_run,
            capture_output=capture_output,
            stall_seconds=stall_seconds,
            echo_output=echo_output,
        )
        last_result = result

        if result.returncode != 124:
            return result

    assert last_result is not None
    return last_result


def _rclone_global_args(config_path: str, extra_args: str) -> List[str]:
    global_args: List[str] = []
    if config_path:
        global_args += ["--config", config_path]
    if extra_args:
        global_args += shlex.split(extra_args)
    return global_args


def _build_rclone_copy_cmd(src: str, dst: str, patterns: Iterable[str], skip_existing: bool) -> List[str]:
    cmd = ["copy", src, dst, "--progress"]
    if skip_existing:
        cmd.append("--ignore-existing")
    for pattern in patterns:
        cmd += ["--include", pattern]
    return cmd


def _build_rclone_folder_copy_cmd(src: str, dst: str, skip_existing: bool) -> List[str]:
    cmd = ["copy", src, dst, "--progress"]
    if skip_existing:
        cmd.append("--ignore-existing")
    return cmd


def _ensure_dir(path: str, *, dry_run: bool = False) -> None:
    if dry_run:
        return
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        raise ValueError(f"permission denied for destination directory: {path}") from exc


def _resolve_date_folder_paths(download_dir: str, folder: str) -> tuple[str, str]:
    final_dir = os.path.join(download_dir, folder)
    partial_dir = os.path.join(download_dir, f"{PARTIAL_DIR_PREFIX}{folder}")
    return final_dir, partial_dir


def _finalize_partial_folder(partial_dir: str, final_dir: str, *, dry_run: bool) -> None:
    if dry_run:
        print(f"[DRY-RUN] finalize folder: {partial_dir} -> {final_dir}")
        return

    partial_path = Path(partial_dir)
    final_path = Path(final_dir)
    if not partial_path.exists():
        return

    if final_path.exists():
        # 예외 상황(동시 실행 등)에서는 partial을 유지해 데이터 유실 위험을 피한다.
        print(
            f"[WARN] final dir already exists; keeping partial for safety: {partial_dir}",
            file=sys.stderr,
        )
        return

    os.replace(partial_dir, final_dir)


def _write_done_marker(folder_dir: str, *, dry_run: bool) -> None:
    marker_path = Path(folder_dir) / DONE_MARKER_FILENAME
    payload = f"completed_at={datetime.utcnow().isoformat()}Z\n"
    if dry_run:
        print(f"[DRY-RUN] write marker: {marker_path}")
        return
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(payload, encoding="utf-8")


def _archive_done_marker_exists(archive_dir: str, bucket: str, folder: str) -> bool:
    """archive(YYYY/MM) 하위에 동일 bucket/folder가 있으면 True.

    우선 _DONE 마커를 확인하고, 과거 데이터(마커 미도입 시점)를 위해
    폴더 존재 + 파일 1개 이상도 archived로 간주한다.
    """
    archive_root = Path(archive_dir)
    if not archive_root.exists():
        return False

    # 빠른 경로: 현재 월 확인
    current_month = datetime.now().strftime("%Y/%m")
    quick_marker = archive_root / current_month / bucket / folder / DONE_MARKER_FILENAME
    try:
        if quick_marker.exists():
            return True
    except OSError:
        return False

    # fallback: archive_root/<YYYY>/<MM>/<bucket>/<folder> 탐색
    try:
        for year_dir in archive_root.iterdir():
            if not year_dir.is_dir() or not year_dir.name.isdigit() or len(year_dir.name) != 4:
                continue
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir() or not month_dir.name.isdigit():
                    continue
                folder_path = month_dir / bucket / folder
                marker = folder_path / DONE_MARKER_FILENAME
                if marker.exists():
                    return True
                if folder_path.is_dir():
                    try:
                        if any(p.is_file() for p in folder_path.rglob("*")):
                            return True
                    except OSError:
                        continue
    except OSError:
        return False

    return False


def _build_gcloud_copy_cmd(src_pattern: str, dst: str, skip_existing: bool) -> List[str]:
    cmd = ["gcloud", "storage", "cp"]
    if skip_existing:
        cmd.append("--no-clobber")
    cmd += [src_pattern, dst]
    return cmd


def _build_gcloud_rsync_cmd(bucket: str, folder: str, dst: str, skip_existing: bool) -> List[str]:
    cmd = ["gcloud", "storage", "rsync", f"gs://{bucket}/{folder}", dst, "--recursive"]
    if skip_existing:
        cmd.append("--no-clobber")
    return cmd


def _extract_gs_urls(raw_output: str) -> List[str]:
    urls: List[str] = []
    for raw_line in raw_output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("gs://"):
            urls.append(line)
            continue
        parts = line.split()
        if parts and parts[-1].startswith("gs://"):
            urls.append(parts[-1])
    return urls


def _is_no_match_output(stdout: str, stderr: str) -> bool:
    merged = f"{stdout}\n{stderr}".lower()
    return any(token in merged for token in NO_MATCH_MESSAGES)


def _is_existing_skip_output(stdout: str, stderr: str) -> bool:
    merged = f"{stdout}\n{stderr}".lower()
    return "skipping existing destination item (no-clobber)" in merged


def _looks_like_skip_output(stdout: str, stderr: str) -> bool:
    merged = f"{stdout}\n{stderr}".lower()
    return "skipping existing destination item" in merged or "no changes made" in merged


def _error_detail(stdout: str, stderr: str, *, limit: int = 600) -> str:
    merged = (stderr or "").strip() or (stdout or "").strip()
    if not merged:
        return "no stderr/stdout"
    compact = " | ".join(line.strip() for line in merged.splitlines() if line.strip())
    if len(compact) > limit:
        return compact[-limit:]
    return compact


def _download_with_rclone(
    *,
    remote_root: str,
    folders: Iterable[int],
    patterns: Iterable[str],
    download_dir: str,
    config_path: str,
    extra_args: str,
    skip_existing: bool,
    dry_run: bool,
    stall_seconds: int,
    max_restarts: int,
) -> DownloadSummary:
    summary = DownloadSummary()
    folders_list = list(folders)
    progress = _make_progress(total=len(folders_list), desc="rclone")

    _ensure_dir(download_dir, dry_run=dry_run)

    try:
        for index, folder in enumerate(folders_list, start=1):
            summary.attempted += 1
            src = f"{remote_root}/{folder}"
            dst = os.path.join(download_dir, str(folder))
            _ensure_dir(dst, dry_run=dry_run)
            progress.set_postfix_str(f"{index}/{len(folders_list)} folder={folder}", refresh=False)

            cmd = ["rclone"] + _rclone_global_args(config_path=config_path, extra_args=extra_args)
            cmd += _build_rclone_copy_cmd(src=src, dst=dst, patterns=patterns, skip_existing=skip_existing)

            print(f"[INFO] rclone [{index}/{len(folders_list)}] folder={folder}")
            result = _run_cmd_with_restarts(
                cmd,
                dry_run=dry_run,
                capture_output=True,
                stall_seconds=stall_seconds,
                max_restarts=max_restarts,
                label=f"rclone folder={folder}",
            )

            if result.returncode == 0:
                summary.succeeded += 1
            elif result.returncode == 124:
                summary.failed += 1
                print(
                    f"[ERROR] rclone stalled and exceeded restart limit for folder={folder}",
                    file=sys.stderr,
                )
            else:
                summary.failed += 1
                print(f"[ERROR] rclone failed for folder={folder} (exit={result.returncode})", file=sys.stderr)
            progress.update(1)
    finally:
        progress.close()

    return summary


def _download_with_gcloud(
    *,
    bucket: str,
    folders: Iterable[int],
    patterns: Iterable[str],
    download_dir: str,
    skip_existing: bool,
    dry_run: bool,
    stall_seconds: int,
    max_restarts: int,
) -> DownloadSummary:
    if not _is_tool_available("gcloud"):
        raise ValueError("gcloud is not installed or not on PATH.")

    summary = DownloadSummary()
    folders_list = list(folders)
    patterns_list = list(patterns)
    total_patterns = len(folders_list) * len(patterns_list)

    _ensure_dir(download_dir, dry_run=dry_run)

    discovery_progress = _make_progress(total=total_patterns, desc="discover", unit="pattern")
    file_progress = _make_progress(total=0, desc="download", unit="file")

    discovered_patterns = 0
    discovered_files = 0
    processed_files = 0
    seen_urls: set[str] = set()

    try:
        for folder in folders_list:
            dst = os.path.join(download_dir, str(folder))
            _ensure_dir(dst, dry_run=dry_run)

            for pattern in patterns_list:
                discovered_patterns += 1
                discovery_progress.set_postfix_str(
                    f"{discovered_patterns}/{total_patterns} folder={folder} pattern={pattern}",
                    refresh=False,
                )

                src_pattern = f"gs://{bucket}/{folder}/{pattern}"
                list_cmd = ["gcloud", "storage", "ls", src_pattern]
                print(f"[INFO] discover [{discovered_patterns}/{total_patterns}] folder={folder} pattern={pattern}")

                list_result = _run_cmd_with_restarts(
                    list_cmd,
                    dry_run=False,
                    capture_output=True,
                    stall_seconds=stall_seconds,
                    max_restarts=max_restarts,
                    label=f"gcloud ls folder={folder} pattern={pattern}",
                    echo_output=False,
                )

                if list_result.returncode == 0:
                    raw_matches = _extract_gs_urls(list_result.stdout)
                    matches = [url for url in raw_matches if url not in seen_urls]
                    for url in matches:
                        seen_urls.add(url)

                    if not matches:
                        print(f"[WARN] No matching objects: {src_pattern}")
                        discovery_progress.update(1)
                        continue

                    discovered_files += len(matches)
                    if tqdm is not None:
                        file_progress.total = discovered_files
                        file_progress.refresh()

                    print(f"[INFO] matched={len(matches)} folder={folder} pattern={pattern}")

                    for url in matches:
                        summary.attempted += 1
                        filename = os.path.basename(url)
                        next_idx = processed_files + 1
                        file_progress.set_postfix_str(
                            f"{next_idx}/{discovered_files} folder={folder} file={filename}",
                            refresh=False,
                        )
                        print(f"[FILE {next_idx}/{discovered_files}] folder={folder} name={filename}")

                        cmd = _build_gcloud_copy_cmd(src_pattern=url, dst=dst, skip_existing=skip_existing)
                        result = _run_cmd_with_restarts(
                            cmd,
                            dry_run=dry_run,
                            capture_output=True,
                            stall_seconds=stall_seconds,
                            max_restarts=max_restarts,
                            label=f"gcloud file={url}",
                            echo_output=False,
                        )

                        if result.returncode == 0:
                            if dry_run:
                                summary.skipped += 1
                            elif skip_existing and _is_existing_skip_output(result.stdout, result.stderr):
                                summary.skipped += 1
                            else:
                                summary.succeeded += 1
                            processed_files += 1
                            file_progress.update(1)
                            continue

                        if _is_no_match_output(result.stdout, result.stderr):
                            summary.skipped += 1
                            print(f"[WARN] No matching object at copy time: {url}")
                            processed_files += 1
                            file_progress.update(1)
                            continue

                        if result.returncode == 124:
                            summary.failed += 1
                            print(
                                f"[ERROR] gcloud stalled and exceeded restart limit for file={url}",
                                file=sys.stderr,
                            )
                            processed_files += 1
                            file_progress.update(1)
                            continue

                        summary.failed += 1
                        print(
                            f"[ERROR] gcloud failed for file={url} (exit={result.returncode})",
                            file=sys.stderr,
                        )
                        processed_files += 1
                        file_progress.update(1)

                    discovery_progress.update(1)
                    continue

                if _is_no_match_output(list_result.stdout, list_result.stderr):
                    print(f"[WARN] No matching objects: {src_pattern}")
                    discovery_progress.update(1)
                    continue

                if list_result.returncode == 124:
                    print(
                        "[ERROR] object discovery stalled and exceeded restart limit "
                        f"for folder={folder}, pattern={pattern}",
                        file=sys.stderr,
                    )
                else:
                    print(
                        "[ERROR] object discovery failed "
                        f"for folder={folder}, pattern={pattern} (exit={list_result.returncode})",
                        file=sys.stderr,
                    )
                summary.failed += 1
                discovery_progress.update(1)
    finally:
        discovery_progress.close()
        file_progress.close()

    print(f"[INFO] Total matched files: {discovered_files}")
    return summary


def _list_with_rclone(
    remote_root: str,
    config_path: str,
    extra_args: str,
    dry_run: bool,
    stall_seconds: int,
    max_restarts: int,
) -> None:
    print("\n[INFO] Listing bucket contents via rclone")
    cmd = ["rclone"] + _rclone_global_args(config_path=config_path, extra_args=extra_args) + ["lsf", remote_root]
    result = _run_cmd_with_restarts(
        cmd,
        dry_run=dry_run,
        capture_output=False,
        stall_seconds=stall_seconds,
        max_restarts=max_restarts,
        label="rclone list",
    )
    if result.returncode != 0:
        sys.exit(result.returncode)


def _list_with_gcloud(bucket: str, dry_run: bool, stall_seconds: int, max_restarts: int) -> None:
    if not _is_tool_available("gcloud"):
        raise ValueError("gcloud is not installed or not on PATH.")
    print("\n[INFO] Listing bucket contents via gcloud")
    cmd = ["gcloud", "storage", "ls", f"gs://{bucket}/"]
    result = _run_cmd_with_restarts(
        cmd,
        dry_run=dry_run,
        capture_output=False,
        stall_seconds=stall_seconds,
        max_restarts=max_restarts,
        label="gcloud list",
    )
    if result.returncode != 0:
        sys.exit(result.returncode)


def _resolve_legacy_filters(args: argparse.Namespace) -> tuple[List[int], List[str], str]:
    if args._range_explicit:
        folders = _build_folder_range(args.folder_start, args.folder_end)
        patterns = _build_date_patterns(args.start_date, args.end_date)
        return folders, patterns, "date-range (CLI override)"

    if args._legacy_explicit or args._legacy_from_env:
        return list(args.folders), list(args.patterns), "legacy folders/patterns"

    folders = _build_folder_range(args.folder_start, args.folder_end)
    patterns = _build_date_patterns(args.start_date, args.end_date)
    return folders, patterns, "date-range (default)"


def _parse_date_folder_name(name: str) -> date | None:
    for fmt in DATE_FOLDER_FORMATS:
        try:
            return datetime.strptime(name, fmt).date()
        except ValueError:
            continue
    return None


def _is_date_folder_name(name: str) -> bool:
    return _parse_date_folder_name(name) is not None


def _pick_date_folders(names: Iterable[str]) -> List[str]:
    parsed: List[tuple[date, str]] = []
    for name in set(names):
        parsed_date = _parse_date_folder_name(name)
        if parsed_date is not None:
            parsed.append((parsed_date, name))
    parsed.sort(key=lambda item: (item[0], item[1]))
    return [name for _, name in parsed]


def _extract_top_level_dir_from_gs_uri(uri: str, bucket: str) -> str | None:
    prefix = f"gs://{bucket}/"
    raw = uri.strip()
    if not raw.startswith(prefix):
        return None
    rest = raw[len(prefix) :]
    if not rest.endswith("/"):
        return None
    rest = rest[:-1]
    if not rest or "/" in rest:
        return None
    return rest


def _list_top_level_dirs_gcloud(bucket: str, stall_seconds: int, max_restarts: int) -> List[str]:
    if not _is_tool_available("gcloud"):
        raise ValueError("gcloud is not installed or not on PATH.")

    cmd = ["gcloud", "storage", "ls", f"gs://{bucket}/"]
    result = _run_cmd_with_restarts(
        cmd,
        dry_run=False,
        capture_output=True,
        stall_seconds=stall_seconds,
        max_restarts=max_restarts,
        label="gcloud top-level list",
        echo_output=False,
    )
    if result.returncode != 0:
        detail = _error_detail(result.stdout, result.stderr)
        raise ValueError(f"gcloud ls failed with exit code {result.returncode}: {detail}")

    dirs = []
    for line in result.stdout.splitlines():
        folder = _extract_top_level_dir_from_gs_uri(line, bucket)
        if folder:
            dirs.append(folder)
    return sorted(set(dirs))


def _list_top_level_dirs_rclone(
    remote_root: str,
    config_path: str,
    extra_args: str,
    stall_seconds: int,
    max_restarts: int,
) -> List[str]:
    cmd = ["rclone"] + _rclone_global_args(config_path=config_path, extra_args=extra_args)
    cmd += ["lsf", remote_root, "--dirs-only"]

    result = _run_cmd_with_restarts(
        cmd,
        dry_run=False,
        capture_output=True,
        stall_seconds=stall_seconds,
        max_restarts=max_restarts,
        label="rclone top-level list",
        echo_output=False,
    )
    if result.returncode != 0:
        detail = _error_detail(result.stdout, result.stderr)
        raise ValueError(f"rclone lsf failed with exit code {result.returncode}: {detail}")

    dirs = []
    for line in result.stdout.splitlines():
        name = line.strip().rstrip("/")
        if name and "/" not in name:
            dirs.append(name)
    return sorted(set(dirs))


def _download_date_folders(
    *,
    backend: str,
    remote_root: str,
    bucket: str,
    folders: Sequence[str],
    download_dir: str,
    skip_existing: bool,
    dry_run: bool,
    config_path: str,
    extra_args: str,
    stall_seconds: int,
    max_restarts: int,
    archive_dir: str,
    skip_archived_done: bool,
) -> DownloadSummary:
    summary = DownloadSummary()
    folders_list = list(folders)
    _ensure_dir(download_dir, dry_run=dry_run)

    for index, folder in enumerate(folders_list, start=1):
        summary.attempted += 1
        if skip_archived_done and _archive_done_marker_exists(archive_dir, bucket, folder):
            summary.skipped += 1
            print(
                f"[SKIP] archive done marker exists: bucket={bucket}, folder={folder}",
            )
            continue

        dst_final, dst_partial = _resolve_date_folder_paths(download_dir, folder)
        transfer_dst = dst_final if Path(dst_final).exists() else dst_partial
        _ensure_dir(transfer_dst, dry_run=dry_run)

        if backend == "rclone":
            src = f"{remote_root}/{folder}"
            cmd = ["rclone"] + _rclone_global_args(config_path=config_path, extra_args=extra_args)
            cmd += _build_rclone_folder_copy_cmd(src, transfer_dst, skip_existing=skip_existing)
            print(f"[INFO] rclone [{index}/{len(folders_list)}] folder={folder}")
            result = _run_cmd_with_restarts(
                cmd,
                dry_run=dry_run,
                capture_output=True,
                stall_seconds=stall_seconds,
                max_restarts=max_restarts,
                label=f"rclone folder={folder}",
            )
        else:
            cmd = _build_gcloud_rsync_cmd(
                bucket=bucket,
                folder=folder,
                dst=transfer_dst,
                skip_existing=skip_existing,
            )
            print(f"[INFO] gcloud [{index}/{len(folders_list)}] folder={folder}")
            result = _run_cmd_with_restarts(
                cmd,
                dry_run=dry_run,
                capture_output=True,
                stall_seconds=stall_seconds,
                max_restarts=max_restarts,
                label=f"gcloud folder={folder}",
            )

        if result.returncode != 0:
            summary.failed += 1
            print(f"[ERROR] download failed for folder={folder} (exit={result.returncode})", file=sys.stderr)
            continue

        if transfer_dst == dst_partial:
            _finalize_partial_folder(dst_partial, dst_final, dry_run=dry_run)
        _write_done_marker(dst_final, dry_run=dry_run)

        if skip_existing and _looks_like_skip_output(result.stdout, result.stderr):
            summary.skipped += 1
        else:
            summary.succeeded += 1

    return summary


def _run_date_folder_mode(args: argparse.Namespace, *, backend: str, bucket: str, remote_root: str) -> int:
    if backend == "rclone":
        top_level_dirs = _list_top_level_dirs_rclone(
            remote_root=remote_root,
            config_path=args.config,
            extra_args=args.extra_args,
            stall_seconds=args.stall_seconds,
            max_restarts=args.max_restarts,
        )
    else:
        top_level_dirs = _list_top_level_dirs_gcloud(
            bucket=bucket,
            stall_seconds=args.stall_seconds,
            max_restarts=args.max_restarts,
        )

    date_folders = _pick_date_folders(top_level_dirs)
    requested = list(args.date_folders) if args.date_folders else []

    if requested:
        requested_set = set(requested)
        missing = sorted(requested_set - set(top_level_dirs))
        non_date = sorted(name for name in requested_set if not _is_date_folder_name(name))
        if missing:
            print(f"[WARN] requested folders not found in bucket root: {', '.join(missing)}")
        if non_date:
            print(f"[WARN] non-date folder names were ignored: {', '.join(non_date)}")
        date_folders = [name for name in date_folders if name in requested_set]

    print("")
    print("[INFO] Date-folder download configuration")
    print(f"Mode: {args.mode}")
    print(f"Backend: {backend} (requested={args.backend})")
    print(f"Bucket: {bucket}")
    print(f"Remote root (rclone): {remote_root}")
    print(f"Download dir: {args.download_dir}")
    print(f"Skip existing: {args.skip_existing}")
    print(f"Stall seconds: {args.stall_seconds}")
    print(f"Max restarts: {args.max_restarts}")
    print(f"Dry run: {args.dry_run}")
    print(f"Archive dir: {args.archive_dir}")
    print(f"Skip archived done: {args.skip_archived_done}")
    print(f"Matched date folders: {len(date_folders)}")
    if date_folders:
        print(f"First: {date_folders[0]}")
        print(f"Last : {date_folders[-1]}")
    print("")

    if not date_folders:
        print("[WARN] No date-format folders found.")
        return 0

    print("[INFO] Target folders:")
    for folder in date_folders:
        print(f"- {folder}")
    print("")

    if args.list_only or args.list:
        print("[OK] List-only mode completed.")
        return 0

    summary = _download_date_folders(
        backend=backend,
        remote_root=remote_root,
        bucket=bucket,
        folders=date_folders,
        download_dir=args.download_dir,
        skip_existing=args.skip_existing,
        dry_run=args.dry_run,
        config_path=args.config,
        extra_args=args.extra_args,
        stall_seconds=args.stall_seconds,
        max_restarts=args.max_restarts,
        archive_dir=args.archive_dir,
        skip_archived_done=args.skip_archived_done,
    )

    print("")
    print("[SUMMARY]")
    print(f"Attempted folders: {summary.attempted}")
    print(f"Succeeded folders: {summary.succeeded}")
    print(f"Skipped folders: {summary.skipped}")
    print(f"Failed folders: {summary.failed}")
    if summary.failed > 0:
        return 1
    print("[OK] Download completed")
    return 0


def _run_legacy_mode(args: argparse.Namespace, *, backend: str, bucket: str, remote_root: str) -> int:
    folders, patterns, filter_mode = _resolve_legacy_filters(args)

    print("")
    print("[INFO] Legacy-range download configuration")
    print(f"Mode: {args.mode}")
    print(f"Backend: {backend} (requested={args.backend})")
    print(f"Remote root (rclone): {remote_root}")
    print(f"Bucket (gcloud): {bucket}")
    print(f"Filter mode: {filter_mode}")
    print(f"Folders ({len(folders)}): {folders[0]}..{folders[-1]}" if folders else "Folders: none")
    if patterns:
        print(f"Patterns ({len(patterns)}): first={patterns[0]} last={patterns[-1]}")
    print(f"Download dir: {args.download_dir}")
    print(f"Skip existing: {args.skip_existing}")
    print(f"Stall seconds: {args.stall_seconds}")
    print(f"Max restarts: {args.max_restarts}")
    print(f"Dry run: {args.dry_run}")
    print("")

    if args.list:
        if backend == "rclone":
            _list_with_rclone(
                remote_root=remote_root,
                config_path=args.config,
                extra_args=args.extra_args,
                dry_run=args.dry_run,
                stall_seconds=args.stall_seconds,
                max_restarts=args.max_restarts,
            )
        else:
            _list_with_gcloud(
                bucket=bucket,
                dry_run=args.dry_run,
                stall_seconds=args.stall_seconds,
                max_restarts=args.max_restarts,
            )
        return 0

    if backend == "rclone":
        summary = _download_with_rclone(
            remote_root=remote_root,
            folders=folders,
            patterns=patterns,
            download_dir=args.download_dir,
            config_path=args.config,
            extra_args=args.extra_args,
            skip_existing=args.skip_existing,
            dry_run=args.dry_run,
            stall_seconds=args.stall_seconds,
            max_restarts=args.max_restarts,
        )
    else:
        summary = _download_with_gcloud(
            bucket=bucket,
            folders=folders,
            patterns=patterns,
            download_dir=args.download_dir,
            skip_existing=args.skip_existing,
            dry_run=args.dry_run,
            stall_seconds=args.stall_seconds,
            max_restarts=args.max_restarts,
        )

    print("")
    print("[SUMMARY]")
    print(f"Attempted: {summary.attempted}")
    print(f"Succeeded: {summary.succeeded}")
    print(f"Skipped: {summary.skipped}")
    print(f"Failed: {summary.failed}")
    if summary.failed > 0:
        return 1
    print("[OK] Download completed")
    return 0


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download files from GCS via rclone/gcloud")
    cli_args = list(argv) if argv is not None else sys.argv[1:]

    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument("-d", "--download", action="store_true", help="Download files (default)")
    action_group.add_argument("-a", "--alternative", action="store_true", help="Alias of download (compatibility)")
    action_group.add_argument("-l", "--list", action="store_true", help="List bucket contents")

    parser.add_argument(
        "--mode",
        choices=["date-folders", "legacy-range"],
        default=_env_str("GCS_MODE", DEFAULT_MODE),
        help="Download mode (default: env GCS_MODE or legacy-range)",
    )
    parser.add_argument(
        "--remote",
        default=_env_str("RCLONE_REMOTE", DEFAULT_RCLONE_REMOTE),
        help="rclone remote name or full remote path (default: env RCLONE_REMOTE or 'gcs')",
    )
    parser.add_argument(
        "--bucket",
        default=_env_str("BUCKET_NAME", DEFAULT_BUCKET_NAME),
        help="GCS bucket name (default: env BUCKET_NAME)",
    )
    env_buckets = _env_list("GCS_BUCKETS", str)
    parser.add_argument(
        "--buckets",
        nargs="+",
        default=env_buckets if env_buckets else None,
        help="GCS bucket names (space-separated). If set, --bucket is ignored.",
    )
    parser.add_argument(
        "--download-dir",
        default=_env_str("DOWNLOAD_DIR", DEFAULT_DOWNLOAD_DIR),
        help="Local download directory (default: env DOWNLOAD_DIR)",
    )
    parser.add_argument(
        "--archive-dir",
        default=_env_str("ARCHIVE_DIR", "/nas/archive"),
        help="Archive root directory to check done markers (default: env ARCHIVE_DIR or /nas/archive)",
    )
    parser.add_argument(
        "--bucket-subdir",
        dest="bucket_subdir",
        action="store_true",
        default=_env_bool("GCS_BUCKET_SUBDIR", False),
        help="Store files under <download-dir>/<bucket>/... to avoid collisions",
    )
    parser.add_argument(
        "--no-bucket-subdir",
        dest="bucket_subdir",
        action="store_false",
        help="Store files directly under <download-dir> (single bucket use-case)",
    )

    parser.add_argument(
        "--folder-start",
        type=int,
        default=_env_int("GCS_FOLDER_START", DEFAULT_FOLDER_START),
        help="(legacy-range) start folder number for generated range",
    )
    parser.add_argument(
        "--folder-end",
        type=int,
        default=_env_int("GCS_FOLDER_END", DEFAULT_FOLDER_END),
        help="(legacy-range) end folder number for generated range (inclusive)",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=_parse_date(_env_str("GCS_START_DATE", DEFAULT_START_DATE)),
        help="(legacy-range) start date (YYYY-MM-DD) for generated file patterns",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=_parse_date(_env_str("GCS_END_DATE", DEFAULT_END_DATE)),
        help="(legacy-range) end date (YYYY-MM-DD, inclusive) for generated file patterns",
    )

    env_folders = _env_list("GCS_FOLDERS", int)
    env_patterns = _env_list("GCS_FILE_PATTERNS", str)
    env_date_folders = _env_list("GCS_DATE_FOLDERS", str)

    parser.add_argument(
        "--folders",
        nargs="+",
        type=int,
        default=env_folders if env_folders else DEFAULT_LEGACY_FOLDERS,
        help="(legacy-range) folder list (space-separated)",
    )
    parser.add_argument(
        "--patterns",
        nargs="+",
        default=env_patterns if env_patterns else DEFAULT_LEGACY_PATTERNS,
        help="(legacy-range) file patterns (space-separated)",
    )
    parser.add_argument(
        "--date-folders",
        nargs="+",
        default=env_date_folders if env_date_folders else None,
        help="(date-folders) explicit date folders to download",
    )

    parser.add_argument(
        "--backend",
        choices=["auto", "rclone", "gcloud"],
        default=_env_str("GCS_BACKEND", DEFAULT_BACKEND),
        help="Transfer backend selection",
    )
    parser.add_argument(
        "--config",
        default=_env_str("RCLONE_CONFIG", ""),
        help="rclone config file path (default: env RCLONE_CONFIG)",
    )
    parser.add_argument(
        "--extra-args",
        default=_env_str("RCLONE_EXTRA_ARGS", ""),
        help="extra rclone args, passed as-is (default: env RCLONE_EXTRA_ARGS)",
    )
    parser.add_argument(
        "--skip-existing",
        dest="skip_existing",
        action="store_true",
        default=True,
        help="Skip files already present at destination (default: true)",
    )
    parser.add_argument("--overwrite", dest="skip_existing", action="store_false", help="Overwrite existing files")
    parser.add_argument(
        "--skip-archived-done",
        dest="skip_archived_done",
        action="store_true",
        default=_env_bool("GCS_SKIP_ARCHIVED_DONE", True),
        help="Skip date folders already archived with _DONE marker (default: true)",
    )
    parser.add_argument(
        "--no-skip-archived-done",
        dest="skip_archived_done",
        action="store_false",
        help="Do not skip folders even if archive _DONE marker exists",
    )
    parser.add_argument(
        "--stall-seconds",
        type=int,
        default=_env_int("GCS_STALL_SECONDS", DEFAULT_STALL_SECONDS),
        help="Restart current task when no output appears for this many seconds (0 to disable)",
    )
    parser.add_argument(
        "--max-restarts",
        type=int,
        default=_env_int("GCS_MAX_RESTARTS", DEFAULT_MAX_RESTARTS),
        help="Maximum restart attempts per stalled task",
    )
    parser.add_argument("--list-only", action="store_true", help="(date-folders) list target folders and exit")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them")

    args = parser.parse_args(cli_args)
    args._range_explicit = any(x in cli_args for x in ("--folder-start", "--folder-end", "--start-date", "--end-date"))
    args._legacy_explicit = any(x in cli_args for x in ("--folders", "--patterns"))
    args._legacy_from_env = bool(env_folders or env_patterns)
    return args


def main() -> None:
    args = _parse_args()

    if args.folder_start > args.folder_end:
        print(f"[ERROR] Invalid folder range: {args.folder_start} > {args.folder_end}", file=sys.stderr)
        sys.exit(2)
    if args.start_date > args.end_date:
        print(f"[ERROR] Invalid date range: {args.start_date} > {args.end_date}", file=sys.stderr)
        sys.exit(2)
    if args.stall_seconds < 0:
        print("[ERROR] --stall-seconds must be >= 0", file=sys.stderr)
        sys.exit(2)
    if args.max_restarts < 0:
        print("[ERROR] --max-restarts must be >= 0", file=sys.stderr)
        sys.exit(2)

    try:
        backend = _choose_backend(args.backend)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)

    raw_buckets = list(args.buckets) if args.buckets else [args.bucket]
    buckets: List[str] = []
    seen = set()
    for raw in raw_buckets:
        try:
            normalized = _normalize_bucket(str(raw))
        except ValueError as exc:
            print(f"[ERROR] invalid bucket '{raw}': {exc}", file=sys.stderr)
            sys.exit(1)
        if normalized not in seen:
            buckets.append(normalized)
            seen.add(normalized)

    if not buckets:
        print("[ERROR] no bucket resolved", file=sys.stderr)
        sys.exit(1)

    overall_code = 0
    backend_chain_template: List[str]
    if args.backend == "auto" and backend == "rclone":
        # rclone 우선, 실패 시 gcloud fallback 시도
        backend_chain_template = ["rclone", "gcloud"]
    else:
        backend_chain_template = [backend]

    for index, bucket in enumerate(buckets, start=1):
        bucket_args = argparse.Namespace(**vars(args))
        bucket_args.bucket = bucket
        if args.bucket_subdir:
            bucket_args.download_dir = os.path.join(args.download_dir, bucket)
        else:
            bucket_args.download_dir = args.download_dir

        remote_root = _resolve_remote(args.remote, bucket)
        print("")
        print(f"[INFO] bucket target [{index}/{len(buckets)}]: {bucket}")
        print(f"[INFO] bucket download_dir: {bucket_args.download_dir}")

        bucket_success = False
        for backend_idx, backend_choice in enumerate(backend_chain_template, start=1):
            if len(backend_chain_template) > 1:
                print(
                    f"[INFO] backend attempt [{backend_idx}/{len(backend_chain_template)}]: "
                    f"{backend_choice}"
                )
            try:
                if args.mode == "date-folders":
                    code = _run_date_folder_mode(
                        bucket_args,
                        backend=backend_choice,
                        bucket=bucket,
                        remote_root=remote_root,
                    )
                else:
                    code = _run_legacy_mode(
                        bucket_args,
                        backend=backend_choice,
                        bucket=bucket,
                        remote_root=remote_root,
                    )
            except ValueError as exc:
                print(f"[ERROR] [{bucket}] backend={backend_choice} {exc}", file=sys.stderr)
                code = 1

            if code == 0:
                bucket_success = True
                break

            if backend_idx < len(backend_chain_template):
                print(
                    f"[WARN] [{bucket}] backend={backend_choice} failed; "
                    "trying fallback backend.",
                    file=sys.stderr,
                )

        if not bucket_success:
            overall_code = 1

    sys.exit(overall_code)


if __name__ == "__main__":
    main()
