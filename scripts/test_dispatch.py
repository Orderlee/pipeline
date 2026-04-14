#!/usr/bin/env python3
"""Test data-plane dispatch trigger JSON 생성 + 데이터 복사 스크립트.

기본 목적은 파일 기반 dispatch ingress(`incoming/.dispatch/pending`) 회귀 테스트입니다.
현재 기본 ingress는 agent polling이지만, 이 스크립트는 fallback JSON 경로와
DB tracking 상태를 빠르게 확인할 때 사용합니다.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_ROOT = Path(os.getenv("TEST_ROOT") or os.getenv("STAGING_ROOT") or "/home/pia/mou/staging")
COMPOSE_FILE = REPO_ROOT / "docker" / "docker-compose.yaml"
COMPOSE_ENV_FILE = REPO_ROOT / "docker" / ".env.test"
COMPOSE_PROJECT_NAME = os.getenv("TEST_COMPOSE_PROJECT_NAME") or "pipeline-test"
TEST_DAGSTER_SERVICE = os.getenv("TEST_DAGSTER_SERVICE") or "dagster-code-server"
TEST_DUCKDB_PATH = os.getenv("TEST_DUCKDB_PATH") or "/data/staging.duckdb"

SOURCE_FOLDERS = {
    "tmp_data_2": TEST_ROOT / "tmp_data_2",
    "GS건설": TEST_ROOT / "GS건설",
}
INCOMING_DIR = TEST_ROOT / "incoming"
DISPATCH_PENDING = INCOMING_DIR / ".dispatch" / "pending"
DISPATCH_PROCESSED = INCOMING_DIR / ".dispatch" / "processed"
DISPATCH_FAILED = INCOMING_DIR / ".dispatch" / "failed"
ARCHIVE_PENDING_DIR = TEST_ROOT / "archive_pending"

OUTPUT_COMBINATIONS = [
    ["bbox"],
    ["timestamp_video", "captioning_video"],
    ["bbox", "timestamp_video", "captioning_video"],
    ["bbox", "captioning_video"],
    ["timestamp_video"],
]

IMAGE_PARAM_PRESETS = [
    None,
    {"max_frames_per_video": 8, "jpeg_quality": 85},
    {"max_frames_per_video": 16, "jpeg_quality": 90},
    {"max_frames_per_video": 24, "jpeg_quality": 95},
    {"max_frames_per_video": 12, "jpeg_quality": 80, "confidence_threshold": 0.3, "iou_threshold": 0.5},
    {"max_frames_per_video": 32, "jpeg_quality": 92, "confidence_threshold": 0.2, "iou_threshold": 0.4},
]


def _compose_python(code: str, *, timeout: int = 30) -> dict | None:
    command = [
        "docker",
        "compose",
        "--env-file",
        str(COMPOSE_ENV_FILE),
        "-p",
        COMPOSE_PROJECT_NAME,
        "-f",
        str(COMPOSE_FILE),
        "exec",
        "-T",
        TEST_DAGSTER_SERVICE,
        "python3",
        "-c",
        code,
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(REPO_ROOT),
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "error": str(exc)}

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        return {"status": "error", "error": stderr or stdout or f"returncode={result.returncode}"}
    if not stdout:
        return {}
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {"status": "ok", "stdout": stdout}


def copy_test_data(folder_name: str) -> Path:
    src = SOURCE_FOLDERS[folder_name]
    dest = INCOMING_DIR / folder_name

    if dest.exists():
        print(f"  [SKIP] 이미 존재: {dest}")
        return dest

    print(f"  [COPY] {src} -> {dest}")
    shutil.copytree(str(src), str(dest), symlinks=False)
    file_count = sum(1 for item in dest.rglob("*") if item.is_file())
    print(f"  [DONE] {file_count}개 파일 복사 완료")
    return dest


def generate_trigger_json(
    folder_name: str,
    round_num: int,
    test_idx: int,
    outputs: list[str] | None = None,
    image_params: dict | None = None,
    request_id_override: str | None = None,
) -> dict:
    if outputs is None:
        outputs = random.choice(OUTPUT_COMBINATIONS)
    if image_params is None:
        image_params = random.choice(IMAGE_PARAM_PRESETS)

    now = datetime.now()
    request_id = request_id_override or f"test_r{round_num:02d}_t{test_idx:02d}_{now:%Y%m%d_%H%M%S}"

    trigger = {
        "request_id": request_id,
        "folder_name": folder_name,
        "labeling_method": outputs,
        "outputs": outputs,
        "requested_by": "test_dispatch_script",
        "requested_at": now.isoformat(),
    }
    if image_params:
        trigger.update(image_params)
    return trigger


def wait_until_folder_in_archive_pending(folder_name: str, timeout_sec: int = 180, interval: float = 5.0) -> None:
    dest = ARCHIVE_PENDING_DIR / folder_name
    print(f"  [WAIT] archive_pending/{folder_name} 생성 대기 (최대 {timeout_sec}s)...")
    start = time.time()
    while time.time() - start < timeout_sec:
        if dest.is_dir():
            print(f"  [OK] archive_pending 확인 ({time.time() - start:.0f}s)")
            return
        time.sleep(interval)
    raise TimeoutError(f"archive_pending/{folder_name} 미생성 ({timeout_sec}s 초과)")


def place_trigger(trigger: dict) -> Path:
    DISPATCH_PENDING.mkdir(parents=True, exist_ok=True)
    DISPATCH_PROCESSED.mkdir(parents=True, exist_ok=True)
    DISPATCH_FAILED.mkdir(parents=True, exist_ok=True)

    filepath = DISPATCH_PENDING / f"{trigger['request_id']}.json"
    filepath.write_text(json.dumps(trigger, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"  [TRIGGER] {filepath}")
    print(f"    request_id: {trigger['request_id']}")
    print(f"    folder:     {trigger['folder_name']}")
    print(f"    outputs:    {trigger['labeling_method']}")
    params = {
        key: trigger[key]
        for key in ("max_frames_per_video", "jpeg_quality", "confidence_threshold", "iou_threshold")
        if key in trigger
    }
    if params:
        print(f"    params:     {params}")
    else:
        print("    params:     (기본값 사용)")
    return filepath


def wait_for_dispatch_processing(request_id: str, timeout_sec: int = 300) -> str:
    print(f"  [WAIT] dispatch sensor 처리 대기 (최대 {timeout_sec}s)...")
    start = time.time()
    while time.time() - start < timeout_sec:
        pending_file = DISPATCH_PENDING / f"{request_id}.json"
        processed_file = DISPATCH_PROCESSED / f"{request_id}.json"
        failed_file = DISPATCH_FAILED / f"{request_id}.json"

        if processed_file.exists():
            print(f"  [OK] dispatch 처리 완료 -> processed ({time.time() - start:.0f}s)")
            return "processed"
        if failed_file.exists():
            print(f"  [FAIL] dispatch 실패 -> failed ({time.time() - start:.0f}s)")
            try:
                print(f"    내용: {failed_file.read_text(encoding='utf-8')[:200]}")
            except Exception:  # noqa: BLE001
                pass
            return "failed"
        if not pending_file.exists():
            print("  [INFO] pending에서 사라짐, DB 확인 중...")
            return "unknown"

        elapsed = int(time.time() - start)
        if elapsed % 30 == 0 and elapsed > 0:
            print(f"    ... {elapsed}s 경과, 아직 pending 상태")
        time.sleep(10)

    print(f"  [TIMEOUT] {timeout_sec}s 초과")
    return "timeout"


def check_dagster_run_status(request_id: str) -> dict:
    print("  [CHECK] Dagster run 상태 확인...")
    check_script = f"""
import duckdb, json
conn = duckdb.connect({json.dumps(TEST_DUCKDB_PATH)}, read_only=True)
try:
    row = conn.execute(
        \"\"\"
        SELECT request_id, folder_name, outputs, status, error_message
        FROM dispatch_requests
        WHERE request_id = ?
        \"\"\",
        [{json.dumps(request_id)}]
    ).fetchone()
    if row:
        print(json.dumps({{
            "request_id": row[0],
            "folder_name": row[1],
            "outputs": row[2],
            "status": row[3],
            "error": row[4],
        }}))
    else:
        print(json.dumps({{"status": "not_found"}}))
finally:
    conn.close()
"""
    data = _compose_python(check_script)
    if data:
        print(f"    DB 상태: {data}")
        return data
    return {"status": "unknown"}


def get_db_summary() -> dict:
    script = f"""
import duckdb, json
conn = duckdb.connect({json.dumps(TEST_DUCKDB_PATH)}, read_only=True)
summary = {{}}
for table in ['raw_files', 'video_metadata', 'labels', 'processed_clips', 'image_metadata', 'image_labels', 'dispatch_requests']:
    try:
        summary[table] = conn.execute(f'SELECT COUNT(*) FROM {{table}}').fetchone()[0]
    except Exception:
        summary[table] = -1
try:
    rows = conn.execute(
        'SELECT step_name, step_status, COUNT(*) FROM dispatch_pipeline_runs GROUP BY step_name, step_status ORDER BY step_name'
    ).fetchall()
    summary['pipeline_runs'] = [{{'step': r[0], 'status': r[1], 'count': r[2]}} for r in rows]
except Exception:
    summary['pipeline_runs'] = []
try:
    rows = conn.execute(
        'SELECT output_type, model_name, is_active FROM dispatch_model_configs ORDER BY output_type'
    ).fetchall()
    summary['model_configs'] = [{{'output': r[0], 'model': r[1], 'active': r[2]}} for r in rows]
except Exception:
    summary['model_configs'] = []
conn.close()
print(json.dumps(summary, default=str))
"""
    data = _compose_python(script)
    return data if isinstance(data, dict) else {}


def run_cycle(round_num: int) -> list[dict]:
    results = []
    for idx, folder in enumerate(["tmp_data_2", "GS건설"], start=1):
        print(f"\n{'=' * 60}")
        print(f"[ROUND {round_num}] TEST {idx}: {folder}")
        print(f"{'=' * 60}")
        copy_test_data(folder)
        trigger = generate_trigger_json(folder, round_num, idx)
        place_trigger(trigger)
        results.append(
            {
                "request_id": trigger["request_id"],
                "folder_name": folder,
                "outputs": trigger["labeling_method"],
                "dispatch_status": wait_for_dispatch_processing(trigger["request_id"], timeout_sec=180),
                "db_status": check_dagster_run_status(trigger["request_id"]),
            }
        )
        if idx < 2:
            print("\n  [PAUSE] 다음 테스트 전 30초 대기...")
            time.sleep(30)
    return results


def print_report(all_results: list[list[dict]]) -> None:
    print(f"\n{'=' * 70}")
    print("  TEST dispatch 보고서")
    print(f"  생성 시각: {datetime.now().isoformat()}")
    print(f"{'=' * 70}")

    for round_idx, round_results in enumerate(all_results, start=1):
        print(f"\n--- Round {round_idx} ---")
        for result in round_results:
            status_icon = "✅" if result["dispatch_status"] == "processed" else "❌"
            print(f"  {status_icon} {result['folder_name']}")
            print(f"     request_id: {result['request_id']}")
            print(f"     outputs:    {result['outputs']}")
            print(f"     dispatch:   {result['dispatch_status']}")
            print(f"     db_status:  {result['db_status'].get('status', 'unknown')}")

    print("\n--- DB 최종 상태 ---")
    summary = get_db_summary()
    for table, count in summary.items():
        if table in {"pipeline_runs", "model_configs"}:
            continue
        print(f"  {table}: {count} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Test data-plane dispatch trigger 생성")
    parser.add_argument("--folder", choices=["tmp_data_2", "GS건설"], help="테스트할 폴더")
    parser.add_argument("--round", type=int, default=1, help="라운드 번호")
    parser.add_argument("--test-idx", type=int, default=1, help="테스트 인덱스")
    parser.add_argument("--outputs", nargs="+", help="출력 타입 목록")
    parser.add_argument("--auto-cycle", action="store_true", help="자동 사이클 모드")
    parser.add_argument("--rounds", type=int, default=3, help="자동 사이클 반복 횟수")
    parser.add_argument("--generate-only", action="store_true", help="trigger JSON만 생성 (대기 안함)")
    parser.add_argument("--wait-archive-pending", action="store_true", help="legacy archive_pending 이동을 기다린 뒤 트리거")
    parser.add_argument("--skip-copy", action="store_true", help="이미 incoming에 배치됨 — 복사 생략")
    parser.add_argument("--request-id", type=str, default=None, help="고정 request_id")
    args = parser.parse_args()

    if args.auto_cycle:
        all_results = []
        for round_num in range(1, args.rounds + 1):
            all_results.append(run_cycle(round_num))
            if round_num < args.rounds:
                print(f"\n[CYCLE] Round {round_num} 완료, 60초 후 다음 라운드...")
                time.sleep(60)
        print_report(all_results)
        return

    if not args.folder:
        parser.print_help()
        return

    trigger = generate_trigger_json(
        args.folder,
        args.round,
        args.test_idx,
        outputs=args.outputs,
        request_id_override=args.request_id,
    )
    if args.skip_copy:
        if not args.wait_archive_pending:
            print("  --skip-copy 는 --wait-archive-pending 와 함께 사용하세요.", file=sys.stderr)
            sys.exit(1)
    else:
        copy_test_data(args.folder)

    if args.wait_archive_pending:
        wait_until_folder_in_archive_pending(args.folder)

    place_trigger(trigger)
    if not args.generate_only:
        wait_for_dispatch_processing(trigger["request_id"], timeout_sec=300)
        check_dagster_run_status(trigger["request_id"])


if __name__ == "__main__":
    main()
