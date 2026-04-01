#!/usr/bin/env python3
"""Staging 테스트용 dispatch trigger JSON 생성 + 데이터 복사 스크립트.

기본 목적은 파일 기반 dispatch ingress(`incoming/.dispatch/pending`) 회귀 테스트입니다.
staging 기본 ingress는 agent polling이며, 이 스크립트는 호환/레거시 경로 검증에 사용합니다.

사용법:
    python3 staging_test_dispatch.py --folder tmp_data_2 --round 1
    python3 staging_test_dispatch.py --folder GS건설 --round 1
    python3 staging_test_dispatch.py --auto-cycle --rounds 3
"""

import argparse
import json
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── 경로 설정 ──
STAGING_ROOT = Path("/home/pia/mou/staging")
SOURCE_FOLDERS = {
    "tmp_data_2": STAGING_ROOT / "tmp_data_2",
    "GS건설": STAGING_ROOT / "GS건설",
}
INCOMING_DIR = STAGING_ROOT / "incoming"
DISPATCH_PENDING = INCOMING_DIR / ".dispatch" / "pending"
DISPATCH_PROCESSED = INCOMING_DIR / ".dispatch" / "processed"
DISPATCH_FAILED = INCOMING_DIR / ".dispatch" / "failed"
ARCHIVE_DIR = STAGING_ROOT / "archive"
ARCHIVE_PENDING_DIR = STAGING_ROOT / "archive_pending"

# ── Output 조합 (번갈아가며 사용) ──
OUTPUT_COMBINATIONS = [
    ["bbox"],
    ["timestamp_video", "captioning_video"],
    ["bbox", "timestamp_video", "captioning_video"],
    ["bbox", "captioning_video"],
    ["timestamp_video"],
]

# ── 이미지 추출 파라미터 프리셋 ──
IMAGE_PARAM_PRESETS = [
    # None = 기본값 사용 (model_configs에서 가져옴)
    None,
    {"max_frames_per_video": 8, "jpeg_quality": 85},
    {"max_frames_per_video": 16, "jpeg_quality": 90},
    {"max_frames_per_video": 24, "jpeg_quality": 95},
    {"max_frames_per_video": 12, "jpeg_quality": 80, "confidence_threshold": 0.3, "iou_threshold": 0.5},
    {"max_frames_per_video": 32, "jpeg_quality": 92, "confidence_threshold": 0.2, "iou_threshold": 0.4},
]


def copy_test_data(folder_name: str) -> Path:
    """소스 폴더를 incoming으로 복사."""
    src = SOURCE_FOLDERS[folder_name]
    dest = INCOMING_DIR / folder_name

    if dest.exists():
        print(f"  [SKIP] 이미 존재: {dest}")
        return dest

    print(f"  [COPY] {src} → {dest}")
    shutil.copytree(str(src), str(dest), symlinks=False)
    file_count = sum(1 for _ in dest.rglob("*") if _.is_file())
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
    """랜덤 trigger JSON 생성."""
    if outputs is None:
        outputs = random.choice(OUTPUT_COMBINATIONS)
    if image_params is None:
        image_params = random.choice(IMAGE_PARAM_PRESETS)

    now = datetime.now()
    request_id = request_id_override or f"staging_r{round_num:02d}_t{test_idx:02d}_{now:%Y%m%d_%H%M%S}"

    trigger = {
        "request_id": request_id,
        "folder_name": folder_name,
        "labeling_method": outputs,
        "requested_by": "staging_test_script",
        "requested_at": now.isoformat(),
    }

    # 이미지 추출 파라미터가 있으면 추가 (없으면 기본값 사용)
    if image_params:
        trigger.update(image_params)

    return trigger


def wait_until_folder_in_archive_pending(folder_name: str, timeout_sec: int = 180, interval: float = 5.0) -> None:
    """레거시 archive_pending 워크플로우를 사용하는 환경에서만 대기."""
    dest = ARCHIVE_PENDING_DIR / folder_name
    print(f"  [WAIT] archive_pending/{folder_name} 생성 대기 (최대 {timeout_sec}s)...")
    start = time.time()
    while time.time() - start < timeout_sec:
        if dest.is_dir():
            elapsed = time.time() - start
            print(f"  [OK] archive_pending 확인 ({elapsed:.0f}s)")
            return
        time.sleep(interval)
    raise TimeoutError(f"archive_pending/{folder_name} 미생성 ({timeout_sec}s 초과)")


def place_trigger(trigger: dict) -> Path:
    """trigger JSON을 dispatch/pending에 배치."""
    DISPATCH_PENDING.mkdir(parents=True, exist_ok=True)
    DISPATCH_PROCESSED.mkdir(parents=True, exist_ok=True)
    DISPATCH_FAILED.mkdir(parents=True, exist_ok=True)

    filename = f"{trigger['request_id']}.json"
    filepath = DISPATCH_PENDING / filename

    filepath.write_text(json.dumps(trigger, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  [TRIGGER] {filepath}")
    print(f"    request_id: {trigger['request_id']}")
    print(f"    folder:     {trigger['folder_name']}")
    print(f"    outputs:    {trigger['outputs']}")
    if any(k in trigger for k in ("max_frames_per_video", "jpeg_quality", "confidence_threshold")):
        params = {k: trigger[k] for k in ("max_frames_per_video", "jpeg_quality", "confidence_threshold", "iou_threshold") if k in trigger}
        print(f"    params:     {params}")
    else:
        print("    params:     (기본값 사용)")
    return filepath


def wait_for_dispatch_processing(request_id: str, timeout_sec: int = 300) -> str:
    """dispatch sensor가 trigger를 처리할 때까지 대기."""
    print(f"  [WAIT] dispatch sensor 처리 대기 (최대 {timeout_sec}s)...")
    start = time.time()
    check_interval = 10

    while time.time() - start < timeout_sec:
        # pending에서 사라졌는지 확인
        pending_file = DISPATCH_PENDING / f"{request_id}.json"
        processed_file = DISPATCH_PROCESSED / f"{request_id}.json"
        failed_file = DISPATCH_FAILED / f"{request_id}.json"

        if processed_file.exists():
            print(f"  [OK] dispatch 처리 완료 → processed ({time.time()-start:.0f}s)")
            return "processed"
        if failed_file.exists():
            print(f"  [FAIL] dispatch 실패 → failed ({time.time()-start:.0f}s)")
            try:
                content = failed_file.read_text()
                print(f"    내용: {content[:200]}")
            except Exception:
                pass
            return "failed"
        if not pending_file.exists():
            # DB에서 상태 확인
            print("  [INFO] pending에서 사라짐, DB 확인 중...")
            return "unknown"

        elapsed = int(time.time() - start)
        if elapsed % 30 == 0 and elapsed > 0:
            print(f"    ... {elapsed}s 경과, 아직 pending 상태")
        time.sleep(check_interval)

    print(f"  [TIMEOUT] {timeout_sec}s 초과")
    return "timeout"


def check_dagster_run_status(request_id: str, timeout_sec: int = 600) -> dict:
    """Dagster run 상태를 DB에서 확인."""
    print("  [CHECK] Dagster run 상태 확인...")
    # 컨테이너 내에서 DB 조회
    check_script = f"""
import duckdb, json
conn = duckdb.connect('/data/staging.duckdb', read_only=True)
try:
    row = conn.execute(
        "SELECT request_id, folder_name, outputs, status, error_message FROM staging_dispatch_requests WHERE request_id = ?",
        ['{request_id}']
    ).fetchone()
    if row:
        print(json.dumps({{"request_id": row[0], "folder_name": row[1], "outputs": row[2], "status": row[3], "error": row[4]}}))
    else:
        print(json.dumps({{"status": "not_found"}}))
except Exception as e:
    print(json.dumps({{"status": "error", "error": str(e)}}))
conn.close()
"""
    try:
        result = subprocess.run(
            ["docker", "compose", "-f", "docker/docker-compose.yaml", "exec", "-T", "dagster-staging", "python3", "-c", check_script],
            capture_output=True, text=True, timeout=30,
            cwd="/home/pia/work_p/Datapipeline-Data-data_pipeline",
        )
        if result.stdout.strip():
            data = json.loads(result.stdout.strip())
            print(f"    DB 상태: {data}")
            return data
    except Exception as e:
        print(f"    DB 조회 실패: {e}")
    return {"status": "unknown"}


def get_db_summary() -> dict:
    """DB 전체 요약 조회."""
    script = """
import duckdb, json
conn = duckdb.connect('/data/staging.duckdb', read_only=True)
summary = {}
for table in ['raw_files', 'video_metadata', 'labels', 'processed_clips', 'image_metadata', 'image_labels', 'staging_dispatch_requests']:
    try:
        count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        summary[table] = count
    except:
        summary[table] = -1
# pipeline_runs
try:
    rows = conn.execute('SELECT step_name, step_status, COUNT(*) FROM staging_pipeline_runs GROUP BY step_name, step_status ORDER BY step_name').fetchall()
    summary['pipeline_runs'] = [{'step': r[0], 'status': r[1], 'count': r[2]} for r in rows]
except:
    summary['pipeline_runs'] = []
# model_configs
try:
    rows = conn.execute('SELECT output_type, model_name, is_active FROM staging_model_configs').fetchall()
    summary['model_configs'] = [{'output': r[0], 'model': r[1], 'active': r[2]} for r in rows]
except:
    summary['model_configs'] = []
conn.close()
print(json.dumps(summary, default=str))
"""
    try:
        result = subprocess.run(
            ["docker", "compose", "-f", "docker/docker-compose.yaml", "exec", "-T", "dagster-staging", "python3", "-c", script],
            capture_output=True, text=True, timeout=30,
            cwd="/home/pia/work_p/Datapipeline-Data-data_pipeline",
        )
        if result.stdout.strip():
            return json.loads(result.stdout.strip())
    except Exception as e:
        print(f"DB 요약 조회 실패: {e}")
    return {}


def cleanup_for_next_test(folder_name: str):
    """다음 테스트를 위해 incoming 폴더 정리."""
    dest = INCOMING_DIR / folder_name
    if dest.exists():
        shutil.rmtree(str(dest))
        print(f"  [CLEANUP] {dest} 제거")


def run_single_test(folder_name: str, round_num: int, test_idx: int):
    """단일 테스트 실행."""
    print(f"\n{'='*60}")
    print(f"[ROUND {round_num}] TEST {test_idx}: {folder_name}")
    print(f"{'='*60}")

    # 1. 데이터 복사
    copy_test_data(folder_name)

    # 2. trigger JSON 생성
    trigger = generate_trigger_json(folder_name, round_num, test_idx)
    place_trigger(trigger)

    # 3. dispatch 처리 대기
    status = wait_for_dispatch_processing(trigger["request_id"], timeout_sec=180)

    # 4. DB 상태 확인
    db_status = check_dagster_run_status(trigger["request_id"])

    return {
        "request_id": trigger["request_id"],
        "folder_name": folder_name,
        "outputs": trigger["outputs"],
        "dispatch_status": status,
        "db_status": db_status,
        "trigger": trigger,
    }


def run_cycle(round_num: int) -> list[dict]:
    """한 사이클 (tmp_data_2 + GS건설) 실행."""
    results = []

    for idx, folder in enumerate(["tmp_data_2", "GS건설"], start=1):
        result = run_single_test(folder, round_num, idx)
        results.append(result)

        # 다음 테스트 전 잠시 대기
        if idx < 2:
            print("\n  [PAUSE] 다음 테스트 전 30초 대기...")
            time.sleep(30)

    return results


def print_report(all_results: list[list[dict]]):
    """전체 테스트 보고서 출력."""
    print(f"\n{'='*70}")
    print("  STAGING 테스트 보고서")
    print(f"  생성 시각: {datetime.now().isoformat()}")
    print(f"{'='*70}")

    for round_idx, round_results in enumerate(all_results, start=1):
        print(f"\n--- Round {round_idx} ---")
        for r in round_results:
            status_icon = "✅" if r["dispatch_status"] == "processed" else "❌"
            print(f"  {status_icon} {r['folder_name']}")
            print(f"     request_id: {r['request_id']}")
            print(f"     outputs:    {r['outputs']}")
            print(f"     dispatch:   {r['dispatch_status']}")
            print(f"     db_status:  {r['db_status'].get('status', 'unknown')}")
            if r['db_status'].get('error'):
                print(f"     error:      {r['db_status']['error']}")

    # DB 요약
    print("\n--- DB 최종 상태 ---")
    summary = get_db_summary()
    for table, count in summary.items():
        if table in ('pipeline_runs', 'model_configs'):
            continue
        print(f"  {table}: {count} rows")

    if summary.get('model_configs'):
        print("\n--- Model Configs ---")
        for mc in summary['model_configs']:
            print(f"  {mc['output']}: {mc['model']} (active={mc['active']})")

    if summary.get('pipeline_runs'):
        print("\n--- Pipeline Runs ---")
        for pr in summary['pipeline_runs']:
            print(f"  {pr['step']}: {pr['status']} ({pr['count']})")


def main():
    parser = argparse.ArgumentParser(description="Staging 테스트 dispatch trigger 생성")
    parser.add_argument("--folder", choices=["tmp_data_2", "GS건설"], help="테스트할 폴더")
    parser.add_argument("--round", type=int, default=1, help="라운드 번호")
    parser.add_argument("--test-idx", type=int, default=1, help="테스트 인덱스")
    parser.add_argument(
        "--outputs",
        nargs="+",
        help="출력 타입 (bbox, timestamp_video, captioning_video, captioning_image, classification_video, classification_image, skip)",
    )
    parser.add_argument("--auto-cycle", action="store_true", help="자동 사이클 모드")
    parser.add_argument("--rounds", type=int, default=3, help="자동 사이클 반복 횟수")
    parser.add_argument("--generate-only", action="store_true", help="trigger JSON만 생성 (대기 안함)")
    parser.add_argument(
        "--wait-archive-pending",
        action="store_true",
        help="레거시 archive_pending 이동을 기다린 뒤 트리거 (기본 staging ingress에는 불필요)",
    )
    parser.add_argument(
        "--skip-copy",
        action="store_true",
        help="이미 incoming에 배치됨 — 복사 생략 (필요 시 legacy archive_pending 대기만 수행)",
    )
    parser.add_argument("--request-id", type=str, default=None, help="고정 request_id (QA 보고용)")
    args = parser.parse_args()

    if args.auto_cycle:
        all_results = []
        for r in range(1, args.rounds + 1):
            results = run_cycle(r)
            all_results.append(results)
            if r < args.rounds:
                print(f"\n[CYCLE] Round {r} 완료, 60초 후 다음 라운드...")
                time.sleep(60)
        print_report(all_results)
    elif args.folder:
        trigger = generate_trigger_json(
            args.folder, args.round, args.test_idx,
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
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
