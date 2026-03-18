#!/usr/bin/env python3
"""dispatch 트리거 이후 파이프라인 완료까지 DuckDB 읽기 전용 폴링."""
import argparse
import json
import subprocess
import sys
import time

REPO = "/home/pia/work_p/Datapipeline-Data-data_pipeline"


def run_sql(query: str) -> int:
    code = f"""import duckdb
c = duckdb.connect("/data/staging.duckdb", read_only=True)
r = c.execute({json.dumps(query)}).fetchone()
print(int(r[0]) if r and r[0] is not None else 0)
c.close()
"""
    r = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "docker/docker-compose.yaml",
            "exec",
            "-T",
            "dagster-staging",
            "python3",
            "-c",
            code,
        ],
        capture_output=True,
        text=True,
        cwd=REPO,
        timeout=90,
    )
    if r.returncode != 0:
        return -1
    try:
        return int((r.stdout or "").strip())
    except ValueError:
        return -1


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--folder-prefix", default="tmp_data_2")
    p.add_argument(
        "--mode",
        choices=[
            "timestamp",
            "ts_cap",
            "bbox",
            "bbox_cap",
            "dual_ts_cap",
            "dispatch_failed",
        ],
        required=True,
    )
    p.add_argument("--request-id", default="", help="dispatch_failed 모드에서 failed 파일 존재 확인")
    p.add_argument("--interval", type=int, default=25)
    p.add_argument("--max-wait", type=int, default=7200)
    args = p.parse_args()
    like = args.folder_prefix + "/%"
    start = time.time()

    while time.time() - start < args.max_wait:
        if args.mode == "timestamp":
            vids = run_sql(
                f"SELECT COUNT(*) FROM raw_files WHERE raw_key LIKE '{like}' AND media_type='video'"
            )
            done = run_sql(
                "SELECT COUNT(*) FROM raw_files rf JOIN video_metadata vm ON vm.asset_id=rf.asset_id "
                f"WHERE rf.raw_key LIKE '{like}' AND rf.ingest_status='completed' "
                "AND vm.auto_label_status='generated'"
            )
            if vids > 0 and done >= vids:
                print(json.dumps({"ok": True, "videos": vids, "generated": done}))
                return
        elif args.mode == "ts_cap":
            clips = run_sql(
                "SELECT COUNT(*) FROM processed_clips pc JOIN raw_files rf ON rf.asset_id=pc.source_asset_id "
                f"WHERE rf.raw_key LIKE '{like}'"
            )
            if clips > 0:
                print(json.dumps({"ok": True, "processed_clips": clips}))
                return
        elif args.mode == "bbox":
            vids = run_sql(f"SELECT COUNT(*) FROM raw_files WHERE raw_key LIKE '{like}' AND media_type='video'")
            fe = run_sql(
                "SELECT COUNT(*) FROM raw_files rf JOIN video_metadata vm ON vm.asset_id=rf.asset_id "
                f"WHERE rf.raw_key LIKE '{like}' AND vm.frame_extract_status='completed'"
            )
            frames = run_sql(
                "SELECT COUNT(*) FROM image_metadata im JOIN raw_files rf ON rf.asset_id=im.source_asset_id "
                f"WHERE rf.raw_key LIKE '{like}' AND im.image_role='raw_video_frame'"
            )
            if vids > 0 and fe >= vids and frames > 0:
                print(json.dumps({"ok": True, "videos": vids, "frames": frames}))
                return
        elif args.mode == "bbox_cap":
            y = run_sql(
                "SELECT COUNT(*) FROM image_labels il JOIN image_metadata im ON im.image_id=il.image_id "
                "JOIN raw_files rf ON rf.asset_id=im.source_asset_id "
                f"WHERE rf.raw_key LIKE '{like}'"
            )
            clips = run_sql(
                "SELECT COUNT(*) FROM processed_clips pc JOIN raw_files rf ON rf.asset_id=pc.source_asset_id "
                f"WHERE rf.raw_key LIKE '{like}'"
            )
            if y > 0 and clips > 0:
                print(json.dumps({"ok": True, "image_labels": y, "clips": clips}))
                return
        elif args.mode == "dual_ts_cap":
            t = run_sql(
                "SELECT COUNT(*) FROM processed_clips pc JOIN raw_files rf ON rf.asset_id=pc.source_asset_id "
                "WHERE rf.raw_key LIKE 'tmp_data_2/%'"
            )
            g = run_sql("SELECT COUNT(*) FROM raw_files WHERE raw_key LIKE 'gsgeonseol/%'")
            if t > 0 and g == 0:
                print(json.dumps({"ok": True, "processed_clips_tmp": t, "gsgeonseol_rows": g}))
                return
        elif args.mode == "dispatch_failed":
            from pathlib import Path

            pth = Path("/home/pia/mou/staging/incoming/.dispatch/failed") / f"{args.request_id}.json"
            if pth.is_file():
                print(json.dumps({"ok": True, "failed_file": str(pth)}))
                return

        elapsed = int(time.time() - start)
        if elapsed % 90 == 0 and elapsed > 0:
            print(f"  ... 대기 {elapsed}s mode={args.mode}", flush=True)
        time.sleep(args.interval)

    print(json.dumps({"ok": False, "timeout_sec": args.max_wait}))
    sys.exit(2)


if __name__ == "__main__":
    main()
