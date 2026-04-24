#!/usr/bin/env python3
"""ingest 파이프라인 단계별 성능 벤치마크 — run-in-place (코드 수정 없음).

각 단계를 isolation 측정하고 thread/process 수에 따른 스케일링 확인:

  nas       — 파일 읽기 throughput (OS/SMB 캐시 영향 완화 후)
  checksum  — SHA256 per file
  phash     — imagehash.phash (이미지만, lib/phash.py 로직과 동일)
  minio     — boto3 PUT → staging MinIO 임시 버킷
  duckdb    — raw_files 스키마 재현, N 프로세스 동시 INSERT (lock 충돌률)

샘플: staging archive 에서 프로젝트 다양하게 pick. 비파괴 읽기만.
MinIO 테스트: vlm-bench-tmp 버킷 생성 후 put, 테스트 종료 시 비움.
DuckDB 테스트: /tmp/bench_*.duckdb 임시 파일.

실행:
    python3 bench_ingest_stages.py --sample 100
    python3 bench_ingest_stages.py --sample 100 --threads 1,2,4,8 --procs 1,2,4,6
    python3 bench_ingest_stages.py --only checksum,minio
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SAMPLE_ROOT = Path(os.environ.get("BENCH_SAMPLE_ROOT", "/home/pia/mou/staging/archive"))
MANIFEST_CSV = Path(
    os.environ.get(
        "BENCH_MANIFEST",
        "/home/pia/work_p/Datapipeline-Data-data_pipeline/scripts/migrate_legacy/pilot_manifest.csv",
    )
)
PILOT_BATCH = os.environ.get("BENCH_PILOT_BATCH", "pilot_20260423")
IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp"}
SKIP_TOP = {"tmp_data_3"}  # 파일럿 아닌 잔재


def human(n: float, unit: str = "") -> str:
    if n >= 1024**3:
        return f"{n / 1024**3:.2f}GB{unit}"
    if n >= 1024**2:
        return f"{n / 1024**2:.1f}MB{unit}"
    if n >= 1024:
        return f"{n / 1024:.1f}KB{unit}"
    return f"{n:.0f}B{unit}"


def fmt_rate(bytes_total: float, secs: float) -> str:
    if secs <= 0:
        return "inf"
    return f"{bytes_total / secs / 1024 / 1024:.1f}MB/s"


def pick_samples(root: Path, n: int, seed: int = 42) -> list[Path]:
    """archive 하위 각 프로젝트의 pilot batch 폴더를 os.walk 로 훑는다.
    프로젝트별 최대 per_project_cap 개까지 후보 수집 (rglob 보다 빠름)."""
    random.seed(seed)
    per_project_cap = max(20, n // 5)
    exts_all = IMG_EXTS | {".mp4", ".mov", ".avi", ".mkv", ".webm", ".mts", ".m2ts", ".m4v", ".mpg", ".mpeg", ".heic"}

    projects = [p for p in sorted(root.iterdir()) if p.is_dir() and p.name not in SKIP_TOP]
    print(f"  scanning {len(projects)} projects (cap {per_project_cap}/proj) ...")
    per_project: dict[str, list[Path]] = {}
    for proj_dir in projects:
        # 파일럿은 archive/<project>/pilot_20260423/ 아래. prod 원본 archive 는 다른 구조일 수 있으니 존재시에만.
        roots = [proj_dir / PILOT_BATCH, proj_dir]
        walk_root = next((r for r in roots if r.exists()), None)
        if walk_root is None:
            continue
        collected: list[Path] = []
        try:
            for cur, dirs, files in os.walk(walk_root):
                for fn in files:
                    if fn.startswith("._") or fn == "_DONE":
                        continue
                    ext = os.path.splitext(fn)[1].lower()
                    if ext not in exts_all:
                        continue
                    collected.append(Path(cur) / fn)
                    if len(collected) >= per_project_cap:
                        break
                if len(collected) >= per_project_cap:
                    break
        except OSError:
            continue
        if collected:
            per_project[proj_dir.name] = collected

    print(f"  collected from {len(per_project)} projects, total candidates = {sum(len(v) for v in per_project.values())}")

    # round-robin 으로 n개 픽
    samples: list[Path] = []
    keys = list(per_project.keys())
    random.shuffle(keys)
    while len(samples) < n and any(per_project.values()):
        for k in list(keys):
            if len(samples) >= n:
                break
            if per_project[k]:
                samples.append(per_project[k].pop(random.randrange(len(per_project[k]))))

    if not samples:
        return []

    sizes = sorted(f.stat().st_size for f in samples)
    total = sum(sizes)
    img = sum(1 for f in samples if f.suffix.lower() in IMG_EXTS)
    vid = len(samples) - img
    print(
        f"  picked {len(samples)} samples  total={human(total)}  median={human(sizes[len(sizes) // 2])}  "
        f"min={human(sizes[0])}  max={human(sizes[-1])}  img={img}  vid={vid}"
    )
    return samples


def _read_bytes(path: Path) -> int:
    with path.open("rb") as fh:
        n = 0
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            n += len(chunk)
        return n


def _sha256_file(path: Path) -> tuple[int, str]:
    h = hashlib.sha256()
    total = 0
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
            total += len(chunk)
    return total, h.hexdigest()


def _phash_file(path: Path) -> tuple[int, str]:
    # 이미지만 대상 (lib/phash.py 와 동일 로직)
    from PIL import Image  # lazy import
    import imagehash

    with Image.open(path) as img:
        size = path.stat().st_size
        return size, str(imagehash.phash(img))


def _run_parallel(
    fn, items: list, threads: int, label: str
) -> tuple[float, int, list[Exception]]:
    t0 = time.perf_counter()
    errors: list[Exception] = []
    total_bytes = 0

    def _wrap(it):
        nonlocal total_bytes
        try:
            n, _ = fn(it)
            return n
        except Exception as e:  # noqa: BLE001
            errors.append(e)
            return 0

    if threads == 1:
        for it in items:
            total_bytes += _wrap(it)
    else:
        with ThreadPoolExecutor(max_workers=threads) as ex:
            for n in ex.map(_wrap, items):
                total_bytes += n
    elapsed = time.perf_counter() - t0
    return elapsed, total_bytes, errors


def bench_nas(samples: list[Path], threads_list: list[int]) -> None:
    print("\n=== NAS read throughput (SMB) ===")
    print(f"{'threads':>7}  {'elapsed':>8}  {'throughput':>12}  {'per-file':>10}  errors")
    for t in threads_list:
        elapsed, total, errs = _run_parallel(lambda p: (_read_bytes(p), None), samples, t, "read")
        print(
            f"{t:>7}  {elapsed:>7.2f}s  {fmt_rate(total, elapsed):>12}  "
            f"{elapsed / len(samples) * 1000:>8.1f}ms  {len(errs)}"
        )


def bench_checksum(samples: list[Path], threads_list: list[int]) -> None:
    print("\n=== SHA256 checksum (NAS read + CPU) ===")
    print(f"{'threads':>7}  {'elapsed':>8}  {'throughput':>12}  {'per-file':>10}  errors")
    for t in threads_list:
        elapsed, total, errs = _run_parallel(_sha256_file, samples, t, "sha256")
        print(
            f"{t:>7}  {elapsed:>7.2f}s  {fmt_rate(total, elapsed):>12}  "
            f"{elapsed / len(samples) * 1000:>8.1f}ms  {len(errs)}"
        )


def bench_phash(samples: list[Path], threads_list: list[int]) -> None:
    imgs = [p for p in samples if p.suffix.lower() in IMG_EXTS]
    if not imgs:
        print("\n=== phash: no image samples, skipped")
        return
    print(f"\n=== phash (imagehash.phash, {len(imgs)} 이미지만) ===")
    print(f"{'threads':>7}  {'elapsed':>8}  {'imgs/s':>10}  {'per-img':>10}  errors")
    for t in threads_list:
        elapsed, _total, errs = _run_parallel(_phash_file, imgs, t, "phash")
        print(
            f"{t:>7}  {elapsed:>7.2f}s  {len(imgs) / elapsed:>8.1f}/s  "
            f"{elapsed / len(imgs) * 1000:>8.1f}ms  {len(errs)}"
        )


# --- MinIO ---
BENCH_BUCKET = "vlm-bench-tmp"


def _minio_client():
    import boto3
    from botocore.client import Config

    return boto3.client(
        "s3",
        endpoint_url="http://172.168.47.36:9002",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4", max_pool_connections=64),
    )


def _minio_put(args: tuple) -> tuple[int, str]:
    s3, path, key = args
    size = path.stat().st_size
    with path.open("rb") as fh:
        s3.put_object(Bucket=BENCH_BUCKET, Key=key, Body=fh)
    return size, key


def bench_minio(samples: list[Path], threads_list: list[int]) -> None:
    print("\n=== MinIO PUT (staging :9002, vlm-bench-tmp) ===")
    s3 = _minio_client()
    try:
        s3.create_bucket(Bucket=BENCH_BUCKET)
    except Exception:  # already exists
        pass

    try:
        print(f"{'threads':>7}  {'elapsed':>8}  {'throughput':>12}  {'per-file':>10}  errors")
        for t in threads_list:
            # 키 유일하게: <t>/<idx>/<basename>
            items = [(s3, p, f"{t}/{i}/{p.name}") for i, p in enumerate(samples)]
            t0 = time.perf_counter()
            errors = []
            total = 0
            if t == 1:
                for it in items:
                    try:
                        n, _ = _minio_put(it)
                        total += n
                    except Exception as e:
                        errors.append(e)
            else:
                with ThreadPoolExecutor(max_workers=t) as ex:
                    for fut in ex.map(_minio_put, items):
                        total += fut[0]
            elapsed = time.perf_counter() - t0
            print(
                f"{t:>7}  {elapsed:>7.2f}s  {fmt_rate(total, elapsed):>12}  "
                f"{elapsed / len(samples) * 1000:>8.1f}ms  {len(errors)}"
            )
    finally:
        # cleanup: 버킷 비우기
        print("  cleanup: bench 버킷 비우는 중 ...")
        try:
            objs = []
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BENCH_BUCKET):
                for o in page.get("Contents", []):
                    objs.append({"Key": o["Key"]})
            while objs:
                batch, objs = objs[:1000], objs[1000:]
                s3.delete_objects(Bucket=BENCH_BUCKET, Delete={"Objects": batch})
            print("  bench 버킷 비움 완료")
        except Exception as e:
            print(f"  cleanup 실패: {e}")


# --- DuckDB ---
DUCKDB_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_files (
    asset_id VARCHAR PRIMARY KEY,
    source_path VARCHAR,
    original_name VARCHAR,
    media_type VARCHAR,
    file_size BIGINT,
    checksum VARCHAR,
    phash VARCHAR,
    raw_bucket VARCHAR,
    raw_key VARCHAR,
    ingest_batch_id VARCHAR,
    transfer_tool VARCHAR,
    ingest_status VARCHAR,
    source_unit_name VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
"""


def _duckdb_writer_proc(db_path: str, rows_per_proc: int, proc_id: int, results_file: str) -> None:
    """INSERT N rows. 실패 카운트 + 총 elapsed 기록."""
    import duckdb

    ok = 0
    locked = 0
    other_err = 0
    t0 = time.perf_counter()
    for i in range(rows_per_proc):
        tries = 0
        while True:
            tries += 1
            try:
                con = duckdb.connect(db_path)
                con.execute(
                    "INSERT INTO raw_files (asset_id, source_path, file_size, ingest_status, created_at) VALUES (?, ?, ?, 'completed', now())",
                    [f"p{proc_id}_r{i}", f"/bench/p{proc_id}/r{i}", i * 1024],
                )
                con.close()
                ok += 1
                break
            except duckdb.Error as e:
                msg = str(e).lower()
                if "lock" in msg or "conflict" in msg:
                    locked += 1
                    if tries < 5:
                        time.sleep(0.05 * tries)
                        continue
                else:
                    other_err += 1
                break
    elapsed = time.perf_counter() - t0
    with open(results_file, "a", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {"proc_id": proc_id, "ok": ok, "locked": locked, "other_err": other_err, "elapsed": elapsed}
            )
            + "\n"
        )


def bench_duckdb(procs_list: list[int], rows_per_proc: int) -> None:
    print(f"\n=== DuckDB concurrent write (procs, {rows_per_proc} inserts each) ===")
    print(f"{'procs':>5}  {'elapsed':>8}  {'total_ok':>9}  {'locked':>7}  {'other_err':>9}  inserts/s")

    for p in procs_list:
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp:
            db_path = tmp.name
        # tempfile 이 만든 0바이트 파일은 DuckDB 가 "잘못된 포맷"으로 reject. 삭제 후 fresh create.
        os.unlink(db_path)
        results_file = db_path + ".results"
        try:
            # 스키마 초기화
            import duckdb

            con = duckdb.connect(db_path)
            con.execute(DUCKDB_SCHEMA)
            con.close()

            Path(results_file).write_text("")  # clear
            # 프로세스들 spawn
            t0 = time.perf_counter()
            procs = []
            for i in range(p):
                proc = subprocess.Popen(
                    [
                        sys.executable,
                        __file__,
                        "--_duckdb_worker",
                        db_path,
                        str(rows_per_proc),
                        str(i),
                        results_file,
                    ]
                )
                procs.append(proc)
            for proc in procs:
                proc.wait()
            elapsed = time.perf_counter() - t0

            # results aggregate
            total_ok = 0
            total_locked = 0
            total_other = 0
            with open(results_file, encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    r = json.loads(line)
                    total_ok += r["ok"]
                    total_locked += r["locked"]
                    total_other += r["other_err"]
            rate = total_ok / elapsed if elapsed > 0 else 0
            print(
                f"{p:>5}  {elapsed:>7.2f}s  {total_ok:>9}  {total_locked:>7}  {total_other:>9}  {rate:>8.1f}/s"
            )
        finally:
            for ext in ("", ".wal", ".results"):
                try:
                    os.unlink(db_path + ext)
                except FileNotFoundError:
                    pass


def main() -> int:
    # 하위 worker 모드
    if len(sys.argv) > 1 and sys.argv[1] == "--_duckdb_worker":
        _duckdb_writer_proc(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5])
        return 0

    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=100, help="샘플 파일 수 (기본 100)")
    ap.add_argument("--threads", default="1,2,4,8", help="스레드 구성 (콤마 구분)")
    ap.add_argument("--procs", default="1,2,4,6", help="DuckDB 프로세스 구성")
    ap.add_argument("--duckdb-rows", type=int, default=200, help="DuckDB 각 프로세스당 insert 수")
    ap.add_argument(
        "--only",
        default="nas,checksum,phash,minio,duckdb",
        help="실행 단계 콤마 구분 (기본: 전체)",
    )
    ap.add_argument("--sample-root", default=str(SAMPLE_ROOT), help="샘플 루트 (기본 staging archive)")
    args = ap.parse_args()

    threads_list = [int(x) for x in args.threads.split(",") if x.strip()]
    procs_list = [int(x) for x in args.procs.split(",") if x.strip()]
    stages = {s.strip() for s in args.only.split(",") if s.strip()}
    sample_root = Path(args.sample_root)

    print(f"sample_root={sample_root}")
    print(f"stages={sorted(stages)}  threads={threads_list}  procs={procs_list}")

    need_samples = stages & {"nas", "checksum", "phash", "minio"}
    samples: list[Path] = []
    if need_samples:
        print("\n--- 샘플 추출 ---")
        samples = pick_samples(sample_root, args.sample)
        if not samples:
            print("샘플 없음, 종료")
            return 1

    if "nas" in stages:
        bench_nas(samples, threads_list)
    if "checksum" in stages:
        bench_checksum(samples, threads_list)
    if "phash" in stages:
        bench_phash(samples, threads_list)
    if "minio" in stages:
        # boto3 의존성 확인
        try:
            import boto3  # noqa: F401
        except ImportError:
            print("\n=== MinIO: boto3 미설치, skip (pip install boto3) ===")
        else:
            bench_minio(samples, threads_list)
    if "duckdb" in stages:
        try:
            import duckdb  # noqa: F401
        except ImportError:
            print("\n=== DuckDB: duckdb 미설치, skip (pip install duckdb) ===")
        else:
            bench_duckdb(procs_list, args.duckdb_rows)

    print("\n=== done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
