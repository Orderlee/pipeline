#!/usr/bin/env python3
"""`raw_files.checksum` 이 NULL 인 행을 MinIO 객체 바이트에서 백필한다.

  python scripts/backfill_checksums_from_minio.py --limit 20          # dry-run 표본
  python scripts/backfill_checksums_from_minio.py                     # dry-run 전수
  python scripts/backfill_checksums_from_minio.py --apply

배경 (2026-07-29 reconciliation 측정에서 발견):
  `raw_files` 129,970행 중 **694행의 checksum 이 NULL** 이다. 전부 `source-b-202512`
  prefix, 2026-03-05 하루치, `source_unit_name` 도 비어 있는 단일 코호트다.
  MinIO 객체는 정상 존재한다(표본 200/200) — 객체 결손이 아니라 **메타데이터 결손**이다.

왜 문제인가:
  `raw_files.checksum` 에는 UNIQUE 제약이 걸려 있고 이것이 정확-중복 판정의 1차 방어선이다.
  NULL 은 UNIQUE 를 비껴가므로, 이 694건은 **중복 검출에서 통째로 빠져 있다**
  (같은 파일이 다시 들어와도 막히지 않는다).

왜 기존 `recompute_archive_checksums.py` 를 못 쓰는가:
  그 스크립트는 NAS archive 파일을 읽는데, 이 코호트의 `archive_path` 는 구 마운트 경로
  (`/nas/archive/...`)이고 현행 경로에도 실물이 없다. **살아있는 사본은 MinIO 뿐**이다.

안전장치:
  - 기본 dry-run. `--apply` 없이는 아무것도 쓰지 않는다.
  - 다운로드한 바이트 수가 `file_size` 와 다르면 그 행은 건너뛴다(부분 다운로드 오탐 방지).
  - 계산된 checksum 이 **다른 asset_id 에 이미 존재**하면 UNIQUE 위반이므로 쓰지 않고
    중복 후보로 보고한다 — 이건 오류가 아니라 이 백필이 드러내려던 바로 그 정보다.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vlm_pipeline.lib.checksum import sha256_stream  # noqa: E402

RAW_BUCKET = "vlm-raw"


def _open_db():
    from vlm_pipeline.resources.postgres import PostgresResource  # lazy

    dsn = os.environ.get("DATAOPS_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("DATAOPS_POSTGRES_DSN not set")
    return PostgresResource(dsn=dsn)


def _open_minio():
    from minio import Minio  # lazy

    endpoint = os.environ.get("MINIO_ENDPOINT", "")
    if not endpoint:
        raise SystemExit("MINIO_ENDPOINT not set")
    return Minio(
        endpoint.replace("http://", "").replace("https://", ""),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=endpoint.startswith("https://"),
    )


def _fetch_null_checksum_rows(db, prefix: str | None, limit: int | None) -> list[dict]:
    sql = "SELECT asset_id, raw_key, file_size FROM raw_files WHERE checksum IS NULL"
    params: list = []
    if prefix:
        sql += " AND raw_key LIKE %s"
        params.append(f"{prefix}%")
    sql += " ORDER BY created_at"
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return [{"asset_id": r[0], "raw_key": r[1], "file_size": r[2]} for r in cur.fetchall()]


def _existing_checksum_owner(db, checksum: str) -> str | None:
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT asset_id FROM raw_files WHERE checksum = %s LIMIT 1", (checksum,))
        row = cur.fetchone()
        return row[0] if row else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prefix", help="raw_key prefix 로 대상 제한 (예: source-b-202512/)")
    parser.add_argument("--limit", type=int, help="처리할 최대 행 수 (표본 확인용)")
    parser.add_argument("--bucket", default=RAW_BUCKET)
    parser.add_argument("--apply", action="store_true", help="실제 UPDATE 수행 (기본 dry-run)")
    args = parser.parse_args(argv)

    db = _open_db()
    minio = _open_minio()

    rows = _fetch_null_checksum_rows(db, args.prefix, args.limit)
    print(f"[backfill] checksum IS NULL 대상 {len(rows)}행 (prefix={args.prefix!r}, limit={args.limit})")

    computed: list[tuple[str, str]] = []  # (asset_id, checksum)
    size_mismatch, missing_object, duplicates, errors = [], [], [], []

    for row in rows:
        try:
            response = minio.get_object(args.bucket, row["raw_key"])
        except Exception as exc:  # noqa: BLE001 — per-row fail-forward
            missing_object.append((row["raw_key"], str(exc)[:80]))
            continue
        try:
            digest, nbytes = sha256_stream(response)
        except Exception as exc:  # noqa: BLE001
            errors.append((row["raw_key"], str(exc)[:80]))
            continue
        finally:
            response.close()
            response.release_conn()

        if row["file_size"] is not None and nbytes != row["file_size"]:
            size_mismatch.append((row["raw_key"], nbytes, row["file_size"]))
            continue

        owner = _existing_checksum_owner(db, digest)
        if owner is not None and owner != row["asset_id"]:
            duplicates.append((row["raw_key"], digest, owner))
            continue
        computed.append((row["asset_id"], digest))

    print(
        f"[backfill] 백필 가능 {len(computed)} / 중복(UNIQUE 충돌) {len(duplicates)} / "
        f"크기 불일치 {len(size_mismatch)} / 객체 없음 {len(missing_object)} / 오류 {len(errors)}"
    )
    for key, digest, owner in duplicates[:5]:
        print(f"   [dup] {key} sha256={digest[:16]}… 이미 asset_id={owner} 가 보유", file=sys.stderr)
    for key, got, want in size_mismatch[:5]:
        print(f"   [size] {key}: downloaded={got} db={want}", file=sys.stderr)
    for key, err in (missing_object + errors)[:5]:
        print(f"   [err] {key}: {err}", file=sys.stderr)

    if not args.apply:
        for asset_id, digest in computed[:5]:
            print(f"   DRY-RUN set checksum: asset_id={asset_id} -> {digest[:16]}…")
        print("[backfill] DRY-RUN — 아무것도 변경하지 않았다 (--apply 로 실행).")
        return 0

    if computed:
        with db.connect() as conn, conn.cursor() as cur:
            cur.executemany(
                "UPDATE raw_files SET checksum = %s WHERE asset_id = %s AND checksum IS NULL",
                [(digest, asset_id) for asset_id, digest in computed],
            )
            print(f"[backfill] checksum 기록 {cur.rowcount if cur.rowcount is not None else len(computed)}행")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
