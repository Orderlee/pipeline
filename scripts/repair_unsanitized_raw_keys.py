#!/usr/bin/env python3
"""MinIO 객체가 비정규(unsanitized) 키로 올라가 raw_files.raw_key 와 어긋난 행을 복구한다.

  python scripts/repair_unsanitized_raw_keys.py --source-prefix 'source-h/'            # dry-run
  python scripts/repair_unsanitized_raw_keys.py --source-prefix 'source-h/' --apply
  python scripts/repair_unsanitized_raw_keys.py --source-prefix 'source-h/' --apply --mark-completed

배경 (2026-07-29 reconciliation 측정에서 발견):
  raw_files 129,970행 중 871행이 3개월 넘게 `uploading` 에 고착돼 있었다. 전부
  source_unit_name='source-h', 2026-04-16 03:38~04:14 의 36분 창. 원인은 업로드 실패가 아니라
  **키 불일치** 였다 —

    MinIO 실제 객체 : source-h/<카테고리>/<원본 한글 파일명>.mp4      (871개)
    DB raw_key      : source-h/<카테고리>/<sanitize 된 로마자명>.mp4  (871행)

  `lib/env_utils.py` 주석이 명시하듯 INGEST 의 raw_key 는 `sanitize_path_component` 기준이
  정본이므로, DB 가 맞고 객체 키가 비정규다. sanitizer 는 결정적이라 객체 → DB 행 매핑이
  **871/871 완전 일치**함을 확인했다. 따라서 복구는 재수집이 아니라 **버킷 내 서버사이드 복사**로
  끝난다 (바이트 재업로드 없음, NAS 접근 없음).

  NAS 원본은 이미 없다 (source_path 가 구 마운트 경로 `/nas/incoming/...` 이고 archive 에도
  source-h 없음). 즉 **살아있는 사본은 MinIO 의 비정규 키 객체뿐**이라, 검증 전까지 원본을 지우면
  안 된다. 이 스크립트는 복사만 하고 원본을 삭제하지 않는다.

`uploading` 이 dedup·build·labeling 쿼리(`ingest_status='completed'`)를 전부 게이트하므로,
이 871개 영상은 현재 라벨링 파이프라인에서 통째로 빠져 있다.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vlm_pipeline.lib.sanitizer import sanitize_filename, sanitize_path_component  # noqa: E402

RAW_BUCKET = "vlm-raw"


def sanitized_key(object_name: str) -> str:
    """비정규 객체 키 → INGEST 정본 raw_key (마지막 컴포넌트만 파일명 규칙 적용)."""
    parts = [p for p in object_name.split("/") if p]
    if not parts:
        return ""
    *dirs, filename = parts
    return "/".join([sanitize_path_component(d) for d in dirs] + [sanitize_filename(filename)])


def _open_db():
    from vlm_pipeline.resources.postgres import PostgresResource  # lazy: --help 를 가볍게

    dsn = os.environ.get("DATAOPS_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("DATAOPS_POSTGRES_DSN not set")
    return PostgresResource(dsn=dsn)


def _open_minio():
    from minio import Minio  # lazy

    endpoint = os.environ.get("MINIO_ENDPOINT", "")
    if not endpoint:
        raise SystemExit("MINIO_ENDPOINT not set")
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        host,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=endpoint.startswith("https://"),
    )


def _fetch_pending_rows(db, status: str) -> dict[str, dict]:
    """{raw_key: row} — 복구 대상 상태의 행만."""
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT asset_id, raw_key, file_size FROM raw_files WHERE ingest_status = %s",
            (status,),
        )
        return {r[1]: {"asset_id": r[0], "raw_key": r[1], "file_size": r[2]} for r in cur.fetchall()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-prefix", required=True, help="비정규 키 prefix (예: 'source-h/')")
    parser.add_argument("--status", default="uploading", help="복구 대상 ingest_status (기본 uploading)")
    parser.add_argument("--bucket", default=RAW_BUCKET)
    parser.add_argument("--apply", action="store_true", help="실제 서버사이드 복사 수행 (기본 dry-run)")
    parser.add_argument(
        "--mark-completed",
        action="store_true",
        help=(
            "복사 성공 행을 ingest_status='completed' 로 전이. --apply 와 함께만 동작. "
            "⚠️ CLAUDE.md 의 'archive 이동 완료된 파일만 completed' 규칙과 어긋난다 — "
            "이 코호트는 NAS 원본이 이미 없어 archive 이동이 불가능하다. 운영자가 그 사실을 "
            "알고 선택하는 경우에만 사용할 것."
        ),
    )
    args = parser.parse_args(argv)

    db = _open_db()
    minio = _open_minio()

    rows = _fetch_pending_rows(db, args.status)
    objects = list(minio.list_objects(args.bucket, prefix=args.source_prefix, recursive=True))
    print(f"[repair] status={args.status} rows={len(rows)}  prefix={args.source_prefix!r} objects={len(objects)}")

    matched, size_mismatch, already, unmatched = [], [], [], []
    for obj in objects:
        target = sanitized_key(obj.object_name)
        row = rows.get(target)
        if row is None:
            unmatched.append(obj.object_name)
            continue
        if row["file_size"] is not None and obj.size != row["file_size"]:
            size_mismatch.append((obj.object_name, obj.size, row["file_size"]))
            continue
        try:
            minio.stat_object(args.bucket, target)
            already.append(target)
            continue
        except Exception:  # noqa: BLE001 — 없으면 복사 대상
            pass
        matched.append((obj.object_name, target, row["asset_id"]))

    print(
        f"[repair] 복사 대상 {len(matched)} / 이미 존재 {len(already)} / "
        f"크기 불일치 {len(size_mismatch)} / DB 행 없음 {len(unmatched)}"
    )
    for name, got, want in size_mismatch[:5]:
        print(f"   [size] {name}: object={got} db={want}", file=sys.stderr)
    for name in unmatched[:5]:
        print(f"   [no-row] {name}", file=sys.stderr)

    if not args.apply:
        for src, dst, _aid in matched[:5]:
            print(f"   DRY-RUN copy: {src}  ->  {dst}")
        print("[repair] DRY-RUN — 아무것도 변경하지 않았다 (--apply 로 실행).")
        return 0

    from minio.commonconfig import CopySource  # lazy

    copied_ids, failed = [], 0
    for src, dst, asset_id in matched:
        try:
            minio.copy_object(args.bucket, dst, CopySource(args.bucket, src))
        except Exception as exc:  # noqa: BLE001 — per-object fail-forward
            failed += 1
            print(f"   [copy-failed] {src} -> {dst}: {exc}", file=sys.stderr)
            continue
        copied_ids.append(asset_id)

    # 상태 전이는 복사가 끝난 뒤 한 번에. connect() 는 블록 종료 시 자동 commit 한다.
    if args.mark_completed and copied_ids:
        with db.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE raw_files SET ingest_status='completed' WHERE asset_id = ANY(%s) AND ingest_status = %s",
                (copied_ids, args.status),
            )
            print(f"[repair] ingest_status → completed: {cur.rowcount} 행")

    print(f"[repair] 복사 완료 {len(copied_ids)}, 실패 {failed}. 원본({args.source_prefix})은 삭제하지 않았다 — 검증 후 수동 정리.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
