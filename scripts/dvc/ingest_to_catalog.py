#!/usr/bin/env python3
"""DVC bare-repo rev → dataset_catalog INSERT (호스트 독립 실행, dagster 무관).

post-receive 훅이 push 된 각 rev 로 이걸 호출 → 커밋 메시지/작성자/날짜 + .dvc 포인터(md5/size)
+ MinIO 객체 존재여부를 묶어 dataset_catalog 에 1행 INSERT (멱등). 그래서 git push = 테이블 insert,
git pull/dataset_pull = 테이블 기반 버전 해석이 된다 — Dagster 재기동/센서 없이.

설계상 _run_catalog_ingest(defs/train/catalog_ingest.py) 와 동일 동작이지만, git 훅에서 가볍게
돌도록 **순수 lib(dvc_git/dvc_catalog) 만 import + psycopg2/boto3 직접** (dagster import 회피).
# ponytail: insert SQL 은 resources.postgres_train.insert_catalog_row 와 동일 컬럼 — 훅 경량화 위해 inline.

env: DATAOPS_POSTGRES_DSN, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
     DVC_DATA_REPO_ID(기본 dvc-datasets), DVC_CATALOG_DEFAULT_TASK(기본 sam3_detection),
     DVC_CATALOG_DEFAULT_DATASET(기본 uncategorized)
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid

_REPO_SRC = os.path.join(os.path.dirname(__file__), "..", "..", "src")
sys.path.insert(0, _REPO_SRC)

# 순수 L1 lib (yaml/subprocess 만 의존 — dagster 안 끌어옴)
from vlm_pipeline.lib.dvc_catalog import dvc_remote_url_for, parse_dvc_pointer  # noqa: E402
from vlm_pipeline.lib.dvc_git import git_log_meta, list_dvc_files_at_rev, read_file_at_rev  # noqa: E402

_INSERT_SQL = """
INSERT INTO dataset_catalog (
    dataset_catalog_id, task, dataset_name, status,
    data_repo_id, data_repo_url, git_rev, git_short_rev, git_ref, git_tag,
    commit_subject, commit_message, commit_author_name, commit_author_email, committed_at,
    dvc_file_path, dvc_out_path, dvc_md5, dvc_size_bytes, dvc_nfiles,
    dvc_remote_name, dvc_remote_url
) VALUES (
    %(dataset_catalog_id)s, %(task)s, %(dataset_name)s, %(status)s,
    %(data_repo_id)s, %(data_repo_url)s, %(git_rev)s, %(git_short_rev)s, %(git_ref)s, %(git_tag)s,
    %(commit_subject)s, %(commit_message)s, %(commit_author_name)s, %(commit_author_email)s, %(committed_at)s,
    %(dvc_file_path)s, %(dvc_out_path)s, %(dvc_md5)s, %(dvc_size_bytes)s, %(dvc_nfiles)s,
    %(dvc_remote_name)s, %(dvc_remote_url)s
)
ON CONFLICT (data_repo_id, git_rev, dvc_file_path, dvc_out_path) DO UPDATE
    SET status = EXCLUDED.status
    WHERE dataset_catalog.status = 'pending_missing_dvc_objects'
"""
# ↑ Codex BUG2: DO NOTHING 이면 일시적 MinIO 체크 실패로 박힌 pending 이 영구 고정.
#   pending 행에 한해 재인제스트 시 status 복구 (available/pinned 행은 WHERE 로 보호).


def _object_exists(out_path: str) -> bool:
    """MinIO vlm-dataset/_dvc/<out> 존재? 오류/부재 → False (status=pending). fail-soft."""
    try:
        import boto3

        cli = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://10.0.0.51:9000"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", ""),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", ""),
        )
        cli.head_object(Bucket="vlm-dataset", Key=f"_dvc/{out_path.strip('/')}")
        return True
    except Exception:  # noqa: BLE001 — 부재/오류 모두 pending 으로
        return False


def ingest_rev(conn, *, repo_path: str, rev: str, data_repo_id: str, task: str, dataset_name: str,
               git_ref=None, only=None):
    """only: 이 .dvc 경로들만 인덱싱 (push 시 '변경된 것만' → 미변경 데이터셋 행 폭발 방지).
    None 이면 rev 의 모든 .dvc (reconciliation 백필용)."""
    meta = git_log_meta(repo_path, rev)
    inserted = 0
    any_missing = False
    files = list_dvc_files_at_rev(repo_path, rev)
    if only:
        keep = set(only)
        files = [f for f in files if f in keep]
    for dvc_file in files:
        pointer = parse_dvc_pointer(dvc_file, read_file_at_rev(repo_path, rev, dvc_file))
        for out in pointer.outs:
            present = _object_exists(out.path)
            any_missing = any_missing or (not present)
            row = {
                "dataset_catalog_id": str(uuid.uuid4()),
                "task": task, "dataset_name": dataset_name,
                "status": "available" if present else "pending_missing_dvc_objects",
                "data_repo_id": data_repo_id, "data_repo_url": repo_path,
                "git_rev": meta.git_rev, "git_short_rev": meta.git_short_rev,
                "git_ref": git_ref, "git_tag": None,
                "commit_subject": meta.commit_subject, "commit_message": meta.commit_message,
                "commit_author_name": meta.commit_author_name, "commit_author_email": meta.commit_author_email,
                "committed_at": meta.committed_at,
                "dvc_file_path": dvc_file, "dvc_out_path": out.path, "dvc_md5": out.md5,
                "dvc_size_bytes": out.size, "dvc_nfiles": out.nfiles,
                "dvc_remote_name": "minio", "dvc_remote_url": dvc_remote_url_for(out.path),
            }
            with conn.cursor() as cur:
                cur.execute(_INSERT_SQL, row)
            inserted += 1
    conn.commit()
    return {"inserted": inserted, "missing_objects": any_missing, "rev": meta.git_short_rev,
            "subject": meta.commit_subject}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="DVC rev → dataset_catalog INSERT (commit msg 포함)")
    p.add_argument("--repo", required=True, help="bare repo 경로")
    p.add_argument("--rev", required=True, help="ingest 할 git rev")
    p.add_argument("--git-ref", default=None)
    p.add_argument("--only", default=None,
                   help="콤마구분 .dvc 경로 — 변경된 것만 인덱싱 (생략 시 rev 의 전체 .dvc)")
    p.add_argument("--data-repo-id", default=os.environ.get("DVC_DATA_REPO_ID", "dvc-datasets"))
    p.add_argument("--task", default=os.environ.get("DVC_CATALOG_DEFAULT_TASK", "sam3_detection"))
    p.add_argument("--dataset-name", default=os.environ.get("DVC_CATALOG_DEFAULT_DATASET", "uncategorized"))
    a = p.parse_args(argv)

    dsn = os.environ.get("DATAOPS_POSTGRES_DSN")
    if not dsn:
        print("[ingest] DATAOPS_POSTGRES_DSN 미설정 — skip (fail-soft)", file=sys.stderr)
        return 0  # 훅이 push 를 막지 않도록 fail-soft
    try:
        import psycopg2

# ponytail: --only 는 콤마 join (post-receive). 이 파이프라인 .dvc 명명(data/<name>.dvc)엔 공백/콤마
# 없음 → 충분. 경로에 공백/콤마가 생기면 null-delim 채널로 (Codex BUG5, 현 명명상 미발생).
        only = [s for s in (a.only or "").replace(",", " ").split() if s] or None
        conn = psycopg2.connect(dsn)
        try:
            out = ingest_rev(conn, repo_path=a.repo, rev=a.rev, data_repo_id=a.data_repo_id,
                             task=a.task, dataset_name=a.dataset_name, git_ref=a.git_ref, only=only)
        finally:
            conn.close()
        print(f"[ingest] rev={out['rev']} subject={out['subject']!r} inserted={out['inserted']} "
              f"missing_objects={out['missing_objects']}")
        return 0
    except Exception as exc:  # noqa: BLE001 — 훅 fail-soft (push 차단 금지). reconciliation 이 backstop.
        print(f"[ingest] 실패(fail-soft, push 는 계속): {exc!r}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
