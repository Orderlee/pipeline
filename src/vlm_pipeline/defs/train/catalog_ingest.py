"""defs/train/catalog_ingest — DVC bare-repo → dataset_catalog ingestion (L4).

post-receive 훅이 단일 rev 로 op 을 호출(운영 시)하고, reconciliation 센서가 git log 를
스캔해 누락분을 backstop 한다(멱등 — dataset_catalog UNIQUE). MinIO 객체 존재를 검증해서만
status='available', 누락이면 'pending_missing_dvc_objects' (dvc push ≠ git commit 갭).
"""
from __future__ import annotations

import os
import uuid

from dagster import DefaultSensorStatus, SkipReason, sensor

from vlm_pipeline.lib.dvc_catalog import dvc_remote_url_for, parse_dvc_pointer
from vlm_pipeline.lib.dvc_git import git_log_meta, iter_recent_revs, list_dvc_files_at_rev, read_file_at_rev
from vlm_pipeline.lib.env_utils import int_env

_DEFAULT_TASK = "sam3_detection"


def _object_exists(minio, out_path: str) -> bool:
    key = f"_dvc/{out_path.strip('/')}"
    try:
        return bool(minio.exists("vlm-dataset", key))
    except Exception:  # noqa: BLE001 — treat probe error as "not present" → pending
        return False


def _run_catalog_ingest(db, minio, *, repo_path, data_repo_id, rev, dataset_name, task, log) -> dict:
    """Ingest one git rev into dataset_catalog. Verifies MinIO objects before 'available'."""
    meta = git_log_meta(repo_path, rev)
    dvc_files = list_dvc_files_at_rev(repo_path, rev)
    ingested: list[str] = []
    any_missing = False

    for dvc_file in dvc_files:
        pointer = parse_dvc_pointer(dvc_file, read_file_at_rev(repo_path, rev, dvc_file))
        for out in pointer.outs:
            present = _object_exists(minio, out.path)
            if not present:
                any_missing = True
            status = "available" if present else "pending_missing_dvc_objects"
            row = {
                "dataset_catalog_id": str(uuid.uuid4()),
                "task": task,
                "dataset_name": dataset_name,
                "status": status,
                "data_repo_id": data_repo_id,
                "data_repo_url": repo_path,
                "git_rev": meta.git_rev,
                "git_short_rev": meta.git_short_rev,
                "git_ref": None,
                "git_tag": None,
                "commit_subject": meta.commit_subject,
                "commit_message": meta.commit_message,
                "commit_author_name": meta.commit_author_name,
                "commit_author_email": meta.commit_author_email,
                "committed_at": meta.committed_at,
                "dvc_file_path": dvc_file,
                "dvc_out_path": out.path,
                "dvc_md5": out.md5,
                "dvc_size_bytes": out.size,
                "dvc_nfiles": out.nfiles,
                "dvc_remote_name": "minio",
                "dvc_remote_url": dvc_remote_url_for(out.path),
                "train_dataset_version_id": None,
                "content_checksum": None,
                "mlflow_run_id": None,
            }
            cid = db.insert_catalog_row(row)
            ingested.append(cid)
            if not present:
                log.warning(
                    f"[dvc-catalog] rev={meta.git_short_rev} out={out.path} "
                    "MinIO object MISSING — status=pending_missing_dvc_objects (dvc push needed)"
                )

    overall = "pending_missing_dvc_objects" if any_missing else "available"
    log.info(
        f"[dvc-catalog] ingested rev={meta.git_short_rev} files={len(dvc_files)} "
        f"rows={len(ingested)} status={overall}"
    )
    return {"ingested": ingested, "missing_objects": any_missing, "status": overall}


@sensor(
    minimum_interval_seconds=int_env("DVC_CATALOG_RECONCILE_INTERVAL_SEC", 600, 60),
    default_status=DefaultSensorStatus.STOPPED,
    description="DVC bare repo git log 스캔 → dataset_catalog 누락 backstop (멱등)",
    required_resource_keys={"db", "minio"},
)
def dataset_catalog_reconciliation_sensor(context):
    # TODO(mlops-audit L-3): DVC Tier-2 실운영 시 — compose dagster 서비스에 bare repo
    # (/srv/data-repos/dvc-datasets.git) bind-mount + DVC_DATA_REPO_PATH env + git safe.directory,
    # 그리고 이 센서를 Dagster UI 에서 enable(기본 STOPPED). 재기동 동반 → 정비 윈도우.
    # 검증: 지연 dvc push → tick 가 pending_missing_dvc_objects→available(H-4 self-heal) 전이.
    # docs/pipeline-flow-audit-2026-07-01.md §추후작업 L-3.
    repo_path = os.environ.get("DVC_DATA_REPO_PATH", "").strip()
    if not repo_path or not os.path.isdir(repo_path):
        return SkipReason(f"DVC_DATA_REPO_PATH 미설정/부재: {repo_path!r}")
    data_repo_id = os.environ.get("DVC_DATA_REPO_ID", "dvc-datasets")
    task = os.environ.get("DVC_CATALOG_DEFAULT_TASK", _DEFAULT_TASK)
    limit = int_env("DVC_CATALOG_RECONCILE_REVS", 50, 1)

    try:
        revs = iter_recent_revs(repo_path, limit)
    except Exception as exc:  # noqa: BLE001
        return SkipReason(f"git log 스캔 실패: {exc}")

    ingested_total = 0
    for rev in revs:
        try:
            out = _run_catalog_ingest(
                context.resources.db,
                context.resources.minio,
                repo_path=repo_path,
                data_repo_id=data_repo_id,
                rev=rev,
                dataset_name=os.environ.get("DVC_CATALOG_DEFAULT_DATASET", "uncategorized"),
                task=task,
                log=context.log,
            )
            ingested_total += len(out["ingested"])
        except Exception as exc:  # noqa: BLE001 — per-rev fail-forward
            context.log.warning(f"[dvc-catalog] rev={rev[:12]} ingest 실패: {exc}")

    return SkipReason(
        f"dataset_catalog reconciliation 완료: revs={len(revs)} rows_touched={ingested_total} "
        "(UNIQUE 로 멱등 — 기존 행은 no-op)"
    )
