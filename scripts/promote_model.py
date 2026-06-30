#!/usr/bin/env python3
"""모델 가중치 승격/롤백 스크립트 (MLOps 스캐폴딩 — built-but-not-executed).

흐름 (promote):
  1. model_registry 에서 status='promotable' 행 선택 (--model-version-id 명시 or 최신 1개)
  2. checkpoint_key 를 MinIO → 호스트 모델 볼륨(./docker/data/models/<model>/) 다운로드
  3. artifact_checksum 검증 (불일치 → 중단, 아무것도 변경 안 함)
  4. 서빙 env 세팅(docker/.env) + docker compose up -d --force-recreate <svc>
  5. 레지스트리 status='promoted', promoted_at=now(), promoted_env 기록

흐름 (rollback): 직전 promoted 행 재승격 (2~4 동일) + 현 promoted → 'rolled_back'.

기본 --dry-run: MinIO 다운로드/checksum/recreate/DB write 전부 시뮬레이션만.
실제 승격은 --apply + 가중치 볼륨 있는 prod 박스에서만.

DSN: --dsn or DATAOPS_POSTGRES_DSN. MinIO: MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY.
레지스트리가 진실 (design §2) — 심볼릭링크 금지.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

REPO_ROOT = Path(__file__).resolve().parent.parent

if TYPE_CHECKING:
    from minio import Minio  # noqa: F401

# model → (serving 컨테이너 svc, 호스트 모델 디렉토리 basename, env 키, 로컬 파일 경로)
_SERVING = {
    "pe_core": {
        "service": "embedding-service",
        "host_subdir": "pe-core",
        "env_path_key": "EMBEDDING_CHECKPOINT_PATH",
        "env_version_key": "EMBEDDING_MODEL_VERSION",
        "container_path": "/data/models/pe-core/merged.pt",
    },
    "sam3": {
        "service": "sam3",
        "host_subdir": "sam3",
        "env_path_key": "SAM3_CHECKPOINT_PATH",
        "env_version_key": None,
        "container_path": "/models/sam3.1_multiplex.pt",
    },
}

_SELECT_COLS = (
    "model_version_id, model, version, train_dataset_version_id, "
    "checkpoint_key, artifact_checksum, status, promoted_env"
)

TRAINSET_BUCKET = "vlm-dataset"  # 5-bucket 정책: 새 버킷 없음, _models/ prefix (design §6)


class PromotionError(RuntimeError):
    """선택/검증/전이 실패 — 아무것도 변경하지 않고 중단."""


def _row_to_dict(cur, row) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def select_promotable_row(cur, *, model: str, model_version_id: int | None) -> dict[str, Any]:
    """status='promotable' 행 한 개를 선택. 명시 id 없으면 최신(created_at desc) 1개."""
    if model_version_id is not None:
        cur.execute(
            f"SELECT {_SELECT_COLS} FROM model_registry "
            "WHERE model_version_id = %(id)s AND status = 'promotable' AND model = %(m)s",
            {"id": model_version_id, "m": model},
        )
    else:
        cur.execute(
            f"SELECT {_SELECT_COLS} FROM model_registry "
            "WHERE status = 'promotable' AND model = %(m)s "
            "ORDER BY created_at DESC LIMIT 1",
            {"m": model},
        )
    row = _row_to_dict(cur, cur.fetchone())
    if not row:
        raise PromotionError(
            f"no 'promotable' model_registry row for model={model!r} id={model_version_id!r}"
        )
    return row


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download_and_verify(
    minio_client,
    *,
    checkpoint_key: str,
    artifact_checksum: str,
    dest_path: Path,
    dry_run: bool,
) -> Path:
    """MinIO vlm-dataset/<checkpoint_key> → dest_path 다운로드 + sha256 검증.

    dry_run 이면 다운로드/FS write 없이 계획된 dest_path 만 반환.
    checksum 불일치 시 받은 파일 삭제 후 PromotionError (서빙 가중치 손상 방지).
    """
    dest_path = Path(dest_path)
    if dry_run:
        print(f"[promote][dry-run] would download {TRAINSET_BUCKET}/{checkpoint_key} -> {dest_path}")
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    minio_client.fget_object(TRAINSET_BUCKET, checkpoint_key, str(dest_path))
    actual = _sha256_file(dest_path)
    if actual != artifact_checksum:
        try:
            dest_path.unlink()
        except OSError:
            pass
        raise PromotionError(
            f"artifact_checksum mismatch for {checkpoint_key}: "
            f"expected {artifact_checksum} got {actual} (downloaded file removed)"
        )
    print(f"[promote] verified {checkpoint_key} sha256={actual}")
    return dest_path


def promote_transition(cur, *, row: dict[str, Any], env: str) -> None:
    """선택 행을 promoted 로, 같은 model 의 기존 promoted 는 archived 로 전이."""
    model = row["model"]
    new_id = row["model_version_id"]
    # 1) 기존 promoted (있으면) → archived
    cur.execute(
        "UPDATE model_registry SET status = 'archived' "
        "WHERE model = %(m)s AND status = 'promoted' AND model_version_id <> %(id)s",
        {"m": model, "id": new_id},
    )
    # 2) 선택 행 → promoted
    cur.execute(
        "UPDATE model_registry SET status = 'promoted', promoted_at = now(), "
        "promoted_env = %(env)s WHERE model_version_id = %(id)s",
        {"env": env, "id": new_id},
    )


def select_rollback_target(cur, *, model: str) -> dict[str, Any]:
    """직전에 promoted 된 적 있는(=promoted_at not null) archived 행 중 최신 1개."""
    cur.execute(
        f"SELECT {_SELECT_COLS} FROM model_registry "
        "WHERE model = %(m)s AND status = 'archived' AND promoted_at IS NOT NULL "
        "ORDER BY promoted_at DESC LIMIT 1",
        {"m": model},
    )
    row = _row_to_dict(cur, cur.fetchone())
    if not row:
        raise PromotionError(f"no prior promoted (archived) row to roll back to for model={model!r}")
    return row


def rollback_transition(cur, *, restore_row: dict[str, Any], current_promoted_id: int, env: str) -> None:
    """현 promoted → rolled_back, restore_row → promoted."""
    cur.execute(
        "UPDATE model_registry SET status = 'rolled_back' WHERE model_version_id = %(id)s",
        {"id": current_promoted_id},
    )
    cur.execute(
        "UPDATE model_registry SET status = 'promoted', promoted_at = now(), "
        "promoted_env = %(env)s WHERE model_version_id = %(id)s",
        {"env": env, "id": restore_row["model_version_id"]},
    )


def docker_recreate(service: str, *, dry_run: bool) -> None:
    """서빙 컨테이너 재생성 (env 반영). dry_run 이면 명령만 출력."""
    cmd = [
        "docker", "compose", "-f", str(REPO_ROOT / "docker" / "docker-compose.yaml"),
        "up", "-d", "--force-recreate", service,
    ]
    if dry_run:
        print(f"[promote][dry-run] would run: {' '.join(cmd)}")
        return
    print(f"[promote] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(REPO_ROOT / "docker"))
    if getattr(result, "returncode", 0) != 0:
        raise PromotionError(f"docker recreate failed for {service} (rc={result.returncode})")


def _write_env_var(env_file: Path, key: str, value: str) -> None:
    """docker/.env 의 key=value 를 추가/갱신 (없으면 append). 레지스트리가 진실 — 심볼릭링크 금지."""
    lines: list[str] = []
    found = False
    if env_file.exists():
        lines = env_file.read_text().splitlines()
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={value}"
            found = True
            break
    if not found:
        lines.append(f"{key}={value}")
    env_file.write_text("\n".join(lines) + "\n")


def _make_minio():
    from minio import Minio

    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret_key = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
    secure = endpoint.startswith("https://")
    for scheme in ("http://", "https://"):
        if endpoint.startswith(scheme):
            endpoint = endpoint[len(scheme):]
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Promote/rollback a model_registry weight.")
    p.add_argument("--model", required=True, choices=sorted(_SERVING))
    p.add_argument("--model-version-id", type=int, default=None)
    p.add_argument("--env", choices=["prod", "staging"], default="prod")
    p.add_argument("--rollback", action="store_true", help="이전 promoted 행 재승격.")
    p.add_argument("--dsn", default=None, help="PG DSN. fallback DATAOPS_POSTGRES_DSN.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--dry-run", dest="dry_run", action="store_true", default=True)
    g.add_argument("--apply", dest="dry_run", action="store_false")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    serving = _SERVING[args.model]
    dsn = args.dsn or os.getenv("DATAOPS_POSTGRES_DSN")
    if not dsn:
        print("[promote] no DSN (--dsn or DATAOPS_POSTGRES_DSN)", file=sys.stderr)
        return 2

    import psycopg2

    host_dir = REPO_ROOT / "docker" / "data" / "models" / serving["host_subdir"]
    dest = host_dir / "merged.pt" if args.model == "pe_core" else host_dir / "sam3.1_multiplex.pt"
    env_file = REPO_ROOT / "docker" / ".env"

    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if args.rollback:
                restore = select_rollback_target(cur, model=args.model)
                cur.execute(
                    "SELECT model_version_id FROM model_registry "
                    "WHERE model = %(m)s AND status = 'promoted' LIMIT 1",
                    {"m": args.model},
                )
                cur_prom = cur.fetchone()
                row = restore
            else:
                row = select_promotable_row(
                    cur, model=args.model, model_version_id=args.model_version_id
                )

            print(f"[promote] target row: id={row['model_version_id']} version={row.get('version')}")
            mc = None if args.dry_run else _make_minio()
            download_and_verify(
                mc,
                checkpoint_key=row["checkpoint_key"],
                artifact_checksum=row["artifact_checksum"],
                dest_path=dest,
                dry_run=args.dry_run,
            )

            if not args.dry_run:
                _write_env_var(env_file, serving["env_path_key"], serving["container_path"])
                if serving["env_version_key"]:
                    _write_env_var(env_file, serving["env_version_key"], str(row.get("version") or ""))

            docker_recreate(serving["service"], dry_run=args.dry_run)

            if args.dry_run:
                print("[promote][dry-run] would commit registry transition; rolling back (no change)")
                conn.rollback()
                return 0

            if args.rollback:
                rollback_transition(
                    cur, restore_row=row,
                    current_promoted_id=(cur_prom[0] if cur_prom else -1), env=args.env,
                )
            else:
                promote_transition(cur, row=row, env=args.env)
        conn.commit()
        print(f"[promote] committed: model={args.model} -> promoted ({args.env})")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
