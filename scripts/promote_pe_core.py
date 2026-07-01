#!/usr/bin/env python3
"""PE-Core champion-challenger 승격 — 포인터 전환 + partial-HNSW + 서빙 교체 (design §8.1).

만들되 미실행 (--dry-run 기본). SAM 의 .pt 교체에 대응하는 PE 동작:
  1) (선결) 재임베딩(H3, Dagster reembed_under_version) 으로 새 model_name 커버리지 확보
  2) partial HNSW 빌드 (H2c create_model_partial_hnsw / lib.pgvector_index)
  3) embedding_active_model 포인터 원자 전환 (flip_pointer)  ← AL/검색이 즉시 새 model_name read
  4) 서빙 EMBEDDING_CHECKPOINT_PATH/MODEL_VERSION 세팅 + recreate (E2/E3 경로 재사용)
롤백 = 포인터를 이전 model_name 으로 되돌림(옛 벡터/인덱스 살아있어 즉시) + checkpoint env 되돌림 + recreate.

registry(model_registry) 가 승격 SoT. 포인터는 'AL/검색이 어떤 벡터를 보는가'의 SoT (design §2/§8.1).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# E 의 공통 헬퍼 재사용. import 실패는 삼키지 않고 기록 → main() 이 loud 하게 exit(silent
# false-success 금지). 신규 함수 단위테스트는 이 import 없이도(dummy PromotionError) 로드 가능.
_PROMOTE_MODEL_IMPORT_ERROR: Exception | None = None
try:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from promote_model import (  # type: ignore  # noqa: F401
        PromotionError,
        _make_minio,
        _write_env_var,
        docker_recreate,
        download_and_verify,
        promote_transition,
        rollback_transition,
        select_promotable_row,
        select_rollback_target,
    )
except Exception as exc:  # pragma: no cover - E 미머지 시
    _PROMOTE_MODEL_IMPORT_ERROR = exc

    class PromotionError(RuntimeError):
        pass


_ACTIVE_SCOPE = "frame_search"
_DOCKER_ENV_FILE = REPO_ROOT / "docker" / ".env"
_PE_MODEL = "pe_core"
_PE_SERVING = {
    "service": "embedding-service",
    "env_path_key": "EMBEDDING_CHECKPOINT_PATH",
    "env_version_key": "EMBEDDING_MODEL_VERSION",
    "host_subdir": "pe-core",
    "container_path": "/data/models/pe-core/merged.pt",
}


def _make_pe_resource(dsn: str):
    """CLI-local PostgresEmbeddingMixin 인스턴스 — PostgresResource 가 세팅하는 dsn/pool_min/pool_max
    계약을 Dagster 밖에서 수동 충족(ConfigurableResource 구성 없이 connect()/create_model_partial_hnsw()/
    get_active_embedding_model() 호출)."""
    src_root = str(REPO_ROOT / "src")
    if src_root not in sys.path:
        sys.path.insert(0, src_root)
    from vlm_pipeline.resources.postgres_base import PostgresBaseMixin
    from vlm_pipeline.resources.postgres_embedding import PostgresEmbeddingMixin

    class PeCorePromotionResource(PostgresBaseMixin, PostgresEmbeddingMixin):
        def __init__(self, dsn: str, *, pool_min: int = 1, pool_max: int = 2) -> None:
            self.dsn = dsn
            self.pool_min = pool_min
            self.pool_max = pool_max

    return PeCorePromotionResource(dsn)


def _model_name_from_row(row: dict, *, override: str | None = None) -> str:
    """model_registry 행의 version → versioned model_name. override 주면 version 일치 검증 후 그대로."""
    src_root = str(REPO_ROOT / "src")
    if src_root not in sys.path:
        sys.path.insert(0, src_root)
    from vlm_pipeline.lib.embedding_model_name import build_versioned_model_name, parse_model_name

    version = str(row.get("version") or "").strip()
    if not version:
        raise PromotionError(f"model_registry row {row.get('model_version_id')!r} has empty version")
    if override:
        _, override_version = parse_model_name(override)
        if not override_version or override_version != version:
            raise PromotionError(
                f"--new-model-name {override!r} version does not match model_registry.version {version!r}"
            )
        return override
    return build_versioned_model_name(version)


def assert_reembed_complete(cur, *, new_model_name: str, incumbent_model_name: str | None = None) -> int:
    """새 model_name 으로 frame 임베딩이 실제로 존재하는지 확인 (포인터 전환 전 선결).

    재임베딩(H3) 미완 상태에서 포인터를 돌리면 AL/검색이 빈 인덱스를 보게 됨 → 0 이면 거부.

    scope=frame_search 포인터는 **frame 검색/AL** 만 지배하므로 frame coverage 만 게이트한다
    (caption 임베딩은 이 포인터가 노출하는 read path 아님 — 의도된 frame-only, Codex BUG4 검토).
    count>0 는 최소 게이트다. incumbent_model_name 이 주어지면 incumbent frame 수 대비 커버리지를
    비교해 부분 재임베딩(중도 실패)을 잡는다 — --apply wiring 에서 이 인자를 넘길 것.
    """
    cur.execute(
        "SELECT count(*) FROM image_embeddings "
        "WHERE entity_type = 'frame' AND model_name = %(model_name)s",
        {"model_name": new_model_name},
    )
    row = cur.fetchone()
    n = int(row[0]) if row else 0
    if n <= 0:
        raise PromotionError(
            f"reembed not complete for {new_model_name!r}: 0 frame embeddings — "
            f"run reembed_under_version (H3) before flipping the pointer"
        )
    if incumbent_model_name:
        cur.execute(
            "SELECT count(*) FROM image_embeddings "
            "WHERE entity_type = 'frame' AND model_name = %(model_name)s",
            {"model_name": incumbent_model_name},
        )
        irow = cur.fetchone()
        incumbent_n = int(irow[0]) if irow else 0
        if incumbent_n > 0 and n < incumbent_n:
            raise PromotionError(
                f"reembed incomplete for {new_model_name!r}: {n} frame embeddings < "
                f"incumbent {incumbent_model_name!r} coverage {incumbent_n} — partial re-embed, "
                f"finish reembed_under_version before flipping the pointer"
            )
    return n


def flip_pointer(cur, *, new_model_name: str, scope: str = _ACTIVE_SCOPE, dry_run: bool) -> None:
    """embedding_active_model 포인터를 새 model_name 으로 원자 전환 (UPSERT)."""
    if dry_run:
        print(f"[promote-pe][dry-run] would flip pointer scope={scope} -> {new_model_name}")
        return
    cur.execute(
        "INSERT INTO embedding_active_model (scope, model_name, updated_at, updated_by) "
        "VALUES (%(scope)s, %(model_name)s, now(), 'promote_pe_core') "
        "ON CONFLICT (scope) DO UPDATE SET "
        "  model_name = EXCLUDED.model_name, updated_at = now(), updated_by = EXCLUDED.updated_by",
        {"scope": scope, "model_name": new_model_name},
    )
    print(f"[promote-pe] pointer flipped scope={scope} -> {new_model_name}")


def rollback_pointer(cur, *, prior_model_name: str, scope: str = _ACTIVE_SCOPE, dry_run: bool) -> None:
    """포인터를 이전 model_name 으로 되돌림 (옛 벡터/인덱스가 살아있어 즉시 복구)."""
    if dry_run:
        print(f"[promote-pe][dry-run] would roll back pointer scope={scope} -> {prior_model_name}")
        return
    cur.execute(
        "INSERT INTO embedding_active_model (scope, model_name, updated_at, updated_by) "
        "VALUES (%(scope)s, %(model_name)s, now(), 'promote_pe_core_rollback') "
        "ON CONFLICT (scope) DO UPDATE SET "
        "  model_name = EXCLUDED.model_name, updated_at = now(), updated_by = EXCLUDED.updated_by",
        {"scope": scope, "model_name": prior_model_name},
    )
    print(f"[promote-pe] pointer rolled back scope={scope} -> {prior_model_name}")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="PE-Core champion-challenger promotion (dry-run default)")
    p.add_argument("--model-version-id", help="promotable model_registry row id (pe_core)")
    p.add_argument("--new-model-name", help="versioned model_name to flip to, e.g. facebook/PE-Core-L14-336@ft-...")
    p.add_argument("--rollback-to", help="prior versioned model_name to restore (rollback mode)")
    p.add_argument("--dsn", default=None, help="PG DSN. fallback DATAOPS_POSTGRES_DSN.")
    p.add_argument("--env", default="prod", choices=("prod", "staging"))
    p.add_argument("--scope", default=_ACTIVE_SCOPE)
    p.add_argument("--skip-serving-recreate", action="store_true",
                   help="pgvector 포인터/레지스트리만 전환; checkpoint 다운로드 + embedding-service recreate 생략")
    p.add_argument("--apply", action="store_true", help="actually flip/recreate (default: dry-run)")
    return p


def main(argv: list[str] | None = None) -> int:
    """PE-Core 승격/롤백 배선 (C-2). dry-run 기본. 이전 no-op(항상 exit 0) → 실제 flip+recreate.

    승격: promotable 행 선택 → reembed 커버리지 확인 → partial HNSW → 포인터 flip + registry 전이
          → commit → (선택) checkpoint 다운로드 + embedding-service recreate.
    롤백: --rollback-to <옛 model_name> → 포인터 되돌림 + registry rolled_back.
    H-5 순서: DB commit 을 docker_recreate 前에. recreate 실패는 loud + 재실행 복구.
    wiring/인자 미비 시 loud non-zero exit (silent false-success 금지).
    """
    import os

    import psycopg2

    args = _build_arg_parser().parse_args(argv)

    if _PROMOTE_MODEL_IMPORT_ERROR is not None:
        print(f"[promote-pe] promote_model helpers unavailable: {_PROMOTE_MODEL_IMPORT_ERROR}", file=sys.stderr)
        return 2
    if args.rollback_to and (args.model_version_id or args.new_model_name):
        print("[promote-pe] --rollback-to cannot combine with --model-version-id/--new-model-name", file=sys.stderr)
        return 2

    dsn = args.dsn or os.getenv("DATAOPS_POSTGRES_DSN")
    if not dsn:
        print("[promote-pe] no DSN (--dsn or DATAOPS_POSTGRES_DSN)", file=sys.stderr)
        return 2

    dry_run = not args.apply
    serving_enabled = not args.skip_serving_recreate
    print(f"[promote-pe] mode={'APPLY' if args.apply else 'DRY-RUN'} env={args.env} scope={args.scope}")

    db = _make_pe_resource(dsn)
    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if args.rollback_to:
                row = select_rollback_target(cur, model=_PE_MODEL)
                target_model_name = _model_name_from_row(row)
                if target_model_name != args.rollback_to:
                    raise PromotionError(
                        f"latest rollback target is {target_model_name!r}, but --rollback-to={args.rollback_to!r}"
                    )
                cur.execute(
                    "SELECT model_version_id FROM model_registry WHERE model=%(m)s AND status='promoted' LIMIT 1",
                    {"m": _PE_MODEL},
                )
                cur_prom = cur.fetchone()
            else:
                row = select_promotable_row(cur, model=_PE_MODEL, model_version_id=args.model_version_id)
                target_model_name = _model_name_from_row(row, override=args.new_model_name)
                incumbent_model_name = db.get_active_embedding_model(scope=args.scope)
                print(f"[promote-pe] target row: id={row['model_version_id']} model_name={target_model_name} "
                      f"incumbent={incumbent_model_name}")
                n = assert_reembed_complete(cur, new_model_name=target_model_name,
                                            incumbent_model_name=incumbent_model_name)
                print(f"[promote-pe] reembed coverage ok: {n} frame embeddings")

            if dry_run:
                if args.rollback_to:
                    rollback_pointer(cur, prior_model_name=target_model_name, scope=args.scope, dry_run=True)
                else:
                    print(f"[promote-pe][dry-run] would create partial HNSW for {target_model_name}")
                    flip_pointer(cur, new_model_name=target_model_name, scope=args.scope, dry_run=True)
                print("[promote-pe][dry-run] no DB write, no pointer flip, no recreate. Pass --apply to execute.")
                conn.rollback()
                return 0

            # --apply: 포인터+registry 전이 먼저 commit (H-5: docker_recreate 前). partial HNSW 는
            # 포인터가 가리키기 전에 존재해야 하므로 flip 前 빌드.
            if args.rollback_to:
                rollback_pointer(cur, prior_model_name=target_model_name, scope=args.scope, dry_run=False)
                rollback_transition(cur, restore_row=row,
                                    current_promoted_id=(cur_prom[0] if cur_prom else -1), env=args.env)
            else:
                names = db.create_model_partial_hnsw(target_model_name)
                print(f"[promote-pe] partial HNSW ready: {', '.join(names)}")
                flip_pointer(cur, new_model_name=target_model_name, scope=args.scope, dry_run=False)
                promote_transition(cur, row=row, env=args.env)

        conn.commit()
        print(f"[promote-pe] committed pointer+registry -> {target_model_name}")

        if serving_enabled and not args.rollback_to:
            if not row.get("checkpoint_key") or not row.get("artifact_checksum"):
                print("[promote-pe] WARNING: row has no checkpoint_key/artifact_checksum — "
                      "skipping embedding-service recreate (pointer-only promotion)", file=sys.stderr)
                return 0
            dest = REPO_ROOT / "docker" / "data" / "models" / _PE_SERVING["host_subdir"] / "merged.pt"
            download_and_verify(_make_minio(), checkpoint_key=row["checkpoint_key"],
                                artifact_checksum=row["artifact_checksum"], dest_path=dest, dry_run=False)
            _write_env_var(_DOCKER_ENV_FILE, _PE_SERVING["env_path_key"], _PE_SERVING["container_path"])
            _write_env_var(_DOCKER_ENV_FILE, _PE_SERVING["env_version_key"], str(row.get("version") or ""))
            try:
                docker_recreate(_PE_SERVING["service"], dry_run=False)
            except Exception:
                print(f"[promote-pe] registry already committed to {target_model_name!r} but embedding-service "
                      f"recreate failed. Repair: docker compose -f {REPO_ROOT / 'docker' / 'docker-compose.yaml'} "
                      f"up -d --force-recreate {_PE_SERVING['service']}", file=sys.stderr)
                raise
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"[promote-pe] ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
