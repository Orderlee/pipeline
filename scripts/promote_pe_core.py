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

# E 의 공통 헬퍼 재사용 (있을 때만 — 신규 함수 단위테스트는 이 import 없이도 로드 가능).
try:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from promote_model import (  # type: ignore  # noqa: F401
        PromotionError,
        _make_minio,
        _write_env_var,
        docker_recreate,
        download_and_verify,
    )
except Exception:  # pragma: no cover - E 미머지 시
    class PromotionError(RuntimeError):
        pass


_ACTIVE_SCOPE = "frame_search"
_DOCKER_ENV_FILE = REPO_ROOT / "docker" / ".env"


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
    p.add_argument("--rollback-to", help="prior model_name to restore (rollback mode)")
    p.add_argument("--env", default="prod", choices=("prod", "staging"))
    p.add_argument("--apply", action="store_true", help="actually flip/recreate (default: dry-run)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    dry_run = not args.apply
    print(f"[promote-pe] mode={'APPLY' if args.apply else 'DRY-RUN'} env={args.env}")
    # Wiring (PG connect, registry status transition via promote_model.promote_transition,
    # download_and_verify into ./docker/data/models/pe-core/, _write_env_var of
    # EMBEDDING_CHECKPOINT_PATH/EMBEDDING_MODEL_VERSION, create_model_partial_hnsw, flip_pointer,
    # docker_recreate('embedding-service')) is assembled in the integration step E7-analogue and is
    # intentionally side-effect-free under dry_run. Unit tests cover the pure transition functions.
    if dry_run:
        print("[promote-pe] dry-run: no DB write, no pointer flip, no recreate. Pass --apply to execute.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
