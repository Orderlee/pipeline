"""DuckDB ↔ PostgreSQL dual-write facade — prod cutover transition vehicle.

이 모듈은 마이그레이션 plan(`db-splendid-crayon`) Phase 6 의 dual-write window 에서
사용되는 facade 다. ``DATAOPS_DB_BACKEND`` env 가 ``dual_*`` 모드일 때
``definitions_production.build_common_resources`` 가 ``DualDBResource`` 인스턴스를
``"db"`` 리소스 키로 wire 한다.

운영 모드 (``DATAOPS_DB_BACKEND``):
  - ``duckdb``                : DuckDBResource 단일 (마이그레이션 이전 default)
  - ``postgres``              : PostgresResource 단일 (마이그레이션 이후)
  - ``dual_duckdb_primary``   : DuckDB가 authoritative, PG는 mirror.
                                 reads → DuckDB, writes → DuckDB(+PG best-effort)
  - ``dual_pg_primary``       : PG가 authoritative, DuckDB는 mirror.
                                 reads → PG, writes → PG(+DuckDB best-effort)

설계 원칙:
  - 도메인 메서드(``insert_*``/``find_*`` 등)는 ``__getattr__`` 으로 자동 위임.
    명시적 메서드 목록을 두지 않아 신규 mixin 메서드 추가 시 자동 지원.
  - mirror 실패는 logging 만, primary 결과를 return — per-file fail-forward 유지.
  - ``ensure_schema()``: 양쪽 모두에 DDL 적용 (mirror 실패는 로그만).
  - ``connect()``: primary 만 노출. 직접 ``with db.connect() as conn`` 사용 코드는
    cutover 동안 mirror 되지 않는다 — 이 한계를 알고 있어야 한다.

종료 조건:
  - main 머지 후 prod 가 ``postgres`` 모드로 ≥2주 안정 → DualDBResource 코드 제거.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Literal

logger = logging.getLogger(__name__)


# 메서드 이름 prefix 기반 read/write 분류 — heuristic.
# 신규 mixin 메서드가 추가되면 prefix 가 누락되지 않았는지 확인해야 한다.
_READ_PREFIXES: tuple[str, ...] = (
    "find_", "get_", "list_", "count_", "has_", "resolve_",
)
_WRITE_PREFIXES: tuple[str, ...] = (
    "insert_", "update_", "upsert_", "delete_", "batch_", "replace_",
    "mark_", "clear_", "recover_", "abort_", "close_", "set_",
    "increment_", "ensure_", "repair_",
)


def _is_read_method(name: str) -> bool:
    return any(name.startswith(p) for p in _READ_PREFIXES)


def _is_write_method(name: str) -> bool:
    return any(name.startswith(p) for p in _WRITE_PREFIXES)


class DualDBResource:
    """primary + mirror DB facade. plain Python class (Pydantic 미사용).

    Dagster ``ConfigurableResource`` 로 등록되지 않지만, asset/op 측 ``db``
    파라미터에 그대로 binding 되어 동일 인터페이스(domain method)를 노출한다.
    """

    # ──────────────────────────────────────────────────────────────────
    # 인스턴스 속성은 __getattr__ 트리거 회피를 위해 __slots__ 또는 명시 init 으로 관리.
    # __getattr__ 은 일반 lookup 실패 시에만 호출되므로 instance dict 에 있는
    # primary/mirror/read_from 은 정상 lookup 됨 → 위임 로직과 충돌 없음.
    # ──────────────────────────────────────────────────────────────────
    __slots__ = ("primary", "mirror", "read_from", "_method_cache")

    def __init__(
        self,
        primary: Any,
        mirror: Any | None = None,
        read_from: Literal["primary", "mirror"] = "primary",
    ) -> None:
        if read_from == "mirror" and mirror is None:
            raise ValueError("read_from='mirror' requires a mirror backend")
        self.primary = primary
        self.mirror = mirror
        self.read_from = read_from
        self._method_cache: dict[str, Callable[..., Any]] = {}

    # ── Lifecycle hooks: 양쪽 모두 적용 ────────────────────────────────

    def ensure_schema(self) -> None:
        """primary 적용 후 mirror 도 시도 (mirror 실패는 raise — 부팅 실패는 명확히 노출)."""
        self.primary.ensure_schema()
        if self.mirror is not None:
            self.mirror.ensure_schema()

    def ensure_runtime_schema(self) -> None:
        """hot path — primary 는 raise, mirror 는 best-effort."""
        self.primary.ensure_runtime_schema()
        if self.mirror is not None:
            try:
                self.mirror.ensure_runtime_schema()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[dual_db] mirror ensure_runtime_schema failed: %s",
                    exc,
                )

    def connect(self):
        """primary backend 의 connect() context manager 를 그대로 반환.

        ⚠️ cutover 한계: 이 path 로 들어간 raw SQL 은 mirror 되지 않는다.
        가능한 한 도메인 메서드(``insert_*``, ``find_*`` 등)를 사용하라.
        """
        target = self.primary if self.read_from == "primary" else self.mirror
        return target.connect()

    # ── 도메인 메서드 자동 위임 ─────────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        # __getattr__ 은 일반 lookup 실패 시만 호출. private/dunder 는 차단.
        if name.startswith("_"):
            raise AttributeError(name)

        # caching — 같은 메서드명에 대한 두 번째 lookup 부터 wrapper 재생성 비용 회피.
        cached = self._method_cache.get(name)
        if cached is not None:
            return cached

        if _is_read_method(name):
            target = self.primary if self.read_from == "primary" else self.mirror
            method = getattr(target, name)
            self._method_cache[name] = method
            return method

        if _is_write_method(name):
            primary_fn = getattr(self.primary, name)
            if self.mirror is None:
                self._method_cache[name] = primary_fn
                return primary_fn
            mirror_fn = getattr(self.mirror, name)

            def _dual(*args: Any, **kwargs: Any) -> Any:
                # primary 는 raise — 호출자가 per-file fail-forward 로 처리.
                result = primary_fn(*args, **kwargs)
                # mirror 는 best-effort — 실패해도 primary 성공 결과 보존.
                try:
                    mirror_fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "[dual_db] mirror write %s failed (%s): %s",
                        name,
                        type(exc).__name__,
                        exc,
                    )
                return result

            _dual.__name__ = name
            _dual.__qualname__ = f"DualDBResource.{name}"
            self._method_cache[name] = _dual
            return _dual

        # 분류되지 않는 메서드는 primary 로만 라우팅 (안전 fallback).
        # 새 mixin 이 추가되면서 prefix 누락 시 즉시 알 수 있게 logger.warning.
        logger.warning(
            "[dual_db] method %r not classified as read/write; routing to primary only. "
            "Consider adding the prefix to _READ_PREFIXES or _WRITE_PREFIXES in data_db.py.",
            name,
        )
        return getattr(self.primary, name)
