"""PG spec 도메인 — labeling_specs, labeling_configs, requester_config_map 조회.

DuckDBSpecMixin 1:1 포팅. 변환 규칙:
  - placeholder ``?`` → ``%s``
  - cursor 패턴 사용
  - DuckDB ``is_active = 1`` → ``is_active = TRUE``
  - JSONB 컬럼은 psycopg2 가 자동으로 dict/list 로 변환해 주므로 수동 ``json.loads()`` 불필요
    (DuckDB era에서는 TEXT-as-JSON 이라 직접 파싱이 필요했음)
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class PostgresSpecMixin:
    """labeling_specs / labeling_configs / requester_config_map CRUD."""

    @staticmethod
    def _coerce_jsonb(value: Any) -> Any:
        """psycopg2 가 JSONB 를 이미 dict/list 로 변환했으면 그대로,
        TEXT-as-JSON 이거나 hybrid 환경(레거시) 일 경우 안전하게 파싱."""
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    def get_labeling_spec_by_id(self, spec_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            if not self._table_exists(conn, "labeling_specs"):
                return None
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM labeling_specs WHERE spec_id = %s",
                    (spec_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                cols = [d[0] for d in cur.description]
            out = dict(zip(cols, row))
            for k in ("categories", "classes", "labeling_method"):
                if k in out:
                    out[k] = self._coerce_jsonb(out[k])
            return out

    def resolve_config_for_requester(
        self, requester_id: str | None, team_id: str | None
    ) -> tuple[str | None, str | None]:
        """personal → team → _fallback 우선순위. (config_id, scope) 또는 (None, None)."""
        with self.connect() as conn:
            if not self._table_exists(conn, "requester_config_map") or not self._table_exists(conn, "labeling_configs"):
                return None, None
            # personal
            if requester_id:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT rcm.config_id, rcm.scope
                        FROM requester_config_map rcm
                        JOIN labeling_configs lc ON lc.config_id = rcm.config_id AND lc.is_active = TRUE
                        WHERE rcm.requester_id = %s AND rcm.team_id IS NULL
                          AND rcm.is_active = TRUE
                        LIMIT 1
                        """,
                        (requester_id,),
                    )
                    row = cur.fetchone()
                if row:
                    return (row[0], row[1])
            # team
            if team_id:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT rcm.config_id, rcm.scope
                        FROM requester_config_map rcm
                        JOIN labeling_configs lc ON lc.config_id = rcm.config_id AND lc.is_active = TRUE
                        WHERE rcm.team_id = %s AND rcm.is_active = TRUE
                        LIMIT 1
                        """,
                        (team_id,),
                    )
                    row = cur.fetchone()
                if row:
                    return (row[0], row[1])
            # _fallback
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT config_id FROM labeling_configs
                    WHERE config_id = '_fallback' AND is_active = TRUE
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
            if row:
                return (row[0], "fallback")
            return None, None

    def update_spec_resolved_config(
        self,
        spec_id: str,
        resolved_config_id: str,
        resolved_config_scope: str,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE labeling_specs
                    SET resolved_config_id = %s, resolved_config_scope = %s, updated_at = %s
                    WHERE spec_id = %s
                    """,
                    (resolved_config_id, resolved_config_scope, datetime.now(), spec_id),
                )

    def get_labeling_config(self, config_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            if not self._table_exists(conn, "labeling_configs"):
                return None
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT config_id, config_json, version, is_active FROM labeling_configs WHERE config_id = %s",
                    (config_id,),
                )
                row = cur.fetchone()
            if row is None:
                return None
            return {
                "config_id": row[0],
                "config_json": self._coerce_jsonb(row[1]) or {},
                "version": row[2],
                "is_active": bool(row[3]),
            }

    def get_config(self, config_id: str) -> dict[str, Any] | None:
        """Backward-compatible alias for legacy callers."""
        return self.get_labeling_config(config_id)
