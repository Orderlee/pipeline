"""DuckDB spec 도메인 — labeling_specs, labeling_configs, requester_config_map 조회.

현재는 dispatch 요청의 spec_id → resolved_config 조회 (read + resolved_config 업데이트) 용도로만 사용된다.
(`lib/spec_config.py` 에서 호출)
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class DuckDBSpecMixin:
    """labeling_specs / labeling_configs / requester_config_map CRUD."""

    def get_labeling_spec_by_id(self, spec_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            if not self._table_exists(conn, "labeling_specs"):
                return None
            row = conn.execute(
                "SELECT * FROM labeling_specs WHERE spec_id = ?",
                [spec_id],
            ).fetchone()
            if row is None:
                return None
            cols = [d[0] for d in conn.description]
            out = dict(zip(cols, row))
            for k in ("categories", "classes", "labeling_method"):
                if k in out and isinstance(out[k], str) and out[k]:
                    try:
                        out[k] = json.loads(out[k])
                    except json.JSONDecodeError:
                        pass
            return out

    def resolve_config_for_requester(
        self, requester_id: str | None, team_id: str | None
    ) -> tuple[str | None, str | None]:
        """personal → team → _fallback 우선순위. (config_id, scope) 또는 (None, None)."""
        with self.connect() as conn:
            if not self._table_exists(conn, "requester_config_map") or not self._table_exists(
                conn, "labeling_configs"
            ):
                return None, None
            # personal
            if requester_id:
                row = conn.execute(
                    """
                    SELECT rcm.config_id, rcm.scope
                    FROM requester_config_map rcm
                    JOIN labeling_configs lc ON lc.config_id = rcm.config_id AND lc.is_active = 1
                    WHERE rcm.requester_id = ? AND rcm.team_id IS NULL
                      AND rcm.is_active = 1
                    LIMIT 1
                    """,
                    [requester_id],
                ).fetchone()
                if row:
                    return (row[0], row[1])
            # team
            if team_id:
                row = conn.execute(
                    """
                    SELECT rcm.config_id, rcm.scope
                    FROM requester_config_map rcm
                    JOIN labeling_configs lc ON lc.config_id = rcm.config_id AND lc.is_active = 1
                    WHERE rcm.team_id = ? AND rcm.is_active = 1
                    LIMIT 1
                    """,
                    [team_id],
                ).fetchone()
                if row:
                    return (row[0], row[1])
            # _fallback
            row = conn.execute(
                """
                SELECT config_id FROM labeling_configs
                WHERE config_id = '_fallback' AND is_active = 1
                LIMIT 1
                """
            ).fetchone()
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
            conn.execute(
                """
                UPDATE labeling_specs
                SET resolved_config_id = ?, resolved_config_scope = ?, updated_at = ?
                WHERE spec_id = ?
                """,
                [resolved_config_id, resolved_config_scope, datetime.now(), spec_id],
            )

    def increment_spec_retry_count(self, spec_id: str) -> int:
        with self.connect() as conn:
            conn.execute(
                "UPDATE labeling_specs SET retry_count = retry_count + 1, updated_at = ? WHERE spec_id = ?",
                [datetime.now(), spec_id],
            )
            row = conn.execute("SELECT retry_count FROM labeling_specs WHERE spec_id = ?", [spec_id]).fetchone()
            return int(row[0]) if row else 0

    def get_labeling_config(self, config_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            if not self._table_exists(conn, "labeling_configs"):
                return None
            row = conn.execute(
                "SELECT config_id, config_json, version, is_active FROM labeling_configs WHERE config_id = ?",
                [config_id],
            ).fetchone()
            if row is None:
                return None
            try:
                config_json = json.loads(row[1]) if isinstance(row[1], str) else row[1]
            except (TypeError, json.JSONDecodeError):
                config_json = {}
            return {
                "config_id": row[0],
                "config_json": config_json,
                "version": row[2],
                "is_active": bool(row[3]),
            }

    def get_config(self, config_id: str) -> dict[str, Any] | None:
        """Backward-compatible alias for legacy callers."""
        return self.get_labeling_config(config_id)

    def list_labeling_configs(self, include_inactive: bool = True) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if not self._table_exists(conn, "labeling_configs"):
                return []
            query = "SELECT config_id, config_json, version, is_active FROM labeling_configs"
            if not include_inactive:
                query += " WHERE is_active = 1"
            rows = conn.execute(query).fetchall()
            result = []
            for row in rows:
                try:
                    config_json = json.loads(row[1]) if isinstance(row[1], str) else row[1]
                except (TypeError, json.JSONDecodeError):
                    config_json = {}
                result.append(
                    {
                        "config_id": row[0],
                        "config_json": config_json,
                        "version": row[2],
                        "is_active": bool(row[3]),
                    }
                )
            return result

    def list_active_configs(self) -> list[dict[str, Any]]:
        return self.list_labeling_configs(include_inactive=False)

    def set_labeling_config_active(self, config_id: str, is_active: bool) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE labeling_configs
                SET is_active = ?, updated_at = ?
                WHERE config_id = ?
                """,
                [is_active, datetime.now(), config_id],
            )

    def get_active_map(self, requester_id: str | None, team_id: str | None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if not self._table_exists(conn, "requester_config_map"):
                return []
            conditions = []
            params = []
            if requester_id:
                conditions.append("(requester_id = ? OR requester_id IS NULL)")
                params.append(requester_id)
            if team_id:
                conditions.append("(team_id = ? OR team_id IS NULL)")
                params.append(team_id)
            where = " AND ".join(conditions) if conditions else "1=1"
            params.append(True)
            rows = conn.execute(
                f"""
                SELECT map_id, requester_id, team_id, scope, config_id, is_active
                FROM requester_config_map
                WHERE {where} AND is_active = ?
                """,
                params,
            ).fetchall()
            cols = ["map_id", "requester_id", "team_id", "scope", "config_id", "is_active"]
            return [dict(zip(cols, row)) for row in rows]
