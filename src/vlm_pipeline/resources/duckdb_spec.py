"""DuckDB spec 도메인 — labeling_specs, labeling_configs, requester_config_map CRUD.

Staging spec flow (auto_labeling_unified_spec) 전용. IS_STAGING 시에만 사용.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import uuid4


def _json_col(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, str):
        return val
    return json.dumps(val, ensure_ascii=False)


class DuckDBSpecMixin:
    """labeling_specs / labeling_configs / requester_config_map CRUD."""

    def upsert_labeling_spec(
        self,
        spec_id: str,
        *,
        requester_id: str | None = None,
        team_id: str | None = None,
        source_unit_name: str | None = None,
        categories: list[str] | None = None,
        classes: list[str] | None = None,
        labeling_method: list[str] | None = None,
        spec_status: str = "pending",
        retry_count: int = 0,
        resolved_config_id: str | None = None,
        resolved_config_scope: str | None = None,
        last_error: str | None = None,
    ) -> None:
        now = datetime.now()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO labeling_specs (
                    spec_id, requester_id, team_id, source_unit_name,
                    categories, classes, labeling_method, spec_status,
                    retry_count, resolved_config_id, resolved_config_scope,
                    last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (spec_id) DO UPDATE SET
                    requester_id = EXCLUDED.requester_id,
                    team_id = EXCLUDED.team_id,
                    source_unit_name = EXCLUDED.source_unit_name,
                    categories = EXCLUDED.categories,
                    classes = EXCLUDED.classes,
                    labeling_method = EXCLUDED.labeling_method,
                    spec_status = EXCLUDED.spec_status,
                    retry_count = EXCLUDED.retry_count,
                    resolved_config_id = EXCLUDED.resolved_config_id,
                    resolved_config_scope = EXCLUDED.resolved_config_scope,
                    last_error = EXCLUDED.last_error,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    spec_id,
                    requester_id,
                    team_id,
                    source_unit_name,
                    _json_col(categories),
                    _json_col(classes),
                    _json_col(labeling_method or []),
                    spec_status,
                    retry_count,
                    resolved_config_id,
                    resolved_config_scope,
                    last_error,
                    now,
                    now,
                ],
            )

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

    def list_specs_by_status(self, spec_status: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if not self._table_exists(conn, "labeling_specs"):
                return []
            rows = conn.execute(
                "SELECT * FROM labeling_specs WHERE spec_status = ? ORDER BY updated_at",
                [spec_status],
            ).fetchall()
            cols = [d[0] for d in conn.description]
            result = []
            for row in rows:
                out = dict(zip(cols, row))
                for k in ("categories", "classes", "labeling_method"):
                    if k in out and isinstance(out[k], str) and out[k]:
                        try:
                            out[k] = json.loads(out[k])
                        except json.JSONDecodeError:
                            pass
                result.append(out)
            return result

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

    def update_spec_status(
        self,
        spec_id: str,
        spec_status: str,
        last_error: str | None = None,
    ) -> None:
        with self.connect() as conn:
            if last_error is not None:
                conn.execute(
                    """
                    UPDATE labeling_specs
                    SET spec_status = ?, last_error = ?, updated_at = ?
                    WHERE spec_id = ?
                    """,
                    [spec_status, last_error, datetime.now(), spec_id],
                )
            else:
                conn.execute(
                    """
                    UPDATE labeling_specs
                    SET spec_status = ?, updated_at = ?
                    WHERE spec_id = ?
                    """,
                    [spec_status, datetime.now(), spec_id],
                )

    def increment_spec_retry_count(self, spec_id: str) -> int:
        with self.connect() as conn:
            conn.execute(
                "UPDATE labeling_specs SET retry_count = retry_count + 1, updated_at = ? WHERE spec_id = ?",
                [datetime.now(), spec_id],
            )
            row = conn.execute("SELECT retry_count FROM labeling_specs WHERE spec_id = ?", [spec_id]).fetchone()
            return int(row[0]) if row else 0

    def get_config(self, config_id: str) -> dict[str, Any] | None:
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

    def list_active_configs(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if not self._table_exists(conn, "labeling_configs"):
                return []
            rows = conn.execute(
                "SELECT config_id, config_json, version FROM labeling_configs WHERE is_active = 1"
            ).fetchall()
            result = []
            for row in rows:
                try:
                    config_json = json.loads(row[1]) if isinstance(row[1], str) else row[1]
                except (TypeError, json.JSONDecodeError):
                    config_json = {}
                result.append({"config_id": row[0], "config_json": config_json, "version": row[2]})
            return result

    def upsert_labeling_config(
        self,
        config_id: str,
        config_json: dict[str, Any],
        version: int = 1,
        is_active: bool = True,
    ) -> None:
        now = datetime.now()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO labeling_configs (config_id, config_json, version, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (config_id) DO UPDATE SET
                    config_json = EXCLUDED.config_json,
                    version = EXCLUDED.version,
                    is_active = EXCLUDED.is_active,
                    updated_at = EXCLUDED.updated_at
                """,
                [config_id, _json_col(config_json), version, is_active, now, now],
            )

    def get_spec_by_source_unit_name(
        self, source_unit_name: str, statuses: list[str] | None = None
    ) -> dict[str, Any] | None:
        """source_unit_name으로 매칭되는 spec 1건 반환. statuses 제한 optional."""
        with self.connect() as conn:
            if not self._table_exists(conn, "labeling_specs"):
                return None
            if statuses:
                placeholders = ", ".join("?" * len(statuses))
                row = conn.execute(
                    f"""
                    SELECT * FROM labeling_specs
                    WHERE source_unit_name = ? AND spec_status IN ({placeholders})
                    ORDER BY updated_at DESC LIMIT 1
                    """,
                    [source_unit_name] + list(statuses),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM labeling_specs WHERE source_unit_name = ? ORDER BY updated_at DESC LIMIT 1",
                    [source_unit_name],
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
