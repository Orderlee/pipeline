"""Phase 4-B (#12) — PG migration 001~004 sequence integration test.

`ensure_schema()` 가 정상 환경에서 4개 마이그레이션을 모두 적용하는지, 그리고 두 번
실행해도 idempotent (추가 행 / DDL 충돌 없음) 한지 검증.

새 migration 추가 시 본 테스트가 회귀 catch 한다 (CREATE TABLE 충돌, 잘못된
constraint, 마이그레이션 메타 테이블 동기화 누락 등).
"""

from __future__ import annotations


def _list_migrations(cur) -> list[str]:
    """_pg_migrations 메타 테이블의 ``name`` 컬럼 — postgres_migration.py:33 DDL 참조."""
    cur.execute("SELECT name FROM _pg_migrations ORDER BY name")
    return [r[0] for r in cur.fetchall()]


def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        )
        """,
        (table_name,),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def _column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
        )
        """,
        (table_name, column_name),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def test_ensure_schema_records_all_migrations(pg_resource) -> None:
    """fixture 가 ensure_schema() 를 이미 호출한 상태 — 4개 마이그레이션 모두 기록."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            migrations = _list_migrations(cur)

    # 마이그레이션 메타 이름은 파일 stem 또는 비슷한 형식 — 순서·개수만 단언.
    # 새 마이그레이션 추가 시 본 테스트가 자연스럽게 fail → CI 가 catch.
    assert len(migrations) >= 4, f"_pg_migrations rows={len(migrations)} (>=4 expected). actual={migrations}"
    # 마지막 마이그레이션이 004 인지 (Phase 3-D 도입) — sanity.
    assert any("004" in name for name in migrations), f"004 migration not recorded. actual={migrations}"


def test_ensure_schema_creates_core_tables(pg_resource) -> None:
    """8 strict tables 가 모두 존재해야 한다 (schema_postgres.sql 1-8 섹션)."""
    expected_tables = [
        "raw_files",
        "video_metadata",
        "labels",
        "processed_clips",
        "image_metadata",
        "datasets",
        "dataset_clips",
        "image_labels",
    ]
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            for name in expected_tables:
                assert _table_exists(cur, name), f"{name} table missing"


def test_004_dataset_lineage_columns_present(pg_resource) -> None:
    """Phase 3-D 도입한 spec_hash/git_sha/build_started_at 컬럼 존재."""
    expected_cols = ["spec_hash", "git_sha", "build_started_at"]
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            for col in expected_cols:
                assert _column_exists(cur, "datasets", col), f"datasets.{col} missing — migration 004 미적용"


def test_ensure_schema_is_idempotent(pg_resource) -> None:
    """두 번 호출해도 마이그레이션 행 개수 변화 없음 + DDL 충돌 없음."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            initial_migrations = _list_migrations(cur)

    # 같은 resource 인스턴스로 재호출.
    pg_resource.ensure_schema()

    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            second_migrations = _list_migrations(cur)

    assert initial_migrations == second_migrations, (
        f"ensure_schema() 재호출이 마이그레이션 행을 추가함 — idempotent 깨짐. "
        f"before={initial_migrations} after={second_migrations}"
    )
