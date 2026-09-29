"""PostgreSQL 스키마 마이그레이션 mixin — file-based forward-only runner.

DuckDB era의 ``DuckDBMigrationMixin`` 은 schema.sql + 17개 ``_ensure_*_columns()``
헬퍼로 이뤄진 ad-hoc upgrade 코드였다. PG에서는 다음과 같이 정돈한다:

  - sql/migrations/postgres/NNN_*.sql 를 정렬해 순회
  - _pg_migrations 메타 테이블에 적용 기록
  - 미적용 파일을 트랜잭션으로 실행
  - schema.sql 본체 import 같은 통째 실행은 없다 (각 파일이 자체적으로 BEGIN/COMMIT)

운영 규칙은 ``src/vlm_pipeline/sql/migrations/postgres/README.md`` 참조.

DuckDB era와 호환을 위해 동일한 entry-point 이름을 유지한다:
  - ``ensure_schema()``    : 모든 미적용 마이그레이션 적용 (배포 단계용)
  - ``ensure_runtime_schema()`` : 같은 동작 + 프로세스당 1회 가드 (hot path용)

PG는 마이그레이션이 멱등하고 빠르므로 두 메서드의 구현은 거의 동일하다.
"""

from __future__ import annotations

import re
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import ClassVar

import psycopg2
import psycopg2.extensions

_CONCURRENTLY_RE = re.compile(r"\bCONCURRENTLY\b", re.IGNORECASE)
_EXPLICIT_BEGIN_RE = re.compile(r"^\s*BEGIN\s*;", re.IGNORECASE | re.MULTILINE)
_DOLLAR_TAG_RE = re.compile(r"\$[A-Za-z_][A-Za-z_0-9]*\$|\$\$")


def _scan_quoted(sql_text: str, start: int) -> int:
    """``'``로 시작하는 리터럴의 끝(닫는 따옴표 다음 인덱스). ``''`` 는 이스케이프."""
    n = len(sql_text)
    j = start + 1
    while j < n:
        if sql_text[j] != "'":
            j += 1
        elif j + 1 < n and sql_text[j + 1] == "'":
            j += 2
        else:
            return j + 1
    return n


def _split_sql_statements(sql_text: str) -> list[str]:
    """세미콜론 기준 문장 분리 — 문자열·달러인용·주석 안의 ``;`` 은 경계로 보지 않는다.

    마이그레이션 파일을 한 문장씩 보내야 하는 경우(CONCURRENTLY)에만 쓴다. 순진한
    ``split(';')`` 은 ``DO $$ ... ; ... $$`` 와 ``'a;b'`` 리터럴을 쪼개 파일을 망가뜨린다.
    """
    out: list[str] = []
    buf: list[str] = []
    has_code = False  # 주석·공백만 있는 조각은 문장이 아니다 (빈 쿼리 실행 = psycopg2 오류)
    i, n = 0, len(sql_text)
    while i < n:
        ch = sql_text[i]
        pair = sql_text[i : i + 2]
        if pair == "--":  # 줄 주석
            end = sql_text.find("\n", i)
            end = n if end == -1 else end + 1
            buf.append(sql_text[i:end])
            i = end
        elif pair == "/*":  # 블록 주석
            end = sql_text.find("*/", i + 2)
            end = n if end == -1 else end + 2
            buf.append(sql_text[i:end])
            i = end
        elif ch == "'":  # 문자열 리터럴('' 이스케이프 포함)
            j = _scan_quoted(sql_text, i)
            buf.append(sql_text[i:j])
            has_code = True
            i = j
        elif ch == "$" and _DOLLAR_TAG_RE.match(sql_text, i):  # 달러 인용 ($$ 또는 $tag$)
            tag = _DOLLAR_TAG_RE.match(sql_text, i).group(0)  # type: ignore[union-attr]
            end = sql_text.find(tag, i + len(tag))
            end = n if end == -1 else end + len(tag)
            buf.append(sql_text[i:end])
            has_code = True
            i = end
        elif ch == ";":
            if has_code:
                out.append("".join(buf).strip() + ";")
                buf = []
                has_code = False
            # 주석만 모인 조각이면 버리지 않고 다음 문장 앞에 붙인다(원문 주석 보존).
            i += 1
        else:
            buf.append(ch)
            has_code = has_code or not ch.isspace()
            i += 1
    if has_code:
        out.append("".join(buf).strip())
    return out


# 메타 테이블 — 적용 이력 추적. 마이그레이션 시스템 자체가 의존하므로 별도 파일 없이
# 코드 안에 둔다 (chicken-and-egg 해결).
_PG_MIGRATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS _pg_migrations (
    name        TEXT PRIMARY KEY,
    applied_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    checksum    TEXT
);
"""


class PostgresSchemaBaselineError(RuntimeError):
    """PostgreSQL 스키마가 애플리케이션이 요구하는 baseline 미만일 때 발생.

    Per-call ``_table_columns()`` 가드를 제거하고 startup-time 단일 검사로
    대체했기 때문에, 필수 migration 누락이 곧 runtime 호환성 실패를 의미한다.
    호출자가 ``RuntimeError`` 일반과 구분해 처리할 수 있도록 별도 subclass.
    """


class PostgresMigrationMixin:
    """forward-only file-based migration runner."""

    _runtime_schema_ensured: ClassVar[bool] = False
    _runtime_schema_lock: ClassVar[threading.Lock] = threading.Lock()

    # ── Public entry-points ──

    def ensure_schema(self) -> None:
        """모든 미적용 마이그레이션을 적용한다 (배포/부팅 시).

        멱등하므로 여러 번 호출해도 안전. 이미 적용된 파일은 skip.

        구현 노트: 마이그레이션 파일이 자체적으로 ``BEGIN/COMMIT`` 을 포함하므로
        ``connect()`` ctxmgr 의 outer transaction 과 충돌하지 않도록 dedicated
        AUTOCOMMIT 커넥션을 직접 열어 사용한다 (pool 우회).

        파일 안에 ``-- @ASSERT_AFTER: <sql>`` 주석이 있으면 적용 직후 + 매 부팅 시
        해당 SQL 을 실행해 결과가 truthy 인지 검증한다. drift detection 역할.
        2026-06-09 005 적용 시 ALTER ADD CONSTRAINT 가 silent fail 한 사고
        ([[project_postgres_migration_runner_quirk]]) 재발 방지.
        """
        with self._autocommit_conn() as conn:
            self._ensure_migrations_table(conn)
            applied = self._fetch_applied(conn)
            for path in self._discover_migrations():
                if not self._optional_precondition_met(conn, path):
                    # optional 마이그레이션의 전제조건 미충족(예: pgvector 미설치 이미지)
                    # → 적용·검증·기록 모두 skip. 전제조건이 충족되는 환경에서만 적용되고,
                    #   미기록이므로 나중에 충족되면 그때 적용된다.
                    continue
                if path.name not in applied:
                    self._apply_one(conn, path)
                self._verify_assertions(conn, path)

    _REQUIRED_MIGRATIONS: ClassVar[frozenset[str]] = frozenset(
        {
            "001_init.sql",
            "002_genai.sql",
            "003_genai_veo.sql",
            "004_dataset_lineage.sql",
            "005_labels_unique.sql",
            "011_image_label_annotations.sql",
            "013_mlops_finetune.sql",
            "014_gpu_maintenance_lock.sql",
            "016_dataset_catalog.sql",
            "030_comfy_local.sql",
        }
    )

    # Optional 마이그레이션: 전제조건 SQL 이 truthy 한 환경에서만 적용한다.
    # 전제조건 미충족 시 apply/assert/record 를 전부 skip (미기록 → 나중에 충족되면 적용).
    # 006: pgvector(`vector`) 확장이 설치 가능한 이미지에서만. vanilla postgres:15
    #      (CI / prod 기본) 에서 `CREATE EXTENSION vector` 가 부팅을 깨뜨리는 것을 방지.
    # 007: 006 이 적용된 환경(image_embeddings 테이블 존재)에서만. caption 텍스트 임베딩 지원.
    _OPTIONAL_MIGRATIONS: ClassVar[dict[str, str]] = {
        "006_image_embeddings.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'",
        "007_caption_embeddings.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'",
        "008_embedding_partial_indexes.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'",
        "009_video_embeddings.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'",
        # 010: 캡션 lexical(키워드) 검색용 pg_trgm. pg_trgm 미가용 이미지에서 부팅 깨짐 방지.
        "010_caption_trgm_index.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_trgm'",
        # 015: 활성 임베딩 model_name 포인터 (PE-Core 승격). image_embeddings 가 있는
        #      (pgvector) 환경에서만 의미 → 008/009 과 동일 전제조건. 비-pgvector prod 부팅 보호.
        "015_embedding_active_model.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'",
        # 021: 뱅크 문장 벡터(entity_type='prompt')용 partial HNSW. 008/009 과 동일 전제조건.
        "021_prompt_embedding_index.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'",
        # 029: al_frame partial HNSW. 027 에서 분리 — 027 은 al_frames 테이블(pgvector 무관)이라
        #      무조건 적용돼야 하는데 인덱스를 같이 넣었다가 vanilla postgres 에서 러너가 죽었다.
        "029_al_frame_embedding_index.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'",
    }

    def ensure_runtime_schema(self) -> None:
        """런타임 hot path 가드 — 프로세스당 1회만 실행.

        Dagster code-server 부팅 시 한 번만 마이그레이션을 적용하기 위함.
        ``ensure_schema()`` 와 동작은 같지만 ClassVar 플래그로 후속 호출을 skip한다.

        모든 필수 마이그레이션(001-004)이 적용됐는지 확인하고, 누락 시 RuntimeError 를 던진다.
        이 검사가 per-call 컬럼 guard 를 대체한다.
        """
        if PostgresMigrationMixin._runtime_schema_ensured:
            return
        with PostgresMigrationMixin._runtime_schema_lock:
            if PostgresMigrationMixin._runtime_schema_ensured:
                return
            self.ensure_schema()
            self._assert_required_migrations_applied()
            PostgresMigrationMixin._runtime_schema_ensured = True

    def _assert_required_migrations_applied(self) -> None:
        """필수 마이그레이션 001-004 가 모두 적용됐는지 확인한다.

        미적용 마이그레이션이 있으면 RuntimeError 를 발생시켜 조용한 no-op 대신
        명확한 오류로 schema drift 를 표면화한다.
        """
        with self._autocommit_conn() as conn:
            applied = self._fetch_applied(conn)
        missing = self._REQUIRED_MIGRATIONS - applied
        if missing:
            missing_str = ", ".join(sorted(missing))
            raise PostgresSchemaBaselineError(
                f"Required PG migrations not applied: {missing_str}. "
                "Run ensure_schema() or check migration runner logs."
            )

    # ── Internals ──

    @contextmanager
    def _autocommit_conn(self):
        """마이그레이션 전용 dedicated 커넥션 (pool 우회, AUTOCOMMIT).

        파일 내부 ``BEGIN/COMMIT`` 이 outer transaction 과 충돌하지 않도록
        autocommit 으로 시작. ``set_isolation_level`` 을 활성 트랜잭션 도중에
        호출하면 implicit ROLLBACK 으로 직전 DDL 이 사라지는 이슈를 회피.
        """
        conn = psycopg2.connect(self.dsn)
        try:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass

    @classmethod
    def _migrations_dir(cls) -> Path:
        # resources/postgres_migration.py → ../sql/migrations/postgres/
        return Path(__file__).resolve().parents[1] / "sql" / "migrations" / "postgres"

    @classmethod
    def _discover_migrations(cls) -> list[Path]:
        directory = cls._migrations_dir()
        if not directory.exists():
            raise FileNotFoundError(f"PG migrations dir missing: {directory}")
        files = sorted(p for p in directory.glob("*.sql") if p.is_file())
        return files

    def _optional_precondition_met(self, conn: psycopg2.extensions.connection, path: Path) -> bool:
        """Optional 마이그레이션 전제조건 검사. 비-optional 은 항상 True.

        전제조건 SQL 이 행을 반환하면 True (적용), 아니면 False (skip).
        """
        precondition = self._OPTIONAL_MIGRATIONS.get(path.name)
        if precondition is None:
            return True
        with conn.cursor() as cur:
            cur.execute(precondition)
            return cur.fetchone() is not None

    @staticmethod
    def _ensure_migrations_table(conn: psycopg2.extensions.connection) -> None:
        with conn.cursor() as cur:
            cur.execute(_PG_MIGRATIONS_TABLE_DDL)

    @staticmethod
    def _fetch_applied(conn: psycopg2.extensions.connection) -> set[str]:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM _pg_migrations")
            return {str(row[0]) for row in cur.fetchall()}

    @classmethod
    def _apply_one(
        cls,
        conn: psycopg2.extensions.connection,
        path: Path,
    ) -> None:
        """단일 마이그레이션 파일 적용.

        호출자(``ensure_schema``)가 AUTOCOMMIT 커넥션을 제공한다. 파일 내부의
        ``BEGIN/COMMIT`` 은 서버 측에서 트랜잭션 경계를 형성하고, 그 바깥에서
        진행되는 ``INSERT INTO _pg_migrations`` 는 autocommit 으로 즉시 반영된다.
        """
        sql_text = path.read_text(encoding="utf-8")
        with conn.cursor() as cur:
            # CONCURRENTLY 는 트랜잭션 안에서 실행할 수 없다. AUTOCOMMIT 커넥션이라 안전해 보이지만,
            # 문장이 2개 이상인 파일을 한 번에 보내면 PG 가 그 simple query 전체를 **암시적 트랜잭션**
            # 으로 감싸므로 첫 CONCURRENTLY 에서 ActiveSqlTransaction 이 난다 (021 은 문장이 하나라
            # 우연히 통과했고, 문장 3개인 024 에서 드러났다 — 2026-09-03 재현). 그런 파일만 문장 단위로
            # 나눠 보낸다. 나머지 파일(BEGIN/COMMIT·DO $$ 포함)은 기존처럼 통째 실행 = 동작 불변.
            for stmt in cls._statements_for(sql_text):
                cur.execute(stmt)
            cur.execute(
                "INSERT INTO _pg_migrations (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                (path.name,),
            )

    @classmethod
    def _statements_for(cls, sql_text: str) -> list[str]:
        """실행 단위 목록. CONCURRENTLY 파일만 문장 단위로 쪼개고, 그 외엔 통째 1건."""
        if not _CONCURRENTLY_RE.search(sql_text):
            return [sql_text]
        if _EXPLICIT_BEGIN_RE.search(sql_text):
            # 파일이 스스로 트랜잭션을 열었다면 쪼개도 어차피 실패한다 — 조용히 고치지 말고
            # 원문 그대로 실행해 "트랜잭션 안에서 CONCURRENTLY" 오류를 그대로 드러낸다.
            return [sql_text]
        return _split_sql_statements(sql_text)

    @classmethod
    def _verify_assertions(
        cls,
        conn: psycopg2.extensions.connection,
        path: Path,
    ) -> None:
        """파일 안 ``-- @ASSERT_AFTER: <sql>`` 주석을 모두 검증.

        각 SQL 의 첫 컬럼이 truthy (boolean true / non-zero / non-empty) 여야 한다.
        실패 시 ``PostgresSchemaBaselineError`` 를 발생시켜 drift 를 표면화한다.

        매 부팅 시 호출되므로 (already-applied migration 도 검증):
          - migration 적용이 silent fail 한 경우 (005 사례) 즉시 fail-fast
          - 누군가 수동으로 schema 객체를 삭제한 경우도 drift 감지
        """
        import re

        sql_text = path.read_text(encoding="utf-8")
        pattern = re.compile(r"--\s*@ASSERT_AFTER:\s*(.+)$", re.MULTILINE)
        asserts = [m.group(1).strip().rstrip(";") for m in pattern.finditer(sql_text)]
        if not asserts:
            return
        failures: list[str] = []
        with conn.cursor() as cur:
            for assert_sql in asserts:
                try:
                    cur.execute(assert_sql)
                    row = cur.fetchone()
                except psycopg2.Error as exc:
                    failures.append(f"  {assert_sql} → query error: {exc}")
                    continue
                value = row[0] if row else None
                if not value:
                    failures.append(f"  {assert_sql} → returned {value!r} (expected truthy)")
        if failures:
            raise PostgresSchemaBaselineError(
                f"Migration {path.name}: post-apply assertion(s) failed:\n" + "\n".join(failures)
            )
