"""
MotherDuck 연결 공유 유틸리티

여러 스크립트에서 공통으로 사용하는 MotherDuck 연결 및 환경 설정 로직.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import duckdb


def load_dotenv_if_present(dotenv_path: Path) -> None:
    """
    .env 파일이 존재하면 환경변수로 로드.

    Args:
        dotenv_path: .env 파일 경로
    """
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            continue
        # 이미 설정된 환경변수는 덮어쓰지 않음
        if key in os.environ and os.environ[key] != "":
            continue

        # 따옴표 제거
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]

        os.environ[key] = value


def load_env_for_motherduck(
    repo_root: Optional[Path] = None,
    env_file: Optional[str] = None,
) -> None:
    """
    MotherDuck 토큰을 위한 환경변수 로드.

    우선순위:
    1. 이미 MOTHERDUCK_TOKEN이 설정되어 있으면 스킵
    2. env_file이 지정되면 해당 파일에서 로드
    3. repo_root/.env에서 로드
    4. repo_root/docker/.env에서 로드

    Args:
        repo_root: 프로젝트 루트 경로 (None이면 현재 파일 기준으로 추정)
        env_file: 명시적 .env 파일 경로
    """
    if os.getenv("MOTHERDUCK_TOKEN"):
        return

    if repo_root is None:
        # common/ -> python/ -> src/ -> repo_root
        repo_root = Path(__file__).resolve().parent.parent.parent.parent

    if env_file:
        p = Path(env_file)
        if not p.is_absolute():
            p = repo_root / p
        load_dotenv_if_present(p)
        return

    load_dotenv_if_present(repo_root / ".env")
    if not os.getenv("MOTHERDUCK_TOKEN"):
        load_dotenv_if_present(repo_root / "docker" / ".env")


def connect_motherduck(
    database: str,
    token: Optional[str] = None,
) -> duckdb.DuckDBPyConnection:
    """
    MotherDuck 데이터베이스에 연결.

    Args:
        database: MotherDuck 데이터베이스 이름
        token: MotherDuck 토큰 (None이면 환경변수에서 읽음)

    Returns:
        DuckDB 연결 객체

    Raises:
        ValueError: 토큰이 없는 경우
    """
    md_token = token or os.getenv("MOTHERDUCK_TOKEN")
    if not md_token:
        raise ValueError(
            "MotherDuck token is required. "
            "Set MOTHERDUCK_TOKEN or pass token argument."
        )
    return duckdb.connect(f"md:{database}?motherduck_token={md_token}")


def connect_motherduck_root(token: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    """
    MotherDuck 루트에 연결 (데이터베이스 생성/목록 조회용).

    Args:
        token: MotherDuck 토큰 (None이면 환경변수에서 읽음)

    Returns:
        DuckDB 연결 객체

    Raises:
        ValueError: 토큰이 없는 경우
    """
    md_token = token or os.getenv("MOTHERDUCK_TOKEN")
    if not md_token:
        raise ValueError(
            "MotherDuck token is required. "
            "Set MOTHERDUCK_TOKEN or pass token argument."
        )
    return duckdb.connect(f"md:?motherduck_token={md_token}")


def database_exists(con: duckdb.DuckDBPyConnection, database: str) -> bool:
    """
    MotherDuck에 데이터베이스가 존재하는지 확인.

    Args:
        con: MotherDuck 루트 연결
        database: 데이터베이스 이름

    Returns:
        존재 여부
    """
    try:
        rows = con.execute("SHOW DATABASES").fetchall()
    except Exception:
        return False
    return any((row and row[0] == database) for row in rows)


def quote_identifier(name: str) -> str:
    """SQL 식별자 이스케이프."""
    return '"' + name.replace('"', '""') + '"'


def quote_string(value: str) -> str:
    """SQL 문자열 이스케이프."""
    return value.replace("'", "''")


__all__ = [
    "load_dotenv_if_present",
    "load_env_for_motherduck",
    "connect_motherduck",
    "connect_motherduck_root",
    "database_exists",
    "quote_identifier",
    "quote_string",
]
