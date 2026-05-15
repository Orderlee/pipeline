"""Config / credentials — env > config file > prompt 우선순위.

env vars (CI 우선):
  GENAI_CLI_BASE
  GENAI_CLI_USER
  GENAI_CLI_PASS

config file: ~/.config/genai-cli/config.toml (mode 0600)
  [default]
  base = "http://10.0.0.10:8088"
  user = "user"
  password = "..."
"""

from __future__ import annotations

import getpass
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib   # py311+
except ImportError:  # pragma: no cover
    import tomli as tomllib   # py310 fallback


DEFAULT_BASE = "http://10.0.0.10:8088"
CONFIG_PATH = Path.home() / ".config" / "genai-cli" / "config.toml"


@dataclass
class CliConfig:
    base: str
    user: str | None
    password: str | None

    @property
    def auth(self) -> tuple[str, str] | None:
        if self.user and self.password:
            return (self.user, self.password)
        return None


def _read_config_file(path: Path = CONFIG_PATH) -> dict:
    if not path.exists():
        return {}
    # 권한 검사 — 0600 가 아니면 경고 + 무시 (비밀번호 누출 방지)
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        print(
            f"warning: {path} mode={oct(mode)} (보안상 0600 권장). 무시함.",
            file=sys.stderr,
        )
        return {}
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except Exception as exc:
        print(f"warning: config 파싱 실패 {path}: {exc}", file=sys.stderr)
        return {}


def load_config(*, base_override: str | None = None,
                interactive: bool = True) -> CliConfig:
    """env > config > prompt 순서로 base/user/pass 해결."""
    cfg_file = _read_config_file().get("default", {})
    base = (
        base_override
        or os.getenv("GENAI_CLI_BASE")
        or cfg_file.get("base")
        or DEFAULT_BASE
    ).rstrip("/")
    user = os.getenv("GENAI_CLI_USER") or cfg_file.get("user")
    password = os.getenv("GENAI_CLI_PASS") or cfg_file.get("password")

    if (not user or not password) and interactive and sys.stdin.isatty():
        if not user:
            user = input(f"username [{base}]: ").strip()
        if not password:
            password = getpass.getpass(f"password for {user}@{base}: ")
    return CliConfig(base=base, user=user or None, password=password or None)


def write_config(*, base: str, user: str, password: str,
                 path: Path = CONFIG_PATH) -> Path:
    """config 파일 작성 (mode 0600)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    body = (
        "# GenAI Studio CLI config — 자동 생성. CLI 와 환경 변수 우선순위 보강용.\n"
        "[default]\n"
        f'base = "{base}"\n'
        f'user = "{user}"\n'
        f'password = "{password}"\n'
    )
    path.write_text(body, encoding="utf-8")
    os.chmod(path, 0o600)
    return path
