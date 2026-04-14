"""Runtime profile helpers for environment-specific policy routing."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Literal

RuntimeProfileName = Literal["production", "test"]


@dataclass(frozen=True)
class RuntimeProfile:
    """Resolved runtime profile for policy decisions."""

    name: RuntimeProfileName

    @property
    def is_test(self) -> bool:
        return self.name == "test"


def resolve_runtime_profile() -> RuntimeProfile:
    """Resolve runtime profile from environment once per call site."""
    raw_name = str(os.getenv("PIPELINE_RUNTIME_ENV") or "").strip().lower()

    if raw_name in {"production", "prod", "main"}:
        return RuntimeProfile(name="production")
    if raw_name in {"test", "dev", "staging"}:
        return RuntimeProfile(name="test")
    return RuntimeProfile(name="production")
