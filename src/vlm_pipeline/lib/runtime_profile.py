"""Runtime profile helpers for environment-specific policy routing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from vlm_pipeline.lib.env_utils import bool_env

RuntimeProfileName = Literal["production", "staging"]


@dataclass(frozen=True)
class RuntimeProfile:
    """Resolved runtime profile for policy decisions."""

    name: RuntimeProfileName
    is_staging: bool


def resolve_runtime_profile() -> RuntimeProfile:
    """Resolve runtime profile from environment once per call site."""
    is_staging = bool_env("IS_STAGING", False)
    return RuntimeProfile(
        name="staging" if is_staging else "production",
        is_staging=is_staging,
    )
