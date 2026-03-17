"""Dagster thin entrypoint.

Legacy monolithic definitions were moved to ``src/vlm_pipeline``.
This module keeps Dagster workspace compatibility by delegating ``defs``.
"""

from vlm_pipeline.definitions import defs

__all__ = ["defs"]
