"""Environment-based Gemini configuration helpers."""

from __future__ import annotations

import os
from pathlib import Path

from vlm_pipeline.lib.gemini import resolve_gemini_credentials_path

GEMINI_PROJECT = (os.getenv("GEMINI_PROJECT") or "gmail-361002").strip()
GEMINI_LOCATION = (os.getenv("GEMINI_LOCATION") or "us-central1").strip()
CREDENTIALS_BASENAME = "gmail-361002-cbcf95afec4a.json"
GEMINI_SERVICE_ACCOUNT_JSON_ENV = "GEMINI_SERVICE_ACCOUNT_JSON"


def get_credentials_path() -> str:
    return resolve_gemini_credentials_path()


def get_credentials_dir() -> Path:
    return Path(__file__).resolve().parent


__all__ = [
    "CREDENTIALS_BASENAME",
    "GEMINI_LOCATION",
    "GEMINI_PROJECT",
    "GEMINI_SERVICE_ACCOUNT_JSON_ENV",
    "get_credentials_dir",
    "get_credentials_path",
]
