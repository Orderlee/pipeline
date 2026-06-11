"""Gemini response text extraction and JSON cleaning helpers (no vertexai dependency)."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


def _collect_text_from_parts(parts: Any) -> list[str]:
    collected: list[str] = []
    for part in parts or []:
        text_value = None
        if isinstance(part, dict):
            text_value = part.get("text")
        else:
            try:
                text_value = getattr(part, "text", None)
            except Exception:
                text_value = None
        rendered = str(text_value or "").strip()
        if rendered:
            collected.append(rendered)
    return collected


def _extract_response_text(response: Any) -> str:
    try:
        text_value = getattr(response, "text", None)
    except Exception:
        text_value = None

    rendered = str(text_value or "").strip()
    if rendered:
        return rendered

    collected: list[str] = []
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = None
        if isinstance(candidate, dict):
            content = candidate.get("content")
        else:
            try:
                content = getattr(candidate, "content", None)
            except Exception:
                content = None

        if isinstance(content, dict):
            parts = content.get("parts")
        else:
            try:
                parts = getattr(content, "parts", None)
            except Exception:
                parts = None
        collected.extend(_collect_text_from_parts(parts))

    joined = "\n".join(part for part in collected if part).strip()
    if joined:
        return joined

    raise RuntimeError("Gemini response did not contain readable text parts")


def extract_clean_json_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    cleaned = re.sub(r"^(?i:json)\s*\n", "", cleaned)
    start_candidates = [pos for pos in (cleaned.find("["), cleaned.find("{")) if pos != -1]
    if start_candidates:
        cleaned = cleaned[min(start_candidates) :]

    end = max(cleaned.rfind("]"), cleaned.rfind("}"))
    if end != -1:
        cleaned = cleaned[: end + 1]
    return cleaned.strip()


def _repair_clean_json_strings(text: str) -> str:
    """Best-effort repair of common Gemini JSON string glitches.

    Inside string values, replaces raw LF/CR/TAB with `\\n`/`\\r`/`\\t`
    and escapes an internal `"` whose next non-whitespace char is not a
    JSON structural delimiter (`,`, `:`, `]`, `}`, EOF).
    """
    out: list[str] = []
    in_string = False
    escape = False
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if escape:
            out.append(ch)
            escape = False
            i += 1
            continue
        if ch == "\\":
            out.append(ch)
            escape = True
            i += 1
            continue
        if in_string:
            if ch == '"':
                j = i + 1
                while j < n and text[j] in " \t\r\n":
                    j += 1
                next_ch = text[j] if j < n else ""
                if next_ch in (",", ":", "]", "}", ""):
                    in_string = False
                    out.append(ch)
                else:
                    out.append('\\"')
                i += 1
                continue
            if ch == "\n":
                out.append("\\n")
                i += 1
                continue
            if ch == "\r":
                out.append("\\r")
                i += 1
                continue
            if ch == "\t":
                out.append("\\t")
                i += 1
                continue
            out.append(ch)
            i += 1
        else:
            if ch == '"':
                in_string = True
            out.append(ch)
            i += 1
    return "".join(out)


def load_clean_json(text: str) -> Any:
    cleaned = extract_clean_json_text(text)
    decoder = json.JSONDecoder()
    try:
        return decoder.decode(cleaned)
    except json.JSONDecodeError as exc:
        if exc.msg == "Extra data":
            payload, _ = decoder.raw_decode(cleaned)
            return payload
        repaired = _repair_clean_json_strings(cleaned)
        if repaired != cleaned:
            try:
                payload = decoder.decode(repaired)
                logger.warning("load_clean_json: repaired malformed JSON (orig_err=%s)", exc)
                return payload
            except json.JSONDecodeError:
                try:
                    payload, _ = decoder.raw_decode(repaired)
                    logger.warning(
                        "load_clean_json: repaired malformed JSON via raw_decode (orig_err=%s)",
                        exc,
                    )
                    return payload
                except json.JSONDecodeError:
                    pass
        raise
