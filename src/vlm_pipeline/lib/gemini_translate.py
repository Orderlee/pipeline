"""Korean→English caption batch translation via Gemini (L1-2 lib — no dagster/defs/resources).

Used by the caption_embedding asset to translate Korean captions before feeding them to
PE-Core-L14-336 text encoder (English-centric: ko→en fixes near-zero cross-modal alignment).

Design:
- Batched: N captions → 1 Gemini call with a numbered list prompt.
- Fail-forward: if a translation is missing/unparseable, the original Korean text is kept
  so the caller never loses a caption.
- Batch size: configurable, default 50 (stays within Gemini token budget per call).
"""

from __future__ import annotations

import logging
import re
from typing import Callable

log = logging.getLogger(__name__)

DEFAULT_TRANSLATE_BATCH_SIZE = 50

# Minimum fraction of batch indices that must parse successfully and uniquely;
# below this threshold the whole batch is treated as misnumbered and originals are returned.
_MIN_PARSE_FRACTION = 0.6

_TRANSLATE_PROMPT_TMPL = """\
Translate each numbered Korean caption to concise English.
Return ONLY the numbered list in the SAME format, one per line. No explanations.

{items}"""


def _sanitize_caption(text: str) -> str:
    """Collapse newlines/control chars to spaces so each caption is a single prompt line."""
    return " ".join(str(text).split())


def _build_prompt(texts: list[str]) -> str:
    # Sanitize each caption to one line to prevent phantom numbered lines from embedded newlines.
    items = "\n".join(f"{i + 1}. {_sanitize_caption(t)}" for i, t in enumerate(texts))
    return _TRANSLATE_PROMPT_TMPL.format(items=items)


def _parse_translated_lines(response_text: str, count: int) -> dict[int, str] | None:
    """Parse numbered list response → {1-based-index: translated_text}.

    Accepts lines starting with "N." or "N)" optionally followed by whitespace.
    Duplicate indices (Gemini renumber/hallucination) are skipped after the first occurrence.
    Returns None when the fraction of successfully-parsed unique indices is below
    _MIN_PARSE_FRACTION (whole-batch fallback signal); otherwise returns the dict of parsed
    entries (missing indices are absent — per-index fallback to original applies at call site).
    """
    result: dict[int, str] = {}
    pattern = re.compile(r"^\s*(\d+)[.)]\s*(.+)$")
    for line in response_text.splitlines():
        m = pattern.match(line)
        if not m:
            continue
        idx = int(m.group(1))
        if 1 <= idx <= count:
            translated = m.group(2).strip()
            if not translated:
                continue
            if idx in result:
                log.warning(
                    "_parse_translated_lines: duplicate index %d in response, skipping second occurrence",
                    idx,
                )
                continue
            result[idx] = translated

    if count > 0 and len(result) < _MIN_PARSE_FRACTION * count:
        log.warning(
            "_parse_translated_lines: only %d/%d indices parsed (< %.0f%%), "
            "treating whole batch as failed → returning None for originals fallback",
            len(result),
            count,
            _MIN_PARSE_FRACTION * 100,
        )
        return None
    return result


def translate_captions_to_english(
    texts: list[str],
    *,
    gemini_generate: Callable[[str], str],
    batch_size: int = DEFAULT_TRANSLATE_BATCH_SIZE,
) -> list[str]:
    """Translate a list of Korean captions to English using batched Gemini calls.

    Args:
        texts: Input captions (any language; intended for Korean).
        gemini_generate: Callable that takes a prompt string and returns the model's text
            response. Typically ``lambda prompt: analyzer.model.generate_content([prompt]).text``
            or a wrapper around GeminiAnalyzer._generate_content_with_retry.
        batch_size: Max captions per Gemini call (default 50).

    Returns:
        List of the same length as ``texts``.  Each element is the English translation of
        the corresponding input, or the original text if translation failed/was missing.
    """
    if not texts:
        return []

    results: list[str] = list(texts)

    for batch_start in range(0, len(texts), batch_size):
        batch = texts[batch_start : batch_start + batch_size]
        try:
            prompt = _build_prompt(batch)
            response_text = gemini_generate(prompt)
            parsed = _parse_translated_lines(response_text, len(batch))
            if parsed is None:
                # Below-threshold parse → whole batch falls back to originals (already set).
                log.warning(
                    "translate_captions_to_english: batch [%d:%d] below parse threshold, keeping originals",
                    batch_start,
                    batch_start + len(batch),
                )
                continue
            translated_count = 0
            for local_idx, text in enumerate(batch, start=1):
                global_idx = batch_start + local_idx - 1
                if local_idx in parsed:
                    results[global_idx] = parsed[local_idx]
                    translated_count += 1
                else:
                    log.warning(
                        "translate_captions_to_english: no translation for item %d (batch offset %d), "
                        "keeping original",
                        global_idx,
                        local_idx,
                    )
            log.info(
                "translate_captions_to_english: batch [%d:%d] translated=%d/%d",
                batch_start,
                batch_start + len(batch),
                translated_count,
                len(batch),
            )
        except Exception as exc:
            log.warning(
                "translate_captions_to_english: batch [%d:%d] failed (%s), keeping originals",
                batch_start,
                batch_start + len(batch),
                exc,
            )

    return results
