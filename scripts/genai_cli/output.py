"""Output formatter — TTY 자동감지 + --json/--table override."""

from __future__ import annotations

import json
import sys
from typing import Any


def auto_format(explicit: str | None) -> str:
    """`explicit` (json | table | None) → 최종 포맷.
    None 일 때 stdout TTY 면 table, pipe 면 json.
    """
    if explicit in ("json", "table"):
        return explicit
    return "table" if sys.stdout.isatty() else "json"


def emit(data: Any, *, fmt: str = "table", columns: list[str] | None = None,
         empty_msg: str = "(empty)") -> None:
    if fmt == "json":
        print(json.dumps(data, ensure_ascii=False, indent=2, default=str))
        return

    # table mode
    if isinstance(data, dict):
        _print_dl(data)
        return
    if isinstance(data, list):
        if not data:
            print(empty_msg)
            return
        _print_table(data, columns=columns)
        return
    # primitive
    print(data)


def _print_dl(d: dict) -> None:
    width = max((len(str(k)) for k in d.keys()), default=0)
    for k, v in d.items():
        v_str = _short(v)
        print(f"{str(k):<{width}}  {v_str}")


def _print_table(rows: list[dict], columns: list[str] | None = None) -> None:
    cols = columns or _pick_columns(rows)
    widths = [max(len(c), *(len(_short(r.get(c, ""))) for r in rows)) for c in cols]
    sep = "  "
    header = sep.join(c.upper().ljust(w) for c, w in zip(cols, widths))
    print(header)
    print(sep.join("-" * w for w in widths))
    for r in rows:
        line = sep.join(_short(r.get(c, "")).ljust(w) for c, w in zip(cols, widths))
        print(line)


def _pick_columns(rows: list[dict]) -> list[str]:
    seen: list[str] = []
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.append(k)
    return seen[:8]


def _short(v: Any, n: int = 60) -> str:
    if v is None:
        return "—"
    s = str(v)
    if len(s) > n:
        return s[: n - 1] + "…"
    return s
