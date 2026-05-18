#!/usr/bin/env python3
"""Quarterly stats reporter for sub-agent invocations.

Parses ``.claude/agent-log.jsonl`` (populated by ``scripts/agent_log.py`` via
the PreToolUse hook in ``.claude/settings.json``) and prints aggregates to
help review whether the routing matrix is being followed and whether the
target model distribution holds.

Usage:
    python3 scripts/agent_stats.py
    python3 scripts/agent_stats.py --since 2026-05-01
    python3 scripts/agent_stats.py --since 2026-05-01 --json
    python3 scripts/agent_stats.py --log path/to/agent-log.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Default model per project sub-agent. Mirrors docs/references/agent-teams.md §1.
AGENT_MODEL = {
    "qa-strategist": "opus",
    "dagster-impl": "sonnet",
    "pipeline-explorer": "sonnet",
    "deploy-auditor": "sonnet",
    "codex": "sonnet-liaison",
}


def _parse_log(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as fh:
        for i, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"warning: malformed entry on line {i}: {exc}", file=sys.stderr)
    return rows


def _filter_since(rows: list[dict], since_iso: str) -> list[dict]:
    cutoff = datetime.fromisoformat(since_iso).replace(tzinfo=timezone.utc)
    out = []
    for row in rows:
        try:
            row_ts = datetime.fromisoformat(row["ts"].replace("Z", "+00:00"))
        except Exception:
            continue
        if row_ts >= cutoff:
            out.append(row)
    return out


def _model_of(subagent: str, override: str) -> str:
    if override:
        return override
    return AGENT_MODEL.get(subagent, "inherited")


def _emit_markdown(rows: list[dict], since: str | None) -> None:
    total = len(rows)
    by_agent = Counter(r["subagent_type"] for r in rows)
    by_day: dict[str, int] = defaultdict(int)
    sessions: set[str] = set()
    by_model: Counter[str] = Counter()
    for r in rows:
        by_day[r["ts"][:10]] += 1
        sessions.add(r.get("session_id", ""))
        by_model[_model_of(r["subagent_type"], r.get("model_override", ""))] += 1

    print("# Agent stats")
    print()
    print(f"- **Total calls**: {total}")
    print(f"- **Distinct sessions**: {len(sessions)}")
    if since:
        print(f"- **Window**: since {since}")
    print()
    print("## By sub-agent type")
    print()
    print("| Sub-agent | Calls | Share |")
    print("|---|---:|---:|")
    for agent, count in by_agent.most_common():
        share = (count / total * 100) if total else 0
        print(f"| `{agent}` | {count} | {share:.1f}% |")
    print()
    print("## By underlying model (target: Opus 10-20% / Sonnet 60-70% / Codex 15-25%)")
    print()
    print("| Model | Calls | Share |")
    print("|---|---:|---:|")
    for model, count in by_model.most_common():
        share = (count / total * 100) if total else 0
        print(f"| `{model}` | {count} | {share:.1f}% |")
    print()
    print("## By day")
    print()
    print("| Date | Calls |")
    print("|---|---:|")
    for day, count in sorted(by_day.items()):
        print(f"| {day} | {count} |")


def _emit_json(rows: list[dict], since: str | None) -> None:
    total = len(rows)
    by_agent = Counter(r["subagent_type"] for r in rows)
    by_day: dict[str, int] = defaultdict(int)
    sessions: set[str] = set()
    by_model: Counter[str] = Counter()
    for r in rows:
        by_day[r["ts"][:10]] += 1
        sessions.add(r.get("session_id", ""))
        by_model[_model_of(r["subagent_type"], r.get("model_override", ""))] += 1
    payload = {
        "total_calls": total,
        "distinct_sessions": len(sessions),
        "since": since,
        "by_subagent_type": dict(by_agent),
        "by_model": dict(by_model),
        "by_day": dict(sorted(by_day.items())),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--since", help="ISO date, e.g. 2026-05-01")
    parser.add_argument("--json", action="store_true", help="emit JSON")
    parser.add_argument("--log", default=".claude/agent-log.jsonl",
                        help="path to JSONL log file")
    args = parser.parse_args()

    rows = _parse_log(Path(args.log))
    if args.since:
        rows = _filter_since(rows, args.since)
    if not rows:
        print("No agent call entries found.", file=sys.stderr)
        return 1
    if args.json:
        _emit_json(rows, args.since)
    else:
        _emit_markdown(rows, args.since)
    return 0


if __name__ == "__main__":
    sys.exit(main())
