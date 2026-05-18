#!/usr/bin/env python3
"""Hook script invoked by Claude Code's PreToolUse hook for the Agent tool.

Reads the hook payload from stdin (JSON), extracts sub-agent invocation info,
and appends a structured entry to ``.claude/agent-log.jsonl``.

Designed to be cheap and silent: never blocks the user's tool call. Errors
go to stderr; exit code is always 0.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception as exc:
        print(f"agent_log: cannot parse stdin payload: {exc}", file=sys.stderr)
        return 0

    if payload.get("tool_name") != "Agent":
        return 0

    tool_input = payload.get("tool_input") or {}
    subagent = tool_input.get("subagent_type") or "unknown"
    description = (tool_input.get("description") or "").strip()[:120]
    model_override = tool_input.get("model") or ""
    session_id = payload.get("session_id") or ""

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "session_id": session_id,
        "subagent_type": subagent,
        "description": description,
        "model_override": model_override,
    }

    project_root = Path(os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd())
    log_path = project_root / ".claude" / "agent-log.jsonl"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"agent_log: cannot write {log_path}: {exc}", file=sys.stderr)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
