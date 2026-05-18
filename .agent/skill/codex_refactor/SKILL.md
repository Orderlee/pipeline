---
description: Collaborative workflow to receive code analysis and refactoring proposals from Codex (OpenAI), then apply and verify with user approval
---

> Multi-agent routing, effort, and escalation policy: [`docs/references/multi-agent.md`](../../../docs/references/multi-agent.md)

# 🎯 Skill Purpose (Trigger)

- When the user requests "refactor X with Codex", "review file X together with Codex", or "/codex_refactor X"
- When a second opinion is needed for non-trivial splitting / renaming / structural changes
- When proceeding with a Phase C pattern split (large file → domain submodules + facade) based on Codex's proposal

This skill is exclusively for **refactoring where logic byte preservation is the core concern**. Use [codex_collab](../codex_collab/SKILL.md) for the following:
- Bug fixes, small feature additions, API/signature changes → general code edits where **logic actually changes**
- Analysis-only questions where no apply step is needed

When this skill is triggered, the main agent must receive a proposal from the codex sub-agent and show it to the user before directly using Edit/Write.

## Cases Where a Codex Call Is Not Appropriate (Handle Directly)

Tasks where Codex validation has low cost-to-value per the multi-agent §3.1 matrix:
- Single-file variable renaming, import cleanup
- Adding simple type annotations
- Trivial typos, one-line fixes
- Pure docstring/comment edits

In these cases the main agent handles directly. Avoid Codex cost/latency.

# 🛠️ Dependencies

- **Sub-agent:** `.claude/agents/codex.md` — native Claude Code location. Called with `Agent(subagent_type="codex", ...)`.
- **MCP server:** `codex` entry in project-scope `.mcp.json` (`@nayagamez/codex-cli-mcp`). Exposed tools: `mcp__codex__codex`, `mcp__codex__codex-reply`.
- **Codex CLI backend:** `/snap/bin/codex` (codex-cli 0.114.0+). The MCP server runs this CLI as a subprocess.
- **Known breakage:** `@nayagamez/codex-cli-mcp` is missing `ajv-formats` as a transitive dep. Before first use, run `npm install ajv-formats` once in the npx cache (`~/.npm/_npx/<hash>/`).

# 📝 Action Steps

The AI agent executing this skill must follow the steps below in order.

## 1. Identify Target
- From the user request, extract: (a) absolute path of target file, (b) goal (e.g., "split into 4 domain submodules, keep facade"), (c) constraints (preserve public API, preserve logic bytes, no caller modifications).
- If ambiguous, confirm once with `AskUserQuestion`. Do not guess.

## 2. Delegate to Codex (Sub-agent Call)

```python
Agent(
  subagent_type="codex",
  description="codex refactoring proposal",
  prompt="""
  Target file: <absolute path>
  Goal: <one-line goal>
  Effort: high  # for general splits (>50 LoC). extra_high for security/auth code splits. medium for import-only cleanups.

  Constraints:
  - Public API preserved (callers <list> should continue to import existing symbols).
  - Function bodies must be byte-identical (no logic changes mixed in).
  - Original file becomes a thin facade re-exporting from new submodules.

  Deliverable: a structured proposal — submodule names, what goes in each, risk of cross-submodule cycles, suggested apply order.
  DO NOT modify any files. Read-only proposal only.
  """
)
```

The sub-agent calls Codex via the MCP's `sandbox: "read-only"` mode so Codex cannot write files. Effort follows the multi-agent §3.3 matrix.

**Pattern B alternative**: For splits involving difficult algorithms (e.g., concurrent cursor serialization, lock ordering changes), use [codex_arbitration](../codex_arbitration/SKILL.md) — give both Sonnet and Codex the problem independently, then have the main agent compare and arbitrate.

## 3. Review Proposal
- Output the proposal returned by the sub-agent to the user as-is.
- Gather questions or revision feedback from the user. If a follow-up question is needed, call the same sub-agent once more — preferably using `mcp__codex__codex-reply` + threadId to maintain thread continuity (so Codex retains prior context).

## 4. Request Approval
- Confirm whether to apply with `AskUserQuestion`. Example options: ["Apply", "Modify then apply", "Cancel"].
- If the user selects "Modify then apply", incorporate the changes and propose a new split → back to step 4.

## 5. Apply
- The main agent (not the sub-agent) applies changes with `Edit` / `Write`.
- Apply one file at a time, in sequence. **Preserve function body bytes** — move code only, no logic changes. If improvements or bug fixes surface, separate them into a different commit.
- After each file:
  - `ruff check <touched paths>`
  - `python3 -c "from <facade module> import <public symbols>"` smoke

## 6. Verify
When all changes are applied:
```bash
# 1) lint
ruff check src/vlm_pipeline/

# 2) facade import smoke (all public symbols importable from original path)
docker exec docker-dagster-code-server-1 python3 -c "from <module> import <syms>; print('ok')"

# 3) Dagster definitions load
docker exec docker-dagster-code-server-1 python3 -c "
from vlm_pipeline.definitions import defs
print(len(list(defs.get_asset_graph().all_asset_keys)))"

# 4) relevant unit tests (on host or in container, whichever has pytest)
pytest tests/unit/<relevant> -q
```

## 7. Commit (When Requested by User)
- Conventional commit message + note Codex collaboration in the footer:
  ```
  refactor(<area>): <what> + facade

  Codex-assisted: yes
  ```
- Commit separation principle: 1 split = 1 commit. Do not commit changes that have not passed verification.

# 🚫 Constraints

- **The codex sub-agent must never directly Edit/Write.** Verify "DO NOT modify any files" at the end of every call prompt.
- **No direct commits to main/dev branches.** Always work on a feature/refactor-* branch.
- **Do not apply Codex proposals uncritically.** User approval is required at every step.
- **Surface it when Codex opposes the user's goal.** The sub-agent will note conflicts in the proposal's "Open questions / risks" section — the main agent must relay that to the user as-is.
- **Codex calls incur cost and time.** Trivial changes (typos, one-line edits, renaming a single identifier) are handled directly without calling Codex.
- **Logging/comment cleanup in a separate round.** Mixing them into split commits degrades diff readability.
- **When Opus is unavailable — Sonnet orchestrator mode (spec §8)**: Break work into smaller pieces and increase Codex dependency. Security/migration/schema changes require user confirmation before proceeding; defer decisions to `decisions.log`.
