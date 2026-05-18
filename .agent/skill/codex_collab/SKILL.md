---
description: Collaborative workflow for general code edits (bug fixes, small feature additions, API changes, code review) — discuss with Codex first, then apply with user approval
---

> Multi-agent routing, effort, and escalation policy: [`docs/references/multi-agent.md`](../../../docs/references/multi-agent.md)

# 🎯 Skill Purpose (Trigger)

Before applying a non-trivial code change, get one review pass from Codex (OpenAI) so changes are applied only after both models agree. Trigger in the following situations:

- **Bug fixes** — "Function X behaves incorrectly under condition Y, fix it"
- **Small feature additions** — "Accept option Z and handle it", "Add dry-run mode to this sensor"
- **API/signature changes** — "I'm changing the arguments of this function, how should I do it"
- **Code review** — "Review this PR diff", "Is this function safe"
- **User explicitly requests "let's do it with Codex" / "have Codex review" / "/codex_collab"**

For refactoring (file splitting, renaming, structural rearrangement) where **logic byte preservation** is the key concern, use the [codex_refactor](../codex_refactor/SKILL.md) skill instead. This skill is for general code edits where **logic actually changes**.

## Cases Where a Codex Call Is Not Appropriate (Handle Directly)

Tasks where Codex validation has low cost-to-value per the multi-agent §3.1 matrix:
- Obvious NPE / KeyError one-line patches
- One-line import additions/removals
- Trivial typos, message text corrections
- Simple cases of only adding an argument to a single function (no signature impact)

In these cases, the main agent handles directly.

# 🛠️ Dependencies

- **Sub-agent:** `.claude/agents/codex.md` — `Agent(subagent_type="codex", ...)`
- **MCP server:** `codex` in project-scope `.mcp.json` (`@nayagamez/codex-cli-mcp`). Exposed tools: `mcp__codex__codex`, `mcp__codex__codex-reply`
- **Codex CLI:** `/snap/bin/codex` (codex-cli 0.114.0+)
- **Known breakage:** See the "ajv-formats" entry in [codex_refactor SKILL.md](../codex_refactor/SKILL.md)

# 📝 Action Steps

## 1. Identify Target + Gather Context
- From the user request, extract: (a) change target (file/function/module), (b) desired behavioral change, (c) constraints (backward compatibility, performance, test pass requirements).
- **In this skill, the main agent reads the code directly first to understand context.** Unlike refactoring, since logic is changing, the main agent must accurately understand the current behavior.
- If ambiguous, confirm once with `AskUserQuestion`. Do not guess.

## 2. Delegate to Codex (Sub-agent Call)

```python
Agent(
  subagent_type="codex",
  description="codex code change review",
  prompt="""
  Ask kind: general edit  # or analysis only (when only a code review is needed)
  Effort: high  # for general fixes. extra_high for security/financial/migration. medium for <50 LoC changes.
  Target: <absolute file path>:<line range or function name>
  Current behavior: <1-3 lines summarizing what the main agent read from the code>
  Desired change: <the behavioral change the user wants>
  Constraints:
  - <e.g.: preserve existing caller signatures, maintain graceful skip for transient errors, ...>
  - <e.g.: must not break test X, ignore env var Y>

  Deliverable: change proposal (lines/functions to modify, intent, risk level, edge cases, whether additional tests are needed).
  Also answer: whether the logic change is justified, and whether a smaller alternative exists.
  DO NOT modify any files.
  """
)
```

- For a code review, change "Ask kind" in the prompt to `analysis only` and in place of "Desired change", specify the review angle ("performance", "security", "concurrency", "error handling").
- Continue follow-up questions using `mcp__codex__codex-reply` + threadId to maintain thread continuity where possible.
- Effort follows the multi-agent §3.3 matrix. If the caller (main agent) omits this hint, the codex sub-agent defaults to `high`.

## 3. Review the Proposal
- Output the sub-agent response to the user as-is.
- If the main agent sees questions or gaps, present its own opinion alongside (without taking sides). If the two models disagree, surface that fact plainly.

## 4. Request Approval
- Use `AskUserQuestion` with options:
  - "Apply"
  - "Modify then apply"
  - "Incorporate my feedback and ask Codex again" — follow-up maintaining threadId
  - "Have both Sonnet and Codex solve it" → delegate to [codex_arbitration](../codex_arbitration/SKILL.md) (multi-agent §4.2 Pattern B — effective for hard algorithms)
  - "Request Opus arbitration" → spec §7.2 trigger: models disagree / big security or performance decision / both models fail the tests
  - "Cancel"
- When "Ask again" is selected, re-call Codex with a new prompt (maintaining threadId).

## 5. Apply
- Main agent applies changes with `Edit` / `Write`.
- **Since this skill changes actual logic**, the byte-preservation principle does not apply. However, scope of changes must stay within the user-approved range.
- Apply in commit-ready logical units. Unrelated cleanup (comments, formatting) goes in a separate commit.

## 6. Verify
Choose appropriate verification based on the type of change:

```bash
# lint
ruff check <touched paths>

# unit tests (add new ones if needed)
pytest tests/unit/<relevant> -q

# import smoke
docker exec docker-dagster-code-server-1 python3 -c "from <module> import <syms>; print('ok')"

# Dagster definitions load (if asset/sensor/op was touched)
docker exec docker-dagster-code-server-1 python3 -c "
from vlm_pipeline.definitions import defs
print(len(list(defs.get_asset_graph().all_asset_keys)))"

# If possible, materialize once in staging Dagster UI (http://10.0.0.10:3031)
```

If you touched an area without tests, ask the user once whether to add the tests Codex suggested.

## 7. Commit (When Requested by User)
- Conventional commit. Choose `fix:` / `feat:` / `chore:` etc. as appropriate for the change type.
- Note Codex collaboration in the footer:
  ```
  fix(<area>): <what changed and why>

  Codex-assisted: yes
  ```

# 🚫 Constraints

- **The codex sub-agent must never directly Edit/Write.** Verify "DO NOT modify any files" at the end of every call prompt.
- **Do not call Codex for trivial changes.** Typos, one-line import additions, obvious NPE patches — handle directly to avoid wasting Codex cost/time.
- **Surface it when Codex opposes the user's goal.** Even if the main agent agrees with Codex's opinion, show both opinions to the user and delegate the decision.
- **Assess impact scope when logic changes.** On signature changes, grep for callers; on behavioral changes, check related tests/sensors/schedules.
- **Production-impacting work goes to staging first.** dispatch / sensor / migration / asset changes should be tested in the staging environment (`:3031`) before production deployment.
- **No direct commits to main/dev.** Always work on a feature/fix branch.
- **Do not call Codex when secrets are visible.** If `.env`, credentials, `OPENAI_API_KEY`, etc. risk exposure in the prompt, block the call and notify the user.
- **When Opus is unavailable — Sonnet orchestrator mode (spec §8)**: Break work into smaller pieces and increase Codex dependency. For data model/schema changes, external API contract changes, security/auth code, and changes with production deployment impact — proceed only after user confirmation. Record deferred decisions in `decisions.log` for batch review when Opus returns.
