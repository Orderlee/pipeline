---
description: Workflow for problems where difficult algorithms or edge conditions are central — assign the same spec independently to both Sonnet and Codex, compare answers, and arbitrate (multi-agent spec §4.2 Pattern B)
---

> Multi-agent routing, effort, and escalation policy: [`docs/references/multi-agent.md`](../../../docs/references/multi-agent.md)

# 🎯 Skill Purpose (Trigger)

The concrete workflow for multi-agent spec §4.2 Pattern B. The same hard problem is given to Sonnet and Codex **independently**, their two answers are compared, and either the better one is adopted or a new answer combining the strengths of both approaches is written.

Trigger in the following situations:
- The user explicitly requests "solve it both ways", "get a second opinion", "/codex_arbitration", etc.
- Difficult algorithmic correctness (e.g., concurrent cursor serialization, race-free state machines, optimization)
- Code with potential race conditions or deadlocks
- Security boundaries / permission checks / input validation edge cases
- Large data model changes (schema, migration strategy)
- When the user selects the "Have both Sonnet and Codex solve it" option in Step 4 of [codex_collab](../codex_collab/SKILL.md) or [codex_refactor](../codex_refactor/SKILL.md)

Do NOT use in the following cases:
- Trivial tasks, simple transformations (Pattern B cost exceeds value)
- Problems without a means of validating correctness (tests/spec) → comparison is impossible. First ask the user to write tests.

# 🛠️ Dependencies

- **Codex sub-agent**: [`.claude/agents/codex.md`](../../../.claude/agents/codex.md) — `Agent(subagent_type="codex", ...)`
- **Sonnet role**: Use a dedicated Sonnet sub-agent if available. Otherwise the main agent attempts the Sonnet solution directly.
- **Opus arbitration (optional)**: Trigger §7.2 escalation when the two answers differ meaningfully or both are insufficient.

# 📝 Action Steps

## 1. Finalize the Spec (Main Agent)

The main agent first structures the problem as follows:

```
Problem: <one-line summary>
Inputs: <types, ranges, constraints>
Outputs: <types, format, postconditions>
Constraints:
- <time/space complexity>
- <thread safety>
- <backward caller compatibility>
- <other>
Edge cases to handle:
- <edge case 1>
- <edge case 2>
Success criteria: <test set, validation method>
```

If there is any ambiguity, confirm once with `AskUserQuestion` then finalize the spec. If the spec itself is incomplete, do not trigger Pattern B — start from the analysis phase with codex_collab instead.

## 2. Sonnet Attempt (Main Agent or Sonnet Sub-agent)

- The main agent solves it directly, or delegates to a Sonnet sub-agent passing only the spec.
- Store the resulting answer in a separate area. **Never expose it to the Codex call** (to prevent contamination/imitation).
- If possible, also write unit tests.

## 3. Codex Independent Attempt (codex sub-agent)

```python
Agent(
  subagent_type="codex",
  description="codex independent attempt (Pattern B)",
  prompt="""
  Ask kind: independent solution (Pattern B from multi-agent spec §4.2)
  Effort: ultra  # Algorithm difficulty is the default trigger, so ultra per §3.3 matrix

  Problem: <spec from above — absolutely do not include the Sonnet answer>
  Inputs: ...
  Outputs: ...
  Constraints: ...
  Edge cases: ...
  Success criteria: ...

  Deliverable:
  - Completed function/module code (whole file or unified diff)
  - Time/space complexity analysis
  - List of edge cases handled
  - Edge cases not handled / intentionally ignored (if any)
  - Recommended unit test set

  DO NOT modify any files. Read-only proposal only.
  """
)
```

- **Ensemble integrity rule**: If the Sonnet answer or hints about the Sonnet approach appear in the prompt, Pattern B loses its meaning. Review the prompt text once more before submitting.

## 4. Write Comparison Table (Main Agent)

Compare the two answers across 4–5 axes:

| Axis | Sonnet | Codex |
|---|---|---|
| Correctness (tests passed) | ✓/✗ + number of cases passed | same |
| Time complexity | O(n log n) | O(n) |
| Space complexity | O(1) | O(n) |
| Readability / maintainability | assessment | assessment |
| Edge cases handled | list | list |
| Risks / assumptions | list | list |

Show the table to the user **together with both answers in full text**. The main agent must not take sides.

## 5. User Decision (`AskUserQuestion`)

Options:
- "Adopt Sonnet's solution"
- "Adopt Codex's solution"
- "Main agent writes a new solution combining the strengths of both"
- "Request Opus arbitration" — when both are insufficient / opinions differ and decision is difficult → §7.2 escalation
- "Cancel / re-spec"

## 6. Apply + Verify (Main Agent)

Apply the chosen solution with `Edit` / `Write`. The following verifications are required:

- Apply the same unit test set to both answers (if only one passes, reflect that in the adoption decision)
- Edge case regression tests
- ruff / lint
- Related integration tests (if any)

## 7. (If Needed) Opus Arbitration

When the user selects "Opus arbitration" in Step 5:
- Pass both answers in full text + comparison table + spec to Opus in one call
- Ask Opus to (a) adopt the better one, (b) write a new answer combining the strengths of both, or (c) review whether the spec itself has gaps
- Show the Opus answer to the user again, then proceed to Step 6

# 🚫 Constraints

- **Ensemble integrity**: Do not include the Sonnet answer or any variation of it in the Codex call prompt. If the two models imitate each other, Pattern B loses its meaning. Mandatory prompt review.
- **Show both answers to the user**: The main agent must not take sides or show only one answer. Present the comparison table together.
- **Avoid Pattern B without tests**: Without a means of validating correctness, it's impossible to judge which answer is correct. First ask the user to write tests, then proceed.
- **Cost awareness**: ultra × Codex call + Sonnet attempt cost. Do not use for trivial problems. Same context as the spec §3.3 "no low" rule — it's better not to call at all than to call weakly.
- **Codex sub-agent must not directly Edit/Write** (codex sub-agent contract).
- **No direct commits to main/dev**. Always use a feature/fix branch.
- **Block secrets exposure risk**: If there is a risk of `.env`, credentials, `OPENAI_API_KEY`, etc. appearing in the prompt, block the call and notify the user.
- **When Opus is unavailable (spec §8)**: Confirm the Sonnet orchestrator mode rules before triggering Pattern B. Decisions requiring Opus arbitration should be deferred and recorded in `decisions.log`.
