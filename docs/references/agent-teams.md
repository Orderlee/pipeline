# Agent Teams — VLM Data Pipeline

> This document elaborates the 3-tier model roles from [`multi-agent.md`](multi-agent.md) (Opus orchestrator / Sonnet implementer / Codex validator) into **this project's concrete sub-agent layout**. It is a routing guide: given a task, which team to assemble.

## 1. Team members (sub-agents)

| Sub-agent | Model | Mode | Role (multi-agent.md mapping) |
|---|---|---|---|
| (main session) | Opus 4.7 | r/w | Tier 1 — orchestrator / architect (§2.1) |
| [`qa-strategist`](../../.claude/agents/qa-strategist.md) | Opus 4.7 | read-only | Tier 1-style — QA strategy, coverage gap analysis, test plan design. Authoring and execution are delegated |
| [`dagster-impl`](../../.claude/agents/dagster-impl.md) | Sonnet 4.6 | r/w | Tier 2 — main implementer (§2.2). Project rules built-in: 5-layer import, `duckdb_writer` tag, fail-forward, archive policy, MinIO key conventions |
| [`pipeline-explorer`](../../.claude/agents/pipeline-explorer.md) | Sonnet 4.6 | read-only | Tier 2 — project-specific navigator. Compressed answers about code location and flow |
| [`deploy-auditor`](../../.claude/agents/deploy-auditor.md) | Sonnet 4.6 | read-only | Tier 2 — CI/CD blast radius auditor + prod/staging drift detector |
| [`codex`](../../.claude/agents/codex.md) | Sonnet 4.6 liaison → GPT-5.5 | read-only | Tier 2 — cross-validator (§2.3). Different family, second perspective. Effort matrix §3.3 |

In addition, **skills** in `.agent/skill/` are workflow macros that compose the above sub-agents — `codex_collab`, `codex_arbitration`, `codex_refactor`, `codex_db_migration`, `dagster_lineage_fixer`, `staging_reset`, `duckdb_staging_wiper`, `daily_worklog`.

### 1.1 Model allocation (reflecting multi-agent.md spirit)

Agent count does not equal model usage. In actual workflow, models split as follows.

| Model | Assigned work | Target call frequency | Call path |
|---|---|---|---|
| **Opus 4.7** | what & why — task decomposition, architecture decisions, QA strategy, final stage of difficult debugging, multi-model arbitration | ~10–20% | main session + `qa-strategist` |
| **Sonnet 4.6** | how — implementation, unit test authoring, exploration, general auditing, review worker | ~60–70% | `dagster-impl`, `pipeline-explorer`, `deploy-auditor`, `codex` liaison shell |
| **GPT-5.5 (Codex)** | is-it-correct — cross-validation, test code quality review (§3.3 `medium`), security/migration `extra_high` validation | ~15–25% | `codex` agent + `codex_*` skills |

**Principles**:
1. Decisions that require reasoning effort (architecture, QA strategy, security judgment) → Opus
2. Pattern/implementation work → Sonnet
3. Work that benefits from a different-family second look → Codex
4. **what & why** questions ("where should this code live?" / "what tests should we add?") → Opus. **how** questions ("implement this function" / "write this test") → Sonnet. **is-it-correct** questions ("is this code safe?" / "does this test really catch the gap?") → Codex.

## 2. Task type → team routing

This is `multi-agent.md` §3.1's general matrix adapted to this project. The main session (Opus) consults this table at the start of each task to decide delegation.

| Task type | Primary owner | Secondary / validator | Notes |
|---|---|---|---|
| **Test strategy / coverage gap analysis / plan design** | **`qa-strategist`** (Opus) | `codex` `medium` (second opinion on the plan) | "what to test / why" — what & why |
| **Test code authoring** | `dagster-impl` (Sonnet) | `codex` `medium` review (multi-agent §3.3) | Start with P0 cases from the qa-strategist plan — how |
| **Test quality review** | `codex` (`Effort: medium`) | — | "does this test actually catch the gap?" — is-it-correct |
| **E2E / staging runtime validation** | `pipeline-explorer` (DuckDB row counts, MinIO object checks) | Manual `staging_test_dispatch.py` when needed | Read-only assertions, no mutations |
| **Regression test execution + result analysis** | main or `dagster-impl` (`pytest -q`) | `codex` (if failure cause is ambiguous) | qa-strategist does not execute tests |
| New Dagster asset / sensor / op | `dagster-impl` | `codex` (50+ line changes) + `qa-strategist` (test plan) | 5-layer rule self-validated by dagster-impl |
| Single-file bug fix (trivial) | main directly | — | Calling codex is wasteful (`codex_collab` SKILL "handle directly" case) |
| Single-file bug fix (non-trivial) | `dagster-impl` or `codex_collab` skill | `codex` review | Grep callers when signature changes |
| Module split / rename / structural rearrangement | `codex_refactor` skill | Byte-preservation required | Logic must not change |
| DuckDB schema / migration | `dagster-impl` authoring + `codex_db_migration` skill | `codex` `extra_high` validation | multi-agent.md §3.3 — subtle consistency hazard |
| Difficult algorithm / concurrency bug | `codex_arbitration` skill (Sonnet + Codex dual + Opus arbitration) | — | multi-agent.md §4.2 Pattern B |
| "Where is it?" / code flow tracing | `pipeline-explorer` | — | Protects main context |
| Pre-deploy change impact analysis | `deploy-auditor` | `codex` (if 🔴 detected) | Recommended right before dev→main merge |
| Suspected prod/staging drift | `deploy-auditor` | — | Two-worktree comparison + `gh run list` |
| Staging reset | `staging_reset` skill | — | Operational task — user approval required |
| DuckDB host lock / stale WAL | `duckdb_staging_wiper` skill | — | For clean re-testing |
| Dagster lineage validation / repair | `dagster_lineage_fixer` skill | Trace scope with `pipeline-explorer` then fix with `dagster-impl` | |
| Daily work log | `daily_worklog` skill | — | Auto-organizes WORKLOG.md |
| Architecture decision / new resource / new bucket | main (Opus) alone | (optional) `codex` analysis | multi-agent.md §2.1 — delegation prohibited |
| Security / auth / secrets | `dagster-impl` authoring | `codex` `extra_high` + Opus final review | multi-agent.md §3.3 |
| Label Studio / Slack / external API integration | `dagster-impl` | `codex` `high` (contract consistency) | Watch for webhook URL, presigned URL expiry |
| Documentation / comments / README | `dagster-impl` (or main) | — | Default is "do not write" — CLAUDE.md rule |

## 3. Team composition by scenario

Flows the table alone cannot capture — for five commonly recurring scenarios, the calling order of members is specified.

### 3.1 Scenario A — Feature development (feature branch → dev → main)

```
User request ("Add dry-run mode to this sensor")
  │
  ├─ main: Requirements analysis, task decomposition
  │     ↓
  ├─ pipeline-explorer: Identify affected files + summarize current behavior
  │     ↓
  ├─ dagster-impl: Code implementation + unit tests
  │     ↓
  ├─ codex: (optional) Pre-merge review — 50+ lines or external impact
  │     ↓
  ├─ deploy-auditor: Confirm blast radius before dev merge
  │     ↓
  └─ (user) PR → dev auto-deploy → :3031 validation → dev→main PR
```

### 3.2 Scenario B — Difficult bug / algorithm (Pattern B dual)

```
User request ("This dedup logic produces false negatives in some case")
  │
  ├─ main: Problem abstraction + reproducibility check
  │     ↓
  ├─ pipeline-explorer: Compress relevant code + data flow
  │     ↓
  ├─ codex_arbitration skill (Pattern B):
  │     ├─ dagster-impl: Solves independently
  │     ├─ codex:        Solves independently (answers not shared between them)
  │     ↓
  ├─ main: Compare two answers → pick better one or hybridize
  │     ↓
  └─ dagster-impl: Apply final solution + regression tests
```

### 3.3 Scenario C — Operational incident (staging stuck / Dagster halted)

```
User request ("staging pipeline is stuck")
  │
  ├─ pipeline-explorer: Last sensor tick, most recent failed manifest
  │     ↓
  ├─ (branch)
  │     ├─ Suspected DuckDB lock → duckdb_staging_wiper skill
  │     ├─ Run state corrupted   → staging_reset skill
  │     ├─ Lineage broken        → dagster_lineage_fixer skill
  │     └─ Other                 → main direct diagnosis
  │     ↓
  ├─ (if fix needed) dagster-impl or codex_collab skill
  │     ↓
  └─ Restart verification — Dagster UI :3031, MinIO :9003, DuckDB row counts
```

### 3.4 Scenario D — Migration (e.g., NAS 10.0.0.36 → 10.0.0.51)

```
User request ("Make a PR moving incoming to the 10.0.0.51 NFS mount")
  │
  ├─ main: Migration plan (including rollback path)
  │     ↓
  ├─ codex: Second opinion on the plan (extra_high — data model / path impact)
  │     ↓
  ├─ dagster-impl: Apply compose / mount / config changes
  │     ↓
  ├─ deploy-auditor: rsync impact + image rebuild + .env requirement check
  │     ↓
  ├─ (user) Operate on staging for 1 week
  │     ↓
  └─ codex + deploy-auditor final review → promote to prod
```

### 3.5 Scenario E — QA / coverage push (Opus + Sonnet + Codex natural split)

**This scenario is where the 3-model team works most naturally** — reasoning, implementation, and validation each land in their own lane.

```
User request ("I want to strengthen test coverage of the dispatch module")
  │
  ├─ main (Opus): Decompose — "strategy → authoring → review → validation" 4 steps
  │     ↓
  ├─ qa-strategist (Opus): Coverage gap analysis + P0/P1/P2 test plan
  │     │   - grep existing dispatch-related tests
  │     │   - map risk categories (DuckDB lock, NFS transient, archive collision, ...)
  │     │   - returns JSON plan with `delegate_to` per case
  │     ↓
  ├─ codex (medium): Second opinion on the plan itself (optional, recommended when 5+ P0 cases)
  │     ↓
  ├─ main: Present plan to user + get approval
  │     ↓
  ├─ dagster-impl (Sonnet): Write approved P0 cases — reuse fixtures, verify fail-forward
  │     ↓
  ├─ codex (medium): Review the written test code — "does this test really defend the invariant?"
  │     │   - multi-agent §3.3: test self-quality review is `medium` effort
  │     ↓
  ├─ main: Re-invoke dagster-impl with codex feedback (if any)
  │     ↓
  ├─ pytest tests/unit -q   (actually run — never trust the model's self-report)
  │     ↓
  └─ pipeline-explorer (Sonnet): On staging, run dispatch once and verify DuckDB rows + MinIO objects
```

**Why this scenario is the model-distribution exemplar**:
- Opus spends its reasoning budget on *what & why* (which risks to defend)
- Sonnet produces the *how* (actual test code) quickly
- Codex looks at *is-it-correct* (does the test really catch the gap) from a different family
- No model reviews its own work — the blind-spot pattern is avoided structurally (multi-agent §4.3 Pattern C)

## 4. Calling rules (multi-agent.md §5–6 applied)

### 4.1 Context to pass

When calling a sub-agent via `Agent({prompt: ...})`, always include in the prompt:

1. **Task goal** — one sentence: "what / why"
2. **Target file absolute paths** (for implementer / explorer agents)
3. **Constraints** — backward compatibility, test pass criteria, behavior to preserve
4. **Output format** — `dagster-impl` returns §6.1 JSON; `codex` may return markdown or JSON; `pipeline-explorer` and `deploy-auditor` use their own defined shapes; `qa-strategist` returns its plan JSON

Never pass:
- Another sub-agent's raw answer (pollution prevention — always relay through main as a summary)
- `.env` / `credentials/` / tokens / secrets
- Unrelated file history

### 4.2 Parallel vs sequential

- **Parallel allowed** — independent information gathering (e.g., `pipeline-explorer` + `deploy-auditor` simultaneously)
- **Sequential required** — one agent's output is the next's input (e.g., `pipeline-explorer` → `dagster-impl`)

main decides. When in doubt, sequential is safer.

### 4.3 Escalation triggers (multi-agent.md §7)

- `dagster-impl` returns `status: partial` → main routes to `codex` validation
- `codex` and `dagster-impl` disagree meaningfully → `codex_arbitration` skill or Opus direct
- Both models fail → main directly (Opus alone)

## 5. Opus absence fallback (multi-agent.md §8)

When Opus is unavailable (quota / cost / availability), Sonnet 4.6 temporarily orchestrates:

- Increase `pipeline-explorer` usage (protects Sonnet's smaller context window)
- Increase `codex` call frequency (compensate with validation density)
- Get user confirmation before proceeding on:
  - Data model / migration
  - External API contract changes
  - Security / auth
  - Production deploy impact
- Record deferred decisions in [`.agent/decisions.log`](../../.agent/decisions.log) (review when Opus returns)

## 6. Monitoring / improvement (multi-agent.md §9)

The harness is actively wired — call data is collected, not aspirational. As data accumulates, use it to incrementally improve §2's routing matrix.

### Live harness components

| Component | Path | Role |
|---|---|---|
| PreToolUse hook | [`.claude/settings.json`](../../.claude/settings.json) | Fires the logger on every `Agent` tool invocation |
| Logger | [`scripts/agent_log.py`](../../scripts/agent_log.py) | Reads hook payload from stdin → appends JSONL to log |
| JSONL log | `.claude/agent-log.jsonl` (runtime, gitignored) | One entry per agent call — ts, session_id, subagent_type, description, model_override |
| Stats reporter | [`scripts/agent_stats.py`](../../scripts/agent_stats.py) | Parses the JSONL log → markdown or JSON aggregates |
| Decisions log | [`.agent/decisions.log`](../../.agent/decisions.log) | Append-only audit trail for orchestration decisions (committed) |

### Quarterly review

```bash
python3 scripts/agent_stats.py --since 2026-02-01           # markdown
python3 scripts/agent_stats.py --since 2026-02-01 --json    # JSON for further processing
```

What to look for:

- **Model distribution drift** — if Codex (`sonnet-liaison`) is under 10% of calls, you're under-validating. If Opus is over 25%, you're over-decomposing.
- **Empty agents** — a sub-agent with zero calls in a quarter is either redundant or routed wrong. Drop it or fix the routing matrix.
- **Decisions log size** — many `revisit: Y` entries mean Sonnet fallback ran long; flag to user for batched re-review.
- **Sub-agent error rate** — track manually for now (no instrumentation yet). If we see `status: failed` more than 10% of the time, tighten agent descriptions.

Then update §2 (routing matrix) and §1.1 (model allocation targets) based on the data.

## 7. Quick checklist

Before starting a new task:
- [ ] Identified the task type in §2's routing table?
- [ ] Is the primary owner sub-agent clear? If unclear, call `pipeline-explorer` first to scope.
- [ ] Are all §4.1 context items in the prompt?
- [ ] Output format specified?

Before wrapping up:
- [ ] Did tests actually run + pass (not just the agent's self-report)?
- [ ] For security / migration / external API per §3.3, was extra validation applied?
- [ ] If change touches deploy, did `deploy-auditor` review?
- [ ] **For new features / non-trivial fixes, did `qa-strategist` see the test plan?** (no safety net otherwise)
- [ ] If in Opus absence mode, was `.agent/decisions.log` updated?
