# Agent Teams — VLM Data Pipeline

> This document elaborates on the 3-tier model roles defined in [`multi-agent.md`](multi-agent.md) (Opus leader / Sonnet implementer / Codex validator) into **this project's specific sub-agent configuration**. It is a routing table for which team to assemble by task type.

## 1. Team Members (sub-agents)

Sub-agents currently defined in `.claude/agents/` and their respective roles.

| Sub-agent | Model | Mode | Role (multi-agent.md mapping) |
|---|---|---|---|
| (main session) | Opus 4.7 | r/w | Tier 1 — Orchestrator / Architect (§2.1) |
| [`qa-strategist`](.claude/agents/qa-strategist.md) | Opus 4.7 | read-only | Tier 1-style — QA strategy / coverage gap analysis / test plan design. Authoring and execution are delegated |
| [`dagster-impl`](.claude/agents/dagster-impl.md) | Sonnet 4.6 | r/w | Tier 2 — Main implementer (§2.2). Project rules built-in: 5-layer import, `duckdb_writer` tag, fail-forward, archive policy, MinIO key convention |
| [`pipeline-explorer`](.claude/agents/pipeline-explorer.md) | Sonnet 4.6 | read-only | Tier 2 — Project-specific navigator. Compressed responses on code location/flow |
| [`deploy-auditor`](.claude/agents/deploy-auditor.md) | Sonnet 4.6 | read-only | Tier 2 — CI/CD blast radius auditor. Detects prod/staging drift |
| [`codex`](.claude/agents/codex.md) | Sonnet 4.6 liaison → GPT-5.5 | read-only | Tier 2 — Cross-validator (§2.3). Different family, second perspective. Effort matrix §3.3 applied |

Additionally, **skills** defined in `.agent/skill/` are workflow macros that combine the above sub-agents — `codex_collab`, `codex_arbitration`, `codex_refactor`, `codex_db_migration`, `dagster_lineage_fixer`, `staging_reset`, `duckdb_staging_wiper`, `daily_worklog`.

### 1.1 Work allocation by model (reflecting multi-agent.md principles)

The number of agents does not equate to model usage. In actual workflows, models are divided as follows.

| Model | Assigned work | Call frequency (target) | Call path |
|---|---|---|---|
| **Opus 4.7** | Task decomposition · architecture decisions · QA strategy · final stage of difficult debugging · multi-model answer arbitration | ~10–20% | main session + `qa-strategist` |
| **Sonnet 4.6** | Implementation · unit test authoring · exploration · general auditing · review worker | ~60–70% | `dagster-impl` · `pipeline-explorer` · `deploy-auditor` · `codex` liaison shell |
| **GPT-5.5 (Codex)** | Cross-validation · dual solution · test code quality review (§3.3 medium) · security/migration extra_high validation | ~15–25% | `codex` agent + `codex_*` skills |

**Principles**:
1. Decisions requiring reasoning effort (architecture, QA strategy, security judgement) → Opus
2. Pattern/implementation-focused work → Sonnet
3. Work requiring a second look from a different perspective → Codex
4. **What & why** questions such as "where should the code live?" / "what tests should be added?" are in Opus's domain; **how** questions such as "implement this function" / "write this test" are in Sonnet's domain; **is-it-correct** questions such as "is this code safe?" / "does this test actually catch the gap?" are in Codex's domain.

## 2. Task Type → Team Routing

This is the general matrix from `multi-agent.md` §3.1 adapted to this project. The main session (Opus) consults this table at the start to decide the delegation path.

| Task type | Primary owner | Secondary / validator | Notes |
|---|---|---|---|
| **Test strategy / coverage gap analysis / test plan design** | **`qa-strategist`** (Opus) | `codex` `medium` (second opinion on the plan itself) | "What to test / why" — what & why |
| **Test code authoring** | `dagster-impl` (Sonnet) | `codex` `medium` review (multi-agent §3.3) | Delegate starting from P0 cases in the qa-strategist plan — how |
| **Test quality review** | `codex` (`Effort: medium`) | — | "Does this test actually catch the gap?" — is-it-correct |
| **E2E / staging runtime validation** | `pipeline-explorer` (DuckDB row count, MinIO object check) | Manual `staging_test_dispatch.py` run if needed | Read-only assertions, no mutations |
| **Regression test execution + result analysis** | main or `dagster-impl` (`pytest -q`) | `codex` (if failure cause is ambiguous) | qa-strategist does not run tests |
| New Dagster asset / sensor / op | `dagster-impl` | `codex` (50+ line changes) + `qa-strategist` (test plan) | 5-layer rules validated internally by dagster-impl |
| Single-file bug fix (trivial) | main directly | — | Calling codex is wasteful (`codex_collab` SKILL's "handle directly" case) |
| Single-file bug fix (non-trivial) | `dagster-impl` or `codex_collab` skill | `codex` review | Grep callers when signature changes |
| Module split / rename / structural rearrangement | `codex_refactor` skill | byte-preservation required | Logic must not change |
| DuckDB schema / migration | `dagster-impl` authoring + `codex_db_migration` skill | `codex` `extra_high` validation | multi-agent.md §3.3 — subtle consistency reasoning hazard |
| Difficult algorithm / concurrency bug | `codex_arbitration` skill (Sonnet + Codex dual + Opus arbitration) | — | multi-agent.md §4.2 Pattern B |
| "Where is it?" / code flow tracing | `pipeline-explorer` | — | Protects main context |
| Pre-deploy change impact analysis | `deploy-auditor` | `codex` (if 🔴 detected) | Recommended just before dev→main merge |
| Suspected prod/staging drift | `deploy-auditor` | — | Two-worktree comparison + `gh run list` |
| Staging reset | `staging_reset` skill | — | Operational task — user approval required |
| DuckDB host lock / stale WAL | `duckdb_staging_wiper` skill | — | For clean re-testing |
| Dagster lineage validation / repair | `dagster_lineage_fixer` skill | Trace impact scope with `pipeline-explorer` then fix with `dagster-impl` | |
| Daily work log | `daily_worklog` skill | — | Auto-organizes WORKLOG.md |
| Architecture decision / new resource / new bucket | main (Opus) alone | (optional) `codex` analysis | multi-agent.md §2.1 — delegation prohibited |
| Security · auth · secrets | `dagster-impl` authoring | `codex` `extra_high` + Opus final review | multi-agent.md §3.3 |
| Label Studio / Slack / external API integration | `dagster-impl` | `codex` `high` (contract consistency) | Watch for webhook URL, presigned URL expiry |
| Documentation / comments / README | `dagster-impl` (or main) | — | Default is "do not write" — CLAUDE.md rule |

## 3. Team Composition by Scenario

Flows that the table alone cannot capture — member call order is specified for four commonly occurring scenarios.

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
User request ("There's a case where this dedup logic produces false negatives")
  │
  ├─ main: Problem abstraction + confirm reproducibility
  │     ↓
  ├─ pipeline-explorer: Compress relevant code + data flow
  │     ↓
  ├─ codex_arbitration skill (Pattern B):
  │     ├─ dagster-impl: Solves independently
  │     ├─ codex:        Solves independently (answers not shared between them)
  │     ↓
  ├─ main: Compare two answers → select better one or create hybrid
  │     ↓
  └─ dagster-impl: Apply final solution + regression tests
```

### 3.3 Scenario C — Operational incident (staging stuck / Dagster halted)

```
User request ("The staging pipeline has stopped")
  │
  ├─ pipeline-explorer: Check last sensor tick, recent failed manifests
  │     ↓
  ├─ (branch)
  │     ├─ Suspected DuckDB lock → duckdb_staging_wiper skill
  │     ├─ Run state corruption  → staging_reset skill
  │     ├─ Lineage broken        → dagster_lineage_fixer skill
  │     └─ Other                 → main diagnoses directly
  │     ↓
  ├─ (if fix needed) dagster-impl or codex_collab skill
  │     ↓
  └─ Restart validation — Dagster UI :3031, MinIO :9003, DuckDB row count
```

### 3.4 Scenario D — Migration (e.g. NAS 10.0.0.36 → 10.0.0.51)

```
User request ("Create a PR to move incoming to the 10.0.0.51 NFS mount")
  │
  ├─ main: Migration plan (including rollback path)
  │     ↓
  ├─ codex: Second opinion on the plan (extra_high — data model/path impact)
  │     ↓
  ├─ dagster-impl: Apply compose / mount / config changes
  │     ↓
  ├─ deploy-auditor: Confirm rsync impact + image rebuild needed + .env changes required
  │     ↓
  ├─ (user) Operate on staging for 1 week
  │     ↓
  └─ codex + deploy-auditor final review before promoting to prod
```

### 3.5 Scenario E — QA / coverage reinforcement (natural distribution across Opus + Sonnet + Codex)

This scenario is **the form where the 3-model team operates most naturally**. Since reasoning, implementation, and validation are clearly separated, the roles fall cleanly along model lines.

```
User request ("I want to strengthen test coverage for the dispatch module")
  │
  ├─ main (Opus): Task decomposition — 4 stages: "strategy → write → review → validate"
  │     ↓
  ├─ qa-strategist (Opus): Coverage gap analysis + P0/P1/P2 test plan
  │     │  - grep existing dispatch-related tests
  │     │  - risk category mapping (DuckDB lock, NFS transient, archive collision, etc.)
  │     │  - return JSON plan (each case specifies delegate_to)
  │     ↓
  ├─ codex (medium): Second opinion on the plan itself (optional, recommended if P0 ≥ 5)
  │     ↓
  ├─ main: Present plan to user + obtain approval
  │     ↓
  ├─ dagster-impl (Sonnet): Write approved P0 cases — fixture reuse, fail-forward validation
  │     ↓
  ├─ codex (medium): Review written test code — "Does this test verify real invariants?"
  │     │  - multi-agent §3.3: test quality review is medium effort
  │     ↓
  ├─ main: Incorporate Codex feedback, re-invoke dagster-impl if needed
  │     ↓
  ├─ pytest tests/unit -q  (actual execution — do not trust model's self-report)
  │     ↓
  └─ pipeline-explorer (Sonnet): Run one dispatch cycle on staging, verify DuckDB rows · MinIO objects
```

**Why this scenario is the model case for 3-model distribution**:
- Opus spends reasoning time on *what & why* (which risks to prevent)
- Sonnet rapidly produces *how* (actual test code)
- Codex views *is-it-correct* (does the test actually catch it) from a different family's perspective
- There is structurally no blind spot where one model validates its own output (multi-agent §4.3 Pattern C)


## 4. Call Rules (applying multi-agent.md §5–6)

### 4.1 Context delivery

When calling a sub-agent, always include in the `Agent({prompt: ...})` prompt:

1. **Task objective** — "what / why" summarized in one sentence
2. **Target file absolute paths** (implementation/exploration agents)
3. **Constraints** — backward compatibility, test pass criteria, behavior that must be preserved
4. **Output format** — `dagster-impl` uses §6.1 JSON, `codex` uses markdown/JSON choice, `pipeline-explorer` and `deploy-auditor` use their own defined shapes

Do not include:

- Raw answers from other sub-agents (prevent contamination — always have main summarize and re-deliver)
- `.env` / `credentials/` / tokens / secrets
- Irrelevant file history

### 4.2 Parallel vs sequential

- **Parallelizable** — independent information gathering (e.g. simultaneous `pipeline-explorer` + `deploy-auditor` call)
- **Must be sequential** — output of one agent is input of the next (e.g. `pipeline-explorer` → `dagster-impl`)

main decides. When in doubt, go sequential for safety.

### 4.3 Escalation thresholds (multi-agent.md §7)

- `dagster-impl` returns `status: partial` → main branches to `codex` validation
- `codex` and `dagster-impl` opinions differ meaningfully → `codex_arbitration` skill or Opus handles directly
- Both models fail → main handles directly (Opus alone)

## 5. Opus absence fallback (multi-agent.md §8)

When Opus is unavailable (quota/cost), Sonnet 4.6 acts as temporary orchestrator:

- Increase reliance on `pipeline-explorer` beyond normal (protects Sonnet's context)
- Increase `codex` call frequency (↑ validation dependency)
- The following require user confirmation before proceeding:
  - Data model / migration
  - External API contract changes
  - Security · auth
  - Production deployment impact
- Deferred decisions are recorded in `decisions.log` (batch re-review when Opus returns)

## 6. Monitoring / improvement (multi-agent.md §9)

Tracking the following during operations provides grounds for incrementally improving the §2 matrix:

- Per-sub-agent task success rate (separated by task type)
- `codex` call → was there actually a difference of opinion? (if not, reduce frequency)
- `deploy-auditor`'s 🔴 predictions vs actual incidents (false positive/negative)
- Ratio of main handling directly vs delegating to sub-agents (update guide when main handled cases that could have been delegated)

Use this data to update the §2, §3 routing tables once per quarter.

## 7. Quick checklist

Before starting a new task:
- [ ] Identified the task type in the §2 routing table?
- [ ] Is the primary sub-agent clear? If ambiguous, call `pipeline-explorer` first to determine scope
- [ ] Included all §4.1 context items in the prompt?
- [ ] Specified the output format?

Before finishing a task:
- [ ] Besides the sub-agent's self-report, did actual tests run and pass?
- [ ] Per §3.3, were security/migration/external API changes given additional validation?
- [ ] If the change has deploy impact, did it go through `deploy-auditor`?
- [ ] **For new features / non-trivial changes, did `qa-strategist` review the test plan?** (Without it, there is no safety net to catch regressions)
- [ ] If operating in Opus absence mode, was `decisions.log` updated?
