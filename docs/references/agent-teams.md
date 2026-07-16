# Agent Teams — VLM Data Pipeline

> This document maps the model-tier roles from [`multi-agent.md`](multi-agent.md) (Opus orchestrator / Sonnet implementers / Haiku observer / Codex cross-validator) onto **this project's concrete persona roster**. It is a routing guide: given a task, which persona owns it and who validates.
>
> Model versions are referenced by tier alias (`opus` / `sonnet` / `haiku`), never by version number — the `model:` field in each [`.claude/agents/*.md`](../../.claude/agents/) frontmatter is the source of truth and resolves to the current model.

## 1. Team members (personas)

The roster is organized by **plane**. Each persona maps to one model tier (multi-agent.md §2). It grew from the original 6-member set (main + qa-strategist + dagster-impl + pipeline-explorer + deploy-auditor + codex) into a domain-persona team as the pipeline gained a labeling/dataset track and an MLOps finetune track.

### Decision / reasoning plane — Opus (multi-agent.md §2.1)

| Persona | Mode | Role |
|---|---|---|
| (main session) | r/w | Acts as orchestrator directly when not spawning `cto`; carries the full codebase context |
| [`cto`](../../.claude/agents/cto.md) | r/w | Lead architect / Opus orchestrator. Decomposes work, routes to personas, owns architecture & deploy-risk decisions and final pre-merge review. Delegates the typing (§2.1 — delegation of *decisions* prohibited) |
| [`ai-modeler`](../../.claude/agents/ai-modeler.md) | r/w | Model **science**: experiment & fine-tune design, eval-gate design (per-metric margin + per-class non-regression floor), promote/reject decisions, GT policy. Statistical judgment |
| [`qa-strategist`](../../.claude/agents/qa-strategist.md) | read-only | QA **architecture**: coverage gap analysis, test-plan design, regression risk. Authoring & execution are delegated |

### Data plane — Sonnet (multi-agent.md §2.2)

| Persona | Mode | Role |
|---|---|---|
| [`data-engineer`](../../.claude/agents/data-engineer.md) | r/w | Core ingest → dedup → dispatch → process; Postgres/DuckDB, MinIO, archive, NAS/NFS, Dagster orchestration plumbing. Everything the AI personas ride on |
| [`dataops-engineer`](../../.claude/agents/dataops-engineer.md) | r/w | Data **correctness & completeness**: reconciliation (raw_files ↔ MinIO ↔ archive), backfills, dedup/checksum maintenance, migration-as-ops, retention, NAS quota. Operates the data `data-engineer` produces |
| [`ai-data-engineer`](../../.claude/agents/ai-data-engineer.md) | r/w | Training-data side: Gemini per-event labeling, Label Studio finalization, GT projection (`image_label_annotations` / `v_finalized_labels`), dataset build, pseudo-label QA, DVC versioning |

### Model plane — Sonnet (multi-agent.md §2.2)

| Persona | Mode | Role |
|---|---|---|
| [`ai-engineer`](../../.claude/agents/ai-engineer.md) | r/w | Model **serving & inference**: SAM3 / YOLO-World / embedding-service containers, inference glue, GPU topology, maintenance-mode drain endpoints, promote/rollback mechanics (checkpoint → env → recreate) |
| [`mlops-engineer`](../../.claude/agents/mlops-engineer.md) | r/w | Model **ops machinery**: trainer lifecycle (independent process, not in-run op), GPU maintenance windows, `model_registry` state, `promote_model.py` / `promote_pe_core.py` automation, MLflow, DVC infra, `clear_maintenance.sh` |

### Implementation / research / audit plane — Sonnet (multi-agent.md §2.2)

| Persona | Mode | Role |
|---|---|---|
| [`dagster-impl`](../../.claude/agents/dagster-impl.md) | r/w | General project-tuned Dagster implementation (assets/sensors/ops/resources). 5-layer import, `duckdb_writer` tag, fail-forward, MinIO keys built-in. **The default implementer when no single domain persona fits** |
| [`pipeline-explorer`](../../.claude/agents/pipeline-explorer.md) | read-only | Project-specific **code** navigator. "Where is X / trace this flow" — compressed pointers (paths + line ranges), protects parent context |
| [`deploy-auditor`](../../.claude/agents/deploy-auditor.md) | read-only | Deploy blast-radius auditor + prod/staging drift detector. Knows the two CI workflows, rsync + `git reset --hard` mechanism, `detect_image_rebuild` triggers, healthcheck endpoints |
| [`tech-scout`](../../.claude/agents/tech-scout.md) | read-only | New/unfamiliar-tech first responder. Verifies against CURRENT docs (context7 + web), **never from memory**. Reports fit / integration cost / risk; `cto` decides adopt/reject |

### Observation plane — Haiku (multi-agent.md §2.4)

| Persona | Mode | Role |
|---|---|---|
| [`ops-engineer`](../../.claude/agents/ops-engineer.md) | read-only | **Runtime** health: `docker ps`, `failed/*.jsonl` triage, DB status snapshots, sensor/daemon liveness, MinIO object presence, deploy/drift status. Cheap & high-frequency. Reports & routes; **never fixes** |

### Cross-validation — GPT-5.x (Codex) (multi-agent.md §2.3)

| Persona | Mode | Role |
|---|---|---|
| [`codex`](../../.claude/agents/codex.md) | read-only | Different-family validator. Reviews implementer output (esp. security/auth, schema/migration, concurrency/locks, hard algorithms), or solves independently in Pattern B arbitration. **Never the first writer.** Effort matrix multi-agent.md §3.3 |

In addition, **skills** in [`.agent/skill/`](../../.agent/skill/) are workflow macros that compose the above personas — `codex_collab`, `codex_arbitration`, `codex_refactor`, `codex_db_migration`, `mlops-finetune`, `dagster_lineage_fixer`, `staging_reset`, `duckdb_staging_wiper`, `daily_worklog`.

### 1.1 Model allocation

Persona count ≠ model usage. Fourteen personas collapse onto four model tiers:

| Model | Personas | Assigned work (what/why · how · is-it-alive · is-it-correct) | Target call frequency |
|---|---|---|---|
| **Opus** (§2.1) | `cto`, `ai-modeler`, `qa-strategist` + main session | **what & why** — decomposition, architecture, model-science & QA judgment, hard-debug final stage, arbitration | ~10–20% |
| **Sonnet** (§2.2) | `data-engineer`, `dataops-engineer`, `ai-data-engineer`, `ai-engineer`, `mlops-engineer`, `dagster-impl`, `pipeline-explorer`, `deploy-auditor`, `tech-scout` | **how** — implementation, dataset/label plumbing, ops automation, exploration, auditing, tech recon | ~55–65% |
| **Haiku** (§2.4) | `ops-engineer` | **is-it-alive** — runtime health, log triage, status snapshots. Cheap, no fixed target | as needed |
| **GPT-5.x (Codex)** (§2.3) | `codex` + `codex_*` skills | **is-it-correct** — cross-validation, test-quality review (§3.3 `medium`), security/migration `ultra` | ~15–20% |

**Principles**:
1. Decisions that require reasoning effort (architecture, model science, QA strategy, security judgment) → Opus (`cto` / `ai-modeler` / `qa-strategist`).
2. Pattern/implementation work → the Sonnet persona whose **domain** fits (data / model / general); `dagster-impl` when none fits cleanly.
3. "Is the running system healthy?" → Haiku (`ops-engineer`) — never spend Opus/Sonnet budget on a status check.
4. Work that benefits from a different-family second look → Codex.
5. **what & why** ("where should this live?" / "which approach?") → Opus. **how** ("implement this") → domain Sonnet persona. **is-it-alive** ("is dispatch running?") → Haiku. **is-it-correct** ("is this safe / does the test catch the gap?") → Codex.

## 2. Task type → persona routing

This is `multi-agent.md` §3.1's model-tier matrix resolved to this project's personas. The orchestrator (main session / `cto`) consults it at the start of each task to decide delegation.

| Task type | Primary owner | Secondary / validator | Notes |
|---|---|---|---|
| Architecture decision / new resource / bucket / cross-cutting change | **`cto`** (Opus) | (optional) `codex` analysis | multi-agent.md §2.1 — decisions not delegated |
| "Should we use X" / new library / version upgrade / migration guide | **`tech-scout`** (verify current docs) | `cto` (adopt/reject) | Never answer new-tech from memory |
| **Ingest / dedup / dispatch / sensor / raw_files / manifest / ffprobe / phash / checksum** | **`data-engineer`** | `codex` (50+ lines) | Core ETL & orchestration plumbing |
| **Postgres/DuckDB schema / migration authoring** | `data-engineer` + `codex_db_migration` skill | `codex` `ultra` | one `DO $$` block per migration (runner applies the first only) — §3.3 subtle hazard |
| **Reconciliation / backfill / dedup cleanup / checksum recompute / retention / NAS quota** | **`dataops-engineer`** | `codex` (if destructive) | Operates the data; doesn't write pipeline code |
| **Labeling (Gemini) / Label Studio / GT curation / dataset build / pseudo-label QA / DVC** | **`ai-data-engineer`** | `ai-modeler` (if it feeds training) | `labels` = per-event; **0 rows ≠ failure** |
| **Model serving / inference / SAM3 / YOLO / embedding-service / GPU alloc / maintenance drain / promote mechanics** | **`ai-engineer`** | `codex` `high` (contract) | Serves what `ai-modeler` decides |
| **Fine-tune design / eval-gate / promote-reject decision / GT policy / experiment design** | **`ai-modeler`** (Opus) | `codex` (eval logic) | Decides what/whether; doesn't run the trainer |
| **Trainer lifecycle / GPU maintenance window / model_registry state / promotion-rollback automation / MLflow / DVC infra** | **`mlops-engineer`** | `codex` (promotion path) | Runs the machinery `ai-modeler` decides |
| **Runtime health / "is the pipeline healthy" / failed.jsonl triage / status snapshot / liveness / object presence** | **`ops-engineer`** (Haiku) | routes to owner on findings | Cheap, read-only, **never fixes** |
| Test strategy / coverage gap / plan design | **`qa-strategist`** (Opus) | `codex` `medium` (plan) | "what to test / why" — what & why |
| Test code authoring | domain Sonnet persona or `dagster-impl` | `codex` `medium` review | Start from qa-strategist P0 cases — how |
| Test quality review | `codex` (`medium`) | — | "does this test actually catch the gap?" |
| E2E / staging runtime validation | `pipeline-explorer` (row/object checks) + `ops-engineer` (liveness) | Manual `staging_test_dispatch.py` | Read-only assertions |
| New Dagster asset / sensor / op (no clear domain) | `dagster-impl` | `codex` (50+ lines) + `qa-strategist` (plan) | General implementer fallback |
| Single-file bug fix (trivial) | main / `cto` directly | — | Calling codex is wasteful |
| Single-file bug fix (non-trivial) | domain persona or `codex_collab` skill | `codex` review | Grep callers when signature changes |
| Module split / rename / structural rearrangement | `codex_refactor` skill | Byte-preservation required | Logic must not change |
| Difficult algorithm / concurrency bug | `codex_arbitration` skill (Sonnet + Codex dual → `cto` arbitration) | — | multi-agent.md §4.2 Pattern B |
| "Where is it?" / code flow tracing | `pipeline-explorer` | — | Protects parent context |
| Pre-deploy change impact analysis | `deploy-auditor` | `codex` (if 🔴 detected) | Recommended before dev→main merge |
| Suspected prod/staging drift | `deploy-auditor` + `ops-engineer` | — | Two-worktree diff + `gh run list` + runtime |
| Staging reset / clean re-test | `staging_reset` / `duckdb_staging_wiper` skill | — | Operational — user approval required |
| Dagster lineage validation / repair | `dagster_lineage_fixer` skill | scope w/ `pipeline-explorer`, fix w/ `data-engineer` or `dagster-impl` | |
| MLOps finetune run (end-to-end) | `mlops-finetune` skill | `ai-modeler` (decisions) + `mlops-engineer` (ops) | Weights promotion stays manual |
| Security / auth / secrets | domain persona authoring | `codex` `ultra` + `cto` final review | multi-agent.md §3.3 |
| Label Studio / Slack / external API integration | `ai-data-engineer` (LS) or `data-engineer` | `codex` `high` (contract consistency) | Watch webhook URL, presigned URL expiry |
| Daily work log | `daily_worklog` skill | — | Auto-organizes WORKLOG.md |
| Documentation / comments / README | domain persona (or main) | — | Default "do not write" — CLAUDE.md rule |

## 3. Team composition by scenario

Flows the table alone cannot capture — for six recurring scenarios, the calling order of personas is specified.

### 3.1 Scenario A — Feature development (feature branch → dev → main)

```
User request ("Add dry-run mode to this dispatch sensor")
  │
  ├─ main / cto: Requirements analysis, task decomposition, pick the domain persona
  │     ↓
  ├─ pipeline-explorer: Identify affected files + summarize current behavior
  │     ↓
  ├─ data-engineer: Code implementation + unit tests   (domain persona — ingest/dispatch)
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
  ├─ main / cto: Problem abstraction + reproducibility check
  │     ↓
  ├─ pipeline-explorer: Compress relevant code + data flow
  │     ↓
  ├─ codex_arbitration skill (Pattern B):
  │     ├─ data-engineer: Solves independently
  │     └─ codex:         Solves independently (answers not shared between them)
  │     ↓
  ├─ cto: Compare two answers → pick better one or hybridize
  │     ↓
  └─ data-engineer: Apply final solution + regression tests
```

### 3.3 Scenario C — Operational incident (staging stuck / Dagster halted)

```
User request ("staging pipeline is stuck")
  │
  ├─ ops-engineer (Haiku): Cheap first triage — docker ps, last sensor tick,
  │     recent failed.jsonl, run state, DB counts. Reports & routes (does not fix)
  │     ↓
  ├─ (branch on what ops-engineer surfaced)
  │     ├─ Suspected DuckDB lock → duckdb_staging_wiper skill
  │     ├─ Run state corrupted   → staging_reset skill
  │     ├─ Lineage broken        → dagster_lineage_fixer skill
  │     ├─ Data inconsistency    → dataops-engineer (reconcile / backfill)
  │     └─ Code bug              → data-engineer / ai-engineer (by domain)
  │     ↓
  ├─ (if fix needed) domain persona or codex_collab skill
  │     ↓
  └─ ops-engineer: Restart verification — Dagster UI :3031, MinIO :9003, DB row counts
```

### 3.4 Scenario D — Migration (e.g., NAS 10.0.0.36 → 10.0.0.51)

```
User request ("Make a PR moving incoming to the 10.0.0.51 NFS mount")
  │
  ├─ cto: Migration plan (including rollback path)
  │     ↓
  ├─ codex: Second opinion on the plan (ultra — data model / path impact)
  │     ↓
  ├─ data-engineer: Apply compose / mount / config changes
  │     ↓
  ├─ dataops-engineer: Post-cutover reconciliation (raw_files ↔ MinIO ↔ archive)
  │     ↓
  ├─ deploy-auditor: rsync impact + image rebuild + .env requirement check
  │     ↓
  ├─ (user) Operate on staging for 1 week
  │     ↓
  └─ codex + deploy-auditor final review → promote to prod
```

### 3.5 Scenario E — QA / coverage push (Opus + Sonnet + Codex natural split)

**The 3-tier exemplar** — reasoning, implementation, and validation each land in their own lane.

```
User request ("I want to strengthen test coverage of the dispatch module")
  │
  ├─ main / cto (Opus): Decompose — "strategy → authoring → review → validation" 4 steps
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
  ├─ data-engineer (Sonnet): Write approved P0 cases — reuse fixtures, verify fail-forward
  │     │   (domain persona for dispatch; use dagster-impl if the module has no domain owner)
  │     ↓
  ├─ codex (medium): Review the written test code — "does this test really defend the invariant?"
  │     ↓
  ├─ main: Re-invoke the implementer persona with codex feedback (if any)
  │     ↓
  ├─ pytest tests/unit -q   (actually run — never trust the model's self-report)
  │     ↓
  └─ pipeline-explorer + ops-engineer: On staging, run dispatch once → verify DB rows + MinIO objects + liveness
```

### 3.6 Scenario F — MLOps finetune track (the model-plane exemplar)

**This is where the four AI personas split cleanly** — no persona both decides the science and serves its own model (blind-spot avoidance, multi-agent.md §4.3 Pattern C).

```
User request ("Fine-tune SAM3 on the confirmed fire/smoke GT and promote if it beats incumbent")
  │
  ├─ cto: Confirm this is a training window (shared GPU — prod deploy hold advised)
  │     ↓
  ├─ ai-data-engineer: Build the frozen train snapshot
  │     (train_dataset_versions row + vlm-dataset/_trainsets/<id>/ immutable)
  │     ↓
  ├─ ai-modeler (Opus): Experiment design — fine-tune config, eval-gate margins, promotion criteria
  │     ↓
  ├─ mlops-engineer: GPU maintenance drain → run independent trainer process
  │     (docker compose run --rm trainer, ENABLE_TRAINING=1) → candidate weights + model_registry row
  │     ↓
  ├─ ai-modeler: Read eval gate (per-metric margin + per-class non-regression floor) → promote / reject
  │     ↓
  ├─ mlops-engineer: promote_model.py (checkpoint → env → recreate)
  │     ↓
  ├─ ai-engineer: Confirm serving picks up new weights (resolved path + checksum in startup log)
  │     ↓
  └─ ops-engineer: Post-promote serving health + /warmup verification
```

**Why the personas split this way**: `ai-modeler` decides the science (what/whether), `mlops-engineer` runs the machinery (drain, trainer, promotion automation), `ai-engineer` owns the serving runtime, `ai-data-engineer` freezes the GT snapshot. The decider never serves its own result.

## 4. Calling rules (multi-agent.md §5–6 applied)

### 4.1 Context to pass

When calling a persona via `Agent({subagent_type, prompt})`, always include in the prompt:

1. **Task goal** — one sentence: "what / why"
2. **Target file absolute paths** (for implementer / explorer personas)
3. **Constraints** — backward compatibility, test pass criteria, behavior to preserve, environment (prod vs staging)
4. **Output format** — implementer personas return §6.1 JSON; `codex` may return markdown or JSON; read-only personas (`pipeline-explorer`, `deploy-auditor`, `ops-engineer`, `tech-scout`, `qa-strategist`) use their own defined report shapes

Never pass:
- Another persona's raw answer (pollution prevention — always relay through the orchestrator as a summary)
- `.env` / `credentials/` / tokens / secrets
- Unrelated file history

### 4.2 Parallel vs sequential

- **Parallel allowed** — independent information gathering (e.g., `pipeline-explorer` + `deploy-auditor` + `ops-engineer` simultaneously)
- **Sequential required** — one persona's output is the next's input (e.g., `pipeline-explorer` → `data-engineer`, or `ai-modeler` decision → `mlops-engineer` run)

The orchestrator decides. When in doubt, sequential is safer.

### 4.3 Escalation triggers (multi-agent.md §7)

- An implementer persona returns `status: partial` → orchestrator routes to `codex` validation
- `codex` and the implementer disagree meaningfully → `codex_arbitration` skill or `cto` direct
- Both models fail → `cto` directly (Opus alone)
- An implementer hits a cross-domain / architecture need → STOP and escalate to `cto` (do not cross persona boundaries silently)

## 5. Opus absence fallback (multi-agent.md §8)

When the Opus personas (`cto` / `ai-modeler` / `qa-strategist`) are unavailable (quota / cost / availability), a Sonnet persona — usually `data-engineer` or `dagster-impl` — temporarily orchestrates:

- Increase `pipeline-explorer` usage (protects the smaller Sonnet context window)
- Increase `codex` call frequency (compensate with validation density)
- Get user confirmation before proceeding on:
  - Data model / migration
  - External API contract changes
  - Security / auth
  - Production deploy impact
  - **Model promotion / eval-gate decisions** (normally `ai-modeler`'s — never auto-promote in fallback)
- Record deferred decisions in [`.agent/decisions.log`](../../.agent/decisions.log) (review when the Opus personas return)

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

- **Model distribution drift** — if Codex is under ~10% of calls, you're under-validating. If Opus (`cto` + `ai-modeler` + `qa-strategist`) is over ~25%, you're over-decomposing.
- **Plane coverage, not per-persona** — with 14 personas a long tail is expected. Check that each *plane* (data / model / decision / observation / validation) sees traffic, not that every persona does. A persona with zero calls over a quarter is a candidate to fold into a sibling or to fix its routing/description.
- **Haiku share** — `ops-engineer` should carry the cheap status checks. If Sonnet/Opus personas are being called for "is it healthy?", the routing is leaking upward and burning budget.
- **Decisions log size** — many `revisit: Y` entries mean Sonnet fallback ran long; flag to user for batched re-review.
- **Sub-agent error rate** — track manually for now. If `status: failed` exceeds ~10%, tighten the persona's description/triggers.

Then update §2 (routing matrix) and §1.1 (model allocation targets) based on the data.

## 7. Quick checklist

Before starting a new task:
- [ ] Identified the task type in §2's routing table?
- [ ] Is the primary owner persona clear? If unclear, call `pipeline-explorer` (code) or `ops-engineer` (runtime) first to scope.
- [ ] For a status/health question, did it go to Haiku (`ops-engineer`) rather than a Sonnet/Opus persona?
- [ ] Are all §4.1 context items in the prompt (including prod vs staging)?
- [ ] Output format specified?

Before wrapping up:
- [ ] Did tests actually run + pass (not just the persona's self-report)?
- [ ] For security / migration / external API per §3.3, was `ultra` validation applied?
- [ ] If the change touches deploy, did `deploy-auditor` review?
- [ ] **For new features / non-trivial fixes, did `qa-strategist` see the test plan?** (no safety net otherwise)
- [ ] For a model promotion, did `ai-modeler` (not the orchestrator) own the eval-gate decision?
- [ ] If in Opus-absence mode, was `.agent/decisions.log` updated?
