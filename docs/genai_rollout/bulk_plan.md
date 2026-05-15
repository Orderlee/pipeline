# GenAI Studio — Bulk Submit (N×M Large-Scale Submission) Design

> Status: merged to dev on 2026-05-13. PR series (server → CLI → UI → wrapper) totaling 4 steps.
> Codex consensus: **Design C on top of A** (no schema change; explicit manifest JSON + multiple batch submission).

---

## 1. Purpose

Submit large volumes of image/prompt combinations in a single operation and manage results in grouped units.
An N×M extension of the existing submit structure where "1 image + 1 prompt = 1 batch".

## 2. Key Decisions (Codex Consensus)

| Decision | Adopted | Reason |
|----------|---------|--------|
| Data model | **Design A** (multi-batch). N batches share the same `bulk_group_id`. | No schema change. Existing `genai_batches.prompt` single column unchanged. |
| User interface | **Design C** (canonical manifest JSON). Convenience input is CLI sugar. | Auditable / reproducible. Dry-run friendly. |
| Pairing default | `paired` (1:1, N==M required) | "N:N" in Korean ML context = items mapped 1:1. Prevents cartesian explosion. |
| Safety gate | `--dry-run` (default) + explicit `--confirm` | Prevents $3000+ billing incidents. |
| Pre-flight limit check | CLI queries `GET /genai/limits` for remaining batch/bytes quota before submitting | Avoids UX of getting a 429 halfway through. |
| Group identifier | `bulk_group_id` (alphanumeric/_/-/. 1–64 chars) — stored inside `options_json` | No schema change; UI chip filter. |
| Operational limits exposure | `GET /genai/limits` new JSON endpoint | Per-user 24h usage + remaining headroom. Returns 5xx on PG failure (silent OK prohibited). |

## 3. Canonical Manifest Format

```json
{
  "engine": "veo",
  "options": {"duration": "8", "aspect_ratio": "9:16"},
  "pairs": [
    {"image": "rel/path/a.png", "prompt": "..."},
    {"image": "rel/path/b.png", "prompt": "..."},
    {"image": null, "prompt": "txt2video prompt only"}
  ]
}
```

- `engine` — one of 5 options (`kling`, `higgsfield`, `veo`, `nanobanana`, `gpt_image`).
- `options` — same as the submit form's `model_name` / `mode` / `duration` / `aspect_ratio` etc.
- `pairs[].image` — absolute or relative file path. `null` means text-only (Veo `mode=txt2video` exclusive).
- `pairs[].prompt` — cannot be empty (server-side guard + CLI-side validation).

## 4. CLI Entry Points

```bash
# explicit manifest
./scripts/genai-cli.sh bulk-submit --file bulk.json --dry-run
./scripts/genai-cli.sh bulk-submit --file bulk.json --confirm

# convenience input (prompts list + images dir)
./scripts/genai-cli.sh bulk-submit \
    --prompts-file prompts.json --images-from-dir ./imgs \
    --engine veo --pair-mode paired --dry-run

# veo txt2video bulk
./scripts/genai-cli.sh bulk-submit \
    --prompts-file p.json --text-only --engine veo --dry-run

# shell wrapper (fill in values in the file, then run)
./scripts/run-bulk.sh                 # dry-run
CONFIRM=true ./scripts/run-bulk.sh    # actual submission (or set CONFIRM="true" inside the file)
```

Exit codes:
- `0` — success or dry-run
- `1` — user error / pre-flight check failure (limit exceeded etc.)
- `2` — some batches failed (`partial`)

## 5. Server API

### `GET /genai/limits` (new)
Current quota status for the calling user. Used for CLI bulk pre-check.

```json
{
  "limits": {"rate_limit_per_min": 0, "daily_batch_limit": 0, "daily_bytes_limit": 0},
  "usage": {
    "user": "user",
    "rate_limit":    {"per_min": 0, "used_60s": 0,  "remaining": null},
    "daily_batches": {"limit":   0, "used_24h": 26, "remaining": null},
    "daily_bytes":   {"limit":   0, "used_24h": 76013522, "remaining": null}
  },
  "per_batch": {"max_files": 20, "max_bytes_per_file": 52428800}
}
```

`remaining: null` = unlimited. Returns 5xx on PG failure (honesty first — silent OK absolutely prohibited).

### `POST /genai/batches` extension
New form field: `bulk_group_id` (alphanumeric/_/-/. 1–64 chars). Stored inside `options_json`.

### `GET /genai/batches?bulk_group_id=<bgi>` extension
New query param. Returns only batches matching the group. Returns 400 if regex does not match.
Match is performed by parsing the PG TEXT column in Python (robust against malformed JSON).
Scan cap: `_LIST_BATCHES_MAX_SCAN = 10_000` rows.

## 6. UI

`/genai/batches` page:
- Add a **`group` chip row** to the existing `engine` / `status` chip rows
- Distinct `bulk_group_id` values extracted from the most recent 500 batches (top-20, active groups always included)
- Group column per row + small clickable chip (truncated at 20 chars, full text on hover)
- Shareable URL: `/genai/batches?engine=veo&status=succeeded&bulk_group_id=bgi-abc123`

The "Recent Batches" sidebar on the submit page (`/`) is unchanged — adding more chip rows would hurt readability.

## 7. Safeguard Summary

| Location | Behavior |
|----------|----------|
| CLI `--dry-run` (default) | Prints plan only and exits without `--confirm` |
| CLI pre-flight check | `GET /genai/limits` → compare remaining batch/bytes; reject if exceeded |
| CLI file validation | Verifies existence/size/extension of all images before the first POST |
| CLI input guard | Reject simultaneous use of `--file` + `--prompts-file`; reject `--text-only` + conflicting mode |
| Server regex | `bulk_group_id` validated with `[A-Za-z0-9_.-]{1,64}` on both POST and GET |
| Server limits | Existing `check_rate_limit` + `check_daily_quota` act as hard guards (separate from CLI pre-flight) |
| Throttle | `--sleep` (default 0.5s) wait between batches |
| Error isolation | per-batch try/except (only KeyboardInterrupt/SystemExit propagates), prevents loss of `submitted[]` data |

## 8. Cost Scenarios (Veo basis)

| Scenario | jobs | Estimated cost (Veo 8s) |
|----------|------|--------------------------|
| paired 30 prompts × 1 image | 30 | ~$96 |
| cartesian 5 images × 50 prompts | 250 | ~$800 |
| cartesian 20 images × 100 prompts | 2,000 | ~$6,400 |

→ **Always verify batches/jobs/cost with dry-run before `--confirm`**. Also recommended to set `GENAI_DAILY_BATCH_LIMIT` to an appropriate value before going to production.

## 9. Future Follow-ups

| Item | Priority | Notes |
|------|----------|-------|
| Per-job prompt (Design B) — add `genai_jobs.prompt_override` column | Medium | Improves DB efficiency for large cartesian runs |
| `options_json` → `JSONB` column + GIN index | Medium | Enables `bulk_group_id` search on the SQL side |
| Per-bulk-run job count cap (`GENAI_MAX_BULK_JOBS`) | High | Production cost guard |
| UI manifest upload form | Low | ROI low since CLI users are primary audience |
| Group-level retry/abort | Medium | Batch operation by `bulk_group_id` |
| Timezone unification (server now() vs PG submitted_at) | Medium | UTC assumption needs verification |

## 10. PR Series

| Step | PR | Core |
|------|----|------|
| Step 1 | (this PR) | Server `GET /genai/limits` + `bulk_group_id` form field + `_daily_usage` refactor |
| Step 2 | (this PR) | `bulk-submit` subcommand (manifest/convenience input, dry-run/confirm, precheck) |
| Step 3 | (this PR) | UI `bulk_group` chip row (+ pg.py iterative fetch, top-20 cap) |
| Step 4 | (this PR) | `scripts/run-bulk.sh` (CONFIRM gate, mode A/B auto-branching) |
| Step 5 | (this PR) | This document |

Each step passed Codex code review (3 rounds total):
- Step 1.5: remove silent fallback, share `_daily_usage`, allow regex `.`
- Step 2.5: broad Exception catch, reject manifest+prompts simultaneous use, reject `--text-only` conflicting mode
- Step 3.5: `bulk_group_id` GET regex validation, iterative fetch (10K row cap), `.gs-chip--tiny`, chip top-20 + pin
