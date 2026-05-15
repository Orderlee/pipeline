# GenAI Studio CLI Plan

**Written**: 2026-05-12 / Opus + Codex consensus
**Target environment**: staging (`feature/genai-studio` branch)
**Purpose**: CLI that performs the same operations as the web UI (`http://10.0.0.10:8088`) from the terminal

---

## 1. Goals / Non-Goals

### In scope
- Single command `genai-cli` (or `genai`) for batch creation, listing, retry, and cost aggregation
- Non-interactive mode (CI / cron / script automation)
- Synchronous progress wait (`--wait`) + separate `wait` command (resumable)
- Three-tier credential resolution: environment variable / config / prompt
- Both human-readable table and machine-readable JSON output
- Directory batch input (`--images-from-dir`)

### Out of scope
- Visual features from the UI (thumbnail previews, toasts, etc.)
- Business logic in the CLI itself — REST API is the single source of truth (CLI is a thin client)
- External network exposure — the existing internal network + REST API auth trust model applies

---

## 2. Key Design Decisions (Codex Consensus)

| # | Area | Choice | Rationale |
|---|------|--------|-----------|
| 1 | Approach | **HTTP client** (call REST API via `requests`) | Reuse container's auth/quota/rate-limit. CLI stays lean. |
| 2 | Command structure | **git-style subcommands** (`genai submit kling`, `genai batches list`) | 1:1 mapping to REST resources; easy to remember. |
| 3 | Progress tracking | **`--wait` flag + separate `wait` command** | Human = one shot; CI = preserve batch_id for later resume. |
| 4 | Credential storage | **env > config > prompt** three-tier priority | env = CI, config = interactive operator, prompt = first run. |
| 5 | Output format | **TTY auto-detect + `--json`/`--table` override** | Safe for CI/scripts, human-friendly. |
| 6 | Batch input | **`--images-from-dir <DIR>`** (alphabetical sort) | Avoids shell glob, explicit order, validatable. |
| 7 | Library | **argparse (stdlib)** | Consistent with existing `scripts/*.py`; zero extra dependencies. |

---

## 3. Command Table (final)

| Command | Mapping | Description |
|---------|---------|-------------|
| `genai-cli submit <engine> --image <PATH> [...]` | `POST /genai/batches` | Submit a 1-image batch |
| `genai-cli submit <engine> --images-from-dir <DIR> [...]` | `POST /genai/batches` | Submit N images from a directory |
| `genai-cli batches list [--status X --engine Y --limit N]` | `GET /genai/batches` (API addition needed) | List batches |
| `genai-cli batches show <batch_id>` | `GET /genai/batches/{id}` (API addition needed) | Batch + job details |
| `genai-cli wait <batch_id> [--timeout SEC --interval SEC]` | `GET /genai/batches/{id}` poll | Poll until done/failed/partial |
| `genai-cli jobs retry <job_id>` | `POST /genai/jobs/{id}/retry` | Retry a failed job |
| `genai-cli costs [--range day|week|month]` | `GET /genai/costs` (JSON response needed) | Cost/activity aggregation |
| `genai-cli engines` | `GET /healthz` | List available engines |
| `genai-cli config init` | (local) | Initialize `~/.config/genai-cli/config.toml` |

### Common Options
- `--base URL` — API base (default `http://10.0.0.10:8088` or from config)
- `--json` / `--table` — force output format
- `-q` / `--quiet` — show errors only
- `-v` / `--verbose` — debug logging

### `submit <engine>` Options (per-engine mapping)
Common:
- `--prompt TEXT` (required, or `--prompt-file FILE`)
- `--image PATH` or `--images-from-dir DIR`
- `--wait` — wait until result arrives (default: return immediately)
- `--timeout SEC` (default 900, used with `--wait`)
- `--interval SEC` (default 10, polling interval)
- `--max-batch INT` (default 20, per-batch limit for directory input)

Per-engine:
- **kling**: `--model`, `--mode std|pro|master`, `--duration 5|10`, `--aspect-ratio 16:9|9:16|1:1`
- **veo**: `--model`, `--aspect-ratio 16:9|9:16`, `--duration 4|6|8`
- **higgsfield / nanobanana / gpt_image**: no additional options

---

## 4. Output Format

### TTY Auto-detect
```python
if sys.stdout.isatty(): fmt = "table"
else: fmt = "json"
```
`--json` / `--table` forces a specific format when specified.

### Example — `genai-cli batches list`

**table** (TTY):
```
BATCH_ID         ENGINE      STATUS        JOBS (✓/✗/T)   SUBMITTED
0bd7bea6-6f4    kling       running       0 / 0 / 1     2026-05-12 14:32:01
caffafc4-e93    nanobanana  failed        0 / 1 / 1     2026-05-12 15:00:00
```

**json** (pipe):
```json
[
  {"batch_id":"0bd7bea6-6f4","engine":"kling","status":"running","n_total":1, ...},
  {"batch_id":"caffafc4-e93","engine":"nanobanana","status":"failed","n_total":1, ...}
]
```

### Example — `genai-cli wait`

```
$ genai-cli wait caffafc4-e93
[polling] caffafc4-e93 · interval=10s · timeout=900s
running (0/0/1) ... 1m12s
failed  (0/1/1) ... 1m21s

batch caffafc4-e93 failed:
  job caffafc4-e93-001 → 400 INVALID_ARGUMENT...

exit code: 2
```

Exit codes:
- 0 = succeeded
- 1 = client error (auth/input)
- 2 = job(s) failed
- 3 = partial success
- 4 = timeout (limit exceeded while waiting)

---

## 5. Authentication / Configuration

### Priority (env > config > prompt)

1. **env**: `GENAI_CLI_BASE`, `GENAI_CLI_USER`, `GENAI_CLI_PASS`
2. **config**: `~/.config/genai-cli/config.toml` (`chmod 0600`)
   ```toml
   [default]
   base = "http://10.0.0.10:8088"
   user = "user"
   password = "..."
   ```
3. **prompt**: if neither of the above is set, prompt via `getpass` on first run and ask whether to save to config

### Security
- Password is **not accepted as a CLI flag** (prevents shell history exposure)
- Config file mode 0600 enforced (warn and reject if mode is 0644)
- Keyring integration can be disabled with `--no-keyring` (no keyring by default; simple config only)

---

## 6. Directory Input Sorting

```
genai-cli submit kling --images-from-dir ./batch/ --prompt "..."
```

- Extension whitelist: `.png .jpg .jpeg .webp`
- Sort: **alphabetical** (locale-independent); `seq_in_batch` maps to sorted order
- Error if more than 20 images (or override with `--max-batch` → truncate to that limit)
- No subdirectory recursion (current directory only)

---

## 7. CI / Automation Patterns

### Single batch + wait + download result
```bash
batch_id=$(genai-cli submit veo --image scene.png --prompt "..." --json | jq -r .batch_id)
genai-cli wait "$batch_id" --timeout 600
# Branch on exit code
[[ $? -eq 0 ]] && echo "OK" || exit 1
```

### N-image directory batch
```bash
for engine in kling veo; do
  genai-cli submit "$engine" --images-from-dir ./batch_test/ \
    --prompt "$(cat prompt.txt)" --wait --timeout 900
done
```

### Nightly cron — clean up completed batches
```cron
30 23 * * * /usr/local/bin/genai-cli batches list --status done --json \
            | jq -r '.[].batch_id' \
            | xargs -I {} ./sync_outputs.sh {}
```

---

## 8. Code Structure

```
scripts/genai_cli/
  __init__.py
  __main__.py            # python -m genai_cli entry point
  cli.py                 # argparse router
  client.py              # HTTPClient (requests + auth)
  config.py              # env > toml > prompt priority
  output.py              # JSON/table rendering (TTY detection)
  commands/
    __init__.py
    submit.py            # submit <engine> ...
    batches.py           # batches list / show
    wait.py              # wait <batch_id>
    jobs.py              # jobs retry
    costs.py             # costs --range
    engines.py           # engines (healthz)
    config_cmd.py        # config init / show
tests/unit/genai_cli/
  test_config.py
  test_client.py
  test_output.py
  test_submit.py         # mock requests
```

Deployment:
- Runnable as-is from `scripts/genai_cli/`: `python -m genai_cli ...`
- Or via `scripts/genai-cli` shim (shebang + `python -m genai_cli`)
- Registration under pyproject.toml `[project.scripts]` is a follow-up (currently stdlib only)

---

## 9. REST API Changes Needed

For the CLI to work, some API endpoints need to support JSON responses:

| Endpoint | Current | Required |
|----------|---------|----------|
| `GET /healthz` | JSON ✓ | unchanged |
| `POST /genai/batches` | HTTP 303 redirect (HTML) / JSON (Accept: json) ✓ | unchanged |
| `GET /genai/batches` | HTML only | **add JSON response** (branch on `Accept: application/json`) |
| `GET /genai/batches/{id}` | HTML only | **add JSON response** |
| `GET /genai/costs` | HTML only | **add JSON response** |
| `POST /genai/jobs/{id}/retry` | JSON ✓ | unchanged |

Add Accept header branching to each endpoint (same pattern already in use for `submit`) — small change.

---

## 10. Risks / Mitigations (Codex Consensus)

| # | Risk | Level | Mitigation |
|---|------|-------|------------|
| R1 | CLI behavior drifts from REST API semantics | HIGH | Keep CLI as thin client (1:1 endpoint mapping, zero local logic) |
| R2 | Async batch_id lost → wait cannot resume | HIGH | Print batch_id as first stdout line immediately after submit (can be separated to stderr even with `--quiet`) |
| R3 | Password exposed in shell history | MED | `--password` flag prohibited. env / config / prompt only |
| R4 | Config file committed to git | MED | Use `~/.config/...` path, outside operator git scope |
| R5 | TTY auto-detect wrong in some CI environments | LOW | `--json`/`--table` explicit override always available |
| R6 | Container down during `submit` → connection error | LOW | 1-2 retries + clear error message + exit code 1 |

---

## 11. Phased Rollout

### Phase 1 — Core client + submit (1 session, ~2 hours)
- [ ] `scripts/genai_cli/` directory + skeleton (cli.py, client.py, config.py)
- [ ] `genai-cli submit <engine> --image <PATH> --prompt "..."` working
- [ ] env / config / prompt authentication
- [ ] TTY auto-detect output
- [ ] 1 end-to-end run (nanobanana mock mode for quick validation)

### Phase 2 — Remaining commands + batch input (1 session)
- [ ] `--images-from-dir`
- [ ] `batches list / show`, `jobs retry`, `costs`, `engines`, `wait`
- [ ] Add JSON response branching to 4 REST API endpoints
- [ ] Table renderer (auto width)

### Phase 3 — Hardening + tests (1 session)
- [ ] `config init` interactive
- [ ] Unit tests (client validation via requests mock)
- [ ] Exit code cleanup (0/1/2/3/4)
- [ ] README + operator guide

### Phase 4 — Operations
- [ ] 1 actual call per engine in staging
- [ ] Validate cron / CI integration scenarios
- [ ] Include in main PR merge

---

## 12. Validation Checklist

- [ ] `genai-cli submit nanobanana --image test.png --prompt "test" --wait` works (synchronous engine)
- [ ] `genai-cli submit kling --image test.png --prompt "test"` async, batch_id printed immediately
- [ ] `genai-cli wait <batch_id>` can be resumed in a separate call
- [ ] `genai-cli batches list --status running --json | jq .[0]` pipe-safe
- [ ] CI environment (no TTY) → JSON automatically
- [ ] Warning when config file is chmod 0644
- [ ] Clear error when `--images-from-dir` exceeds 20 images
- [ ] Connection error + exit 1 when container is down
- [ ] All endpoints respond to both JSON / HTML (Accept header branching)

---

## 13. Next Steps

After user approval:
1. **Review this plan + command structure/names**
2. **Write Phase 1 code** — minimum working in staging (submit + auth)
3. **Phase 2-3** in sequence

Proceed to Phase 1 code writing once approval is given.
