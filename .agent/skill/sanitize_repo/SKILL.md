---
description: Strip private values (internal IPs, host paths, host username, org/service identifiers, GCP project id, customer bucket/source names, secret-pattern API keys/tokens) from the working tree — including local `.env*` files — AND delete prod-only artifacts that should not live in this copy (CI/CD workflows, pre-commit hooks). Use after an rsync from prod, or on any "sanitize / scrub before publishing" request.
---

# 🎯 Skill Purpose (Trigger)
- Run **whenever fresh code has been synced from the prod host into this working tree** and must be made public-safe before committing.
- Typical sync overwrites tracked files but keeps `.git` and local env files, e.g.:
  ```bash
  rsync -av --exclude='.git' --exclude='.env' --exclude='.env.*' \
    <prod-user>@<prod-host>:<prod-repo-path>/  <local-working-tree>/
  ```
  After this the working tree is "re-polluted" with real internal values → run this skill.
- Also triggers on requests like "민감정보/IP 정리해줘", "공개 전 스크럽", "sanitize the repo".

# 🛠️ Dependencies
- **Sanitizer:** `.agent/skill/sanitize_repo/scripts/sanitize.py` (stdlib only, Python 3.10+)
- **Rule map (GITIGNORED — holds the real values):** `.agent/sanitize_repo_rules.local.txt`
  - Lives directly under `.agent/` (not under `.agent/skill/`) so the repo's own `.agent/*` ignore keeps it untracked **without any `.gitignore` edit** — and that survives every rsync from prod (prod's `.gitignore` already has `.agent/*`). Do NOT move it back under `.agent/skill/`, where the `!.agent/skill/**` un-ignore would force it to be tracked.
  - Format, one per line, comments with `#`, applied **top-to-bottom**:
    - `<from>==><to>` — literal substring replacement (specific/compound entries first)
    - `regex:<pattern>==><to>` — Python regex; use for value-pattern matches (e.g. API-key prefixes). Put regex rules **early** so partial-token literal rules cannot corrupt the value before it is redacted.
  - This file is **never committed** (`.gitignore`) — it is the only place the real internal values live. If it is missing the script aborts; restore it from your local backup.
- **File scope:** tracked + untracked-not-ignored files, **plus local `.env*` files** even if gitignored (so real secret values get scrubbed for defense-in-depth — the script prints an explicit notice when an env file is modified).
- **Purge scope (deleted, not scrubbed):** prod-only artifacts that should never live in this copy — currently `.github/workflows/` (auto CI/CD) and `.pre-commit-config.yaml` (pre-commit hook). The list is the `PURGE_PATHS` constant in `sanitize.py`; extend it if more prod-only paths start leaking in. The companion `scripts/sync_from_prod.sh` already `--exclude`s these, so the purge is defense-in-depth against ad-hoc rsync invocations.
- Run from inside the git repo.

# 📝 Action Steps
Execute strictly in order.

1. **[Dry-run first]** Preview every change without writing:
   ```bash
   python3 .agent/skill/sanitize_repo/scripts/sanitize.py --dry-run
   ```
   The script enforces the two easy-to-forget rules for you at runtime:
   - **Rule-map safety self-check** (top of output): warns if `rules.local.txt` is not gitignored or is currently staged — fix it before committing.
   - **`⚠️ REVIEW` section** (bottom of output): prints, with `file:line` context, every line touched by a **risky rule** (short numeric shorthand like a partial IP, or a short bare word). The verify pass only catches *leftovers*, never *over-matches*, so eyeball each REVIEW line: confirm it is genuinely sensitive, not an unrelated number/word. If one is a false positive, make that rule more specific in `rules.local.txt` and re-run.

2. **[Apply]** Run for real — content replace + path/file renames + verify:
   ```bash
   python3 .agent/skill/sanitize_repo/scripts/sanitize.py
   ```
   Must finish with `✅ verify: no residual sensitive tokens` (exit 0). If it exits 1, a token was missed — add it to `rules.local.txt` and re-run until clean.

3. **[Commit]** Stage content edits + renames together:
   ```bash
   git add -A
   git commit -m "chore: sanitize after sync from prod"
   ```

4. **[Push]**
   ```bash
   git push origin main
   ```
   - **First push after a one-time history rewrite** diverges from the remote, so a normal push is rejected. That single time the human runs the force-push themselves (the harness blocks agent force-push to the default branch by design). Once the remote carries the sanitized baseline, every later sync → sanitize → commit → push is an ordinary fast-forward.

# 🚫 Constraints
- **Never commit the rule map** — it lists the real internal values. Keep it gitignored; if you ever see it staged, unstage it.
- **Never** hard-code real IPs / host paths / usernames / customer names / secret values into `sanitize.py` or this `SKILL.md`. Real values live **only** in the gitignored rule map. (Both this doc and the script are scanned by the sanitizer and must stay token-free.)
- Do not skip the dry-run review for the short shorthand / bare-token rules — the residual scan only catches **leftovers**, not **over-replacement**, so a wrong substitution would pass silently.
- Do not run `rsync --delete` against this tree unless intended — it would delete the gitignored rule map and local env files.
- **Working secrets in `.env*` are intentionally scrubbed each apply.** Keep the live values you actually use (API keys, DB passwords, etc.) in a **password manager / vault / shell-env**, not solely in the in-repo `.env` — otherwise you'll have to restore them after every sanitize cycle. The script prints an explicit "Local secret-bearing file(s) modified" notice when this happens.
- The same rule map doubles as a `git filter-repo --replace-text` rules file if a **history** rewrite is ever required — that is a separate, one-time, human-run operation (requires a force-push).
