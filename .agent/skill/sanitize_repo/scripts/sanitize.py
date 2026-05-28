#!/usr/bin/env python3
"""Repo sanitizer — strip internal IPs / host paths / private identifiers / known
secret patterns from the working tree after syncing fresh code from the prod host.

Reads the replacement mapping from `rules.local.txt` (sibling of this script's parent
dir, i.e. .agent/skill/sanitize_repo/rules.local.txt). That file is GITIGNORED because
it lists the real internal values; this script contains none.

Rule format (one per line):
  <from>==><to>            literal substring replacement
  regex:<pattern>==><to>   Python regex; <to> may use \\1, \\g<name>, etc.

What it does (in order):
  0. self-check    — warn loudly if rules.local.txt is not gitignored or is staged.
  1. content pass  — replace every `from`->`to` in all git-tracked + untracked-not-ignored
                     text files, PLUS local `.env*` files (typically gitignored, but
                     hold real secret values we want scrubbed for defense-in-depth).
  2. rename pass   — for any file whose *path* contains a `from` token, rename the
                     file/dir applying the same mapping to the path; prune empty dirs.
  3. verify pass   — scan for any leftover `from` token; exit 1 if found.
  4. review        — print every line a RISKY literal rule touched (short numeric
                     shorthand or short bare word) so over-matches can be caught by
                     eye — verify only catches leftovers, never wrong substitutions.

Usage:
  python3 .agent/skill/sanitize_repo/scripts/sanitize.py            # apply + verify
  python3 .agent/skill/sanitize_repo/scripts/sanitize.py --dry-run  # show, change nothing
  python3 .agent/skill/sanitize_repo/scripts/sanitize.py --verify-only

Exit codes: 0 = clean, 1 = residual tokens remain (or error).
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
RULES_FILE = SCRIPT_DIR.parent / "rules.local.txt"

# Local files that often hold real secrets (typically gitignored, never published) but
# should still be scrubbed before sharing the working tree.
EXTRA_FILE_GLOBS = (".env*",)

# how many sample lines to show per (file, risky-token) in the review section
REVIEW_SAMPLES_PER_HIT = 3

# Marker in hit/residual dict keys for regex-rule entries (so REVIEW skips them —
# regex rules are intentional patterns, not at risk of accidental over-match).
REGEX_KEY_PREFIX = "regex:"


def repo_root() -> Path:
    out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    return Path(out)


def is_risky_literal(token: str) -> bool:
    """A literal rule whose `from` could plausibly match unrelated content:
    a pure numeric/dotted shorthand (e.g. a partial IP octet pair) or a short bare word."""
    return bool(re.fullmatch(r"[0-9.]+", token)) or bool(re.fullmatch(r"[A-Za-z]{2,6}", token))


def load_rules() -> list[tuple[str, str, bool]]:
    """Returns list of (from_pattern, to, is_regex).

    Lines beginning with 'regex:' are compiled as Python regular expressions; otherwise
    the `from` is treated as a literal substring.
    """
    if not RULES_FILE.exists():
        sys.exit(
            f"ERROR: missing {RULES_FILE}\n"
            "This file holds the real internal->placeholder mapping and is gitignored.\n"
            "Restore it from your local backup (it is never committed)."
        )
    rules: list[tuple[str, str, bool]] = []
    for ln, raw in enumerate(RULES_FILE.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "==>" not in line:
            sys.exit(f"ERROR: {RULES_FILE}:{ln} not in '<from>==><to>' form: {raw!r}")
        old, new = line.split("==>", 1)
        if not old:
            sys.exit(f"ERROR: {RULES_FILE}:{ln} empty 'from' side")
        is_regex = old.startswith(REGEX_KEY_PREFIX)
        if is_regex:
            old = old[len(REGEX_KEY_PREFIX):]
            try:
                re.compile(old)
            except re.error as e:
                sys.exit(f"ERROR: {RULES_FILE}:{ln} invalid regex {old!r}: {e}")
        rules.append((old, new, is_regex))
    if not rules:
        sys.exit(f"ERROR: no rules parsed from {RULES_FILE}")
    return rules


def check_rules_safety(root: Path) -> list[str]:
    """Guard the two easy-to-forget mistakes: publishing the rule map, or staging it."""
    warnings: list[str] = []
    try:
        rel = os.path.relpath(RULES_FILE, root)
    except ValueError:
        rel = str(RULES_FILE)
    ignored = subprocess.run(["git", "check-ignore", "-q", rel], cwd=root).returncode == 0
    if not ignored:
        warnings.append(
            f"rules map '{rel}' is NOT gitignored — it lists real internal values and would be "
            "PUBLISHED if committed. Add it to .gitignore now."
        )
    staged = subprocess.check_output(
        ["git", "diff", "--cached", "--name-only"], cwd=root, text=True
    ).split()
    if any(os.path.basename(s) == RULES_FILE.name for s in staged):
        warnings.append(
            f"rules map '{RULES_FILE.name}' is STAGED — run `git restore --staged {rel}` "
            "before committing so it is never published."
        )
    return warnings


def list_files(root: Path) -> list[str]:
    """Tracked + untracked-not-ignored, plus local `.env*` files (often gitignored
    but holding real secrets we want to scrub for defense-in-depth)."""
    seen_set: set[str] = set()
    out: list[str] = []

    def add(rel: str) -> None:
        if rel not in seen_set:
            seen_set.add(rel)
            out.append(rel)

    for args in (["git", "ls-files", "-z"],
                 ["git", "ls-files", "--others", "--exclude-standard", "-z"]):
        raw = subprocess.check_output(args, cwd=root)
        for p in raw.split(b"\x00"):
            if p:
                add(p.decode("utf-8"))
    for pattern in EXTRA_FILE_GLOBS:
        for path in root.rglob(pattern):
            if ".git" in path.parts or "__pycache__" in path.parts or "node_modules" in path.parts:
                continue
            if not path.is_file():
                continue
            try:
                add(str(path.relative_to(root)))
            except ValueError:
                continue
    return out


def apply_to_text(text: str, rules: list[tuple[str, str, bool]]) -> tuple[str, dict[str, int]]:
    hits: dict[str, int] = {}
    for old, new, is_regex in rules:
        if is_regex:
            pat = re.compile(old)
            new_text, n = pat.subn(new, text)
            if n:
                key = REGEX_KEY_PREFIX + old
                hits[key] = hits.get(key, 0) + n
                text = new_text
        else:
            n = text.count(old)
            if n:
                hits[old] = hits.get(old, 0) + n
                text = text.replace(old, new)
    return text, hits


def apply_to_path(path: str, rules: list[tuple[str, str, bool]]) -> str:
    for old, new, is_regex in rules:
        if is_regex:
            path = re.sub(old, new, path)
        else:
            path = path.replace(old, new)
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="Sanitize the working tree using rules.local.txt")
    ap.add_argument("--dry-run", action="store_true", help="report changes, write nothing")
    ap.add_argument("--verify-only", action="store_true", help="only scan for residual tokens")
    args = ap.parse_args()

    root = repo_root()
    rules = load_rules()

    # 0) self-check the rule map (loud, but non-fatal)
    safety = check_rules_safety(root)
    if safety:
        print("⚠️  RULE-MAP SAFETY:")
        for w in safety:
            print(f"    - {w}")
        print()

    files = list_files(root)
    changed_content: list[tuple[str, dict[str, int]]] = []
    env_touched: list[str] = []
    renamed: list[tuple[str, str]] = []
    review: list[tuple[str, str, int, str]] = []   # (rel, token, lineno, line)
    skipped_binary = 0

    if not args.verify_only:
        # 1) content pass
        for rel in files:
            fp = root / rel
            try:
                text = fp.read_text(encoding="utf-8")
            except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
                skipped_binary += 1
                continue
            new_text, hits = apply_to_text(text, rules)
            if hits:
                changed_content.append((rel, hits))
                for key in hits:
                    if key.startswith(REGEX_KEY_PREFIX):
                        continue
                    if not is_risky_literal(key):
                        continue
                    shown = 0
                    for i, line in enumerate(text.splitlines(), 1):
                        if key in line:
                            review.append((rel, key, i, line.strip()[:140]))
                            shown += 1
                            if shown >= REVIEW_SAMPLES_PER_HIT:
                                break
                if Path(rel).name.startswith(".env"):
                    env_touched.append(rel)
                if not args.dry_run:
                    fp.write_text(new_text, encoding="utf-8")

        # 2) rename pass (path tokens) — longest paths first to avoid parent clashes
        for rel in sorted(files, key=len, reverse=True):
            new_rel = apply_to_path(rel, rules)
            if new_rel != rel:
                renamed.append((rel, new_rel))
                if not args.dry_run:
                    dst = root / new_rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    src = root / rel
                    if src.exists():
                        os.replace(src, dst)
        if not args.dry_run:
            for dirpath, dirnames, filenames in os.walk(root, topdown=False):
                if ".git" in Path(dirpath).parts:
                    continue
                if not dirnames and not filenames and Path(dirpath) != root:
                    try:
                        os.rmdir(dirpath)
                    except OSError:
                        pass

    # 3) verify pass — residual scan over current file list
    residual: dict[str, list[str]] = {}
    for rel in list_files(root):
        fp = root / rel
        try:
            text = fp.read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
        for old, new, is_regex in rules:
            if is_regex:
                pat = re.compile(old)
                if pat.search(text) or pat.search(rel):
                    residual.setdefault(REGEX_KEY_PREFIX + old, []).append(rel)
            else:
                if old in text or old in rel:
                    residual.setdefault(old, []).append(rel)

    # ---- report ----
    tag = "[DRY-RUN] " if args.dry_run else ""
    if not args.verify_only:
        print(f"{tag}content: {len(changed_content)} files changed, {skipped_binary} binary skipped")
        for rel, hits in changed_content:
            print(f"  ~ {rel}  [{', '.join(f'{k}:{v}' for k, v in hits.items())}]")
        print(f"{tag}rename: {len(renamed)} paths")
        for old, new in renamed:
            print(f"  > {old}  ->  {new}")
        if env_touched:
            print(f"\n⚠️  Local secret-bearing file(s) modified (gitignored — NOT pushed; "
                  "but your working values were replaced — restore from a vault / shell env if "
                  "you actively use them):")
            for e in env_touched:
                print(f"    - {e}")

        if review:
            print(f"\n⚠️  REVIEW — {len(review)} line(s) touched by RISKY literal rules "
                  "(short numeric / short bare word). Confirm each is genuinely sensitive, "
                  "NOT an unrelated number/word. If wrong, make that rule more specific in rules.local.txt:")
            for rel, tok, i, line in review:
                print(f"    {rel}:{i}  «{tok}»  {line}")

    if residual:
        print(f"\n❌ RESIDUAL tokens still present in {sum(len(v) for v in residual.values())} spots:")
        for old, locs in residual.items():
            uniq = sorted(set(locs))
            print(f"  {old!r}: {', '.join(uniq[:8])}{' ...' if len(uniq) > 8 else ''}")
        if args.dry_run:
            print("(dry-run: residuals above would remain until you apply)")
            return 0
        return 1

    print("\n✅ verify: no residual sensitive tokens")
    print("REMINDERS: (1) never commit rules.local.txt  (2) the REVIEW lines above are NOT "
          "checked by verify — eyeball them before you commit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
