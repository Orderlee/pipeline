#!/usr/bin/env python3
"""Repo sanitizer — strip internal IPs / host paths / private identifiers from the
working tree after syncing fresh code from the prod host.

Reads the replacement mapping from `rules.local.txt` (sibling of this script's parent
dir, i.e. .agent/skill/sanitize_repo/rules.local.txt). That file is GITIGNORED because
it lists the real internal values; this script contains none.

What it does (in order):
  0. self-check    — warn loudly if rules.local.txt is not gitignored or is staged.
  1. content pass  — replace every `from`->`to` in all git-tracked + untracked
                     (non-ignored, non-binary) text files.
  2. rename pass   — for any file whose *path* contains a `from` token, rename the
                     file/dir applying the same mapping to the path; prune empty dirs.
  3. verify pass   — scan for any leftover `from` token; exit 1 if found.
  4. review        — print every line a RISKY rule touched (short numeric shorthand or
                     short bare word) so over-matches can be caught by eye — the verify
                     pass only catches leftovers, never wrong substitutions.

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

# how many sample lines to show per (file, risky-token) in the review section
REVIEW_SAMPLES_PER_HIT = 3


def repo_root() -> Path:
    out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    return Path(out)


def is_risky(token: str) -> bool:
    """A rule whose literal `from` could plausibly match unrelated content:
    a pure numeric/dotted shorthand (e.g. a partial IP octet pair) or a short bare word."""
    return bool(re.fullmatch(r"[0-9.]+", token)) or bool(re.fullmatch(r"[A-Za-z]{2,6}", token))


def load_rules() -> list[tuple[str, str]]:
    if not RULES_FILE.exists():
        sys.exit(
            f"ERROR: missing {RULES_FILE}\n"
            "This file holds the real internal->placeholder mapping and is gitignored.\n"
            "Restore it from your local backup (it is never committed)."
        )
    rules: list[tuple[str, str]] = []
    for ln, raw in enumerate(RULES_FILE.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "==>" not in line:
            sys.exit(f"ERROR: {RULES_FILE}:{ln} not in '<from>==><to>' form: {raw!r}")
        old, new = line.split("==>", 1)
        if not old:
            sys.exit(f"ERROR: {RULES_FILE}:{ln} empty 'from' side")
        rules.append((old, new))
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
    """Tracked + untracked-not-ignored, NUL-separated to survive odd filenames."""
    seen: list[str] = []
    for args in (["git", "ls-files", "-z"],
                 ["git", "ls-files", "--others", "--exclude-standard", "-z"]):
        raw = subprocess.check_output(args, cwd=root)
        seen += [p.decode("utf-8") for p in raw.split(b"\x00") if p]
    out, sawset = [], set()
    for p in seen:
        if p not in sawset:
            sawset.add(p)
            out.append(p)
    return out


def apply_to_text(text: str, rules: list[tuple[str, str]]) -> tuple[str, dict[str, int]]:
    hits: dict[str, int] = {}
    for old, new in rules:
        n = text.count(old)
        if n:
            hits[old] = hits.get(old, 0) + n
            text = text.replace(old, new)
    return text, hits


def apply_to_path(path: str, rules: list[tuple[str, str]]) -> str:
    for old, new in rules:
        path = path.replace(old, new)
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="Sanitize the working tree using rules.local.txt")
    ap.add_argument("--dry-run", action="store_true", help="report changes, write nothing")
    ap.add_argument("--verify-only", action="store_true", help="only scan for residual tokens")
    args = ap.parse_args()

    root = repo_root()
    rules = load_rules()
    froms = [old for old, _ in rules]

    # 0) self-check the rule map (loud, but non-fatal)
    safety = check_rules_safety(root)
    if safety:
        print("⚠️  RULE-MAP SAFETY:")
        for w in safety:
            print(f"    - {w}")
        print()

    files = list_files(root)
    changed_content: list[tuple[str, dict[str, int]]] = []
    renamed: list[tuple[str, str]] = []
    review: list[tuple[str, str, int, str]] = []   # (rel, token, lineno, line) for RISKY rules
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
                # collect review samples for risky tokens, from the ORIGINAL text
                for tok in hits:
                    if not is_risky(tok):
                        continue
                    shown = 0
                    for i, line in enumerate(text.splitlines(), 1):
                        if tok in line:
                            review.append((rel, tok, i, line.strip()[:140]))
                            shown += 1
                            if shown >= REVIEW_SAMPLES_PER_HIT:
                                break
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
        # prune now-empty dirs
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
        for old in froms:
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

        # 4) review section — risky rules need a human eye (verify can't catch over-match)
        if review:
            print(f"\n⚠️  REVIEW — {len(review)} line(s) touched by RISKY rules "
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
    # closing reminders (the two easy-to-forget rules, surfaced every run)
    print("REMINDERS: (1) never commit rules.local.txt  (2) the REVIEW lines above are NOT "
          "checked by verify — eyeball them before you commit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
