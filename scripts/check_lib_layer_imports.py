#!/usr/bin/env python3
"""lib/ layer (L1-2) 안에서 dagster / defs / resources / ops import 차단.

Top-level import 만 검사 (TYPE_CHECKING 가드 안 import 는 string annotation 만이라 허용).
함수 본문 안 lazy import 는 의도적 layer 우회라 발견 시 fail.

CI / pre-commit 에서 실행:
    python3 scripts/check_lib_layer_imports.py

실패 시 exit code 1 + 위반 항목 stderr 출력.
"""

import ast
import sys
from pathlib import Path

ROOT = Path("src/vlm_pipeline/lib")
BANNED_PREFIXES = (
    "dagster",
    "vlm_pipeline.defs",
    "vlm_pipeline.resources",
    "vlm_pipeline.ops",
)


def is_type_checking_guard(node: ast.If) -> bool:
    """if TYPE_CHECKING / if typing.TYPE_CHECKING — string annotation only block."""
    test = node.test
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    if (
        isinstance(test, ast.Attribute)
        and test.attr == "TYPE_CHECKING"
        and isinstance(test.value, ast.Name)
        and test.value.id == "typing"
    ):
        return True
    return False


def collect_violations(py: Path) -> list[str]:
    src = py.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(py))
    violations: list[str] = []

    for stmt in tree.body:
        if isinstance(stmt, ast.If) and is_type_checking_guard(stmt):
            continue

        if isinstance(stmt, (ast.Import, ast.ImportFrom)):
            check_import(stmt, py, violations)
        elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            for inner in ast.walk(stmt):
                if isinstance(inner, (ast.Import, ast.ImportFrom)):
                    check_import(inner, py, violations, where="function body")
    return violations


def check_import(node: ast.stmt, py: Path, violations: list[str], where: str = "top-level") -> None:
    if isinstance(node, ast.ImportFrom):
        mod = node.module or ""
        for banned in BANNED_PREFIXES:
            if mod == banned or mod.startswith(banned + "."):
                violations.append(
                    f"{py}:{node.lineno}: [{where}] 'from {mod} import ...' is banned in lib/ (L1-2 layer)"
                )
                return
    elif isinstance(node, ast.Import):
        for alias in node.names:
            for banned in BANNED_PREFIXES:
                if alias.name == banned or alias.name.startswith(banned + "."):
                    violations.append(
                        f"{py}:{node.lineno}: [{where}] 'import {alias.name}' is banned in lib/ (L1-2 layer)"
                    )
                    return


def main() -> int:
    if not ROOT.is_dir():
        print(f"ERROR: {ROOT} does not exist", file=sys.stderr)
        return 1
    all_violations: list[str] = []
    file_count = 0
    for py in sorted(ROOT.rglob("*.py")):
        file_count += 1
        all_violations.extend(collect_violations(py))
    if all_violations:
        print(f"Layer violation 발견 ({len(all_violations)}건):", file=sys.stderr)
        for v in all_violations:
            print(f"  {v}", file=sys.stderr)
        return 1
    print(f"lib/ layer import 검사 통과: {file_count} files clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
