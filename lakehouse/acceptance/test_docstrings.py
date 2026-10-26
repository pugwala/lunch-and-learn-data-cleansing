"""Acceptance test for the docstring rule: every function says what it does.

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_docstrings.py

Every Python function and class under lakehouse/ has a docstring, and every shell function (in .sh and .bats files)
has a comment on the line above it. Generated folders (dbt's target, caches) are skipped.
"""
import ast
import re
from pathlib import Path

LAKEHOUSE = Path(__file__).resolve().parents[1]
SKIP = ("dbt/target", "dbt/dbt_packages", "__pycache__", ".ipynb_checkpoints")
SHELL_FUNCTION = re.compile(r"\s*(?:function\s+)?([A-Za-z_][\w-]*)\s*\(\)\s*\{")


def files(*patterns):
    """The lakehouse's files matching the glob patterns, generated folders left out."""
    return sorted(f for p in patterns for f in LAKEHOUSE.rglob(p) if not any(s in str(f) for s in SKIP))


def test_every_python_function_and_class_has_a_docstring():
    """No def or class in lakehouse/ without a docstring."""
    missing = [f"{f.relative_to(LAKEHOUSE)}:{n.lineno} {n.name}" for f in files("*.py")
               for n in ast.walk(ast.parse(f.read_text()))
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and not ast.get_docstring(n)]
    assert not missing, "\n".join(missing)


def test_every_shell_function_has_a_comment_above_it():
    """No shell function in a .sh or .bats file without a comment on the line before it."""
    missing = []
    for f in files("*.sh", "*.bats"):
        lines = f.read_text().splitlines()
        for i, line in enumerate(lines):
            m = SHELL_FUNCTION.match(line)
            if m and not (i > 0 and lines[i - 1].lstrip().startswith("#")):
                missing.append(f"{f.relative_to(LAKEHOUSE)}:{i + 1} {m.group(1)}")
    assert not missing, "\n".join(missing)
