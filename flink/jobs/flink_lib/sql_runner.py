"""
Utilities for loading and executing Flink SQL from .sql files.

Pattern
-------
All DDL/DML lives in .sql files under jobs/sql/.  Python calls only
execute_sql_file() and add_inserts_from_file() — never inline SQL strings.

Substitution
------------
SQL files use {param_name} placeholders (Python str.format syntax).
Pass **cfg.as_dict() to substitute runtime values (brokers, URIs, topics).

Splitting
---------
Statements are split on ';'.  Comment-only blocks (lines starting with --)
and blank blocks are skipped.  EXECUTE STATEMENT SET is intentionally NOT
used inside .sql files; use the Python StatementSet API (add_inserts_from_file)
so that multiple INSERT statements share one Flink job.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def load_sql(path: Path, **params: str) -> str:
    """Read a .sql file and substitute {params} placeholders."""
    text = path.read_text(encoding="utf-8")
    return text.format(**params) if params else text


def split_statements(sql: str) -> list[str]:
    """
    Split SQL text into individual statements on ';'.

    Correctly handles semicolons inside -- line comments, /* */ block comments,
    and single-quoted string literals (none of these are statement delimiters).
    Filters out empty and comment-only blocks.
    """
    stmts: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]

        # -- line comment: consume through end of line
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            end = sql.find("\n", i)
            if end == -1:
                buf.append(sql[i:])
                break
            buf.append(sql[i : end + 1])
            i = end + 1
            continue

        # /* block comment: consume through */
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            end = sql.find("*/", i + 2)
            if end == -1:
                buf.append(sql[i:])
                break
            buf.append(sql[i : end + 2])
            i = end + 2
            continue

        # single-quoted string literal: consume through closing '
        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'" and (j + 1 >= n or sql[j + 1] != "'"):
                    break
                j += 1 if sql[j] != "'" else 2
            buf.append(sql[i : j + 1])
            i = j + 1
            continue

        # statement delimiter
        if ch == ";":
            stmt = "".join(buf).strip()
            non_comment = [
                ln for ln in stmt.splitlines() if ln.strip() and not ln.strip().startswith("--")
            ]
            if non_comment:
                stmts.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    # trailing statement without semicolon
    stmt = "".join(buf).strip()
    non_comment = [ln for ln in stmt.splitlines() if ln.strip() and not ln.strip().startswith("--")]
    if non_comment:
        stmts.append(stmt)

    return stmts


def execute_sql_file(t_env: Any, path: Path, **params: str) -> None:
    """
    Load a .sql file, substitute params, and execute each statement via t_env.

    Use for DDL files (CREATE TABLE, CREATE CATALOG, CREATE DATABASE).
    """
    sql = load_sql(path, **params)
    for stmt in split_statements(sql):
        t_env.execute_sql(stmt)


def add_inserts_from_file(stmt_set: Any, path: Path, **params: str) -> None:
    """
    Load INSERT statements from a .sql file and add them to a Flink StatementSet.

    Use for DML files (INSERT INTO … SELECT …).  All inserts share one Flink job,
    enabling fan-out to multiple sinks (Iceberg + Kafka) from a single source scan.
    """
    sql = load_sql(path, **params)
    for stmt in split_statements(sql):
        stmt_set.add_insert_sql(stmt)
