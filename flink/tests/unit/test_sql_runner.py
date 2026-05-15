"""Unit tests for flink/jobs/lib/sql_runner.py — no PyFlink dependency."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from lib.sql_runner import (
    add_inserts_from_file,
    execute_sql_file,
    load_sql,
    split_statements,
)

# ── load_sql ──────────────────────────────────────────────────────────────────


class TestLoadSql:
    def test_reads_file_content(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1")
        assert load_sql(sql_file) == "SELECT 1"

    def test_substitutes_params(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("'{broker}' = 'kafka:9092'")
        result = load_sql(sql_file, broker="redpanda:9092")
        assert result == "'redpanda:9092' = 'kafka:9092'"

    def test_no_params_returns_raw(self, tmp_path: Path) -> None:
        content = "CREATE TABLE foo (id BIGINT)"
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(content)
        assert load_sql(sql_file) == content

    def test_missing_placeholder_raises(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("'{missing}'")
        with pytest.raises(KeyError):
            load_sql(sql_file, other="value")


# ── split_statements ──────────────────────────────────────────────────────────


class TestSplitStatements:
    def test_single_statement(self) -> None:
        assert split_statements("SELECT 1") == ["SELECT 1"]

    def test_splits_on_semicolon(self) -> None:
        sql = "CREATE TABLE a (id INT);\nCREATE TABLE b (id INT)"
        stmts = split_statements(sql)
        assert len(stmts) == 2
        assert "CREATE TABLE a" in stmts[0]
        assert "CREATE TABLE b" in stmts[1]

    def test_trailing_semicolon_not_an_extra_stmt(self) -> None:
        sql = "SELECT 1;\n"
        assert split_statements(sql) == ["SELECT 1"]

    def test_blank_blocks_filtered(self) -> None:
        sql = "SELECT 1;;\n;\nSELECT 2"
        stmts = split_statements(sql)
        assert len(stmts) == 2

    def test_comment_only_blocks_filtered(self) -> None:
        sql = "-- this is a comment;\nSELECT 1"
        stmts = split_statements(sql)
        assert len(stmts) == 1
        assert "SELECT 1" in stmts[0]

    def test_semicolon_in_line_comment_not_a_split(self) -> None:
        # semicolons inside -- comments must not split the statement
        sql = "-- use DAY(ts) when row counts justify it; add partition later\nCREATE TABLE foo (id BIGINT)"
        stmts = split_statements(sql)
        assert len(stmts) == 1
        assert "CREATE TABLE foo" in stmts[0]

    def test_inline_comment_inside_statement_kept(self) -> None:
        sql = "CREATE TABLE foo (\n    -- pk\n    id BIGINT\n)"
        stmts = split_statements(sql)
        assert len(stmts) == 1
        assert "-- pk" in stmts[0]

    def test_multiline_statement(self) -> None:
        sql = "CREATE TABLE foo (\n    id BIGINT,\n    name STRING\n) WITH (\n    'x' = 'y'\n)"
        stmts = split_statements(sql)
        assert len(stmts) == 1


# ── execute_sql_file ──────────────────────────────────────────────────────────


class TestExecuteSqlFile:
    def test_calls_execute_sql_for_each_statement(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "ddl.sql"
        sql_file.write_text("CREATE TABLE a (id INT);\nCREATE TABLE b (id INT)")
        t_env = MagicMock()

        execute_sql_file(t_env, sql_file)

        assert t_env.execute_sql.call_count == 2
        first_call_arg = t_env.execute_sql.call_args_list[0][0][0]
        assert "CREATE TABLE a" in first_call_arg

    def test_substitutes_params(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "source.sql"
        sql_file.write_text("'{broker}'")
        t_env = MagicMock()

        execute_sql_file(t_env, sql_file, broker="redpanda:9092")

        t_env.execute_sql.assert_called_once_with("'redpanda:9092'")

    def test_skips_blank_statements(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "ddl.sql"
        sql_file.write_text("CREATE TABLE a (id INT);;")
        t_env = MagicMock()

        execute_sql_file(t_env, sql_file)

        assert t_env.execute_sql.call_count == 1


# ── add_inserts_from_file ─────────────────────────────────────────────────────


class TestAddInsertsFromFile:
    def test_adds_each_insert_to_statement_set(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "insert.sql"
        sql_file.write_text("INSERT INTO a SELECT 1;\nINSERT INTO b SELECT 2")
        stmt_set = MagicMock()

        add_inserts_from_file(stmt_set, sql_file)

        assert stmt_set.add_insert_sql.call_count == 2
        args = [c[0][0] for c in stmt_set.add_insert_sql.call_args_list]
        assert any("INSERT INTO a" in a for a in args)
        assert any("INSERT INTO b" in a for a in args)

    def test_substitutes_params(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "insert.sql"
        sql_file.write_text("INSERT INTO {table} SELECT 1")
        stmt_set = MagicMock()

        add_inserts_from_file(stmt_set, sql_file, table="my_table")

        stmt_set.add_insert_sql.assert_called_once_with("INSERT INTO my_table SELECT 1")
