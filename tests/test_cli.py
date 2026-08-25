from __future__ import annotations

import io
from pathlib import Path

import pytest

from ingestion_framework.cli import EXIT_FAILED, EXIT_OK, EXIT_USAGE, build_parser, main

from .conftest import write_yaml


def run_cli(*argv, config_root=None):
    """Run the CLI, returning (exit_code, output)."""
    out = io.StringIO()
    args = list(argv)
    if config_root is not None:
        args = ["--config-root", str(config_root)] + args
    code = main(args, out=out)
    return code, out.getvalue()


class TestParser:
    def test_command_is_required(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args([])

    def test_run_requires_a_selection(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["run", "--env", "prod"])

    def test_selection_options_are_mutually_exclusive(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["run", "--env", "prod", "--all", "--domain", "finance"])

    def test_table_is_repeatable(self):
        args = build_parser().parse_args(
            ["run", "--env", "prod", "--table", "a.b", "--table", "c.d"]
        )
        assert args.table == ["a.b", "c.d"]


class TestValidate:
    def test_shipped_config_passes(self, shipped_config: Path):
        code, out = run_cli("validate", "--env", "prod", config_root=shipped_config)
        assert code == EXIT_OK
        assert "OK    finance.gl_transactions" in out
        assert "3 table(s): 3 ok" in out

    def test_broken_config_reports_and_fails(self, config_tree: Path):
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            """
version: 1
table: {domain: finance, source_schema: FINOWNER, source_object: WIDGETS}
target: {write_mode: merge}
""",
        )
        code, out = run_cli("validate", "--env", "dev", config_root=config_tree)
        assert code == EXIT_FAILED
        assert "ERROR finance.widgets" in out
        assert "merge_keys is required" in out

    def test_filters_to_one_table(self, shipped_config: Path):
        code, out = run_cli(
            "validate", "--env", "prod", "--table", "finance.gl_accounts",
            config_root=shipped_config,
        )
        assert code == EXIT_OK
        assert "gl_accounts" in out and "gl_transactions" not in out

    def test_no_match_is_a_usage_error(self, shipped_config: Path):
        code, out = run_cli(
            "validate", "--env", "prod", "--table", "nope.nope", config_root=shipped_config
        )
        assert code == EXIT_USAGE

    def test_strict_turns_warnings_into_failure(self, config_tree: Path):
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            """
version: 1
table: {domain: finance, source_schema: FINOWNER, source_object: WIDGETS, business_key: [W_ID]}
extraction: {mode: full, columns: "*"}
source: {read: {num_partitions: 8}}
target: {write_mode: merge, merge_guard: none}
""",
        )
        assert run_cli("validate", "--env", "dev", config_root=config_tree)[0] == EXIT_OK
        code, out = run_cli("validate", "--env", "dev", "--strict", config_root=config_tree)
        assert code == EXIT_FAILED
        assert "WARN" in out

    def test_unknown_environment_is_a_usage_error(self, shipped_config: Path):
        code, out = run_cli("validate", "--env", "staging", config_root=shipped_config)
        assert code == EXIT_USAGE
        assert "unknown environment" in out

    def test_runs_without_spark(self, shipped_config: Path):
        # The point of validate: catch config mistakes before a job starts.
        code, _ = run_cli("validate", "--env", "dev", config_root=shipped_config)
        assert code == EXIT_OK


class TestShowSql:
    def test_prints_the_source_query(self, shipped_config: Path):
        code, out = run_cli(
            "show-sql", "--table", "finance.gl_transactions", "--env", "prod",
            config_root=shipped_config,
        )
        assert code == EXIT_OK
        assert "FROM GLOWNER.GL_TRANSACTIONS" in out
        assert "STATUS <> 'DELETED'" in out

    def test_bounds_can_be_simulated(self, shipped_config: Path):
        code, out = run_cli(
            "show-sql", "--table", "finance.gl_transactions", "--env", "prod",
            "--lower-bound", "2026-08-24 04:00:00.000000",
            config_root=shipped_config,
        )
        assert "LAST_UPDATE_DATE >= TO_TIMESTAMP('2026-08-24 04:00:00.000000'" in out

    def test_shows_the_staging_statement_too(self, shipped_config: Path):
        code, out = run_cli(
            "show-sql", "--table", "finance.gl_transactions", "--env", "prod",
            config_root=shipped_config,
        )
        assert "ROW_NUMBER() OVER" in out
        assert "_ingested_at" in out

    def test_unknown_table_is_a_usage_error(self, shipped_config: Path):
        code, out = run_cli(
            "show-sql", "--table", "finance.ghost", "--env", "prod", config_root=shipped_config
        )
        assert code == EXIT_USAGE
        assert "no config file" in out


class TestListTables:
    def test_lists_with_dependency_levels(self, shipped_config: Path):
        code, out = run_cli("list-tables", "--env", "prod", config_root=shipped_config)
        assert code == EXIT_OK
        assert "finance.gl_transactions" in out
        assert "3 table(s) in 1 dependency level(s)" in out

    def test_shows_mode_and_write_mode(self, shipped_config: Path):
        _, out = run_cli("list-tables", "--env", "prod", config_root=shipped_config)
        assert "incremental" in out and "merge" in out and "append" in out


class TestDryRun:
    def test_run_dry_run_touches_nothing(self, shipped_config: Path):
        code, out = run_cli(
            "run", "--all", "--env", "prod", "--dry-run", config_root=shipped_config
        )
        assert code == EXIT_OK
        assert "dry run: 3 table(s)" in out
        assert "prod_lakehouse.bronze.gl_transactions" in out

    def test_dry_run_shows_the_config_hash(self, shipped_config: Path):
        _, out = run_cli(
            "run", "--table", "finance.gl_accounts", "--env", "prod", "--dry-run",
            config_root=shipped_config,
        )
        assert "config=" in out

    def test_dry_run_by_group(self, shipped_config: Path):
        code, out = run_cli(
            "run", "--group", "finance_hourly", "--env", "prod", "--dry-run",
            config_root=shipped_config,
        )
        assert code == EXIT_OK
        assert "gl_transactions" in out and "gl_accounts" not in out

    def test_dry_run_by_domain(self, shipped_config: Path):
        code, out = run_cli(
            "run", "--domain", "sales", "--env", "prod", "--dry-run", config_root=shipped_config
        )
        assert "sales.order_events" in out and "finance." not in out

    def test_empty_selection_is_a_usage_error(self, shipped_config: Path):
        code, out = run_cli(
            "run", "--group", "nonexistent", "--env", "prod", "--dry-run",
            config_root=shipped_config,
        )
        assert code == EXIT_USAGE
        assert "no tables matched" in out

    def test_invalid_config_fails_before_any_cluster_work(self, config_tree: Path):
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            "version: 1\ntable: {domain: finance, source_schema: S, source_object: W}\n"
            "target: {write_mode: merge}\n",
        )
        code, out = run_cli("run", "--all", "--env", "dev", "--dry-run", config_root=config_tree)
        assert code == EXIT_USAGE
        assert "merge_keys is required" in out


class TestInitControl:
    def test_dry_run_prints_idempotent_ddl(self, shipped_config: Path):
        code, out = run_cli(
            "init-control", "--env", "prod", "--dry-run", config_root=shipped_config
        )
        assert code == EXIT_OK
        assert "CREATE SCHEMA IF NOT EXISTS prod_lakehouse.control" in out
        assert out.count("CREATE TABLE IF NOT EXISTS") == 6

    def test_uses_the_environment_control_location(self, shipped_config: Path):
        _, out = run_cli("init-control", "--env", "dev", "--dry-run", config_root=shipped_config)
        assert "dev_lakehouse.control" in out


class TestBackfill:
    def test_refuses_without_confirmation(self, shipped_config: Path):
        # Rewinding re-reads history and, on an append target, duplicates it.
        code, out = run_cli(
            "backfill", "--table", "finance.gl_transactions", "--env", "prod",
            "--from", "2026-01-01 00:00:00", config_root=shipped_config,
        )
        assert code == EXIT_USAGE
        assert "--yes to proceed" in out

    def test_refuses_for_a_table_with_no_watermark(self, shipped_config: Path):
        code, out = run_cli(
            "backfill", "--table", "finance.gl_accounts", "--env", "prod",
            "--from", "1", "--yes", config_root=shipped_config,
        )
        assert code == EXIT_USAGE
        assert "nothing to rewind" in out


class TestErrorHandling:
    def test_missing_config_root_is_a_usage_error(self):
        code, out = run_cli("validate", "--env", "prod", config_root="does/not/exist")
        assert code == EXIT_USAGE
        assert "config root not found" in out

    def test_errors_are_messages_not_stack_traces(self, shipped_config: Path):
        _, out = run_cli("show-sql", "--table", "bad-ref", "--env", "prod",
                         config_root=shipped_config)
        assert "Traceback" not in out
        assert out.startswith("error:")
