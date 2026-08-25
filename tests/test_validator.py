from __future__ import annotations

import copy
from pathlib import Path

import pytest

from ingestion_framework.config import validate_all
from ingestion_framework.config.resolver import ConfigResolver
from ingestion_framework.config.validator import (
    ConfigValidationError,
    ValidationReport,
    validate,
)

BASE = {
    "version": 1,
    "env": "dev",
    "table": {
        "domain": "finance",
        "name": "gl_transactions",
        "source_schema": "GLOWNER",
        "source_object": "GL_TRANSACTIONS",
        "business_key": ["TXN_ID"],
    },
    "source": {
        "type": "oracle",
        "secret_scope": "oracle-dev",
        "jdbc": {"url": "jdbc:oracle:thin:@//dev:1521/DEV"},
        "read": {"num_partitions": 1},
    },
    "extraction": {
        "mode": "incremental",
        "columns": ["TXN_ID", "AMOUNT", "LAST_UPDATE_DATE"],
        "incremental": {
            "strategy": "watermark",
            "watermark_column": "LAST_UPDATE_DATE",
            "watermark_type": "timestamp",
            "overlap": "PT6H",
        },
    },
    "target": {
        "catalog": "dev_lakehouse",
        "schema": "bronze",
        "table_name": "gl_transactions",
        "write_mode": "merge",
        "merge_keys": ["TXN_ID"],
        "cluster_by": ["TXN_ID"],
    },
    "control": {"catalog": "dev_lakehouse", "schema": "control"},
}


def cfg(**patch) -> dict:
    """BASE with a deep patch applied, for one-line negative cases."""
    out = copy.deepcopy(BASE)
    for dotted, value in patch.items():
        node = out
        parts = dotted.split("__")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        if value is _DELETE:
            node.pop(parts[-1], None)
        else:
            node[parts[-1]] = value
    return out


class _Delete:
    pass


_DELETE = _Delete()


def errors(config: dict) -> list[str]:
    return validate(config).errors


class TestHappyPath:
    def test_base_config_is_valid(self):
        report = validate(BASE)
        assert report.ok, report.errors
        assert report.warnings == []


class TestStructural:
    def test_unknown_top_level_key_rejected(self):
        assert any("<root>" in e for e in errors(cfg(extractionn={})))

    def test_unknown_nested_key_rejected(self):
        assert any("target" in e for e in errors(cfg(target__wrote_mode="merge")))

    def test_bad_enum_rejected(self):
        assert any("write_mode" in e for e in errors(cfg(target__write_mode="upsert")))

    def test_missing_required_block_rejected(self):
        assert any("control" in e or "<root>" in e for e in errors(cfg(control=_DELETE)))

    def test_bad_overlap_format_rejected(self):
        assert any("overlap" in e for e in errors(cfg(extraction__incremental={"overlap": "6h"})))

    def test_structural_failure_suppresses_semantic_noise(self):
        # One malformed block should not produce a wall of downstream errors.
        report = validate(cfg(target__write_mode="upsert"))
        assert len(report.errors) == 1


class TestExtractionCoherence:
    def test_incremental_requires_watermark_column(self):
        bad = cfg()
        bad["extraction"]["incremental"]["watermark_column"] = None
        assert any("watermark_column is required" in e for e in errors(bad))

    def test_scn_strategy_rejects_watermark_column(self):
        bad = cfg()
        bad["extraction"]["incremental"]["strategy"] = "scn"
        assert any("must not be set when strategy is 'scn'" in e for e in errors(bad))

    def test_overlap_on_non_incremental_is_an_error_not_a_shrug(self):
        bad = cfg(extraction__mode="full")
        bad["extraction"]["incremental"]["watermark_column"] = None
        assert any("overlap only applies to incremental" in e for e in errors(bad))

    def test_query_mode_requires_query_file(self):
        bad = cfg(extraction__mode="query", extraction__columns="*")
        bad["extraction"]["incremental"] = {}
        assert any("query_file is required" in e for e in errors(bad))

    def test_query_file_without_query_mode_is_rejected(self):
        bad = cfg(extraction__query_file="sql/x.sql")
        assert any("would be ignored" in e for e in errors(bad))

    def test_query_mode_rejects_column_list(self):
        bad = cfg(extraction__mode="query", extraction__query_file="sql/x.sql")
        bad["extraction"]["incremental"] = {}
        assert any("cannot be combined with mode 'query'" in e for e in errors(bad))

    def test_query_file_must_exist_when_root_given(self, tmp_path: Path):
        bad = cfg(
            extraction__mode="query",
            extraction__query_file="sql/missing.sql",
            extraction__columns="*",
        )
        bad["extraction"]["incremental"] = {}
        (tmp_path / "schema").mkdir()
        (tmp_path / "schema" / "config.schema.json").write_text(
            (Path(__file__).resolve().parents[1] / "config" / "schema" / "config.schema.json").read_text()
        )
        report = validate(bad, config_root=tmp_path)
        assert any("file not found" in e for e in report.errors)


class TestPredicateSafety:
    @pytest.mark.parametrize(
        "predicate",
        ["STATUS='A'; DROP TABLE X", "STATUS='A' -- comment", "STATUS='A' /* x */"],
    )
    def test_statement_breakers_rejected(self, predicate):
        assert any("single boolean expression" in e for e in errors(cfg(extraction__filter=predicate)))

    def test_ordinary_predicate_accepted(self):
        assert validate(cfg(extraction__filter="STATUS <> 'DELETED' AND AMOUNT > 0")).ok

    def test_invalid_oracle_identifier_rejected(self):
        assert any("not a valid Oracle identifier" in e for e in errors(cfg(table__source_object="A B")))

    def test_expression_in_column_list_is_rejected_with_a_pointer(self):
        bad = cfg(extraction__columns=["TXN_ID", "TRUNC(POSTED_DATE)", "LAST_UPDATE_DATE"])
        assert any("mode: query for computed columns" in e for e in errors(bad))

    def test_duplicate_column_rejected(self):
        bad = cfg(extraction__columns=["TXN_ID", "txn_id", "LAST_UPDATE_DATE"])
        assert any("listed more than once" in e for e in errors(bad))


class TestWriteModeCoherence:
    def test_merge_requires_merge_keys(self):
        assert any("merge_keys is required" in e for e in errors(cfg(target__merge_keys=[], target__cluster_by=[])))

    def test_merge_guard_without_watermark_warns(self):
        c = cfg(extraction__mode="full", extraction__columns="*")
        c["extraction"]["incremental"] = {}
        report = validate(c)
        assert report.ok
        assert any("last-write-wins" in w for w in report.warnings)

    def test_partitioning_a_merge_table_warns(self):
        report = validate(cfg(target__partition_by=["AMOUNT"]))
        assert report.ok
        assert any("prefer cluster_by" in w for w in report.warnings)

    def test_merge_keys_with_overwrite_warns(self):
        report = validate(cfg(target__write_mode="overwrite"))
        assert any("keys are unused" in w for w in report.warnings)


class TestColumnSetConsistency:
    def test_watermark_must_be_selected(self):
        bad = cfg(extraction__columns=["TXN_ID", "AMOUNT"])
        assert any("watermark_column 'LAST_UPDATE_DATE' is not in" in e for e in errors(bad))

    def test_merge_key_must_be_selected(self):
        bad = cfg(
            extraction__columns=["AMOUNT", "LAST_UPDATE_DATE"],
            target__cluster_by=[],
        )
        assert any("target.merge_keys: 'TXN_ID'" in e for e in errors(bad))

    def test_partition_column_must_be_selected(self):
        bad = cfg(source__read={"num_partitions": 8, "partition_column": "ROW_KEY"})
        assert any("partition_column 'ROW_KEY' is not in" in e for e in errors(bad))

    def test_expectation_column_must_be_selected(self):
        bad = cfg(quality={"expectations": [{"column": "GHOST", "rule": "not_null"}]})
        assert any("quality.expectations: 'GHOST'" in e for e in errors(bad))

    def test_star_projection_skips_column_checks(self):
        ok = cfg(extraction__columns="*", quality={"expectations": [{"column": "ANY", "rule": "not_null"}]})
        assert validate(ok).ok


class TestParallelRead:
    def test_partitions_without_partition_column_warns(self):
        report = validate(cfg(source__read={"num_partitions": 16}))
        assert report.ok
        assert any("single-threaded" in w for w in report.warnings)

    def test_explicit_bounds_required_when_declared(self):
        bad = cfg(
            source__read={
                "num_partitions": 8,
                "partition_column": "TXN_ID",
                "bounds_strategy": "explicit",
            }
        )
        assert any("lower_bound and upper_bound are required" in e for e in errors(bad))


class TestExpectations:
    def test_in_set_requires_values(self):
        bad = cfg(quality={"expectations": [{"column": "AMOUNT", "rule": "in_set"}]})
        assert any("requires a non-empty 'values'" in e for e in errors(bad))

    def test_regex_requires_pattern(self):
        bad = cfg(quality={"expectations": [{"column": "AMOUNT", "rule": "regex"}]})
        assert any("requires 'pattern'" in e for e in errors(bad))

    def test_min_requires_value(self):
        bad = cfg(quality={"expectations": [{"column": "AMOUNT", "rule": "min"}]})
        assert any("requires 'value'" in e for e in errors(bad))


class TestCredentials:
    def test_secret_scope_required(self):
        bad = cfg(source__secret_scope=_DELETE)
        assert any("secret_scope is required" in e for e in errors(bad))

    def test_jdbc_url_required(self):
        bad = cfg(source__jdbc={})
        assert any("jdbc.url is required" in e for e in errors(bad))

    def test_credentials_in_url_rejected(self):
        bad = cfg(source__jdbc={"url": "jdbc:oracle:thin:user/pw@//h:1521/S?password=hunter2"})
        assert any("must not embed credentials" in e for e in errors(bad))


class TestReporting:
    def test_raise_if_failed_lists_every_problem(self):
        bad = cfg(target__merge_keys=[], target__cluster_by=[], source__secret_scope=_DELETE)
        with pytest.raises(ConfigValidationError) as exc:
            validate(bad, table_fqn="finance.gl_transactions", env="dev").raise_if_failed()
        message = str(exc.value)
        assert "finance.gl_transactions [dev]" in message
        assert "merge_keys is required" in message
        assert "secret_scope is required" in message

    def test_raise_if_failed_is_a_no_op_when_valid(self):
        assert validate(BASE).raise_if_failed().ok

    def test_report_ok_property(self):
        report = ValidationReport("t", "dev")
        assert report.ok
        report.warnings.append("just a warning")
        assert report.ok
        report.errors.append("real problem")
        assert not report.ok


class TestShippedConfigs:
    @pytest.mark.parametrize("env", ["dev", "test", "prod"])
    def test_every_shipped_table_validates_in_every_env(self, shipped_config: Path, env: str):
        reports = validate_all(shipped_config, env)
        assert reports, "no tables discovered"
        failures = {r.table_fqn: r.errors for r in reports if not r.ok}
        assert not failures, failures

    def test_shipped_configs_are_warning_free(self, shipped_config: Path):
        reports = validate_all(shipped_config, "prod")
        warned = {r.table_fqn: r.warnings for r in reports if r.warnings}
        assert not warned, warned

    def test_build_run_spec_end_to_end(self, shipped_config: Path):
        from ingestion_framework.config import build_run_spec

        spec = build_run_spec(shipped_config, "finance.gl_transactions", "prod")
        assert spec.target.fqn == "prod_lakehouse.bronze.gl_transactions"
        assert spec.config_hash

    def test_validate_all_reports_broken_config_without_raising(self, config_tree: Path):
        (config_tree / "tables" / "finance" / "widgets.yaml").write_text(
            "version: 1\ntable: {domain: sales}\n", encoding="utf-8"
        )
        reports = validate_all(config_tree, "dev")
        assert len(reports) == 1 and not reports[0].ok

    def test_resolver_and_validator_agree_on_shipped_tree(self, shipped_config: Path):
        resolver = ConfigResolver(shipped_config)
        for fqn in resolver.list_tables():
            resolved = resolver.resolve(fqn, "prod")
            assert validate(
                resolved.data, table_fqn=fqn, env="prod", config_root=shipped_config
            ).ok
