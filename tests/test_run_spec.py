from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from ingestion_framework.config import build_run_spec
from ingestion_framework.engine.run_spec import RunSpec
from ingestion_framework.utils.duration import DurationParseError, format_duration, parse_duration

from .test_validator import BASE


class TestFromConfig:
    def test_identity(self):
        spec = RunSpec.from_config(BASE, config_hash="abc")
        assert spec.table_fqn == "finance.gl_transactions"
        assert spec.table.source_fqn == "GLOWNER.GL_TRANSACTIONS"
        assert spec.config_hash == "abc"

    def test_target_and_control_names_are_three_level(self):
        spec = RunSpec.from_config(BASE)
        assert spec.target.fqn == "dev_lakehouse.bronze.gl_transactions"
        assert spec.control.watermarks == "dev_lakehouse.control.watermarks"
        assert spec.control.tasks == "dev_lakehouse.control.ingestion_tasks"

    def test_lists_become_tuples_so_the_spec_stays_immutable(self):
        spec = RunSpec.from_config(BASE)
        assert spec.target.merge_keys == ("TXN_ID",)
        with pytest.raises(AttributeError):
            spec.target.merge_keys = ("OTHER",)  # type: ignore[misc]

    def test_extraction_helpers(self):
        spec = RunSpec.from_config(BASE)
        assert spec.extraction.is_incremental
        assert not spec.extraction.selects_all_columns
        assert spec.extraction.column_list == ("TXN_ID", "AMOUNT", "LAST_UPDATE_DATE")
        assert spec.extraction.incremental.overlap_delta == timedelta(hours=6)
        assert spec.extraction.incremental.effective_watermark_column == "LAST_UPDATE_DATE"

    def test_scn_strategy_reports_the_pseudo_column(self):
        cfg = {**BASE, "extraction": {**BASE["extraction"]}}
        cfg["extraction"]["incremental"] = {"strategy": "scn"}
        spec = RunSpec.from_config(cfg)
        assert spec.extraction.incremental.uses_scn
        assert spec.extraction.incremental.effective_watermark_column == "ORA_ROWSCN"

    def test_star_projection(self):
        cfg = {**BASE, "extraction": {**BASE["extraction"], "columns": "*"}}
        spec = RunSpec.from_config(cfg)
        assert spec.extraction.selects_all_columns
        assert spec.extraction.column_list == ()

    def test_parallel_read_detection(self):
        cfg = {**BASE, "source": {**BASE["source"], "read": {"num_partitions": 16, "partition_column": "TXN_ID"}}}
        assert RunSpec.from_config(cfg).source.read.is_parallel
        assert not RunSpec.from_config(BASE).source.read.is_parallel

    def test_secret_key_defaults(self):
        spec = RunSpec.from_config(BASE)
        assert (spec.source.username_key, spec.source.password_key) == ("username", "password")

    def test_defaults_fill_absent_optional_blocks(self):
        spec = RunSpec.from_config(BASE)
        assert spec.runtime.retries == 2
        assert spec.quality.row_count_reconciliation is True
        assert spec.alerting.on_failure == ()
        assert spec.log_level == "INFO"


class TestFromShippedConfig:
    def test_incremental_merge_table(self, shipped_config: Path):
        spec = build_run_spec(shipped_config, "finance.gl_transactions", "prod")
        assert spec.target.is_merge
        assert spec.target.cluster_by == ("TXN_ID",)
        assert spec.source.read.num_partitions == 32
        assert spec.extraction.incremental.overlap_delta == timedelta(hours=6)
        assert len(spec.quality.expectations) == 2

    def test_full_load_table(self, shipped_config: Path):
        spec = build_run_spec(shipped_config, "finance.gl_accounts", "prod")
        assert spec.extraction.mode == "full"
        assert spec.extraction.selects_all_columns
        assert spec.target.merge_guard == "none"
        assert spec.target.merge_keys == ("ACCOUNT_ID",)  # inherited from business_key

    def test_append_override_table(self, shipped_config: Path):
        spec = build_run_spec(shipped_config, "sales.order_events", "prod")
        assert spec.target.write_mode == "append"
        assert spec.extraction.incremental.uses_scn
        assert spec.target.partition_by == ("_ingested_date",)


class TestDuration:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("PT0S", timedelta(0)),
            ("PT6H", timedelta(hours=6)),
            ("PT30M", timedelta(minutes=30)),
            ("P1D", timedelta(days=1)),
            ("P1DT2H30M", timedelta(days=1, hours=2, minutes=30)),
            (None, timedelta(0)),
        ],
    )
    def test_parse(self, text, expected):
        assert parse_duration(text) == expected

    @pytest.mark.parametrize("text", ["6h", "P", "PT", "1D", "P1Y", "P1M", ""])
    def test_rejects_ambiguous_or_variable_length(self, text):
        if text == "":
            assert parse_duration(text) == timedelta(0)
            return
        with pytest.raises(DurationParseError):
            parse_duration(text)

    @pytest.mark.parametrize("text", ["PT0S", "PT6H", "P1D", "P1DT2H30M", "PT45S"])
    def test_round_trip(self, text):
        assert format_duration(parse_duration(text)) == text
