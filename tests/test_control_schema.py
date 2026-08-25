from __future__ import annotations

import pytest

from ingestion_framework.control.schema import (
    ALL_TABLES,
    AUDIT_LOG,
    INGESTION_TASKS,
    Column,
    ControlSchemaError,
    TableDef,
    ddl_statements,
    qualify,
)


class TestQualify:
    def test_joins_parts(self):
        assert qualify("cat", "control", "watermarks") == "cat.control.watermarks"

    @pytest.mark.parametrize(
        "bad", ["", "has space", "has-dash", "1leading", "drop;table", "back`tick", "a.b"]
    )
    def test_rejects_unsafe_identifiers(self, bad):
        # Identifiers cannot be bound as parameters, so this is the only thing
        # standing between a config value and a SQL string.
        with pytest.raises(ControlSchemaError):
            qualify("cat", bad)

    def test_accepts_underscores_and_digits(self):
        assert qualify("prod_lakehouse2", "_control") == "prod_lakehouse2._control"


class TestDDL:
    def test_every_table_is_idempotent(self):
        for statement in ddl_statements("cat", "control"):
            assert "IF NOT EXISTS" in statement

    def test_creates_schema_first(self):
        statements = ddl_statements("cat", "control")
        assert statements[0].startswith("CREATE SCHEMA IF NOT EXISTS cat.control")
        assert len(statements) == len(ALL_TABLES) + 1

    def test_all_six_control_tables_present(self):
        names = {t.name for t in ALL_TABLES}
        assert names == {
            "config_registry",
            "watermarks",
            "ingestion_runs",
            "ingestion_tasks",
            "audit_log",
            "reconciliation",
        }

    def test_three_level_names_throughout(self):
        for statement in ddl_statements("prod_lakehouse", "control"):
            assert "prod_lakehouse.control" in statement

    def test_table_ddl_shape(self):
        sql = INGESTION_TASKS.create_sql("cat", "control")
        assert sql.startswith("CREATE TABLE IF NOT EXISTS cat.control.ingestion_tasks (")
        assert "USING DELTA" in sql
        assert "CLUSTER BY (table_fqn, run_id)" in sql
        assert "TBLPROPERTIES" in sql

    def test_not_null_and_comments_render(self):
        sql = AUDIT_LOG.create_sql("cat", "control")
        assert "event_id STRING NOT NULL" in sql
        assert "COMMENT 'Order of events within a task, for stable replay'" in sql

    def test_comment_quotes_are_escaped(self):
        table = TableDef(
            name="t",
            comment="it's fine",
            columns=(Column("c", "STRING", "also it's fine"),),
        )
        sql = table.create_sql("cat", "control")
        assert "it''s fine" in sql
        assert "'it's fine'" not in sql

    def test_cluster_by_must_reference_real_columns(self):
        table = TableDef(
            name="t", comment="", columns=(Column("a", "STRING"),), cluster_by=("ghost",)
        )
        with pytest.raises(ControlSchemaError, match="not defined on the table"):
            table.create_sql("cat", "control")

    def test_bad_catalog_is_rejected_before_reaching_sql(self):
        with pytest.raises(ControlSchemaError):
            ddl_statements("cat; DROP DATABASE x", "control")

    def test_task_table_carries_the_metrics_the_design_requires(self):
        required = {
            "run_id", "table_fqn", "env", "attempt", "status", "extraction_mode",
            "write_mode", "watermark_from", "watermark_to", "source_count",
            "rows_read", "rows_written", "rows_inserted", "rows_updated",
            "config_hash", "started_at", "ended_at", "duration_ms", "error_message",
        }
        assert required <= set(INGESTION_TASKS.column_names)
