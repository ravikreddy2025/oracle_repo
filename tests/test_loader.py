from __future__ import annotations

import pytest

from ingestion_framework.engine.loader import (
    build_create_table_sql,
    build_history_metrics_query,
    build_merge_sql,
    merge_guard_column,
    parse_operation_metrics,
)
from ingestion_framework.engine.sql_builder import SqlBuildError

from .test_sql_builder import spec

DATA = ["TXN_ID", "AMOUNT", "LAST_UPDATE_DATE"]
AUDIT = ["_ingested_at", "_ingested_date", "_run_id", "_batch_id", "_source_op", "_first_ingested_at"]
STAGED = DATA + AUDIT


class TestMergeGuard:
    def test_watermark_column_is_the_guard(self):
        assert merge_guard_column(spec()) == "LAST_UPDATE_DATE"

    def test_guard_can_be_switched_off(self):
        assert merge_guard_column(spec(target__merge_guard="none")) is None

    def test_no_watermark_means_no_guard(self):
        # A full-load table has nothing to order two versions of a row by.
        s = spec(extraction__mode="full", extraction__incremental={})
        assert merge_guard_column(s) is None

    def test_scn_pseudocolumn_can_guard(self):
        s = spec(extraction__incremental={"strategy": "scn", "watermark_type": "number"})
        assert merge_guard_column(s) == "ORA_ROWSCN"

    def test_guard_follows_the_casing_policy(self):
        assert merge_guard_column(spec(target__column_case="lower")) == "last_update_date"


class TestMergeSql:
    def test_matches_on_the_merge_keys(self):
        sql = build_merge_sql(spec(), STAGED)
        assert sql.startswith("MERGE INTO dev_lakehouse.bronze.gl_transactions AS t")
        assert "ON t.TXN_ID = s.TXN_ID" in sql

    def test_composite_keys_are_anded(self):
        s = spec(target__merge_keys=["TXN_ID", "AMOUNT"], target__cluster_by=[])
        assert "ON t.TXN_ID = s.TXN_ID AND t.AMOUNT = s.AMOUNT" in build_merge_sql(s, STAGED)

    def test_update_uses_an_explicit_column_list_not_star(self):
        # UPDATE SET * would clobber _first_ingested_at and _source_op.
        sql = build_merge_sql(spec(), STAGED)
        assert "UPDATE SET *" not in sql
        assert "t.AMOUNT = s.AMOUNT" in sql

    def test_update_never_touches_first_ingested_at(self):
        sql = build_merge_sql(spec(), STAGED)
        update_clause = sql.split("WHEN NOT MATCHED")[0]
        assert "t._first_ingested_at" not in update_clause

    def test_update_flips_source_op_to_u(self):
        assert "t._source_op = 'U'" in build_merge_sql(spec(), STAGED)

    def test_update_does_not_rewrite_the_merge_keys(self):
        update_clause = build_merge_sql(spec(), STAGED).split("WHEN NOT MATCHED")[0]
        assert "t.TXN_ID = s.TXN_ID," not in update_clause

    def test_insert_carries_every_column_including_audit(self):
        sql = build_merge_sql(spec(), STAGED)
        insert_clause = sql.split("WHEN NOT MATCHED")[1]
        for column in STAGED:
            assert column in insert_clause

    def test_guard_blocks_older_rows(self):
        sql = build_merge_sql(spec(), STAGED)
        assert (
            "WHEN MATCHED AND (t.LAST_UPDATE_DATE IS NULL "
            "OR s.LAST_UPDATE_DATE >= t.LAST_UPDATE_DATE)"
        ) in sql

    def test_guard_is_inclusive_so_a_rerun_refreshes_audit_columns(self):
        assert ">=" in build_merge_sql(spec(), STAGED)

    def test_unguarded_merge_is_plain_when_matched(self):
        sql = build_merge_sql(spec(target__merge_guard="none"), STAGED)
        assert "WHEN MATCHED THEN UPDATE SET" in sql
        assert "IS NULL OR" not in sql

    def test_guard_column_missing_from_the_batch_is_refused(self):
        with pytest.raises(SqlBuildError, match="either project it or set target.merge_guard"):
            build_merge_sql(spec(), ["TXN_ID", "AMOUNT"] + AUDIT)

    def test_merge_key_missing_from_the_batch_is_refused(self):
        with pytest.raises(SqlBuildError, match="not in the staged columns"):
            build_merge_sql(spec(), ["AMOUNT", "LAST_UPDATE_DATE"] + AUDIT)

    def test_merge_without_keys_is_refused(self):
        s = spec(target__merge_keys=[], target__cluster_by=[])
        with pytest.raises(SqlBuildError, match="requires merge_keys"):
            build_merge_sql(s, STAGED)

    def test_empty_batch_schema_is_refused(self):
        with pytest.raises(SqlBuildError, match="no columns"):
            build_merge_sql(spec(), [])

    def test_reads_from_the_staged_view(self):
        assert "USING _ingest_staged AS s" in build_merge_sql(spec(), STAGED)

    def test_custom_staged_view(self):
        assert "USING other_view AS s" in build_merge_sql(spec(), STAGED, staged_view="other_view")

    def test_lower_case_policy_reaches_the_merge(self):
        s = spec(target__column_case="lower")
        staged = [c.lower() for c in STAGED]
        sql = build_merge_sql(s, staged)
        assert "ON t.txn_id = s.txn_id" in sql
        assert "s.last_update_date >= t.last_update_date" in sql

    def test_sql_is_deterministic(self):
        assert build_merge_sql(spec(), STAGED) == build_merge_sql(spec(), STAGED)


class TestCreateTableSql:
    SCHEMA = [("TXN_ID", "DECIMAL(38,0)"), ("AMOUNT", "DECIMAL(18,2)"), ("_ingested_at", "TIMESTAMP")]

    def test_idempotent_three_level_name(self):
        sql = build_create_table_sql(spec(), self.SCHEMA)
        assert sql.startswith(
            "CREATE TABLE IF NOT EXISTS dev_lakehouse.bronze.gl_transactions ("
        )
        assert "USING DELTA" in sql

    def test_columns_and_types(self):
        sql = build_create_table_sql(spec(), self.SCHEMA)
        assert "TXN_ID DECIMAL(38,0)" in sql
        assert "_ingested_at TIMESTAMP" in sql

    def test_clusters_on_the_merge_keys_by_default(self):
        assert "CLUSTER BY (TXN_ID)" in build_create_table_sql(spec(), self.SCHEMA)

    def test_partitioning_wins_when_explicitly_set(self):
        # Delta rejects both on one table; partitioning is the explicit opt-out.
        s = spec(target__partition_by=["_ingested_date"], target__cluster_by=["TXN_ID"])
        sql = build_create_table_sql(s, self.SCHEMA)
        assert "PARTITIONED BY (_ingested_date)" in sql
        assert "CLUSTER BY" not in sql

    def test_change_data_feed_is_opt_in(self):
        assert "enableChangeDataFeed" not in build_create_table_sql(spec(), self.SCHEMA)
        s = spec(target__enable_change_data_feed=True)
        assert "'delta.enableChangeDataFeed' = 'true'" in build_create_table_sql(s, self.SCHEMA)

    def test_provenance_properties_are_stamped(self):
        sql = build_create_table_sql(spec(), self.SCHEMA)
        assert "'ingestion.source_table' = 'GLOWNER.GL_TRANSACTIONS'" in sql
        assert "'ingestion.managed_by' = 'ingestion-framework'" in sql

    def test_description_becomes_a_comment(self):
        s = spec(table__description="GL lines")
        assert "COMMENT 'GL lines'" in build_create_table_sql(s, self.SCHEMA)

    def test_comment_quotes_are_escaped(self):
        s = spec(table__description="it's fine")
        assert "it''s fine" in build_create_table_sql(s, self.SCHEMA)

    def test_no_columns_is_refused(self):
        with pytest.raises(SqlBuildError, match="no columns"):
            build_create_table_sql(spec(), [])


class TestMetrics:
    def test_history_query_reads_the_latest_version(self):
        query = build_history_metrics_query(spec())
        assert "DESCRIBE HISTORY dev_lakehouse.bronze.gl_transactions" in query
        assert "ORDER BY version DESC LIMIT 1" in query

    def test_merge_metrics_are_typed(self):
        metrics = parse_operation_metrics({
            "numTargetRowsInserted": "900",
            "numTargetRowsUpdated": "100",
            "numTargetRowsDeleted": "0",
            "numOutputRows": "1000",
        })
        assert metrics == {
            "rows_inserted": 900, "rows_updated": 100,
            "rows_deleted": 0, "rows_written": 1000,
        }

    def test_rows_written_is_derived_when_absent(self):
        metrics = parse_operation_metrics(
            {"numTargetRowsInserted": "5", "numTargetRowsUpdated": "3"}
        )
        assert metrics["rows_written"] == 8

    def test_missing_metrics_are_none_not_zero(self):
        # Zero rows written and 'Delta did not report' are different facts.
        assert parse_operation_metrics({})["rows_updated"] is None
        assert parse_operation_metrics(None)["rows_inserted"] is None

    def test_unparseable_values_do_not_raise(self):
        assert parse_operation_metrics({"numTargetRowsUpdated": "n/a"})["rows_updated"] is None


class TestShippedTables:
    def build(self, shipped_config, fqn, columns, env="prod"):
        from ingestion_framework.config import build_run_spec

        return build_run_spec(shipped_config, fqn, env), columns

    def test_gl_transactions_merge(self, shipped_config):
        s, columns = self.build(
            shipped_config, "finance.gl_transactions",
            ["TXN_ID", "AMOUNT", "LAST_UPDATE_DATE"] + AUDIT,
        )
        sql = build_merge_sql(s, columns)
        assert "MERGE INTO prod_lakehouse.bronze.gl_transactions" in sql
        assert "ON t.TXN_ID = s.TXN_ID" in sql
        assert "s.LAST_UPDATE_DATE >= t.LAST_UPDATE_DATE" in sql

    def test_gl_accounts_merge_is_last_write_wins(self, shipped_config):
        s, columns = self.build(shipped_config, "finance.gl_accounts", ["ACCOUNT_ID"] + AUDIT)
        sql = build_merge_sql(s, columns)
        assert "WHEN MATCHED THEN UPDATE SET" in sql  # merge_guard: none

    def test_order_events_is_partitioned_not_clustered(self, shipped_config):
        s, _ = self.build(shipped_config, "sales.order_events", [])
        sql = build_create_table_sql(s, [("EVENT_ID", "DECIMAL(38,0)"), ("_ingested_date", "DATE")])
        assert "PARTITIONED BY (_ingested_date)" in sql
        assert "CLUSTER BY" not in sql
