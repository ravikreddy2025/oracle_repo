from __future__ import annotations

from datetime import datetime

import pytest

from ingestion_framework.engine.sql_builder import SqlBuildError
from ingestion_framework.engine.transformer import (
    AUDIT_COLUMNS,
    INSERT_ONLY_AUDIT_COLUMNS,
    build_duplicate_count_query,
    build_null_key_query,
    build_stage_query,
    dedupe_order_expression,
    needs_dedupe,
    normalize_column,
    source_column_name,
)

from .test_sql_builder import spec

INGESTED_AT = datetime(2026, 8, 24, 10, 15, 30, 500000)
COLUMNS = ["TXN_ID", "AMOUNT", "LAST_UPDATE_DATE"]


_UNSET = object()


def stage(s=None, columns=_UNSET, **kwargs):
    return build_stage_query(
        s or spec(),
        source_columns=COLUMNS if columns is _UNSET else columns,
        run_id=kwargs.pop("run_id", "prod-20260824T100000-a1b2c3"),
        batch_id=kwargs.pop("batch_id", "batch-001"),
        ingested_at=kwargs.pop("ingested_at", INGESTED_AT),
        **kwargs,
    )


class TestAuditColumns:
    def test_all_six_are_stamped(self):
        query = stage()
        assert query.audit_columns == AUDIT_COLUMNS
        assert set(AUDIT_COLUMNS) <= set(query.columns)

    def test_values_are_rendered_as_typed_literals(self):
        sql = stage().sql
        assert "TIMESTAMP '2026-08-24 10:15:30.500000' AS _ingested_at" in sql
        assert "DATE '2026-08-24' AS _ingested_date" in sql
        assert "'prod-20260824T100000-a1b2c3' AS _run_id" in sql
        assert "'batch-001' AS _batch_id" in sql

    def test_ingested_date_is_derived_from_ingested_at(self):
        sql = stage(ingested_at=datetime(2026, 12, 31, 23, 59, 59)).sql
        assert "DATE '2026-12-31' AS _ingested_date" in sql

    def test_every_staged_row_claims_insert(self):
        # The MERGE's update branch rewrites the ones that turn out to be updates.
        assert "'I' AS _source_op" in stage().sql

    def test_first_ingested_at_starts_equal_to_ingested_at(self):
        sql = stage().sql
        assert "TIMESTAMP '2026-08-24 10:15:30.500000' AS _first_ingested_at" in sql

    def test_first_ingested_at_is_marked_insert_only(self):
        assert INSERT_ONLY_AUDIT_COLUMNS == {"_first_ingested_at"}

    def test_audit_columns_can_be_turned_off(self):
        s = spec(target__add_audit_columns=False)
        query = stage(s)
        assert query.audit_columns == ()
        assert query.columns == tuple(COLUMNS)

    def test_unsafe_framework_literal_is_refused(self):
        with pytest.raises(SqlBuildError, match="safe framework literal"):
            stage(run_id="run'; DROP TABLE x --")


class TestProjection:
    def test_columns_are_preserved_by_default(self):
        # Bronze is a 1:1 mirror, so Oracle's spelling survives.
        query = stage()
        assert query.data_columns == ("TXN_ID", "AMOUNT", "LAST_UPDATE_DATE")
        assert "TXN_ID," in query.sql
        assert "AS txn_id" not in query.sql

    def test_lower_case_policy_aliases_every_column(self):
        query = stage(spec(target__column_case="lower"))
        assert query.data_columns == ("txn_id", "amount", "last_update_date")
        assert "TXN_ID AS txn_id" in query.sql

    def test_scn_alias_is_unwrapped(self):
        assert source_column_name("ORA_ROWSCN AS ORA_ROWSCN") == "ORA_ROWSCN"
        assert source_column_name("TXN_ID") == "TXN_ID"

    def test_normalize_column(self):
        assert normalize_column("A_B", "preserve") == "A_B"
        assert normalize_column("A_B", "lower") == "a_b"

    def test_empty_batch_schema_is_refused(self):
        with pytest.raises(SqlBuildError, match="no columns"):
            stage(columns=[])


class TestDedupe:
    def test_merge_target_dedupes(self):
        assert needs_dedupe(spec(target__write_mode="merge"))

    def test_append_target_does_not(self):
        # Only a MERGE can fail on duplicate keys, so only a MERGE pays for it.
        assert not needs_dedupe(spec(target__write_mode="append"))

    def test_merge_without_keys_does_not(self):
        assert not needs_dedupe(spec(target__write_mode="merge", target__merge_keys=[]))

    def test_window_partitions_by_the_merge_keys(self):
        query = stage()
        assert query.dedupe_applied
        assert "PARTITION BY TXN_ID" in query.sql
        assert "ROW_NUMBER() OVER" in query.sql
        assert "WHERE _rn = 1" in query.sql

    def test_composite_keys(self):
        s = spec(target__merge_keys=["TXN_ID", "AMOUNT"], target__cluster_by=[])
        assert "PARTITION BY TXN_ID, AMOUNT" in stage(s).sql

    def test_newest_row_wins_by_watermark(self):
        assert "ORDER BY LAST_UPDATE_DATE DESC NULLS LAST" in stage().sql

    def test_scn_orders_by_the_pseudocolumn(self):
        s = spec(
            extraction__columns="*",
            extraction__incremental={"strategy": "scn", "watermark_type": "number"},
            target__write_mode="merge",
        )
        sql = stage(s, columns=["TXN_ID", "ORA_ROWSCN AS ORA_ROWSCN"]).sql
        assert "ORDER BY ORA_ROWSCN DESC NULLS LAST" in sql

    def test_no_watermark_falls_back_to_a_deterministic_order(self):
        s = spec(extraction__mode="full", extraction__incremental={})
        assert "monotonically_increasing_id() DESC" in dedupe_order_expression(s, COLUMNS)

    def test_watermark_absent_from_projection_falls_back(self):
        s = spec()
        assert "monotonically_increasing_id()" in dedupe_order_expression(s, ["TXN_ID", "AMOUNT"])

    def test_append_target_has_no_window(self):
        query = stage(spec(target__write_mode="append"))
        assert not query.dedupe_applied
        assert "ROW_NUMBER" not in query.sql

    def test_merge_key_missing_from_the_batch_is_refused(self):
        # This would otherwise surface as a confusing Delta error mid-merge.
        with pytest.raises(SqlBuildError, match="are not in the extracted columns"):
            stage(columns=["AMOUNT", "LAST_UPDATE_DATE"])

    def test_dedupe_reports_its_keys_and_order(self):
        query = stage()
        assert query.dedupe_keys == ("TXN_ID",)
        assert query.order_by == "LAST_UPDATE_DATE DESC NULLS LAST"


class TestQualityQueries:
    def test_duplicate_count(self):
        assert build_duplicate_count_query(spec()) == (
            "SELECT COUNT(*) - COUNT(DISTINCT TXN_ID) AS DUPLICATE_ROWS FROM _ingest_raw"
        )

    def test_null_key_check(self):
        assert build_null_key_query(spec()).endswith("WHERE TXN_ID IS NULL")

    def test_composite_null_key_check(self):
        s = spec(target__merge_keys=["A", "B"], target__cluster_by=[])
        assert build_null_key_query(s).endswith("WHERE A IS NULL OR B IS NULL")

    def test_without_keys_both_are_refused(self):
        s = spec(target__merge_keys=[], target__cluster_by=[])
        with pytest.raises(SqlBuildError):
            build_duplicate_count_query(s)
        with pytest.raises(SqlBuildError):
            build_null_key_query(s)


class TestShippedTables:
    def build(self, shipped_config, fqn, env="prod", columns=None):
        from ingestion_framework.config import build_run_spec

        s = build_run_spec(shipped_config, fqn, env)
        return build_stage_query(
            s,
            source_columns=columns or ["TXN_ID", "LAST_UPDATE_DATE"],
            run_id="prod-1",
            batch_id="b1",
            ingested_at=INGESTED_AT,
        )

    def test_gl_transactions_dedupes_on_txn_id(self, shipped_config):
        query = self.build(shipped_config, "finance.gl_transactions")
        assert query.dedupe_applied
        assert query.dedupe_keys == ("TXN_ID",)

    def test_gl_accounts_dedupes_with_no_watermark_to_order_by(self, shipped_config):
        query = self.build(shipped_config, "finance.gl_accounts", columns=["ACCOUNT_ID", "NAME"])
        assert query.dedupe_applied
        assert "monotonically_increasing_id()" in query.sql

    def test_order_events_skips_dedupe_entirely(self, shipped_config):
        query = self.build(
            shipped_config, "sales.order_events", columns=["EVENT_ID", "ORA_ROWSCN AS ORA_ROWSCN"]
        )
        assert not query.dedupe_applied
