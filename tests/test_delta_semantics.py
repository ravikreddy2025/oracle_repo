"""Merge semantics asserted against real Delta.

The SQL these tests exercise is already asserted as text in test_loader.py and
test_transformer.py. What cannot be proved from text is whether Delta *behaves*
the way the design assumes -- that the dedupe actually prevents the multi-match
error, that a re-run converges, that the guard rejects an older replay. Those
are the acceptance criteria from BUILD_PROMPT.md, and they need an engine.

Skipped where no local Spark is available (e.g. Windows without winutils);
they run in CI and on Databricks.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from ingestion_framework.engine.loader import build_create_table_sql, build_merge_sql
from ingestion_framework.engine.transformer import build_stage_query

from .test_sql_builder import spec

pytestmark = pytest.mark.spark

INGESTED_AT = datetime(2026, 8, 24, 10, 0, 0)
LATER = datetime(2026, 8, 24, 12, 0, 0)

RAW_SCHEMA = "TXN_ID INT, AMOUNT DECIMAL(18,2), LAST_UPDATE_DATE TIMESTAMP"


@pytest.fixture
def target(spark, request):
    """A fresh Delta table name per test, dropped afterwards."""
    name = f"bronze_{request.node.name[:40].replace('-', '_')}"
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_bronze")
    fqn = f"test_bronze.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")
    yield fqn
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")


def table_spec(fqn: str, **patch):
    catalog, schema, table = fqn.split(".") if fqn.count(".") == 2 else ("spark_catalog", *fqn.split("."))
    return spec(
        target__catalog=catalog,
        target__schema=schema,
        target__table_name=table,
        **patch,
    )


def load(spark, s, rows, *, ingested_at=INGESTED_AT, run_id="run-1", create=True):
    """Run the framework's real staging + merge SQL over an in-memory batch."""
    raw = spark.createDataFrame(rows, RAW_SCHEMA)
    raw.createOrReplaceTempView("_ingest_raw")

    stage = build_stage_query(
        s,
        source_columns=raw.columns,
        run_id=run_id,
        batch_id="b1",
        ingested_at=ingested_at,
    )
    staged = spark.sql(stage.sql)
    staged.createOrReplaceTempView("_ingest_staged")

    if create:
        columns = [(f.name, f.dataType.simpleString().upper()) for f in staged.schema.fields]
        spark.sql(build_create_table_sql(s, columns))

    if s.target.write_mode == "merge":
        spark.sql(build_merge_sql(s, list(staged.columns)))
    else:
        staged.write.format("delta").mode(s.target.write_mode).saveAsTable(s.target.fqn)
    return stage


class TestDedupeBeforeMerge:
    def test_multiple_versions_of_one_key_merge_without_error(self, spark, target):
        """The acceptance criterion: an overlap window returns several versions
        of a key, and Delta raises on multi-match unless we dedupe first."""
        s = table_spec(target)
        load(spark, s, [
            (1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0)),
            (1, Decimal("20.00"), datetime(2026, 8, 24, 10, 0)),
            (1, Decimal("30.00"), datetime(2026, 8, 24, 11, 0)),
        ])
        rows = spark.table(target).collect()
        assert len(rows) == 1

    def test_the_surviving_row_is_the_newest(self, spark, target):
        s = table_spec(target)
        load(spark, s, [
            (1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0)),
            (1, Decimal("30.00"), datetime(2026, 8, 24, 11, 0)),
            (1, Decimal("20.00"), datetime(2026, 8, 24, 10, 0)),
        ])
        row = spark.table(target).collect()[0]
        assert float(row["AMOUNT"]) == 30.00
        assert row["LAST_UPDATE_DATE"] == datetime(2026, 8, 24, 11, 0)

    def test_distinct_keys_all_land(self, spark, target):
        s = table_spec(target)
        load(spark, s, [
            (1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0)),
            (2, Decimal("20.00"), datetime(2026, 8, 24, 9, 0)),
            (3, Decimal("30.00"), datetime(2026, 8, 24, 9, 0)),
        ])
        assert spark.table(target).count() == 3


class TestIdempotency:
    def test_rerunning_the_same_batch_converges(self, spark, target):
        """Acceptance criterion: re-running any task twice yields identical state."""
        s = table_spec(target)
        batch = [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0)), (2, Decimal("20.00"), datetime(2026, 8, 24, 9, 0))]
        load(spark, s, batch)
        first = {r["TXN_ID"]: float(r["AMOUNT"]) for r in spark.table(target).collect()}
        load(spark, s, batch, create=False, run_id="run-2")
        second = {r["TXN_ID"]: float(r["AMOUNT"]) for r in spark.table(target).collect()}
        assert first == second
        assert spark.table(target).count() == 2

    def test_overlap_window_reprocessing_does_not_duplicate(self, spark, target):
        # The overlap re-reads rows already loaded; the merge must absorb them.
        s = table_spec(target)
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))])
        load(spark, s, [
            (1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0)),   # re-read by the overlap
            (2, Decimal("20.00"), datetime(2026, 8, 24, 11, 0)),  # genuinely new
        ], create=False, run_id="run-2")
        assert spark.table(target).count() == 2


class TestMergeGuard:
    def test_older_replay_does_not_overwrite_a_newer_row(self, spark, target):
        """Acceptance criterion: an out-of-order batch must not win."""
        s = table_spec(target)
        load(spark, s, [(1, Decimal("30.00"), datetime(2026, 8, 24, 11, 0))])
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))], create=False, run_id="backfill")
        row = spark.table(target).collect()[0]
        assert float(row["AMOUNT"]) == 30.00  # the newer row survived

    def test_newer_batch_does_overwrite(self, spark, target):
        s = table_spec(target)
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))])
        load(spark, s, [(1, Decimal("30.00"), datetime(2026, 8, 24, 11, 0))], create=False, run_id="run-2")
        assert float(spark.table(target).collect()[0]["AMOUNT"]) == 30.00

    def test_unguarded_merge_is_last_write_wins(self, spark, target):
        s = table_spec(target, target__merge_guard="none")
        load(spark, s, [(1, Decimal("30.00"), datetime(2026, 8, 24, 11, 0))])
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))], create=False, run_id="run-2")
        assert float(spark.table(target).collect()[0]["AMOUNT"]) == 10.00


class TestAuditColumns:
    def test_first_ingested_at_survives_an_update(self, spark, target):
        s = table_spec(target)
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))], ingested_at=INGESTED_AT)
        load(spark, s, [(1, Decimal("30.00"), datetime(2026, 8, 24, 11, 0))],
             create=False, ingested_at=LATER, run_id="run-2")
        row = spark.table(target).collect()[0]
        assert row["_first_ingested_at"] == INGESTED_AT  # preserved
        assert row["_ingested_at"] == LATER              # refreshed

    def test_source_op_flips_from_insert_to_update(self, spark, target):
        s = table_spec(target)
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))])
        assert spark.table(target).collect()[0]["_source_op"] == "I"
        load(spark, s, [(1, Decimal("30.00"), datetime(2026, 8, 24, 11, 0))], create=False, run_id="run-2")
        assert spark.table(target).collect()[0]["_source_op"] == "U"

    def test_run_id_tracks_the_last_touching_run(self, spark, target):
        s = table_spec(target)
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))], run_id="run-1")
        load(spark, s, [(1, Decimal("30.00"), datetime(2026, 8, 24, 11, 0))], create=False, run_id="run-2")
        assert spark.table(target).collect()[0]["_run_id"] == "run-2"


class TestAppendMode:
    def test_append_keeps_every_version(self, spark, target):
        s = table_spec(target, target__write_mode="append", target__cluster_by=[])
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))])
        load(spark, s, [(1, Decimal("30.00"), datetime(2026, 8, 24, 11, 0))], create=False, run_id="run-2")
        assert spark.table(target).count() == 2

    def test_append_does_not_dedupe_the_batch(self, spark, target):
        s = table_spec(target, target__write_mode="append", target__cluster_by=[])
        load(spark, s, [
            (1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0)),
            (1, Decimal("20.00"), datetime(2026, 8, 24, 10, 0)),
        ])
        assert spark.table(target).count() == 2


class TestTableCreation:
    def test_created_table_carries_provenance_properties(self, spark, target):
        s = table_spec(target)
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))])
        properties = {
            r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()
        }
        assert properties.get("ingestion.source_table") == "GLOWNER.GL_TRANSACTIONS"
        assert properties.get("ingestion.managed_by") == "ingestion-framework"

    def test_audit_columns_exist_on_the_created_table(self, spark, target):
        s = table_spec(target)
        load(spark, s, [(1, Decimal("10.00"), datetime(2026, 8, 24, 9, 0))])
        columns = set(spark.table(target).columns)
        assert {"_ingested_at", "_ingested_date", "_run_id", "_batch_id",
                "_source_op", "_first_ingested_at"} <= columns
