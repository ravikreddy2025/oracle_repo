from __future__ import annotations

import pytest

from ingestion_framework.observability.monitoring import (
    all_queries,
    config_changes,
    freshness,
    recent_failures,
    reconciliation_issues,
    run_success_rate,
    stuck_runs,
    table_health,
    volume_trend,
    watermark_stalls,
)

from .test_batch_runner import table

CATALOG, SCHEMA = "prod_lakehouse", "control"


def sql(builder, *args, **kwargs) -> str:
    return builder(CATALOG, SCHEMA, *args, **kwargs).sql


class TestCoverage:
    def test_every_query_is_named_and_described(self):
        for query in all_queries(CATALOG, SCHEMA):
            assert query.name and query.title and query.description
            assert query.sql.strip()

    def test_names_are_unique(self):
        names = [q.name for q in all_queries(CATALOG, SCHEMA)]
        assert len(names) == len(set(names))

    def test_the_operational_questions_are_covered(self):
        names = {q.name for q in all_queries(CATALOG, SCHEMA)}
        assert names >= {
            "run_success_rate",   # is anything failing
            "freshness",          # is anything stale
            "volume_trend",       # did volumes move
            "recent_failures",    # what broke, with the error
            "reconciliation_issues",
            "config_changes",     # did something change under me
        }

    def test_every_query_targets_the_control_schema(self):
        for query in all_queries(CATALOG, SCHEMA):
            assert "prod_lakehouse.control." in query.sql

    def test_catalog_is_parameterised(self):
        assert "dev_lakehouse.control." in run_success_rate("dev_lakehouse", "control").sql


class TestTableHealth:
    def test_counts_only_the_final_attempt(self):
        # Retries would otherwise inflate both the run count and the failures.
        query = sql(table_health)
        assert "MAX(attempt)" in query
        assert "t.attempt = latest.attempt" in query

    def test_orders_worst_first(self):
        assert "ORDER BY failed DESC" in sql(table_health)


class TestFreshness:
    def test_inlines_the_sla_from_config(self):
        specs = [table("gl", alerting={"freshness_sla_hours": 4})]
        query = freshness(CATALOG, SCHEMA, specs).sql
        assert "('finance.gl', 'prod', 4)" in query or "('finance.gl', 'dev', 4)" in query

    def test_tables_without_an_sla_still_appear(self):
        specs = [table("gl")]
        query = freshness(CATALOG, SCHEMA, specs).sql
        assert "CAST(NULL AS DOUBLE)" in query
        assert "LEFT JOIN sla" in query  # a missing SLA must not hide the table

    def test_no_specs_still_produces_valid_sql(self):
        query = freshness(CATALOG, SCHEMA, []).sql
        assert "WHERE 1 = 0" in query
        assert "FROM prod_lakehouse.control.watermarks" in query

    def test_breaches_sort_to_the_top(self):
        assert "ORDER BY sla_breached DESC" in sql(freshness)

    def test_measures_age_since_the_watermark_advanced(self):
        query = sql(freshness)
        assert "hours_since_advance" in query
        assert "w.updated_at" in query


class TestVolumeTrend:
    def test_compares_each_table_against_its_own_median(self):
        # An absolute threshold is meaningless across tables of different sizes.
        query = sql(volume_trend)
        assert "PERCENTILE(rows_written, 0.5)" in query
        assert "pct_vs_median" in query

    def test_only_successful_loads_count(self):
        assert "WHERE status = 'SUCCEEDED'" in sql(volume_trend)

    def test_zero_median_does_not_divide_by_zero(self):
        assert "b.median_rows = 0 THEN NULL" in sql(volume_trend)


class TestFailuresAndChecks:
    def test_failures_carry_the_error_and_the_bound(self):
        query = sql(recent_failures)
        assert "error_type" in query and "error_message" in query
        assert "watermark_from" in query  # what window was being read

    def test_failure_message_is_truncated_for_a_dashboard(self):
        assert "SUBSTRING(error_message, 1, 500)" in sql(recent_failures)

    def test_reconciliation_includes_warnings_not_just_failures(self):
        # WARNED checks did not stop the load, so they are the ones most likely
        # to go unnoticed.
        assert "IN ('FAILED', 'WARNED')" in sql(reconciliation_issues)


class TestStallDetection:
    def test_stuck_runs_look_for_open_run_rows(self):
        query = sql(stuck_runs)
        assert "WHERE status = 'RUNNING'" in query
        assert "hours_open" in query

    def test_watermark_stalls_require_every_recent_run_to_be_empty(self):
        query = sql(watermark_stalls)
        assert "watermark_to IS NULL" in query
        assert "HAVING" in query

    def test_stall_window_is_configurable(self):
        assert "rn <= 3" in watermark_stalls(CATALOG, SCHEMA, runs=3).sql


class TestConfigChanges:
    def test_reads_the_registry_with_provenance(self):
        query = sql(config_changes)
        assert "config_registry" in query
        assert "config_sources" in query and "first_seen_at" in query


class TestWindows:
    @pytest.mark.parametrize(
        "builder,needle",
        [
            (run_success_rate, "INTERVAL 3 DAYS"),
            (table_health, "INTERVAL 3 DAYS"),
            (volume_trend, "INTERVAL 3 DAYS"),
            (reconciliation_issues, "INTERVAL 3 DAYS"),
            (config_changes, "INTERVAL 3 DAYS"),
        ],
    )
    def test_lookback_is_configurable(self, builder, needle):
        assert needle in builder(CATALOG, SCHEMA, 3).sql
