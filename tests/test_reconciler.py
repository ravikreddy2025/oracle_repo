from __future__ import annotations

import pytest

from ingestion_framework.engine.reconciler import (
    CheckStatus,
    ReconciliationReport,
    Reconciler,
    build_expectation_query,
    check_duplicates,
    check_null_keys,
    evaluate_expectation,
    reconcile_row_count,
    summarize,
)
from ingestion_framework.engine.run_spec import Expectation
from ingestion_framework.engine.sql_builder import SqlBuildError

from .test_sql_builder import spec


class TestRowCount:
    def test_balanced_counts_pass(self):
        result = reconcile_row_count(source_count=100, duplicates_removed=0, rows_written=100)
        assert result.status is CheckStatus.PASSED
        assert result.delta == 0

    def test_dedupe_is_accounted_for(self):
        """The subtlety: a healthy incremental run writes fewer rows than the
        source returned, because the overlap window duplicates keys."""
        result = reconcile_row_count(source_count=120, duplicates_removed=20, rows_written=100)
        assert result.status is CheckStatus.PASSED
        assert result.delta == 0

    def test_missing_rows_fail_with_the_arithmetic_spelled_out(self):
        result = reconcile_row_count(source_count=100, duplicates_removed=0, rows_written=95)
        assert result.status is CheckStatus.FAILED
        assert result.delta == -5
        assert "expected 100 rows" in result.details and "wrote 95" in result.details

    def test_extra_rows_also_fail(self):
        result = reconcile_row_count(source_count=100, duplicates_removed=0, rows_written=105)
        assert result.status is CheckStatus.FAILED
        assert result.delta == 5

    def test_unavailable_counts_skip_rather_than_pass(self):
        # 'We could not check' must never look like 'we checked and it was fine'.
        assert reconcile_row_count(
            source_count=None, duplicates_removed=None, rows_written=100
        ).status is CheckStatus.SKIPPED
        assert reconcile_row_count(
            source_count=100, duplicates_removed=None, rows_written=None
        ).status is CheckStatus.SKIPPED


class TestNullKeys:
    def test_no_nulls_pass(self):
        assert check_null_keys(0).status is CheckStatus.PASSED

    def test_nulls_fail_and_explain_why_it_matters(self):
        result = check_null_keys(3)
        assert result.status is CheckStatus.FAILED
        assert "can never match on merge" in result.details

    def test_warn_action(self):
        assert check_null_keys(3, action="warn").status is CheckStatus.WARNED

    def test_unknown_skips(self):
        assert check_null_keys(None).status is CheckStatus.SKIPPED


class TestDuplicates:
    def test_none_is_a_pass(self):
        assert check_duplicates(0, spec()).status is CheckStatus.PASSED

    def test_expected_on_an_incremental_overlap(self):
        result = check_duplicates(15, spec())
        assert result.status is CheckStatus.PASSED
        assert "collapsed to latest" in result.details

    def test_suspicious_on_a_full_load(self):
        # A full load of a table with a unique key should not have duplicates.
        s = spec(extraction__mode="full", extraction__incremental={})
        result = check_duplicates(15, s)
        assert result.status is CheckStatus.WARNED
        assert "may not be unique" in result.details


class TestExpectationQueries:
    def q(self, **kwargs):
        return build_expectation_query(Expectation(**kwargs), spec())

    def test_not_null(self):
        assert self.q(column="AMOUNT", rule="not_null").endswith("WHERE AMOUNT IS NULL")

    def test_unique(self):
        assert "COUNT(*) - COUNT(DISTINCT AMOUNT)" in self.q(column="AMOUNT", rule="unique")

    def test_in_set_quotes_values(self):
        query = self.q(column="CURRENCY", rule="in_set", values=("USD", "EUR"))
        assert "NOT IN ('USD', 'EUR')" in query

    def test_in_set_ignores_nulls(self):
        # NULL handling belongs to not_null, or every in_set would double-report.
        assert "IS NOT NULL" in self.q(column="CURRENCY", rule="in_set", values=("USD",))

    def test_in_set_escapes_quotes(self):
        query = self.q(column="NAME", rule="in_set", values=("O'Brien",))
        assert "'O''Brien'" in query

    def test_numeric_values_are_not_quoted(self):
        assert "NOT IN (1, 2)" in self.q(column="CODE", rule="in_set", values=(1, 2))

    def test_min_and_max(self):
        assert "AMOUNT < 0" in self.q(column="AMOUNT", rule="min", value=0)
        assert "AMOUNT > 100" in self.q(column="AMOUNT", rule="max", value=100)

    def test_regex(self):
        assert "NOT CODE RLIKE '^[A-Z]+$'" in self.q(column="CODE", rule="regex", pattern="^[A-Z]+$")

    def test_column_case_policy_is_applied(self):
        query = build_expectation_query(
            Expectation(column="AMOUNT", rule="not_null"), spec(target__column_case="lower")
        )
        assert "WHERE amount IS NULL" in query

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"column": "C", "rule": "in_set"},
            {"column": "C", "rule": "min"},
            {"column": "C", "rule": "regex"},
            {"column": "C", "rule": "bogus"},
        ],
    )
    def test_incomplete_or_unknown_rules_are_refused(self, kwargs):
        with pytest.raises(SqlBuildError):
            build_expectation_query(Expectation(**kwargs), spec())


class TestEvaluateExpectation:
    def test_zero_violations_pass(self):
        assert evaluate_expectation(
            Expectation(column="A", rule="not_null"), 0
        ).status is CheckStatus.PASSED

    def test_violations_fail_by_default(self):
        result = evaluate_expectation(Expectation(column="A", rule="not_null"), 7)
        assert result.status is CheckStatus.FAILED
        assert result.check_name == "A not_null"
        assert result.delta == 7

    def test_warn_action_does_not_fail_the_task(self):
        result = evaluate_expectation(
            Expectation(column="A", rule="not_null", action="warn"), 7
        )
        assert result.status is CheckStatus.WARNED


class FakeSpark:
    def __init__(self, violations_by_query=None):
        self.violations = violations_by_query or {}
        self.queries: list[str] = []

    def sql(self, query):
        self.queries.append(query)
        count = 0
        for needle, value in self.violations.items():
            if needle in query:
                count = value
                break
        return FakeResult(count)


class FakeResult:
    def __init__(self, value):
        self.value = value

    def collect(self):
        return [[self.value]]


class TestReconciler:
    def test_runs_counts_and_expectations(self):
        s = spec(quality={
            "row_count_reconciliation": True,
            "null_check_keys": True,
            "expectations": [{"column": "AMOUNT", "rule": "not_null"}],
        })
        spark = FakeSpark()
        report = Reconciler(spark).run(
            s, source_count=100, duplicates_removed=0, null_key_rows=0, rows_written=100
        )
        assert report.ok
        assert {c.check_type for c in report.checks} == {"row_count", "duplicates", "null_key", "expectation"}

    def test_failing_expectation_makes_the_report_not_ok(self):
        s = spec(quality={"expectations": [{"column": "AMOUNT", "rule": "not_null"}]})
        spark = FakeSpark({"AMOUNT IS NULL": 4})
        report = Reconciler(spark).run(
            s, source_count=100, duplicates_removed=0, null_key_rows=0, rows_written=100
        )
        assert not report.ok
        assert report.failures[0].check_name == "AMOUNT not_null"

    def test_warning_expectation_keeps_the_report_ok(self):
        s = spec(quality={
            "expectations": [{"column": "AMOUNT", "rule": "not_null", "action": "warn"}]
        })
        spark = FakeSpark({"AMOUNT IS NULL": 4})
        report = Reconciler(spark).run(
            s, source_count=100, duplicates_removed=0, null_key_rows=0, rows_written=100
        )
        assert report.ok
        assert len(report.warnings) == 1

    def test_reconciliation_can_be_switched_off(self):
        s = spec(quality={"row_count_reconciliation": False, "null_check_keys": False})
        report = Reconciler(FakeSpark()).run(
            s, source_count=100, duplicates_removed=0, null_key_rows=0, rows_written=95
        )
        assert {c.check_type for c in report.checks} == {"duplicates"}

    def test_rows_are_shaped_for_the_control_table(self):
        report = Reconciler(FakeSpark()).run(
            spec(), source_count=10, duplicates_removed=0, null_key_rows=0, rows_written=10
        )
        rows = report.to_rows(run_id="prod-1", table_fqn="finance.gl", env="prod")
        assert all(r["run_id"] == "prod-1" and r["env"] == "prod" for r in rows)
        assert set(rows[0]) >= {"check_type", "check_name", "status", "delta", "details"}

    def test_summary_is_compact(self):
        report = ReconciliationReport(checks=[
            evaluate_expectation(Expectation(column="A", rule="not_null"), 1),
            evaluate_expectation(Expectation(column="B", rule="not_null"), 0),
        ])
        assert summarize(report) == {
            "checks": 2, "failed": 1, "warned": 0, "failed_checks": ["A not_null"]
        }
