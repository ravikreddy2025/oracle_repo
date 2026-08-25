"""Source-vs-target reconciliation and data-quality expectations.

The row-count check is less obvious than it looks. The extract may return
several versions of a key (an overlap window does this routinely), and the
transformer collapses them before the merge. So comparing the source count
straight against rows written would raise a false alarm on every incremental
run of a healthy table. What must balance is:

    source_count - duplicates_removed == rows_written

Expectations run against the *staged* view -- after dedupe, before the load --
because that is the data that is about to become the table.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Sequence

from .run_spec import Expectation, RunSpec
from .sql_builder import SqlBuildError
from .transformer import STAGED_VIEW, normalize_column


class CheckStatus(str, Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNED = "WARNED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class CheckResult:
    check_type: str
    check_name: str
    status: CheckStatus
    source_count: int | None = None
    target_count: int | None = None
    delta: int | None = None
    details: str | None = None

    @property
    def is_failure(self) -> bool:
        return self.status is CheckStatus.FAILED

    def to_row(self, *, run_id: str, table_fqn: str, env: str) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "table_fqn": table_fqn,
            "env": env,
            "check_type": self.check_type,
            "check_name": self.check_name,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "delta": self.delta,
            "status": self.status.value,
            "details": self.details,
        }


# -- row count --------------------------------------------------------------


def reconcile_row_count(
    *,
    source_count: int | None,
    duplicates_removed: int | None,
    rows_written: int | None,
) -> CheckResult:
    """Compare what the source offered with what the target absorbed."""
    if source_count is None or rows_written is None:
        return CheckResult(
            check_type="row_count",
            check_name="source_vs_target",
            status=CheckStatus.SKIPPED,
            source_count=source_count,
            target_count=rows_written,
            details="counts unavailable (reconciliation disabled or metrics missing)",
        )

    expected = source_count - (duplicates_removed or 0)
    delta = rows_written - expected
    status = CheckStatus.PASSED if delta == 0 else CheckStatus.FAILED
    details = None
    if delta != 0:
        details = (
            f"expected {expected} rows (source {source_count} "
            f"- {duplicates_removed or 0} deduped) but wrote {rows_written}"
        )
    return CheckResult(
        check_type="row_count",
        check_name="source_vs_target",
        status=status,
        source_count=source_count,
        target_count=rows_written,
        delta=delta,
        details=details,
    )


def check_null_keys(null_key_rows: int | None, *, action: str = "fail") -> CheckResult:
    """A NULL merge key can never match, so it silently inserts a new row forever."""
    if null_key_rows is None:
        return CheckResult("null_key", "merge_keys_not_null", CheckStatus.SKIPPED)
    if null_key_rows == 0:
        return CheckResult("null_key", "merge_keys_not_null", CheckStatus.PASSED, delta=0)
    status = CheckStatus.FAILED if action == "fail" else CheckStatus.WARNED
    return CheckResult(
        check_type="null_key",
        check_name="merge_keys_not_null",
        status=status,
        delta=null_key_rows,
        details=f"{null_key_rows} row(s) have a NULL merge key and can never match on merge",
    )


def check_duplicates(duplicates_removed: int | None, spec: RunSpec) -> CheckResult:
    """Duplicates are expected on an incremental overlap, suspicious on a full load."""
    if duplicates_removed is None:
        return CheckResult("duplicates", "duplicate_keys", CheckStatus.SKIPPED)
    if duplicates_removed == 0:
        return CheckResult("duplicates", "duplicate_keys", CheckStatus.PASSED, delta=0)
    expected = spec.extraction.is_incremental
    return CheckResult(
        check_type="duplicates",
        check_name="duplicate_keys",
        status=CheckStatus.PASSED if expected else CheckStatus.WARNED,
        delta=duplicates_removed,
        details=(
            f"{duplicates_removed} duplicate key row(s) collapsed to latest"
            + ("" if expected else " -- unexpected on a full load; the source key may not be unique")
        ),
    )


# -- expectations -----------------------------------------------------------


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def build_expectation_query(
    expectation: Expectation, spec: RunSpec, view: str = STAGED_VIEW
) -> str:
    """SQL returning the number of rows that VIOLATE the expectation."""
    column = normalize_column(expectation.column, spec.target.column_case)
    rule = expectation.rule

    if rule == "not_null":
        return f"SELECT COUNT(*) AS VIOLATIONS FROM {view} WHERE {column} IS NULL"
    if rule == "unique":
        return (
            f"SELECT COUNT(*) - COUNT(DISTINCT {column}) AS VIOLATIONS FROM {view}"
        )
    if rule == "in_set":
        if not expectation.values:
            raise SqlBuildError(f"expectation on {column}: rule 'in_set' needs values")
        values = ", ".join(_literal(v) for v in expectation.values)
        # NULLs are the not_null rule's business, not this one's.
        return (
            f"SELECT COUNT(*) AS VIOLATIONS FROM {view} "
            f"WHERE {column} IS NOT NULL AND {column} NOT IN ({values})"
        )
    if rule in {"min", "max"}:
        if expectation.value is None:
            raise SqlBuildError(f"expectation on {column}: rule {rule!r} needs a value")
        operator = "<" if rule == "min" else ">"
        return (
            f"SELECT COUNT(*) AS VIOLATIONS FROM {view} "
            f"WHERE {column} IS NOT NULL AND {column} {operator} {_literal(expectation.value)}"
        )
    if rule == "regex":
        if not expectation.pattern:
            raise SqlBuildError(f"expectation on {column}: rule 'regex' needs a pattern")
        return (
            f"SELECT COUNT(*) AS VIOLATIONS FROM {view} "
            f"WHERE {column} IS NOT NULL AND NOT {column} RLIKE {_literal(expectation.pattern)}"
        )
    raise SqlBuildError(f"unknown expectation rule {rule!r}")


def evaluate_expectation(expectation: Expectation, violations: int) -> CheckResult:
    name = f"{expectation.column} {expectation.rule}"
    if violations == 0:
        return CheckResult("expectation", name, CheckStatus.PASSED, delta=0)
    status = CheckStatus.FAILED if expectation.action == "fail" else CheckStatus.WARNED
    return CheckResult(
        check_type="expectation",
        check_name=name,
        status=status,
        delta=violations,
        details=f"{violations} row(s) violate {name}",
    )


# -- orchestration ----------------------------------------------------------


@dataclass
class ReconciliationReport:
    checks: list[CheckResult]

    @property
    def failures(self) -> list[CheckResult]:
        return [c for c in self.checks if c.is_failure]

    @property
    def warnings(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status is CheckStatus.WARNED]

    @property
    def ok(self) -> bool:
        return not self.failures

    def to_rows(self, *, run_id: str, table_fqn: str, env: str) -> list[dict[str, Any]]:
        return [c.to_row(run_id=run_id, table_fqn=table_fqn, env=env) for c in self.checks]


class Reconciler:
    """Runs the counts and expectations for one loaded batch."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def run(
        self,
        spec: RunSpec,
        *,
        source_count: int | None,
        duplicates_removed: int | None,
        null_key_rows: int | None,
        rows_written: int | None,
        view: str = STAGED_VIEW,
    ) -> ReconciliationReport:
        checks: list[CheckResult] = []

        if spec.quality.row_count_reconciliation:
            checks.append(
                reconcile_row_count(
                    source_count=source_count,
                    duplicates_removed=duplicates_removed,
                    rows_written=rows_written,
                )
            )
        checks.append(check_duplicates(duplicates_removed, spec))
        if spec.quality.null_check_keys:
            checks.append(check_null_keys(null_key_rows))

        for expectation in spec.quality.expectations:
            checks.append(self._evaluate(expectation, spec, view))

        return ReconciliationReport(checks=checks)

    def _evaluate(self, expectation: Expectation, spec: RunSpec, view: str) -> CheckResult:
        query = build_expectation_query(expectation, spec, view)
        rows = self._spark.sql(query).collect()
        violations = int(rows[0][0]) if rows else 0
        return evaluate_expectation(expectation, violations)


def summarize(report: ReconciliationReport) -> Mapping[str, Any]:
    """A compact form for logs and audit payloads."""
    return {
        "checks": len(report.checks),
        "failed": len(report.failures),
        "warned": len(report.warnings),
        "failed_checks": [c.check_name for c in report.failures],
    }


def all_checks(reports: Sequence[ReconciliationReport]) -> list[CheckResult]:
    return [check for report in reports for check in report.checks]
