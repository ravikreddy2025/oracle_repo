"""A narrow SQL seam between the control plane and Spark.

Everything the control plane does is expressed as parameterised SQL through
this interface, which keeps two things true:

* values never reach a SQL string by concatenation -- they are bound, so a
  quote in an error message cannot corrupt a statement;
* the control logic is unit-testable without a cluster, by substituting a
  recording client.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Protocol, Sequence, runtime_checkable


@runtime_checkable
class SqlClient(Protocol):
    """Minimal contract: run a statement, optionally return rows as dicts."""

    def execute(self, statement: str, params: Mapping[str, Any] | None = None) -> None:
        """Run a statement for its effect."""

    def query(
        self, statement: str, params: Mapping[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Run a statement and return its rows."""


class SparkSqlClient:
    """SqlClient backed by a SparkSession.

    Uses Spark's named-parameter form (``spark.sql(text, args=...)``) so values
    are bound as literals by Spark rather than formatted by us.
    """

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def execute(self, statement: str, params: Mapping[str, Any] | None = None) -> None:
        self._run(statement, params)

    def query(
        self, statement: str, params: Mapping[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        df = self._run(statement, params)
        return [row.asDict(recursive=True) for row in df.collect()]

    def insert_rows(self, table: str, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> None:
        """Append rows via the DataFrame API.

        Row-at-a-time INSERT for an audit stream would be one Delta commit per
        event; building a DataFrame keeps a batch of events to a single commit.
        """
        if not rows:
            return
        ordered = [tuple(row.get(col) for col in columns) for row in rows]
        df = self._spark.createDataFrame(ordered, schema=list(columns))
        df.write.format("delta").mode("append").saveAsTable(table)

    def _run(self, statement: str, params: Mapping[str, Any] | None):
        if params:
            return self._spark.sql(statement, args=dict(params))
        return self._spark.sql(statement)


class RecordingSqlClient:
    """Test double: records every call and replays queued query results.

    Not a SQL engine -- it verifies *what* the control plane asks for, which is
    what the control-plane tests are about. Behaviour that depends on real
    execution (MERGE semantics) is tested against Delta in the Spark suite.
    """

    def __init__(self, results: Iterable[list[dict[str, Any]]] | None = None) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []  # (kind, statement, params)
        self._results: list[list[dict[str, Any]]] = list(results or [])
        self.inserted: list[tuple[str, list[dict[str, Any]]]] = []

    # -- SqlClient ---------------------------------------------------------

    def execute(self, statement: str, params: Mapping[str, Any] | None = None) -> None:
        self.calls.append(("execute", statement, dict(params or {})))

    def query(
        self, statement: str, params: Mapping[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        self.calls.append(("query", statement, dict(params or {})))
        return self._results.pop(0) if self._results else []

    def insert_rows(self, table: str, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> None:
        self.calls.append(("insert", table, {"columns": list(columns), "count": len(rows)}))
        self.inserted.append((table, [dict(r) for r in rows]))

    # -- assertions helpers ------------------------------------------------

    def queue_result(self, rows: list[dict[str, Any]]) -> None:
        self._results.append(rows)

    @property
    def statements(self) -> list[str]:
        return [statement for kind, statement, _ in self.calls if kind != "insert"]

    def statements_matching(self, needle: str) -> list[str]:
        return [s for s in self.statements if needle.upper() in s.upper()]

    def params_for(self, needle: str) -> dict[str, Any]:
        for _, statement, params in self.calls:
            if needle.upper() in statement.upper():
                return params
        raise AssertionError(f"no statement matching {needle!r} in {self.statements}")
