"""Shape an extracted batch into exactly what the target table expects.

The transform is expressed as **one SQL statement** over a temp view rather
than a chain of DataFrame calls. Two reasons:

* the statement is a pure function of the spec, so every transform decision --
  including the dedupe the merge depends on -- is provable by string assertion
  without a cluster;
* it is one plan, so Spark optimises the projection, the audit columns, and the
  window together instead of stacking projections.

The dedupe is not an optimisation. An incremental window with ``overlap``
routinely returns several versions of the same key, and Delta raises
``UnsupportedOperationException`` when multiple source rows match one target
row. Reducing to latest-per-key before the MERGE is what makes the load work at
all (DESIGN 3.7).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence

from .run_spec import RunSpec
from .sql_builder import SCN_COLUMN, SqlBuildError

RAW_VIEW = "_ingest_raw"
STAGED_VIEW = "_ingest_staged"
ROW_NUMBER_COLUMN = "_rn"


@dataclass(frozen=True)
class ViewNames:
    """Per-task temp view names.

    They must be unique per task: the batch runner can run several tables
    concurrently on one Spark session, and fixed names would have one table's
    staged batch silently replace another's.
    """

    raw: str
    staged: str


def view_names(table_fqn: str, run_id: str = "", attempt: int = 1) -> ViewNames:
    suffix = re.sub(r"[^A-Za-z0-9]", "_", f"{table_fqn}_{run_id}_{attempt}").strip("_")
    return ViewNames(raw=f"{RAW_VIEW}_{suffix}", staged=f"{STAGED_VIEW}_{suffix}")

# Stamped on every row. Order matters: it is the order they appear in the
# staged projection and therefore in the created table.
AUDIT_COLUMNS: tuple[str, ...] = (
    "_ingested_at",
    "_ingested_date",
    "_run_id",
    "_batch_id",
    "_source_op",
    "_first_ingested_at",
)

# Set on insert only; the MERGE update clause must never touch it.
INSERT_ONLY_AUDIT_COLUMNS: frozenset[str] = frozenset({"_first_ingested_at"})

_SAFE_LITERAL = re.compile(r"^[A-Za-z0-9_.:+\-]*$")


@dataclass(frozen=True)
class StageQuery:
    """The staging statement plus what it did, for logging and metrics."""

    sql: str
    columns: tuple[str, ...]
    data_columns: tuple[str, ...]
    dedupe_applied: bool
    dedupe_keys: tuple[str, ...] = ()
    order_by: str | None = None

    @property
    def audit_columns(self) -> tuple[str, ...]:
        return tuple(c for c in self.columns if c in AUDIT_COLUMNS)


def normalize_column(name: str, column_case: str) -> str:
    """Apply the table's casing policy to one column name."""
    if column_case == "lower":
        return name.lower()
    return name


def normalize_columns(names: Sequence[str], column_case: str) -> tuple[str, ...]:
    return tuple(normalize_column(n, column_case) for n in names)


def source_column_name(name: str) -> str:
    """The name a column has coming out of the JDBC read.

    ``ORA_ROWSCN AS ORA_ROWSCN`` arrives as ``ORA_ROWSCN``; everything else
    arrives as Oracle spelled it.
    """
    return name.split(" AS ")[-1].strip().strip('"')


def _literal(value: Any) -> str:
    """Render a framework-generated value as a Spark SQL literal.

    Only framework-generated values pass through here (run ids, batch ids,
    timestamps), never source data -- but the charset is asserted anyway
    rather than assumed.
    """
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    if isinstance(value, date):
        return f"DATE '{value.strftime('%Y-%m-%d')}'"
    text = str(value)
    if not _SAFE_LITERAL.match(text):
        raise SqlBuildError(
            f"{value!r} is not a safe framework literal (expected letters, digits, "
            f"'_', '-', '.', ':', '+')"
        )
    return f"'{text}'"


def dedupe_order_expression(spec: RunSpec, available: Sequence[str]) -> str:
    """How to pick the surviving row when a key appears more than once.

    Newest wins, by whatever the table's notion of 'newest' is. With no
    watermark there is no principled ordering, so an arbitrary-but-deterministic
    one is used and the caller is expected to surface the duplicate count --
    silently picking a random row from a table that should have unique keys is
    a data-quality finding, not a detail.
    """
    incremental = spec.extraction.incremental
    upper = {c.upper() for c in available}
    order_parts: list[str] = []

    watermark = incremental.effective_watermark_column
    if watermark and watermark.upper() in upper:
        order_parts.append(f"{watermark} DESC NULLS LAST")
    if SCN_COLUMN in upper and (not watermark or watermark.upper() != SCN_COLUMN):
        order_parts.append(f"{SCN_COLUMN} DESC NULLS LAST")

    if not order_parts:
        order_parts.append("monotonically_increasing_id() DESC")
    return ", ".join(order_parts)


def needs_dedupe(spec: RunSpec) -> bool:
    """Only a MERGE can fail on duplicate keys, so only a MERGE pays for dedupe."""
    return spec.target.is_merge and bool(spec.target.merge_keys)


def build_stage_query(
    spec: RunSpec,
    *,
    source_columns: Sequence[str],
    run_id: str,
    batch_id: str,
    ingested_at: datetime,
    raw_view: str = RAW_VIEW,
) -> StageQuery:
    """Build the single statement that turns the raw extract into staged rows."""
    if not source_columns:
        raise SqlBuildError("cannot stage a batch with no columns")

    case = spec.target.column_case
    incoming = [source_column_name(c) for c in source_columns]
    data_columns = normalize_columns(incoming, case)

    projection: list[str] = []
    for raw, final in zip(incoming, data_columns):
        projection.append(f"{raw}" if raw == final else f"{raw} AS {final}")

    if spec.target.add_audit_columns:
        ingested_date = ingested_at.date()
        projection.extend(
            [
                f"{_literal(ingested_at)} AS _ingested_at",
                f"{_literal(ingested_date)} AS _ingested_date",
                f"{_literal(run_id)} AS _run_id",
                f"{_literal(batch_id)} AS _batch_id",
                # Every staged row claims 'I'; the MERGE's update branch
                # rewrites the ones that turn out to be updates.
                "'I' AS _source_op",
                f"{_literal(ingested_at)} AS _first_ingested_at",
            ]
        )

    columns = tuple(data_columns) + (AUDIT_COLUMNS if spec.target.add_audit_columns else ())
    select_list = ",\n  ".join(projection)

    if not needs_dedupe(spec):
        return StageQuery(
            sql=f"SELECT\n  {select_list}\nFROM {raw_view}",
            columns=columns,
            data_columns=tuple(data_columns),
            dedupe_applied=False,
        )

    keys = normalize_columns(spec.target.merge_keys, case)
    missing = [k for k in keys if k.upper() not in {c.upper() for c in data_columns}]
    if missing:
        raise SqlBuildError(
            f"merge key(s) {', '.join(missing)} are not in the extracted columns "
            f"({', '.join(data_columns)})"
        )
    order_by = dedupe_order_expression(spec, incoming)
    partition_by = ", ".join(spec.target.merge_keys)

    sql = (
        f"SELECT\n  {select_list}\n"
        f"FROM (\n"
        f"  SELECT *, ROW_NUMBER() OVER (\n"
        f"    PARTITION BY {partition_by}\n"
        f"    ORDER BY {order_by}\n"
        f"  ) AS {ROW_NUMBER_COLUMN}\n"
        f"  FROM {raw_view}\n"
        f")\n"
        f"WHERE {ROW_NUMBER_COLUMN} = 1"
    )
    return StageQuery(
        sql=sql,
        columns=columns,
        data_columns=tuple(data_columns),
        dedupe_applied=True,
        dedupe_keys=keys,
        order_by=order_by,
    )


def build_duplicate_count_query(spec: RunSpec, raw_view: str = RAW_VIEW) -> str:
    """Count how many rows the dedupe will discard.

    A non-zero count on a merge table is expected for an overlap window and
    suspicious for a full load, so it is recorded rather than swallowed.
    """
    if not spec.target.merge_keys:
        raise SqlBuildError("cannot count duplicates without merge keys")
    keys = ", ".join(spec.target.merge_keys)
    return (
        f"SELECT COUNT(*) - COUNT(DISTINCT {keys}) AS DUPLICATE_ROWS FROM {raw_view}"
    )


def build_null_key_query(spec: RunSpec, raw_view: str = RAW_VIEW) -> str:
    """Count rows whose merge key is NULL -- they can never match on merge."""
    if not spec.target.merge_keys:
        raise SqlBuildError("cannot check null keys without merge keys")
    predicate = " OR ".join(f"{k} IS NULL" for k in spec.target.merge_keys)
    return f"SELECT COUNT(*) AS NULL_KEY_ROWS FROM {raw_view} WHERE {predicate}"


@dataclass
class StageResult:
    dataframe: Any
    query: StageQuery
    rows_staged: int | None = None
    duplicates_removed: int | None = None
    null_key_rows: int | None = None
    views: ViewNames | None = None


class Transformer:
    """Applies the staging statement to an extracted DataFrame."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def stage(
        self,
        dataframe: Any,
        spec: RunSpec,
        *,
        run_id: str,
        batch_id: str,
        ingested_at: datetime,
        count_rows: bool = False,
        views: ViewNames | None = None,
    ) -> StageResult:
        views = views or view_names(spec.table_fqn, run_id)
        dataframe.createOrReplaceTempView(views.raw)
        query = build_stage_query(
            spec,
            source_columns=list(dataframe.columns),
            run_id=run_id,
            batch_id=batch_id,
            ingested_at=ingested_at,
            raw_view=views.raw,
        )

        duplicates = None
        null_keys = None
        if spec.target.merge_keys:
            if query.dedupe_applied:
                duplicates = int(
                    self._spark.sql(build_duplicate_count_query(spec, views.raw)).collect()[0][0]
                )
            if spec.quality.null_check_keys:
                null_keys = int(
                    self._spark.sql(build_null_key_query(spec, views.raw)).collect()[0][0]
                )

        staged = self._spark.sql(query.sql)
        staged.createOrReplaceTempView(views.staged)

        return StageResult(
            dataframe=staged,
            query=query,
            views=views,
            rows_staged=staged.count() if count_rows else None,
            duplicates_removed=duplicates,
            null_key_rows=null_keys,
        )
