"""Build the Oracle SQL each extraction mode needs.

Everything here is a pure function of a :class:`RunSpec` plus the run's
watermark window, so every mode is provable by string assertion without a
cluster or a database.

**Why values are rendered as literals here, when the control plane binds them.**
Spark's JDBC source takes the source query as an opaque string -- there is no
parameter binding on that path. So bounds must be rendered into the SQL. That
is safe only because of what reaches this module: watermark values are
canonicalised and type-checked by ``control.watermark`` before they get here,
and ``config.validator`` rejects any filter containing a statement terminator
or comment marker. :func:`literal` re-checks each value against its declared
type as the last line of defence -- if a value ever fails that check, the
extract fails rather than guessing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..control.watermark import DATE_FORMAT, TIMESTAMP_FORMAT
from .run_spec import RunSpec

SCN_COLUMN = "ORA_ROWSCN"
SUBQUERY_ALIAS = "src"

_ORACLE_IDENT = re.compile(r'^(?:[A-Za-z][A-Za-z0-9_$#]*|"[^"]+")$')
_CANONICAL_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$")
_CANONICAL_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

ORACLE_TIMESTAMP_FMT = "YYYY-MM-DD HH24:MI:SS.FF6"
ORACLE_DATE_FMT = "YYYY-MM-DD"


class SqlBuildError(ValueError):
    """Raised when a query cannot be built safely from the given spec."""


@dataclass(frozen=True)
class SourceQuery:
    """A built source query plus the facts about it worth logging."""

    sql: str
    mode: str
    lower_bound: str | None = None
    upper_bound: str | None = None
    lower_inclusive: bool = True
    watermark_column: str | None = None
    projected_columns: tuple[str, ...] = ()

    def as_subquery(self, alias: str = SUBQUERY_ALIAS) -> str:
        """Wrap for Spark's ``dbtable`` option, which requires a table expression."""
        return f"({self.sql}) {alias}"

    @property
    def is_bounded(self) -> bool:
        return self.lower_bound is not None or self.upper_bound is not None


# -- literals ---------------------------------------------------------------


def literal(value: Any, watermark_type: str) -> str:
    """Render a canonical watermark value as an Oracle literal expression.

    Rejects anything that is not exactly the canonical form for its type. The
    value cannot be bound (see module docstring), so it must be provably safe.
    """
    if value is None:
        raise SqlBuildError("cannot render a NULL bound as a literal")
    text = str(value)
    wtype = watermark_type.lower()

    if wtype == "number":
        try:
            return str(Decimal(text))
        except InvalidOperation as exc:
            raise SqlBuildError(
                f"{value!r} is not a valid numeric bound; refusing to build SQL"
            ) from exc

    if wtype == "date":
        if not _CANONICAL_DATE.match(text):
            raise SqlBuildError(
                f"{value!r} is not a canonical date ({DATE_FORMAT}); refusing to build SQL"
            )
        return f"TO_DATE('{text}', '{ORACLE_DATE_FMT}')"

    if wtype == "timestamp":
        if not _CANONICAL_TIMESTAMP.match(text):
            raise SqlBuildError(
                f"{value!r} is not a canonical timestamp ({TIMESTAMP_FORMAT}); "
                f"refusing to build SQL"
            )
        if "." not in text:
            text += ".000000"
        return f"TO_TIMESTAMP('{text}', '{ORACLE_TIMESTAMP_FMT}')"

    raise SqlBuildError(f"unknown watermark_type {watermark_type!r}")


def identifier(name: str) -> str:
    """Pass through an Oracle identifier, refusing anything that is not one."""
    if not name or not _ORACLE_IDENT.match(str(name)):
        raise SqlBuildError(f"{name!r} is not a valid Oracle identifier")
    return str(name)


# -- bound semantics --------------------------------------------------------


def lower_bound_is_inclusive(spec: RunSpec) -> bool:
    """Decide whether the incremental lower bound is ``>=`` or ``>``.

    Neither choice is free:

    * ``>`` (exclusive) can silently lose rows that share the boundary value --
      a row committed after our read but stamped with the same second is never
      seen again.
    * ``>=`` (inclusive) re-reads the boundary rows on every run. Harmless for a
      merge target, which is idempotent; duplicates for an append target.

    ``auto`` therefore picks inclusive for merge/overwrite targets and exclusive
    for append. An explicit ``bound_inclusive`` overrides the inference.
    """
    configured = spec.extraction.incremental.bound_inclusive
    if configured is True or configured is False:
        return configured
    return spec.target.write_mode != "append"


# -- projection -------------------------------------------------------------


def build_projection(spec: RunSpec) -> tuple[str, tuple[str, ...]]:
    """Return the SELECT list and the columns it projects."""
    extraction = spec.extraction
    columns: list[str]
    if extraction.selects_all_columns:
        columns = ["*"]
    else:
        columns = [identifier(c) for c in extraction.column_list]
        excluded = {c.upper() for c in extraction.exclude_columns}
        if excluded:
            columns = [c for c in columns if c.upper() not in excluded]
            if not columns:
                raise SqlBuildError(
                    "exclude_columns removed every projected column; nothing left to select"
                )

    # An SCN-based extract needs the pseudo-column in the projection: it is both
    # the predicate and the source of the next watermark, and it is not part of
    # SELECT * .
    if extraction.is_incremental and extraction.incremental.uses_scn:
        columns.append(f"{SCN_COLUMN} AS {SCN_COLUMN}")

    return ", ".join(columns), tuple(columns)


# -- predicates -------------------------------------------------------------


def build_predicates(
    spec: RunSpec,
    *,
    lower_bound: str | None = None,
    upper_bound: str | None = None,
) -> list[str]:
    """Every WHERE fragment for this extract, in a stable order."""
    predicates: list[str] = []
    extraction = spec.extraction
    incremental = extraction.incremental

    if extraction.is_incremental:
        column = incremental.effective_watermark_column
        if not column:
            raise SqlBuildError(
                "incremental extraction needs a watermark column; config validation "
                "should have caught this"
            )
        column = identifier(column)
        wtype = "number" if incremental.uses_scn else incremental.watermark_type

        if lower_bound is not None:
            operator = ">=" if lower_bound_is_inclusive(spec) else ">"
            predicates.append(f"{column} {operator} {literal(lower_bound, wtype)}")
        if upper_bound is not None:
            # Strictly less-than: the upper bound becomes the next run's lower
            # bound, so an inclusive upper would double-count the boundary.
            predicates.append(f"{column} < {literal(upper_bound, wtype)}")

    if extraction.filter:
        predicates.append(f"({extraction.filter})")

    return predicates


# -- queries ----------------------------------------------------------------


def build_source_query(
    spec: RunSpec,
    *,
    lower_bound: str | None = None,
    upper_bound: str | None = None,
    custom_sql: str | None = None,
) -> SourceQuery:
    """Build the extract query for any mode.

    ``custom_sql`` carries the contents of ``extraction.query_file`` for
    ``mode: query``; the caller reads the file so this module stays pure.
    """
    extraction = spec.extraction

    if extraction.mode == "query":
        if not custom_sql or not custom_sql.strip():
            raise SqlBuildError(
                f"mode 'query' requires the contents of {extraction.query_file!r}"
            )
        sql = render_query_template(
            custom_sql, spec, lower_bound=lower_bound, upper_bound=upper_bound
        )
        return SourceQuery(
            sql=sql,
            mode="query",
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            lower_inclusive=lower_bound_is_inclusive(spec),
            watermark_column=extraction.incremental.effective_watermark_column,
        )

    projection, columns = build_projection(spec)
    source = f"{identifier(spec.table.source_schema)}.{identifier(spec.table.source_object)}"
    parts = [f"SELECT {projection}", f"FROM {source}"]

    predicates = build_predicates(spec, lower_bound=lower_bound, upper_bound=upper_bound)
    if predicates:
        parts.append("WHERE " + "\n  AND ".join(predicates))

    if extraction.row_limit:
        # 12c+ syntax; the framework targets 19c and above.
        parts.append(f"FETCH FIRST {int(extraction.row_limit)} ROWS ONLY")

    return SourceQuery(
        sql="\n".join(parts),
        mode=extraction.mode,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        lower_inclusive=lower_bound_is_inclusive(spec),
        watermark_column=extraction.incremental.effective_watermark_column
        if extraction.is_incremental
        else None,
        projected_columns=columns,
    )


def render_query_template(
    template: str,
    spec: RunSpec,
    *,
    lower_bound: str | None = None,
    upper_bound: str | None = None,
) -> str:
    """Substitute the framework's bind-style placeholders in a custom query file.

    Supported placeholders: ``:lower_bound``, ``:upper_bound``, ``:table_fqn``,
    ``:env``. An unresolved placeholder is an error -- a query that silently
    ships with a literal ``:lower_bound`` in it would read the whole table.
    """
    incremental = spec.extraction.incremental
    wtype = "number" if incremental.uses_scn else incremental.watermark_type

    replacements: dict[str, str] = {
        ":env": f"'{spec.env}'",
        ":table_fqn": f"'{spec.table_fqn}'",
    }
    if lower_bound is not None:
        replacements[":lower_bound"] = literal(lower_bound, wtype)
    if upper_bound is not None:
        replacements[":upper_bound"] = literal(upper_bound, wtype)

    rendered = template.strip().rstrip(";")
    for placeholder, value in replacements.items():
        rendered = re.sub(rf"{placeholder}\b", value, rendered)

    leftovers = sorted(set(re.findall(r":(lower_bound|upper_bound|table_fqn|env)\b", rendered)))
    if leftovers:
        raise SqlBuildError(
            f"query template still contains unresolved placeholder(s): "
            f"{', '.join(':' + p for p in leftovers)} -- no value was available for this run"
        )
    return rendered


def build_count_query(query: SourceQuery, alias: str = SUBQUERY_ALIAS) -> str:
    """Count rows the extract *would* read, for source-vs-target reconciliation."""
    return f"SELECT COUNT(*) AS SOURCE_COUNT FROM ({query.sql}) {alias}"


def build_bounds_query(spec: RunSpec, query: SourceQuery, alias: str = SUBQUERY_ALIAS) -> str:
    """Probe MIN/MAX of the partition column, for bounded parallel JDBC reads.

    The bounds are probed over the *filtered* query, not the whole table: using
    whole-table bounds against a filtered read leaves most partitions empty and
    the work lands on one executor.
    """
    column = spec.source.read.partition_column
    if not column:
        raise SqlBuildError("cannot probe bounds without source.read.partition_column")
    column = identifier(column)
    return (
        f"SELECT MIN({column}) AS LOWER_BOUND, MAX({column}) AS UPPER_BOUND "
        f"FROM ({query.sql}) {alias}"
    )


def build_upper_bound_query(spec: RunSpec) -> str:
    """Ask the *source* for the cut-off point of this batch.

    Two reasons this comes from Oracle rather than the driver's clock:

    * with parallel reads, Spark issues one query per partition at slightly
      different times -- without a pinned upper bound the partitions see
      different data, and rows can be double-counted or missed;
    * the driver's clock and the database's clock are not the same clock, and
      a watermark derived from the wrong one drifts.
    """
    incremental = spec.extraction.incremental
    if incremental.uses_scn:
        return "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER AS UPPER_BOUND FROM DUAL"
    if incremental.watermark_type == "date":
        return "SELECT TRUNC(SYSDATE) AS UPPER_BOUND FROM DUAL"
    return "SELECT SYSTIMESTAMP AS UPPER_BOUND FROM DUAL"


def build_max_watermark_query(spec: RunSpec, query: SourceQuery, alias: str = SUBQUERY_ALIAS) -> str:
    """The highest watermark actually present in this batch.

    Used when no upper bound was pinned: the new mark is what the data showed,
    never a clock reading, so a row that arrives late cannot be skipped over.
    """
    column = query.watermark_column
    if not column:
        raise SqlBuildError("cannot compute a max watermark for a non-incremental extract")
    return f"SELECT MAX({identifier(column)}) AS MAX_WATERMARK FROM ({query.sql}) {alias}"
