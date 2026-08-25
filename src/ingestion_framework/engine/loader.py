"""Write a staged batch into the Bronze Delta table.

Bronze is a 1:1 current-state mirror of Oracle, so ``merge`` is the default
path (DESIGN 3.7). Three properties the MERGE must have, all of them built
here as SQL text so they can be asserted without a cluster:

* an explicit column list rather than ``UPDATE SET *``, so ``_first_ingested_at``
  survives an update and ``_source_op`` can flip to ``'U'``;
* a watermark guard, so a replayed or backfilled batch cannot overwrite a newer
  row with an older one;
* clustering on the merge keys rather than date partitioning, because merge keys
  scatter across business-date partitions and partitioning would defeat pruning.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ..control.schema import qualify
from .run_spec import RunSpec
from .sql_builder import SqlBuildError
from .transformer import INSERT_ONLY_AUDIT_COLUMNS, STAGED_VIEW, normalize_column

# Target-side identifiers follow Spark/Delta rules, not Oracle's. The audit
# columns all lead with an underscore, which the Oracle validator rejects --
# using that one here would fail every merge the framework builds.
_DELTA_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def delta_identifier(name: str) -> str:
    """Pass through a Spark/Delta identifier, refusing anything that is not one."""
    if not name or not _DELTA_IDENT.match(str(name)):
        raise SqlBuildError(f"{name!r} is not a valid Delta identifier")
    return str(name)

# Delta reports what a write actually did; these are the keys we care about.
_METRIC_KEYS = {
    "rows_inserted": ("numTargetRowsInserted",),
    "rows_updated": ("numTargetRowsUpdated",),
    "rows_deleted": ("numTargetRowsDeleted",),
    "rows_written": ("numOutputRows",),
}


class LoadError(RuntimeError):
    """Raised when a batch cannot be written as configured."""


@dataclass
class LoadResult:
    target: str
    write_mode: str
    rows_written: int | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None
    rows_deleted: int | None = None
    table_created: bool = False
    schema_evolved: bool = False
    statements: list[str] = field(default_factory=list)


# -- guard ------------------------------------------------------------------


def merge_guard_column(spec: RunSpec) -> str | None:
    """The column the update branch compares, or None for last-write-wins.

    Configured off, or no watermark to compare, means no guard: a full-load
    table has nothing to order two versions of a row by.
    """
    if spec.target.merge_guard != "watermark":
        return None
    watermark = spec.extraction.incremental.effective_watermark_column
    if not watermark:
        return None
    return normalize_column(watermark, spec.target.column_case)


# -- statements -------------------------------------------------------------


def build_merge_sql(
    spec: RunSpec,
    columns: Sequence[str],
    *,
    staged_view: str = STAGED_VIEW,
) -> str:
    """The MERGE that keeps Bronze a current-state mirror."""
    if not spec.target.merge_keys:
        raise SqlBuildError("write_mode 'merge' requires merge_keys")
    if not columns:
        raise SqlBuildError("cannot merge a batch with no columns")

    target = spec.target.fqn
    keys = [normalize_column(k, spec.target.column_case) for k in spec.target.merge_keys]
    column_set = {c.upper() for c in columns}
    missing = [k for k in keys if k.upper() not in column_set]
    if missing:
        raise SqlBuildError(
            f"merge key(s) {', '.join(missing)} are not in the staged columns"
        )

    on_clause = " AND ".join(f"t.{delta_identifier(k)} = s.{delta_identifier(k)}" for k in keys)

    # Keys never change (they are what matched), and _first_ingested_at is the
    # one column an update must preserve.
    key_set = {k.upper() for k in keys}
    updatable = [
        c
        for c in columns
        if c.upper() not in key_set and c not in INSERT_ONLY_AUDIT_COLUMNS
    ]
    set_parts = [
        f"t.{delta_identifier(c)} = s.{delta_identifier(c)}" for c in updatable if c != "_source_op"
    ]
    if "_source_op" in columns:
        # The staged row claims 'I'; this branch is by definition an update.
        set_parts.append("t._source_op = 'U'")

    insert_columns = ", ".join(delta_identifier(c) for c in columns)
    insert_values = ", ".join(f"s.{delta_identifier(c)}" for c in columns)

    guard = merge_guard_column(spec)
    matched = "WHEN MATCHED"
    if guard:
        if guard.upper() not in column_set:
            raise SqlBuildError(
                f"merge_guard column {guard!r} is not in the staged columns; "
                f"either project it or set target.merge_guard: none"
            )
        # NULL on the target side means 'never seen a watermark for this row',
        # which anything beats. Equal values still update, so a re-run refreshes
        # the audit columns without changing the data.
        matched = (
            f"WHEN MATCHED AND (t.{delta_identifier(guard)} IS NULL "
            f"OR s.{delta_identifier(guard)} >= t.{delta_identifier(guard)})"
        )

    return (
        f"MERGE INTO {target} AS t\n"
        f"USING {staged_view} AS s\n"
        f"ON {on_clause}\n"
        f"{matched} THEN UPDATE SET\n  " + ",\n  ".join(set_parts) + "\n"
        f"WHEN NOT MATCHED THEN INSERT (\n  {insert_columns}\n) VALUES (\n  {insert_values}\n)"
    )


def build_create_table_sql(
    spec: RunSpec, columns: Sequence[tuple[str, str]]
) -> str:
    """CREATE TABLE IF NOT EXISTS for the Bronze target, from the batch's schema."""
    if not columns:
        raise SqlBuildError("cannot create a table with no columns")
    target = qualify(spec.target.catalog, spec.target.schema, spec.target.table_name)
    body = ",\n  ".join(f"{delta_identifier(name)} {dtype}" for name, dtype in columns)
    parts = [f"CREATE TABLE IF NOT EXISTS {target} (", f"  {body}", ")", "USING DELTA"]

    if spec.table.description:
        parts.append(f"COMMENT '{spec.table.description.replace(chr(39), chr(39) * 2)}'")
    if spec.target.partition_by:
        parts.append(
            f"PARTITIONED BY ({', '.join(delta_identifier(c) for c in spec.target.partition_by)})"
        )
    elif spec.target.cluster_by:
        # Delta rejects both on one table; partitioning is the explicit opt-out.
        parts.append(
            f"CLUSTER BY ({', '.join(delta_identifier(c) for c in spec.target.cluster_by)})"
        )

    properties = dict(spec.target.table_properties)
    if spec.target.enable_change_data_feed:
        properties["delta.enableChangeDataFeed"] = "true"
    properties.setdefault("ingestion.source_table", spec.table.source_fqn)
    properties.setdefault("ingestion.managed_by", "ingestion-framework")
    rendered = ", ".join(f"'{k}' = '{v}'" for k, v in sorted(properties.items()))
    parts.append(f"TBLPROPERTIES ({rendered})")
    return "\n".join(parts)


def build_history_metrics_query(spec: RunSpec) -> str:
    """Read what the last write to this table actually did."""
    return (
        f"SELECT operationMetrics FROM (DESCRIBE HISTORY {spec.target.fqn}) "
        f"ORDER BY version DESC LIMIT 1"
    )


def parse_operation_metrics(metrics: Mapping[str, Any] | None) -> dict[str, int | None]:
    """Turn Delta's string-valued operationMetrics into typed counters."""
    out: dict[str, int | None] = {}
    metrics = metrics or {}
    for name, candidates in _METRIC_KEYS.items():
        value = None
        for key in candidates:
            if key in metrics and metrics[key] is not None:
                value = _to_int(metrics[key])
                break
        out[name] = value
    inserted, updated = out.get("rows_inserted"), out.get("rows_updated")
    if out.get("rows_written") is None and inserted is not None:
        out["rows_written"] = inserted + (updated or 0)
    return out


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# -- loader -----------------------------------------------------------------


class DeltaLoader:
    """Executes the write for one staged batch."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def table_exists(self, spec: RunSpec) -> bool:
        return bool(self._spark.catalog.tableExists(spec.target.fqn))

    def ensure_table(self, spec: RunSpec, dataframe: Any) -> tuple[bool, str | None]:
        """Create the target if absent. Returns (created, statement)."""
        if self.table_exists(spec):
            return False, None
        columns = [(f.name, f.dataType.simpleString().upper()) for f in dataframe.schema.fields]
        statement = build_create_table_sql(spec, columns)
        self._spark.sql(statement)
        return True, statement

    def write(
        self,
        dataframe: Any,
        spec: RunSpec,
        *,
        staged_view: str = STAGED_VIEW,
    ) -> LoadResult:
        result = LoadResult(target=spec.target.fqn, write_mode=spec.target.write_mode)

        created, create_statement = self.ensure_table(spec, dataframe)
        result.table_created = created
        if create_statement:
            result.statements.append(create_statement)

        mode = spec.target.write_mode
        if mode == "merge":
            self._merge(dataframe, spec, staged_view, result)
        elif mode == "append":
            self._write_df(dataframe, spec, "append", result)
        elif mode == "overwrite":
            self._write_df(dataframe, spec, "overwrite", result)
        else:
            raise LoadError(f"unsupported write_mode {mode!r}")

        self._collect_metrics(spec, result)
        return result

    # -- write paths -------------------------------------------------------

    def _merge(self, dataframe: Any, spec: RunSpec, staged_view: str, result: LoadResult) -> None:
        dataframe.createOrReplaceTempView(staged_view)
        if spec.target.schema_evolution:
            # Delta's MERGE only evolves the target schema when this is on; the
            # DataFrame writer's mergeSchema option does not apply to MERGE.
            self._spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            result.schema_evolved = True
        statement = build_merge_sql(spec, list(dataframe.columns), staged_view=staged_view)
        result.statements.append(statement)
        self._spark.sql(statement)

    def _write_df(self, dataframe: Any, spec: RunSpec, mode: str, result: LoadResult) -> None:
        writer = dataframe.write.format("delta").mode(mode)
        if spec.target.schema_evolution:
            option = "overwriteSchema" if mode == "overwrite" else "mergeSchema"
            writer = writer.option(option, "true")
            result.schema_evolved = True
        if spec.target.partition_by:
            writer = writer.partitionBy(list(spec.target.partition_by))
        writer.saveAsTable(spec.target.fqn)

    def _collect_metrics(self, spec: RunSpec, result: LoadResult) -> None:
        try:
            rows = self._spark.sql(build_history_metrics_query(spec)).collect()
        except Exception:  # metrics are diagnostics, never the reason a load fails
            return
        if not rows:
            return
        metrics = parse_operation_metrics(rows[0][0])
        result.rows_inserted = metrics.get("rows_inserted")
        result.rows_updated = metrics.get("rows_updated")
        result.rows_deleted = metrics.get("rows_deleted")
        result.rows_written = metrics.get("rows_written")
