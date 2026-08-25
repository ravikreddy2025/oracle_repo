"""Read from Oracle over JDBC, driven by a RunSpec.

The extractor owns three things the SQL builder deliberately does not: talking
to Oracle, assembling JDBC options, and deciding what the next watermark should
be. Everything it computes without a database is a pure helper, so the parts
that need a cluster stay thin.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from ..control.watermark import WatermarkWindow, canonicalize
from .run_spec import RunSpec
from .sql_builder import (
    SourceQuery,
    SqlBuildError,
    build_bounds_query,
    build_count_query,
    build_max_watermark_query,
    build_source_query,
    build_upper_bound_query,
)


class SecretProvider(Protocol):
    """Fetches a secret value. Backed by ``dbutils.secrets`` at runtime."""

    def get(self, scope: str, key: str) -> str: ...


class ExtractionError(RuntimeError):
    """Raised when the source cannot be read as configured."""


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str


@dataclass
class ExtractResult:
    """A materialised extract plus everything the control plane wants to record."""

    dataframe: Any
    query: SourceQuery
    lower_bound: str | None = None
    upper_bound: str | None = None
    new_watermark: str | None = None
    source_count: int | None = None
    partition_bounds: tuple[Any, Any] | None = None
    num_partitions: int = 1
    options: Mapping[str, Any] = field(default_factory=dict)

    @property
    def is_empty_batch(self) -> bool:
        """No new watermark means nothing moved -- the mark must hold."""
        return self.new_watermark is None


class OracleExtractor:
    """Builds and executes the source read for one table."""

    def __init__(
        self,
        spark: Any,
        *,
        secrets: SecretProvider | None = None,
        config_root: str | Path | None = None,
        reader: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> None:
        self._spark = spark
        self._secrets = secrets
        self._config_root = Path(config_root) if config_root else None
        # Injectable so tests can exercise option assembly without a JVM.
        self._reader = reader or self._default_reader

    # -- credentials -------------------------------------------------------

    def credentials(self, spec: RunSpec) -> Credentials:
        if not spec.source.secret_scope:
            raise ExtractionError(
                f"{spec.table_fqn}: source.secret_scope is not set; Oracle credentials "
                f"must come from a Databricks secret scope"
            )
        if self._secrets is None:
            raise ExtractionError("no secret provider configured")
        return Credentials(
            username=self._secrets.get(spec.source.secret_scope, spec.source.username_key),
            password=self._secrets.get(spec.source.secret_scope, spec.source.password_key),
        )

    # -- option assembly ---------------------------------------------------

    def jdbc_options(
        self,
        spec: RunSpec,
        query: SourceQuery,
        credentials: Credentials,
        *,
        partition_bounds: tuple[Any, Any] | None = None,
    ) -> dict[str, Any]:
        """Assemble Spark JDBC options for this read.

        Spark rejects ``query`` and ``partitionColumn`` together, so a parallel
        read has to pass the extract as a ``dbtable`` subquery instead. Getting
        this wrong surfaces as a confusing Spark error, so the choice is made
        here in one place.
        """
        source = spec.source
        options: dict[str, Any] = {
            "url": source.url,
            "driver": source.driver,
            "user": credentials.username,
            "password": credentials.password,
            "fetchsize": source.fetch_size,
        }
        if source.session_init_statement:
            options["sessionInitStatement"] = source.session_init_statement

        read = source.read
        use_parallel = read.is_parallel and partition_bounds is not None
        if use_parallel:
            lower, upper = partition_bounds
            if lower is None or upper is None:
                # An empty batch has no bounds to split on; one partition reads
                # nothing far more cheaply than N partitions reading nothing.
                options["query"] = query.sql
            else:
                options["dbtable"] = query.as_subquery()
                options["partitionColumn"] = read.partition_column
                options["lowerBound"] = lower
                options["upperBound"] = upper
                options["numPartitions"] = read.num_partitions
        else:
            options["query"] = query.sql

        options.update(source.options)
        return options

    # -- probes ------------------------------------------------------------

    def _scalar(self, sql: str, column: str, credentials: Credentials, spec: RunSpec) -> Any:
        options = {
            "url": spec.source.url,
            "driver": spec.source.driver,
            "user": credentials.username,
            "password": credentials.password,
            "query": sql,
        }
        if spec.source.session_init_statement:
            options["sessionInitStatement"] = spec.source.session_init_statement
        rows = self._reader(options).collect()
        if not rows:
            return None
        row = rows[0]
        value = row[column] if hasattr(row, "__getitem__") else getattr(row, column)
        return value

    def probe_upper_bound(self, spec: RunSpec, credentials: Credentials) -> str | None:
        """Pin this batch's cut-off using the source's own clock or SCN."""
        if not spec.extraction.tracks_watermark or not spec.extraction.incremental.use_upper_bound:
            return None
        value = self._scalar(build_upper_bound_query(spec), "UPPER_BOUND", credentials, spec)
        if value is None:
            return None
        wtype = "number" if spec.extraction.incremental.uses_scn else spec.extraction.incremental.watermark_type
        return canonicalize(value, wtype)

    def probe_partition_bounds(
        self, spec: RunSpec, query: SourceQuery, credentials: Credentials
    ) -> tuple[Any, Any] | None:
        """MIN/MAX of the partition column over the filtered extract."""
        read = spec.source.read
        if not read.is_parallel:
            return None
        if read.bounds_strategy == "explicit":
            if read.lower_bound is None or read.upper_bound is None:
                raise ExtractionError(
                    f"{spec.table_fqn}: bounds_strategy is 'explicit' but lower_bound/"
                    f"upper_bound are not both set"
                )
            return (read.lower_bound, read.upper_bound)
        rows = self._reader(
            {
                "url": spec.source.url,
                "driver": spec.source.driver,
                "user": credentials.username,
                "password": credentials.password,
                "query": build_bounds_query(spec, query),
            }
        ).collect()
        if not rows:
            return None
        row = rows[0]
        return (row["LOWER_BOUND"], row["UPPER_BOUND"])

    def count_source(self, spec: RunSpec, query: SourceQuery, credentials: Credentials) -> int | None:
        """Count rows at source for reconciliation, if the table asks for it."""
        if not spec.quality.row_count_reconciliation:
            return None
        value = self._scalar(build_count_query(query), "SOURCE_COUNT", credentials, spec)
        return int(value) if value is not None else None

    def probe_max_watermark(
        self, spec: RunSpec, query: SourceQuery, credentials: Credentials
    ) -> str | None:
        """The highest watermark present in this batch."""
        if not spec.extraction.tracks_watermark:
            return None
        value = self._scalar(build_max_watermark_query(spec, query), "MAX_WATERMARK", credentials, spec)
        if value is None:
            return None
        wtype = "number" if spec.extraction.incremental.uses_scn else spec.extraction.incremental.watermark_type
        return canonicalize(value, wtype)

    # -- the read ----------------------------------------------------------

    def build_query(
        self, spec: RunSpec, window: WatermarkWindow, upper_bound: str | None = None
    ) -> SourceQuery:
        custom_sql = None
        if spec.extraction.mode == "query":
            custom_sql = self._load_query_file(spec)
        return build_source_query(
            spec,
            lower_bound=window.lower_bound if spec.extraction.tracks_watermark else None,
            upper_bound=upper_bound,
            custom_sql=custom_sql,
        )

    def extract(self, spec: RunSpec, window: WatermarkWindow) -> ExtractResult:
        """Run the full extract: pin the bound, build the query, read, measure."""
        credentials = self.credentials(spec)

        upper_bound = self.probe_upper_bound(spec, credentials)
        query = self.build_query(spec, window, upper_bound)

        partition_bounds = self.probe_partition_bounds(spec, query, credentials)
        options = self.jdbc_options(spec, query, credentials, partition_bounds=partition_bounds)
        dataframe = self._reader(options)

        source_count = self.count_source(spec, query, credentials)

        # Prefer the pinned upper bound: it is the exact cut this batch was
        # read at, so the next run resumes exactly where this one stopped. Fall
        # back to what the data actually contained.
        new_watermark = upper_bound or self.probe_max_watermark(spec, query, credentials)

        return ExtractResult(
            dataframe=dataframe,
            query=query,
            lower_bound=window.lower_bound,
            upper_bound=upper_bound,
            new_watermark=new_watermark,
            source_count=source_count,
            partition_bounds=partition_bounds,
            num_partitions=spec.source.read.num_partitions if partition_bounds else 1,
            options=_redact(options),
        )

    # -- internals ---------------------------------------------------------

    def _load_query_file(self, spec: RunSpec) -> str:
        query_file = spec.extraction.query_file
        if not query_file:
            raise SqlBuildError(f"{spec.table_fqn}: mode 'query' requires extraction.query_file")
        path = Path(query_file)
        if not path.is_absolute() and self._config_root:
            path = self._config_root / query_file
        if not path.is_file():
            raise ExtractionError(f"{spec.table_fqn}: query file not found: {path}")
        return path.read_text(encoding="utf-8")

    def _default_reader(self, options: Mapping[str, Any]):
        reader = self._spark.read.format("jdbc")
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load()


def _redact(options: Mapping[str, Any]) -> dict[str, Any]:
    """Options are logged and stored; the password must never travel with them."""
    return {k: ("***" if k.lower() in {"password", "user"} else v) for k, v in options.items()}
