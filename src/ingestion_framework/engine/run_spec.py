"""Typed, immutable view over an effective config.

Everything downstream (extractor, loader, control plane, observability) reads
a ``RunSpec`` rather than raw dicts, so a config key is named in exactly one
place and a typo becomes an AttributeError at import time instead of a wrong
query at 3am.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Mapping, Sequence

from ..utils.duration import parse_duration


@dataclass(frozen=True)
class TableIdentity:
    domain: str
    name: str
    source_schema: str
    source_object: str
    business_key: tuple[str, ...] = ()
    description: str | None = None
    owner: str | None = None
    depends_on: tuple[str, ...] = ()

    @property
    def fqn(self) -> str:
        """``domain.table`` -- the framework's handle for this table."""
        return f"{self.domain}.{self.name}"

    @property
    def source_fqn(self) -> str:
        """``SCHEMA.OBJECT`` as Oracle knows it."""
        return f"{self.source_schema}.{self.source_object}"


@dataclass(frozen=True)
class ReadSpec:
    num_partitions: int = 1
    partition_column: str | None = None
    lower_bound: Any = None
    upper_bound: Any = None
    bounds_strategy: str = "auto"

    @property
    def is_parallel(self) -> bool:
        return self.num_partitions > 1 and bool(self.partition_column)


@dataclass(frozen=True)
class SourceSpec:
    type: str = "oracle"
    fetch_size: int = 10_000
    driver: str = "oracle.jdbc.OracleDriver"
    url: str | None = None
    session_init_statement: str | None = None
    options: Mapping[str, Any] = field(default_factory=dict)
    secret_scope: str | None = None
    secret_keys: Mapping[str, str] = field(default_factory=dict)
    custom_schema: Mapping[str, str] = field(default_factory=dict)
    read: ReadSpec = field(default_factory=ReadSpec)

    @property
    def username_key(self) -> str:
        return self.secret_keys.get("username", "username")

    @property
    def password_key(self) -> str:
        return self.secret_keys.get("password", "password")


@dataclass(frozen=True)
class IncrementalSpec:
    strategy: str = "watermark"
    watermark_column: str | None = None
    watermark_type: str = "timestamp"
    overlap: str = "PT0S"
    lower_bound_default: Any = None
    bound_inclusive: Any = "auto"   # "auto" | True | False -- see sql_builder
    use_upper_bound: bool = True

    @property
    def overlap_delta(self) -> timedelta:
        return parse_duration(self.overlap)

    @property
    def uses_scn(self) -> bool:
        return self.strategy == "scn"

    @property
    def effective_watermark_column(self) -> str | None:
        """The column the incremental predicate compares against."""
        return "ORA_ROWSCN" if self.uses_scn else self.watermark_column


@dataclass(frozen=True)
class ExtractionSpec:
    mode: str = "full"
    columns: tuple[str, ...] | str = "*"
    exclude_columns: tuple[str, ...] = ()
    filter: str | None = None
    query_file: str | None = None
    row_limit: int | None = None
    incremental: IncrementalSpec = field(default_factory=IncrementalSpec)

    @property
    def is_incremental(self) -> bool:
        return self.mode == "incremental"

    @property
    def tracks_watermark(self) -> bool:
        """Whether this extract carries watermark bounds at all.

        A custom query that references :lower_bound is incremental in every
        sense that matters, even though the framework does not build its
        predicate -- it still needs bounds supplied and a watermark advanced.
        """
        if self.is_incremental:
            return True
        return self.mode == "query" and bool(self.incremental.effective_watermark_column)

    @property
    def selects_all_columns(self) -> bool:
        return self.columns == "*"

    @property
    def column_list(self) -> tuple[str, ...]:
        return () if self.selects_all_columns else tuple(self.columns)


@dataclass(frozen=True)
class TargetSpec:
    catalog: str
    schema: str
    table_name: str
    layer: str = "bronze"
    format: str = "delta"
    write_mode: str = "merge"
    merge_keys: tuple[str, ...] = ()
    merge_guard: str = "watermark"
    enable_change_data_feed: bool = False
    partition_by: tuple[str, ...] = ()
    cluster_by: tuple[str, ...] = ()
    add_audit_columns: bool = True
    schema_evolution: bool = True
    column_case: str = "preserve"
    table_properties: Mapping[str, str] = field(default_factory=dict)

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_name}"

    @property
    def is_merge(self) -> bool:
        return self.write_mode == "merge"


@dataclass(frozen=True)
class ControlSpec:
    catalog: str
    schema: str

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    @property
    def watermarks(self) -> str:
        return self.table("watermarks")

    @property
    def runs(self) -> str:
        return self.table("ingestion_runs")

    @property
    def tasks(self) -> str:
        return self.table("ingestion_tasks")

    @property
    def audit_log(self) -> str:
        return self.table("audit_log")

    @property
    def config_registry(self) -> str:
        return self.table("config_registry")

    @property
    def reconciliation(self) -> str:
        return self.table("reconciliation")


@dataclass(frozen=True)
class Expectation:
    column: str
    rule: str
    values: tuple[Any, ...] = ()
    value: Any = None
    pattern: str | None = None
    action: str = "fail"


@dataclass(frozen=True)
class QualitySpec:
    row_count_reconciliation: bool = True
    fail_on_schema_drift: bool = False
    null_check_keys: bool = True
    expectations: tuple[Expectation, ...] = ()


@dataclass(frozen=True)
class RuntimeSpec:
    retries: int = 2
    retry_backoff_seconds: int = 30
    timeout_minutes: int = 120
    enabled: bool = True


@dataclass(frozen=True)
class AlertingSpec:
    on_failure: tuple[str, ...] = ()
    on_reconciliation_mismatch: tuple[str, ...] = ()
    on_freshness_breach: tuple[str, ...] = ()
    freshness_sla_hours: float | None = None


@dataclass(frozen=True)
class ScheduleSpec:
    group: str = "default"
    cron: str | None = None
    timezone: str = "UTC"
    job_retries: int = 0


@dataclass(frozen=True)
class RunSpec:
    """The complete, validated instruction for ingesting one table once."""

    env: str
    table: TableIdentity
    source: SourceSpec
    extraction: ExtractionSpec
    target: TargetSpec
    control: ControlSpec
    quality: QualitySpec = field(default_factory=QualitySpec)
    runtime: RuntimeSpec = field(default_factory=RuntimeSpec)
    alerting: AlertingSpec = field(default_factory=AlertingSpec)
    schedule: ScheduleSpec = field(default_factory=ScheduleSpec)
    log_level: str = "INFO"
    config_hash: str = ""

    @property
    def table_fqn(self) -> str:
        return self.table.fqn

    @classmethod
    def from_config(cls, cfg: Mapping[str, Any], *, config_hash: str = "") -> "RunSpec":
        """Build a RunSpec from an effective config. Assumes it has passed validation."""
        table_cfg = cfg.get("table", {})
        source_cfg = cfg.get("source", {})
        jdbc_cfg = source_cfg.get("jdbc") or {}
        read_cfg = source_cfg.get("read") or {}
        extraction_cfg = cfg.get("extraction", {})
        incremental_cfg = extraction_cfg.get("incremental") or {}
        target_cfg = cfg.get("target", {})
        control_cfg = cfg.get("control", {})
        quality_cfg = cfg.get("quality") or {}
        runtime_cfg = cfg.get("runtime") or {}
        alerting_cfg = cfg.get("alerting") or {}
        schedule_cfg = cfg.get("schedule") or {}

        columns = extraction_cfg.get("columns", "*")

        return cls(
            env=cfg.get("env", ""),
            table=TableIdentity(
                domain=table_cfg["domain"],
                name=table_cfg["name"],
                source_schema=table_cfg["source_schema"],
                source_object=table_cfg["source_object"],
                business_key=_tuple(table_cfg.get("business_key")),
                description=table_cfg.get("description"),
                owner=table_cfg.get("owner"),
                depends_on=_tuple(table_cfg.get("depends_on")),
            ),
            source=SourceSpec(
                type=source_cfg.get("type", "oracle"),
                fetch_size=source_cfg.get("fetch_size", 10_000),
                driver=jdbc_cfg.get("driver", "oracle.jdbc.OracleDriver"),
                url=jdbc_cfg.get("url"),
                session_init_statement=jdbc_cfg.get("session_init_statement"),
                options=dict(jdbc_cfg.get("options") or {}),
                secret_scope=source_cfg.get("secret_scope"),
                secret_keys=dict(source_cfg.get("secret_keys") or {}),
                custom_schema=dict(source_cfg.get("custom_schema") or {}),
                read=ReadSpec(
                    num_partitions=read_cfg.get("num_partitions", 1),
                    partition_column=read_cfg.get("partition_column"),
                    lower_bound=read_cfg.get("lower_bound"),
                    upper_bound=read_cfg.get("upper_bound"),
                    bounds_strategy=read_cfg.get("bounds_strategy", "auto"),
                ),
            ),
            extraction=ExtractionSpec(
                mode=extraction_cfg.get("mode", "full"),
                columns="*" if columns == "*" else tuple(columns),
                exclude_columns=_tuple(extraction_cfg.get("exclude_columns")),
                filter=extraction_cfg.get("filter"),
                query_file=extraction_cfg.get("query_file"),
                row_limit=extraction_cfg.get("row_limit"),
                incremental=IncrementalSpec(
                    strategy=incremental_cfg.get("strategy", "watermark"),
                    watermark_column=incremental_cfg.get("watermark_column"),
                    watermark_type=incremental_cfg.get("watermark_type", "timestamp"),
                    overlap=incremental_cfg.get("overlap", "PT0S"),
                    lower_bound_default=incremental_cfg.get("lower_bound_default"),
                    bound_inclusive=incremental_cfg.get("bound_inclusive", "auto"),
                    use_upper_bound=incremental_cfg.get("use_upper_bound", True),
                ),
            ),
            target=TargetSpec(
                catalog=target_cfg["catalog"],
                schema=target_cfg["schema"],
                table_name=target_cfg["table_name"],
                layer=target_cfg.get("layer", "bronze"),
                format=target_cfg.get("format", "delta"),
                write_mode=target_cfg.get("write_mode", "merge"),
                merge_keys=_tuple(target_cfg.get("merge_keys")),
                merge_guard=target_cfg.get("merge_guard", "watermark"),
                enable_change_data_feed=target_cfg.get("enable_change_data_feed", False),
                partition_by=_tuple(target_cfg.get("partition_by")),
                cluster_by=_tuple(target_cfg.get("cluster_by")),
                add_audit_columns=target_cfg.get("add_audit_columns", True),
                schema_evolution=target_cfg.get("schema_evolution", True),
                column_case=target_cfg.get("column_case", "preserve"),
                table_properties=dict(target_cfg.get("table_properties") or {}),
            ),
            control=ControlSpec(
                catalog=control_cfg["catalog"], schema=control_cfg["schema"]
            ),
            quality=QualitySpec(
                row_count_reconciliation=quality_cfg.get("row_count_reconciliation", True),
                fail_on_schema_drift=quality_cfg.get("fail_on_schema_drift", False),
                null_check_keys=quality_cfg.get("null_check_keys", True),
                expectations=tuple(
                    Expectation(
                        column=e["column"],
                        rule=e["rule"],
                        values=_tuple(e.get("values")),
                        value=e.get("value"),
                        pattern=e.get("pattern"),
                        action=e.get("action", "fail"),
                    )
                    for e in quality_cfg.get("expectations") or []
                ),
            ),
            runtime=RuntimeSpec(
                retries=runtime_cfg.get("retries", 2),
                retry_backoff_seconds=runtime_cfg.get("retry_backoff_seconds", 30),
                timeout_minutes=runtime_cfg.get("timeout_minutes", 120),
                enabled=runtime_cfg.get("enabled", True),
            ),
            alerting=AlertingSpec(
                on_failure=_tuple(alerting_cfg.get("on_failure")),
                on_reconciliation_mismatch=_tuple(
                    alerting_cfg.get("on_reconciliation_mismatch")
                ),
                on_freshness_breach=_tuple(alerting_cfg.get("on_freshness_breach")),
                freshness_sla_hours=alerting_cfg.get("freshness_sla_hours"),
            ),
            schedule=ScheduleSpec(
                group=schedule_cfg.get("group", "default"),
                cron=schedule_cfg.get("cron"),
                timezone=schedule_cfg.get("timezone", "UTC"),
                job_retries=schedule_cfg.get("job_retries", 0),
            ),
            log_level=(cfg.get("logging") or {}).get("level", "INFO"),
            config_hash=config_hash,
        )


def _tuple(value: Sequence[Any] | None) -> tuple:
    return tuple(value) if value else ()
