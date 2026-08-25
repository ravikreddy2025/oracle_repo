"""Structural (JSON Schema) plus semantic validation of an effective config.

The schema catches shape errors -- unknown keys, wrong types, bad enums. The
semantic pass catches the combinations that are individually well-formed but
together mean something the framework cannot do, or that would silently do
nothing. Anything that would be a *silent* no-op is an error here, not a
warning: a developer who writes a filter expects it to filter.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping

from jsonschema import Draft202012Validator

_SCHEMA_RELATIVE = Path("schema") / "config.schema.json"

# Unquoted Oracle identifier. Quoted ones ("My Col") are passed through as-is.
_ORACLE_IDENT = re.compile(r'^(?:[A-Za-z][A-Za-z0-9_$#]*|"[^"]+")$')

# Statement terminators / comment starters have no business in a WHERE fragment.
_UNSAFE_PREDICATE = re.compile(r"(;|--|/\*|\*/)")


class ConfigValidationError(ValueError):
    """Raised when a config is unusable. Message lists every problem found."""


@dataclass
class ValidationReport:
    table_fqn: str
    env: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def raise_if_failed(self) -> "ValidationReport":
        if self.errors:
            bullets = "\n".join(f"  - {e}" for e in self.errors)
            raise ConfigValidationError(
                f"{self.table_fqn} [{self.env}]: {len(self.errors)} config problem(s):\n{bullets}"
            )
        return self


def validate(
    cfg: Mapping[str, Any],
    *,
    table_fqn: str = "<unknown>",
    env: str = "<unknown>",
    config_root: str | Path | None = None,
) -> ValidationReport:
    """Validate an effective (already merged) config. Never raises; inspect the report."""
    report = ValidationReport(table_fqn=table_fqn, env=env)
    _validate_schema(cfg, report, config_root)
    if report.errors:
        # Semantic checks assume a well-shaped config; running them on a
        # structurally broken one produces noise on top of the real error.
        return report
    _validate_semantics(cfg, report, config_root)
    return report


# -- structural -------------------------------------------------------------


@lru_cache(maxsize=8)
def _load_schema(schema_path: str) -> dict:
    return json.loads(Path(schema_path).read_text(encoding="utf-8"))


def _schema_path(config_root: str | Path | None) -> Path:
    if config_root is not None:
        candidate = Path(config_root) / _SCHEMA_RELATIVE
        if candidate.is_file():
            return candidate
    # Fall back to the schema shipped alongside the repo's config tree.
    default = Path(__file__).resolve().parents[3] / "config" / _SCHEMA_RELATIVE
    if not default.is_file():
        raise ConfigValidationError(f"config schema not found (looked at {default})")
    return default


def _validate_schema(
    cfg: Mapping[str, Any], report: ValidationReport, config_root: str | Path | None
) -> None:
    schema = _load_schema(str(_schema_path(config_root)))
    validator = Draft202012Validator(schema)
    for error in sorted(validator.iter_errors(dict(cfg)), key=lambda e: list(e.path)):
        location = ".".join(str(p) for p in error.path) or "<root>"
        report.errors.append(f"{location}: {error.message}")


# -- semantic ---------------------------------------------------------------


def _validate_semantics(
    cfg: Mapping[str, Any], report: ValidationReport, config_root: str | Path | None
) -> None:
    table = cfg.get("table", {})
    source = cfg.get("source", {})
    extraction = cfg.get("extraction", {})
    incremental = extraction.get("incremental", {}) or {}
    target = cfg.get("target", {})
    quality = cfg.get("quality", {}) or {}

    mode = extraction.get("mode")
    strategy = incremental.get("strategy", "watermark")
    watermark_column = incremental.get("watermark_column")
    write_mode = target.get("write_mode")
    columns = extraction.get("columns", "*")
    explicit_columns = columns if isinstance(columns, list) else None
    column_set = {c.upper() for c in explicit_columns} if explicit_columns else None

    # --- identifiers ------------------------------------------------------
    for label, value in (
        ("table.source_schema", table.get("source_schema")),
        ("table.source_object", table.get("source_object")),
    ):
        if value and not _ORACLE_IDENT.match(str(value)):
            report.errors.append(f"{label}: {value!r} is not a valid Oracle identifier")

    if explicit_columns:
        seen: set[str] = set()
        for col in explicit_columns:
            if not _ORACLE_IDENT.match(col):
                report.errors.append(
                    f"extraction.columns: {col!r} is not a plain Oracle identifier. "
                    f"Use extraction.mode: query for computed columns or expressions."
                )
            if col.upper() in seen:
                report.errors.append(f"extraction.columns: {col!r} listed more than once")
            seen.add(col.upper())

    # --- predicates -------------------------------------------------------
    for label, predicate in (
        ("extraction.filter", extraction.get("filter")),
    ):
        if predicate and _UNSAFE_PREDICATE.search(str(predicate)):
            report.errors.append(
                f"{label}: must be a single boolean expression -- ';' and SQL comments "
                f"are rejected because this text is concatenated into the source query"
            )

    # --- extraction mode coherence ---------------------------------------
    if mode == "incremental":
        if strategy == "watermark" and not watermark_column:
            report.errors.append(
                "extraction.incremental.watermark_column is required when "
                "mode is 'incremental' and strategy is 'watermark'"
            )
        if strategy == "scn" and watermark_column:
            report.errors.append(
                "extraction.incremental.watermark_column must not be set when strategy "
                "is 'scn' (the SCN pseudo-column ORA_ROWSCN is used instead)"
            )
    else:
        overlap = incremental.get("overlap")
        if overlap and overlap != "PT0S":
            report.errors.append(
                f"extraction.incremental.overlap is {overlap!r} but mode is {mode!r} -- "
                f"overlap only applies to incremental extraction and would be ignored"
            )
        if watermark_column and mode == "full":
            report.warnings.append(
                f"extraction.incremental.watermark_column ({watermark_column}) is set but "
                f"mode is 'full'; it will only be used as the merge tie-breaker"
            )

    lower_default = incremental.get("lower_bound_default")
    if lower_default is not None and mode == "incremental":
        declared_type = "number" if strategy == "scn" else incremental.get(
            "watermark_type", "timestamp"
        )
        from ..control.watermark import WatermarkError, canonicalize

        try:
            canonicalize(lower_default, declared_type)
        except WatermarkError:
            report.errors.append(
                f"extraction.incremental.lower_bound_default {lower_default!r} is not a valid "
                f"{declared_type} value. It is only used on a table's first run, so a mismatch "
                f"here fails once -- in production, on a table nobody has run before."
            )

    if mode == "query":
        query_file = extraction.get("query_file")
        if not query_file:
            report.errors.append("extraction.query_file is required when mode is 'query'")
        elif config_root is not None:
            path = Path(config_root) / query_file
            if not path.is_file():
                report.errors.append(f"extraction.query_file: file not found: {path}")
        if explicit_columns:
            report.errors.append(
                "extraction.columns cannot be combined with mode 'query' -- the query "
                "defines its own projection and the column list would be ignored"
            )
        if extraction.get("filter"):
            report.errors.append(
                "extraction.filter cannot be combined with mode 'query' -- put the "
                "predicate in the query file, otherwise it would be ignored"
            )
    elif extraction.get("query_file"):
        report.errors.append(
            f"extraction.query_file is set but mode is {mode!r}; it would be ignored"
        )

    # --- write mode coherence --------------------------------------------
    merge_keys = target.get("merge_keys") or []
    if write_mode == "merge":
        if not merge_keys:
            report.errors.append(
                "target.merge_keys is required when write_mode is 'merge' "
                "(set table.business_key and it will be inherited)"
            )
        if target.get("merge_guard", "watermark") == "watermark" and not watermark_column:
            report.warnings.append(
                "target.merge_guard is 'watermark' but no watermark column is configured; "
                "the guard will be omitted and the merge becomes last-write-wins"
            )
        if target.get("partition_by"):
            report.warnings.append(
                "target.partition_by is set on a merge table; merge keys scatter across "
                "partitions, so prefer cluster_by on the merge keys (see DESIGN 3.7)"
            )
    elif merge_keys and write_mode == "overwrite":
        report.warnings.append(
            f"target.merge_keys is set but write_mode is {write_mode!r}; the keys are unused"
        )

    # --- column-set consistency ------------------------------------------
    if column_set is not None:
        _require_columns(report, "target.merge_keys", merge_keys, column_set)
        _require_columns(report, "target.partition_by", target.get("partition_by") or [], column_set)
        _require_columns(report, "target.cluster_by", target.get("cluster_by") or [], column_set)
        _require_columns(
            report,
            "quality.expectations",
            [e.get("column") for e in quality.get("expectations") or []],
            column_set,
        )
        if watermark_column and watermark_column.upper() not in column_set:
            report.errors.append(
                f"extraction.incremental.watermark_column {watermark_column!r} is not in "
                f"extraction.columns -- the incremental predicate would reference a column "
                f"that is not selected"
            )
        partition_column = (source.get("read") or {}).get("partition_column")
        if partition_column and partition_column.upper() not in column_set:
            report.errors.append(
                f"source.read.partition_column {partition_column!r} is not in "
                f"extraction.columns -- JDBC bounded reads need the column in the projection"
            )

    if write_mode == "merge" and not (cfg.get("table") or {}).get("business_key"):
        report.warnings.append(
            "table.business_key is empty; merge_keys were taken from config instead. "
            "Declare business_key so the key is documented with the table."
        )

    # --- parallel read ----------------------------------------------------
    read = source.get("read") or {}
    bounds_strategy = read.get("bounds_strategy", "auto")
    if read.get("num_partitions", 1) > 1 and not read.get("partition_column"):
        report.warnings.append(
            f"source.read.num_partitions is {read['num_partitions']} but no partition_column "
            f"is set; the JDBC read will run single-threaded"
        )
    if bounds_strategy == "explicit" and read.get("partition_column"):
        if read.get("lower_bound") is None or read.get("upper_bound") is None:
            report.errors.append(
                "source.read.lower_bound and upper_bound are required when "
                "bounds_strategy is 'explicit'"
            )

    # --- expectations -----------------------------------------------------
    for i, exp in enumerate(quality.get("expectations") or []):
        rule = exp.get("rule")
        where = f"quality.expectations[{i}] ({exp.get('column')})"
        if rule == "in_set" and not exp.get("values"):
            report.errors.append(f"{where}: rule 'in_set' requires a non-empty 'values' list")
        if rule in {"min", "max"} and exp.get("value") is None:
            report.errors.append(f"{where}: rule {rule!r} requires 'value'")
        if rule == "regex" and not exp.get("pattern"):
            report.errors.append(f"{where}: rule 'regex' requires 'pattern'")

    # --- credentials ------------------------------------------------------
    if not source.get("secret_scope"):
        report.errors.append(
            "source.secret_scope is required -- Oracle credentials must come from a "
            "Databricks secret scope, never from config"
        )
    jdbc_url = (source.get("jdbc") or {}).get("url")
    if not jdbc_url:
        report.errors.append("source.jdbc.url is required (set it per environment)")
    elif re.search(r"(?i)(password|pwd)\s*=", str(jdbc_url)):
        report.errors.append("source.jdbc.url must not embed credentials")


def _require_columns(
    report: ValidationReport, label: str, values: Iterable[Any], column_set: set[str]
) -> None:
    for value in values:
        if value and str(value).upper() not in column_set:
            report.errors.append(
                f"{label}: {value!r} is not in extraction.columns"
            )
