"""Configuration layer: load YAML, merge the hierarchy, validate, build a RunSpec."""

from __future__ import annotations

from pathlib import Path

from ..engine.run_spec import RunSpec
from .loader import ConfigLoadError, discover_tables, load_yaml
from .merger import AppendList, ConfigMergeError, MergeByList, deep_merge, merge_layers
from .resolver import ConfigResolutionError, ConfigResolver, ResolvedConfig
from .validator import ConfigValidationError, ValidationReport, validate

__all__ = [
    "AppendList",
    "ConfigLoadError",
    "ConfigMergeError",
    "ConfigResolutionError",
    "ConfigResolver",
    "ConfigValidationError",
    "MergeByList",
    "ResolvedConfig",
    "RunSpec",
    "ValidationReport",
    "build_run_spec",
    "deep_merge",
    "discover_tables",
    "load_yaml",
    "merge_layers",
    "validate",
    "validate_all",
]


def build_run_spec(config_root: str | Path, table_fqn: str, env: str) -> RunSpec:
    """Resolve, validate, and type the config for one table in one environment.

    This is the single entry point every runtime component uses -- there is no
    supported path that reaches a RunSpec without passing validation.
    """
    resolver = ConfigResolver(config_root)
    resolved = resolver.resolve(table_fqn, env)
    validate(
        resolved.data, table_fqn=table_fqn, env=env, config_root=config_root
    ).raise_if_failed()
    return RunSpec.from_config(resolved.data, config_hash=resolved.config_hash)


def validate_all(config_root: str | Path, env: str) -> list[ValidationReport]:
    """Validate every discovered table for one environment (used by CI and the CLI).

    Resolution failures are reported as validation errors rather than raised, so
    one broken file does not hide the state of every other table.
    """
    resolver = ConfigResolver(config_root)
    reports: list[ValidationReport] = []
    for table_fqn in resolver.list_tables():
        try:
            resolved = resolver.resolve(table_fqn, env)
        except (ConfigLoadError, ConfigResolutionError, ConfigMergeError) as exc:
            report = ValidationReport(table_fqn=table_fqn, env=env)
            report.errors.append(str(exc))
            reports.append(report)
            continue
        reports.append(
            validate(resolved.data, table_fqn=table_fqn, env=env, config_root=config_root)
        )
    return reports
