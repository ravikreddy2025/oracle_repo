"""YAML loading for the framework's config tree.

Adds two things on top of ``yaml.safe_load``:

1. The ``!append`` / ``!merge_by:<key>`` list-strategy tags used by the merger.
2. Duplicate-key detection. Plain YAML silently keeps the last of two identical
   keys, which in a config-driven framework means a setting a developer wrote
   is quietly ignored. That is worth an error, not a shrug.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .merger import AppendList, MergeByList


class ConfigLoadError(ValueError):
    """Raised when a config file cannot be read or parsed."""


class FrameworkLoader(yaml.SafeLoader):
    """SafeLoader plus merge-strategy tags and duplicate-key detection."""


def _construct_mapping(loader: FrameworkLoader, node: yaml.MappingNode) -> dict:
    mapping: dict = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in mapping:
            raise ConfigLoadError(
                f"duplicate key {key!r} at line {key_node.start_mark.line + 1} "
                f"of {key_node.start_mark.name}: the earlier value would be silently lost"
            )
        mapping[key] = loader.construct_object(value_node, deep=True)
    return mapping


def _construct_append(loader: FrameworkLoader, node: yaml.Node) -> AppendList:
    if not isinstance(node, yaml.SequenceNode):
        raise ConfigLoadError(
            f"!append expects a list at line {node.start_mark.line + 1} "
            f"of {node.start_mark.name}"
        )
    return AppendList(loader.construct_sequence(node, deep=True))


def _construct_merge_by(loader: FrameworkLoader, suffix: str, node: yaml.Node) -> MergeByList:
    if not isinstance(node, yaml.SequenceNode):
        raise ConfigLoadError(
            f"!merge_by:{suffix} expects a list at line {node.start_mark.line + 1} "
            f"of {node.start_mark.name}"
        )
    if not suffix:
        raise ConfigLoadError(
            f"!merge_by requires a key (e.g. !merge_by:column) at line "
            f"{node.start_mark.line + 1} of {node.start_mark.name}"
        )
    return MergeByList(loader.construct_sequence(node, deep=True), key=suffix)


FrameworkLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
)
FrameworkLoader.add_constructor("!append", _construct_append)
FrameworkLoader.add_multi_constructor("!merge_by:", _construct_merge_by)


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load one YAML file into a dict. Missing file -> ConfigLoadError."""
    path = Path(path)
    if not path.is_file():
        raise ConfigLoadError(f"config file not found: {path}")
    try:
        data = yaml.load(path.read_text(encoding="utf-8"), Loader=FrameworkLoader)
    except ConfigLoadError:
        raise
    except yaml.YAMLError as exc:
        raise ConfigLoadError(f"invalid YAML in {path}: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigLoadError(f"{path}: top level of a config file must be a mapping")
    return data


def load_yaml_if_present(path: str | Path) -> dict[str, Any]:
    """Load a YAML file, returning ``{}`` when it does not exist."""
    return load_yaml(path) if Path(path).is_file() else {}


def discover_tables(config_root: str | Path) -> list[str]:
    """Return every table FQN (``domain.table``) found under ``config/tables``.

    Discovery is purely filesystem-driven, so onboarding really is 'add a
    YAML file' -- nothing registers a table anywhere else.
    """
    tables_dir = Path(config_root) / "tables"
    if not tables_dir.is_dir():
        return []
    found = []
    for path in sorted(tables_dir.glob("*/*.y*ml")):
        if path.name.startswith("_"):
            continue  # partials / shared fragments
        found.append(f"{path.parent.name}.{path.stem}")
    return found


def table_config_path(config_root: str | Path, table_fqn: str) -> Path:
    """Resolve ``domain.table`` to its config file path."""
    if table_fqn.count(".") != 1:
        raise ConfigLoadError(
            f"table reference {table_fqn!r} must be 'domain.table' (exactly one dot)"
        )
    domain, table = table_fqn.split(".")
    base = Path(config_root) / "tables" / domain
    for suffix in (".yaml", ".yml"):
        candidate = base / f"{table}{suffix}"
        if candidate.is_file():
            return candidate
    raise ConfigLoadError(
        f"no config file for table {table_fqn!r} (looked for {base / (table + '.yaml')})"
    )
