"""Hierarchical deep-merge with explicit list strategies.

Merge semantics (override wins over base):

- scalars / None      -> override replaces base
- mappings            -> merged key-by-key, recursively
- sequences           -> override REPLACES base by default

List behaviour is opt-in configurable from YAML via tags on the *override*
side:

    alerting:
      on_failure: !append ["slack:#data-alerts"]      # base + override

    quality:
      expectations: !merge_by:column                  # merge maps by "column"
        - column: AMOUNT
          rule: not_null

See ``loader.py`` for how those tags are parsed.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence


class StrategyList(list):
    """Base for lists that carry a non-default merge strategy."""

    strategy: str = "replace"


class AppendList(StrategyList):
    """Concatenate onto the base list (duplicates preserved in order)."""

    strategy = "append"


class MergeByList(StrategyList):
    """Merge a list of mappings against the base, matching on ``key``.

    Base entries whose key is not present in the override are kept; matching
    entries are deep-merged; new entries are appended in override order.
    """

    strategy = "merge_by"

    def __init__(self, items: Sequence[Any] = (), key: str = "") -> None:
        super().__init__(items)
        if not key:
            raise ValueError("!merge_by requires a key, e.g. !merge_by:column")
        self.key = key


class ConfigMergeError(ValueError):
    """Raised when two config layers cannot be reconciled."""


def deep_merge(base: Any, override: Any, _path: str = "") -> Any:
    """Return a new value combining ``base`` and ``override``.

    Neither input is mutated. ``_path`` is threaded through purely so error
    messages can point at the offending key.
    """
    if override is _MISSING:
        return _copy(base)
    if isinstance(base, Mapping) and isinstance(override, Mapping):
        return _merge_mappings(base, override, _path)
    if isinstance(override, StrategyList):
        return _merge_strategy_list(base, override, _path)
    # Scalars, plain lists, and type mismatches: override wins outright.
    return _copy(override)


def _merge_mappings(base: Mapping, override: Mapping, path: str) -> dict:
    merged = {k: _copy(v) for k, v in base.items()}
    for key, value in override.items():
        child = f"{path}.{key}" if path else str(key)
        merged[key] = deep_merge(merged.get(key, _MISSING), value, child)
    return merged


def _merge_strategy_list(base: Any, override: StrategyList, path: str) -> list:
    base_list = list(base) if isinstance(base, (list, tuple)) else []

    if isinstance(override, AppendList):
        return [_copy(v) for v in base_list] + [_copy(v) for v in override]

    if isinstance(override, MergeByList):
        key = override.key
        merged: list = []
        index: dict[Any, int] = {}
        for item in base_list:
            if not isinstance(item, Mapping) or key not in item:
                raise ConfigMergeError(
                    f"{path or '<root>'}: !merge_by:{key} needs every base entry to be a "
                    f"mapping containing '{key}', got: {item!r}"
                )
            index[item[key]] = len(merged)
            merged.append(_copy(item))
        for item in override:
            if not isinstance(item, Mapping) or key not in item:
                raise ConfigMergeError(
                    f"{path or '<root>'}: !merge_by:{key} needs every override entry to be "
                    f"a mapping containing '{key}', got: {item!r}"
                )
            identity = item[key]
            if identity in index:
                position = index[identity]
                merged[position] = deep_merge(merged[position], item, f"{path}[{identity}]")
            else:
                index[identity] = len(merged)
                merged.append(_copy(item))
        return merged

    raise ConfigMergeError(f"{path or '<root>'}: unknown list strategy {override.strategy!r}")


def merge_layers(*layers: Mapping | None) -> dict:
    """Fold any number of config layers left-to-right; later layers win."""
    result: dict = {}
    for layer in layers:
        if not layer:
            continue
        result = deep_merge(result, layer)
    return result


class _Missing:
    """Sentinel distinguishing 'absent' from an explicit ``None`` override."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<MISSING>"


_MISSING = _Missing()


def _copy(value: Any) -> Any:
    """Deep-copy plain config data, collapsing strategy lists to plain lists."""
    if value is _MISSING:
        return None
    if isinstance(value, Mapping):
        return {k: _copy(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_copy(v) for v in value]
    return value
