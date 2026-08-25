"""Resolve the three config layers into one effective config for (table, env).

Layer order -- later layers win:

    1. config/defaults.yaml
    2. config/tables/<domain>/<table>.yaml
    3. config/environments/<env>.yaml  (global block)
    4. config/environments/<env>.yaml  overrides."<domain>.<table>"

Steps 3 and 4 are separate merges on purpose: the global env block carries
connection details and catalog names that apply to every table, while the
overrides block carries per-table tuning for that environment only.

After merging, the resolver fills in derived defaults (target table name,
merge keys from the business key) and substitutes ``${...}`` placeholders.
Secrets are never resolved here -- only the scope/key reference travels in
the config; the runtime fetches the value.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .loader import (
    ConfigLoadError,
    discover_tables,
    load_yaml,
    load_yaml_if_present,
    table_config_path,
)
from .merger import merge_layers

_PLACEHOLDER = re.compile(r"\$\{([a-zA-Z0-9_.]+)\}")
_MAX_INTERPOLATION_PASSES = 5

# Keys that exist only to steer resolution and must not reach the effective config.
_ENV_CONTROL_KEYS = {"env", "overrides", "version"}


class ConfigResolutionError(ValueError):
    """Raised when the layers cannot be resolved into a usable config."""


@dataclass(frozen=True)
class ResolvedConfig:
    """The effective config for one table in one environment."""

    table_fqn: str
    env: str
    data: dict[str, Any]
    config_hash: str
    sources: tuple[str, ...]  # files that contributed, in merge order

    def to_json(self) -> str:
        return json.dumps(self.data, sort_keys=True, separators=(",", ":"), default=str)


class ConfigResolver:
    """Loads and merges the config tree rooted at ``config_root``."""

    def __init__(self, config_root: str | Path) -> None:
        self.config_root = Path(config_root)
        if not self.config_root.is_dir():
            raise ConfigResolutionError(f"config root not found: {self.config_root}")

    # -- public API ---------------------------------------------------------

    def list_tables(self) -> list[str]:
        return discover_tables(self.config_root)

    def environments(self) -> list[str]:
        """Every environment the config tree defines."""
        env_dir = self.config_root / "environments"
        if not env_dir.is_dir():
            return []
        return sorted(p.stem for p in env_dir.glob("*.y*ml"))

    def require_environment(self, env: str) -> None:
        """Raise if the environment does not exist.

        An unknown env is a usage mistake, not a per-table config error, so it
        is surfaced before any table is resolved.
        """
        self._env_path(env)

    def resolve(self, table_fqn: str, env: str) -> ResolvedConfig:
        domain, table = _split_fqn(table_fqn)

        defaults_path = self.config_root / "defaults.yaml"
        table_path = table_config_path(self.config_root, table_fqn)
        env_path = self._env_path(env)

        defaults = load_yaml(defaults_path)
        table_cfg = load_yaml(table_path)
        env_cfg = load_yaml(env_path)

        declared_env = env_cfg.get("env")
        if declared_env and declared_env != env:
            raise ConfigResolutionError(
                f"{env_path}: declares env {declared_env!r} but was loaded as {env!r}"
            )

        env_global = {k: v for k, v in env_cfg.items() if k not in _ENV_CONTROL_KEYS}
        env_table = self._env_overrides_for(env_cfg, table_fqn, env_path)

        merged = merge_layers(defaults, table_cfg, env_global, env_table)
        merged = self._apply_identity(merged, domain, table, env)
        merged = self._apply_derived_defaults(merged)
        merged = _interpolate(merged, table_fqn=table_fqn, env=env, domain=domain, table=table)

        return ResolvedConfig(
            table_fqn=table_fqn,
            env=env,
            data=merged,
            config_hash=_hash_config(merged),
            sources=(str(defaults_path), str(table_path), str(env_path)),
        )

    def resolve_all(self, env: str) -> list[ResolvedConfig]:
        return [self.resolve(fqn, env) for fqn in self.list_tables()]

    # -- internals ----------------------------------------------------------

    def _env_path(self, env: str) -> Path:
        for suffix in (".yaml", ".yml"):
            candidate = self.config_root / "environments" / f"{env}{suffix}"
            if candidate.is_file():
                return candidate
        available = sorted(
            p.stem for p in (self.config_root / "environments").glob("*.y*ml")
        )
        raise ConfigResolutionError(
            f"unknown environment {env!r}; available: {', '.join(available) or 'none'}"
        )

    @staticmethod
    def _env_overrides_for(env_cfg: Mapping, table_fqn: str, env_path: Path) -> dict:
        overrides = env_cfg.get("overrides") or {}
        if not isinstance(overrides, Mapping):
            raise ConfigResolutionError(f"{env_path}: 'overrides' must be a mapping")
        block = overrides.get(table_fqn, {})
        if block and not isinstance(block, Mapping):
            raise ConfigResolutionError(
                f"{env_path}: overrides.{table_fqn} must be a mapping"
            )
        return dict(block)

    @staticmethod
    def _apply_identity(cfg: dict, domain: str, table: str, env: str) -> dict:
        """Stamp identity fields, and check the file agrees with its location."""
        table_block = dict(cfg.get("table") or {})
        declared_domain = table_block.get("domain")
        if declared_domain and declared_domain != domain:
            raise ConfigResolutionError(
                f"table.domain is {declared_domain!r} but the file lives under "
                f"tables/{domain}/ -- the directory is the source of truth"
            )
        table_block["domain"] = domain
        table_block.setdefault("name", table)
        cfg["table"] = table_block
        cfg["env"] = env
        return cfg

    @staticmethod
    def _apply_derived_defaults(cfg: dict) -> dict:
        """Fill values a developer should not have to repeat."""
        table_block = cfg.get("table") or {}
        target = dict(cfg.get("target") or {})

        # Target table name mirrors the Oracle object name, lower-cased.
        source_object = table_block.get("source_object")
        if not target.get("table_name") and source_object:
            target["table_name"] = str(source_object).lower()

        # Merge keys default to the declared business key.
        business_key = table_block.get("business_key") or []
        if not target.get("merge_keys") and business_key:
            target["merge_keys"] = list(business_key)

        # Cluster on the merge keys unless the table says otherwise (see DESIGN
        # 3.7: partitioning by business date defeats MERGE pruning).
        if (
            target.get("write_mode") == "merge"
            and not target.get("cluster_by")
            and not target.get("partition_by")
            and target.get("merge_keys")
        ):
            target["cluster_by"] = list(target["merge_keys"])

        cfg["target"] = target
        return cfg


# -- placeholder interpolation ---------------------------------------------


def _interpolate(cfg: dict, **identity: str) -> dict:
    """Substitute ``${name}`` / ``${dotted.path}`` references throughout.

    Names resolve against the identity kwargs first, then against the config
    itself by dotted path -- so ``${target.catalog}`` works. Several passes run
    so a placeholder may expand into another placeholder.
    """
    current = cfg
    for _ in range(_MAX_INTERPOLATION_PASSES):
        replaced_any = False

        def substitute(value: str) -> str:
            def repl(match: re.Match) -> str:
                # nonlocal must be declared here, in the function that actually
                # assigns the flag -- declaring it in substitute() only would
                # make this a local write and the loop would exit after one pass.
                nonlocal replaced_any
                name = match.group(1)
                if name in identity:
                    resolved = identity[name]
                else:
                    resolved = _lookup_path(current, name)
                    if resolved is None:
                        raise ConfigResolutionError(
                            f"cannot resolve placeholder ${{{name}}} "
                            f"(no identity value and no config key at that path)"
                        )
                if isinstance(resolved, (dict, list)):
                    raise ConfigResolutionError(
                        f"placeholder ${{{name}}} resolves to a {type(resolved).__name__}, "
                        f"which cannot be substituted into a string"
                    )
                replaced_any = True
                return str(resolved)

            return _PLACEHOLDER.sub(repl, value)

        current = _walk_strings(current, substitute)
        if not replaced_any:
            return current

    raise ConfigResolutionError(
        "placeholder interpolation did not converge -- check for a circular ${...} reference"
    )


def _walk_strings(value: Any, fn) -> Any:
    if isinstance(value, Mapping):
        return {k: _walk_strings(v, fn) for k, v in value.items()}
    if isinstance(value, list):
        return [_walk_strings(v, fn) for v in value]
    if isinstance(value, str):
        return fn(value)
    return value


def _lookup_path(cfg: Mapping, dotted: str) -> Any:
    node: Any = cfg
    for part in dotted.split("."):
        if not isinstance(node, Mapping) or part not in node:
            return None
        node = node[part]
    return node


# -- helpers ----------------------------------------------------------------


def _split_fqn(table_fqn: str) -> tuple[str, str]:
    if table_fqn.count(".") != 1:
        raise ConfigLoadError(
            f"table reference {table_fqn!r} must be 'domain.table' (exactly one dot)"
        )
    domain, table = table_fqn.split(".")
    return domain, table


def _hash_config(cfg: Mapping) -> str:
    canonical = json.dumps(cfg, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
