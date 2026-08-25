from __future__ import annotations

from pathlib import Path

import pytest

from ingestion_framework.config.resolver import ConfigResolutionError, ConfigResolver

from .conftest import write_yaml


def resolve(root: Path, fqn: str = "finance.widgets", env: str = "dev"):
    return ConfigResolver(root).resolve(fqn, env)


class TestHierarchy:
    def test_defaults_apply_when_nothing_overrides(self, config_tree: Path):
        cfg = resolve(config_tree).data
        assert cfg["source"]["fetch_size"] == 10000
        assert cfg["runtime"]["retries"] == 2

    def test_table_overrides_defaults(self, config_tree: Path):
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            """
version: 1
table: {domain: finance, source_schema: FINOWNER, source_object: WIDGETS, business_key: [WIDGET_ID]}
runtime: {retries: 7}
""",
        )
        assert resolve(config_tree).data["runtime"]["retries"] == 7

    def test_env_global_overrides_table(self, config_tree: Path):
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            """
version: 1
table: {domain: finance, source_schema: FINOWNER, source_object: WIDGETS}
runtime: {retries: 7}
""",
        )
        env = (config_tree / "environments" / "dev.yaml").read_text(encoding="utf-8")
        write_yaml(config_tree / "environments" / "dev.yaml", env + "\nruntime:\n  retries: 9")
        assert resolve(config_tree).data["runtime"]["retries"] == 9

    def test_env_table_override_beats_env_global(self, config_tree: Path):
        env = (config_tree / "environments" / "dev.yaml").read_text(encoding="utf-8")
        write_yaml(
            config_tree / "environments" / "dev.yaml",
            env.replace("overrides: {}", "")
            + """
runtime:
  retries: 9
overrides:
  finance.widgets:
    runtime:
      retries: 11
""",
        )
        assert resolve(config_tree).data["runtime"]["retries"] == 11

    def test_override_for_another_table_is_ignored(self, config_tree: Path):
        env = (config_tree / "environments" / "dev.yaml").read_text(encoding="utf-8")
        write_yaml(
            config_tree / "environments" / "dev.yaml",
            env.replace("overrides: {}", "")
            + "\noverrides:\n  sales.other:\n    runtime:\n      retries: 99",
        )
        assert resolve(config_tree).data["runtime"]["retries"] == 2

    def test_full_precedence_chain_in_one_key(self, config_tree: Path):
        """defaults < table < env-global < env-table, proven on a single key."""
        env_path = config_tree / "environments" / "dev.yaml"
        env_base = """
version: 1
env: dev
source:
  secret_scope: oracle-dev
  jdbc:
    url: "jdbc:oracle:thin:@//dev:1521/DEV"
{extra}
target:
  catalog: dev_lakehouse
  schema: bronze
control:
  catalog: dev_lakehouse
  schema: control
{overrides}
"""
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            """
version: 1
table: {domain: finance, source_schema: FINOWNER, source_object: WIDGETS}
source: {fetch_size: 2}
""",
        )
        write_yaml(env_path, env_base.format(extra="", overrides="overrides: {}"))
        assert resolve(config_tree).data["source"]["fetch_size"] == 2  # table beats defaults (10000)

        write_yaml(env_path, env_base.format(extra="  fetch_size: 3", overrides="overrides: {}"))
        assert resolve(config_tree).data["source"]["fetch_size"] == 3  # env-global beats table

        write_yaml(
            env_path,
            env_base.format(
                extra="  fetch_size: 3",
                overrides=(
                    "overrides:\n  finance.widgets:\n    source:\n      fetch_size: 4"
                ),
            ),
        )
        assert resolve(config_tree).data["source"]["fetch_size"] == 4  # env-table beats env-global


class TestIdentityAndDerivedDefaults:
    def test_identity_comes_from_the_directory(self, config_tree: Path):
        cfg = resolve(config_tree).data
        assert cfg["table"]["domain"] == "finance"
        assert cfg["table"]["name"] == "widgets"
        assert cfg["env"] == "dev"

    def test_domain_mismatch_is_rejected(self, config_tree: Path):
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            """
version: 1
table: {domain: sales, source_schema: FINOWNER, source_object: WIDGETS}
""",
        )
        with pytest.raises(ConfigResolutionError, match="directory is the source of truth"):
            resolve(config_tree)

    def test_target_table_name_defaults_to_lowercased_source_object(self, config_tree: Path):
        assert resolve(config_tree).data["target"]["table_name"] == "widgets"

    def test_merge_keys_default_to_business_key(self, config_tree: Path):
        assert resolve(config_tree).data["target"]["merge_keys"] == ["WIDGET_ID"]

    def test_explicit_merge_keys_are_kept(self, config_tree: Path):
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            """
version: 1
table: {domain: finance, source_schema: FINOWNER, source_object: WIDGETS, business_key: [A]}
target: {merge_keys: [B], merge_guard: none}
""",
        )
        assert resolve(config_tree).data["target"]["merge_keys"] == ["B"]

    def test_cluster_by_defaults_to_merge_keys(self, config_tree: Path):
        assert resolve(config_tree).data["target"]["cluster_by"] == ["WIDGET_ID"]

    def test_cluster_by_not_defaulted_when_partitioned(self, config_tree: Path):
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            """
version: 1
table: {domain: finance, source_schema: FINOWNER, source_object: WIDGETS, business_key: [WIDGET_ID]}
target: {partition_by: [LOAD_DATE], merge_guard: none}
""",
        )
        assert resolve(config_tree).data["target"]["cluster_by"] == []


class TestInterpolation:
    def test_identity_placeholders(self, config_tree: Path):
        env_path = config_tree / "environments" / "dev.yaml"
        env = env_path.read_text(encoding="utf-8")
        write_yaml(env_path, env + '\nschedule:\n  group: "${domain}_${env}"')
        assert resolve(config_tree).data["schedule"]["group"] == "finance_dev"

    def test_dotted_config_path_placeholder(self, config_tree: Path):
        env_path = config_tree / "environments" / "dev.yaml"
        env = env_path.read_text(encoding="utf-8").replace(
            "control:\n  catalog: dev_lakehouse", 'control:\n  catalog: "${target.catalog}"'
        )
        write_yaml(env_path, env)
        assert resolve(config_tree).data["control"]["catalog"] == "dev_lakehouse"

    def test_chained_placeholder_resolves_through_multiple_passes(self, config_tree: Path):
        """A placeholder whose value is itself a placeholder must fully resolve."""
        env_path = config_tree / "environments" / "dev.yaml"
        env = env_path.read_text(encoding="utf-8").replace(
            "control:\n  catalog: dev_lakehouse", 'control:\n  catalog: "${target.catalog}"'
        ).replace("target:\n  catalog: dev_lakehouse", 'target:\n  catalog: "${env}_lakehouse"')
        write_yaml(env_path, env)
        cfg = resolve(config_tree).data
        assert cfg["target"]["catalog"] == "dev_lakehouse"
        assert cfg["control"]["catalog"] == "dev_lakehouse"

    def test_unknown_placeholder_raises(self, config_tree: Path):
        env_path = config_tree / "environments" / "dev.yaml"
        env = env_path.read_text(encoding="utf-8").replace(
            "overrides: {}", 'schedule:\n  group: "${nope}"\noverrides: {}'
        )
        write_yaml(env_path, env)
        with pytest.raises(ConfigResolutionError, match=r"cannot resolve placeholder"):
            resolve(config_tree)

    def test_circular_placeholder_raises(self, config_tree: Path):
        env_path = config_tree / "environments" / "dev.yaml"
        env = env_path.read_text(encoding="utf-8").replace(
            "overrides: {}",
            'schedule:\n  group: "${schedule.timezone}"\n  timezone: "${schedule.group}"\noverrides: {}',
        )
        write_yaml(env_path, env)
        with pytest.raises(ConfigResolutionError, match="did not converge"):
            resolve(config_tree)


class TestConfigHash:
    def test_is_stable_across_resolutions(self, config_tree: Path):
        assert resolve(config_tree).config_hash == resolve(config_tree).config_hash

    def test_changes_when_any_layer_changes(self, config_tree: Path):
        before = resolve(config_tree).config_hash
        write_yaml(
            config_tree / "tables" / "finance" / "widgets.yaml",
            """
version: 1
table: {domain: finance, source_schema: FINOWNER, source_object: WIDGETS, business_key: [WIDGET_ID]}
runtime: {retries: 5}
target: {merge_guard: none}
""",
        )
        assert resolve(config_tree).config_hash != before

    def test_differs_between_environments(self, shipped_config: Path):
        resolver = ConfigResolver(shipped_config)
        dev = resolver.resolve("finance.gl_transactions", "dev")
        prod = resolver.resolve("finance.gl_transactions", "prod")
        assert dev.config_hash != prod.config_hash

    def test_records_contributing_files(self, config_tree: Path):
        sources = resolve(config_tree).sources
        assert len(sources) == 3
        assert sources[0].endswith("defaults.yaml")
        assert sources[2].endswith("dev.yaml")


class TestEnvironments:
    def test_unknown_environment_lists_available(self, config_tree: Path):
        with pytest.raises(ConfigResolutionError, match="available: dev"):
            resolve(config_tree, env="staging")

    def test_env_declaring_a_different_name_is_rejected(self, config_tree: Path):
        write_yaml(config_tree / "environments" / "qa.yaml", "version: 1\nenv: notqa")
        with pytest.raises(ConfigResolutionError, match="declares env"):
            resolve(config_tree, env="qa")

    def test_non_mapping_overrides_block_is_rejected(self, config_tree: Path):
        env_path = config_tree / "environments" / "dev.yaml"
        env = env_path.read_text(encoding="utf-8").replace("overrides: {}", "overrides: [a, b]")
        write_yaml(env_path, env)
        with pytest.raises(ConfigResolutionError, match="'overrides' must be a mapping"):
            resolve(config_tree)

    def test_resolve_all_covers_every_table(self, shipped_config: Path):
        resolved = ConfigResolver(shipped_config).resolve_all("prod")
        assert {r.table_fqn for r in resolved} == {
            "finance.gl_accounts",
            "finance.gl_transactions",
            "sales.order_events",
        }


class TestShippedTreeBehaviour:
    def test_prod_append_strategy_accumulates_alerts(self, shipped_config: Path):
        cfg = ConfigResolver(shipped_config).resolve("finance.gl_transactions", "prod").data
        assert cfg["alerting"]["on_failure"] == [
            "email:data-eng-oncall@example.com",
            "slack:#data-alerts",
        ]

    def test_prod_table_override_wins_over_env_global(self, shipped_config: Path):
        cfg = ConfigResolver(shipped_config).resolve("finance.gl_transactions", "prod").data
        assert cfg["source"]["read"]["num_partitions"] == 32  # env-global is 8
        assert cfg["runtime"]["timeout_minutes"] == 240

    def test_dev_row_limit_override_applies_only_in_dev(self, shipped_config: Path):
        resolver = ConfigResolver(shipped_config)
        assert resolver.resolve("finance.gl_transactions", "dev").data["extraction"]["row_limit"] == 100000
        assert resolver.resolve("finance.gl_transactions", "prod").data["extraction"]["row_limit"] is None
