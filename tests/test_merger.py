from __future__ import annotations

import pytest

from ingestion_framework.config.merger import (
    AppendList,
    ConfigMergeError,
    MergeByList,
    deep_merge,
    merge_layers,
)


class TestScalarsAndMappings:
    def test_scalar_override_wins(self):
        assert deep_merge({"a": 1}, {"a": 2}) == {"a": 2}

    def test_missing_key_keeps_base(self):
        assert deep_merge({"a": 1, "b": 2}, {"a": 9}) == {"a": 9, "b": 2}

    def test_nested_maps_merge_key_by_key(self):
        base = {"source": {"read": {"num_partitions": 8, "partition_column": None}}}
        override = {"source": {"read": {"num_partitions": 32}}}
        merged = deep_merge(base, override)
        assert merged["source"]["read"] == {"num_partitions": 32, "partition_column": None}

    def test_explicit_null_overrides(self):
        # Setting a value back to null must be possible from a higher layer.
        assert deep_merge({"filter": "X = 1"}, {"filter": None}) == {"filter": None}

    def test_type_mismatch_lets_override_win(self):
        assert deep_merge({"a": {"b": 1}}, {"a": "scalar"}) == {"a": "scalar"}

    def test_inputs_are_not_mutated(self):
        base = {"a": {"b": [1, 2]}}
        override = {"a": {"c": 3}}
        deep_merge(base, override)
        assert base == {"a": {"b": [1, 2]}}
        assert override == {"a": {"c": 3}}


class TestListStrategies:
    def test_plain_list_replaces(self):
        merged = deep_merge({"cols": ["A", "B", "C"]}, {"cols": ["D"]})
        assert merged["cols"] == ["D"]

    def test_append_concatenates(self):
        merged = deep_merge({"on_failure": ["email:a"]}, {"on_failure": AppendList(["slack:b"])})
        assert merged["on_failure"] == ["email:a", "slack:b"]

    def test_append_onto_absent_base(self):
        merged = deep_merge({}, {"on_failure": AppendList(["slack:b"])})
        assert merged["on_failure"] == ["slack:b"]

    def test_merge_by_updates_matching_entry(self):
        base = {"exp": [{"column": "AMOUNT", "rule": "not_null", "action": "fail"}]}
        override = {"exp": MergeByList([{"column": "AMOUNT", "action": "warn"}], key="column")}
        merged = deep_merge(base, override)
        assert merged["exp"] == [{"column": "AMOUNT", "rule": "not_null", "action": "warn"}]

    def test_merge_by_appends_new_entry_and_keeps_order(self):
        base = {"exp": [{"column": "A", "rule": "not_null"}]}
        override = {"exp": MergeByList([{"column": "B", "rule": "unique"}], key="column")}
        merged = deep_merge(base, override)
        assert [e["column"] for e in merged["exp"]] == ["A", "B"]

    def test_merge_by_rejects_entry_without_key(self):
        base = {"exp": [{"rule": "not_null"}]}
        override = {"exp": MergeByList([{"column": "A"}], key="column")}
        with pytest.raises(ConfigMergeError, match="column"):
            deep_merge(base, override)

    def test_merge_by_requires_a_key(self):
        with pytest.raises(ValueError, match="requires a key"):
            MergeByList([{"column": "A"}], key="")

    def test_strategy_lists_collapse_to_plain_lists(self):
        merged = deep_merge({}, {"x": AppendList(["a"])})
        # Downstream code (and JSON serialisation for the config hash) should
        # never have to know about the strategy wrapper.
        assert merged["x"] == ["a"]


class TestMergeLayers:
    def test_folds_left_to_right(self):
        merged = merge_layers({"a": 1, "b": 1}, {"b": 2, "c": 2}, {"c": 3})
        assert merged == {"a": 1, "b": 2, "c": 3}

    def test_skips_empty_layers(self):
        assert merge_layers({"a": 1}, None, {}, {"a": 2}) == {"a": 2}

    def test_append_accumulates_across_three_layers(self):
        merged = merge_layers(
            {"alerts": ["base"]},
            {"alerts": AppendList(["table"])},
            {"alerts": AppendList(["env"])},
        )
        assert merged["alerts"] == ["base", "table", "env"]
