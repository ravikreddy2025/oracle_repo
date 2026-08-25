from __future__ import annotations

import pytest

from ingestion_framework.control.control_store import RunStatus, TaskStatus
from ingestion_framework.orchestration.batch_runner import (
    BatchRunner,
    DependencyError,
    blocked_by,
    order_tables,
    select_specs,
)

from .test_runner import FakeLoader, RUN_ID, audit_types, build_engine
from .test_sql_builder import spec


def table(name, *, domain="finance", depends_on=(), group="default", **patch):
    """A RunSpec for a named table. An explicit `schedule=` wins over `group=`."""
    schedule = {"group": group, **patch.pop("schedule", {})}
    return spec(
        table__name=name,
        table__domain=domain,
        table__depends_on=list(depends_on),
        schedule=schedule,
        **patch,
    )


class TestOrdering:
    def test_independent_tables_share_one_level(self):
        levels = order_tables([table("a"), table("b"), table("c")])
        assert len(levels) == 1
        assert {s.table_fqn for s in levels[0]} == {"finance.a", "finance.b", "finance.c"}

    def test_dependency_creates_a_second_level(self):
        levels = order_tables([table("child", depends_on=["finance.parent"]), table("parent")])
        assert [s.table_fqn for s in levels[0]] == ["finance.parent"]
        assert [s.table_fqn for s in levels[1]] == ["finance.child"]

    def test_chain_produces_one_level_each(self):
        levels = order_tables([
            table("c", depends_on=["finance.b"]),
            table("b", depends_on=["finance.a"]),
            table("a"),
        ])
        assert [[s.table_fqn for s in level] for level in levels] == [
            ["finance.a"], ["finance.b"], ["finance.c"]
        ]

    def test_diamond(self):
        levels = order_tables([
            table("d", depends_on=["finance.b", "finance.c"]),
            table("b", depends_on=["finance.a"]),
            table("c", depends_on=["finance.a"]),
            table("a"),
        ])
        assert [s.table_fqn for s in levels[0]] == ["finance.a"]
        assert {s.table_fqn for s in levels[1]} == {"finance.b", "finance.c"}
        assert [s.table_fqn for s in levels[2]] == ["finance.d"]

    def test_dependency_outside_the_batch_is_ignored(self):
        # Running one table of a domain must stay possible; refusing because a
        # sibling was not selected would make targeted reruns impossible.
        levels = order_tables([table("child", depends_on=["sales.elsewhere"])])
        assert [s.table_fqn for s in levels[0]] == ["finance.child"]

    def test_self_dependency_is_rejected(self):
        with pytest.raises(DependencyError, match="depends on itself"):
            order_tables([table("a", depends_on=["finance.a"])])

    def test_cycle_is_rejected_up_front(self):
        with pytest.raises(DependencyError, match="dependency cycle"):
            order_tables([
                table("a", depends_on=["finance.b"]),
                table("b", depends_on=["finance.a"]),
            ])

    def test_ordering_is_deterministic(self):
        specs = [table("c"), table("a"), table("b")]
        assert [s.table_fqn for s in order_tables(specs)[0]] == [
            "finance.a", "finance.b", "finance.c"
        ]

    def test_empty_batch(self):
        assert order_tables([]) == []


class TestBlockedBy:
    def test_reports_the_failed_dependency(self):
        assert blocked_by(table("c", depends_on=["finance.a"]), {"finance.a"}) == ["finance.a"]

    def test_unaffected_when_others_failed(self):
        assert blocked_by(table("c", depends_on=["finance.a"]), {"finance.z"}) == []


class TestSelection:
    def specs(self):
        return [
            table("gl", group="finance_hourly"),
            table("accounts", group="finance_daily"),
            table("orders", domain="sales", group="sales_hourly"),
        ]

    def test_by_table(self):
        selected = select_specs(self.specs(), tables=["finance.gl"])
        assert [s.table_fqn for s in selected] == ["finance.gl"]

    def test_by_group(self):
        assert [s.table_fqn for s in select_specs(self.specs(), group="finance_daily")] == [
            "finance.accounts"
        ]

    def test_by_domain(self):
        assert [s.table_fqn for s in select_specs(self.specs(), domain="sales")] == ["sales.orders"]

    def test_no_filter_returns_everything(self):
        assert len(select_specs(self.specs())) == 3

    def test_unknown_table_lists_what_is_known(self):
        with pytest.raises(DependencyError, match="unknown table"):
            select_specs(self.specs(), tables=["finance.ghost"])


class TestBatchRun:
    def test_all_succeed(self):
        engine, client = build_engine()
        result = BatchRunner(engine).run([table("a"), table("b")], RUN_ID, "prod")
        assert result.status is RunStatus.SUCCEEDED
        assert len(result.succeeded) == 2
        assert result.summary()["tables"] == 2

    def test_run_row_is_opened_and_closed(self):
        engine, client = build_engine()
        BatchRunner(engine).run([table("a")], RUN_ID, "prod")
        assert client.statements_matching("INSERT INTO prod_lakehouse.control.ingestion_runs")
        assert client.statements_matching("UPDATE prod_lakehouse.control.ingestion_runs")

    def test_run_events_bracket_the_tasks(self):
        engine, client = build_engine()
        BatchRunner(engine).run([table("a")], RUN_ID, "prod")
        types = audit_types(client)
        assert types[0] == "RUN_STARTED"
        assert types[-1] == "RUN_FINISHED"

    def test_partial_when_one_table_fails(self):
        engine, _ = build_engine(loader=FailOnce("finance.b"))
        result = BatchRunner(engine).run([table("a"), table("b")], RUN_ID, "prod")
        assert result.status is RunStatus.PARTIAL
        assert [o.table_fqn for o in result.failed] == ["finance.b"]

    def test_failed_when_every_table_fails(self):
        engine, _ = build_engine(loader=FakeLoader(error=RuntimeError("source down")))
        result = BatchRunner(engine).run(
            [table("a", runtime={"retries": 0}), table("b", runtime={"retries": 0})],
            RUN_ID, "prod",
        )
        assert result.status is RunStatus.FAILED

    def test_run_row_closes_even_if_the_run_itself_explodes(self):
        # Task-level errors are absorbed by TableRunner; this is the other kind
        # -- the run machinery itself coming apart. The row must still close.
        engine, client = build_engine()
        engine.audit.emit = _raise
        with pytest.raises(RuntimeError):
            BatchRunner(engine).run([table("a")], RUN_ID, "prod")
        update = client.params_for("UPDATE prod_lakehouse.control.ingestion_runs")
        assert update["status"] == "FAILED"


class TestDependencySkipping:
    def test_dependent_is_skipped_when_its_dependency_fails(self):
        engine, _ = build_engine(loader=FailOnce("finance.parent"))
        result = BatchRunner(engine).run(
            [table("parent", runtime={"retries": 0}), table("child", depends_on=["finance.parent"])],
            RUN_ID, "prod",
        )
        child = next(o for o in result.outcomes if o.table_fqn == "finance.child")
        assert child.status is TaskStatus.SKIPPED
        assert "dependency failed" in result.skipped_for_dependencies["finance.child"]

    def test_skipping_cascades_down_the_chain(self):
        engine, _ = build_engine(loader=FailOnce("finance.a"))
        result = BatchRunner(engine).run(
            [
                table("a", runtime={"retries": 0}),
                table("b", depends_on=["finance.a"]),
                table("c", depends_on=["finance.b"]),
            ],
            RUN_ID, "prod",
        )
        statuses = {o.table_fqn: o.status for o in result.outcomes}
        assert statuses["finance.b"] is TaskStatus.SKIPPED
        assert statuses["finance.c"] is TaskStatus.SKIPPED

    def test_unrelated_tables_still_run(self):
        engine, _ = build_engine(loader=FailOnce("finance.a"))
        result = BatchRunner(engine).run(
            [
                table("a", runtime={"retries": 0}),
                table("b", depends_on=["finance.a"]),
                table("z"),
            ],
            RUN_ID, "prod",
        )
        statuses = {o.table_fqn: o.status for o in result.outcomes}
        assert statuses["finance.z"] is TaskStatus.SUCCEEDED

    def test_continue_on_failure_runs_dependents_anyway(self):
        engine, _ = build_engine(loader=FailOnce("finance.parent"))
        result = BatchRunner(engine, skip_dependents_on_failure=False).run(
            [table("parent", runtime={"retries": 0}), table("child", depends_on=["finance.parent"])],
            RUN_ID, "prod",
        )
        child = next(o for o in result.outcomes if o.table_fqn == "finance.child")
        assert child.status is TaskStatus.SUCCEEDED


class TestConcurrency:
    def test_parallel_level_runs_every_table(self):
        engine, _ = build_engine()
        result = BatchRunner(engine, max_workers=3).run(
            [table("a"), table("b"), table("c")], RUN_ID, "prod"
        )
        assert len(result.succeeded) == 3

    def test_parallel_respects_dependency_levels(self):
        engine, _ = build_engine(loader=FailOnce("finance.parent"))
        result = BatchRunner(engine, max_workers=4).run(
            [table("parent", runtime={"retries": 0}), table("child", depends_on=["finance.parent"])],
            RUN_ID, "prod",
        )
        child = next(o for o in result.outcomes if o.table_fqn == "finance.child")
        assert child.status is TaskStatus.SKIPPED

    def test_workers_are_clamped_to_at_least_one(self):
        engine, _ = build_engine()
        result = BatchRunner(engine, max_workers=0).run([table("a")], RUN_ID, "prod")
        assert result.status is RunStatus.SUCCEEDED


class TestViewIsolation:
    def test_each_table_stages_into_its_own_views(self):
        """Fixed view names would have one table's staged batch silently
        replace another's when running concurrently."""
        from ingestion_framework.engine.transformer import view_names

        a = view_names("finance.a", RUN_ID, 1)
        b = view_names("finance.b", RUN_ID, 1)
        assert a.raw != b.raw and a.staged != b.staged

    def test_retries_get_fresh_views(self):
        from ingestion_framework.engine.transformer import view_names

        assert view_names("finance.a", RUN_ID, 1).raw != view_names("finance.a", RUN_ID, 2).raw

    def test_view_names_are_valid_identifiers(self):
        from ingestion_framework.engine.transformer import view_names

        names = view_names("finance.gl_transactions", "prod-20260824T100000-a1b2c3", 1)
        assert names.raw.replace("_", "").isalnum()
        assert names.staged.replace("_", "").isalnum()


# -- helpers ----------------------------------------------------------------


class FailOnce(FakeLoader):
    """Fails for one table, succeeds for the rest."""

    def __init__(self, failing_table: str):
        super().__init__()
        self.failing_table = failing_table

    def write(self, dataframe, spec, *, staged_view=None):
        if spec.table_fqn == self.failing_table:
            raise RuntimeError(f"load failed for {spec.table_fqn}")
        return super().write(dataframe, spec, staged_view=staged_view)


def _raise(*args, **kwargs):
    raise RuntimeError("unexpected control-plane error")
