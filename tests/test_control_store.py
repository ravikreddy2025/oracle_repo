from __future__ import annotations

from datetime import datetime

import pytest

from ingestion_framework.control.audit import AuditLog, EventType
from ingestion_framework.control.control_store import (
    ControlStateError,
    ControlStore,
    RunStatus,
    TaskMetrics,
    TaskRef,
    TaskStatus,
    assert_transition,
    derive_run_status,
    make_run_id,
)
from ingestion_framework.control.sql_client import RecordingSqlClient

NOW = datetime(2026, 8, 24, 10, 0, 0)
LATER = datetime(2026, 8, 24, 10, 5, 30)


@pytest.fixture
def client() -> RecordingSqlClient:
    return RecordingSqlClient()


@pytest.fixture
def store(client: RecordingSqlClient) -> ControlStore:
    return ControlStore(client, "prod_lakehouse", "control", now=lambda: NOW)


class TestStateMachine:
    @pytest.mark.parametrize(
        "current,new",
        [
            (TaskStatus.PENDING, TaskStatus.RUNNING),
            (TaskStatus.PENDING, TaskStatus.SKIPPED),
            (TaskStatus.RUNNING, TaskStatus.SUCCEEDED),
            (TaskStatus.RUNNING, TaskStatus.FAILED),
            (TaskStatus.RUNNING, TaskStatus.SKIPPED),
        ],
    )
    def test_legal_transitions(self, current, new):
        assert_transition(current, new)

    @pytest.mark.parametrize(
        "current,new",
        [
            (TaskStatus.PENDING, TaskStatus.SUCCEEDED),  # cannot skip RUNNING
            (TaskStatus.RUNNING, TaskStatus.PENDING),  # no going back
            (TaskStatus.SUCCEEDED, TaskStatus.RUNNING),
            (TaskStatus.FAILED, TaskStatus.SUCCEEDED),
        ],
    )
    def test_illegal_transitions(self, current, new):
        with pytest.raises(ControlStateError):
            assert_transition(current, new)

    def test_terminal_error_names_the_remedy(self):
        with pytest.raises(ControlStateError, match="record a new attempt"):
            assert_transition(TaskStatus.FAILED, TaskStatus.RUNNING)

    def test_accepts_plain_strings(self):
        assert_transition("RUNNING", "SUCCEEDED")


class TestDeriveRunStatus:
    def test_all_succeeded(self):
        assert derive_run_status(["SUCCEEDED", "SUCCEEDED"]) is RunStatus.SUCCEEDED

    def test_skipped_counts_as_not_failed(self):
        assert derive_run_status(["SUCCEEDED", "SKIPPED"]) is RunStatus.SUCCEEDED

    def test_all_failed_is_failed(self):
        assert derive_run_status(["FAILED", "FAILED"]) is RunStatus.FAILED

    def test_mixed_is_partial(self):
        # Distinguishing these matters operationally: all-failed points at the
        # source or the cluster, some-failed points at a specific table.
        assert derive_run_status(["SUCCEEDED", "FAILED"]) is RunStatus.PARTIAL

    def test_unfinished_task_keeps_the_run_running(self):
        assert derive_run_status(["SUCCEEDED", "RUNNING"]) is RunStatus.RUNNING

    def test_empty_run_is_vacuously_successful(self):
        assert derive_run_status([]) is RunStatus.SUCCEEDED


class TestRunIds:
    def test_shape_is_sortable_and_scannable(self):
        assert make_run_id("prod", NOW, "a1b2c3d4") == "prod-20260824T100000-a1b2c3"

    def test_ids_sort_chronologically(self):
        early = make_run_id("prod", NOW, "zzzzzz")
        late = make_run_id("prod", LATER, "aaaaaa")
        assert early < late


class TestInitialize:
    def test_runs_every_ddl_statement(self, client, store):
        statements = store.initialize()
        assert len(statements) == 7  # schema + 6 tables
        assert len(client.calls) == 7
        assert all("IF NOT EXISTS" in s for s in client.statements)


class TestRuns:
    def test_start_run_inserts_running(self, client, store):
        run = store.start_run("prod-1", "prod", trigger="schedule", table_count=3)
        assert run.run_id == "prod-1" and run.started_at == NOW
        params = client.params_for("INSERT INTO prod_lakehouse.control.ingestion_runs")
        assert params["status"] == "RUNNING"
        assert params["trigger"] == "schedule"
        assert params["table_count"] == 3

    def test_finish_run_with_explicit_status(self, client, store):
        run = store.start_run("prod-1", "prod")
        status = store.finish_run(run, RunStatus.SUCCEEDED, ended_at=LATER)
        assert status is RunStatus.SUCCEEDED
        params = client.params_for("UPDATE prod_lakehouse.control.ingestion_runs")
        assert params["duration_ms"] == 330_000
        assert params["ended_at"] == LATER

    def test_finish_run_derives_status_from_tasks(self, client, store):
        run = store.start_run("prod-1", "prod")
        client.queue_result([{"status": "SUCCEEDED"}, {"status": "FAILED"}])
        assert store.finish_run(run) is RunStatus.PARTIAL

    def test_task_statuses_reads_only_the_latest_attempt(self, client, store):
        client.queue_result([{"status": "SUCCEEDED"}])
        store.task_statuses("prod-1")
        statement = client.statements_matching("SELECT status")[0]
        assert "MAX(attempt)" in statement


class TestTasks:
    def test_start_task_opens_in_running(self, client, store):
        task = store.start_task(
            "prod-1", "finance.gl_transactions", "prod",
            extraction_mode="incremental", write_mode="merge",
            config_hash="abc123", watermark_from="2026-08-24 04:00:00.000000",
        )
        assert task == TaskRef("prod-1", "finance.gl_transactions", 1)
        params = client.params_for("INSERT INTO prod_lakehouse.control.ingestion_tasks")
        assert params["status"] == "RUNNING"
        assert params["watermark_from"] == "2026-08-24 04:00:00.000000"
        assert params["config_hash"] == "abc123"

    def test_finish_task_writes_metrics(self, client, store):
        task = TaskRef("prod-1", "finance.gl_transactions", 1)
        store.finish_task(
            task,
            TaskStatus.SUCCEEDED,
            metrics=TaskMetrics(
                source_count=1000, rows_read=1000, rows_written=1000,
                rows_inserted=900, rows_updated=100,
                watermark_to="2026-08-24 11:00:00.000000",
            ),
            started_at=NOW,
            ended_at=LATER,
        )
        params = client.params_for("UPDATE prod_lakehouse.control.ingestion_tasks")
        assert params["status"] == "SUCCEEDED"
        assert params["rows_inserted"] == 900
        assert params["duration_ms"] == 330_000
        assert params["attempt"] == 1

    def test_finish_task_enforces_the_state_machine(self, store):
        task = TaskRef("prod-1", "finance.gl", 1)
        with pytest.raises(ControlStateError):
            store.finish_task(task, TaskStatus.SUCCEEDED, current_status=TaskStatus.SUCCEEDED)

    def test_failure_records_error_type_and_message(self, client, store):
        store.finish_task(
            TaskRef("prod-1", "finance.gl", 1),
            TaskStatus.FAILED,
            error_type="OracleConnectionError",
            error_message="ORA-12541: TNS:no listener",
        )
        params = client.params_for("UPDATE prod_lakehouse.control.ingestion_tasks")
        assert params["error_type"] == "OracleConnectionError"
        assert "ORA-12541" in params["error_message"]

    def test_long_error_is_truncated_with_a_marker(self, client, store):
        store.finish_task(
            TaskRef("prod-1", "finance.gl", 1),
            TaskStatus.FAILED,
            error_message="x" * 9000,
        )
        message = client.params_for("UPDATE prod_lakehouse.control.ingestion_tasks")["error_message"]
        assert len(message) < 9000 and "truncated 1000 chars" in message

    def test_finish_task_keeps_watermark_from_when_not_resupplied(self, client, store):
        store.finish_task(TaskRef("prod-1", "finance.gl", 1), TaskStatus.SUCCEEDED)
        statement = client.statements_matching("UPDATE prod_lakehouse.control.ingestion_tasks")[0]
        assert "COALESCE(:watermark_from, watermark_from)" in statement

    def test_next_attempt_starts_at_one(self, client, store):
        assert store.next_attempt("prod-1", "finance.gl") == 1

    def test_next_attempt_increments_past_the_last(self, client, store):
        client.queue_result([{"max_attempt": 2}])
        assert store.next_attempt("prod-1", "finance.gl") == 3

    def test_retry_appends_an_attempt_rather_than_overwriting(self, client, store):
        store.start_task("prod-1", "finance.gl", "prod", extraction_mode="full", write_mode="merge")
        store.finish_task(TaskRef("prod-1", "finance.gl", 1), TaskStatus.FAILED)
        client.queue_result([{"max_attempt": 1}])
        attempt = store.next_attempt("prod-1", "finance.gl")
        store.start_task(
            "prod-1", "finance.gl", "prod",
            extraction_mode="full", write_mode="merge", attempt=attempt,
        )
        inserts = [p for k, s, p in client.calls if "INSERT INTO" in s]
        assert [p["attempt"] for p in inserts] == [1, 2]


class TestConfigRegistry:
    def test_registers_by_content_hash(self, client, store):
        store.register_config(
            table_fqn="finance.gl", env="prod", config_hash="deadbeef",
            resolved_json='{"a":1}', config_sources=["defaults.yaml", "gl.yaml"],
            run_id="prod-1",
        )
        merge = client.statements_matching("MERGE INTO prod_lakehouse.control.config_registry")[0]
        assert "ON t.config_hash = s.config_hash" in merge
        assert "t.last_seen_at" in merge  # unchanged config touches, does not duplicate
        params = client.params_for("MERGE INTO prod_lakehouse.control.config_registry")
        assert params["config_sources"] == ["defaults.yaml", "gl.yaml"]
        assert params["seen_at"] == NOW


class TestReconciliation:
    def test_records_checks_in_one_commit(self, client, store):
        written = store.record_checks([
            {"run_id": "prod-1", "table_fqn": "finance.gl", "env": "prod",
             "check_type": "row_count", "source_count": 100, "target_count": 100,
             "delta": 0, "status": "PASSED"},
            {"run_id": "prod-1", "table_fqn": "finance.gl", "env": "prod",
             "check_type": "expectation", "check_name": "AMOUNT not_null", "status": "PASSED"},
        ])
        assert written == 2
        assert len(client.inserted) == 1  # batched, not one commit per check
        table, rows = client.inserted[0]
        assert table == "prod_lakehouse.control.reconciliation"
        assert all(row["checked_at"] == NOW for row in rows)

    def test_no_checks_writes_nothing(self, client, store):
        assert store.record_checks([]) == 0
        assert client.calls == []


class TestLifecycleIntegration:
    def test_successful_task_lifecycle_in_order(self, client, store):
        """The ordering the design requires: task closes before the run does."""
        audit = AuditLog(
            client, "prod_lakehouse", "control",
            run_id="prod-1", env="prod", actor="sp-ingest",
            now=lambda: NOW, id_factory=lambda: "evt",
        )
        run = store.start_run("prod-1", "prod", trigger="schedule", table_count=1)
        audit.emit(EventType.RUN_STARTED)
        task = store.start_task(
            "prod-1", "finance.gl", "prod", extraction_mode="incremental", write_mode="merge"
        )
        audit.emit(EventType.TASK_STARTED, table_fqn="finance.gl")
        store.finish_task(task, TaskStatus.SUCCEEDED, metrics=TaskMetrics(rows_written=10))
        audit.emit(EventType.TASK_SUCCEEDED, table_fqn="finance.gl")
        client.queue_result([{"status": "SUCCEEDED"}])
        assert store.finish_run(run) is RunStatus.SUCCEEDED
        audit.emit(EventType.RUN_FINISHED, flush=True)

        kinds = [(k, s.split()[0] + " " + s.split()[2] if k != "insert" else s) for k, s, _ in client.calls]
        assert kinds[0][0] == "execute"  # run insert
        assert any(k == "insert" for k, _ in kinds)  # audit flushed
        # The task UPDATE must precede the run UPDATE.
        statements = client.statements
        task_update = next(i for i, s in enumerate(statements) if s.startswith("UPDATE prod_lakehouse.control.ingestion_tasks"))
        run_update = next(i for i, s in enumerate(statements) if s.startswith("UPDATE prod_lakehouse.control.ingestion_runs"))
        assert task_update < run_update
