from __future__ import annotations

import json
from datetime import datetime
from itertools import count

import pytest

from ingestion_framework.control.audit import AuditEvent, AuditLog, EventType
from ingestion_framework.control.schema import AUDIT_LOG
from ingestion_framework.control.sql_client import RecordingSqlClient

NOW = datetime(2026, 8, 24, 10, 0, 0)


@pytest.fixture
def client() -> RecordingSqlClient:
    return RecordingSqlClient()


@pytest.fixture
def audit(client: RecordingSqlClient) -> AuditLog:
    ids = count(1)
    return AuditLog(
        client, "prod_lakehouse", "control",
        run_id="prod-1", env="prod", actor="sp-ingest",
        now=lambda: NOW, id_factory=lambda: f"evt-{next(ids)}",
    )


class TestEmit:
    def test_table_is_three_level(self, audit):
        assert audit.table == "prod_lakehouse.control.audit_log"

    def test_bound_context_is_applied(self, audit):
        event = audit.emit(EventType.TASK_STARTED, table_fqn="finance.gl")
        assert event.run_id == "prod-1"
        assert event.env == "prod"
        assert event.actor == "sp-ingest"
        assert event.table_fqn == "finance.gl"
        assert event.event_ts == NOW

    def test_event_type_accepts_enum_or_string(self, audit):
        assert audit.emit(EventType.LOAD_DONE).event_type == "LOAD_DONE"
        assert audit.emit("CUSTOM_EVENT").event_type == "CUSTOM_EVENT"

    def test_sequence_increments_for_stable_replay(self, audit):
        events = [audit.emit(EventType.EXTRACT_STARTED) for _ in range(3)]
        assert [e.sequence for e in events] == [1, 2, 3]

    def test_bind_updates_context_later(self, client):
        audit = AuditLog(client, "cat", "control", now=lambda: NOW, id_factory=lambda: "e")
        audit.bind(run_id="prod-2", env="prod", actor="me")
        event = audit.emit(EventType.RUN_STARTED)
        assert (event.run_id, event.env, event.actor) == ("prod-2", "prod", "me")

    def test_payload_serialises_as_sorted_json(self, audit):
        event = audit.emit(
            EventType.EXTRACT_DONE, payload={"rows": 10, "bounds": {"lower": "2026-01-01"}}
        )
        payload = json.loads(event.to_row()["payload"])
        assert payload == {"rows": 10, "bounds": {"lower": "2026-01-01"}}
        assert event.to_row()["payload"].index('"bounds"') < event.to_row()["payload"].index('"rows"')

    def test_payload_handles_non_json_types(self, audit):
        event = audit.emit(EventType.WATERMARK_ADVANCED, payload={"at": NOW})
        assert "2026-08-24 10:00:00" in event.to_row()["payload"]


class TestBuffering:
    def test_events_are_buffered_not_written_immediately(self, client, audit):
        audit.emit(EventType.TASK_STARTED)
        assert audit.pending == 1
        assert client.inserted == []

    def test_flush_writes_one_batch(self, client, audit):
        for _ in range(3):
            audit.emit(EventType.EXTRACT_STARTED)
        assert audit.flush() == 3
        # One commit for three events, not three.
        assert len(client.inserted) == 1
        assert len(client.inserted[0][1]) == 3
        assert audit.pending == 0

    def test_flush_is_a_no_op_when_empty(self, client, audit):
        assert audit.flush() == 0
        assert client.inserted == []

    def test_explicit_flush_flag_writes_through(self, client, audit):
        audit.emit(EventType.TASK_SUCCEEDED, flush=True)
        assert len(client.inserted) == 1
        assert audit.pending == 0

    def test_buffer_limit_forces_a_flush(self, client):
        audit = AuditLog(
            client, "cat", "control", now=lambda: NOW, id_factory=lambda: "e", buffer_limit=2
        )
        audit.emit(EventType.EXTRACT_STARTED)
        assert client.inserted == []
        audit.emit(EventType.EXTRACT_DONE)
        assert len(client.inserted) == 1

    def test_context_manager_flushes_on_exception(self, client, audit):
        # The events leading up to a failure are exactly the ones worth keeping.
        with pytest.raises(RuntimeError):
            with audit:
                audit.emit(EventType.TASK_STARTED)
                raise RuntimeError("extract blew up")
        assert len(client.inserted) == 1
        assert client.inserted[0][1][0]["event_type"] == "TASK_STARTED"

    def test_written_rows_match_the_table_columns(self, client, audit):
        audit.emit(EventType.TASK_STARTED, table_fqn="finance.gl", flush=True)
        table, rows = client.inserted[0]
        assert table == "prod_lakehouse.control.audit_log"
        assert set(rows[0]) == set(AUDIT_LOG.column_names)


class TestEventTypes:
    def test_design_lifecycle_events_exist(self):
        required = {
            "TASK_STARTED", "EXTRACT_DONE", "LOAD_DONE",
            "WATERMARK_ADVANCED", "TASK_SUCCEEDED", "TASK_FAILED",
        }
        assert required <= {e.value for e in EventType}

    def test_watermark_held_is_distinct_from_advanced(self):
        # An empty batch holds the mark; that is a real outcome, not a failure,
        # and the audit trail should say which of the two happened.
        assert EventType.WATERMARK_HELD != EventType.WATERMARK_ADVANCED

    def test_event_is_frozen(self):
        event = AuditEvent(event_id="e", event_type="X", event_ts=NOW)
        with pytest.raises(AttributeError):
            event.event_type = "Y"  # type: ignore[misc]
