from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from ingestion_framework.control.sql_client import RecordingSqlClient
from ingestion_framework.control.watermark import (
    WatermarkError,
    WatermarkRecord,
    WatermarkStore,
    advance_guard_sql,
    canonicalize,
    compute_window,
    parse,
    should_advance,
)

NOW = datetime(2026, 8, 24, 10, 0, 0)


def record(value, wtype="timestamp", run_id="run-1"):
    return WatermarkRecord(
        table_fqn="finance.gl_transactions",
        env="prod",
        watermark_column="LAST_UPDATE_DATE",
        watermark_type=wtype,
        watermark_value=value,
        run_id=run_id,
    )


class TestCanonicalize:
    def test_datetime(self):
        assert canonicalize(datetime(2026, 8, 24, 10, 30, 5), "timestamp") == "2026-08-24 10:30:05.000000"

    def test_date_type_drops_time(self):
        assert canonicalize(datetime(2026, 8, 24, 10, 30), "date") == "2026-08-24"

    def test_string_input_is_normalised(self):
        assert canonicalize("2026-08-24T10:30:05", "timestamp") == "2026-08-24 10:30:05.000000"

    def test_number(self):
        assert canonicalize(12345, "number") == "12345"
        assert canonicalize(Decimal("99.5"), "number") == "99.5"

    def test_none_stays_none(self):
        assert canonicalize(None, "timestamp") is None

    def test_unknown_type_rejected(self):
        with pytest.raises(WatermarkError, match="unknown watermark_type"):
            canonicalize(1, "guess")

    def test_unparseable_timestamp_rejected(self):
        with pytest.raises(WatermarkError, match="not a recognised timestamp"):
            canonicalize("24/08/2026", "timestamp")

    def test_non_numeric_rejected(self):
        with pytest.raises(WatermarkError, match="not a valid numeric"):
            canonicalize("abc", "number")

    def test_round_trip(self):
        for value, wtype in [
            (datetime(2026, 1, 2, 3, 4, 5, 678901), "timestamp"),
            (date(2026, 1, 2), "date"),
            (Decimal("42"), "number"),
        ]:
            assert parse(canonicalize(value, wtype), wtype) == value


class TestShouldAdvance:
    def test_first_ever_value_advances(self):
        assert should_advance(None, "2026-08-24 10:00:00.000000", "timestamp")

    def test_newer_advances(self):
        assert should_advance("2026-08-24 10:00:00.000000", "2026-08-24 11:00:00.000000", "timestamp")

    def test_older_does_not(self):
        assert not should_advance("2026-08-24 11:00:00.000000", "2026-08-24 10:00:00.000000", "timestamp")

    def test_equal_does_not(self):
        same = "2026-08-24 10:00:00.000000"
        assert not should_advance(same, same, "timestamp")

    def test_empty_batch_holds_the_mark(self):
        # No rows extracted -> no candidate -> the watermark must not move.
        assert not should_advance("2026-08-24 10:00:00.000000", None, "timestamp")

    def test_numeric_compares_numerically_not_lexically(self):
        # '9' > '10' as strings; as SCNs it is the other way round.
        assert should_advance("9", "10", "number")
        assert not should_advance("10", "9", "number")


class TestComputeWindow:
    def test_first_run_uses_configured_default(self):
        window = compute_window(
            None, watermark_type="timestamp", lower_bound_default="1900-01-01 00:00:00"
        )
        assert window.is_first_run
        assert window.lower_bound == "1900-01-01 00:00:00.000000"
        assert not window.overlap_applied

    def test_first_run_without_default_has_no_bound(self):
        window = compute_window(None, watermark_type="timestamp")
        assert window.is_first_run and not window.has_bound

    def test_overlap_rewinds_the_stored_mark(self):
        window = compute_window(
            record("2026-08-24 10:00:00.000000"),
            watermark_type="timestamp",
            overlap=timedelta(hours=6),
        )
        assert window.lower_bound == "2026-08-24 04:00:00.000000"
        assert window.stored_value == "2026-08-24 10:00:00.000000"
        assert window.overlap_applied

    def test_zero_overlap_uses_the_mark_as_is(self):
        window = compute_window(record("2026-08-24 10:00:00.000000"), watermark_type="timestamp")
        assert window.lower_bound == "2026-08-24 10:00:00.000000"
        assert not window.overlap_applied

    def test_date_overlap_rewinds_whole_days(self):
        window = compute_window(
            record("2026-08-24", "date"), watermark_type="date", overlap=timedelta(days=2)
        )
        assert window.lower_bound == "2026-08-22"

    def test_overlap_is_not_applied_to_numeric_watermarks(self):
        # A duration has no meaning against an SCN; the window reports that it
        # was not applied rather than inventing an arithmetic.
        window = compute_window(
            record("500000", "number"), watermark_type="number", overlap=timedelta(hours=6)
        )
        assert window.lower_bound == "500000"
        assert not window.overlap_applied


class TestAdvanceGuardSql:
    @pytest.mark.parametrize(
        "wtype,cast",
        [("timestamp", "TO_TIMESTAMP"), ("date", "TO_DATE"), ("number", "CAST")],
    )
    def test_guard_is_type_aware(self, wtype, cast):
        guard = advance_guard_sql(wtype)
        assert cast in guard
        assert "s.watermark_value" in guard and "t.watermark_value" in guard
        assert ">" in guard

    def test_null_stored_value_is_beaten_by_anything(self):
        assert "t.watermark_value IS NULL OR" in advance_guard_sql("timestamp")

    def test_unknown_type_rejected(self):
        with pytest.raises(WatermarkError):
            advance_guard_sql("guess")


class TestWatermarkStore:
    def make(self, results=None):
        client = RecordingSqlClient(results)
        return client, WatermarkStore(client, "prod_lakehouse", "control")

    def test_table_is_three_level(self):
        _, store = self.make()
        assert store.table == "prod_lakehouse.control.watermarks"

    def test_get_binds_parameters_rather_than_formatting(self):
        client, store = self.make([[{"table_fqn": "finance.gl", "env": "prod", "watermark_value": "x"}]])
        got = store.get("finance.gl", "prod")
        assert got and got.watermark_value == "x"
        statement, params = client.calls[0][1], client.calls[0][2]
        assert ":table_fqn" in statement and "finance.gl" not in statement
        assert params == {"table_fqn": "finance.gl", "env": "prod"}

    def test_get_returns_none_when_absent(self):
        _, store = self.make()
        assert store.get("finance.gl", "prod") is None

    def test_advance_emits_a_guarded_merge(self):
        client, store = self.make([[]])
        store.advance(
            "finance.gl", "prod",
            new_value=datetime(2026, 8, 24, 11, 0),
            run_id="prod-1",
            watermark_column="LAST_UPDATE_DATE",
            watermark_type="timestamp",
            updated_at=NOW,
        )
        merge = client.statements_matching("MERGE INTO")[0]
        assert "WHEN MATCHED AND (t.watermark_value IS NULL OR TO_TIMESTAMP" in merge
        assert "t.previous_value = t.watermark_value" in merge
        assert "WHEN NOT MATCHED THEN INSERT" in merge

    def test_advance_canonicalises_the_value_it_binds(self):
        client, store = self.make([[]])
        store.advance(
            "finance.gl", "prod",
            new_value="2026-08-24T11:00:00",
            run_id="prod-1",
            watermark_column="LAST_UPDATE_DATE",
            watermark_type="timestamp",
            updated_at=NOW,
        )
        assert client.params_for("MERGE INTO")["watermark_value"] == "2026-08-24 11:00:00.000000"

    def test_advance_with_no_candidate_writes_nothing(self):
        client, store = self.make()
        assert store.advance(
            "finance.gl", "prod",
            new_value=None,
            run_id="prod-1",
            watermark_column="LAST_UPDATE_DATE",
            watermark_type="timestamp",
            updated_at=NOW,
        ) is False
        assert client.calls == []

    def test_advance_reports_true_only_when_this_run_owns_the_mark(self):
        moved_value = "2026-08-24 11:00:00.000000"
        client, store = self.make([[{
            "table_fqn": "finance.gl", "env": "prod", "watermark_value": moved_value,
            "watermark_type": "timestamp", "run_id": "prod-1",
        }]])
        assert store.advance(
            "finance.gl", "prod", new_value=moved_value, run_id="prod-1",
            watermark_column="C", watermark_type="timestamp", updated_at=NOW,
        )

    def test_advance_reports_false_when_the_guard_rejected_it(self):
        # Another run already moved the mark further ahead; ours was a no-op.
        client, store = self.make([[{
            "table_fqn": "finance.gl", "env": "prod",
            "watermark_value": "2026-08-24 12:00:00.000000",
            "watermark_type": "timestamp", "run_id": "prod-2",
        }]])
        assert not store.advance(
            "finance.gl", "prod", new_value="2026-08-24 11:00:00.000000", run_id="prod-1",
            watermark_column="C", watermark_type="timestamp", updated_at=NOW,
        )

    def test_force_set_has_no_monotonic_guard(self):
        client, store = self.make()
        store.force_set(
            "finance.gl", "prod", value="2020-01-01 00:00:00", watermark_type="timestamp",
            watermark_column="C", run_id="backfill-1", updated_at=NOW,
        )
        merge = client.statements_matching("MERGE INTO")[0]
        assert "WHEN MATCHED THEN UPDATE" in merge
        assert "TO_TIMESTAMP(s.watermark_value)" not in merge
        assert "t.previous_value = t.watermark_value" in merge

    def test_window_reads_through_to_the_store(self):
        client, store = self.make([[{
            "table_fqn": "finance.gl", "env": "prod",
            "watermark_value": "2026-08-24 10:00:00.000000",
            "watermark_type": "timestamp", "run_id": "prod-1",
        }]])
        window = store.window(
            "finance.gl", "prod", watermark_type="timestamp", overlap=timedelta(hours=6)
        )
        assert window.lower_bound == "2026-08-24 04:00:00.000000"
