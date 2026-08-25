from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path

import pytest

from ingestion_framework.control.watermark import WatermarkWindow
from ingestion_framework.engine.extractor import (
    Credentials,
    ExtractionError,
    OracleExtractor,
)
from ingestion_framework.engine.run_spec import RunSpec

from .test_sql_builder import LOWER, spec

CREDS = Credentials("svc_ingest", "hunter2")
WINDOW = WatermarkWindow(
    lower_bound=LOWER, stored_value=LOWER, overlap_applied=True, is_first_run=False
)
NO_WINDOW = WatermarkWindow(
    lower_bound=None, stored_value=None, overlap_applied=False, is_first_run=True
)


class FakeSecrets:
    def __init__(self, values=None):
        self.values = values or {("oracle-dev", "username"): "svc_ingest",
                                 ("oracle-dev", "password"): "hunter2"}
        self.requested: list[tuple[str, str]] = []

    def get(self, scope: str, key: str) -> str:
        self.requested.append((scope, key))
        try:
            return self.values[(scope, key)]
        except KeyError:
            raise ExtractionError(f"secret {scope}/{key} not found")


class FakeRow(dict):
    def __getitem__(self, key):
        return super().__getitem__(key)


class FakeDataFrame:
    def __init__(self, rows=None, options=None):
        self.rows = rows or []
        self.options = options or {}

    def collect(self):
        return [FakeRow(r) for r in self.rows]


class FakeReader:
    """Stands in for the JDBC read: records options, replays canned results."""

    def __init__(self, results: dict[str, list[dict]] | None = None):
        self.calls: list[dict] = []
        self.results = results or {}

    def __call__(self, options):
        self.calls.append(dict(options))
        sql = str(options.get("query") or options.get("dbtable") or "")
        for needle, rows in self.results.items():
            if needle in sql:
                return FakeDataFrame(rows, options)
        return FakeDataFrame([], options)

    @property
    def queries(self) -> list[str]:
        return [str(c.get("query") or c.get("dbtable") or "") for c in self.calls]

    def query_matching(self, needle: str) -> str:
        for q in self.queries:
            if needle in q:
                return q
        raise AssertionError(f"no query matching {needle!r} in {self.queries}")


def make_extractor(results=None, secrets=None, config_root=None):
    reader = FakeReader(results)
    extractor = OracleExtractor(
        spark=None,
        secrets=secrets or FakeSecrets(),
        config_root=config_root,
        reader=reader,
    )
    return extractor, reader


class TestCredentials:
    def test_reads_from_the_configured_scope(self):
        secrets = FakeSecrets()
        extractor, _ = make_extractor(secrets=secrets)
        creds = extractor.credentials(spec())
        assert creds == CREDS
        assert secrets.requested == [("oracle-dev", "username"), ("oracle-dev", "password")]

    def test_custom_secret_keys_are_honoured(self):
        secrets = FakeSecrets({("oracle-dev", "ora_user"): "u", ("oracle-dev", "ora_pw"): "p"})
        extractor, _ = make_extractor(secrets=secrets)
        s = spec(source__secret_keys={"username": "ora_user", "password": "ora_pw"})
        assert extractor.credentials(s) == Credentials("u", "p")

    def test_missing_scope_is_refused(self):
        extractor, _ = make_extractor()
        s = spec()
        object.__setattr__(s.source, "secret_scope", None)
        with pytest.raises(ExtractionError, match="secret_scope is not set"):
            extractor.credentials(s)

    def test_no_provider_is_refused(self):
        extractor = OracleExtractor(spark=None, secrets=None, reader=FakeReader())
        with pytest.raises(ExtractionError, match="no secret provider"):
            extractor.credentials(spec())


class TestJdbcOptions:
    def test_single_threaded_read_uses_the_query_option(self):
        extractor, _ = make_extractor()
        query = extractor.build_query(spec(), WINDOW)
        options = extractor.jdbc_options(spec(), query, CREDS)
        assert options["query"] == query.sql
        assert "dbtable" not in options
        assert "numPartitions" not in options

    def test_parallel_read_uses_dbtable_because_spark_forbids_query(self):
        # Spark rejects 'query' together with 'partitionColumn'; getting this
        # wrong surfaces as a confusing engine error at runtime.
        s = spec(source__read={"num_partitions": 16, "partition_column": "TXN_ID"})
        extractor, _ = make_extractor()
        query = extractor.build_query(s, WINDOW)
        options = extractor.jdbc_options(s, query, CREDS, partition_bounds=(1, 1000))
        assert "query" not in options
        assert options["dbtable"] == query.as_subquery()
        assert options["partitionColumn"] == "TXN_ID"
        assert (options["lowerBound"], options["upperBound"]) == (1, 1000)
        assert options["numPartitions"] == 16

    def test_empty_batch_falls_back_to_a_single_partition(self):
        s = spec(source__read={"num_partitions": 16, "partition_column": "TXN_ID"})
        extractor, _ = make_extractor()
        query = extractor.build_query(s, WINDOW)
        options = extractor.jdbc_options(s, query, CREDS, partition_bounds=(None, None))
        assert "query" in options and "numPartitions" not in options

    def test_core_connection_options(self):
        extractor, _ = make_extractor()
        options = extractor.jdbc_options(spec(), extractor.build_query(spec(), WINDOW), CREDS)
        assert options["url"] == "jdbc:oracle:thin:@//dev:1521/DEV"
        assert options["driver"] == "oracle.jdbc.OracleDriver"
        assert options["user"] == "svc_ingest"
        assert options["password"] == "hunter2"

    def test_session_init_statement_is_passed_through(self):
        s = spec(source__jdbc={
            "url": "jdbc:oracle:thin:@//dev:1521/DEV",
            "session_init_statement": "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD'",
        })
        extractor, _ = make_extractor()
        options = extractor.jdbc_options(s, extractor.build_query(s, WINDOW), CREDS)
        assert "NLS_DATE_FORMAT" in options["sessionInitStatement"]

    def test_fetch_size_is_applied(self):
        s = spec(source__fetch_size=50000)
        extractor, _ = make_extractor()
        assert extractor.jdbc_options(s, extractor.build_query(s, WINDOW), CREDS)["fetchsize"] == 50000

    def test_extra_options_override_framework_defaults(self):
        s = spec(source__jdbc={
            "url": "jdbc:oracle:thin:@//dev:1521/DEV",
            "options": {"oracle.jdbc.timezoneAsRegion": "false", "fetchsize": 1},
        })
        extractor, _ = make_extractor()
        options = extractor.jdbc_options(s, extractor.build_query(s, WINDOW), CREDS)
        assert options["oracle.jdbc.timezoneAsRegion"] == "false"
        assert options["fetchsize"] == 1


class TestProbes:
    def test_upper_bound_is_pinned_from_the_source_clock(self):
        stamp = datetime(2026, 8, 24, 10, 0, 0)
        extractor, reader = make_extractor({"SYSTIMESTAMP": [{"UPPER_BOUND": stamp}]})
        assert extractor.probe_upper_bound(spec(), CREDS) == "2026-08-24 10:00:00.000000"
        assert "SYSTIMESTAMP" in reader.queries[0]

    def test_upper_bound_skipped_when_disabled(self):
        s = spec(extraction__incremental={
            "strategy": "watermark", "watermark_column": "LAST_UPDATE_DATE",
            "watermark_type": "timestamp", "use_upper_bound": False})
        extractor, reader = make_extractor()
        assert extractor.probe_upper_bound(s, CREDS) is None
        assert reader.calls == []

    def test_upper_bound_skipped_for_full_extracts(self):
        s = spec(extraction__mode="full", extraction__incremental={})
        extractor, reader = make_extractor()
        assert extractor.probe_upper_bound(s, CREDS) is None

    def test_partition_bounds_probed_over_the_filtered_query(self):
        s = spec(source__read={"num_partitions": 8, "partition_column": "TXN_ID"},
                 extraction__filter="STATUS = 'A'")
        extractor, reader = make_extractor(
            {"MIN(TXN_ID)": [{"LOWER_BOUND": 1, "UPPER_BOUND": 999}]}
        )
        query = extractor.build_query(s, WINDOW)
        assert extractor.probe_partition_bounds(s, query, CREDS) == (1, 999)
        assert "STATUS = 'A'" in reader.query_matching("MIN(TXN_ID)")

    def test_explicit_bounds_skip_the_probe(self):
        s = spec(source__read={
            "num_partitions": 8, "partition_column": "TXN_ID",
            "bounds_strategy": "explicit", "lower_bound": 1, "upper_bound": 100})
        extractor, reader = make_extractor()
        assert extractor.probe_partition_bounds(s, extractor.build_query(s, WINDOW), CREDS) == (1, 100)
        assert reader.calls == []

    def test_explicit_bounds_without_values_is_refused(self):
        s = spec(source__read={
            "num_partitions": 8, "partition_column": "TXN_ID", "bounds_strategy": "explicit"})
        extractor, _ = make_extractor()
        with pytest.raises(ExtractionError, match="lower_bound/upper_bound"):
            extractor.probe_partition_bounds(s, extractor.build_query(s, WINDOW), CREDS)

    def test_no_probe_for_single_threaded_reads(self):
        extractor, reader = make_extractor()
        assert extractor.probe_partition_bounds(spec(), extractor.build_query(spec(), WINDOW), CREDS) is None
        assert reader.calls == []

    def test_source_count_for_reconciliation(self):
        extractor, reader = make_extractor({"COUNT(*)": [{"SOURCE_COUNT": 4242}]})
        query = extractor.build_query(spec(), WINDOW)
        assert extractor.count_source(spec(), query, CREDS) == 4242

    def test_source_count_skipped_when_reconciliation_is_off(self):
        s = spec(quality={"row_count_reconciliation": False})
        extractor, reader = make_extractor()
        assert extractor.count_source(s, extractor.build_query(s, WINDOW), CREDS) is None
        assert reader.calls == []

    def test_max_watermark_is_canonicalised(self):
        extractor, _ = make_extractor(
            {"MAX(LAST_UPDATE_DATE)": [{"MAX_WATERMARK": datetime(2026, 8, 24, 11, 30)}]}
        )
        query = extractor.build_query(spec(), WINDOW)
        assert extractor.probe_max_watermark(spec(), query, CREDS) == "2026-08-24 11:30:00.000000"


class TestExtract:
    def results(self, count=100, upper=datetime(2026, 8, 24, 10, 0, 0)):
        return {
            "SYSTIMESTAMP": [{"UPPER_BOUND": upper}],
            "COUNT(*)": [{"SOURCE_COUNT": count}],
        }

    def test_end_to_end_incremental_extract(self):
        extractor, reader = make_extractor(self.results())
        result = extractor.extract(spec(), WINDOW)
        assert result.lower_bound == LOWER
        assert result.upper_bound == "2026-08-24 10:00:00.000000"
        assert result.source_count == 100
        assert "LAST_UPDATE_DATE >= TO_TIMESTAMP" in result.query.sql
        assert "LAST_UPDATE_DATE < TO_TIMESTAMP" in result.query.sql

    def test_new_watermark_prefers_the_pinned_upper_bound(self):
        # The pinned bound is the exact cut this read was taken at, so the next
        # run resumes exactly where this one stopped.
        extractor, reader = make_extractor(self.results())
        result = extractor.extract(spec(), WINDOW)
        assert result.new_watermark == "2026-08-24 10:00:00.000000"
        assert not any("MAX(LAST_UPDATE_DATE)" in q for q in reader.queries)

    def test_new_watermark_falls_back_to_the_data(self):
        s = spec(extraction__incremental={
            "strategy": "watermark", "watermark_column": "LAST_UPDATE_DATE",
            "watermark_type": "timestamp", "use_upper_bound": False})
        extractor, reader = make_extractor({
            "COUNT(*)": [{"SOURCE_COUNT": 5}],
            "MAX(LAST_UPDATE_DATE)": [{"MAX_WATERMARK": datetime(2026, 8, 24, 9, 0)}],
        })
        result = extractor.extract(s, WINDOW)
        assert result.new_watermark == "2026-08-24 09:00:00.000000"

    def test_empty_batch_reports_itself(self):
        s = spec(extraction__incremental={
            "strategy": "watermark", "watermark_column": "LAST_UPDATE_DATE",
            "watermark_type": "timestamp", "use_upper_bound": False})
        extractor, _ = make_extractor({"COUNT(*)": [{"SOURCE_COUNT": 0}]})
        result = extractor.extract(s, WINDOW)
        assert result.is_empty_batch  # no candidate -> the watermark must hold

    def test_full_extract_has_no_bounds(self):
        s = spec(extraction__mode="full", extraction__columns="*", extraction__incremental={})
        extractor, _ = make_extractor({"COUNT(*)": [{"SOURCE_COUNT": 7}]})
        result = extractor.extract(s, NO_WINDOW)
        assert result.lower_bound is None and result.upper_bound is None
        assert result.new_watermark is None

    def test_parallel_extract_records_partitioning(self):
        s = spec(source__read={"num_partitions": 16, "partition_column": "TXN_ID"})
        extractor, _ = make_extractor({
            **self.results(),
            "MIN(TXN_ID)": [{"LOWER_BOUND": 1, "UPPER_BOUND": 5000}],
        })
        result = extractor.extract(s, WINDOW)
        assert result.partition_bounds == (1, 5000)
        assert result.num_partitions == 16

    def test_recorded_options_never_carry_the_password(self):
        extractor, _ = make_extractor(self.results())
        result = extractor.extract(spec(), WINDOW)
        assert result.options["password"] == "***"
        assert result.options["user"] == "***"
        assert "hunter2" not in str(result.options)


class TestQueryFile:
    def test_reads_the_file_relative_to_the_config_root(self, tmp_path: Path):
        (tmp_path / "sql").mkdir()
        (tmp_path / "sql" / "custom.sql").write_text(
            "SELECT * FROM T WHERE UPD >= :lower_bound", encoding="utf-8"
        )
        s = spec(extraction__mode="query", extraction__columns="*",
                 extraction__query_file="sql/custom.sql", extraction__filter=None)
        extractor, _ = make_extractor(config_root=tmp_path)
        query = extractor.build_query(s, WINDOW)
        assert f"UPD >= TO_TIMESTAMP('{LOWER}'" in query.sql

    def test_missing_file_is_reported_with_the_path(self, tmp_path: Path):
        s = spec(extraction__mode="query", extraction__columns="*",
                 extraction__query_file="sql/nope.sql", extraction__filter=None)
        extractor, _ = make_extractor(config_root=tmp_path)
        with pytest.raises(ExtractionError, match="query file not found"):
            extractor.build_query(s, WINDOW)
