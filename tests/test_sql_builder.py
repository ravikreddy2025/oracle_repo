from __future__ import annotations

import copy

import pytest

from ingestion_framework.engine.run_spec import RunSpec
from ingestion_framework.engine.sql_builder import (
    SqlBuildError,
    build_bounds_query,
    build_count_query,
    build_max_watermark_query,
    build_predicates,
    build_projection,
    build_source_query,
    build_upper_bound_query,
    identifier,
    literal,
    lower_bound_is_inclusive,
    render_query_template,
)

from .test_validator import BASE

LOWER = "2026-08-24 04:00:00.000000"
UPPER = "2026-08-24 10:00:00.000000"


def spec(**patch) -> RunSpec:
    """A RunSpec from BASE with a deep patch applied (``a__b=value``)."""
    cfg = copy.deepcopy(BASE)
    for dotted, value in patch.items():
        node = cfg
        parts = dotted.split("__")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return RunSpec.from_config(cfg)


class TestLiteral:
    def test_timestamp_renders_with_explicit_format(self):
        assert literal(LOWER, "timestamp") == f"TO_TIMESTAMP('{LOWER}', 'YYYY-MM-DD HH24:MI:SS.FF6')"

    def test_timestamp_without_fraction_is_padded(self):
        assert "2026-08-24 04:00:00.000000" in literal("2026-08-24 04:00:00", "timestamp")

    def test_date_renders_as_to_date(self):
        assert literal("2026-08-24", "date") == "TO_DATE('2026-08-24', 'YYYY-MM-DD')"

    def test_number_renders_bare(self):
        assert literal("123456", "number") == "123456"

    @pytest.mark.parametrize(
        "bad",
        [
            "2026-08-24' OR '1'='1",
            "24/08/2026",
            "2026-08-24T04:00:00",  # not canonical: canonicalize() produces a space
            "SYSDATE",
        ],
    )
    def test_non_canonical_timestamp_is_refused(self, bad):
        # This value cannot be bound as a parameter on the Spark JDBC path, so
        # anything that is not exactly canonical must not reach the SQL string.
        with pytest.raises(SqlBuildError, match="refusing to build SQL"):
            literal(bad, "timestamp")

    def test_non_numeric_is_refused(self):
        with pytest.raises(SqlBuildError, match="refusing to build SQL"):
            literal("1; DROP TABLE X", "number")

    def test_null_bound_is_refused(self):
        with pytest.raises(SqlBuildError, match="NULL bound"):
            literal(None, "timestamp")

    @pytest.mark.parametrize("bad", ["a b", "x;y", "a-b", "", "1col"])
    def test_identifier_rejects_unsafe_names(self, bad):
        with pytest.raises(SqlBuildError):
            identifier(bad)

    def test_identifier_allows_oracle_specials_and_quoting(self):
        assert identifier("MY_COL$#") == "MY_COL$#"
        assert identifier('"Mixed Case"') == '"Mixed Case"'


class TestBoundInclusivity:
    def test_merge_target_defaults_to_inclusive(self):
        # Re-reading the boundary row is harmless when the load is idempotent,
        # and it cannot lose a row that shares the boundary timestamp.
        assert lower_bound_is_inclusive(spec(target__write_mode="merge"))

    def test_append_target_defaults_to_exclusive(self):
        # Inclusive would duplicate the boundary rows on every run.
        assert not lower_bound_is_inclusive(spec(target__write_mode="append"))

    def test_explicit_setting_overrides_inference(self):
        s = spec(target__write_mode="append")
        s = spec(target__write_mode="append", extraction__incremental={
            **BASE["extraction"]["incremental"], "bound_inclusive": True})
        assert lower_bound_is_inclusive(s)

    def test_operator_follows_the_decision(self):
        merge_sql = build_source_query(spec(), lower_bound=LOWER).sql
        assert "LAST_UPDATE_DATE >= TO_TIMESTAMP" in merge_sql
        append_sql = build_source_query(spec(target__write_mode="append"), lower_bound=LOWER).sql
        assert "LAST_UPDATE_DATE > TO_TIMESTAMP" in append_sql
        assert ">=" not in append_sql


class TestProjection:
    def test_star(self):
        projection, _ = build_projection(spec(extraction__columns="*"))
        assert projection == "*"

    def test_explicit_columns_in_order(self):
        projection, _ = build_projection(spec())
        assert projection == "TXN_ID, AMOUNT, LAST_UPDATE_DATE"

    def test_exclude_columns_removes_from_the_list(self):
        projection, _ = build_projection(spec(extraction__exclude_columns=["AMOUNT"]))
        assert projection == "TXN_ID, LAST_UPDATE_DATE"

    def test_excluding_everything_is_an_error(self):
        with pytest.raises(SqlBuildError, match="nothing left to select"):
            build_projection(spec(extraction__exclude_columns=["TXN_ID", "AMOUNT", "LAST_UPDATE_DATE"]))

    def test_scn_pseudocolumn_is_added_for_scn_strategy(self):
        # ORA_ROWSCN is not part of SELECT *, and it is both the predicate and
        # the next watermark -- it has to be projected explicitly.
        s = spec(extraction__columns="*", extraction__incremental={"strategy": "scn", "watermark_type": "number"})
        projection, _ = build_projection(s)
        assert projection == "*, ORA_ROWSCN AS ORA_ROWSCN"

    def test_scn_column_not_added_for_watermark_strategy(self):
        projection, _ = build_projection(spec())
        assert "ORA_ROWSCN" not in projection


class TestFullExtract:
    def test_selects_all_rows_and_columns(self):
        s = spec(extraction__mode="full", extraction__columns="*", extraction__incremental={})
        query = build_source_query(s)
        assert query.sql == "SELECT *\nFROM GLOWNER.GL_TRANSACTIONS"
        assert query.mode == "full"
        assert not query.is_bounded

    def test_full_extract_ignores_watermark_bounds(self):
        s = spec(extraction__mode="full", extraction__columns="*", extraction__incremental={})
        assert "WHERE" not in build_source_query(s, lower_bound=LOWER).sql

    def test_selective_columns_only(self):
        s = spec(extraction__mode="full", extraction__incremental={})
        assert build_source_query(s).sql.startswith("SELECT TXN_ID, AMOUNT, LAST_UPDATE_DATE\nFROM")

    def test_selective_rows_only(self):
        s = spec(extraction__mode="full", extraction__incremental={}, extraction__filter="STATUS = 'A'")
        assert build_source_query(s).sql.endswith("WHERE (STATUS = 'A')")

    def test_row_limit_uses_12c_syntax(self):
        s = spec(extraction__mode="full", extraction__incremental={}, extraction__row_limit=1000)
        assert build_source_query(s).sql.endswith("FETCH FIRST 1000 ROWS ONLY")


class TestIncrementalExtract:
    def test_lower_bound_only(self):
        query = build_source_query(spec(), lower_bound=LOWER)
        assert f"WHERE LAST_UPDATE_DATE >= TO_TIMESTAMP('{LOWER}'" in query.sql
        assert query.lower_bound == LOWER

    def test_lower_and_upper_bound(self):
        query = build_source_query(spec(), lower_bound=LOWER, upper_bound=UPPER)
        assert ">= TO_TIMESTAMP" in query.sql
        assert "< TO_TIMESTAMP" in query.sql
        assert query.upper_bound == UPPER

    def test_upper_bound_is_always_exclusive(self):
        # It becomes the next run's lower bound; inclusive on both ends would
        # double-count the boundary row every single run.
        sql = build_source_query(spec(), lower_bound=LOWER, upper_bound=UPPER).sql
        assert f"LAST_UPDATE_DATE < TO_TIMESTAMP('{UPPER}'" in sql
        assert "<=" not in sql

    def test_filter_is_anded_with_the_watermark(self):
        s = spec(extraction__filter="STATUS <> 'DELETED'")
        sql = build_source_query(s, lower_bound=LOWER).sql
        assert "WHERE LAST_UPDATE_DATE >=" in sql
        assert "AND (STATUS <> 'DELETED')" in sql

    def test_first_run_without_a_bound_reads_everything(self):
        assert "WHERE" not in build_source_query(spec()).sql

    def test_scn_predicate_uses_the_pseudocolumn(self):
        s = spec(
            extraction__columns="*",
            extraction__incremental={"strategy": "scn", "watermark_type": "number"},
        )
        sql = build_source_query(s, lower_bound="500000", upper_bound="600000").sql
        assert "ORA_ROWSCN >= 500000" in sql or "ORA_ROWSCN > 500000" in sql
        assert "ORA_ROWSCN < 600000" in sql

    def test_date_watermark_uses_to_date(self):
        s = spec(extraction__incremental={
            "strategy": "watermark", "watermark_column": "LAST_UPDATE_DATE", "watermark_type": "date"})
        assert "TO_DATE('2026-08-24', 'YYYY-MM-DD')" in build_source_query(s, lower_bound="2026-08-24").sql

    def test_missing_watermark_column_is_refused(self):
        s = spec(extraction__incremental={"strategy": "watermark", "watermark_column": None})
        with pytest.raises(SqlBuildError, match="needs a watermark column"):
            build_source_query(s, lower_bound=LOWER)

    def test_predicate_order_is_stable(self):
        # Stable text means the query hash is stable, which makes plan reuse and
        # log diffing meaningful.
        s = spec(extraction__filter="STATUS = 'A'")
        first = build_source_query(s, lower_bound=LOWER, upper_bound=UPPER).sql
        second = build_source_query(s, lower_bound=LOWER, upper_bound=UPPER).sql
        assert first == second
        assert build_predicates(s, lower_bound=LOWER, upper_bound=UPPER) == [
            f"LAST_UPDATE_DATE >= TO_TIMESTAMP('{LOWER}', 'YYYY-MM-DD HH24:MI:SS.FF6')",
            f"LAST_UPDATE_DATE < TO_TIMESTAMP('{UPPER}', 'YYYY-MM-DD HH24:MI:SS.FF6')",
            "(STATUS = 'A')",
        ]


class TestQueryMode:
    def base(self, **extra):
        return spec(
            extraction__mode="query",
            extraction__columns="*",
            extraction__query_file="sql/custom.sql",
            extraction__filter=None,
            **extra,
        )

    def test_template_placeholders_are_substituted(self):
        sql = build_source_query(
            self.base(),
            lower_bound=LOWER,
            custom_sql="SELECT * FROM T WHERE UPD >= :lower_bound",
        ).sql
        assert f"UPD >= TO_TIMESTAMP('{LOWER}'" in sql

    def test_env_and_table_placeholders(self):
        sql = render_query_template(
            "SELECT :env AS E, :table_fqn AS T FROM DUAL", self.base()
        )
        assert sql == "SELECT 'dev' AS E, 'finance.gl_transactions' AS T FROM DUAL"

    def test_trailing_semicolon_is_stripped(self):
        # A trailing ';' is fine in a .sql file but breaks a JDBC subquery.
        assert render_query_template("SELECT 1 FROM DUAL;", self.base()) == "SELECT 1 FROM DUAL"

    def test_unresolved_placeholder_is_an_error(self):
        # Shipping ':lower_bound' as literal text would read the whole table.
        with pytest.raises(SqlBuildError, match="unresolved placeholder"):
            render_query_template("SELECT * FROM T WHERE UPD >= :lower_bound", self.base())

    def test_query_mode_without_sql_is_refused(self):
        with pytest.raises(SqlBuildError, match="requires the contents"):
            build_source_query(self.base())

    def test_query_mode_passes_through_arbitrary_sql(self):
        custom = "SELECT a.X, b.Y FROM A a JOIN B b ON a.ID = b.ID GROUP BY a.X, b.Y"
        assert build_source_query(self.base(), custom_sql=custom).sql == custom


class TestDerivedQueries:
    def test_count_wraps_the_extract(self):
        query = build_source_query(spec(), lower_bound=LOWER)
        count = build_count_query(query)
        assert count.startswith("SELECT COUNT(*) AS SOURCE_COUNT FROM (")
        assert query.sql in count

    def test_bounds_probe_runs_over_the_filtered_extract(self):
        # Whole-table bounds on a filtered read leave most partitions empty.
        s = spec(source__read={"num_partitions": 16, "partition_column": "TXN_ID"},
                 extraction__filter="STATUS = 'A'")
        query = build_source_query(s, lower_bound=LOWER)
        bounds = build_bounds_query(s, query)
        assert bounds.startswith("SELECT MIN(TXN_ID) AS LOWER_BOUND, MAX(TXN_ID) AS UPPER_BOUND")
        assert "STATUS = 'A'" in bounds

    def test_bounds_probe_needs_a_partition_column(self):
        with pytest.raises(SqlBuildError, match="partition_column"):
            build_bounds_query(spec(), build_source_query(spec()))

    def test_upper_bound_comes_from_the_source_clock(self):
        assert build_upper_bound_query(spec()) == "SELECT SYSTIMESTAMP AS UPPER_BOUND FROM DUAL"

    def test_upper_bound_for_date_watermark_is_truncated(self):
        s = spec(extraction__incremental={
            "strategy": "watermark", "watermark_column": "LAST_UPDATE_DATE", "watermark_type": "date"})
        assert "TRUNC(SYSDATE)" in build_upper_bound_query(s)

    def test_upper_bound_for_scn_uses_flashback(self):
        s = spec(extraction__incremental={"strategy": "scn", "watermark_type": "number"})
        assert "GET_SYSTEM_CHANGE_NUMBER" in build_upper_bound_query(s)

    def test_max_watermark_query(self):
        query = build_source_query(spec(), lower_bound=LOWER)
        assert build_max_watermark_query(spec(), query).startswith(
            "SELECT MAX(LAST_UPDATE_DATE) AS MAX_WATERMARK FROM ("
        )

    def test_max_watermark_needs_an_incremental_extract(self):
        s = spec(extraction__mode="full", extraction__incremental={})
        with pytest.raises(SqlBuildError, match="non-incremental"):
            build_max_watermark_query(s, build_source_query(s))


class TestSubqueryWrapping:
    def test_as_subquery_adds_an_alias(self):
        query = build_source_query(spec())
        assert query.as_subquery().startswith("(SELECT")
        assert query.as_subquery().endswith(") src")

    def test_custom_alias(self):
        assert build_source_query(spec()).as_subquery("t").endswith(") t")


class TestShippedTables:
    """The three example configs must produce the SQL their comments promise."""

    def build(self, shipped_config, fqn, env="prod", **kwargs):
        from ingestion_framework.config import build_run_spec

        return build_source_query(build_run_spec(shipped_config, fqn, env), **kwargs)

    def test_gl_transactions_is_selective_and_incremental(self, shipped_config):
        query = self.build(shipped_config, "finance.gl_transactions", lower_bound=LOWER, upper_bound=UPPER)
        assert query.sql.startswith(
            "SELECT TXN_ID, ACCOUNT_ID, POSTED_DATE, AMOUNT, CURRENCY, STATUS, LAST_UPDATE_DATE"
        )
        assert "FROM GLOWNER.GL_TRANSACTIONS" in query.sql
        assert "LAST_UPDATE_DATE >= TO_TIMESTAMP" in query.sql
        assert "AND (STATUS <> 'DELETED')" in query.sql

    def test_gl_accounts_is_a_plain_full_read(self, shipped_config):
        query = self.build(shipped_config, "finance.gl_accounts")
        assert query.sql == "SELECT *\nFROM GLOWNER.GL_ACCOUNTS"

    def test_order_events_uses_scn_and_exclusive_bound(self, shipped_config):
        query = self.build(shipped_config, "sales.order_events", lower_bound="500000")
        assert "*, ORA_ROWSCN AS ORA_ROWSCN" in query.sql
        # append target -> exclusive, or the boundary rows duplicate every run
        assert "ORA_ROWSCN > 500000" in query.sql
        assert ">=" not in query.sql

    def test_dev_row_limit_reaches_the_sql(self, shipped_config):
        query = self.build(shipped_config, "finance.gl_transactions", env="dev")
        assert query.sql.endswith("FETCH FIRST 100000 ROWS ONLY")
