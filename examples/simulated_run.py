"""Simulate a full ingestion run with no Oracle and no Spark.

What is REAL here: config resolution and validation, every SQL statement the
framework would issue against Oracle and Delta, the control-plane writes, the
state machine, watermark arithmetic, reconciliation, and the audit trail.

What is STUBBED: the JDBC read and the Delta write. Nothing touches a database.
Row counts are invented so the lifecycle has something to carry.

So this proves the framework's *decisions* end to end -- it does not prove that
Delta behaves as assumed. That is what ``pytest -m spark`` is for.

    python examples/simulated_run.py [--env dev] [--table finance.gl_transactions]
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from ingestion_framework.config import build_run_spec  # noqa: E402
from ingestion_framework.config.resolver import ConfigResolver  # noqa: E402
from ingestion_framework.control.audit import AuditLog  # noqa: E402
from ingestion_framework.control.control_store import ControlStore, make_run_id  # noqa: E402
from ingestion_framework.control.sql_client import RecordingSqlClient  # noqa: E402
from ingestion_framework.control.watermark import WatermarkStore, WatermarkWindow  # noqa: E402
from ingestion_framework.engine.extractor import ExtractResult  # noqa: E402
from ingestion_framework.engine.loader import LoadResult, build_merge_sql  # noqa: E402
from ingestion_framework.engine.reconciler import Reconciler  # noqa: E402
from ingestion_framework.engine.sql_builder import SourceQuery, build_source_query  # noqa: E402
from ingestion_framework.engine.transformer import (  # noqa: E402
    StageResult,
    build_stage_query,
    needs_dedupe,
    view_names,
)
from ingestion_framework.observability.alerts import AlertDispatcher  # noqa: E402
from ingestion_framework.observability.logger import StructuredLogger  # noqa: E402
from ingestion_framework.orchestration.batch_runner import BatchRunner  # noqa: E402
from ingestion_framework.orchestration.runner import Engine  # noqa: E402

NOW = datetime(2026, 8, 24, 10, 0, 0)
STORED_WATERMARK = "2026-08-24 08:00:00.000000"
NEW_WATERMARK = "2026-08-24 10:00:00.000000"
NEW_SCN = "58200145"


def new_watermark_for(spec) -> str | None:
    """What the source would have reported as this batch's ceiling.

    An SCN table gets a number, not a timestamp -- the same distinction the
    SQL builder enforces when it refuses to render one as the other.
    """
    if not spec.extraction.tracks_watermark:
        return None
    return NEW_SCN if spec.extraction.incremental.uses_scn else NEW_WATERMARK


class DemoSqlClient(RecordingSqlClient):
    """Recording client that remembers the watermark it was just told to set.

    The plain recorder returns nothing from every SELECT, which would make each
    advance look like it was rejected. Echoing the value back lets the demo show
    advanced-vs-held truthfully.
    """

    def __init__(self) -> None:
        super().__init__()
        self._watermarks: dict[tuple[str, str], dict] = {}

    def execute(self, statement, params=None):
        super().execute(statement, params)
        params = params or {}
        if "control.watermarks" in statement and "watermark_value" in params:
            key = (params["table_fqn"], params["env"])
            self._watermarks[key] = dict(params)

    def query(self, statement, params=None):
        super().query(statement, params)
        params = params or {}
        if "control.watermarks" in statement:
            row = self._watermarks.get((params.get("table_fqn"), params.get("env")))
            return [row] if row else []
        return []


# -- stubs for the two things that need infrastructure ----------------------


class StubExtractor:
    """Pretends to read Oracle. Builds the real query, invents the rows."""

    def __init__(self, source_count: int = 1200) -> None:
        self.source_count = source_count

    def extract(self, spec, window: WatermarkWindow) -> ExtractResult:
        ceiling = new_watermark_for(spec)
        query = build_source_query(
            spec,
            lower_bound=window.lower_bound if spec.extraction.tracks_watermark else None,
            upper_bound=ceiling,
            custom_sql="SELECT 1 FROM DUAL" if spec.extraction.mode == "query" else None,
        )
        return ExtractResult(
            dataframe=StubDataFrame(spec),
            query=query,
            lower_bound=window.lower_bound,
            upper_bound=ceiling,
            new_watermark=ceiling,
            source_count=self.source_count,
            num_partitions=spec.source.read.num_partitions,
        )


class StubDataFrame:
    """Stands in for the extracted DataFrame, with columns the spec implies."""

    def __init__(self, spec) -> None:
        columns = list(spec.extraction.column_list)
        if not columns:  # SELECT * -- invent the columns the config references
            columns = list(spec.target.merge_keys)
            watermark = spec.extraction.incremental.effective_watermark_column
            if watermark and watermark not in columns:
                columns.append(watermark)
            columns.append("PAYLOAD")
        self.columns = columns

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802 - Spark's name
        pass


class StubTransformer:
    """Builds the real staging statement; reports plausible dedupe counts."""

    def __init__(self, duplicates: int = 200) -> None:
        self.duplicates = duplicates

    def stage(self, dataframe, spec, *, run_id, batch_id, ingested_at, count_rows=False, views=None):
        views = views or view_names(spec.table_fqn, run_id)
        query = build_stage_query(
            spec,
            source_columns=dataframe.columns,
            run_id=run_id,
            batch_id=batch_id,
            ingested_at=ingested_at,
            raw_view=views.raw,
        )
        duplicates = self.duplicates if query.dedupe_applied else 0
        return StageResult(
            dataframe=dataframe,
            query=query,
            duplicates_removed=duplicates,
            null_key_rows=0,
            views=views,
        )


class StubLoader:
    """Builds the real MERGE; reports what Delta would have reported.

    Rows written must equal source rows minus the ones the dedupe collapsed --
    the same invariant the reconciler checks. A table that does not dedupe
    (append) writes every row it read.
    """

    def __init__(self, source_count: int = 1200, duplicates: int = 200) -> None:
        self.source_count = source_count
        self.duplicates = duplicates

    def write(self, dataframe, spec, *, staged_view=None) -> LoadResult:
        rows = self.source_count - (self.duplicates if needs_dedupe(spec) else 0)
        statements = []
        if spec.target.is_merge:
            columns = list(dataframe.columns) + [
                "_ingested_at", "_ingested_date", "_run_id",
                "_batch_id", "_source_op", "_first_ingested_at",
            ]
            statements.append(build_merge_sql(spec, columns, staged_view=staged_view or "staged"))
        return LoadResult(
            target=spec.target.fqn,
            write_mode=spec.target.write_mode,
            rows_written=rows,
            rows_inserted=int(rows * 0.9),
            rows_updated=rows - int(rows * 0.9),
            table_created=True,
            statements=statements,
        )


class StubSpark:
    """Answers the reconciler's expectation queries with zero violations."""

    def sql(self, query: str):
        return StubResult()


class StubResult:
    def collect(self):
        return [[0]]


# -- the demo ---------------------------------------------------------------


def banner(text: str) -> None:
    print(f"\n{'=' * 78}\n{text}\n{'=' * 78}")


def show_sql_for(config_root: Path, table_fqn: str, env: str) -> None:
    spec = build_run_spec(config_root, table_fqn, env)
    banner(f"SQL the framework would issue for {table_fqn} [{env}]")

    print(f"target        : {spec.target.fqn}")
    print(f"mode          : {spec.extraction.mode} / {spec.target.write_mode}")
    print(f"config hash   : {spec.config_hash[:12]}")
    print(f"stored mark   : {STORED_WATERMARK}")
    print(f"overlap       : {spec.extraction.incremental.overlap} -> lower bound rewound\n")

    scn = spec.extraction.incremental.uses_scn
    window = WatermarkWindow(
        lower_bound="58100000" if scn else "2026-08-24 02:00:00.000000",
        stored_value=STORED_WATERMARK,
        overlap_applied=not scn,  # a duration cannot rewind an SCN
        is_first_run=False,
    )
    extract = StubExtractor().extract(spec, window)
    print("--- 1. Oracle source query -------------------------------------------")
    print(extract.query.sql)

    stage = StubTransformer().stage(
        StubDataFrame(spec), spec,
        run_id="dev-20260824T100000-abc123", batch_id="b1", ingested_at=NOW,
    )
    print("\n--- 2. Staging (audit columns + dedupe before merge) ------------------")
    print(stage.query.sql)

    load = StubLoader().write(StubDataFrame(spec), spec, staged_view=stage.views.staged)
    if load.statements:
        print("\n--- 3. Delta MERGE ---------------------------------------------------")
        print(load.statements[0])
    else:
        print(f"\n--- 3. Delta write ---------------------------------------------------")
        print(f"{spec.target.write_mode} into {spec.target.fqn}")


def simulate_run(config_root: Path, env: str) -> int:
    resolver = ConfigResolver(config_root)
    specs = [build_run_spec(config_root, fqn, env) for fqn in resolver.list_tables()]

    client = DemoSqlClient()
    run_id = make_run_id(env, NOW, "demo01")
    logger = StructuredLogger()  # quiet: no handler configured

    engine = Engine(
        extractor=StubExtractor(),
        transformer=StubTransformer(),
        loader=StubLoader(),
        reconciler=Reconciler(StubSpark()),
        control=ControlStore(client, specs[0].control.catalog, specs[0].control.schema, now=lambda: NOW),
        watermarks=WatermarkStore(client, specs[0].control.catalog, specs[0].control.schema),
        audit=AuditLog(client, specs[0].control.catalog, specs[0].control.schema,
                       run_id=run_id, env=env, actor="demo", now=lambda: NOW),
        logger=logger,
        alerts=AlertDispatcher(logger),
        now=lambda: NOW,
        batch_id_factory=lambda: "batch-demo",
    )

    banner(f"Simulated run of {len(specs)} table(s) in [{env}]")
    result = BatchRunner(engine).run(specs, run_id, env, trigger="manual")

    print(f"run id   : {result.run_id}")
    print(f"status   : {result.status.value}")
    for outcome in result.outcomes:
        mark = "advanced" if outcome.watermark_advanced else "held"
        print(
            f"  {outcome.table_fqn:<26} {outcome.status.value:<10} "
            f"rows={outcome.metrics.rows_written} watermark={mark}"
        )

    banner("Control-plane writes this run produced")
    for kind, statement, _ in client.calls:
        if kind == "insert":
            continue
        verb = statement.split()[0]
        target = next((w for w in statement.split() if "." in w and "control" in w), "")
        print(f"  {verb:<8} {target}")
    for tablename, rows in client.inserted:
        print(f"  INSERT   {tablename} ({len(rows)} row(s))")

    audit_rows = [r for t, rows in client.inserted if t.endswith("audit_log") for r in rows]
    banner(f"Audit trail ({len(audit_rows)} events)")
    for row in audit_rows:
        table = row["table_fqn"] or "-"
        print(f"  {row['sequence']:>3}  {row['event_type']:<22} {table}")

    checks = [r for t, rows in client.inserted if t.endswith("reconciliation") for r in rows]
    banner(f"Reconciliation ({len(checks)} checks)")
    for row in checks:
        detail = f" -- {row['details']}" if row.get("details") else ""
        print(f"  {row['table_fqn']:<26} {row['check_name']:<24} {row['status']}{detail}")

    print(
        "\nNOTE: the JDBC read and the Delta write were stubbed. This shows what the "
        "framework decides and emits, not that Delta behaves as assumed."
    )
    return 0 if result.status.value == "SUCCEEDED" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config-root", default=str(REPO / "config"))
    parser.add_argument("--env", default="dev")
    parser.add_argument("--table", default="finance.gl_transactions",
                        help="table to show generated SQL for")
    parser.add_argument("--sql-only", action="store_true")
    args = parser.parse_args(argv)

    config_root = Path(args.config_root)
    show_sql_for(config_root, args.table, args.env)
    if args.sql_only:
        return 0
    return simulate_run(config_root, args.env)


if __name__ == "__main__":
    raise SystemExit(main())
