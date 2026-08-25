"""``ingest`` -- the framework's command line.

Commands that need a cluster (``run``, ``backfill``, ``init-control``) import
Spark lazily, so ``validate`` and ``show-sql`` work anywhere -- on a laptop, in
a pre-commit hook, in CI. That matters: config mistakes should be caught before
a job starts, not by a failing job.

Exit codes: 0 success, 1 failure, 2 usage/config error.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from .config import build_run_spec, validate_all
from .config.loader import ConfigLoadError
from .config.resolver import ConfigResolutionError, ConfigResolver
from .config.validator import ConfigValidationError
from .control.control_store import RunStatus, make_run_id
from .control.schema import ddl_statements
from .control.watermark import WatermarkWindow
from .engine.sql_builder import build_source_query
from .engine.transformer import build_stage_query
from .orchestration.batch_runner import BatchRunner, DependencyError, order_tables, select_specs

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2

DEFAULT_CONFIG_ROOT = "config"


# -- argument parsing -------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingest", description=__doc__.splitlines()[0])
    parser.add_argument("--config-root", default=DEFAULT_CONFIG_ROOT, help="path to the config tree")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_selection(p: argparse.ArgumentParser) -> None:
        group = p.add_mutually_exclusive_group(required=True)
        group.add_argument("--table", action="append", help="domain.table (repeatable)")
        group.add_argument("--group", help="schedule group")
        group.add_argument("--domain", help="all tables in a domain")
        group.add_argument("--all", action="store_true", help="every configured table")

    run = sub.add_parser("run", help="ingest one or more tables")
    add_selection(run)
    run.add_argument("--env", required=True)
    run.add_argument("--run-id", help="reuse an existing run id (for a retry)")
    run.add_argument("--trigger", default="manual", choices=["manual", "schedule", "backfill", "retry"])
    run.add_argument("--max-workers", type=int, default=1, help="tables to run concurrently")
    run.add_argument("--dry-run", action="store_true", help="resolve and validate, touch nothing")
    run.add_argument("--continue-on-failure", action="store_true",
                     help="run dependent tables even if a dependency failed")

    validate = sub.add_parser("validate", help="check config without touching anything")
    validate.add_argument("--env", required=True)
    validate.add_argument("--table", action="append")
    validate.add_argument("--strict", action="store_true", help="treat warnings as failures")

    show = sub.add_parser("show-sql", help="print the SQL a table would run")
    show.add_argument("--table", required=True)
    show.add_argument("--env", required=True)
    show.add_argument("--lower-bound", help="pretend the watermark is here")
    show.add_argument("--upper-bound")

    backfill = sub.add_parser("backfill", help="re-load a window by resetting the watermark")
    backfill.add_argument("--table", required=True)
    backfill.add_argument("--env", required=True)
    backfill.add_argument("--from", dest="from_value", required=True, help="watermark to reset to")
    backfill.add_argument("--yes", action="store_true", help="skip the confirmation prompt")

    init = sub.add_parser("init-control", help="create the control schema and tables")
    init.add_argument("--env", required=True)
    init.add_argument("--dry-run", action="store_true", help="print the DDL instead of running it")

    tables = sub.add_parser("list-tables", help="show configured tables and their run order")
    tables.add_argument("--env", required=True)

    return parser


# -- commands ---------------------------------------------------------------


def cmd_validate(args: argparse.Namespace, out) -> int:
    ConfigResolver(args.config_root).require_environment(args.env)
    reports = validate_all(args.config_root, args.env)
    if args.table:
        wanted = set(args.table)
        reports = [r for r in reports if r.table_fqn in wanted]
    if not reports:
        print("no tables matched", file=out)
        return EXIT_USAGE

    failed = warned = 0
    for report in sorted(reports, key=lambda r: r.table_fqn):
        for error in report.errors:
            print(f"ERROR {report.table_fqn}: {error}", file=out)
        for warning in report.warnings:
            print(f"WARN  {report.table_fqn}: {warning}", file=out)
        failed += 1 if report.errors else 0
        warned += 1 if report.warnings else 0
        if report.ok and not report.warnings:
            print(f"OK    {report.table_fqn}", file=out)

    print(
        f"\n{len(reports)} table(s): {len(reports) - failed} ok, {failed} with errors, "
        f"{warned} with warnings",
        file=out,
    )
    if failed:
        return EXIT_FAILED
    return EXIT_FAILED if (args.strict and warned) else EXIT_OK


def cmd_show_sql(args: argparse.Namespace, out) -> int:
    spec = build_run_spec(args.config_root, args.table, args.env)
    query = build_source_query(
        spec, lower_bound=args.lower_bound, upper_bound=args.upper_bound
    ) if spec.extraction.mode != "query" else None

    print(f"-- source query for {spec.table_fqn} [{spec.env}] -> {spec.target.fqn}", file=out)
    if query is None:
        print("-- mode: query; see " + str(spec.extraction.query_file), file=out)
    else:
        print(query.sql + "\n", file=out)
        stage = build_stage_query(
            spec,
            source_columns=list(query.projected_columns) or ["<all columns>"],
            run_id="run_id_placeholder",
            batch_id="batch_id_placeholder",
            ingested_at=datetime(2026, 1, 1),
        )
        if not spec.extraction.selects_all_columns:
            print(f"-- staging ({spec.target.write_mode})", file=out)
            print(stage.sql, file=out)
    return EXIT_OK


def cmd_list_tables(args: argparse.Namespace, out) -> int:
    specs = _resolve_all(args.config_root, args.env)
    levels = order_tables(specs)
    for depth, level in enumerate(levels):
        for spec in level:
            print(
                f"{depth}  {spec.table_fqn:<32} {spec.extraction.mode:<12} "
                f"{spec.target.write_mode:<10} group={spec.schedule.group}",
                file=out,
            )
    print(f"\n{len(specs)} table(s) in {len(levels)} dependency level(s)", file=out)
    return EXIT_OK


def cmd_run(args: argparse.Namespace, out) -> int:
    specs = select_specs(
        _resolve_all(args.config_root, args.env),
        tables=args.table,
        group=args.group,
        domain=args.domain,
    )
    if not specs:
        print("no tables matched the selection", file=out)
        return EXIT_USAGE

    if args.dry_run:
        levels = order_tables(specs)
        print(f"dry run: {len(specs)} table(s) in {len(levels)} level(s)", file=out)
        for depth, level in enumerate(levels):
            for spec in level:
                print(
                    f"  [{depth}] {spec.table_fqn} -> {spec.target.fqn} "
                    f"({spec.extraction.mode}/{spec.target.write_mode}) "
                    f"config={spec.config_hash[:8]}",
                    file=out,
                )
        return EXIT_OK

    from .orchestration.factory import build_engine, get_spark

    spark = get_spark()
    started = datetime.utcnow()
    run_id = args.run_id or make_run_id(args.env, started, uuid.uuid4().hex)
    engine = build_engine(
        specs[0], spark=spark, config_root=args.config_root, run_id=run_id
    )
    batch = BatchRunner(
        engine,
        max_workers=args.max_workers,
        skip_dependents_on_failure=not args.continue_on_failure,
    )
    result = batch.run(specs, run_id, args.env, trigger=args.trigger)

    print(json.dumps(result.summary(), indent=2), file=out)
    for outcome in result.failed:
        print(f"FAILED {outcome.table_fqn}: {outcome.error}", file=out)
    return EXIT_OK if result.status is RunStatus.SUCCEEDED else EXIT_FAILED


def cmd_backfill(args: argparse.Namespace, out) -> int:
    spec = build_run_spec(args.config_root, args.table, args.env)
    if not spec.extraction.tracks_watermark:
        print(
            f"{spec.table_fqn} is a {spec.extraction.mode} extract with no watermark; "
            f"there is nothing to rewind. Re-run it instead.",
            file=out,
        )
        return EXIT_USAGE

    if not args.yes:
        # Rewinding a watermark re-reads history and, on an append target,
        # duplicates it. That deserves an explicit yes.
        print(
            f"About to reset the watermark for {spec.table_fqn} [{args.env}] to "
            f"{args.from_value!r}. Re-run with --yes to proceed.",
            file=out,
        )
        return EXIT_USAGE

    from .orchestration.factory import build_engine, get_spark

    spark = get_spark()
    started = datetime.utcnow()
    run_id = make_run_id(args.env, started, uuid.uuid4().hex)
    engine = build_engine(spec, spark=spark, config_root=args.config_root, run_id=run_id)

    incremental = spec.extraction.incremental
    engine.watermarks.force_set(
        spec.table_fqn,
        args.env,
        value=args.from_value,
        watermark_type="number" if incremental.uses_scn else incremental.watermark_type,
        watermark_column=incremental.effective_watermark_column,
        run_id=run_id,
        updated_at=started,
    )
    from .control.audit import EventType

    engine.audit.emit(
        EventType.WATERMARK_FORCED,
        table_fqn=spec.table_fqn,
        run_id=run_id,
        payload={"value": args.from_value, "reason": "backfill"},
        flush=True,
    )
    print(f"watermark for {spec.table_fqn} reset to {args.from_value}", file=out)

    result = BatchRunner(engine).run([spec], run_id, args.env, trigger="backfill")
    print(json.dumps(result.summary(), indent=2), file=out)
    return EXIT_OK if result.status is RunStatus.SUCCEEDED else EXIT_FAILED


def cmd_init_control(args: argparse.Namespace, out) -> int:
    specs = _resolve_all(args.config_root, args.env)
    if not specs:
        print("no tables configured, so no control plane location is known", file=out)
        return EXIT_USAGE
    control = specs[0].control
    statements = ddl_statements(control.catalog, control.schema)

    if args.dry_run:
        for statement in statements:
            print(statement + ";\n", file=out)
        return EXIT_OK

    from .orchestration.factory import get_spark
    from .control.sql_client import SparkSqlClient

    client = SparkSqlClient(get_spark())
    for statement in statements:
        client.execute(statement)
    print(f"control plane ready at {control.catalog}.{control.schema}", file=out)
    return EXIT_OK


# -- helpers ----------------------------------------------------------------


def _resolve_all(config_root: str, env: str):
    resolver = ConfigResolver(config_root)
    return [
        build_run_spec(config_root, fqn, env) for fqn in resolver.list_tables()
    ]


COMMANDS = {
    "run": cmd_run,
    "validate": cmd_validate,
    "show-sql": cmd_show_sql,
    "backfill": cmd_backfill,
    "init-control": cmd_init_control,
    "list-tables": cmd_list_tables,
}


def main(argv: Sequence[str] | None = None, out: Any = None) -> int:
    out = out or sys.stdout
    args = build_parser().parse_args(argv)
    handler = COMMANDS[args.command]
    try:
        return handler(args, out)
    except (ConfigValidationError, ConfigResolutionError, ConfigLoadError, DependencyError) as exc:
        # Config problems are the user's to fix, not a stack trace to decode.
        print(f"error: {exc}", file=out)
        return EXIT_USAGE
    except Exception as exc:  # noqa: BLE001
        print(f"error: {type(exc).__name__}: {exc}", file=out)
        return EXIT_FAILED


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
