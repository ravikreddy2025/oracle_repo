"""Generate Databricks Asset Bundle job resources and dashboard SQL from config.

Both outputs are derived artefacts -- the config tree is the source of truth.
Regenerate after adding or regrouping a table:

    python tools/generate_bundle.py --env prod

Writes:
    resources/jobs_<env>.yml     one job per schedule group
    sql/monitoring/*.sql         dashboard queries
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion_framework.config import build_run_spec  # noqa: E402
from ingestion_framework.config.resolver import ConfigResolver  # noqa: E402
from ingestion_framework.observability.monitoring import all_queries  # noqa: E402
from ingestion_framework.orchestration.bundle import (  # noqa: E402
    BundleOptions,
    build_resources,
    cross_group_dependency_report,
)

HEADER = (
    "# GENERATED FILE -- do not edit.\n"
    "# Source: config/ (tables, environments) via tools/generate_bundle.py\n"
    "# Regenerate: python tools/generate_bundle.py --env {env}\n\n"
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-root", default="config")
    parser.add_argument("--env", required=True)
    parser.add_argument("--resources-dir", default="resources")
    parser.add_argument("--sql-dir", default="sql/monitoring")
    parser.add_argument("--job-cluster-key", default="ingest_cluster")
    args = parser.parse_args(argv)

    resolver = ConfigResolver(args.config_root)
    specs = [build_run_spec(args.config_root, fqn, args.env) for fqn in resolver.list_tables()]
    if not specs:
        print("no tables configured", file=sys.stderr)
        return 1

    # Jobs -----------------------------------------------------------------
    resources = build_resources(
        specs, env=args.env, options=BundleOptions(job_cluster_key=args.job_cluster_key)
    )
    resources_dir = Path(args.resources_dir)
    resources_dir.mkdir(parents=True, exist_ok=True)
    jobs_path = resources_dir / f"jobs_{args.env}.yml"
    jobs_path.write_text(
        HEADER.format(env=args.env)
        + yaml.safe_dump(resources, sort_keys=False, default_flow_style=False, width=100),
        encoding="utf-8",
    )
    job_count = len(resources["resources"]["jobs"])
    print(f"wrote {jobs_path} ({job_count} job(s), {len(specs)} table(s))")

    # A dependency that crosses a schedule group cannot become a task edge.
    # Saying so is the whole point -- a dropped ordering constraint that nobody
    # mentions is the worst outcome.
    for table, group, dep, dep_group in cross_group_dependency_report(specs):
        print(
            f"  WARNING: {table} (group {group}) depends on {dep} (group {dep_group}); "
            f"separate jobs cannot express that ordering -- move them into one group",
            file=sys.stderr,
        )

    # Dashboard ------------------------------------------------------------
    control = specs[0].control
    sql_dir = Path(args.sql_dir)
    sql_dir.mkdir(parents=True, exist_ok=True)
    for query in all_queries(control.catalog, control.schema, specs=specs):
        path = sql_dir / f"{query.name}.sql"
        path.write_text(
            f"-- {query.title}\n-- {query.description}\n"
            f"-- GENERATED from src/ingestion_framework/observability/monitoring.py\n\n"
            f"{query.sql};\n",
            encoding="utf-8",
        )
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
