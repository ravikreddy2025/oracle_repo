"""Generate Databricks Workflows (Asset Bundle resources) from the config tree.

A developer onboarding a table adds one YAML file; the job that runs it is
derived, not hand-maintained. Tables are grouped into one job per
``schedule.group``, with ``depends_on`` becoming task edges inside that job.

Two settings here are correctness, not preference:

* ``max_concurrent_runs: 1`` -- two concurrent runs of the same table would race
  each other's watermark, and the losing run's rows would sit below a mark that
  has already moved past them.
* task ``max_retries: 0`` by default -- the framework retries transient source
  errors itself, recording each attempt. Job-level retries on top of that would
  multiply attempts and blur the audit trail. Raise ``schedule.job_retries``
  where infrastructure failure (driver loss) is the concern, since a job retry
  is the only thing that can recover from that.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from ..engine.run_spec import RunSpec
from .batch_runner import order_tables

DEFAULT_JOB_CLUSTER_KEY = "ingest_cluster"
DEFAULT_PACKAGE = "ingestion_framework"
DEFAULT_ENTRY_POINT = "ingest"
CONFIG_ROOT_TEMPLATE = "${workspace.file_path}/config"


class BundleError(ValueError):
    """Raised when the config cannot be expressed as a Workflow."""


@dataclass
class BundleOptions:
    """Knobs that belong to deployment rather than to a table."""

    job_cluster_key: str = DEFAULT_JOB_CLUSTER_KEY
    package_name: str = DEFAULT_PACKAGE
    entry_point: str = DEFAULT_ENTRY_POINT
    config_root: str = CONFIG_ROOT_TEMPLATE
    env_expression: str | None = None  # defaults to the bundle target name
    max_concurrent_runs: int = 1
    tags: Mapping[str, str] = field(default_factory=dict)
    job_cluster: Mapping[str, Any] | None = None

    def cluster_definition(self) -> dict[str, Any]:
        """The job cluster every generated task runs on.

        Sizing comes from bundle variables so one generated file serves every
        target; only the config-independent Spark settings are fixed here.
        """
        if self.job_cluster is not None:
            return dict(self.job_cluster)
        return {
            "job_cluster_key": self.job_cluster_key,
            "new_cluster": {
                "spark_version": "${var.spark_version}",
                "node_type_id": "${var.node_type}",
                "num_workers": "${var.workers}",
                # Unity Catalog access from a job needs a single-user cluster.
                "data_security_mode": "SINGLE_USER",
                "spark_conf": {
                    # The loader turns this on per write, but setting it here
                    # means a manual re-run from a notebook behaves the same.
                    "spark.databricks.delta.schema.autoMerge.enabled": "true",
                },
            },
        }


def task_key(table_fqn: str) -> str:
    """A Workflow task key derived from the table name."""
    return re.sub(r"[^A-Za-z0-9_]", "_", table_fqn)


def job_name(env: str, group: str) -> str:
    return f"ingest_{env}_{group}"


def group_specs(specs: Sequence[RunSpec]) -> dict[str, list[RunSpec]]:
    """Bucket tables by schedule group, preserving a stable order."""
    grouped: dict[str, list[RunSpec]] = {}
    for spec in sorted(specs, key=lambda s: s.table_fqn):
        grouped.setdefault(spec.schedule.group, []).append(spec)
    return grouped


def resolve_group_schedule(group: str, specs: Sequence[RunSpec]) -> tuple[str | None, str]:
    """The cron for a group, or an error if its tables disagree.

    A group is one job and a job has one schedule, so two tables in the same
    group asking for different crons is a config mistake rather than something
    to silently resolve.
    """
    crons = {s.schedule.cron for s in specs if s.schedule.cron}
    if len(crons) > 1:
        offenders = ", ".join(f"{s.table_fqn}={s.schedule.cron}" for s in specs if s.schedule.cron)
        raise BundleError(
            f"schedule group {group!r} has conflicting cron expressions ({offenders}). "
            f"A group becomes one job, so split the tables into separate groups."
        )
    timezones = {s.schedule.timezone for s in specs}
    if len(timezones) > 1:
        raise BundleError(
            f"schedule group {group!r} has conflicting timezones: {', '.join(sorted(timezones))}"
        )
    return (crons.pop() if crons else None), (timezones.pop() if timezones else "UTC")


def email_targets(channels: Iterable[str]) -> list[str]:
    """The email addresses among a table's alert channels.

    Slack and webhook channels are delivered by the framework's dispatcher;
    only email maps onto a Workflow's native notification.
    """
    out: list[str] = []
    for channel in channels:
        text = str(channel)
        if text.lower().startswith("email:"):
            address = text.split(":", 1)[1].strip()
            if address and address not in out:
                out.append(address)
    return out


def build_task(
    spec: RunSpec,
    *,
    options: BundleOptions,
    env_expression: str,
    known_tables: set[str],
) -> dict[str, Any]:
    """One Workflow task: run this table via the framework CLI."""
    parameters = [
        "run",
        "--table", spec.table_fqn,
        "--env", env_expression,
        "--config-root", options.config_root,
        "--trigger", "schedule",
    ]

    task: dict[str, Any] = {
        "task_key": task_key(spec.table_fqn),
        "job_cluster_key": options.job_cluster_key,
        "python_wheel_task": {
            "package_name": options.package_name,
            "entry_point": options.entry_point,
            "parameters": parameters,
        },
        "max_retries": spec.schedule.job_retries,
        "timeout_seconds": spec.runtime.timeout_minutes * 60,
    }

    # Only dependencies inside this job can be expressed as task edges.
    edges = [d for d in spec.table.depends_on if d in known_tables]
    if edges:
        task["depends_on"] = [{"task_key": task_key(d)} for d in sorted(edges)]

    if spec.table.description:
        task["description"] = spec.table.description
    return task


def build_job(
    group: str,
    specs: Sequence[RunSpec],
    *,
    env: str,
    options: BundleOptions | None = None,
) -> dict[str, Any]:
    """One job per schedule group, one task per table."""
    if not specs:
        raise BundleError(f"schedule group {group!r} has no tables")
    options = options or BundleOptions()
    env_expression = options.env_expression or "${bundle.target}"

    cron, timezone = resolve_group_schedule(group, specs)
    order_tables(specs)  # reject cycles before emitting a job that cannot run

    known = {s.table_fqn for s in specs}
    tasks = [
        build_task(spec, options=options, env_expression=env_expression, known_tables=known)
        for spec in specs
    ]

    job: dict[str, Any] = {
        "name": job_name(env, group),
        "job_clusters": [options.cluster_definition()],
        # Overlapping runs of one table would race the watermark.
        "max_concurrent_runs": options.max_concurrent_runs,
        "tags": {
            "managed_by": "ingestion-framework",
            "schedule_group": group,
            **dict(options.tags),
        },
        "tasks": tasks,
    }

    if cron:
        job["schedule"] = {
            "quartz_cron_expression": cron,
            "timezone_id": timezone,
            "pause_status": "UNPAUSED",
        }

    emails = sorted({e for spec in specs for e in email_targets(spec.alerting.on_failure)})
    if emails:
        job["email_notifications"] = {"on_failure": emails}

    dropped = _cross_group_dependencies(specs, known)
    if dropped:
        # Silently dropping an ordering constraint would be the worst outcome,
        # so it is surfaced in the artefact itself.
        job["description"] = (
            "Cross-group dependencies are not expressed as task edges: "
            + "; ".join(f"{table} depends on {dep}" for table, dep in dropped)
        )
    return job


def _cross_group_dependencies(
    specs: Sequence[RunSpec], known: set[str]
) -> list[tuple[str, str]]:
    return [
        (spec.table_fqn, dep)
        for spec in specs
        for dep in spec.table.depends_on
        if dep not in known
    ]


def build_resources(
    specs: Sequence[RunSpec], *, env: str, options: BundleOptions | None = None
) -> dict[str, Any]:
    """The ``resources.jobs`` block for every schedule group."""
    options = options or BundleOptions()
    jobs = {}
    for group, group_specs_ in group_specs(specs).items():
        enabled = [s for s in group_specs_ if s.runtime.enabled]
        if not enabled:
            continue
        jobs[f"ingest_{group}"] = build_job(group, enabled, env=env, options=options)
    return {"resources": {"jobs": jobs}}


def cross_group_dependency_report(specs: Sequence[RunSpec]) -> list[tuple[str, str, str, str]]:
    """Dependencies that cross a schedule-group boundary.

    These cannot become task edges, so they are reported for the operator to
    resolve -- usually by moving the tables into one group.
    """
    group_of = {s.table_fqn: s.schedule.group for s in specs}
    out = []
    for spec in specs:
        for dep in spec.table.depends_on:
            dep_group = group_of.get(dep)
            if dep_group is not None and dep_group != spec.schedule.group:
                out.append((spec.table_fqn, spec.schedule.group, dep, dep_group))
    return out
