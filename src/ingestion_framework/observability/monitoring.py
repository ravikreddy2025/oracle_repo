"""Dashboard queries over the control plane.

These are the questions an on-call person actually asks -- is anything failing,
is anything stale, did volumes move, did a config change under me -- expressed
as SQL against the control tables so they can back a Databricks SQL dashboard,
an alert, or an ad-hoc investigation.

Freshness needs config the control tables do not hold (each table's SLA), so
that query is built from the resolved specs and joins the declared SLA onto the
observed watermark age.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from ..control.schema import qualify
from ..engine.run_spec import RunSpec


@dataclass(frozen=True)
class MonitoringQuery:
    name: str
    title: str
    description: str
    sql: str


def _tables(catalog: str, schema: str) -> dict[str, str]:
    return {
        name: qualify(catalog, schema, name)
        for name in (
            "ingestion_runs",
            "ingestion_tasks",
            "audit_log",
            "watermarks",
            "reconciliation",
            "config_registry",
        )
    }


# -- individual queries -----------------------------------------------------


def run_success_rate(catalog: str, schema: str, days: int = 7) -> MonitoringQuery:
    t = _tables(catalog, schema)
    return MonitoringQuery(
        name="run_success_rate",
        title=f"Run outcomes, last {days} days",
        description="Daily run counts by status. A rising PARTIAL count means one "
        "table is sick; a rising FAILED count means the source or cluster is.",
        sql=f"""
SELECT
  DATE(started_at)                                   AS run_date,
  env,
  status,
  COUNT(*)                                           AS runs,
  ROUND(AVG(duration_ms) / 1000.0, 1)                AS avg_seconds
FROM {t['ingestion_runs']}
WHERE started_at >= CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
GROUP BY DATE(started_at), env, status
ORDER BY run_date DESC, env, status
""".strip(),
    )


def table_health(catalog: str, schema: str, days: int = 7) -> MonitoringQuery:
    t = _tables(catalog, schema)
    return MonitoringQuery(
        name="table_health",
        title=f"Per-table outcomes, last {days} days",
        description="Success rate and failure count per table, using only the "
        "final attempt of each task so retries do not inflate the counts.",
        sql=f"""
WITH final_attempts AS (
  SELECT t.*
  FROM {t['ingestion_tasks']} t
  JOIN (
    SELECT run_id, table_fqn, MAX(attempt) AS attempt
    FROM {t['ingestion_tasks']}
    WHERE started_at >= CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
    GROUP BY run_id, table_fqn
  ) latest
    ON t.run_id = latest.run_id
   AND t.table_fqn = latest.table_fqn
   AND t.attempt = latest.attempt
)
SELECT
  table_fqn,
  env,
  COUNT(*)                                                        AS runs,
  SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END)           AS succeeded,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)              AS failed,
  ROUND(100.0 * SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                  AS success_pct,
  MAX(attempt)                                                    AS max_attempts,
  ROUND(AVG(duration_ms) / 1000.0, 1)                             AS avg_seconds
FROM final_attempts
GROUP BY table_fqn, env
ORDER BY failed DESC, success_pct ASC, table_fqn
""".strip(),
    )


def freshness(catalog: str, schema: str, specs: Sequence[RunSpec] = ()) -> MonitoringQuery:
    """Watermark age against each table's declared SLA.

    The SLA lives in config, not in the control plane, so it is inlined here
    from the resolved specs. Tables without an SLA still appear, with a NULL
    breach flag rather than being hidden.
    """
    t = _tables(catalog, schema)
    rows = [
        f"('{s.table_fqn}', '{s.env}', "
        f"{s.alerting.freshness_sla_hours if s.alerting.freshness_sla_hours is not None else 'CAST(NULL AS DOUBLE)'})"
        for s in specs
    ]
    sla_cte = (
        f"WITH sla(table_fqn, env, sla_hours) AS (VALUES\n  " + ",\n  ".join(rows) + "\n)"
        if rows
        else "WITH sla(table_fqn, env, sla_hours) AS (SELECT NULL, NULL, CAST(NULL AS DOUBLE) WHERE 1 = 0)"
    )
    return MonitoringQuery(
        name="freshness",
        title="Table freshness vs SLA",
        description="Hours since each watermark last advanced, compared with the "
        "declared freshness SLA. Breaches sort to the top.",
        sql=f"""
{sla_cte}
SELECT
  w.table_fqn,
  w.env,
  w.watermark_value,
  w.updated_at                                                   AS watermark_updated_at,
  ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(w.updated_at)) / 3600.0, 2)
                                                                 AS hours_since_advance,
  s.sla_hours,
  CASE
    WHEN s.sla_hours IS NULL THEN NULL
    WHEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(w.updated_at)) / 3600.0 > s.sla_hours
      THEN TRUE
    ELSE FALSE
  END                                                            AS sla_breached
FROM {t['watermarks']} w
LEFT JOIN sla s
  ON s.table_fqn = w.table_fqn AND s.env = w.env
ORDER BY sla_breached DESC NULLS LAST, hours_since_advance DESC
""".strip(),
    )


def volume_trend(catalog: str, schema: str, days: int = 30) -> MonitoringQuery:
    t = _tables(catalog, schema)
    return MonitoringQuery(
        name="volume_trend",
        title=f"Row volume by table, last {days} days",
        description="Daily rows written per table, with the deviation from that "
        "table's own median. A table that suddenly writes 10x or 0 rows is worth "
        "looking at even when the run reported success.",
        sql=f"""
WITH daily AS (
  SELECT
    table_fqn,
    env,
    DATE(started_at)        AS load_date,
    SUM(rows_written)       AS rows_written,
    SUM(source_count)       AS source_count
  FROM {t['ingestion_tasks']}
  WHERE status = 'SUCCEEDED'
    AND started_at >= CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
  GROUP BY table_fqn, env, DATE(started_at)
),
baseline AS (
  SELECT table_fqn, env, PERCENTILE(rows_written, 0.5) AS median_rows
  FROM daily
  GROUP BY table_fqn, env
)
SELECT
  d.table_fqn,
  d.env,
  d.load_date,
  d.rows_written,
  d.source_count,
  ROUND(b.median_rows, 0)                                        AS median_rows,
  CASE
    WHEN b.median_rows IS NULL OR b.median_rows = 0 THEN NULL
    ELSE ROUND(100.0 * (d.rows_written - b.median_rows) / b.median_rows, 1)
  END                                                            AS pct_vs_median
FROM daily d
LEFT JOIN baseline b ON b.table_fqn = d.table_fqn AND b.env = d.env
ORDER BY d.load_date DESC, d.table_fqn
""".strip(),
    )


def recent_failures(catalog: str, schema: str, limit: int = 50) -> MonitoringQuery:
    t = _tables(catalog, schema)
    return MonitoringQuery(
        name="recent_failures",
        title="Recent failures",
        description="The most recent failed attempts with their error, for triage.",
        sql=f"""
SELECT
  started_at,
  env,
  table_fqn,
  run_id,
  attempt,
  extraction_mode,
  error_type,
  SUBSTRING(error_message, 1, 500)                               AS error_message,
  watermark_from
FROM {t['ingestion_tasks']}
WHERE status = 'FAILED'
ORDER BY started_at DESC
LIMIT {limit}
""".strip(),
    )


def reconciliation_issues(catalog: str, schema: str, days: int = 7) -> MonitoringQuery:
    t = _tables(catalog, schema)
    return MonitoringQuery(
        name="reconciliation_issues",
        title=f"Reconciliation and quality failures, last {days} days",
        description="Checks that failed or warned. WARNED rows did not stop the "
        "load, so they are the ones most likely to go unnoticed.",
        sql=f"""
SELECT
  checked_at,
  env,
  table_fqn,
  run_id,
  check_type,
  check_name,
  status,
  source_count,
  target_count,
  delta,
  details
FROM {t['reconciliation']}
WHERE status IN ('FAILED', 'WARNED')
  AND checked_at >= CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
ORDER BY checked_at DESC
""".strip(),
    )


def stuck_runs(catalog: str, schema: str, hours: int = 6) -> MonitoringQuery:
    t = _tables(catalog, schema)
    return MonitoringQuery(
        name="stuck_runs",
        title=f"Runs still RUNNING after {hours}h",
        description="A run left open long past its timeout usually means the "
        "driver died before it could close its own row -- the data may be fine, "
        "but the control plane no longer reflects reality.",
        sql=f"""
SELECT
  run_id,
  env,
  trigger,
  started_at,
  ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(started_at)) / 3600.0, 2)
                                                                 AS hours_open,
  table_count
FROM {t['ingestion_runs']}
WHERE status = 'RUNNING'
  AND started_at < CURRENT_TIMESTAMP() - INTERVAL {hours} HOURS
ORDER BY started_at
""".strip(),
    )


def config_changes(catalog: str, schema: str, days: int = 30) -> MonitoringQuery:
    t = _tables(catalog, schema)
    return MonitoringQuery(
        name="config_changes",
        title=f"Config changes, last {days} days",
        description="When a table's effective config last changed. Pairs with the "
        "volume trend: a shift in row counts on the day a config hash changed is "
        "rarely a coincidence.",
        sql=f"""
SELECT
  table_fqn,
  env,
  config_hash,
  first_seen_at,
  last_seen_at,
  last_run_id,
  config_sources
FROM {t['config_registry']}
WHERE first_seen_at >= CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
ORDER BY first_seen_at DESC, table_fqn
""".strip(),
    )


def watermark_stalls(catalog: str, schema: str, runs: int = 5) -> MonitoringQuery:
    t = _tables(catalog, schema)
    return MonitoringQuery(
        name="watermark_stalls",
        title=f"Tables whose watermark held for the last {runs} runs",
        description="Successful runs that moved no watermark. Legitimate when a "
        "source is genuinely quiet, and the first symptom of a filter or "
        "predicate that silently matches nothing.",
        sql=f"""
WITH recent AS (
  SELECT
    table_fqn,
    env,
    run_id,
    started_at,
    watermark_to,
    ROW_NUMBER() OVER (PARTITION BY table_fqn, env ORDER BY started_at DESC) AS rn
  FROM {t['ingestion_tasks']}
  WHERE status = 'SUCCEEDED'
)
SELECT
  table_fqn,
  env,
  COUNT(*)                                                       AS successful_runs,
  SUM(CASE WHEN watermark_to IS NULL THEN 1 ELSE 0 END)          AS runs_with_no_new_data,
  MAX(started_at)                                                AS last_run_at
FROM recent
WHERE rn <= {runs}
GROUP BY table_fqn, env
HAVING SUM(CASE WHEN watermark_to IS NULL THEN 1 ELSE 0 END) = COUNT(*)
   AND COUNT(*) = {runs}
ORDER BY last_run_at
""".strip(),
    )


# -- collection -------------------------------------------------------------


def all_queries(
    catalog: str, schema: str, *, specs: Sequence[RunSpec] = ()
) -> list[MonitoringQuery]:
    """Every dashboard query, in the order they belong on a dashboard."""
    return [
        run_success_rate(catalog, schema),
        table_health(catalog, schema),
        freshness(catalog, schema, specs),
        recent_failures(catalog, schema),
        reconciliation_issues(catalog, schema),
        volume_trend(catalog, schema),
        watermark_stalls(catalog, schema),
        stuck_runs(catalog, schema),
        config_changes(catalog, schema),
    ]
