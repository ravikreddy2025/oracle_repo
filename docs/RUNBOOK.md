# Operations Runbook

For on-call engineers, support teams, and anyone paged about an ingestion
failure. Written as symptom → diagnosis → action. If you are trying to
*understand* the system rather than fix an incident, start with
[README.md](../README.md) or [CODEMAP.md](CODEMAP.md) instead.

Every query below runs in a Databricks SQL editor or notebook. Replace
`<catalog>` with the environment's control catalog (`dev_lakehouse`,
`test_lakehouse`, or `prod_lakehouse` — see `config/environments/<env>.yaml`
→ `control.catalog`).

---

## 0. First 60 seconds — orientation

1. **What failed?** Check the Databricks Workflow run UI first — job name is
   `ingest_<env>_<schedule_group>` (e.g. `ingest_prod_finance_hourly`), task
   name is the table (e.g. `finance_gl_transactions`).
2. **Cross-reference the control plane** — the job UI shows *that* it failed;
   these tables show *what actually happened*:
   ```sql
   SELECT * FROM <catalog>.control.ingestion_tasks
   WHERE table_fqn = 'finance.gl_transactions'
   ORDER BY started_at DESC LIMIT 5;
   ```
3. **Read the error.** `error_type` and `error_message` on the failed row are
   the actual exception from Oracle, Delta, or the framework's own validation.
4. **Check the audit trail** for exactly how far the run got before it died:
   ```sql
   SELECT event_ts, event_type, payload FROM <catalog>.control.audit_log
   WHERE table_fqn = 'finance.gl_transactions' AND run_id = '<run_id>'
   ORDER BY sequence;
   ```

**The one fact that should calm you down:** if a task shows `FAILED`, its
watermark did **not** move (see [Guarantee #1](#guarantee-1-the-watermark-only-moves-after-a-committed-successful-load)
below). Re-running is always safe. Nothing is lost by taking your time.

---

## 1. The two guarantees that make every procedure below safe

### Guarantee #1: the watermark only moves after a committed, successful load

`extract → stage → load → reconcile → advance watermark`, in that order, and
the advance only happens if every prior step — **including reconciliation**
— succeeded. This is enforced in code
(`orchestration/runner.py::TableRunner._execute`), not by convention.

**What this means for you:** a `FAILED` task never leaves the source window
half-consumed. Re-running the exact same task re-reads the exact same window.
There is no "did the failure happen before or after the watermark moved"
question to answer — it's always *before*.

### Guarantee #2: retries append; they never overwrite history

Every attempt at a table gets its own row in `ingestion_tasks`, keyed by
`(run_id, table_fqn, attempt)`. A `FAILED` row is never reopened or deleted.

**What this means for you:** `attempt = 3` failing doesn't erase what
`attempt = 1` and `attempt = 2` recorded. The full history of what actually
happened is always there.

```sql
SELECT attempt, status, started_at, ended_at, error_type, error_message
FROM <catalog>.control.ingestion_tasks
WHERE run_id = '<run_id>' AND table_fqn = 'finance.gl_transactions'
ORDER BY attempt;
```

---

## 2. Symptom → diagnosis → action

### 2.1 A task failed

**Check:**
```sql
SELECT status, extraction_mode, write_mode, error_type, error_message,
       watermark_from, source_count, rows_written
FROM <catalog>.control.ingestion_tasks
WHERE table_fqn = '<table>' AND run_id = '<run_id>';
```

**Common `error_type` values and what they mean:**

| `error_type` | Meaning | Action |
|---|---|---|
| `ExtractionError` | Could not read Oracle — bad credentials, network, or the table/columns don't exist | See §2.2 |
| `SqlBuildError` | The framework refused to build a query — usually a bad bound value or a missing key | See §2.3 (this should have been caught by `ingest validate`, so also check whether config drifted since the last validate) |
| `ReconciliationFailure` | Row counts didn't balance, or a `quality.expectations` rule failed | See §2.4 |
| `WatermarkError` | A watermark value didn't parse against its declared type | See §2.5 |
| `ControlStateError` | The task state machine rejected a transition — this indicates a bug, not a data problem | Escalate to the framework owner (see §5) |
| anything Delta/Spark-shaped (`AnalysisException`, `Py4JJavaError`, etc.) | The write itself failed | See §2.6 |

**Then, always:** if `runtime.retries > 0` for the table, the framework
already retried automatically before you got paged — check `attempt` to see
how many times. If every attempt failed the same way, it's not transient;
fix the root cause before re-running.

**To re-run once the cause is fixed:**
```bash
ingest run --table <domain>.<table> --env <env> --trigger retry
```

---

### 2.2 Oracle connection / credential failures

**Symptoms:** `ExtractionError`, or a raw `ORA-*` code in `error_message`
(`ORA-12541` no listener, `ORA-01017` invalid credentials, `ORA-12154` TNS
name not found).

**Check:**
1. Is the secret scope still valid? `source.secret_scope` in the table's
   effective config (`ingest show-sql --table <fqn> --env <env>` prints the
   target and won't leak the secret, but confirms the scope name is what you
   expect).
2. Is the Oracle listener up / is this a scheduled maintenance window?
3. Has the JDBC URL in `config/environments/<env>.yaml` changed (host
   migration, port change)?

**This is never a framework bug** — it's connectivity or credentials. Fix at
the source, then re-run (§2.1).

---

### 2.3 `SqlBuildError` at runtime (config passed validation, but failed live)

This should be rare — `ingest validate` catches almost everything before a
job ever runs. If it happens anyway, the most likely causes:

- **A watermark value from the control plane didn't canonicalize.** Check
  `watermarks.watermark_value` for the table — if someone hand-edited it
  outside `ingest backfill`, it may not match the declared `watermark_type`.
  ```sql
  SELECT * FROM <catalog>.control.watermarks WHERE table_fqn = '<table>';
  ```
- **A source column referenced in `merge_keys`/`cluster_by`/an expectation
  was dropped from Oracle** since the config was last validated. Re-run
  `ingest validate --env <env> --table <domain>.<table>` — if it now fails,
  the config needs updating to match the current source schema.

**Action:** fix the config or the watermark row, `ingest validate` again to
confirm, then re-run.

---

### 2.4 Reconciliation failed (row counts didn't balance, or an expectation failed)

**This is the framework working as designed** — it stopped the load rather
than commit data it couldn't verify. It is **not** data loss; the source
window is still fully available on re-run.

**Check what actually failed:**
```sql
SELECT check_type, check_name, status, source_count, target_count, delta, details
FROM <catalog>.control.reconciliation
WHERE run_id = '<run_id>' AND table_fqn = '<table>'
ORDER BY checked_at;
```

**By `check_type`:**

| `check_type` | What a FAILED status means | Likely cause |
|---|---|---|
| `row_count` | `source_count - duplicates_removed != rows_written` | Genuine write problem, or a schema-evolution/type mismatch mid-write |
| `null_key` | A merge key was NULL in the extracted batch | NULL keys can never match on merge — the source data itself has a problem, or the wrong column is configured as `business_key` |
| `expectation` | A `quality.expectations` rule was violated | Read `details` — it names the rule and the violation count |
| `duplicates` | Only `WARNED`, never fails the task, but worth a look on a full-load table — duplicates there suggest the source key isn't actually unique |

**Action:**
- If it's a genuine source data-quality issue: this is expected behaviour,
  not an incident — loop in the table owner (`table.owner` in the config).
  Consider changing the failing expectation's `action: fail` to `action: warn`
  *only if the business has decided that's acceptable* — this is a config
  change a human should approve, not an automatic fix.
- If it's a framework-side miscount: escalate (§5) with the `run_id` and the
  reconciliation rows above.

---

### 2.5 `WatermarkError` — a stored or default watermark won't parse

**Symptom:** `'<value>' is not a valid numeric watermark` (or timestamp/date).

**Most likely cause:** `extraction.incremental.lower_bound_default` doesn't
match `watermark_type` for that table (e.g. a timestamp default inherited by
an SCN-strategy table). `ingest validate` catches this for the *configured*
default; it can't catch a bad value already sitting in the `watermarks`
table from a manual edit.

**Check:**
```sql
SELECT table_fqn, watermark_type, watermark_value, previous_value, run_id, updated_at
FROM <catalog>.control.watermarks WHERE table_fqn = '<table>';
```

**Action:** if the stored value is wrong, use `ingest backfill` (§4.3) to set
a correct one — never hand-edit the `watermarks` table directly, because
that bypasses the audit trail.

---

### 2.6 The Delta write itself failed

**Symptom:** a Spark/Delta exception (`AnalysisException`,
`DeltaConcurrentModificationException`, a Py4J wrapper) in `error_message`.

**Common causes:**

| Signature in the error | Cause | Action |
|---|---|---|
| "cannot resolve column" / schema mismatch | Oracle added/renamed/retyped a column and `schema_evolution` didn't cover it (e.g. a type narrowing) | Check the table's actual Oracle DDL vs `config/tables/.../<table>.yaml`; may need `custom_schema` overrides |
| `DeltaConcurrentModificationException` | Two writers hit the same table at once | Should not happen — `max_concurrent_runs: 1` is set on every generated job specifically to prevent this. If you see this, check whether someone ran the CLI manually against a table while its scheduled job was also running |
| Multiple source rows matched one target row during MERGE | The pre-merge dedupe didn't collapse everything — check `reconciliation.check_type = 'duplicates'` for that run; if `duplicates_removed` looks too low, the `merge_keys` may not actually be unique-enough for the ordering used | Escalate (§5) — this is the exact failure mode `pytest -m spark` exists to catch, so it points at either a genuinely non-unique key or a framework regression |

---

### 2.7 A watermark hasn't moved in several runs, but tasks report SUCCEEDED

**This can be completely normal** (a quiet source) or **a silent problem** (a
filter matching nothing, or a broken upstream feed). They look identical from
the job UI, which is exactly why this needs a dashboard, not vibes.

**Check:**
```sql
-- from sql/monitoring/watermark_stalls.sql
WITH recent AS (
  SELECT table_fqn, env, run_id, started_at, watermark_to,
         ROW_NUMBER() OVER (PARTITION BY table_fqn, env ORDER BY started_at DESC) AS rn
  FROM <catalog>.control.ingestion_tasks WHERE status = 'SUCCEEDED'
)
SELECT table_fqn, env, COUNT(*) AS successful_runs,
       SUM(CASE WHEN watermark_to IS NULL THEN 1 ELSE 0 END) AS runs_with_no_new_data
FROM recent WHERE rn <= 5 GROUP BY table_fqn, env
HAVING SUM(CASE WHEN watermark_to IS NULL THEN 1 ELSE 0 END) = COUNT(*) AND COUNT(*) = 5;
```

**Action:**
1. `ingest show-sql --table <fqn> --env <env>` — read the generated `WHERE`
   clause. Does the `filter` predicate look like it could be matching zero
   rows (e.g. a status value that no longer exists in the source)?
2. Query Oracle directly with that same predicate to confirm whether rows
   genuinely exist in the window.
3. If the source is legitimately quiet, no action needed — this is a
   dashboard entry to watch, not an incident.

---

### 2.8 A run has been stuck in `RUNNING` for hours

**Symptom:** the `ingestion_runs` row never closed. Almost always means the
Databricks cluster/driver died mid-run before the framework's own
try/finally could close the row.

**Check:**
```sql
-- from sql/monitoring/stuck_runs.sql
SELECT run_id, env, trigger, started_at,
       ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(started_at)) / 3600.0, 2) AS hours_open
FROM <catalog>.control.ingestion_runs
WHERE status = 'RUNNING' AND started_at < CURRENT_TIMESTAMP() - INTERVAL 6 HOURS;
```

**Action:**
1. Confirm in the Databricks Jobs UI that the job run is actually gone (not
   just slow) — check the cluster event log for termination.
2. **The data is fine** — per Guarantee #1, no watermark advanced without a
   committed load, so there is nothing to roll back.
3. This is a housekeeping cleanup only: manually close the stale run row for
   accurate reporting (no framework command does this yet — see
   [OPEN_ITEMS.md](OPEN_ITEMS.md) item on stuck-run cleanup):
   ```sql
   UPDATE <catalog>.control.ingestion_runs
   SET status = 'FAILED', ended_at = CURRENT_TIMESTAMP(),
       error_message = 'Manually closed: driver/cluster loss, see incident <ref>'
   WHERE run_id = '<run_id>';
   ```
4. Re-run the affected tables normally.

---

### 2.9 Target row counts look wrong vs Oracle (drift, not a failed check)

**Known limitation, not a bug:** deletes are not propagated (see
[README.md § Known limitations](../README.md#known-limitations)). If Oracle
rows were hard-deleted, Bronze will not reflect that, and
`reconciliation.row_count` will **not** catch it (it compares source vs
*this run's* load, not cumulative totals).

**Action:**
1. Confirm with the table owner whether the source table has physical
   deletes.
2. Interim options: an `extraction.filter` on a soft-delete/status column if
   one exists, or `mode: full` + `write_mode: overwrite` for small tables.
3. This is a product decision, not an on-call fix — escalate to the
   framework owner if it needs a permanent solution (§5, and see
   [OPEN_ITEMS.md](OPEN_ITEMS.md)).

---

## 3. Dashboard quick reference

All generated into `sql/monitoring/` by `tools/generate_bundle.py`. Wire
these into a Databricks SQL dashboard once at setup time (see
[OPEN_ITEMS.md](OPEN_ITEMS.md)).

| Query file | Answers | Check this when... |
|---|---|---|
| `run_success_rate.sql` | Daily run counts by status | Starting an incident triage — is this one table or everything? |
| `table_health.sql` | Per-table success rate (final attempts only) | Identifying a chronically unhealthy table |
| `freshness.sql` | Hours since each watermark advanced vs its SLA | A "data looks stale" complaint |
| `recent_failures.sql` | Last 50 failures with error text | Fast triage without writing SQL |
| `reconciliation_issues.sql` | Failed *and warned* quality checks | A "numbers look off" complaint — warnings don't stop a load and are easy to miss |
| `volume_trend.sql` | Rows written vs each table's own median | A "did something break upstream" question |
| `watermark_stalls.sql` | Tables whose last 5 runs moved nothing | §2.7 |
| `stuck_runs.sql` | Runs open past their timeout | §2.8 |
| `config_changes.sql` | When a table's effective config last changed | Correlating a volume/behaviour change with a deploy |

---

## 4. Common operator actions

### 4.1 Manually run one table

```bash
ingest run --table finance.gl_transactions --env prod
```

### 4.2 Run everything in a schedule group (e.g. after a fix)

```bash
ingest run --group finance_hourly --env prod
```

### 4.3 Backfill / correct a watermark

Use this — never a hand-written `UPDATE` — so the change is captured in the
audit trail as a `WATERMARK_FORCED` event.

```bash
ingest backfill --table finance.gl_transactions --env prod --from "2026-08-20 00:00:00" --yes
```

⚠️ **Read before running:** this rewinds the watermark and then re-runs the
table. On a `merge` target this is safe (idempotent — re-processing already-
loaded rows converges to the same state). On an `append` target it
**duplicates history** for the re-read window — the command will refuse for
tables with no watermark at all, but it does **not** currently warn
specifically for `append` targets (see [OPEN_ITEMS.md](OPEN_ITEMS.md)).
Confirm the target's `write_mode` before backfilling an append table.

### 4.4 Validate config before/after a change

```bash
ingest validate --env prod --strict
```

Exit code `0` = clean, `1` = errors (or warnings under `--strict`), `2` =
usage error (bad table name, unknown environment). This runs with **no
cluster and no credentials** — safe to run from a laptop or a CI gate.

### 4.5 See exactly what SQL a table will run, without touching anything

```bash
ingest show-sql --table finance.gl_transactions --env prod
```

Prints the actual Oracle SELECT, the staging statement (dedupe + audit
columns), and the Delta MERGE. This is the fastest way to answer "what does
this table actually do" during an investigation.

### 4.6 List every table and its dependency order

```bash
ingest list-tables --env prod
```

---

## 5. Escalation

| Situation | Escalate to |
|---|---|
| `ControlStateError`, or anything suggesting the control-plane state machine is wrong | Framework owner — this indicates a code bug, not a data/ops issue |
| A Delta multi-match / merge semantics failure that isn't explained by a non-unique key | Framework owner, with the `run_id` — this is exactly what `pytest -m spark` exists to prevent, so it needs a regression fix |
| Persistent reconciliation failures tied to real source data quality | Table owner (see `table.owner` in the table's config file) — this is a data problem, not an ops problem |
| A decision about the delete-propagation limitation, cross-group job dependencies, or any item in OPEN_ITEMS.md | Framework owner / whoever owns the roadmap decision |
| Oracle connectivity, credentials, listener issues | Oracle DBA team |
| Databricks cluster/workspace issues (stuck runs, permission errors unrelated to the framework) | Platform/Databricks admin team |

**Owner contacts:** ⚠️ not yet filled in — see
[OPEN_ITEMS.md](OPEN_ITEMS.md) item "escalation contacts". Table-level owners
are declared per table in `config/tables/<domain>/<table>.yaml` →
`table.owner`, but there is no single named framework owner or on-call
rotation recorded anywhere in this repository yet.
