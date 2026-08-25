# Oracle → Databricks Ingestion Framework

Config-driven ingestion from an Oracle Data Warehouse into Databricks Delta.
**Onboarding a table means adding one YAML file** — no code changes, no job
edits, no dashboard edits.

- **Bronze is a 1:1 current-state mirror of Oracle**, maintained with `MERGE`.
- **Config is hierarchical**: `defaults → table → environment`, deep-merged.
- **Everything is recorded**: run/task state, watermarks, an immutable audit
  trail, reconciliation results, and the exact config each run used.

See [DESIGN.md](DESIGN.md) for the architecture and the reasoning behind each
decision.

### Documentation index

| Document | For | Answers |
|---|---|---|
| **README.md** *(this file)* | Everyone | How to onboard a table, run it, and configure it |
| [DESIGN.md](DESIGN.md) | Architects, reviewers | Why each architectural decision was made |
| [docs/CODEMAP.md](docs/CODEMAP.md) | Developers | Where a given piece of behavior lives in the code |
| [docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md) | Contributors | How to extend the framework, the rules the codebase follows, how to run the Spark-backed tests |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Support / on-call | Symptom → diagnosis → action for a paged failure |
| [docs/OPEN_ITEMS.md](docs/OPEN_ITEMS.md) | Everyone, especially before go-live | Every placeholder, assumption, and deferred decision that needs a human to resolve it |

⚠️ **Before any production deployment, read
[docs/OPEN_ITEMS.md](docs/OPEN_ITEMS.md).** It lists every workspace URL,
credential, and business decision still marked as a placeholder in this
repository.

---

## Quick start

```bash
pip install -e ".[dev]"
```

Validate the config tree — no cluster, no database, no credentials needed:

```bash
python -m ingestion_framework.cli --config-root config validate --env prod
```

See exactly what a table would run:

```bash
python -m ingestion_framework.cli --config-root config show-sql --table finance.gl_transactions --env prod
```

Watch a whole run happen against stubbed infrastructure:

```bash
python examples/simulated_run.py --env dev
```

---

## Onboarding a new table

1. **Copy an example** into `config/tables/<domain>/<table>.yaml`. The directory
   is the source of truth for the domain, and the filename for the table name.
2. **Fill in the source**: `source_schema`, `source_object`, `business_key`.
3. **Choose an extraction mode** (see the table below).
4. **Add environment overrides** in `config/environments/<env>.yaml` under
   `overrides:` if this table needs different tuning per environment.
5. **Validate**: `ingest validate --env dev --table <domain>.<table>`
6. **Dry run**: `ingest run --table <domain>.<table> --env dev --dry-run`
7. **Regenerate the job**: `python tools/generate_bundle.py --env prod`, then
   `databricks bundle deploy -t prod`.

Steps 1–6 need nothing but Python. The table is discovered by the filesystem,
so nothing else registers it.

### A minimal table config

```yaml
version: 1
table:
  domain: finance
  source_schema: GLOWNER
  source_object: GL_ACCOUNTS
  business_key: [ACCOUNT_ID]
extraction:
  mode: full
target:
  merge_guard: none      # no watermark to order two versions of a row by
```

Everything else is inherited from [config/defaults.yaml](config/defaults.yaml).

---

## Extraction modes

| Goal | Config | Generated Oracle SQL |
|---|---|---|
| Whole table | `mode: full` | `SELECT * FROM GLOWNER.GL_ACCOUNTS` |
| Selected columns | `columns: [A, B, C]` | `SELECT A, B, C FROM ...` |
| Selected rows | `filter: "STATUS <> 'DELETED'"` | `... WHERE (STATUS <> 'DELETED')` |
| Incremental by timestamp/date | `mode: incremental` + `watermark_column` | `... WHERE COL >= TO_TIMESTAMP(...) AND COL < TO_TIMESTAMP(...)` |
| Incremental by SCN | `strategy: scn` | `SELECT *, ORA_ROWSCN ... WHERE ORA_ROWSCN > 58100000` |
| Parallel read | `num_partitions` + `partition_column` | bounded `dbtable` subquery |
| Anything else | `mode: query` + `query_file` | your SQL, with `:lower_bound` / `:upper_bound` substituted |

All of these compose: selective columns **and** a filter **and** an incremental
predicate all push down to Oracle in one query.

### Incremental extraction, in detail

```yaml
extraction:
  mode: incremental
  columns: [TXN_ID, AMOUNT, STATUS, LAST_UPDATE_DATE]
  filter: "STATUS <> 'DELETED'"
  incremental:
    watermark_column: LAST_UPDATE_DATE
    watermark_type: timestamp     # timestamp | date | number
    overlap: PT6H                 # re-scan this far back for late updates
    bound_inclusive: auto         # auto | true | false
    use_upper_bound: true
```

Three things worth understanding:

**`overlap` rewinds the lower bound** so rows updated during the previous run's
read are picked up. Safe because a merge target is idempotent. It is a duration,
so it does **not** apply to numeric/SCN watermarks — the framework reports
`overlap_applied: false` rather than inventing arithmetic.

**`use_upper_bound` pins the batch ceiling from Oracle's own clock**
(`SYSTIMESTAMP`, or `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` for SCN). With
parallel reads Spark issues one query per partition at slightly different
moments; without a pinned ceiling those partitions see different cuts of the
table and boundary rows get double-counted or missed. The pinned value also
becomes the next watermark, so runs resume exactly where the last one stopped.

> ⚠️ SCN tables need `EXECUTE` on `DBMS_FLASHBACK` for the service account. If
> your DBAs won't grant it, set `use_upper_bound: false` and the framework falls
> back to `MAX(ORA_ROWSCN)` from the data.

**`bound_inclusive: auto`** picks `>=` for merge targets and `>` for append.
Neither is free: `>` can lose a row committed after your read but stamped with
the same second; `>=` re-reads boundary rows every run (harmless when merging,
duplicates when appending).

---

## Write modes

| Mode | When | Behaviour |
|---|---|---|
| `merge` *(default)* | Bronze mirror | One row per business key, latest state |
| `append` | Immutable event streams | Every row kept; no dedupe |
| `overwrite` | Small full-refresh dimensions | Target replaced wholesale |

The merge path does three things that matter:

1. **Deduplicates the batch to latest-per-key before merging.** An overlap
   window routinely returns several versions of a key, and Delta errors when
   multiple source rows match one target row. This is what makes incremental
   loads work at all.
2. **Guards the update with the watermark** (`merge_guard: watermark`), so a
   backfill or replay cannot overwrite a newer row with an older one.
3. **Clusters on the merge keys** rather than partitioning by date — merge keys
   scatter across date partitions, so partitioning would defeat pruning.

### Audit columns

Every row gets these unless `add_audit_columns: false`:

| Column | Set on insert | Set on update |
|---|---|---|
| `_ingested_at`, `_ingested_date`, `_run_id`, `_batch_id` | yes | refreshed |
| `_first_ingested_at` | yes | **never touched** |
| `_source_op` | `'I'` | `'U'` |

---

## Configuration hierarchy

Four layers, deep-merged, later wins:

```
1. config/defaults.yaml
2. config/tables/<domain>/<table>.yaml
3. config/environments/<env>.yaml            (global block)
4. config/environments/<env>.yaml            overrides."<domain>.<table>"
```

Scalars override, maps merge key-by-key, lists replace. Two tags change list
behaviour on the overriding side:

```yaml
alerting:
  on_failure: !append ["slack:#data-alerts"]     # base + this

quality:
  expectations: !merge_by:column                 # merge entries by "column"
    - column: AMOUNT
      action: warn
```

Strings support `${...}` interpolation against `env`, `domain`, `table`,
`table_fqn`, or any dotted config path (`${target.catalog}`).

### Config reference

<details>
<summary><b>source</b> — connection and read behaviour</summary>

| Key | Default | Meaning |
|---|---|---|
| `type` | `oracle` | Source system |
| `fetch_size` | `10000` | JDBC fetch size |
| `secret_scope` | — | **Required.** Databricks secret scope holding credentials |
| `secret_keys.username` / `.password` | `username` / `password` | Keys within the scope |
| `jdbc.url` | — | **Required.** Set per environment |
| `jdbc.session_init_statement` | NLS formats | Runs on each connection |
| `jdbc.options` | `{}` | Extra Spark JDBC options |
| `custom_schema` | `{}` | Force Spark type mapping (bare Oracle `NUMBER`) |
| `read.num_partitions` | `1` | Parallel JDBC readers |
| `read.partition_column` | `null` | Numeric column to split on |
| `read.bounds_strategy` | `auto` | `auto` probes MIN/MAX; `explicit` uses your bounds |
</details>

<details>
<summary><b>extraction</b> — what to read</summary>

| Key | Default | Meaning |
|---|---|---|
| `mode` | `full` | `full` \| `incremental` \| `query` |
| `columns` | `"*"` | `"*"` or an explicit list of identifiers |
| `exclude_columns` | `[]` | Removed from an explicit list |
| `filter` | `null` | WHERE fragment, ANDed with the incremental predicate |
| `query_file` | `null` | Path to a `.sql` file, for `mode: query` |
| `row_limit` | `null` | `FETCH FIRST n ROWS ONLY` |
| `incremental.strategy` | `watermark` | `watermark` \| `scn` |
| `incremental.watermark_column` | `null` | Required for `strategy: watermark` |
| `incremental.watermark_type` | `timestamp` | `timestamp` \| `date` \| `number` |
| `incremental.overlap` | `PT0S` | ISO-8601 lookback |
| `incremental.lower_bound_default` | `1900-01-01 00:00:00` | First-run floor; must match `watermark_type` |
| `incremental.bound_inclusive` | `auto` | `auto` \| `true` \| `false` |
| `incremental.use_upper_bound` | `true` | Pin the ceiling from the source clock |
</details>

<details>
<summary><b>target</b> — where it lands</summary>

| Key | Default | Meaning |
|---|---|---|
| `catalog` / `schema` | per environment | Unity Catalog location |
| `table_name` | lower-cased `source_object` | Target table |
| `write_mode` | `merge` | `merge` \| `append` \| `overwrite` |
| `merge_keys` | `table.business_key` | Required for merge |
| `merge_guard` | `watermark` | `watermark` \| `none` |
| `partition_by` | `[]` | Usually empty — prefer clustering |
| `cluster_by` | `merge_keys` | Liquid clustering |
| `column_case` | `preserve` | `preserve` (true mirror) \| `lower` |
| `enable_change_data_feed` | `false` | Opt-in change trail |
| `schema_evolution` | `true` | Auto-merge new columns |
| `add_audit_columns` | `true` | Stamp the `_`-prefixed columns |
</details>

<details>
<summary><b>quality, runtime, alerting, schedule</b></summary>

| Key | Default | Meaning |
|---|---|---|
| `quality.row_count_reconciliation` | `true` | Source vs target counts |
| `quality.null_check_keys` | `true` | NULL merge keys can never match |
| `quality.expectations[]` | `[]` | `not_null`, `unique`, `in_set`, `min`, `max`, `regex`; `action: fail\|warn` |
| `runtime.enabled` | `true` | `false` skips the table |
| `runtime.retries` | `2` | Framework retries; each is a new attempt row |
| `runtime.retry_backoff_seconds` | `30` | |
| `runtime.timeout_minutes` | `120` | Becomes the job task timeout |
| `alerting.on_failure` | `[]` | `email:`, `slack:`, `webhook:` channels |
| `alerting.on_reconciliation_mismatch` | `[]` | |
| `alerting.freshness_sla_hours` | `null` | Feeds the freshness dashboard |
| `schedule.group` | `default` | One Databricks job per group |
| `schedule.cron` | `null` | Quartz expression |
| `schedule.job_retries` | `0` | Job-level retries — see below |
</details>

---

## Running

```bash
ingest run --table finance.gl_transactions --env prod
```
```bash
ingest run --group finance_hourly --env prod --max-workers 4
```

| Command | Needs a cluster? | Purpose |
|---|---|---|
| `validate` | no | Structural + semantic config checks |
| `show-sql` | no | Print the SQL a table would run |
| `list-tables` | no | Tables and their dependency order |
| `run --dry-run` | no | Resolve, validate, plan; touch nothing |
| `run` | yes | Ingest |
| `init-control` | yes (`--dry-run`: no) | Create the control schema |
| `backfill` | yes | Rewind a watermark, then re-run |

Exit codes: `0` success, `1` failure, `2` usage/config error — so `validate` can
gate a CI pipeline.

---

## The control plane

Six Delta tables in `<catalog>.control` ([DDL](sql/control/)):

| Table | Holds |
|---|---|
| `ingestion_runs` | One row per run: `SUCCEEDED` / `PARTIAL` / `FAILED` |
| `ingestion_tasks` | One row per table per **attempt**, with metrics |
| `watermarks` | Current high-water mark per table and environment |
| `audit_log` | Immutable event stream |
| `reconciliation` | Count and expectation results |
| `config_registry` | Every effective config ever run, by content hash |

**The ordering guarantee.** The watermark advances *after* the load commits and
*only* if the task succeeded — including reconciliation. Advance-then-load loses
the batch on failure and the next run resumes past rows that were never written.
A mark that has moved past rows we aren't sure landed is worse than a run that
stops and tells someone.

**Retries append attempts.** A terminal task never reopens; a retry is
`attempt = n + 1`, so the history of what actually happened survives.

---

## Monitoring

`tools/generate_bundle.py` emits [sql/monitoring/](sql/monitoring/) — nine
queries for a Databricks SQL dashboard:

| Query | Answers |
|---|---|
| `run_success_rate` | Is anything failing, and how badly? |
| `table_health` | Which table is unhealthy? (final attempts only) |
| `freshness` | Is anything stale vs its declared SLA? |
| `recent_failures` | What broke, with the error and the window |
| `reconciliation_issues` | What failed or *warned* a quality check |
| `volume_trend` | Did volumes move vs each table's own median? |
| `watermark_stalls` | Successful runs that moved nothing — a filter matching zero rows looks exactly like a quiet source |
| `stuck_runs` | Runs left open past their timeout |
| `config_changes` | Did something change under me? |

---

## Deployment

```bash
python tools/generate_bundle.py --env prod
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

One job per `schedule.group`, one task per table, `depends_on` becoming task
edges. Two generated settings are correctness, not preference:

- **`max_concurrent_runs: 1`** — two concurrent runs of one table would race
  each other's watermark.
- **`max_retries: 0`** — the framework retries source errors itself and records
  each attempt. Job retries on top would multiply attempts (3 × 3 = 9) and blur
  the audit trail. Raise `schedule.job_retries` where *driver loss* is the
  concern; that's the one failure the framework can't retry itself.

Before your first deploy, fill in the placeholders in
[databricks.yml](databricks.yml): workspace `host` URLs, the prod
`service_principal`, and the node types (currently Azure-flavoured).

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `merge_keys is required` | `write_mode: merge` with no keys | Set `table.business_key` |
| `watermark_column ... is not in extraction.columns` | Selective projection omits the watermark | Add it to `columns` |
| `overlap only applies to incremental` | `overlap` set on a full load | Remove it, or switch to `incremental` |
| `'...' is not a canonical timestamp; refusing to build SQL` | A non-canonical bound reached the SQL builder | Expected — the value cannot be bound as a parameter on the JDBC path, so anything non-canonical is refused rather than escaped |
| `lower_bound_default ... is not a valid number` | Timestamp floor inherited by an SCN table | Set a numeric `lower_bound_default` |
| Delta multi-match error on merge | Dedupe skipped or keys not unique | Check `duplicate_keys` in `reconciliation` |
| Task fails on `reconciliation failed` | Counts didn't balance | Query `reconciliation`; the watermark held, so re-running is safe |
| Watermark never advances | Every batch empty | Check `watermark_stalls`; a filter matching nothing looks identical to a quiet source |
| Target row count drifts above source | Deletes aren't propagated | Known limitation — see below |

---

## Testing

```bash
pytest -q
```

591 tests run anywhere. **15 more need a working Spark + Delta session**
(`pytest -m spark`) — they assert Delta *behaviour*, not just generated SQL
text: that the pre-merge dedupe prevents Delta's multi-match error, that a
re-run converges, that the watermark guard rejects an older replay, that
`_first_ingested_at` survives an update.

✅ **All 606 tests pass, including the 15 Delta-backed ones** — verified
against real Delta MERGE, not just asserted SQL text. On native Windows this
needs a small filesystem shim (`tools/localspark/`) because Hadoop's local
filesystem code otherwise shells out to `winutils.exe`, which this project
avoids installing; see [docs/DEVELOPER_GUIDE.md §5](docs/DEVELOPER_GUIDE.md#5-testing-against-real-sparkdelta-pytest--m-spark)
for what the shim does, what it does not change (nothing in Delta's commit
protocol or MERGE semantics), and how to run these on Linux/Databricks where
no shim is needed at all.

---

## Known limitations

**Deletes are not propagated.** Watermark-based extraction cannot see a row that
no longer exists, and no CDC layer is in scope. So Bronze is 1:1 with Oracle for
inserts and updates but **not** for deletes: hard-deleted rows stay forever, and
target counts drift above source. Reconciliation compares source vs
*loaded-this-run*, so it will not flag this drift. Deferred pending DBA input on
which tables actually experience physical deletes. Interim workarounds:
`extraction.filter` on a soft-delete column, or `mode: full` + `overwrite` for
small tables. The upgrade path (`WHEN NOT MATCHED BY SOURCE`) drops into the
merge model without a redesign.

**Cross-group dependencies can't be task edges.** A `depends_on` that crosses a
`schedule.group` boundary is reported as a warning and written into the job's
description, but separate jobs can't express that ordering. Move the tables into
one group.

**Column expressions need `mode: query`.** `extraction.columns` accepts plain
identifiers only.

See [docs/OPEN_ITEMS.md](docs/OPEN_ITEMS.md) for these plus every other item
— placeholder credentials, unconfirmed assumptions, deferred product
decisions — that needs a human decision before production use.

---

## Layout

```
config/            the source of truth — tables, environments, defaults, schema
src/ingestion_framework/
  config/          load, deep-merge, resolve, validate
  engine/          run_spec, sql_builder, extractor, transformer, loader, reconciler
  control/         schema (DDL), control_store, watermark, audit, sql_client
  observability/   logger, alerts, monitoring
  orchestration/   runner, batch_runner, bundle, factory
  cli.py
tools/             emit_control_ddl, generate_bundle  (derived artefacts)
  localspark/      TEST-ONLY Windows Spark/Delta shim — see its own README
examples/          simulated_run.py
sql/               GENERATED — control DDL and dashboard queries
resources/         GENERATED — Databricks job definitions
docs/              codemap, developer guide, operations runbook, open items
```
