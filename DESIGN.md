# Oracle → Databricks Ingestion Framework — Detailed Design

**Status:** Design draft (pre-implementation)
**Target platform:** Databricks (Lakehouse, Unity Catalog, Delta), source Oracle Data Warehouse
**Languages:** Python (orchestration, config, control/audit APIs) + Spark SQL / Oracle SQL (extract & load logic)

---

## 1. Goals & Principles

| Goal | How the framework delivers it |
|------|-------------------------------|
| **Config-driven onboarding** | A developer onboards a new table by adding one YAML file — no code changes. |
| **Hierarchical config** | `default template → table template → environment overrides`, merged additively with override semantics. |
| **Flexible extraction** | Full table, selective columns, selective rows (predicate), incremental (watermark on date/timestamp/SCN), and custom SQL. |
| **Operational rigor** | Central control plane, audit trail, structured logging, metrics/monitoring, and alerting. |
| **Idempotent & restartable** | Every run is tracked; failed/partial runs can be safely retried without duplication. |
| **Environment portability** | Same config runs across dev/test/prod with only environment parameter overrides. |

**Design tenets:** declarative over imperative; convention over configuration; fail loud, recover clean; everything observable; no hard-coded table logic.

---

## 2. High-Level Architecture

```
                         ┌────────────────────────────────────────────────┐
                         │              CONFIG LAYER (YAML)                 │
                         │  defaults.yaml  ▶  <table>.yaml  ▶  env/<env>.yaml│
                         └───────────────────────┬──────────────────────────┘
                                                 │ resolve & validate
                                                 ▼
   ┌──────────┐   JDBC   ┌────────────────────────────────────────┐   Delta   ┌───────────────┐
   │  Oracle  │◀────────▶│          INGESTION ENGINE (PySpark)      │─────────▶│  Bronze (raw) │
   │   DWH    │  extract │  ┌───────────┐ ┌──────────┐ ┌──────────┐ │  MERGE/   │  Delta tables │
   └──────────┘          │  │ Extractor │ │Transform │ │  Loader  │ │  APPEND   └───────┬───────┘
                         │  └───────────┘ └──────────┘ └──────────┘ │                   │
                         └───────┬─────────────────┬────────────────┘                   ▼
                                 │                  │                            Silver / curated
                    ┌────────────▼────────┐  ┌──────▼───────────┐              (downstream, optional)
                    │  CONTROL PLANE       │  │  OBSERVABILITY   │
                    │  (Delta control DB)  │  │  logs / metrics  │
                    │  - config registry   │  │  - structured log│
                    │  - watermarks        │  │  - run metrics   │
                    │  - run/batch state   │  │  - alerts        │
                    │  - audit log         │  │  - dashboards    │
                    └──────────────────────┘  └──────────────────┘
                                 ▲
                                 │
                    ┌────────────┴─────────┐
                    │  ORCHESTRATION        │
                    │  Databricks Workflows │
                    │  (Jobs / DAB / DLT)   │
                    └───────────────────────┘
```

**Layers**
1. **Config layer** — hierarchical YAML resolved into an effective per-table run spec.
2. **Ingestion engine** — reusable PySpark package: `Extractor → Transformer → Loader`, driven by the run spec.
3. **Control plane** — Delta tables in a dedicated `control` schema: config registry snapshot, watermarks, run/batch/task state, audit log.
4. **Observability** — structured JSON logging, run metrics table, monitoring dashboards, alert hooks.
5. **Orchestration** — Databricks Workflows / Asset Bundles that trigger the engine per table or per group.

---

## 3. Configuration Model (the heart of the framework)

### 3.1 Hierarchy & merge semantics

Three levels, merged in order (later wins). Merge is **deep**: scalars override, maps merge key-by-key, lists override by default but support explicit `merge`/`append` strategy tags.

```
1. defaults.yaml          (framework-wide defaults — every table inherits)
2. tables/<domain>/<table>.yaml   (table-specific — overrides defaults)
3. environments/<env>.yaml + per-table env block  (env-specific — overrides both)
```

Resolution rule:
```
effective = deep_merge(defaults, table_config)
effective = deep_merge(effective, env_overrides_for_this_table)
```

List merge strategies (declared via a small DSL):
- default → **replace**
- `!merge_by: <key>` → merge list-of-maps by matching key
- `!append` → concatenate

### 3.2 Directory layout

```
config/
├── defaults.yaml                     # global defaults
├── environments/
│   ├── dev.yaml
│   ├── test.yaml
│   └── prod.yaml                     # connection strings, secrets scopes, target catalogs
├── tables/
│   ├── finance/
│   │   ├── gl_transactions.yaml
│   │   └── gl_accounts.yaml
│   └── sales/
│       └── orders.yaml
└── schema/
    └── config.schema.json            # JSON Schema for validation
```

### 3.3 `defaults.yaml` (framework defaults)

```yaml
version: 1
source:
  type: oracle
  fetch_size: 10000
  jdbc:
    driver: oracle.jdbc.OracleDriver
    session_init_statement: "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
  read:
    num_partitions: 8              # JDBC read parallelism
    partition_column: null         # set per table for parallel reads
extraction:
  mode: full                       # full | incremental | query
  incremental:
    strategy: watermark            # watermark | scn | logminer(future)
    watermark_column: null
    watermark_type: timestamp      # timestamp | date | number
    overlap: "PT0S"                # ISO8601 lookback to catch late arrivals
    lower_bound_default: "1900-01-01 00:00:00"
  columns: "*"                     # "*" or explicit list
  filter: null                     # optional static WHERE predicate
target:
  layer: bronze
  format: delta
  write_mode: merge                # merge (default, 1:1 mirror) | append | overwrite
  merge_keys: []                   # required for merge; defaults to table.business_key
  merge_guard: watermark           # watermark | none — blocks out-of-order overwrites
  enable_change_data_feed: false   # opt-in change trail without losing the 1:1 mirror
  partition_by: []                 # usually empty — prefer cluster_by on merge_keys
  cluster_by: []                   # liquid clustering
  add_audit_columns: true          # _ingested_at, _run_id, _source_op, _batch_id
  schema_evolution: true
quality:
  row_count_reconciliation: true   # compare source count vs loaded
  fail_on_schema_drift: false
  null_check_keys: true
runtime:
  retries: 2
  retry_backoff_seconds: 30
  timeout_minutes: 120
logging:
  level: INFO
alerting:
  on_failure: ["email:data-eng-oncall"]
  on_reconciliation_mismatch: ["email:data-eng-oncall"]
```

### 3.4 Table config — `tables/finance/gl_transactions.yaml`

```yaml
version: 1
extends: defaults                  # explicit; optional (defaults always applied)
table:
  domain: finance
  source_schema: GLOWNER
  source_object: GL_TRANSACTIONS
  business_key: [TXN_ID]
source:
  read:
    num_partitions: 16
    partition_column: TXN_ID       # numeric → bounded parallel reads
extraction:
  mode: incremental
  columns:                         # selective columns
    - TXN_ID
    - ACCOUNT_ID
    - POSTED_DATE
    - AMOUNT
    - CURRENCY
    - LAST_UPDATE_DATE
  filter: "STATUS <> 'DELETED'"    # selective rows (predicate)
  incremental:
    watermark_column: LAST_UPDATE_DATE
    watermark_type: timestamp
    overlap: "PT6H"                # re-scan last 6h for late updates
target:
  write_mode: merge                # Bronze = 1:1 current-state mirror of Oracle
  merge_keys: [TXN_ID]             # defaults to table.business_key if omitted
  cluster_by: [TXN_ID]             # liquid clustering on the merge key — not partitioning
quality:
  expectations:                    # optional data-quality rules
    - column: AMOUNT
      rule: "not_null"
    - column: CURRENCY
      rule: "in_set"
      values: ["USD","EUR","GBP","INR"]
```

### 3.5 Environment overrides — `environments/prod.yaml`

```yaml
version: 1
env: prod
source:
  jdbc:
    url: "jdbc:oracle:thin:@//prod-oracle-scan:1521/DWHPRD"
  secret_scope: oracle-prod        # Databricks secret scope for user/pw
target:
  catalog: prod_lakehouse
  bronze_schema: bronze
control:
  catalog: prod_lakehouse
  schema: control
overrides:                         # per-table env-specific overrides
  finance.gl_transactions:
    source:
      read:
        num_partitions: 32         # more parallelism in prod
    runtime:
      timeout_minutes: 240
```

### 3.6 Extraction modes catalogue

| Mode | Config | Generated source read |
|------|--------|-----------------------|
| **Full table** | `mode: full`, `columns: "*"` | `SELECT * FROM schema.obj` |
| **Selective columns** | `columns: [A, B, C]` | `SELECT A,B,C FROM schema.obj` |
| **Selective rows** | `filter: "STATUS='ACTIVE'"` | `... WHERE STATUS='ACTIVE'` |
| **Incremental (timestamp/date)** | `mode: incremental`, `watermark_column` | `... WHERE wm_col > :last_wm - overlap` |
| **Incremental (SCN)** | `strategy: scn` | `... WHERE ORA_ROWSCN > :last_scn` |
| **Bounded parallel read** | `partition_column`, `num_partitions` | JDBC lower/upper bound partitioning |
| **Custom query** | `mode: query`, `query_file: sql/custom.sql` | Arbitrary Oracle SQL (templated with `:params`) |

All predicates compose: incremental watermark `AND` static filter `AND` selective columns, pushed down to Oracle.

### 3.7 Target write semantics — Bronze is a 1:1 current-state mirror of Oracle

**Decision:** Bronze holds exactly one row per business key, reflecting the latest state seen at source. Loads use Delta `MERGE` on `merge_keys`. Bronze is a faithful mirror of the Oracle table, so any future consumer can read it without understanding the ingestion mechanics.

- Default `write_mode: merge`; `merge_keys` defaults to `table.business_key` and is **required** (validation fails without it).
- Audit columns: `_ingested_at`, `_run_id`, `_batch_id` are refreshed on every insert **and** update; `_first_ingested_at` is set on insert only and never overwritten; `_source_op` is `I` on insert, `U` on update. No `D` — see §3.8.
- **No Silver current-state view is generated** — Bronze already *is* current state. Silver remains free for business modelling.

```sql
-- Generated MERGE, from merge_keys + column list
MERGE INTO {catalog}.bronze.gl_transactions AS t
USING staged_batch AS s
   ON t.TXN_ID = s.TXN_ID
WHEN MATCHED AND s.LAST_UPDATE_DATE >= t.LAST_UPDATE_DATE THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Three correctness points the implementation must handle:

1. **Dedupe the source batch before merging.** An incremental window (especially with `overlap`) can return several versions of the same key. Delta raises an error when multiple source rows match one target row, so the loader must reduce the batch to latest-per-key — `ROW_NUMBER() OVER (PARTITION BY merge_keys ORDER BY watermark_column DESC, ROWID DESC) = 1` — before the MERGE. This is a hard requirement, not an optimization.
2. **Guard the update against out-of-order data.** The `WHEN MATCHED AND s.watermark >= t.watermark` clause stops a backfill or a late replay from overwriting a newer row with an older one. Where a table has no watermark column (`mode: full`), the guard is omitted and last-write-wins.
3. **Prefer liquid clustering on `merge_keys` over partitioning.** Merge keys scatter across business-date partitions, so partitioning by business date makes MERGE scan everything. Default to `cluster_by: <merge_keys>` and leave `partition_by` empty unless a table has a genuinely partition-aligned load pattern.

`append` and `overwrite` remain supported per-table overrides — `overwrite` for small full-refresh dimensions, `append` for genuine event/fact streams where a key never updates. `merge` is the default and the recommended path.

**Re-run safety:** MERGE on `merge_keys` is naturally idempotent — re-processing the overlap window or retrying a failed task converges to the same target state. No pre-delete step is needed.

**If history is wanted later:** enable Delta **Change Data Feed** on the Bronze table (`delta.enableChangeDataFeed = true`, settable per table from config). That preserves a full change trail without giving up the 1:1 mirror, and is the designed path to SCD2 in Silver.

### 3.8 Delete policy — deletes are not propagated

**Decision:** the framework does **not** detect or propagate physical deletes from Oracle. Watermark-based incremental extraction cannot see a row that no longer exists, and no CDC (GoldenGate/LogMiner) layer is in scope.

**Status:** deferred pending input from the Oracle DBAs on which tables actually experience physical deletes. Revisit once that comes back.

Implications, stated explicitly so they are a conscious trade-off:
- A row hard-deleted in Oracle remains in the Bronze mirror indefinitely — so Bronze is 1:1 with Oracle for inserts and updates, but *not* for deletes.
- Target row counts drift above source over time for tables with physical deletes.
- Reconciliation (§6) compares source vs *loaded-this-run* counts, not total target counts — so it will not flag this drift.

Mitigations available per table via config today, without framework changes:
- `extraction.filter` on a source soft-delete/status column, where the source has one.
- `extraction.mode: full` + `write_mode: overwrite` on a periodic schedule for small tables — a full snapshot replaces the target wholesale, so deleted keys disappear naturally.

Designed upgrade paths once the DBA picture is clear (the extractor/loader interfaces leave room for both, no redesign required):
- **Full-refresh reconcile** — periodically pull the source key set and soft- or hard-delete target rows absent from it. Fits the merge model directly as a `WHEN NOT MATCHED BY SOURCE` clause.
- **CDC** (GoldenGate / LogMiner) — a new source type feeding `_source_op = D` into the same loader.

---

## 4. Control Plane (Delta tables in `control` schema)

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `config_registry` | Snapshot of resolved effective config per table+env per run (config-as-of) | `table_fqn, env, config_hash, resolved_json, effective_from` |
| `watermarks` | Current high-water mark per table+env | `table_fqn, env, watermark_column, watermark_value, watermark_type, updated_at, run_id` |
| `ingestion_runs` | One row per pipeline run (batch of tables) | `run_id, env, status, started_at, ended_at, triggered_by` |
| `ingestion_tasks` | One row per table per run | `run_id, table_fqn, status, mode, rows_read, rows_written, rows_inserted, rows_updated, source_count, error, started_at, ended_at` |
| `audit_log` | Immutable event stream (who/what/when) | `event_id, run_id, table_fqn, event_type, payload_json, actor, event_ts` |
| `reconciliation` | Source vs target counts & checks | `run_id, table_fqn, source_count, target_count, delta, status` |

**State machine per task:** `PENDING → RUNNING → (SUCCEEDED | FAILED | SKIPPED)`; watermark is committed **only** on `SUCCEEDED` in the same transaction boundary as the data write (write-then-commit-watermark, with run_id fencing for idempotency).

**Idempotency & restart:** each task keyed by `(run_id, table_fqn)`. On retry, the loader uses `MERGE` on `merge_keys` so re-processing the overlap window is safe. Watermark advances monotonically.

---

## 5. Ingestion Engine (Python package)

```
src/ingestion_framework/
├── config/
│   ├── loader.py          # discover + load YAML files
│   ├── merger.py          # hierarchical deep-merge + list strategies
│   ├── resolver.py        # produce effective RunSpec, inject env, secrets
│   └── validator.py       # JSON Schema + semantic validation
├── engine/
│   ├── run_spec.py        # dataclasses: RunSpec, SourceSpec, TargetSpec...
│   ├── extractor.py       # build Oracle SQL, JDBC read, partitioning
│   ├── transformer.py     # add audit cols, type/casing normalization
│   ├── loader.py          # Delta append/overwrite/merge, schema evolution
│   └── reconciler.py      # source/target counts, expectations
├── control/
│   ├── control_store.py   # CRUD over control Delta tables
│   ├── watermark.py       # read/advance watermarks
│   └── audit.py           # emit audit events
├── observability/
│   ├── logger.py          # structured JSON logging w/ run context
│   ├── metrics.py         # emit run metrics
│   └── alerts.py          # failure / mismatch notifications
├── orchestration/
│   ├── runner.py          # single-table run (entry point)
│   └── batch_runner.py    # group/domain run, dependency ordering
└── cli.py                 # `ingest --table finance.gl_transactions --env prod`
```

**Core flow (`runner.py`):**
```
1. resolve_config(table, env)          -> RunSpec
2. validate(RunSpec)
3. control.start_task(run_id, table)   -> audit: TASK_STARTED
4. wm = watermark.get(table, env)
5. sql = extractor.build_sql(RunSpec, wm)
6. df  = extractor.read(sql)           -> capture source_count
7. df  = transformer.apply(df, RunSpec)  # audit cols, casts
8. res = loader.write(df, RunSpec)       # append/overwrite/merge
9. reconciler.check(RunSpec, res)
10. watermark.advance(table, env, new_wm)  # only on success, fenced by run_id
11. control.finish_task(SUCCEEDED, metrics) -> audit: TASK_SUCCEEDED
    (on any error: control.finish_task(FAILED); alerts.notify)
```

---

## 6. Observability

- **Logging:** structured JSON, one logger with bound context (`run_id`, `table_fqn`, `env`, `stage`). Levels from config. Written to driver stdout (captured by Databricks) + optional log Delta table.
- **Metrics:** per task — `rows_read`, `rows_written`, `inserted/updated`, `bytes`, `duration_ms`, `source_count`, `reconciliation_delta`, `partitions`. Persisted to `ingestion_tasks` and optionally to a metrics sink (system tables / Lakehouse Monitoring).
- **Monitoring dashboards:** SQL dashboard on control tables — run success rate, freshness (max watermark vs now), row-volume trends, failure log, reconciliation mismatches.
- **Alerting:** pluggable channels (email, Slack, PagerDuty webhook) triggered on failure, reconciliation mismatch, or SLA/freshness breach.

---

## 7. Orchestration (Databricks Workflows)

- **Databricks Asset Bundle (DAB)** defines jobs; config-driven job generation (one task per table, grouped by domain).
- Two orchestration styles supported:
  1. **Per-table tasks** in a Workflow with a shared cluster/job pool; dependency edges from config (`depends_on`).
  2. **Single parameterized job** looping over a table list (`batch_runner`) for large fan-out.
- Scheduling, concurrency limits, retries, and notifications set from config → rendered into the bundle.
- **Databricks Workflows is the only orchestrator.** No Airflow adapter or external trigger API is built. (A DLT path for streaming/CDC remains a future extension, out of scope now.)

---

## 8. Onboarding a New Table (developer experience)

```
1. Copy an existing table YAML into config/tables/<domain>/<table>.yaml
2. Set source_schema, source_object, business_key, columns, filter, mode, watermark_column.
3. (If needed) add per-table override block in environments/<env>.yaml.
4. Run:  ingest validate --table <domain>.<table>
5. Run:  ingest --table <domain>.<table> --env dev   (dry-run supported)
6. Add table to the workflow group (or it's auto-discovered).
```
No Python changes. CI validates every YAML against JSON Schema + semantic rules on PR.

---

## 9. Security & Secrets
- Oracle credentials via **Databricks Secret Scopes** (referenced by name in env config; never in YAML).
- Unity Catalog governs target tables; least-privilege service principal for the job.
- PII columns can be tagged in config for masking/tokenization in the transformer (future hook).

---

## 10. Confirmed Decisions

| # | Decision | Choice | Consequence in the build |
|---|----------|--------|--------------------------|
| 1 | Catalog model | **Unity Catalog** | Three-level names `catalog.schema.table` everywhere; control plane at `{catalog}.control.*`; liquid clustering available; job runs as a least-privilege service principal. No `hive_metastore` code paths. |
| 2 | CDC depth | **Watermark only** (timestamp / date / number / `ORA_ROWSCN`) | No GoldenGate or LogMiner integration. Extractor interface stays open for a future CDC source type. |
| 3 | Deletes | **Not propagated** | See §3.8. Documented drift; per-table `filter` on a soft-delete column is the available workaround. |
| 4 | Historization | **Bronze = 1:1 current-state mirror** (revised 2026-08-24) | See §3.7. Default `write_mode: merge` on `business_key`; source batch deduped to latest-per-key before MERGE; watermark guard against out-of-order updates; liquid clustering on merge keys. No Silver current-state view needed. Delta CDF is the opt-in path if history is wanted later. |
| 5 | Orchestration | **Databricks Workflows only** | Asset Bundles generate Jobs from config. No Airflow adapter, no external trigger layer. |

### Still open (assumptions applied — correct me if wrong)

| Item | Assumption used | Why it matters |
|------|-----------------|----------------|
| Largest table volume / daily delta | Assume largest table ≤ ~500M rows, daily delta ≤ ~50M rows | Drives default `num_partitions` (8, overridable to 32), partition strategy, and cluster sizing guidance in the README. |
| Oracle version | Assume 19c+ | `ORA_ROWSCN` behavior, `FETCH FIRST` syntax availability in the SQL builder. |
| Databricks Runtime | Assume DBR 14.3 LTS+ | Liquid clustering, `DeltaTable` API surface, Python version in the package metadata. |

These three only affect defaults and documentation — none of them block starting the build, and each is a one-line config change if the real numbers differ.

See [docs/OPEN_ITEMS.md](docs/OPEN_ITEMS.md) for the complete, current list of everything — these three plus every deployment placeholder and deferred product decision — that needs a human to resolve before production use.
```
