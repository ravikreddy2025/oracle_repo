# Build Prompt — Oracle → Databricks Ingestion Framework

> Paste this prompt (with the "Decisions" section filled in) to drive implementation. It assumes `DESIGN.md` in the same repo is the source of truth for architecture.

---

## Role & Objective
You are a senior data platform engineer. Build a **production-grade, config-driven ingestion framework** that moves data from an **Oracle Data Warehouse** into **Databricks (Delta / Unity Catalog)**. Follow `DESIGN.md` exactly for architecture, control-plane schema, and config hierarchy. Deliver Python + Spark SQL / Oracle SQL. A developer must be able to **onboard a new table by adding a single YAML file — no code changes.**

## Non-negotiable Requirements
1. **Hierarchical config** merged in order `defaults.yaml → tables/<domain>/<table>.yaml → environments/<env>.yaml (per-table override block)`. Deep merge: scalars override, maps merge, lists replace by default with `!merge_by:<key>` and `!append` strategies. Produce one immutable **effective RunSpec** per (table, env).
2. **Validation**: JSON Schema for structure + semantic checks (e.g. `write_mode: merge` requires `merge_keys`; `mode: incremental` requires `watermark_column`). Fail fast with clear messages. Provide `ingest validate`.
3. **Extraction modes** (all pushed down to Oracle): full table, selective columns, selective rows (predicate), incremental by timestamp/date/number watermark with configurable overlap, incremental by `ORA_ROWSCN`, custom SQL file, and JDBC bounded parallel reads (`partition_column`, lower/upper bound, `num_partitions`).
4. **Load modes**: Delta `merge` (**default** — Bronze is a 1:1 current-state mirror of Oracle), `append`, `overwrite`. `merge_keys` defaults to `table.business_key` and is required for merge. Three things the loader MUST get right:
   - **Dedupe the source batch to latest-per-key before MERGE** (`ROW_NUMBER() OVER (PARTITION BY merge_keys ORDER BY watermark DESC)`) — the incremental `overlap` window returns multiple versions of a key and Delta errors on multi-match. Hard requirement, must have a dedicated test.
   - **Watermark guard on update** (`WHEN MATCHED AND s.wm >= t.wm`) so backfills/replays can't overwrite newer rows with older; omitted when `merge_guard: none` or the table has no watermark.
   - **Liquid clustering on `merge_keys` by default**, `partition_by` empty unless explicitly configured — partitioning by business date defeats MERGE pruning.
   Stamp `_ingested_at, _run_id, _batch_id, _source_op` on insert and update; `_first_ingested_at` on insert only, never overwritten. Support schema evolution and per-table `enable_change_data_feed`. Do NOT generate a Silver current-state view — Bronze is current state. `append`/`overwrite` remain per-table overrides and must still be correct and idempotent.
4b. **Deletes are explicitly NOT propagated.** Do not build delete detection, full-refresh reconcile, or CDC — this is deferred pending DBA input. Keep the loader's merge-clause construction structured so a `WHEN NOT MATCHED BY SOURCE` clause can be added later without a rewrite. Document the drift caveat in the README where a user will actually hit it.
5. **Control plane** — implement all Delta control tables from DESIGN.md §4 (`config_registry, watermarks, ingestion_runs, ingestion_tasks, audit_log, reconciliation`). Task state machine `PENDING→RUNNING→SUCCEEDED|FAILED|SKIPPED`. **Advance watermark only on success**, fenced by `run_id`, committed after the data write.
6. **Auditing**: immutable event stream (`TASK_STARTED`, `EXTRACT_DONE`, `LOAD_DONE`, `WATERMARK_ADVANCED`, `TASK_SUCCEEDED/FAILED`) with payloads.
7. **Logging**: structured JSON logger with bound context (`run_id, table_fqn, env, stage`), level from config.
8. **Monitoring/metrics**: persist per-task metrics (rows_read/written/inserted/updated, duration, source_count, reconciliation delta) and provide a SQL dashboard definition (success rate, freshness, volume trend, failures).
9. **Reconciliation**: source count vs loaded count; configurable expectations (not_null, in_set, etc.); alert on mismatch.
10. **Alerting**: pluggable channels (email/Slack/webhook) on failure, reconciliation mismatch, freshness breach.
11. **Orchestration**: Databricks Asset Bundle (DAB) + Workflows **only** — no Airflow adapter, no external trigger API. Per-table tasks and a batch runner over a table list; retries/timeouts/concurrency/schedule from config; support `depends_on` ordering. All target and control objects use **Unity Catalog three-level names**; no `hive_metastore` code paths.
12. **Secrets**: Oracle creds only via Databricks Secret Scopes referenced by env config — never in YAML or code.

## Deliverables
- Python package `src/ingestion_framework/` with the exact module layout in DESIGN.md §5.
- `config/` tree with `defaults.yaml`, `environments/{dev,test,prod}.yaml`, `tables/` examples for **one incremental merge table** (the primary path), **one full-load table**, and **one table using an `append`/`overwrite` override** (proving the override path), plus `schema/config.schema.json`.
- SQL/DDL for all control tables (idempotent `CREATE ... IF NOT EXISTS`, Unity Catalog three-level names).
- CLI (`cli.py`): `ingest run --table <fqn> --env <env> [--dry-run]`, `ingest validate`, `ingest backfill --from --to`, `ingest init-control`.
- Databricks Asset Bundle (`databricks.yml` + `resources/*.yml`) generating jobs from config.
- Unit tests: config merge, schema validation, SQL builder for every extraction mode, watermark advance/fence logic, merge idempotency (re-run produces no dupes). Use `pytest` + a local Spark/Delta session; mock JDBC.
- `README.md`: onboarding steps, config reference table, extraction-mode examples, run/troubleshoot guide.

## Engineering Standards
- Type-hinted Python, dataclasses/pydantic for RunSpec, small pure functions for SQL building (unit-testable without a cluster).
- No hard-coded table logic anywhere — behavior comes only from config.
- Idempotent and restartable; safe to re-run any task.
- Clear separation: `extractor` builds SQL & reads; `transformer` shapes; `loader` writes; `control` records; `observability` reports. No cross-talk.
- Handle Oracle specifics: NLS date/timestamp formatting, `ORA_ROWSCN`, NUMBER precision → Spark decimal mapping, LOB handling, case-sensitivity of identifiers.

## Build Order (do it in phases, show me each before proceeding)
1. Config model: loader + deep-merge + resolver + RunSpec + JSON Schema + validator + tests.
2. Control plane DDL + `control_store` + `watermark` + `audit` + tests.
3. Extractor (SQL builder for all modes) + JDBC read + tests (SQL assertions).
4. Transformer + Loader (append/overwrite/merge, schema evolution) + merge-idempotency tests.
5. Runner + reconciler + observability (logging/metrics/alerts).
6. Batch runner + CLI.
7. Databricks Asset Bundle + Workflows + monitoring dashboard.
8. README + end-to-end example run (mocked source).

## Decisions (CONFIRMED — build to these, do not re-litigate; see DESIGN.md §10)
- Catalog model: **Unity Catalog** (three-level names throughout)
- CDC depth: **Watermark only** — timestamp / date / number / `ORA_ROWSCN`. No GoldenGate, no LogMiner.
- Deletes: **Not propagated.** No delete detection of any kind. Document the drift caveat.
- Historization: **Bronze is a 1:1 current-state mirror of Oracle.** `merge` is the default `write_mode`. No Silver current-state view. Delta CDF is the opt-in path if a change trail is needed later.
- Orchestrator: **Databricks Workflows only.**

Assumptions applied where unspecified (affect defaults/docs only — flag them in the README, don't block on them):
- Largest table ≤ ~500M rows, daily delta ≤ ~50M rows → default `num_partitions: 8`, overridable.
- Oracle 19c+ (`ORA_ROWSCN`, `FETCH FIRST` available to the SQL builder).
- DBR 14.3 LTS+ (liquid clustering, current `DeltaTable` API).

## Acceptance Criteria
- Adding a new YAML under `config/tables/` and (optionally) an env override block is the ONLY change needed to onboard a table.
- `ingest validate` catches all invalid configs with actionable errors.
- Every extraction mode produces correct, pushed-down Oracle SQL (proven by tests).
- Re-running any task twice yields identical target state (merge idempotency test passes, including across the incremental `overlap` window).
- A source batch containing multiple versions of the same key merges without error and leaves exactly one row per key, holding the **latest** version (dedupe-before-merge test passes).
- An out-of-order/replayed batch carrying older watermark values does **not** overwrite newer target rows (merge-guard test passes).
- Watermark never advances on failure; retries never duplicate rows.
- Control/audit/reconciliation tables populate on every run; dashboard renders.
