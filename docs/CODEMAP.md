# Code Map — where things live and how they connect

A navigation guide, not a design doc. For *why* a decision was made, see
[DESIGN.md](../DESIGN.md). For *how to run it*, see [README.md](../README.md).
This page answers "which file do I open".

---

## 1. Repository layout

```
config/                     THE source of truth. Everything else is derived from this.
  defaults.yaml                framework-wide defaults
  environments/{dev,test,prod}.yaml
  tables/<domain>/<table>.yaml one file per table — this is what a developer adds
  schema/config.schema.json    JSON Schema used by the validator

src/ingestion_framework/    the Python package
  config/                      load YAML -> merge hierarchy -> validate -> RunSpec
  engine/                      per-table logic: SQL building, extract, stage, load, reconcile
  control/                     control-plane state: DDL, watermarks, audit, run/task store
  observability/               logging, alert dispatch, dashboard SQL
  orchestration/               wiring: single-table runner, multi-table batch runner,
                                Databricks bundle generation, live-Spark factory
  cli.py                       `ingest` command line — the only supported entry point

tools/                       one-shot generators, NOT part of the runtime
  emit_control_ddl.py           control/schema.py -> sql/control/*.sql
  generate_bundle.py            config/ -> resources/*.yml + sql/monitoring/*.sql
  localspark/                   TEST-ONLY Windows Spark shim (see its own README)

tests/                       591 tests that run anywhere + 15 that need real Spark/Delta
examples/simulated_run.py    end-to-end walkthrough with Oracle/Delta stubbed out

sql/                         GENERATED — do not hand-edit, see header of each file
resources/                   GENERATED — Databricks job YAML
databricks.yml               Asset Bundle root (deployment settings only, hand-maintained)

README.md                    usage, onboarding, config reference
DESIGN.md                    architecture and the reasoning behind each decision
BUILD_PROMPT.md              the original spec this was built against
docs/                        this folder — codemap, runbook, dev guide, open items
```

---

## 2. The one-sentence job of every module

| Module | Job |
|---|---|
| `config/loader.py` | Parse one YAML file. Reject duplicate keys and malformed merge tags. |
| `config/merger.py` | Deep-merge two config layers. Knows `!append` and `!merge_by:<key>`. |
| `config/resolver.py` | Merge all four layers for one (table, env); apply derived defaults; interpolate `${...}`. |
| `config/validator.py` | JSON Schema + ~30 semantic rules. Never raises — returns a report. |
| `config/__init__.py` | `build_run_spec()` — the one function everything else calls to go from files to a typed spec. |
| `engine/run_spec.py` | Frozen dataclasses. The typed contract every other module reads. |
| `engine/sql_builder.py` | Pure functions: RunSpec + bounds → the exact Oracle SELECT. |
| `engine/extractor.py` | Talks to Oracle over JDBC: credentials, bound probing, the actual read. |
| `engine/transformer.py` | One SQL statement: audit columns + the dedupe a merge target depends on. |
| `engine/loader.py` | Pure functions building MERGE / CREATE TABLE SQL, plus the thin executor. |
| `engine/reconciler.py` | Source-vs-target counts, duplicate/null-key checks, quality expectations. |
| `control/schema.py` | The six control table definitions, as data — DDL is generated from this, not hand-written. |
| `control/watermark.py` | Canonical watermark storage, window arithmetic, the fenced/guarded advance. |
| `control/control_store.py` | Run/task state machine, config registry, reconciliation writes. |
| `control/audit.py` | Buffered, append-only event stream. |
| `control/sql_client.py` | The only seam that touches Spark SQL directly — bound parameters, never string concat. |
| `observability/logger.py` | Structured JSON logs with bound context. |
| `observability/alerts.py` | `kind:target` channel parsing + dispatch. |
| `observability/monitoring.py` | The 9 dashboard SQL queries. |
| `orchestration/runner.py` | One table, one attempt: the extract→stage→load→reconcile→advance ordering. |
| `orchestration/batch_runner.py` | Many tables: dependency ordering, run-level state, concurrency. |
| `orchestration/bundle.py` | RunSpecs → Databricks job/task JSON (well, YAML). |
| `orchestration/factory.py` | The only place a live `SparkSession` / `dbutils` is touched. |
| `cli.py` | argparse wiring. Config-only commands never import Spark. |

---

## 3. How a table config becomes a running job — the call chain

```
config/tables/finance/gl_transactions.yaml
        |
        v
config.resolver.ConfigResolver.resolve()      merges defaults+table+env layers
        |
        v
config.validator.validate()                    schema + semantic checks
        |
        v
engine.run_spec.RunSpec.from_config()           dict -> typed, frozen spec
        |
        v
orchestration.runner.TableRunner.run()          the per-table lifecycle:
        |
        +- control.watermark.WatermarkStore.window()      where to start reading
        +- engine.extractor.OracleExtractor.extract()      -> engine.sql_builder (SELECT ...)
        +- engine.transformer.Transformer.stage()          -> dedupe + audit columns
        +- engine.loader.DeltaLoader.write()                -> engine.loader (MERGE ...)
        +- engine.reconciler.Reconciler.run()               -> counts/expectations
        +- control.watermark.WatermarkStore.advance()       ONLY after a successful load
        |
        v
control.control_store.ControlStore                          records every step
control.audit.AuditLog                                       immutable event trail
```

`orchestration.batch_runner.BatchRunner` wraps `TableRunner` to run many tables under
one `run_id`, honouring `table.depends_on`.

`orchestration.factory.build_engine()` is the only function that turns this into
something that touches a real cluster — everything above it is testable with fakes,
which is why 591 of the 606 tests need no Spark session at all.

---

## 4. "Where do I find..." quick index

| I need to... | Look at |
|---|---|
| Change how a `filter` predicate is validated | `config/validator.py` -> `_validate_semantics` |
| Add a new extraction mode | `engine/sql_builder.py` (`build_source_query`) + `engine/run_spec.py` (`ExtractionSpec`) + schema + validator |
| Change the MERGE statement's shape | `engine/loader.py` -> `build_merge_sql` |
| Change what counts as a duplicate before merge | `engine/transformer.py` -> `dedupe_order_expression` |
| Add a new control table or column | `control/schema.py`, then `tools/emit_control_ddl.py` to regenerate `sql/control/` |
| Add a new audit event type | `control/audit.py` -> `EventType` |
| Change the watermark comparison logic | `control/watermark.py` -> `advance_guard_sql` |
| Add a new quality/expectation rule | `engine/reconciler.py` -> `build_expectation_query` + schema `rule` enum |
| Add a dashboard query | `observability/monitoring.py`, then `tools/generate_bundle.py --env <env>` to regenerate |
| Change how jobs are generated | `orchestration/bundle.py` |
| Add a CLI command | `cli.py` -> `build_parser()` + `COMMANDS` dict |
| See exactly what SQL a table will run, without a cluster | `ingest show-sql --table <fqn> --env <env>` |

---

## 5. Generated vs hand-maintained — do not confuse these

| Path | Status | Regenerate with |
|---|---|---|
| `sql/control/*.sql` | GENERATED from `control/schema.py` | `python tools/emit_control_ddl.py --catalog <cat> --schema control` |
| `sql/monitoring/*.sql` | GENERATED from `observability/monitoring.py` | `python tools/generate_bundle.py --env <env>` |
| `resources/jobs_<env>.yml` | GENERATED from `config/tables/**` | `python tools/generate_bundle.py --env <env>` |
| `config/**` | HAND-MAINTAINED — this is the actual source of truth | — |
| `databricks.yml` | HAND-MAINTAINED — deployment settings only | — |
| everything under `src/` | HAND-MAINTAINED | — |

Every generated file carries a `GENERATED FILE -- do not edit` header naming its
source. If you find yourself editing one, stop and edit the source instead.

---

## 6. Test suite map

| File | Covers |
|---|---|
| `test_merger.py`, `test_loader.py` *(config)*, `test_resolver.py`, `test_validator.py` | the config hierarchy end to end |
| `test_run_spec.py` | RunSpec construction, ISO-8601 duration parsing |
| `test_sql_builder.py` | every extraction mode's generated SQL, by string assertion |
| `test_extractor.py` | JDBC option assembly, bound probing, credential lookup (fakes) |
| `test_transformer.py` | audit columns, dedupe-before-merge SQL |
| `test_loader.py` *(engine)* | MERGE / CREATE TABLE SQL shape, metric parsing |
| `test_reconciler.py` | row-count arithmetic, expectation SQL, rule evaluation |
| `test_watermark.py` | canonicalization, window arithmetic, the guarded advance |
| `test_control_store.py`, `test_audit.py`, `test_control_schema.py` | control-plane state machine, DDL, audit buffering |
| `test_runner.py` | the single-table lifecycle and ordering guarantee (fakes) |
| `test_batch_runner.py` | dependency ordering, concurrency, run-level status rollup |
| `test_bundle.py` | Databricks job generation |
| `test_monitoring.py` | dashboard SQL shape |
| `test_cli.py` | every CLI command, exit codes |
| `test_example.py` | the shipped walkthrough stays runnable |
| `test_delta_semantics.py` | **needs real Spark/Delta** (`pytest -m spark`) — the properties SQL text can't prove: dedupe prevents Delta's multi-match error, re-runs converge, the guard rejects an older replay |

Run everything text-provable, anywhere: `pytest -q`

Run the Delta-behaviour tests: `pytest -q -m spark` (see [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for environment notes)
