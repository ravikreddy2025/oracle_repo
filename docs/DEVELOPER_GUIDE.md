# Developer Guide

For engineers extending the framework itself — adding an extraction mode, a
control table, a dashboard query, a CLI command. If you only need to onboard
a new Oracle table, you don't need this file: see
[README.md § Onboarding a new table](../README.md#onboarding-a-new-table).

For where code lives, see [CODEMAP.md](CODEMAP.md). For why things are built
the way they are, see [DESIGN.md](../DESIGN.md).

---

## 1. Setup

```bash
git clone <repo>
cd oracle-ingestion-framework
pip install -e ".[dev]"
pytest -q
```

You should see `591 passed, 15 skipped` on a machine without a working local
Spark/Delta (Windows without the shim, or no JDK). See §5 for how to get the
remaining 15 running.

No Oracle connection, Databricks workspace, or credentials are needed for
development. Everything except the 15 Spark-marked tests runs against pure
Python and fakes.

---

## 2. Ground rules this codebase follows

These aren't arbitrary style points — each one was a deliberate response to a
bug the test suite caught during the build, or a design pressure worth
naming explicitly.

1. **SQL is built as pure functions of a `RunSpec`.** Never issue Oracle or
   Delta SQL any other way. This is what makes ~500 of the tests runnable
   without a cluster — every generated statement is provable by string
   assertion. If you find yourself writing an f-string SQL fragment outside
   `engine/sql_builder.py` or `engine/loader.py`, stop and put it there.

2. **Never concatenate a value into Oracle SQL.** Spark's JDBC source takes
   the query as an opaque string with no parameter binding, so
   `sql_builder.literal()` re-validates every bound value against its
   canonical form and *refuses* to render anything that isn't exactly that
   form. If you need a new kind of bound value, extend `literal()`'s type
   dispatch — do not add a new f-string path around it.

3. **Control-plane SQL always binds parameters.** `control/sql_client.py` is
   the only place that touches Spark SQL directly for the control plane, and
   every value goes through `:named` parameters via `spark.sql(text,
   args=...)`. Identifiers (catalog/schema/table names) can't be bound as
   parameters, so `control/schema.py::qualify()` validates them before they
   ever reach a string.

4. **A silent no-op is a validation error, not a warning.** If a config
   combination would cause part of a table's config to be silently ignored
   (e.g. `extraction.filter` set alongside `mode: query`, where the query
   file — not the filter — controls the predicate), `config/validator.py`
   rejects it. A developer who writes a filter expects it to filter.

5. **Two identifier dialects, two validators.** Oracle identifiers
   (`sql_builder.identifier()`) don't allow a leading underscore; Delta/Spark
   identifiers (`loader.delta_identifier()`) do — and the framework's own
   audit columns all start with `_`. Using the wrong one is a real bug that
   happened once during the build (every merge failed on `_ingested_at`).
   Source-side code uses the Oracle validator; target-side code uses the
   Delta one.

6. **The watermark advances only after a committed, successful write —
   enforced by ordering in code, not by convention.** See
   `orchestration/runner.py::TableRunner._execute` and don't reorder those
   steps without re-reading DESIGN.md §4.

7. **Retries append; they never reopen a terminal task.**
   `control/control_store.py::assert_transition` enforces this — a
   `SUCCEEDED`/`FAILED`/`SKIPPED` task cannot transition anywhere. A retry
   is `attempt = n + 1`, a brand new row.

8. **Every temp view name is scoped to `(table, run_id, attempt)`.**
   (`engine/transformer.py::view_names`.) This was a real bug found while
   building the batch runner: fixed view names meant two tables running
   concurrently on one Spark session would silently overwrite each other's
   staged batch. Never hardcode a temp view name.

9. **Config-only code paths must not import Spark.** `validate`, `show-sql`,
   `list-tables`, and `run --dry-run` all need to work on a laptop with no
   cluster. `orchestration/factory.py` is the only module allowed to import
   `pyspark`/`dbutils` at module scope; everything else takes a `spark`
   object as a constructor argument.

10. **Generated files carry a header naming their source and are never
    hand-edited.** `sql/control/`, `sql/monitoring/`, `resources/*.yml`. If a
    generated file needs to change, change what generates it
    (`control/schema.py`, `observability/monitoring.py`,
    `orchestration/bundle.py`) and re-run the corresponding `tools/` script.

---

## 3. Adding a new extraction mode

Extraction modes live end-to-end across four files. Walk them in this order:

1. **`config/schema/config.schema.json`** — add the new `extraction.mode`
   enum value (or new fields under `extraction.incremental`) to the JSON
   Schema.
2. **`config/validator.py`** — add semantic rules: what other fields does
   this mode require or forbid? What would be a silent no-op if combined
   wrong? (Ground rule #4.)
3. **`engine/run_spec.py`** — extend `ExtractionSpec`/`IncrementalSpec` with
   any new typed fields, and `RunSpec.from_config()` to populate them.
4. **`engine/sql_builder.py`** — extend `build_source_query()` (and
   `build_predicates()` if it changes the WHERE clause). Write the query as
   a pure function — no Spark, no Oracle, just a `RunSpec` in, a `SourceQuery`
   out.
5. **`engine/extractor.py`** — if the mode needs a new probe (like the
   upper-bound clock probe for incremental modes), add it here.

Then test in this order:
- `tests/test_sql_builder.py` — string-assert the generated SQL for every
  combination the mode supports (with/without a filter, with/without column
  selection, etc.). This is where most of the value is; these tests run
  instantly and need nothing but Python.
- `tests/test_validator.py` — the new semantic rules, both the happy path
  and the rejected combinations.
- `tests/test_extractor.py` — if you added a probe, test its query and its
  handling of an empty/None result with the `FakeReader` pattern already in
  that file.
- Add one example table to `config/tables/` if the new mode represents a
  genuinely new pattern (see the three that already exist — full load,
  incremental merge, append+SCN — as the template), and extend
  `tests/test_sql_builder.py::TestShippedTables` to cover it.

## 4. Adding a new write mode / changing MERGE behaviour

Write-path logic is in `engine/loader.py` (SQL construction, pure functions)
and `engine/transformer.py` (staging + dedupe). Same discipline as above:
change the pure SQL-building function first, prove it with a string-assertion
test in `tests/test_loader.py` or `tests/test_transformer.py`, and — because
this is the one area SQL text can't fully prove — add or extend a case in
`tests/test_delta_semantics.py` if the change affects MERGE semantics against
real Delta (multi-match handling, the watermark guard, idempotency).

## 5. Testing against real Spark/Delta (`pytest -m spark`)

591 of 606 tests need nothing but Python. The other 15
(`tests/test_delta_semantics.py`) assert properties that only a real engine
can prove: that the pre-merge dedupe actually prevents Delta's multi-match
error, that re-running a batch converges to the same state, that the
watermark guard rejects an out-of-order replay, that `_first_ingested_at`
survives an update.

**On Linux/WSL or a Databricks cluster:** these run with no special setup —
`delta.configure_spark_with_delta_pip()` handles jar resolution, and
`tests/conftest.py::spark` detects the platform and skips the Windows-only
shim entirely.

**On native Windows:** Hadoop's local filesystem code shells out to
`winutils.exe` / `hadoop.dll` for four operations (chmod, file-create-with-
mode, readlink, and a native `listStatus` permission check). There is no
official Apache Windows binary — the common workaround is downloading one
from an unofficial GitHub mirror, which this project deliberately avoids.

Instead, `tools/localspark/` contains three small Java classes that answer
those four operations with plain `java.io` calls (no-op chmod, `File.exists`/
`File.listFiles`, etc.) — see `tools/localspark/README.md` for exactly what
each override does and, importantly, **what it does not change**: Delta's
transaction log, commit protocol, and MERGE semantics are completely
untouched; only how the local filesystem answers permission questions is
different. `tests/conftest.py::spark` compiles this shim automatically with
`javac` (needs a JDK on `PATH`) and points `HADOOP_HOME`/
`SPARK_DIST_CLASSPATH` at it, entirely inside the pytest session — no
environment variables to set by hand, no global install.

**Requirements to get the 15 tests running on Windows:**
- A JDK with `javac` on `PATH` (any recent JDK; this was verified against
  OpenJDK 21).
- `delta-spark` installed (`pip install delta-spark`), which also caches the
  required jars in the local Ivy cache the first time it runs with network
  access.
- If either is missing, the fixture skips with a clear reason rather than
  failing — you'll see `SKIPPED` with a message naming what's absent.

Run just these: `pytest -q -m spark`
Run everything except these: `pytest -q -m "not spark"` (the default; `spark`
is excluded by nothing special — they're simply slow enough, and JVM-
dependent enough, that CI may want to run them as a separate stage. See
[OPEN_ITEMS.md](OPEN_ITEMS.md) for the CI wiring decision.)

## 6. Regenerating derived artifacts

After changing `control/schema.py`:
```bash
python tools/emit_control_ddl.py --catalog '${catalog}' --schema control
```

After changing `observability/monitoring.py`, `orchestration/bundle.py`, or
any table/environment config:
```bash
python tools/generate_bundle.py --env <env>
```

Commit the regenerated files alongside the source change in the same PR —
`sql/`, `resources/` should never be out of sync with what produced them.

## 7. Code review checklist (self-review before opening a PR)

- [ ] Every new SQL-generating code path is a pure function taking a
      `RunSpec` (or explicit args) and returning a string/dataclass — no
      Spark/Oracle calls mixed in.
- [ ] Every bound value that reaches Oracle SQL goes through
      `sql_builder.literal()`'s type-checked rendering, not an f-string.
- [ ] Every value written to a control-plane table is passed as a named
      parameter, never concatenated into the SQL text.
- [ ] If you touched the extraction/validation surface, `config.validator`
      rejects the combinations that would silently do nothing.
- [ ] New temp views (if any) are scoped via `transformer.view_names()`, not
      hardcoded.
- [ ] `pytest -q` passes with no new skips beyond the existing 15 spark
      tests.
- [ ] If the change affects MERGE semantics, `pytest -q -m spark` passes.
- [ ] If you changed `control/schema.py`, `observability/monitoring.py`, or
      `orchestration/bundle.py`, the corresponding `tools/` script was
      re-run and the generated files are included in the diff.
- [ ] If you changed the config schema, `README.md`'s config reference table
      is updated to match.

## 8. Where to raise a design question vs just build it

Small, local decisions (a new expectation rule, a new CLI flag) — use your
judgment and the ground rules above.

Anything that changes one of the five confirmed decisions in
[DESIGN.md §10](../DESIGN.md#10-confirmed-decisions) (catalog model, CDC
depth, delete propagation, historization model, orchestration platform) is a
product decision, not an implementation detail — raise it with whoever owns
the roadmap before building it. See [OPEN_ITEMS.md](OPEN_ITEMS.md).
