# Open Items — decisions and actions required from developers/stakeholders

This is the single explicit list of everything in this repository that is a
placeholder, an assumption, or a deferred decision. Nothing here blocks
development or testing — the full test suite (606 tests) passes today with
none of these resolved. **Everything here blocks a first production
deployment.**

Each item states: what it is, where it lives, why it matters, and who should
resolve it. Items are grouped by urgency, not by topic.

---

## A. Must resolve before first deployment (blocking)

### A1. Databricks workspace URLs are placeholders

**Where:** [`databricks.yml`](../databricks.yml), all three targets
(`dev`, `test`, `prod`) currently point at
`https://adb-000000000000000.0.azuredatabricks.net`.

**Action required:** Replace with the real workspace URL for each
environment.

**Owner:** Platform/Databricks admin.

---

### A2. Production service principal is unset

**Where:** [`databricks.yml`](../databricks.yml) line ~75 —
`service_principal: REPLACE_WITH_SERVICE_PRINCIPAL_APPLICATION_ID`. Prod jobs
are configured to `run_as` this identity rather than whoever deploys the
bundle.

**Action required:** Create (or identify) a service principal for production
ingestion, grant it Unity Catalog permissions on the target catalog/schemas
and the control schema, and put its application ID here.

**Owner:** Platform admin + framework owner.

---

### A3. Oracle secret scopes referenced but not confirmed to exist

**Where:** `config/environments/{dev,test,prod}.yaml` → `source.secret_scope`
= `oracle-dev`, `oracle-test`, `oracle-prod` respectively.

**Action required:** Confirm these Databricks secret scopes exist in each
workspace and contain `username`/`password` keys (or update
`source.secret_keys` per table if the org's naming differs) with valid
Oracle credentials for the ingestion service account.

**Owner:** Platform admin, working with Oracle DBAs for the credential
itself.

---

### A4. Control-plane catalogs/schemas must actually be created

**Where:** `config/environments/*.yaml` → `control.catalog` /
`control.schema` currently point at `dev_lakehouse.control`,
`test_lakehouse.control`, `prod_lakehouse.control`. `target.catalog` /
`target.schema` similarly point at `<env>_lakehouse.bronze`.

**Action required:**
1. Confirm these catalog names match your org's actual Unity Catalog naming
   convention (they are illustrative, not fetched from anywhere real).
2. Create the catalogs/schemas, or update the config to point at existing
   ones.
3. Run `ingest init-control --env <env>` once per environment to create the
   six control tables (dry-run first: `ingest init-control --env <env>
   --dry-run` to review the DDL, or inspect the already-generated
   `sql/control/*.sql`).

**Owner:** Data platform team, in coordination with whoever owns Unity
Catalog governance.

---

### A5. `EXECUTE` on `DBMS_FLASHBACK` for the SCN extraction table

**Where:** `config/tables/sales/order_events.yaml` uses
`extraction.incremental.strategy: scn`, which by default calls
`DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` to pin each batch's upper bound
(`extraction.incremental.use_upper_bound: true`, the default).

**Action required:** Confirm with Oracle DBAs whether the ingestion service
account has (or can be granted) `EXECUTE` on `DBMS_FLASHBACK`. If not,
set `use_upper_bound: false` on that table — the framework falls back to
`MAX(ORA_ROWSCN)` from the extracted batch, which is safe but slightly less
precise under heavy concurrent write load on the source.

**Owner:** Oracle DBA team + framework owner.

---

### A6. Alert email addresses are placeholder `@example.com` addresses

**Where:**
- `config/environments/prod.yaml` → `alerting.on_failure` /
  `on_reconciliation_mismatch` = `data-eng-oncall@example.com`
- `config/environments/test.yaml` → `data-eng-test@example.com`
- `config/tables/**/*.yaml` → `table.owner` = `finance-data@example.com`,
  `sales-data@example.com`

**Action required:** Replace with real distribution lists / on-call channel
addresses before deploying to any environment where a failure should
actually page someone. These flow directly into Databricks job
`email_notifications` (see `orchestration/bundle.py::email_targets`) as well
as the framework's own alert dispatch.

**Owner:** Whoever owns each table (`table.owner`) for the per-table
addresses; data engineering lead for the environment-wide on-call address.

---

### A7. No named framework owner or on-call rotation exists anywhere in this repo

**Where:** Referenced from [`RUNBOOK.md` §5 Escalation](RUNBOOK.md#5-escalation)
but not defined anywhere.

**Action required:** Decide who is paged for framework-level failures
(control-plane state-machine errors, MERGE semantics regressions) as
distinct from table-owner-level data issues, and record it — either in this
repo (a `CODEOWNERS` file or a section in the runbook) or in whatever
on-call tool the org uses, with a pointer back here.

**Owner:** Data engineering leadership.

---

### A8. Cloud provider / node types are Azure-flavored by default

**Where:** [`databricks.yml`](../databricks.yml) `variables.node_type`
defaults to `Standard_DS3_v2`/`Standard_DS4_v2`/`Standard_DS5_v2` (Azure VM
SKUs), and workspace URLs use the `azuredatabricks.net` domain.

**Action required:** If deploying on AWS or GCP, replace node types with the
equivalent instance families (e.g. AWS `i3.xlarge`-class, GCP
`n2-highmem`-class) and update the workspace URL pattern in A1.

**Owner:** Platform admin.

---

### A9. `spark_version` (Databricks Runtime) is an assumption, not a confirmed target

**Where:** [`databricks.yml`](../databricks.yml) `variables.spark_version`
defaults to `15.4.x-scala2.12`. [`DESIGN.md` §10](../DESIGN.md#10-confirmed-decisions)
originally assumed "DBR 14.3 LTS+" for liquid clustering and current Delta
APIs; the shipped default is newer than that floor.

**Action required:** Confirm the DBR version the org standardizes on and set
it here. Anything ≥ 14.3 LTS should work (liquid clustering + the Delta APIs
the loader uses), but this hasn't been validated against every intermediate
version.

**Owner:** Platform admin.

---

### A10. The `python -m build --wheel` packaging step is unverified in a real CI/CD pipeline

**Where:** [`databricks.yml`](../databricks.yml) `artifacts.ingestion_framework`
declares a wheel build; [`pyproject.toml`](../pyproject.toml) has the
build-system block. No `.github/workflows/`, Azure Pipelines, or other CI
config exists in this repository.

**Action required:** Decide and set up the actual build/deploy pipeline:
- Where does `databricks bundle deploy` run from — a CI runner or a
  developer's machine?
- Is there a gate that runs `pytest -q` (and ideally `pytest -q -m spark` on
  a Linux runner — see A11) before deploy?
- Does `tools/generate_bundle.py` run as part of CI, or is its output
  committed manually and CI just validates it's not stale?

**Owner:** Framework owner + platform/DevOps.

---

### A11. The 15 Delta-semantics tests need a CI decision

**Where:** `tests/test_delta_semantics.py`, marked `pytest.mark.spark`.

**Status:** ✅ Verified passing (15/15) via the Windows shim in
`tools/localspark/` during development — see that folder's README for what
the shim does and does not change. This is real Delta MERGE behavior being
exercised, not a placeholder result.

**Action required:** Decide how these run in CI going forward. They need a
JDK + `delta-spark`'s jars (network access on first run to populate the Ivy
cache, or a pre-warmed cache) and take ~7–8 minutes due to JVM/Spark
startup. Recommended: a dedicated CI stage on a Linux runner (no shim
needed there — `delta-spark` resolves cleanly), run on every PR that touches
`engine/loader.py`, `engine/transformer.py`, or `control/watermark.py` at
minimum, and ideally on every PR.

**Owner:** Framework owner + DevOps.

---

## B. Product/business decisions (deferred by design, not forgotten)

### B1. Deletes are not propagated from Oracle to Bronze

**Where:** [`DESIGN.md` §3.8](../DESIGN.md#38-delete-policy--deletes-are-not-propagated),
[`README.md` § Known limitations](../README.md#known-limitations).

**Status:** Explicitly deferred pending input from Oracle DBAs on which
tables actually experience physical deletes. This was a deliberate build
decision, not an oversight — see the design doc for the full reasoning and
the two available upgrade paths (full-refresh reconcile, or CDC).

**Action required:** Get the DBA answer. If any table in scope has physical
deletes that matter to consumers, decide between:
- an `extraction.filter` on a soft-delete/status column (works today, no
  framework change),
- `mode: full` + `write_mode: overwrite` for small tables (works today),
- building the `WHEN NOT MATCHED BY SOURCE` reconcile path (a real
  engineering task — the merge SQL builder is structured to accept this as
  an additive change, not a rewrite).

**Owner:** Framework owner, blocked on Oracle DBA input.

---

### B2. Reconciliation failure currently always fails the task (no per-table warn option)

**Where:** `orchestration/runner.py` — a failed reconciliation check
(`row_count`, `null_key`) always raises `ReconciliationFailure` and blocks
the watermark. `quality.expectations` rules *do* support `action: warn` per
rule, but the row-count and null-key checks do not.

**Action required:** Decide whether any table needs the row-count/null-key
checks to be soft (warn-only). If yes, this needs a small config + code
change (a `quality.row_count_reconciliation_action: fail|warn` knob,
mirroring how `expectations[].action` already works).

**Owner:** Framework owner, on request from a table owner.

---

### B3. Cross-schedule-group dependencies cannot become Workflow task edges

**Where:** `orchestration/bundle.py::cross_group_dependency_report`. Today,
none of the three shipped tables have this problem — all `depends_on`
targets are within the same `schedule.group`. But nothing prevents a future
table config from creating one, and when it happens, `tools/generate_bundle.py`
only prints a warning; it does not fail the build.

**Action required:** No action needed today. When a future table's
`depends_on` crosses groups, either move both tables into the same group, or
decide whether the generator should hard-fail instead of warn (currently a
judgment call left to whoever adds the table).

**Owner:** Whoever adds the next table with a cross-group dependency;
framework owner if the warn-vs-fail policy should change.

---

### B4. `ingest backfill` doesn't specifically warn about `append`-target duplication

**Where:** `cli.py::cmd_backfill`. It refuses to run without `--yes`, and
refuses outright for tables with no watermark at all — but the specific risk
called out in [`README.md`](../README.md#running) ("on an append target this
duplicates history") is only documentation, not an extra runtime check.

**Action required:** Decide whether this needs a hard extra confirmation
step for `write_mode: append` tables specifically (a one-line addition to
`cmd_backfill`), or whether the existing `--yes` + documentation is
sufficient given how few append tables exist today.

**Owner:** Framework owner.

---

### B5. Freshness SLA is only set on one of the three shipped tables

**Where:** `config/tables/finance/gl_transactions.yaml` (via
`config/environments/prod.yaml` override) has `freshness_sla_hours: 4`.
`finance.gl_accounts` and `sales.order_events` have none — the freshness
dashboard shows them with a `NULL` breach flag rather than hiding them
(see `observability/monitoring.py::freshness`), which is correct behavior,
but it means no one is alerted if those two go stale.

**Action required:** Confirm with each table's owner whether a freshness SLA
should be set, and add `alerting.freshness_sla_hours` to the relevant
table/environment config.

**Owner:** Table owners.

---

### B6. PII / column masking is a documented future hook, not implemented

**Where:** [`DESIGN.md` §9](../DESIGN.md#9-security--secrets) — "PII columns
can be tagged in config for masking/tokenization in the transformer (future
hook)." No such tagging mechanism, masking logic, or config field exists
yet.

**Action required:** If any in-scope table carries PII that needs masking or
tokenization before landing in Bronze, this needs actual design + build work
— it is currently not just unconfigured, it is unbuilt.

**Owner:** Framework owner + data governance/compliance stakeholder.

---

### B7. Oracle connection security (TLS/wallet) is not addressed in config

**Where:** `config/environments/*.yaml` → `source.jdbc.url` is a plain
`jdbc:oracle:thin:@//host:port/service` connection string. Nothing in the
schema or the extractor currently models an Oracle wallet, TCPS/SSL, or a
network path requirement (VPN/VPC peering) between Databricks and Oracle.

**Action required:** Confirm with security/network teams what's required to
reach the Oracle hosts from the Databricks workspace, and whether the
connection needs to be encrypted in transit beyond whatever the network path
already provides. If a wallet or TCPS is required, `source.jdbc.options` can
carry the extra JDBC properties, but this hasn't been exercised or tested.

**Owner:** Security/network team + framework owner.

---

## C. Recommended, not blocking

### C1. Stuck-run cleanup has no CLI command

**Where:** [`RUNBOOK.md` §2.8](RUNBOOK.md#28-a-run-has-been-stuck-in-running-for-hours)
documents a manual `UPDATE` statement as the current procedure for closing a
run row orphaned by a lost driver/cluster.

**Suggestion:** A small `ingest close-stuck-runs --env <env> --older-than
<hours>` command would remove the need for anyone to hand-write SQL against
the control plane during an incident. Not blocking — the manual procedure
works and is documented.

**Owner:** Framework owner, low priority.

---

### C2. Largest-table-size and Oracle-version assumptions haven't been confirmed

**Where:** [`DESIGN.md` §10](../DESIGN.md#10-confirmed-decisions), "Still
open" table — largest table ≤ ~500M rows / ≤~50M daily delta (drives the
default `num_partitions`), and Oracle 19c+ (drives `ORA_ROWSCN` and
`FETCH FIRST` syntax assumptions).

**Suggestion:** Confirm both against the real source system. Neither blocks
deployment — they're one-line config changes (`source.read.num_partitions`
per table) if wrong, and the SQL builder's `FETCH FIRST`/`ORA_ROWSCN` usage
is standard from Oracle 12c onward, so this is very likely fine — but worth
a five-minute confirmation with the DBA team rather than leaving as an
assumption.

**Owner:** Framework owner + Oracle DBA team.

---

### C3. Databricks SQL dashboard has not been wired up from the generated queries

**Where:** `sql/monitoring/*.sql` are generated and ready; no actual
Databricks SQL Dashboard object references them yet.

**Suggestion:** Create the dashboard once per environment (or once, pointed
at a catalog-agnostic view) using these nine queries — see
[`RUNBOOK.md` §3](RUNBOOK.md#3-dashboard-quick-reference) for what each one
answers. This is manual UI work that can't be generated from config.

**Owner:** Whoever sets up environment monitoring, one-time task per
environment.

---

### C4. The Unity Catalog `permissions` block in `databricks.yml` is minimal

**Where:** [`databricks.yml`](../databricks.yml) prod target grants only
`CAN_VIEW` to a `data-engineering` group. No other role/permission grants
are declared (e.g. who can trigger a manual run, who can edit the job).

**Suggestion:** Review against your org's actual RBAC policy for production
data jobs before first deploy — the current block is a minimal placeholder,
not a considered policy.

**Owner:** Platform admin.

---

## D. Assumptions already made — sanity-check, don't necessarily change

These were deliberate calls made during the build, each documented at the
point of decision. Listed here so they're visible in one place; no action
needed unless you disagree with the call.

| Assumption | Where decided | Why it's probably fine |
|---|---|---|
| Unity Catalog, not `hive_metastore` | [DESIGN.md §10](../DESIGN.md#10-confirmed-decisions) #1 | Confirmed decision, not an assumption — you chose this explicitly |
| Watermark-only CDC, no GoldenGate/LogMiner | [DESIGN.md §10](../DESIGN.md#10-confirmed-decisions) #2 | Confirmed decision |
| Bronze = 1:1 merge mirror, not append-only history | [DESIGN.md §10](../DESIGN.md#10-confirmed-decisions) #4 | Confirmed decision, revised once already during design — see the doc's changelog note |
| Databricks Workflows only, no Airflow | [DESIGN.md §10](../DESIGN.md#10-confirmed-decisions) #5 | Confirmed decision |
| Job-level retries default to 0 | `config/defaults.yaml` → `schedule.job_retries` | Deliberate — the framework's own retries already cover transient errors; job retries would blur the audit trail. Raise per-table only for infra-loss scenarios (see README) |
| Merge guard defaults to inclusive (`>=`) comparison, tie goes to the new value | `engine/loader.py::build_merge_sql` | Means a re-run refreshes audit columns even when the data didn't change — intentional, not a bug |
| Column case is preserved (not lower-cased) by default | `config/defaults.yaml` → `target.column_case` | Chosen so Bronze is a literal 1:1 mirror of Oracle's casing |

---

## How to use this document

- When an item is resolved, move it out of this file into a short changelog
  note (or just delete it) rather than leaving it marked done in place —
  this file should only ever list what's *still* open.
- If you find something during deployment that isn't listed here, add it —
  this list was compiled by reviewing the whole build, but it is not
  guaranteed exhaustive.
