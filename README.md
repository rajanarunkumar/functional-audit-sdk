# functional-audit-sdk

Transformation contract and evidence framework for Spark / Databricks. Produces one deterministic,
regulator-ready record per processing stage that ties a business requirement to the code that
implemented it, the plan that executed, the inputs it read, the rows it wrote, and the evidence
that declared intent matched observed execution. Domain-agnostic: the SDK never sees business columns.

Targets Databricks Runtime 19 / Spark 4.2; runs on 18.x and 17.3 LTS with feature detection.

## Install

```
pip install functional-audit-sdk            # or add to requirements.txt / bundle environments
```

Cluster policy / pipeline config (startup properties):

```
spark.functional_audit.enabled          true
spark.functional_audit.catalog          platform_gov
spark.functional_audit.schema           functional_audit
spark.functional_audit.telemetry.tier   1
spark.functional_audit.enforce          warn        # strict for regulatory scope
spark.functional_audit.run_id_column    __run_id
```

## Author a stage

```python
import functional_audit as fa

@fa.stage("rwa_retail.ead")
def build_ead(inputs, reporting_period="2027-03-31"):
    exp = inputs["cap_silver.retail.exposures"]     # pinned by the SDK
    ccf = inputs["cap_ref.capital.ccf_offbalance"]
    return exp.join(ccf, "facility_type", "left").withColumn("ead", ...)

build_ead(run=fa.runtime.stage.RunOptions(reporting_period="2027-03-31"))
```

The contract lives in `functional_audit/contracts/` next to the code (see `examples/contracts/rwa_retail.yaml`
for a bundle that covers a whole calculation).

## CLI

| command | where it runs | what it does |
|---|---|---|
| `fa validate` | CI, every PR | gate: contracts present and valid, every stage has a contract, no `__` misuse, no drift without a version bump |
| `fa lock` | after a reviewed version bump | writes `contracts.lock` used for drift detection |
| `fa init` | CD, platform principal | creates/upgrades schema, tables, `explain()` TVF, ledger view; precondition checks |
| `fa publish` | CD on merge to main | publishes contracts to `functional_audit.contracts` with the git SHA |
| `fa sync-views` | CD or on demand | creates explain views for governed tables |
| `fa reconcile` | daily job, one per metastore | ingests UC lineage + query history, promotes to FINAL |
| `fa attest` | period close | records APPROVED / WAIVED / REVOKED with segregation of duties |

`fa init --render-to ./sql` writes the SQL for review without executing.

## Query the evidence

```sql
SELECT * FROM platform_gov.functional_audit_views.cap_gold__retail__rwa
WHERE exposure_id = 'RM-10023';            -- your WHERE; the SDK contributes the audit columns

SELECT * FROM platform_gov.functional_audit.explain('<run_id>');
```

## Tests

`pytest` runs the SDK suite. The package also ships a pytest plugin: `fa_contracts`, `fa_validate`,
`fa_spark`, `fa_local_stage` fixtures and a `ConformanceTests` mixin that auto-parametrizes over
every contract in the repo.

## Layout

```
functional_audit/
  contracts/      model (Pydantic), loader (YAML, bundles)
  validate.py     CI gate (V001–V008)
  init/           versioned SQL migrations + runner
  runtime/        context capture, plan hashing, telemetry, controls, stage, store, reconcile, ingest, explain
  autoload/       .pth session hook (read-only)
  testing/        pytest plugin
  cli.py          fa
examples/         rwa_retail bundle + stages
```

# functional_audit — FAQ

## Purpose

**What problem does functional_audit solve?**
In Spark-based data platforms, the logic that ran, the data it read, the controls that were applied, and the evidence that it all happened are spread across code, job logs, Delta history, and Unity Catalog lineage — at inconsistent levels of detail. functional_audit produces one deterministic record per processing stage that ties a business requirement to the code that implemented it, the plan that executed, the inputs it read, the rows it wrote, and the evidence that declared intent matched observed execution.

**Who is it for?**
Any team writing governed Spark/Databricks pipelines — capital, liquidity, finance, risk, fraud, marketing. The SDK knows nothing about your domain. The primary driver is regulatory audit (BCBS), but the same evidence serves change management, impact analysis, and incident review.

**What does a consumer actually have to do?**
Three things: install the `functional-audit-sdk` wheel, set the `spark.functional_audit.*` startup properties (usually via cluster policy), and decorate each governed stage with `@fa.stage("<contract_id>")` alongside a contract YAML in the repo. Everything else is automatic.

**Why is AI-generated code a specific concern?**
Two code-generation runs can produce different but equally valid implementations of the same requirement. If governance is anchored on source code, every regeneration looks like a logic change. functional_audit anchors governance on the canonical execution plan instead: same plan, same logic, regardless of how the code was written.

## Contracts

**What is a contract?**
A versioned, machine-readable statement of intent for one stage: the requirement it implements, the rule citation, the business logic in plain language, declared inputs and outputs, controls, telemetry tier, and pinned execution semantics (ANSI mode, timezone, decimal scale).

**Where do contracts live?**
In the same repository as the stage code, under `functional_audit/contracts/`. A change to logic and a change to its contract are reviewed in the same pull request. CODEOWNERS on that directory requires a governance reviewer.

**When is a contract registered?**
On merge to main. `fa publish` in the CD pipeline writes the contract to `functional_audit.contracts` with the git commit, the deploying principal, and an effective date. Nothing is registered from a laptop. At runtime, a stage resolves its contract from the table, never from the filesystem.

**Can one YAML describe a whole calculation with many stages?**
Yes. A bundle declares a `calculation_id` and a list of stages. `fa publish` flattens it into one contract row per stage sharing the `calculation_id` and `bundle_version`. Each stage keeps its own logic hash, so a bundle change shows exactly which stages moved. Cross-stage dependencies are validated at publish.

**What happens if I change logic without bumping the contract version?**
`fa validate` fails the build. Logic drift without a declared change is the single most important thing the CI gate catches.

## How it works

**What is the logic hash?**
A hash of the canonicalized Spark Connect plan of the DataFrame written by the stage — after alias stripping and predicate ordering. Two implementations that resolve to the same plan share a hash. A hardcoded constant replacing a reference-table join produces a different one.

**What is `__run_id`?**
The only column functional_audit adds to a business table. It links every row to `functional_audit.runs`, and from there to the contract, inputs, evidence, lineage, and reconciliation. The column name is configurable (`spark.functional_audit.run_id_column`), and the `__` prefix is reserved for system columns: they are excluded from hashing, drift checks, and controls, and user code cannot write to them inside a stage.

**Does every table need `__run_id`?**
No. Only tables declared as contract outputs. Bronze, scratch, and intermediate DataFrames are never stamped. Governed tables that cannot be widened fall back to Delta row tracking and the commit version recorded in `run_outputs`.

**What happens inside `@fa.stage`, in order?**
1. Generate a `run_id` and resolve the effective contract.
2. Capture execution context (job, pipeline, task, compute, principal, runtime, confs) and write the `runs` row — before any data is read.
3. Set Spark query tags for the duration of the stage.
4. Resolve declared inputs to pinned Delta versions and record them.
5. Build the DataFrame, canonicalize and hash the plan, attach in-plan observation metrics.
6. Write the output with `__run_id` and commit metadata; record the commit version.
7. Write evidence and control results.
8. Run in-process reconciliation; mark the attestation stage `PROVISIONAL`.

**How are inputs tracked?**
Declared inputs are loaded with `versionAsOf` by the SDK's input resolver, and the version is recorded in `run_inputs`. At commit time the SDK also walks the executed plan and lists every relation actually read; any read that was not declared becomes an `UNDECLARED_INPUT` deviation immediately.

**How are outputs tracked?**
The write hook reads the commit version from the target's Delta log after the write and stores it, with the commit metrics, in `run_outputs`. The Delta commit's `userMetadata` carries the run and contract identifiers, so the table's own history is also evidence.

**Does telemetry slow my job down?**
It is designed not to. Tier 0 (always on) uses Delta commit metrics and snapshot statistics — no scan. Tier 1 (default) attaches `observe()` metrics to the final DataFrame only, computed in the same pass as the write. Tier 2 (hash totals) is opt-in per contract. The SDK measures its own overhead per run, records it, and downgrades the tier automatically if a budget is exceeded — while flagging that it did so.

## Technical audit and lineage

**Are you replicating Databricks lineage?**
No. Unity Catalog lineage and query history remain the source. functional_audit stores a projection of them — only rows that match a run — for three reasons UC does not cover: attaching `run_id`, retaining evidence beyond the system-table retention window, and reconciling observed lineage against declared intent. Each stored row records how it was matched (`ENTITY`, `TAG`, or `WINDOW`).

**How does a run get linked to system tables?**
The `runs` row captures every identifier the system tables use: job and task run ids, pipeline update ids, compute id, principal, and a query tag containing the run id. Lineage is joined on entity ids; query history is joined on the query tag; the time window is used only as a graded fallback.

**When does that linking happen?**
A single platform-owned Lakeflow job per metastore, scheduled daily (system tables lag by a few hours) and available on demand via `fa reconcile`. It reads only new partitions past a watermark with a short lookback for late arrivals, and joins the day's runs against them. At bank scale it runs in minutes on a small cluster; cost does not grow with the number of consuming teams.

**What does `explain_table` give me?**
A generated view, `functional_audit.views.<catalog>__<schema>__<table>`, that joins every business column to the audit context for that row: requirement, rule citation, contract version, business logic text, logic hash, pinned inputs, controls and results, the job and principal that produced it, the statements executed, the tables and columns actually read, and reconciliation status. You add the `WHERE`; the SDK never sees your domain columns.

**Why is `explain()` a SQL function and not a Python UDF?**
A SQL table function is inlined into the query plan: the optimizer sees its joins, pushes predicates through it, and Photon executes it natively. A Python UDF — even Arrow-optimized in Spark 4.2 — is opaque to the optimizer and cannot read catalog tables from inside a worker. Python UDTFs are reserved for procedural work such as plan diffing.

## Reconciliation and attestation

**What is reconciliation?**
The comparison of declared versus observed: executed plan hash against declared, pinned inputs against lineage, controls declared against controls evaluated, semantics pinned against runtime confs, and outputs declared against outputs written.

**When does it run?**
Three times. In-process at the end of every stage, producing `PROVISIONAL`. Scheduled the next day once lineage and query history have landed, promoting to `FINAL` when every declared output has an exact match. On demand via `fa reconcile --run-id` for re-grading.

**What does `strict` mode do?**
Refuses to run a stage without an effective contract, and blocks downstream promotion on any `DEVIATION`. `warn` mode records the same findings without blocking, for first-wave adoption.

**How does attestation work?**
`fa attest` records `APPROVED`, or `WAIVED` with a mandatory reason when reconciliation shows a deviation. Decisions are superseded by new rows, never edited; `REVOKED` closes one out. Period-level attestation for a `(contract_id, period)` is accepted only when every run in scope is `FINAL` and individually approved or waived. The attester must not be the principal that ran the job. A Databricks App can front the same function if a UI is wanted; there is no separate service.

**How do I explain why a number changed between periods?**
`fa.diff_runs(run_a, run_b)` decomposes the difference into four dimensions: contract change, logic change, data change (input versions), and semantics change (confs). For restatements, the row-level delta is computed with Spark 4.2's `CHANGES` clause.

## Run types and scenarios

**What are `run_kind` and `run_purpose`?**
`run_kind` describes execution semantics: `incremental`, `backfill`, `replay` (contract as of the original period), `restatement` (current contract, superseding a prior run). `run_purpose` describes why it ran: `PRODUCTION`, `SHADOW`, `WHAT_IF`, `ESTIMATE`, `TEST`. Only `PRODUCTION` may write to the declared output; other purposes are routed to a suffixed table and tracked the same way.

**Can I track a what-if scenario or a QIS estimate?**
Yes. A what-if run carries a `scenario_id` and `scenario_params`. The contract declares which parameters may be overridden (a reference-table version, a rate, a threshold); overriding anything else fails validation. The result has full lineage and can be diffed against production.

**How is a backfill handled?**
As a parent `backfill_id` with one child run per chunk, each with its own pinned inputs and evidence. Outputs use `INSERT ... REPLACE ON` period keys so retries are idempotent.

## Operations

**What does `fa init` do, and who runs it?**
Creates the `functional_audit` schema, tables, the `explain()` function, the ledger metric view, grants, tags, the daily reconciliation job, and runs precondition checks (system tables enabled, query tags propagating). It runs from CI/CD under the platform principal with versioned, idempotent migrations. Consumers never run it.

**What runs at cluster startup?**
A `.pth` autoload that reads the `spark.functional_audit.*` properties, checks the schema version, and installs the stage hooks. It is read-only — no DDL at session start.

**Which tables are audit-tracked for human changes?**
`contracts` and `attestations` carry `created`, `created_by`, `last_altered`, `last_altered_by`, set by the SDK from the platform identity. All other tables are system-written and append-only with `created`. No table permits deletes; Delta history and change data feed provide a second, engine-level trail.

**What runtimes are supported?**
Built for Databricks Runtime 19 (Spark 4.2). Runs on 18.x and 17.3 LTS with feature detection: atomic multi-table commits, the `CHANGES` clause, and Arrow-default UDFs are used where available and degrade gracefully otherwise.

**What if a table already has a `__run_id` column?**
`fa validate` checks whether its values resolve to `functional_audit.runs`. If they do, the SDK adopts the column as-is. If they do not, validation fails with an explicit message rather than mixing two meanings.
