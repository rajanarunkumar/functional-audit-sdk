# functional-audit-sdk

Transformation contract and evidence framework for Spark / Databricks. Produces one deterministic,
regulator-ready record per processing stage that ties a business requirement to the code that
implemented it, the plan that executed, the inputs it read, the rows it wrote, and the evidence
that declared intent matched observed execution. Domain-agnostic: the SDK never sees business columns.

Targets Databricks Runtime 19 / Spark 4.2 (classic and Spark Connect). The system schema and the
runtime are portable Delta SQL: the test suite runs them on OSS Spark 4 + delta-spark.

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
def build_ead(inputs, reporting_period):             # contract parameters arrive by name
    exp = inputs["cap_silver.retail.exposures"]     # pinned by the SDK
    ccf = inputs["cap_ref.capital.ccf_offbalance"]
    return exp.join(ccf, "facility_type", "left").withColumn("ead", ...)

build_ead(run=fa.RunOptions(reporting_period="2027-03-31"))
```

The contract lives in `functional_audit/contracts/` next to the code (see `examples/contracts/rwa_retail.yaml`
for a bundle that covers a whole calculation).

## CLI

| command | where it runs | what it does |
|---|---|---|
| `fa validate` | CI, every PR | gate: contracts present and valid, every stage has a contract, no `__` misuse, no dependency cycles, no contract drift without a version bump |
| `fa lock` | after a reviewed version bump | writes `contracts.lock` used for drift detection |
| `fa init` | CD, platform principal | creates/upgrades schema, tables, `explain()` TVF, ledger view, grants; checksums every applied migration; precondition checks |
| `fa publish` | CD on merge to main | publishes contracts to `functional_audit.contracts` with the git SHA |
| `fa seal` | governance, after review | binds a contract version to a plan structure hash (normally sealed automatically by the first clean run) |
| `fa sync-views` | CD or on demand | creates explain views for governed tables |
| `fa reconcile` | daily job, one per metastore | ingests UC lineage + query history, promotes to FINAL, reaps abandoned runs, reports stale PROVISIONALs |
| `fa attest` | period close | records APPROVED / WAIVED / REVOKED with segregation of duties |

`fa init --render-to ./sql` writes the SQL for review without executing.
`fa init --grant-writer <sp> --grant-reader <group>` applies the least-privilege grants: only the writer principal
holds MODIFY on the audit schema; readers get SELECT on the schema, the views and `explain()`.

## Query the evidence

```sql
SELECT * FROM platform_gov.functional_audit_views.cap_gold__retail__rwa
WHERE exposure_id = 'RM-10023';            -- your WHERE; the SDK contributes the audit columns

SELECT * FROM platform_gov.functional_audit.explain('<run_id>');
```

## Tests

`pytest` runs the suite: pure units, and Spark tests on a local session (set `FA_TEST_CONNECT=1` to run the
same tests through an embedded Spark Connect server — the plan-identity tests pass identically on both).
The package also ships a pytest plugin: `fa_contracts`, `fa_validate`, `fa_spark`, `fa_local_stage` fixtures and
a `ConformanceTests` mixin that auto-parametrizes over every contract in the repo.

## Layout

```
functional_audit/
  contracts/      model (Pydantic), loader (YAML, bundles)
  validate.py     CI gate (V001–V008)
  init/           versioned, checksummed SQL migrations + runner + grants
  runtime/        context capture, plan identity, telemetry, controls, stage, store, reconcile, ingest, explain
  testing/        pytest plugin
  cli.py          fa
examples/         rwa_retail bundle + stages
```

# functional_audit — FAQ

## Purpose

**What problem does functional_audit solve?**
In Spark-based data platforms, the logic that ran, the data it read, the controls that were applied, and the
evidence that it all happened are spread across code, job logs, Delta history, and Unity Catalog lineage — at
inconsistent levels of detail. functional_audit produces one deterministic record per processing stage that ties a
business requirement to the code that implemented it, the plan that executed, the inputs it read, the rows it
wrote, and the evidence that declared intent matched observed execution.

**Who is it for?**
Any team writing governed Spark/Databricks pipelines — capital, liquidity, finance, risk, fraud, marketing. The SDK
knows nothing about your domain. The primary driver is regulatory audit (BCBS), but the same evidence serves change
management, impact analysis, and incident review.

**What does a consumer actually have to do?**
Three things: install the `functional-audit-sdk` wheel, set the `spark.functional_audit.*` startup properties
(usually via cluster policy), and decorate each governed stage with `@fa.stage("<contract_id>")` alongside a
contract YAML in the repo. Everything else is automatic.

**Why is AI-generated code a specific concern?**
Two code-generation runs can produce different but equally valid implementations of the same requirement. If
governance is anchored on source code, every regeneration looks like a logic change. functional_audit anchors
governance on the server-side optimized execution plan instead, normalized so that alias names, predicate and
join-key order, `withColumn` chains versus a single `select`, and SQL formatting do not change the hash. This
property is tested on real plans, on classic and Spark Connect sessions.

## Contracts

**What is a contract?**
A versioned, machine-readable statement of intent for one stage: the requirement it implements, the rule citation,
the business logic in plain language, declared inputs (with a pin mode) and outputs, controls, telemetry tier,
pinned execution semantics (ANSI mode, timezone, confs), and the parameters a run may bind. Every field is enforced
by the runtime or the CI gate.

**Where do contracts live?**
In the same repository as the stage code, under `functional_audit/contracts/`. A change to logic and a change to
its contract are reviewed in the same pull request. CODEOWNERS on that directory requires a governance reviewer.

**When is a contract registered?**
On merge to main. `fa publish` in the CD pipeline writes the contract to `functional_audit.contracts` with the git
commit, the deploying principal, and an effective date. Nothing is registered from a laptop. At runtime, a stage
resolves its contract from the table, never from the filesystem.

**Can one YAML describe a whole calculation with many stages?**
Yes. A bundle declares a `calculation_id` and a list of stages. `fa publish` flattens it into one contract row per
stage sharing the `calculation_id` and `bundle_version`. Each stage keeps its own plan hash, so a bundle change
shows exactly which stages moved. `fa validate` rejects dependency cycles between stages of a calculation.

**What happens if I change the contract without bumping its version?**
`fa validate` fails the build (V007, against `contracts.lock`).

**What happens if I change the logic without bumping the contract version?**
The first clean PRODUCTION run of a contract version seals its plan structure hash into
`contracts.logic_hash_declared` (or a reviewer seals it with `fa seal`). Every later run compares its executed plan
to the seal; a different plan is a `PLAN_MISMATCH` deviation — in `strict` mode the stage refuses to write. A
reviewed change bumps the version, which starts unsealed and seals again on its first clean run.

**What are contract parameters?**
Named values (`parameters:`) with defaults that the SDK passes to the stage function by name. A `WHAT_IF` /
`ESTIMATE` run overrides them through `RunOptions.scenario_params`; overriding a name the contract does not declare
fails the run. Parameters that become literals in the plan land in the binding hash, not the structure hash.

## How it works

**What is the logic hash?**
Two hashes per run. The *structure hash* (`runs.logic_hash_executed`) is the SHA-256 of the optimized logical plan
after normalization: expression ids and generated aliases stripped, time-travel options dropped, numeric and date
literals lifted out, commutative operands (`AND`, `OR`, `=`) sorted. The *binding hash* covers the lifted literals,
the pinned input versions, the reporting period and the bound parameters. Same structure, different period: the
structure hash is unchanged and the binding hash moves. A hardcoded constant replacing a reference-table join, or a
different table with the same schema, changes the structure hash. The canonical plan text is stored with the run.

**What is `__run_id`?**
The only column functional_audit adds to a business table. It links every row to `functional_audit.runs`, and from
there to the contract, inputs, evidence, lineage, and reconciliation. The column name is configurable
(`spark.functional_audit.run_id_column`), and the `__` prefix is reserved for system columns: they are excluded from
hashing and controls, and user code cannot write to them inside a stage.

**Does every table need `__run_id`?**
No. Only tables declared as contract outputs. Bronze, scratch, and intermediate DataFrames are never stamped.

**What happens inside `@fa.stage`, in order?**
1. Resolve the effective contract (by period, or `as_of_contract_version` for a replay) and bind parameters.
2. Capture execution context (job, pipeline, task, compute, principal, runtime, confs) and write the `runs` row —
   before any data is read.
3. Set Spark query tags for the duration of the stage.
4. Resolve declared inputs to pinned Delta versions (`latest`, `contract_effective`, or `explicit`) and check the
   state of the run that last produced each of them.
5. Build the DataFrame and identify its plan (structure hash, binding, relations read).
6. Reconcile what is already known: plan against the seal, reads against declared inputs, semantics against the
   contract, upstream state, evidence gaps. In `strict` mode a deviation here means the stage does not write.
7. Write the output with `__run_id` and commit metadata; record the commit version.
8. Evaluate controls on the rows just written and complete the reconciliation.
9. Quarantine: move this run's rows to `<output>__quarantine` when a `quarantine` control failed, or a `fail`
   control failed in `strict` mode.
10. Flush evidence (one statement per table), mark the run `PROVISIONAL`, and seal the contract if this was its
    first clean run.

**How are inputs tracked?**
Declared inputs are loaded with `versionAsOf` by the SDK's input resolver, and the version is recorded in
`run_inputs`. `pin: contract_effective` resolves the version as of the contract's `effective_from`. Any relation the
executed plan read that the contract does not declare — including a name resolved through `inputs[...]` that is not
in the contract — is an `UNDECLARED_INPUT` deviation before the write. A version that could not be resolved is an
`EVIDENCE_INCOMPLETE` deviation, never a silent unpinned read.

**How are outputs tracked?**
The write hook finds the Delta commit whose `userMetadata` carries the run id and stores its version and metrics in
`run_outputs`. The table's own history is therefore also evidence.

**Does telemetry slow my job down?**
Tier 0 (always on) uses Delta commit metrics — no scan. Tier 1 (default) attaches `observe()` metrics to the final
DataFrame, computed in the same pass as the write. Tier 2 adds an order-independent `bit_xor(xxhash64(...))` total
over the declared hash columns, in the same pass. Expectation controls are counted in that pass too. Uniqueness and
reconcile controls need `DISTINCT` or a second relation, which `observe()` cannot express: they run as one scan over
the rows this run wrote, selected by `__run_id` — the transformation is never re-executed.

## Technical audit and lineage

**Are you replicating Databricks lineage?**
No. Unity Catalog lineage and query history remain the source. functional_audit stores a projection of them — only
rows that match a run — for three reasons UC does not cover: attaching `run_id`, retaining evidence beyond the
system-table retention window, and reconciling observed lineage against declared intent. Each stored row records
how it was matched (`ENTITY`, `TAG`, or `WINDOW`).

**How does a run get linked to system tables?**
The `runs` row captures every identifier the system tables use: job and task run ids, pipeline update ids, compute
id, principal, and a query tag containing the run id. Lineage is joined on entity ids; query history is joined on
the query tag; the time window is used only as a graded fallback. A run without an entity id records
`linkage: WINDOW_ONLY` in its reconciliation checks.

**When does that linking happen?**
A single platform-owned job per metastore, scheduled daily (system tables lag by a few hours) and available on
demand via `fa reconcile`. It reads only new partitions past a watermark with a short lookback for late arrivals,
joins the day's runs against them, and bounds its own de-duplication to the same window, so daily cost tracks the
day's runs rather than the size of the evidence store. It also marks `RUNNING` rows older than
`spark.functional_audit.abandon_after_hours` as `ABANDONED` and reports runs still `PROVISIONAL` after three days.

**What does `explain_table` give me?**
A generated view, `functional_audit_views.<catalog>__<schema>__<table>`, that joins every business column to the
audit context for that row: requirement, rule citation, contract version, business logic text, structure and
binding hashes, pinned inputs, controls and results, the job and principal that produced it, the tables and columns
actually read, and reconciliation status. You add the `WHERE`; the SDK never sees your domain columns. Views are
created by `fa sync-views` under the platform principal — a stage never runs DDL.

**Why is `explain()` a SQL function and not a Python UDF?**
A SQL table function is inlined into the query plan: the optimizer sees its joins, pushes predicates through it,
and Photon executes it natively. A Python UDF is opaque to the optimizer and cannot read catalog tables from inside
a worker.

## Reconciliation and attestation

**What is reconciliation?**
The comparison of declared versus observed: executed plan structure against the sealed structure, reads against
declared inputs, upstream run state, semantics pinned against runtime confs, controls declared against controls
evaluated, outputs declared against outputs written, and evidence that should exist against evidence that was
captured.

**When does it run?**
Three times. In-process at the end of every stage, producing `PROVISIONAL`. Scheduled the next day once lineage
and query history have landed, promoting to `FINAL` when every declared output has an `ENTITY` or `TAG` match.
On demand via `fa reconcile --run-id` for re-grading.

**What does `strict` mode do?**
Refuses to run a stage without an effective contract, refuses to write on any pre-write deviation, quarantines the
run's rows when a `fail` control fails, and fails the run. `warn` mode records the same findings without blocking,
for first-wave adoption — with one exception: a control declared `on_fail: quarantine` moves rows in both modes. In
`warn` mode a failure to write evidence is logged and never fails the business job; in `strict` mode it does.

**How does attestation work?**
`fa attest` records `APPROVED`, or `WAIVED` with a mandatory reason when reconciliation shows a deviation. Decisions
are superseded by new rows, never edited; `REVOKED` closes one out. A run can be attested only when it is `FINAL`
and the attester is not the principal that ran it. Period-level attestation for a `(contract_id, period)` is
accepted only when every PRODUCTION run in scope is `FINAL` and individually approved or waived.

**How do I explain why a number changed between periods?**
`fa.diff_runs(run_a, run_b)` decomposes the difference into contract change, logic (structure) change, parameter
change (literals, reporting period, scenario overrides), data change (input versions), and semantics change (confs).

## Run types and scenarios

**What are `run_kind` and `run_purpose`?**
`run_kind` describes execution semantics: `incremental`, `backfill`, `replay` (contract as of
`as_of_contract_version`), `restatement` (current contract, superseding a prior run via `supersedes_run_id`).
`run_purpose` describes why it ran: `PRODUCTION`, `SHADOW`, `WHAT_IF`, `ESTIMATE`, `TEST`. Only `PRODUCTION` may
write to the declared output; other purposes are routed to a suffixed table and tracked the same way. Only
`PRODUCTION` runs seal a contract or count as an input's producing run.

**Can I track a what-if scenario or a QIS estimate?**
Yes. A what-if run carries a `scenario_id` and `scenario_params`. The contract declares which parameters may be
overridden; overriding anything else fails the run. The result has full lineage and can be diffed against
production.

## Operations

**What does `fa init` do, and who runs it?**
Creates the `functional_audit` schema, tables, the `explain()` function, the ledger view, the grants you pass, and
runs precondition checks (system tables enabled, query tags propagating). It runs from CI/CD under the platform
principal with versioned, idempotent, checksummed migrations: a migration file that changed after it was applied
fails `fa init`. Consumers never run it.

**Who writes the evidence tables?**
The SDK, as the principal the stage runs under. Use `fa init --grant-writer` so that principal is a dedicated
service principal with MODIFY on the audit schema; job principals that only read evidence get SELECT. The SDK issues
no `DELETE`; every human-touched table (`contracts`, `attestations`) carries `created_by` / `last_altered_by`, and
change data feed provides a second, engine-level trail.

**What runs at cluster startup?**
Nothing. The SDK is imported by the stage code; there is no session hook.

**What if a table already has a `__run_id` column?**
A stage refuses to produce any `__`-prefixed column, and the SDK only ever stamps its own. A pre-existing column
of that name would be a schema conflict at write time, reported as a run failure.
