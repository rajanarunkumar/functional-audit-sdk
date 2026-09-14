"""Writes to the functional_audit system schema.

No value ever travels inside SQL text: rows are written through the DataFrame API and
updates go through the Delta table API, so the only strings spliced into statements are
identifiers validated by config.check_identifier / check_fqn. Per-run evidence is buffered
in RunEvidence and flushed with one append per table. Nothing here deletes evidence."""
from __future__ import annotations
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from functional_audit.config import Settings, check_fqn, check_identifier
from functional_audit.contracts.model import Contract

log = logging.getLogger("functional_audit.store")

_RUNS_SCHEMA = {
    "run_id": "string", "contract_id": "string", "contract_version": "int", "run_kind": "string",
    "run_purpose": "string", "scenario_id": "string", "scenario_params": "variant", "backfill_id": "string",
    "supersedes_run_id": "string", "reporting_period": "string", "logic_hash_executed": "string",
    "binding_hash": "string", "code_hash": "string", "code_hash_source": "string", "plan_text": "string",
    "literals": "variant", "plan_proto": "binary", "workspace_id": "string", "metastore_id": "string",
    "entity_type": "string", "entity_id": "string", "entity_run_id": "string", "task_run_id": "string",
    "task_key": "string", "compute_id": "string", "compute_type": "string", "run_as_principal": "string",
    "executed_by_principal": "string", "query_tag": "string", "runtime_version": "string", "spark_version": "string",
    "conf_snapshot": "variant", "started_at": "timestamp", "ended_at": "timestamp", "output_commit_version": "bigint",
    "status": "string", "attestation_stage": "string",
}


def _json(obj) -> str:
    return json.dumps(obj, default=str, sort_keys=True)


@dataclass
class RunEvidence:
    run_id: str
    inputs: list[dict] = field(default_factory=list)      # object_name, delta_version, pin_mode, declared
    outputs: list[dict] = field(default_factory=list)     # object_name, commit_version, num_rows
    evidence: list[tuple[str, dict]] = field(default_factory=list)

    def add(self, evidence_type: str, payload: dict) -> None:
        self.evidence.append((evidence_type, payload))


class Store:
    def __init__(self, spark, s: Settings):
        self.spark, self.s = spark, s

    # --- row building -----------------------------------------------------------
    def _frame(self, rows: list[dict], schema: dict[str, str], created: bool = True):
        """DataFrame with the given typed columns; VARIANT columns are passed as JSON text and parsed."""
        from pyspark.sql import functions as F
        cols = list(schema)
        for c in cols:
            check_identifier(c, "column")
        ddl = ", ".join(f"{c} {'string' if t == 'variant' else t}" for c, t in schema.items())
        data = [tuple(_json(r.get(c)) if schema[c] == "variant" and r.get(c) is not None else r.get(c) for c in cols)
                for r in rows]
        df = self.spark.createDataFrame(data, ddl)
        for c, t in schema.items():
            if t == "variant":
                df = df.withColumn(c, F.parse_json(F.col(c)))
        if created:
            df = df.withColumn("created", F.current_timestamp())
        return df

    def _append(self, table: str, df) -> None:
        df.write.format("delta").mode("append").saveAsTable(table)

    def _delta(self, table: str):
        from delta.tables import DeltaTable
        return DeltaTable.forName(self.spark, table)

    # --- contracts ------------------------------------------------------------
    def resolve_contract(self, contract_id: str, period: str | None, version: int | None = None) -> Contract | None:
        from pyspark.sql import functions as F
        df = self.spark.table(self.s.table("contracts")).where(F.col("contract_id") == F.lit(contract_id))
        if version is not None:
            df = df.where(F.col("contract_version") == F.lit(version))
        elif period:
            p = F.to_date(F.lit(period))
            df = df.where((F.col("effective_from").isNull() | (F.col("effective_from") <= p))
                          & (F.col("effective_to").isNull() | (F.col("effective_to") >= p)))
        rows = df.select("body_yaml", "logic_hash_declared").orderBy(F.desc("contract_version")).limit(1).collect()
        if not rows:
            return None
        from functional_audit.contracts.loader import parse_document
        for c in parse_document(rows[0][0]):
            if c.contract_id == contract_id:
                c.logic_hash_declared = rows[0][1]
                return c
        return None

    def publish_contract(self, c: Contract, body_yaml: str, source_commit: str | None) -> None:
        from pyspark.sql import functions as F
        src = self._frame([{
            "contract_id": c.contract_id, "contract_version": c.contract_version, "contract_hash": c.contract_hash,
            "calculation_id": c.calculation_id, "bundle_version": c.bundle_version, "requirement_id": c.requirement_id,
            "rule_citation": c.rule_citation, "effective_from": c.effective_from, "effective_to": c.effective_to,
            "body": c.body_json(), "body_yaml": body_yaml, "source_commit": source_commit,
        }], {"contract_id": "string", "contract_version": "int", "contract_hash": "string", "calculation_id": "string",
             "bundle_version": "int", "requirement_id": "string", "rule_citation": "string", "effective_from": "date",
             "effective_to": "date", "body": "variant", "body_yaml": "string", "source_commit": "string"}, created=False)
        upd = {k: F.col(f"src.{k}") for k in ("contract_hash", "calculation_id", "bundle_version", "requirement_id",
                                                "rule_citation", "effective_from", "effective_to", "body", "body_yaml",
                                                "source_commit")}
        (self._delta(self.s.table("contracts")).alias("x")
             .merge(src.alias("src"), "x.contract_id = src.contract_id AND x.contract_version = src.contract_version")
             .whenMatchedUpdate(set={**upd, "last_altered": F.current_timestamp(), "last_altered_by": F.current_user()})
             .whenNotMatchedInsert(values={**{k: F.col(f"src.{k}") for k in src.columns},
                                           "logic_hash_declared": F.lit(None).cast("string"),
                                           "created": F.current_timestamp(), "created_by": F.current_user(),
                                           "last_altered": F.current_timestamp(), "last_altered_by": F.current_user()})
             .execute())
        deps = [{"contract_id": c.contract_id, "contract_version": c.contract_version, "direction": "INPUT",
                 "object_name": i.object, "pin_mode": i.pin.value} for i in c.inputs]
        deps += [{"contract_id": c.contract_id, "contract_version": c.contract_version, "direction": "OUTPUT",
                  "object_name": o.object, "pin_mode": None} for o in c.outputs]
        self._append(self.s.table("contract_deps"), self._frame(deps, {
            "contract_id": "string", "contract_version": "int", "direction": "string", "object_name": "string",
            "pin_mode": "string"}))

    def seal_logic_hash(self, contract_id: str, version: int, structure_hash: str, force: bool = False) -> bool:
        """Record the plan the contract is bound to. Only the first seal succeeds unless forced."""
        from pyspark.sql import functions as F
        key = (F.col("contract_id") == F.lit(contract_id)) & (F.col("contract_version") == F.lit(version))
        rows = self.spark.table(self.s.table("contracts")).where(key).select("logic_hash_declared").collect()
        if not rows or (rows[0][0] is not None and not force):
            return False
        cond = key if force else key & F.col("logic_hash_declared").isNull()
        self._delta(self.s.table("contracts")).update(condition=cond, set={
            "logic_hash_declared": F.lit(structure_hash), "last_altered": F.current_timestamp(),
            "last_altered_by": F.current_user()})
        return True

    # --- runs -----------------------------------------------------------------
    def insert_run(self, run: dict) -> None:
        unknown = set(run) - set(_RUNS_SCHEMA)
        if unknown:
            raise ValueError(f"unknown runs columns {sorted(unknown)}")
        self._append(self.s.table("runs"), self._frame([run], {k: _RUNS_SCHEMA[k] for k in run}))

    def update_run(self, run_id: str, **fields) -> None:
        from pyspark.sql import functions as F
        unknown = set(fields) - set(_RUNS_SCHEMA)
        if unknown:
            raise ValueError(f"unknown runs columns {sorted(unknown)}")
        sets = {}
        for k, v in fields.items():
            t = _RUNS_SCHEMA[k]
            if v is None:
                sets[k] = F.lit(None).cast("string" if t == "variant" else t)
            elif t == "variant":
                sets[k] = F.parse_json(F.lit(_json(v)))
            else:
                sets[k] = F.lit(v).cast(t)
        self._delta(self.s.table("runs")).update(condition=F.col("run_id") == F.lit(run_id), set=sets)

    def flush(self, ev: RunEvidence) -> None:
        """One append per evidence table for the whole run."""
        if ev.inputs:
            self._append(self.s.table("run_inputs"), self._frame(
                [{"run_id": ev.run_id, "object_name": r["object_name"], "delta_version": r.get("delta_version"),
                  "num_records": r.get("num_records"), "pin_mode": r.get("pin_mode"),
                  "declared": bool(r.get("declared", True))} for r in ev.inputs],
                {"run_id": "string", "object_name": "string", "delta_version": "bigint", "num_records": "bigint",
                 "pin_mode": "string", "declared": "boolean"}))
        if ev.outputs:
            self._append(self.s.table("run_outputs"), self._frame(
                [{"run_id": ev.run_id, "object_name": r["object_name"], "commit_version": r.get("commit_version"),
                  "num_rows": r.get("num_rows")} for r in ev.outputs],
                {"run_id": "string", "object_name": "string", "commit_version": "bigint", "num_rows": "bigint"}))
        if ev.evidence:
            self._append(self.s.table("evidence"), self._frame(
                [{"run_id": ev.run_id, "evidence_type": t, "payload": p} for t, p in ev.evidence],
                {"run_id": "string", "evidence_type": "string", "payload": "variant"}))

    def upsert_reconciliation(self, run_id: str, stage: str, status: str, checks: dict, deviations: list) -> None:
        from pyspark.sql import functions as F
        src = self._frame([{"run_id": run_id, "stage": stage, "status": status, "checks": checks, "deviations": deviations}],
                          {"run_id": "string", "stage": "string", "status": "string", "checks": "variant",
                           "deviations": "variant"}, created=False)
        (self._delta(self.s.table("reconciliations")).alias("x")
             .merge(src.alias("src"), "x.run_id = src.run_id")
             .whenMatchedUpdate(set={"stage": F.col("src.stage"), "status": F.col("src.status"),
                                     "checks": F.col("src.checks"), "deviations": F.col("src.deviations"),
                                     "last_altered": F.current_timestamp()})
             .whenNotMatchedInsert(values={k: F.col(f"src.{k}") for k in src.columns} |
                                   {"created": F.current_timestamp(), "last_altered": F.current_timestamp()})
             .execute())

    def insert_attestation(self, attestation_id: str, run_id, contract_id, period, decision, reason) -> None:
        from pyspark.sql import functions as F
        df = self._frame([{"attestation_id": attestation_id, "run_id": run_id, "contract_id": contract_id,
                           "period": period, "decision": decision, "waiver_reason": reason}],
                         {"attestation_id": "string", "run_id": "string", "contract_id": "string", "period": "string",
                          "decision": "string", "waiver_reason": "string"})
        self._append(self.s.table("attestations"), df.withColumn("created_by", F.current_user())
                     .withColumn("last_altered", F.current_timestamp()).withColumn("last_altered_by", F.current_user()))

    # --- queries ----------------------------------------------------------------
    def latest_producer(self, object_name: str) -> dict | None:
        """Status of the most recent PRODUCTION run that wrote object_name, with its reconciliation status."""
        from pyspark.sql import functions as F
        o = self.spark.table(self.s.table("run_outputs")).where(F.col("object_name") == F.lit(object_name))
        r = self.spark.table(self.s.table("runs")).where(F.col("run_purpose") == "PRODUCTION")
        x = self.spark.table(self.s.table("reconciliations")).select("run_id", F.col("status").alias("reconciliation_status"))
        rows = (o.join(r, "run_id").join(x, "run_id", "left")
                 .select("run_id", "status", "reconciliation_status", "started_at")
                 .orderBy(F.desc("started_at")).limit(1).collect())
        return {k: v for k, v in rows[0].asDict().items() if k != "started_at"} if rows else None

    def run_row(self, run_id: str) -> dict | None:
        from pyspark.sql import functions as F
        rows = self.spark.table(self.s.table("runs")).where(F.col("run_id") == F.lit(run_id)).collect()
        return rows[0].asDict() if rows else None

    def period_runs(self, contract_id: str, period: str) -> list[dict]:
        from pyspark.sql import functions as F, Window
        r = (self.spark.table(self.s.table("runs"))
                 .where((F.col("contract_id") == F.lit(contract_id)) & (F.col("reporting_period") == F.lit(period))
                        & (F.col("run_purpose") == "PRODUCTION"))
                 .select("run_id", "status", "attestation_stage"))
        a = self.spark.table(self.s.table("attestations")).where(F.col("run_id").isNotNull())
        latest = (a.withColumn("_rn", F.row_number().over(Window.partitionBy("run_id").orderBy(F.desc("created"))))
                    .where(F.col("_rn") == 1).select("run_id", "decision"))
        return [x.asDict() for x in r.join(latest, "run_id", "left").collect()]

    def mark_abandoned(self, older_than_hours: int) -> None:
        from pyspark.sql import functions as F
        cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
        self._delta(self.s.table("runs")).update(
            condition=(F.col("status") == "RUNNING") & (F.col("started_at") < F.lit(cutoff)),
            set={"status": F.lit("ABANDONED"), "ended_at": F.current_timestamp()})

    # --- delta helpers ----------------------------------------------------------
    def history(self, table: str, limit: int = 50):
        check_fqn(table)
        return self._delta(table).history(limit)

    def latest_version(self, table: str) -> int | None:
        check_fqn(table)
        try:
            r = self.history(table, 1).select("version").collect()
            return int(r[0][0]) if r else None
        except Exception as e:
            log.warning("latest_version(%s) failed: %s", table, type(e).__name__)
            return None

    def version_as_of(self, table: str, ts: datetime) -> int | None:
        from pyspark.sql import functions as F
        check_fqn(table)
        try:
            r = self.history(table, 10_000).where(F.col("timestamp") <= F.lit(ts)).agg(F.max("version")).collect()
            return int(r[0][0]) if r and r[0][0] is not None else None
        except Exception as e:
            log.warning("version_as_of(%s) failed: %s", table, type(e).__name__)
            return None

    def commit_for_run(self, table: str, run_id: str) -> dict | None:
        """The Delta commit whose userMetadata carries this run_id (falls back to the newest commit)."""
        check_fqn(table)
        try:
            rows = self.history(table, 50).select("version", "operation", "operationMetrics", "userMetadata").collect()
        except Exception as e:
            log.warning("commit_for_run(%s) failed: %s", table, type(e).__name__)
            return None
        match = [r for r in rows if r["userMetadata"] and run_id in r["userMetadata"]]
        row = (match or rows[:1] or [None])[0]
        if row is None:
            return None
        d = row.asDict()
        return {"version": int(d["version"]), "operation": d.get("operation"),
                "operationMetrics": dict(d.get("operationMetrics") or {}), "userMetadata": d.get("userMetadata"),
                "matched_by": "userMetadata" if match else "latest"}

    def delete_run_rows(self, table: str, run_id: str) -> None:
        from pyspark.sql import functions as F
        check_fqn(table)
        self._delta(table).delete(F.col(check_identifier(self.s.run_id_column)) == F.lit(run_id))
