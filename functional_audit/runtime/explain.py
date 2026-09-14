"""Read-side helpers: generated explain views, explain_table(), business_columns(), diff_runs()."""
from __future__ import annotations
import json

from functional_audit.config import RESERVED_PREFIX, Settings, check_fqn


def view_name(s: Settings, table: str) -> str:
    check_fqn(table)
    parts = table.split(".")
    return f"{s.catalog}.{s.schema}_views.{'__'.join(parts)}"


# output columns of the explain() table function (V002); exposed on views with the fa_ prefix so
# they can never collide with a business column
EXPLAIN_COLUMNS = [
    "contract_id", "contract_version", "calculation_id", "requirement_id", "rule_citation", "business_logic",
    "logic_hash", "logic_hash_declared", "binding_hash", "run_kind", "run_purpose", "scenario_id",
    "supersedes_run_id", "reporting_period", "entity_type", "entity_id", "entity_run_id", "compute_id",
    "run_as_principal", "started_at", "ended_at", "run_status", "output_commit_version", "inputs_pinned",
    "controls", "tables_read", "columns_read", "reconciliation_stage", "reconciliation_status", "attestation_decision",
]


def _explain_select(s: Settings, table: str) -> str:
    check_fqn(table)
    audit = ", ".join(f"e.{c} AS fa_{c}" for c in EXPLAIN_COLUMNS)
    return (f"SELECT t.*, {audit} FROM {table} t "
            f"LEFT JOIN LATERAL {s.table('explain')}(t.{s.run_id_column}) e")


def view_sql(s: Settings, table: str) -> str:
    return f"CREATE OR REPLACE VIEW {view_name(s, table)} AS {_explain_select(s, table)}"


def explain_table(table: str, spark=None):
    """Business table joined to audit context via __run_id (audit columns prefixed fa_). Returns a DataFrame."""
    from pyspark.sql import SparkSession
    spark = spark or SparkSession.getActiveSession()
    s = Settings.from_spark(spark)
    return spark.sql(_explain_select(s, table))


def business_columns(df):
    cols = [c for c in df.columns if not c.startswith(RESERVED_PREFIX)]
    return df.select(*cols)


def _as_dict(v) -> dict:
    if v is None:
        return {}
    return json.loads(v) if isinstance(v, str) else dict(v)


def _as_list(v) -> list:
    if v is None:
        return []
    return json.loads(v) if isinstance(v, str) else list(v)


def diff_runs(run_a: str, run_b: str, spark=None) -> dict:
    """Decompose the difference between two runs into contract, logic (structure), parameters
    (literals, reporting period, scenario overrides), data (pinned input versions) and semantics."""
    from pyspark.sql import SparkSession, functions as F
    spark = spark or SparkSession.getActiveSession()
    s = Settings.from_spark(spark)
    rows = (spark.table(s.table("runs")).where(F.col("run_id").isin(run_a, run_b))
                 .select("run_id", "contract_id", "contract_version", "logic_hash_executed", "binding_hash",
                         F.to_json("literals").alias("literals"), F.to_json("scenario_params").alias("scenario_params"),
                         "reporting_period", F.to_json("conf_snapshot").alias("conf_snapshot"), "scenario_id").collect())
    r = {x["run_id"]: x.asDict() for x in rows}
    if len(r) != 2:
        raise ValueError("both runs must exist")
    a, b = r[run_a], r[run_b]
    ins = (spark.table(s.table("run_inputs")).where(F.col("run_id").isin(run_a, run_b) & F.col("declared"))
                .select("run_id", "object_name", "delta_version").collect())
    ia = {x["object_name"]: x["delta_version"] for x in ins if x["run_id"] == run_a}
    ib = {x["object_name"]: x["delta_version"] for x in ins if x["run_id"] == run_b}
    la, lb = _as_list(a["literals"]), _as_list(b["literals"])
    pa, pb = _as_dict(a["scenario_params"]), _as_dict(b["scenario_params"])
    ca, cb = _as_dict(a["conf_snapshot"]), _as_dict(b["conf_snapshot"])
    return {
        "contract": {"changed": (a["contract_id"], a["contract_version"]) != (b["contract_id"], b["contract_version"]),
                     "a": (a["contract_id"], a["contract_version"]), "b": (b["contract_id"], b["contract_version"])},
        "logic": {"changed": a["logic_hash_executed"] != b["logic_hash_executed"],
                  "a": a["logic_hash_executed"], "b": b["logic_hash_executed"]},
        "parameters": {"changed": a["binding_hash"] != b["binding_hash"] and (la != lb or pa != pb or
                                                                              a["reporting_period"] != b["reporting_period"]),
                       "reporting_period": {"a": a["reporting_period"], "b": b["reporting_period"]},
                       "literals": [{"a": x, "b": y} for x, y in zip(la, lb) if x != y] +
                                   ([{"a": la[len(lb):], "b": lb[len(la):]}] if len(la) != len(lb) else []),
                       "scenario_params": {k: {"a": pa.get(k), "b": pb.get(k)} for k in sorted(set(pa) | set(pb))
                                           if pa.get(k) != pb.get(k)}},
        "data": {"changed": ia != ib,
                 "inputs": {k: {"a": ia.get(k), "b": ib.get(k)} for k in sorted(set(ia) | set(ib)) if ia.get(k) != ib.get(k)}},
        "semantics": {"changed": ca != cb,
                      "confs": {k: {"a": ca.get(k), "b": cb.get(k)} for k in sorted(set(ca) | set(cb)) if ca.get(k) != cb.get(k)}},
        "scenario": {"a": a.get("scenario_id"), "b": b.get("scenario_id")},
    }
