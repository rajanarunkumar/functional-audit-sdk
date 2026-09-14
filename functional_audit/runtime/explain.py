"""Read-side helpers: generated explain views, explain_table(), business_columns(), diff_runs()."""
from __future__ import annotations

from functional_audit.config import RESERVED_PREFIX, Settings


def view_name(s: Settings, table: str) -> str:
    cat, sch, tbl = table.split(".")
    return f"{s.catalog}.{s.schema}_views.{cat}__{sch}__{tbl}"


def view_sql(s: Settings, table: str) -> str:
    return (f"CREATE OR REPLACE VIEW {view_name(s, table)} AS "
            f"SELECT t.*, e.* EXCEPT (e.run_id) FROM {table} t "
            f"LEFT JOIN LATERAL {s.table('explain')}(t.{s.run_id_column}) e")


def explain_table(table: str, spark=None):
    """Business table joined to audit context via __run_id. Returns a DataFrame."""
    from pyspark.sql import SparkSession
    spark = spark or SparkSession.getActiveSession()
    s = Settings.from_spark(spark)
    t = spark.table(table)
    e = spark.sql(f"SELECT * FROM {s.table('explain')}(NULL) WHERE 1=0") if False else None  # placeholder for schema
    return spark.sql(f"SELECT t.*, e.* EXCEPT (e.run_id) FROM {table} t "
                     f"LEFT JOIN LATERAL {s.table('explain')}(t.{s.run_id_column}) e")


def business_columns(df):
    cols = [c for c in df.columns if not c.startswith(RESERVED_PREFIX)]
    return df.select(*cols)


def diff_runs(run_a: str, run_b: str, spark=None) -> dict:
    """Decompose the difference between two runs into contract / logic / data / semantics."""
    from pyspark.sql import SparkSession
    spark = spark or SparkSession.getActiveSession()
    s = Settings.from_spark(spark)
    rows = spark.sql(f"SELECT * FROM {s.table('runs')} WHERE run_id IN ('{run_a}','{run_b}')").collect()
    r = {x["run_id"]: x.asDict() for x in rows}
    if len(r) != 2:
        raise ValueError("both runs must exist")
    a, b = r[run_a], r[run_b]
    ins = spark.sql(f"SELECT run_id, object_name, delta_version FROM {s.table('run_inputs')} "
                    f"WHERE run_id IN ('{run_a}','{run_b}') AND declared").collect()
    ia = {x["object_name"]: x["delta_version"] for x in ins if x["run_id"] == run_a}
    ib = {x["object_name"]: x["delta_version"] for x in ins if x["run_id"] == run_b}
    import json
    ca, cb = a.get("conf_snapshot"), b.get("conf_snapshot")
    ca = json.loads(ca) if isinstance(ca, str) else (ca or {})
    cb = json.loads(cb) if isinstance(cb, str) else (cb or {})
    return {
        "contract": {"changed": (a["contract_id"], a["contract_version"]) != (b["contract_id"], b["contract_version"]),
                     "a": (a["contract_id"], a["contract_version"]), "b": (b["contract_id"], b["contract_version"])},
        "logic": {"changed": a["logic_hash_executed"] != b["logic_hash_executed"],
                  "a": a["logic_hash_executed"], "b": b["logic_hash_executed"]},
        "data": {"changed": ia != ib,
                 "inputs": {k: {"a": ia.get(k), "b": ib.get(k)} for k in sorted(set(ia) | set(ib)) if ia.get(k) != ib.get(k)}},
        "semantics": {"changed": ca != cb,
                      "confs": {k: {"a": ca.get(k), "b": cb.get(k)} for k in sorted(set(ca) | set(cb)) if ca.get(k) != cb.get(k)}},
        "scenario": {"a": a.get("scenario_id"), "b": b.get("scenario_id")},
    }
