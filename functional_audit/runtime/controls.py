"""Evaluate contract controls against the output DataFrame in a single aggregate pass."""
from __future__ import annotations

from functional_audit.contracts.model import Contract, Control


def evaluate(df, contract: Contract, spark=None) -> list[dict]:
    from pyspark.sql import functions as F
    results = []
    aggs, meta = [], []
    for c in contract.controls:
        if c.type == "expectation" and c.expr:
            aggs.append(F.sum(F.when(F.expr(c.expr), 0).otherwise(1)).alias(f"fail_{c.id}"))
            meta.append(c)
        elif c.type == "uniqueness":
            cols = c.expr.split(",") if c.expr else contract.outputs[0].key
            aggs.append((F.count(F.lit(1)) - F.count_distinct(*[F.col(x.strip()) for x in cols])).alias(f"fail_{c.id}"))
            meta.append(c)
    row = df.agg(*aggs).collect()[0].asDict() if aggs else {}
    for c in meta:
        failures = int(row.get(f"fail_{c.id}") or 0)
        results.append({"id": c.id, "type": c.type, "expr": c.expr, "on_fail": c.on_fail.value,
                        "failures": failures, "passed": failures == 0})
    for c in contract.controls:
        if c.type == "reconcile":
            results.append(_reconcile(df, c, spark))
    return results


def _reconcile(df, c: Control, spark) -> dict:
    from pyspark.sql import functions as F
    try:
        lhs = float(df.agg(F.expr(c.expr)).collect()[0][0] or 0)
        rhs = float(spark.sql(f"SELECT {c.against}").collect()[0][0] or 0) if spark and c.against else 0.0
        tol = c.tolerance or 0.0
        diff = abs(lhs - rhs)
        passed = diff <= tol * max(abs(rhs), 1.0) if tol < 1 else diff <= tol
        return {"id": c.id, "type": c.type, "expr": c.expr, "against": c.against, "on_fail": c.on_fail.value,
                "lhs": lhs, "rhs": rhs, "diff": diff, "passed": passed}
    except Exception as e:
        return {"id": c.id, "type": c.type, "on_fail": c.on_fail.value, "passed": False, "error": str(e)}
