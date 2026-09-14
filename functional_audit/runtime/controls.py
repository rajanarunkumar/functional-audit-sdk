"""Contract controls.

Expectations are counted inside the write pass (observe()), so they cost nothing extra and
see exactly the rows that were written. Uniqueness and reconcile controls need DISTINCT or a
second relation, which observe() cannot express; they run as one scan over the rows just
written, selected by __run_id — never by re-executing the transformation."""
from __future__ import annotations

from functional_audit.contracts.model import Contract, Control

_PREFIX = "__ctl_"


def observe_exprs(contract: Contract) -> list:
    from pyspark.sql import functions as F
    out = []
    for c in contract.controls:
        if c.type == "expectation":
            # NULL is a failure: a control must evaluate to TRUE for every row
            out.append(F.sum(F.when(F.expr(c.expr), 0).otherwise(1)).alias(_PREFIX + c.id))
    return out


def evaluate(contract: Contract, observed: dict, written_df, spark=None) -> list[dict]:
    """observed: metrics from the Observation; written_df: the rows this run wrote."""
    from pyspark.sql import functions as F
    results = []
    scan_aggs, scan_meta = [], []
    for c in contract.controls:
        if c.type == "expectation":
            key = _PREFIX + c.id
            if key in observed:
                failures = int(observed[key] or 0)
                results.append(_result(c, passed=failures == 0, failures=failures, source="observe"))
            elif written_df is not None:
                # not propagated through the write: count it over the written rows instead
                scan_aggs.append(F.sum(F.when(F.expr(c.expr), 0).otherwise(1)).alias(key))
                scan_meta.append(c)
            else:
                results.append(_result(c, passed=False, error="expectation not observed"))
        elif c.type == "uniqueness":
            cols = [x.strip() for x in c.expr.split(",")] if c.expr else contract.outputs[0].key
            scan_aggs.append((F.count(F.lit(1)) - F.count_distinct(*[F.col(x) for x in cols])).alias(_PREFIX + c.id))
            scan_meta.append(c)
    if scan_aggs:
        try:
            row = written_df.agg(*scan_aggs).collect()[0].asDict()
            for c in scan_meta:
                failures = int(row.get(_PREFIX + c.id) or 0)
                results.append(_result(c, passed=failures == 0, failures=failures, source="scan"))
        except Exception as e:
            results.extend(_result(c, passed=False, error=str(e)) for c in scan_meta)
    for c in contract.controls:
        if c.type == "reconcile":
            results.append(_reconcile(written_df, c, spark))
    return results


def _result(c: Control, **detail) -> dict:
    return {"id": c.id, "type": c.type, "expr": c.expr, "on_fail": c.on_fail.value, **detail}


def _reconcile(df, c: Control, spark) -> dict:
    from pyspark.sql import functions as F
    try:
        lhs = float(df.agg(F.expr(c.expr)).collect()[0][0] or 0)
        rhs = float(spark.sql(f"SELECT {c.against}").collect()[0][0] or 0)
        tol = c.tolerance or 0.0
        diff = abs(lhs - rhs)
        # tolerance < 1 is relative to the reference value, otherwise absolute
        passed = diff <= tol * max(abs(rhs), 1.0) if tol < 1 else diff <= tol
        return _result(c, against=c.against, lhs=lhs, rhs=rhs, diff=diff, passed=passed)
    except Exception as e:
        return _result(c, against=c.against, passed=False, error=str(e))
