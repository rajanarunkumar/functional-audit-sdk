"""Tiered telemetry, computed inside the write pass via observe(). Never a separate action.
  tier 0  commit metrics + snapshot stats only
  tier 1  row count, sums over numeric_cols, approx distinct
  tier 2  + bit_xor of xxhash64 over hash_cols (order-independent, cannot overflow)
"""
from __future__ import annotations
import logging

from functional_audit.contracts.model import Telemetry

log = logging.getLogger("functional_audit.telemetry")


def effective_tier(contract_tier: int | None, settings_tier: int) -> int:
    """The contract can lower the tier but never raise it above the platform setting."""
    return min(contract_tier, settings_tier) if contract_tier is not None else settings_tier


def metric_exprs(spec: Telemetry, tier: int) -> list:
    if tier <= 0:
        return []
    from pyspark.sql import functions as F
    exprs = [F.count(F.lit(1)).alias("row_count")]
    for c in spec.numeric_cols:
        exprs.append(F.sum(F.col(c)).alias(f"sum_{c}"))
    if spec.distinct_col:
        exprs.append(F.approx_count_distinct(F.col(spec.distinct_col)).alias(f"adc_{spec.distinct_col}"))
    if tier >= 2 and spec.hash_cols:
        exprs.append(F.bit_xor(F.xxhash64(*[F.col(c) for c in spec.hash_cols])).alias("xxhash64_xor"))
    return exprs


def attach(df, exprs: list, name: str = "fa"):
    """Attach aggregate expressions as an Observation. Returns (df, observation | None)."""
    if not exprs:
        return df, None
    from pyspark.sql import Observation
    obs = Observation(name)
    return df.observe(obs, *exprs), obs


def collect(obs) -> dict:
    """Observed metrics after the action that consumed the DataFrame has completed.

    Empty when the engine did not propagate them through the write command (seen with Delta
    writes on OSS Spark); callers then fall back to one scan of the rows that were written."""
    if obs is None:
        return {}
    try:
        return {k: (int(v) if isinstance(v, bool) else v) for k, v in obs.get.items()}
    except Exception as e:
        log.warning("observe() metrics unavailable after the write (%s); falling back to a scan", type(e).__name__)
        return {}


def scan(df, exprs: list) -> dict:
    """Fallback: the same aggregate expressions evaluated over df in one pass."""
    if not exprs:
        return {}
    return df.agg(*exprs).collect()[0].asDict()
