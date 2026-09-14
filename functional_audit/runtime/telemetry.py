"""Tiered telemetry. Never a separate action:
  tier 0  commit metrics + snapshot stats (free)
  tier 1  observe() on the final DataFrame: count, sums, approx distinct
  tier 2  + xxhash64 totals over declared hash_cols
"""
from __future__ import annotations
import time
from dataclasses import dataclass, field

from functional_audit.contracts.model import Telemetry


@dataclass
class ObserveResult:
    tier: int
    metrics: dict = field(default_factory=dict)
    overhead_ms: float = 0.0
    downgraded_from: int | None = None


def attach(df, spec: Telemetry, tier: int, name: str = "fa"):
    """Attach observation metrics to df. Returns (df, observation or None)."""
    if tier <= 0:
        return df, None
    from pyspark.sql import Observation
    from pyspark.sql import functions as F
    exprs = [F.count(F.lit(1)).alias("row_count")]
    for c in spec.numeric_cols:
        exprs.append(F.sum(F.col(c)).alias(f"sum_{c}"))
    if spec.distinct_col:
        exprs.append(F.approx_count_distinct(F.col(spec.distinct_col)).alias(f"adc_{spec.distinct_col}"))
    if tier >= 2 and spec.hash_cols:
        exprs.append(F.sum(F.xxhash64(*[F.col(c) for c in spec.hash_cols])).alias("xxhash64_total"))
    obs = Observation(name)
    return df.observe(obs, *exprs), obs


def collect(obs, tier: int, started: float) -> ObserveResult:
    if obs is None:
        return ObserveResult(tier=0)
    metrics = {k: (int(v) if isinstance(v, (int,)) else v) for k, v in obs.get.items()}
    return ObserveResult(tier=tier, metrics=metrics, overhead_ms=(time.time() - started) * 1000)


def effective_tier(contract_tier: int | None, settings_tier: int, budget_exceeded: bool) -> tuple[int, int | None]:
    """Contract can lower but not raise beyond settings; budget breach downgrades one tier."""
    tier = min(contract_tier, settings_tier) if contract_tier is not None else settings_tier
    if budget_exceeded and tier > 0:
        return tier - 1, tier
    return tier, None
