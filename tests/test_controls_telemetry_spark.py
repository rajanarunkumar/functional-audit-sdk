import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from functional_audit.contracts.model import Contract  # noqa: E402
from functional_audit.runtime import controls, telemetry  # noqa: E402


def contract(**over):
    base = dict(contract_id="t.c", contract_version=1, requirement_id="R", business_logic="b",
                inputs=[{"object": "c.s.i"}], outputs=[{"object": "c.s.o", "key": ["exposure_id"]}],
                telemetry={"tier": 2, "numeric_cols": ["drawn"], "distinct_col": "facility_type",
                           "hash_cols": ["exposure_id", "drawn", "undrawn"]},
                controls=[{"id": "C1", "expr": "drawn >= 0"},
                          {"id": "C2", "expr": "undrawn > 10", "on_fail": "warn"},
                          {"id": "C3", "type": "uniqueness"},
                          {"id": "C4", "type": "reconcile", "expr": "sum(drawn)",
                           "against": "sum(drawn) FROM cap_silver.exposures", "tolerance": 0.001}])
    base.update(over)
    return Contract.model_validate(base)


def test_tier2_hash_total_does_not_overflow_under_ansi_and_is_order_independent(spark, tables):
    df = spark.table(tables["exp"])
    exprs = telemetry.metric_exprs(contract().telemetry, 2)
    a = df.agg(*exprs).collect()[0].asDict()
    b = df.orderBy(F.desc("exposure_id")).agg(*exprs).collect()[0].asDict()
    assert a["row_count"] == 60 and a["xxhash64_xor"] == b["xxhash64_xor"]
    assert a["sum_drawn"] == b["sum_drawn"] and a["adc_facility_type"] == 2


def test_expectations_are_counted_in_the_same_pass_and_null_is_a_failure(spark, tables):
    c = contract()
    df = spark.table(tables["exp"]).withColumn("drawn", F.when(F.col("exposure_id") == 1, None).otherwise(F.col("drawn")))
    df_obs, obs = telemetry.attach(df, telemetry.metric_exprs(c.telemetry, 1) + controls.observe_exprs(c))
    n = df_obs.count()
    m = telemetry.collect(obs)
    assert n == 60 and m["row_count"] == 60
    assert m["__ctl_C1"] == 1          # the NULL row fails "drawn >= 0"
    assert m["__ctl_C2"] == 60         # undrawn is 10.0 everywhere, so "> 10" fails every row


def test_uniqueness_and_reconcile_run_against_written_rows_only(spark, tables):
    c = contract()
    written = spark.table(tables["exp"])
    dup = written.union(written.limit(3))
    observed = {"__ctl_C1": 0, "__ctl_C2": 60}
    res = {r["id"]: r for r in controls.evaluate(c, observed, dup, spark)}
    assert res["C1"]["passed"] and not res["C2"]["passed"] and res["C2"]["on_fail"] == "warn"
    assert not res["C3"]["passed"] and res["C3"]["failures"] == 3
    assert not res["C4"]["passed"] and res["C4"]["lhs"] > res["C4"]["rhs"]
    ok = {r["id"]: r for r in controls.evaluate(c, observed, written, spark)}
    assert ok["C3"]["passed"] and ok["C4"]["passed"] and ok["C4"]["diff"] == 0.0


def test_missing_observation_is_a_failed_control_not_a_pass():
    res = controls.evaluate(contract(controls=[{"id": "C1", "expr": "x > 0"}]), {}, None, None)
    assert res == [{"id": "C1", "type": "expectation", "expr": "x > 0", "on_fail": "fail",
                    "passed": False, "error": "expectation not observed"}]


def test_unobserved_expectations_fall_back_to_one_scan_of_written_rows(spark, tables):
    c = contract(controls=[{"id": "C1", "expr": "drawn >= 0"}, {"id": "C2", "expr": "undrawn > 10", "on_fail": "warn"}])
    res = {r["id"]: r for r in controls.evaluate(c, {}, spark.table(tables["exp"]), spark)}
    assert res["C1"]["passed"] and res["C1"]["source"] == "scan"
    assert not res["C2"]["passed"] and res["C2"]["failures"] == 60
    assert telemetry.scan(spark.table(tables["exp"]), telemetry.metric_exprs(c.telemetry, 1))["row_count"] == 60


def test_effective_tier_never_exceeds_platform_setting():
    assert telemetry.effective_tier(2, 1) == 1 and telemetry.effective_tier(1, 2) == 1
    assert telemetry.effective_tier(None, 2) == 2 and telemetry.effective_tier(0, 2) == 0
