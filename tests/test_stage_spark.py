"""@fa.stage end to end on local Delta: the full sequence, sealing, deviations, strict refusal,
quarantine, evidence, explain() and diff_runs."""
import json
import os

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")
from pyspark.sql import functions as F  # noqa: E402

import functional_audit as fa  # noqa: E402
from functional_audit.contracts.loader import parse_document  # noqa: E402
from functional_audit.runtime import stage as stagemod  # noqa: E402
from functional_audit.runtime.store import Store  # noqa: E402

CONTRACT = """calculation_id: ead_calc
bundle_version: 1
effective_from: 2027-01-01
stages:
  - contract_id: ead_calc.ead
    requirement_id: REQ-1
    business_logic: drawn plus ccf times undrawn
    inputs:
      - {object: spark_catalog.cap_silver.exposures, pin: latest}
      - {object: spark_catalog.cap_ref.ccf_offbalance, pin: contract_effective}
    outputs:
      - {object: spark_catalog.cap_gold.ead, key: [exposure_id, reporting_period]}
    controls:
      - {id: C1, expr: ead >= 0}
      - {id: C2, type: uniqueness}
      - {id: C3, expr: drawn < 130, on_fail: warn}
      - {id: C4, expr: "undrawn < 100", on_fail: quarantine}
    telemetry: {tier: 2, numeric_cols: [ead], distinct_col: exposure_id, hash_cols: [exposure_id, ead]}
    parameters:
      - {name: reporting_period, default: "2027-03-31"}
      - {name: undrawn_cap, default: 100}
"""


def build(inputs, reporting_period, undrawn_cap):
    exp = inputs["spark_catalog.cap_silver.exposures"]
    ccf = inputs["spark_catalog.cap_ref.ccf_offbalance"]
    return (exp.join(ccf, "facility_type", "left")
               .withColumn("ead", F.col("drawn") + F.coalesce(F.col("ccf"), F.lit(0)) * F.least(F.col("undrawn"), F.lit(undrawn_cap)))
               .withColumn("reporting_period", F.lit(reporting_period))
               .select("exposure_id", "reporting_period", "drawn", "undrawn", "ead"))


@pytest.fixture(scope="module")
def published(delta, audit_schema, tables):
    st = Store(delta, audit_schema)
    st.publish_contract(parse_document(CONTRACT)[0], CONTRACT, "sha-e2e")
    return st


@pytest.fixture
def enforce(monkeypatch):
    def _set(mode):
        monkeypatch.setenv("FA_ENFORCE", mode)
    return _set


def _run_of(delta, audit_schema, run_id):
    return Store(delta, audit_schema).run_row(run_id)


def _rec(delta, audit_schema, run_id):
    r = delta.table(audit_schema.table("reconciliations")).where(F.col("run_id") == run_id).collect()[0]
    return r["stage"], r["status"], delta.sql(f"SELECT to_json(deviations) FROM {audit_schema.table('reconciliations')} "
                                              f"WHERE run_id = '{run_id}'").collect()[0][0]


def test_clean_run_writes_evidence_and_seals_the_contract(delta, audit_schema, published, tables, enforce):
    enforce("warn")
    out = fa.stage("ead_calc.ead")(build)(run=fa.RunOptions(reporting_period="2027-03-31"), spark=delta)
    run_id = out.__fa_run_id__
    row = _run_of(delta, audit_schema, run_id)
    assert row["status"] == "SUCCEEDED" and row["attestation_stage"] == "PROVISIONAL" and row["code_hash_source"] == "source"
    assert row["logic_hash_executed"] and row["binding_hash"] and "Join LeftOuter" in row["plan_text"]
    assert row["output_commit_version"] is not None and row["query_tag"].startswith(f"fa_run_id:{run_id}")
    written = delta.table("cap_gold.ead").where(F.col("__run_id") == run_id)
    assert written.count() == 60 and "__run_id" in written.columns
    stage, status, dev = _rec(delta, audit_schema, run_id)
    assert (stage, status, dev) == ("PROVISIONAL", "ATTESTED", "[]")
    inputs = {r["object_name"]: r for r in delta.table(audit_schema.table("run_inputs")).where(F.col("run_id") == run_id).collect()}
    exposures, ccf = inputs["spark_catalog.cap_silver.exposures"], inputs["spark_catalog.cap_ref.ccf_offbalance"]
    assert exposures["pin_mode"] == "latest" and exposures["delta_version"] is not None
    assert ccf["pin_mode"] == "contract_effective"
    kinds = {r["evidence_type"] for r in delta.table(audit_schema.table("evidence")).where(F.col("run_id") == run_id).collect()}
    assert kinds == {"observe", "delta_ops", "context", "controls", "plan"}
    metrics = delta.sql(f"SELECT variant_get(payload, '$.metrics.row_count', 'long'), "
                        f"variant_get(payload, '$.metrics.xxhash64_xor', 'long') "
                        f"FROM {audit_schema.table('evidence')} WHERE run_id = '{run_id}' AND evidence_type = 'observe'").collect()[0]
    assert metrics[0] == 60 and metrics[1] is not None
    controls_json = delta.sql(f"SELECT to_json(payload) FROM {audit_schema.table('evidence')} "
                              f"WHERE run_id = '{run_id}' AND evidence_type = 'controls'").collect()[0][0]
    controls = {c["id"]: c for c in json.loads(controls_json)["results"]}
    assert controls["C1"]["passed"] and controls["C2"]["passed"] and controls["C4"]["passed"]
    assert not controls["C3"]["passed"] and controls["C3"]["failures"] == 30      # drawn >= 130 for half the rows, warn only
    sealed = published.resolve_contract("ead_calc.ead", None, version=1).logic_hash_declared
    assert sealed == row["logic_hash_executed"]


def test_regenerated_code_matches_the_seal_and_next_period_is_a_binding_change(delta, audit_schema, published, enforce):
    enforce("strict")

    def regenerated(inputs, reporting_period, undrawn_cap):
        e = inputs["spark_catalog.cap_silver.exposures"].alias("e")
        c = inputs["spark_catalog.cap_ref.ccf_offbalance"].alias("c")
        return e.join(c, "facility_type", "left").select(
            "exposure_id", F.lit(reporting_period).alias("reporting_period"), "drawn", "undrawn",
            (F.col("drawn") + F.least(F.col("undrawn"), F.lit(undrawn_cap)) * F.coalesce(F.col("ccf"), F.lit(0))).alias("ead"))

    out = fa.stage("ead_calc.ead")(regenerated)(run=fa.RunOptions(reporting_period="2027-06-30"), spark=delta)
    a = _run_of(delta, audit_schema, out.__fa_run_id__)
    first = delta.table(audit_schema.table("runs")).where(F.col("reporting_period") == "2027-03-31").collect()[0]
    assert a["logic_hash_executed"] == first["logic_hash_executed"]
    assert a["binding_hash"] != first["binding_hash"]
    assert _rec(delta, audit_schema, out.__fa_run_id__)[1] == "ATTESTED"
    d = fa.diff_runs(first["run_id"], out.__fa_run_id__, spark=delta)
    assert not d["logic"]["changed"] and d["parameters"]["changed"] and not d["contract"]["changed"]
    assert d["parameters"]["reporting_period"] == {"a": "2027-03-31", "b": "2027-06-30"}


def test_logic_change_is_plan_mismatch_and_strict_refuses_to_write(delta, audit_schema, published, enforce):
    enforce("strict")
    before = delta.table("cap_gold.ead").count()

    def hardcoded(inputs, reporting_period, undrawn_cap):
        exp = inputs["spark_catalog.cap_silver.exposures"]
        return (exp.withColumn("ead", F.col("drawn") + F.lit(0.5) * F.col("undrawn"))
                   .withColumn("reporting_period", F.lit(reporting_period))
                   .select("exposure_id", "reporting_period", "drawn", "undrawn", "ead"))

    with pytest.raises(fa.StageError, match="PLAN_MISMATCH"):
        fa.stage("ead_calc.ead")(hardcoded)(spark=delta)
    assert delta.table("cap_gold.ead").count() == before
    run = delta.table(audit_schema.table("runs")).orderBy(F.desc("started_at")).limit(1).collect()[0]
    assert run["status"] == "FAILED"
    evidence = delta.table(audit_schema.table("evidence")).where(F.col("run_id") == run["run_id"])
    assert evidence.where(F.col("evidence_type") == "error").count() == 1


def test_undeclared_read_is_visible_and_warn_mode_records_it(delta, audit_schema, published, tables, enforce):
    enforce("warn")

    def sneaky(inputs, reporting_period, undrawn_cap):
        df = build(inputs, reporting_period, undrawn_cap)
        draft = inputs["spark_catalog.cap_ref.ccf_draft"]           # not in the contract
        return df.join(draft.select(F.lit("A").alias("k")), F.lit(True), "left_semi") if False else \
            df.join(draft.select("ccf").limit(1).withColumnRenamed("ccf", "x"), F.lit(True), "cross").drop("x")

    out = fa.stage("ead_calc.ead")(sneaky)(spark=delta)
    _, status, dev = _rec(delta, audit_schema, out.__fa_run_id__)
    assert status == "DEVIATION" and "UNDECLARED_INPUT" in dev and "cap_ref.ccf_draft" in dev
    undeclared = delta.table(audit_schema.table("run_inputs")).where((F.col("run_id") == out.__fa_run_id__) & ~F.col("declared")).collect()
    assert [r["object_name"] for r in undeclared] == ["spark_catalog.cap_ref.ccf_draft"]
    assert _run_of(delta, audit_schema, out.__fa_run_id__)["status"] == "SUCCEEDED"


def test_quarantine_control_moves_rows_in_warn_mode(delta, audit_schema, published, enforce):
    enforce("warn")
    delta.createDataFrame([(999, "A", 1.0, 500.0)], "exposure_id INT, facility_type STRING, drawn DOUBLE, undrawn DOUBLE"
                          ).write.format("delta").mode("append").saveAsTable("cap_silver.exposures")
    out = fa.stage("ead_calc.ead")(build)(spark=delta)
    run_id = out.__fa_run_id__
    assert _run_of(delta, audit_schema, run_id)["status"] == "QUARANTINED"
    assert delta.table("cap_gold.ead").where(F.col("__run_id") == run_id).count() == 0
    assert delta.table("cap_gold.ead__quarantine").where(F.col("__run_id") == run_id).count() == 61
    outputs = {r["object_name"] for r in delta.table(audit_schema.table("run_outputs")).where(F.col("run_id") == run_id).collect()}
    assert outputs == {"spark_catalog.cap_gold.ead", "spark_catalog.cap_gold.ead__quarantine"}
    _, status, dev = _rec(delta, audit_schema, run_id)
    assert status == "DEVIATION" and "CONTROL_FAILED" in dev and '"control":"C4"' in dev and "PLAN_MISMATCH" not in dev


def test_downstream_stage_sees_upstream_deviation(delta, audit_schema, published, enforce):
    enforce("strict")
    down = """contract_id: ead_calc.down
contract_version: 1
requirement_id: REQ-2
business_logic: passthrough
inputs: [{object: spark_catalog.cap_gold.ead}]
outputs: [{object: spark_catalog.cap_gold.ead_copy}]
"""
    published.publish_contract(parse_document(down)[0], down, "sha")
    with pytest.raises(fa.StageError, match="UPSTREAM_DEVIATION"):
        fa.stage("ead_calc.down")(lambda inputs: inputs["spark_catalog.cap_gold.ead"].select("exposure_id", "ead"))(spark=delta)
    assert not delta.catalog.tableExists("cap_gold.ead_copy")


def test_scenario_params_must_be_declared_and_reach_the_function(delta, audit_schema, published, enforce):
    enforce("warn")
    with pytest.raises(fa.StageError, match="not declared"):
        fa.stage("ead_calc.ead")(build)(run=fa.RunOptions(run_purpose="WHAT_IF", scenario_params={"bogus": 1}), spark=delta)
    out = fa.stage("ead_calc.ead")(build)(run=fa.RunOptions(run_purpose="WHAT_IF", scenario_id="s1",
                                                            scenario_params={"undrawn_cap": 0}), spark=delta)
    run_id = out.__fa_run_id__
    # the poisoned row from the quarantine test is still an input, so this run is quarantined too — under its own suffix
    rows = delta.table("cap_gold.ead__what_if__quarantine").where(F.col("__run_id") == run_id)
    assert delta.table("cap_gold.ead__what_if").where(F.col("__run_id") == run_id).count() == 0
    assert rows.count() == 61 and rows.where(F.col("ead") != F.col("drawn")).count() == 0      # cap 0 => ead == drawn
    assert delta.sql(f"SELECT variant_get(scenario_params, '$.undrawn_cap', 'int') FROM {audit_schema.table('runs')} "
                     f"WHERE run_id = '{run_id}'").collect()[0][0] == 0


def test_disabled_sdk_runs_the_function_ungoverned(delta, monkeypatch):
    monkeypatch.setenv("FA_ENABLED", "false")
    before = delta.table("cap_gold.ead").count()
    df = fa.stage("ead_calc.ead")(build)(reporting_period="p", undrawn_cap=1, spark=delta)
    assert df.count() > 0
    assert delta.table("cap_gold.ead").count() == before and not hasattr(df, "__fa_run_id__")


def test_missing_contract_is_fatal_only_in_strict(delta, audit_schema, published, enforce):
    enforce("strict")
    with pytest.raises(fa.StageError, match="no effective contract"):
        fa.stage("ead_calc.nope")(build)(spark=delta)
    enforce("warn")
    df = fa.stage("ead_calc.nope")(lambda inputs: inputs["spark_catalog.cap_ref.ccf_offbalance"])(spark=delta)
    assert df.count() > 0


def test_explain_function_and_ledger(delta, audit_schema):
    sealed = Store(delta, audit_schema).resolve_contract("ead_calc.ead", None, version=1).logic_hash_declared
    run = (delta.table(audit_schema.table("runs"))
                .where((F.col("status") == "SUCCEEDED") & (F.col("logic_hash_executed") == sealed)).limit(1).collect()[0])
    e = delta.sql(f"SELECT * FROM {audit_schema.table('explain')}('{run['run_id']}')").collect()
    assert len(e) == 1 and e[0]["business_logic"] == "drawn plus ccf times undrawn" and e[0]["logic_hash"] == sealed
    assert e[0]["inputs_pinned"] and e[0]["reconciliation_stage"] == "PROVISIONAL" and e[0]["run_status"] == "SUCCEEDED"
    ledger = delta.table(audit_schema.table("ledger")).where(F.col("run_id") == run["run_id"]).collect()[0]
    assert ledger["inputs_declared"] == 2 and ledger["logic_hash_declared"] == sealed


def test_explain_view_joins_business_rows_to_audit_context(delta, audit_schema):
    from functional_audit.runtime.explain import view_sql, view_name
    delta.sql(view_sql(audit_schema, "spark_catalog.cap_gold.ead"))
    v = delta.table(view_name(audit_schema, "spark_catalog.cap_gold.ead"))
    row = v.where(F.col("exposure_id") == 1).orderBy("reporting_period").limit(1).collect()[0]
    assert row["ead"] is not None and row["fa_requirement_id"] == "REQ-1" and row["fa_contract_version"] == 1
    assert "reporting_period" in v.columns and "fa_reporting_period" in v.columns      # no collision with business columns


def test_stage_never_produces_reserved_columns(delta, audit_schema, published, enforce):
    enforce("warn")
    with pytest.raises(fa.StageError, match="reserved"):
        fa.stage("ead_calc.ead")(lambda inputs, **_: build(inputs, "p", 1).withColumn("__x", F.lit(1)))(spark=delta)


def test_purpose_suffix_and_quarantine_suffix_are_identifiers():
    assert stagemod._purpose_suffix(fa.RunOptions(run_purpose="ESTIMATE").run_purpose) == "__estimate"
    assert os.path.basename(stagemod.QUARANTINE_SUFFIX) == "__quarantine"
