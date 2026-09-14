"""Store against the real migrated schema on local Delta."""
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")
from pyspark.sql import functions as F  # noqa: E402

from functional_audit.contracts.model import Contract  # noqa: E402
from functional_audit.runtime.store import RunEvidence, Store  # noqa: E402

BUNDLE = """calculation_id: calc
bundle_version: 3
effective_from: 2027-01-01
stages:
  - contract_id: calc.a
    requirement_id: R
    business_logic: x
    inputs: [{object: c.s.i}]
    outputs: [{object: c.s.o}]
"""


@pytest.fixture(scope="module")
def store(delta, audit_schema):
    return Store(delta, audit_schema)


def test_migrations_recorded_with_checksums(delta, audit_schema):
    rows = delta.table(audit_schema.table("_migrations")).select("version", "checksum", "sdk_version").collect()
    assert sorted(r["version"] for r in rows) == ["001", "002"] and all(len(r["checksum"]) == 64 for r in rows)


def test_publish_is_an_upsert_and_deps_are_append_only(store, delta, audit_schema):
    from functional_audit.contracts.loader import parse_document
    c = parse_document(BUNDLE)[0]
    store.publish_contract(c, BUNDLE, "sha1")
    store.publish_contract(c, BUNDLE.replace("business_logic: x", "business_logic: x2"), "sha2")
    rows = delta.table(audit_schema.table("contracts")).where(F.col("contract_id") == "calc.a").collect()
    assert len(rows) == 1 and rows[0]["source_commit"] == "sha2" and rows[0]["created_by"] == rows[0]["last_altered_by"]
    assert delta.sql(f"SELECT variant_get(body, '$.calculation_id', 'string') FROM {audit_schema.table('contracts')} "
                     f"WHERE contract_id = 'calc.a'").collect()[0][0] == "calc"
    deps = delta.table(audit_schema.table("contract_deps")).where(F.col("contract_id") == "calc.a").count()
    assert deps == 4                      # two publishes x (1 input + 1 output), never deleted


def test_resolve_contract_by_period_and_version(store):
    assert store.resolve_contract("calc.a", "2027-03-31").contract_version == 3
    assert store.resolve_contract("calc.a", "2026-12-31") is None            # before effective_from
    assert store.resolve_contract("calc.a", None, version=3).calculation_id == "calc"
    assert store.resolve_contract("calc.a", None, version=99) is None
    assert store.resolve_contract("calc.nope", None) is None


def test_seal_is_first_wins_unless_forced(store):
    assert store.seal_logic_hash("calc.a", 3, "h1") is True
    assert store.seal_logic_hash("calc.a", 3, "h2") is False
    assert store.resolve_contract("calc.a", None, version=3).logic_hash_declared == "h1"
    assert store.seal_logic_hash("calc.a", 3, "h2", force=True) is True
    assert store.resolve_contract("calc.a", None, version=3).logic_hash_declared == "h2"
    assert store.seal_logic_hash("calc.missing", 1, "h") is False


def test_run_lifecycle_and_evidence_flush(store, delta, audit_schema):
    started = datetime.now(timezone.utc)
    store.insert_run({"run_id": "r-1", "contract_id": "calc.a", "contract_version": 3, "run_kind": "incremental",
                      "run_purpose": "PRODUCTION", "conf_snapshot": {"spark.sql.ansi.enabled": "true"},
                      "scenario_params": {"p": 1}, "started_at": started, "status": "RUNNING"})
    with pytest.raises(ValueError):
        store.update_run("r-1", not_a_column=1)
    ev = RunEvidence("r-1")
    ev.inputs += [{"object_name": "c.s.i", "delta_version": 3, "pin_mode": "latest", "declared": True},
                  {"object_name": "c.s.x", "declared": False}]
    ev.outputs.append({"object_name": "c.s.o", "commit_version": 7, "num_rows": 10})
    ev.add("observe", {"row_count": 10, "nested": {"a": [1, 2]}})
    ev.add("controls", {"results": []})
    store.flush(ev)
    store.upsert_reconciliation("r-1", "PROVISIONAL", "ATTESTED", {"plan": {"executed": "h"}}, [])
    store.upsert_reconciliation("r-1", "PROVISIONAL", "DEVIATION", {"plan": {}}, [{"code": "X"}])
    store.update_run("r-1", status="SUCCEEDED", ended_at=datetime.now(timezone.utc), logic_hash_executed="h",
                     plan_proto_path="/Volumes/x/y/plans/r-1.pb", plan_proto=b"\x01\x02", attestation_stage="PROVISIONAL")
    row = store.run_row("r-1")
    assert row["status"] == "SUCCEEDED" and row["plan_proto_path"].endswith("r-1.pb") and bytes(row["plan_proto"]) == b"\x01\x02"
    assert delta.sql(f"SELECT variant_get(scenario_params, '$.p', 'int') "
                     f"FROM {audit_schema.table('runs')} WHERE run_id = 'r-1'").collect()[0][0] == 1
    inputs = delta.table(audit_schema.table("run_inputs")).where(F.col("run_id") == "r-1").orderBy("object_name").collect()
    assert [(r["object_name"], r["delta_version"], r["declared"]) for r in inputs] == [("c.s.i", 3, True), ("c.s.x", None, False)]
    rec = delta.table(audit_schema.table("reconciliations")).where(F.col("run_id") == "r-1").collect()
    assert len(rec) == 1 and rec[0]["status"] == "DEVIATION"
    payload = delta.sql(f"SELECT variant_get(payload, '$.nested.a[1]', 'int') FROM {audit_schema.table('evidence')} "
                        f"WHERE run_id = 'r-1' AND evidence_type = 'observe'").collect()[0][0]
    assert payload == 2


def test_latest_producer_sees_only_production_runs(store):
    assert store.latest_producer("c.s.o") == {"run_id": "r-1", "status": "SUCCEEDED", "reconciliation_status": "DEVIATION"}
    store.insert_run({"run_id": "r-2", "contract_id": "calc.a", "contract_version": 3, "run_kind": "incremental",
                      "run_purpose": "WHAT_IF", "started_at": datetime.now(timezone.utc), "status": "SUCCEEDED"})
    ev = RunEvidence("r-2")
    ev.outputs.append({"object_name": "c.s.o"})
    store.flush(ev)
    assert store.latest_producer("c.s.o")["run_id"] == "r-1"
    assert store.latest_producer("c.s.never") is None


def test_period_runs_and_attestations(store):
    store.insert_run({"run_id": "r-3", "contract_id": "calc.a", "contract_version": 3, "run_kind": "incremental",
                      "run_purpose": "PRODUCTION", "reporting_period": "2027-03-31",
                      "started_at": datetime.now(timezone.utc), "status": "SUCCEEDED", "attestation_stage": "FINAL"})
    store.insert_attestation("a-1", "r-3", None, None, "APPROVED", None)
    store.insert_attestation("a-2", "r-3", None, None, "REVOKED", "oops")
    runs = {r["run_id"]: r for r in store.period_runs("calc.a", "2027-03-31")}
    assert runs["r-3"]["decision"] == "REVOKED" and runs["r-3"]["attestation_stage"] == "FINAL"


def test_abandoned_runs_are_reaped(store):
    store.insert_run({"run_id": "r-old", "contract_id": "calc.a", "contract_version": 3, "run_kind": "incremental",
                      "run_purpose": "PRODUCTION", "started_at": datetime(2020, 1, 1, tzinfo=timezone.utc), "status": "RUNNING"})
    store.mark_abandoned(12)
    assert store.run_row("r-old")["status"] == "ABANDONED" and store.run_row("r-1")["status"] == "SUCCEEDED"


def test_delta_helpers(store, delta, tables):
    v0 = store.latest_version(tables["ccf"])
    assert v0 is not None
    delta.createDataFrame([("C", 0.1)], "facility_type STRING, ccf DOUBLE").write.format("delta").mode("append").saveAsTable(tables["ccf"])
    assert store.latest_version(tables["ccf"]) == v0 + 1
    assert store.version_as_of(tables["ccf"], datetime.now(timezone.utc)) == v0 + 1
    assert store.version_as_of(tables["ccf"], datetime(2000, 1, 1, tzinfo=timezone.utc)) is None
    delta.conf.set("spark.databricks.delta.commitInfo.userMetadata", '{"run_id": "run-xyz"}')
    delta.createDataFrame([("D", 0.1)], "facility_type STRING, ccf DOUBLE").write.format("delta").mode("append").saveAsTable(tables["ccf"])
    delta.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
    delta.createDataFrame([("E", 0.1)], "facility_type STRING, ccf DOUBLE").write.format("delta").mode("append").saveAsTable(tables["ccf"])
    c = store.commit_for_run(tables["ccf"], "run-xyz")
    assert c["version"] == v0 + 2 and c["matched_by"] == "userMetadata" and c["operationMetrics"]["numOutputRows"] == "1"
    assert store.commit_for_run(tables["ccf"], "unknown")["matched_by"] == "latest"
    with pytest.raises(ValueError):
        store.latest_version("cap.s.t; DROP TABLE x")


def test_contract_model_roundtrip_through_store(store):
    c = store.resolve_contract("calc.a", None, version=3)
    assert isinstance(c, Contract) and "logic_hash_declared" not in c.body_json()
