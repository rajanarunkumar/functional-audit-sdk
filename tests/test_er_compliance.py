"""The ER model as an executable specification, checked against the migrated schema.

Columns listed per table are the ER's; the schema may add columns but never drop or
retype one. Audit-column classes follow the UC information_schema convention."""
import re
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

ROOT = Path(__file__).resolve().parents[1]

HUMAN = {"created", "created_by", "last_altered", "last_altered_by"}
SYSTEM = {"created"}
REGRADED = {"created", "last_altered"}

ER = {
    "contracts": ("human", {"contract_id": "string", "contract_version": "int", "contract_hash": "string",
                            "calculation_id": "string", "bundle_version": "int", "requirement_id": "string",
                            "rule_citation": "string", "logic_hash_declared": "string", "effective_from": "date",
                            "effective_to": "date", "body": "variant", "body_yaml": "string", "source_commit": "string"}),
    "contract_deps": ("system", {"contract_id": "string", "contract_version": "int", "direction": "string",
                                 "object_name": "string", "pin_mode": "string"}),
    "runs": ("system", {"run_id": "string", "contract_id": "string", "contract_version": "int", "run_kind": "string",
                        "run_purpose": "string", "scenario_id": "string", "scenario_params": "variant",
                        "backfill_id": "string", "supersedes_run_id": "string", "reporting_period": "string",
                        "logic_hash_executed": "string", "code_hash": "string", "plan_proto_path": "string",
                        "entity_type": "string", "entity_id": "string", "entity_run_id": "string",
                        "task_run_id": "string", "compute_id": "string", "run_as_principal": "string",
                        "executed_by_principal": "string", "query_tag": "string", "runtime_version": "string",
                        "spark_version": "string", "conf_snapshot": "variant", "started_at": "timestamp",
                        "ended_at": "timestamp", "output_commit_version": "bigint", "status": "string",
                        "attestation_stage": "string"}),
    "run_inputs": ("system", {"run_id": "string", "object_name": "string", "delta_version": "bigint",
                              "num_records": "bigint", "declared": "boolean"}),
    "run_outputs": ("system", {"run_id": "string", "object_name": "string", "commit_version": "bigint",
                               "num_rows": "bigint"}),
    "evidence": ("system", {"run_id": "string", "evidence_type": "string", "payload": "variant"}),
    "lineage_observed": ("system", {"run_id": "string", "level": "string", "direction": "string",
                                    "source_object": "string", "source_column": "string", "target_object": "string",
                                    "target_column": "string", "entity_run_id": "string", "event_time": "timestamp",
                                    "match_method": "string"}),
    "query_observed": ("system", {"run_id": "string", "statement_id": "string", "event_time": "timestamp",
                                  "rows_produced": "bigint", "match_method": "string"}),
    "reconciliations": ("regraded", {"run_id": "string", "stage": "string", "status": "string", "checks": "variant",
                                     "deviations": "variant"}),
    "attestations": ("human", {"attestation_id": "string", "run_id": "string", "contract_id": "string",
                               "period": "string", "decision": "string", "waiver_reason": "string"}),
}
ENUMS = {
    ("runs", "status"): {"RUNNING", "SUCCEEDED", "QUARANTINED", "FAILED", "ABANDONED"},
    ("runs", "attestation_stage"): {"PROVISIONAL", "FINAL"},
    ("contract_deps", "direction"): {"INPUT", "OUTPUT"},
    ("lineage_observed", "level"): {"TABLE", "COLUMN"},
    ("lineage_observed", "direction"): {"READ", "WRITE"},
    ("lineage_observed", "match_method"): {"ENTITY", "TAG", "WINDOW"},
    ("reconciliations", "stage"): {"PROVISIONAL", "FINAL"},
    ("reconciliations", "status"): {"ATTESTED", "DEVIATION", "ERROR"},
    ("attestations", "decision"): {"APPROVED", "WAIVED", "REVOKED"},
}
# the ER's keys, applied by the Databricks-only migration as informational constraints
KEYS = {
    "pk_contracts": ("contracts", "PRIMARY KEY (contract_id, contract_version)"),
    "pk_runs": ("runs", "PRIMARY KEY (run_id)"),
    "pk_reconciliations": ("reconciliations", "PRIMARY KEY (run_id)"),
    "pk_attestations": ("attestations", "PRIMARY KEY (attestation_id)"),
    "fk_contract_deps_contract": ("contract_deps", "REFERENCES ${CATALOG}.${SCHEMA}.contracts (contract_id, contract_version)"),
    "fk_runs_contract": ("runs", "REFERENCES ${CATALOG}.${SCHEMA}.contracts (contract_id, contract_version)"),
    "fk_run_inputs_run": ("run_inputs", "REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id)"),
    "fk_run_outputs_run": ("run_outputs", "REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id)"),
    "fk_evidence_run": ("evidence", "REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id)"),
    "fk_lineage_observed_run": ("lineage_observed", "REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id)"),
    "fk_query_observed_run": ("query_observed", "REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id)"),
    "fk_reconciliations_run": ("reconciliations", "REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id)"),
    "fk_attestations_run": ("attestations", "REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id)"),
}


def _types(spark, table):
    return {f.name: f.dataType.simpleString() for f in spark.table(table).schema.fields}


@pytest.mark.parametrize("table", sorted(ER))
def test_table_has_er_columns_and_audit_class(delta, audit_schema, table):
    klass, columns = ER[table]
    actual = _types(delta, audit_schema.table(table))
    missing = {c: t for c, t in columns.items() if actual.get(c) != t}
    assert not missing, f"{table}: ER columns missing or retyped: {missing}"
    audit = {c for c in actual if c in HUMAN}
    expected = {"human": HUMAN, "system": SYSTEM, "regraded": REGRADED}[klass]
    assert audit == expected, f"{table}: audit columns {audit} != {expected} for class {klass}"
    props = {r[0]: r[1] for r in delta.sql(f"SHOW TBLPROPERTIES {audit_schema.table(table)}").collect()}
    assert props["functional_audit.write_class"] == ("human" if klass == "human" else "system")


def test_enum_columns_are_check_constrained(delta, audit_schema):
    for (table, column), values in ENUMS.items():
        props = {r[0]: r[1] for r in delta.sql(f"SHOW TBLPROPERTIES {audit_schema.table(table)}").collect()}
        exprs = [v for k, v in props.items() if k.startswith("delta.constraints.") and column in v]
        assert exprs, f"{table}.{column} has no CHECK constraint"
        for v in values:
            assert f"'{v}'" in " ".join(exprs), f"{table}.{column}: {v} not in constraint {exprs}"


def test_enum_constraints_are_enforced(delta, audit_schema):
    from functional_audit.runtime.store import Store
    from datetime import datetime, timezone
    with pytest.raises(Exception, match="ck_runs_status|CHECK constraint"):
        Store(delta, audit_schema).insert_run({"run_id": "bad", "contract_id": "x.y", "contract_version": 1,
                                               "run_kind": "incremental", "run_purpose": "PRODUCTION",
                                               "started_at": datetime.now(timezone.utc), "status": "WHATEVER"})


def test_keys_are_declared_in_the_databricks_migration():
    from functional_audit.init.migrate import load_migrations
    v3 = [m for m in load_migrations() if m.version == "003"][0]
    assert v3.requires == "databricks"
    for name, (table, clause) in KEYS.items():
        stmt = re.search(rf"ALTER TABLE \$\{{CATALOG}}\.\$\{{SCHEMA}}\.{table}\s+ADD CONSTRAINT {name} (.+?);", v3.sql, re.S)
        assert stmt, f"{name} missing"
        assert clause in stmt.group(1) and "NOT ENFORCED" in stmt.group(1)
    assert "CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.plans" in v3.sql


def test_databricks_only_migration_is_skipped_here_and_planned_there(delta, audit_schema):
    from functional_audit import __version__
    from functional_audit.init import migrate
    assert migrate.apply(delta, audit_schema, __version__) == []
    assert migrate.apply(delta, audit_schema, __version__, dry_run=True, databricks=True) == ["003"]
    assert not migrate.is_databricks(delta)


def test_derived_objects_exist(delta, audit_schema):
    fns = {r["function"] for r in delta.sql(f"SHOW USER FUNCTIONS IN {audit_schema.fq_schema}").collect()}
    assert any(f.endswith("explain") for f in fns)
    assert delta.table(audit_schema.table("ledger")).columns[:3] == ["run_id", "contract_id", "contract_version"]
    m = delta.sql(f"SELECT run_status, MEASURE(runs) AS n, MEASURE(runs_with_deviation) AS d "
                  f"FROM {audit_schema.table('ledger_metrics')} GROUP BY run_status").collect()
    assert isinstance(m, list)
    assert delta.catalog.databaseExists(f"{audit_schema.schema}_views")


def test_system_tables_are_never_deleted_from_by_the_sdk():
    """The only delete the SDK issues is the quarantine move on a business table."""
    import ast
    deletes, sql_deletes = [], []
    for f in (ROOT / "functional_audit").rglob("*.py"):
        tree = ast.parse(f.read_text())
        for fn in [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
            for node in ast.walk(fn):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "delete":
                    deletes.append((f.name, fn.name))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and "DELETE FROM" in node.value.upper():
                sql_deletes.append(f.name)
    assert deletes == [("store.py", "delete_run_rows")]
    assert sql_deletes == []


def test_run_id_column_is_the_only_business_column_and_is_reserved(delta, settings):
    from functional_audit.config import RESERVED_PREFIX, Settings
    assert settings.run_id_column.startswith(RESERVED_PREFIX)
    with pytest.raises(ValueError):
        Settings(run_id_column="run_id").validate()
    if delta.catalog.tableExists("cap_gold.ead"):
        extra = [c for c in delta.table("cap_gold.ead").columns if c.startswith(RESERVED_PREFIX)]
        assert extra == [settings.run_id_column]
