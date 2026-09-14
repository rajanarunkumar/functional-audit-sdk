"""Runtime units that do not need a SparkSession."""
from functional_audit.ids import uuid7, content_hash
from functional_audit.runtime.plan_hash import logic_hash_from_text, relations_in_plan
from functional_audit.runtime.telemetry import effective_tier
from functional_audit.runtime.reconcile import provisional
from functional_audit.runtime.explain import view_name, view_sql
from functional_audit.init.migrate import load_migrations, render, split_statements
from functional_audit.config import Settings
from functional_audit.contracts.model import Contract


def test_uuid7_is_time_ordered():
    a, b = uuid7(), uuid7()
    assert a < b or a[:8] == b[:8]


def test_plan_hash_ignores_expression_ids_and_whitespace():
    p1 = "Project [ead#12L, rw#13]\n+- Join Inner, (k#1 = k#2)\n   +- Relation cap.s.t"
    p2 = "Project [ead#99L, rw#100]   +- Join Inner, (k#7 = k#8)      +- Relation cap.s.t"
    assert logic_hash_from_text(p1) == logic_hash_from_text(p2)
    assert logic_hash_from_text(p1) != logic_hash_from_text(p1.replace("Inner", "LeftOuter"))


def test_relations_extracted():
    assert relations_in_plan("Relation cap_silver.retail.exposures[..] Relation cap_ref.capital.ccf[..]") == \
        {"cap_silver.retail.exposures", "cap_ref.capital.ccf"}


def test_effective_tier_and_downgrade():
    assert effective_tier(2, 1, False) == (1, None)
    assert effective_tier(1, 2, False) == (1, None)
    assert effective_tier(2, 2, True) == (1, 2)
    assert effective_tier(0, 2, True) == (0, None)


def _contract():
    return Contract.model_validate(dict(
        contract_id="a.b", contract_version=1, requirement_id="R", business_logic="x",
        inputs=[{"object": "c.s.i"}], outputs=[{"object": "c.s.o"}],
        controls=[{"id": "C1", "expr": "x > 0"}], semantics={"ansi": True, "timezone": "UTC"}))


def test_provisional_reconciliation_flags_each_dimension():
    r = provisional(_contract(), logic_hash_executed="h", logic_hash_declared=None,
                    declared_inputs={"c.s.i"}, observed_reads={"c.s.i", "c.s.extra", "system.access.x"},
                    declared_outputs={"c.s.o"}, written_outputs=set(),
                    control_results=[{"id": "C1", "passed": False, "on_fail": "fail"}],
                    conf_snapshot={"spark.sql.ansi.enabled": "false", "spark.sql.session.timeZone": "UTC"},
                    telemetry_downgraded_from=None)
    codes = sorted(d["code"] for d in r.deviations)
    assert r.status == "DEVIATION"
    assert codes == ["CONTROL_FAILED", "OUTPUT_NOT_WRITTEN", "SEMANTICS_MISMATCH", "UNDECLARED_INPUT"]


def test_provisional_reconciliation_attests_clean_run():
    r = provisional(_contract(), logic_hash_executed="h", logic_hash_declared="h",
                    declared_inputs={"c.s.i"}, observed_reads={"c.s.i"},
                    declared_outputs={"c.s.o"}, written_outputs={"c.s.o"},
                    control_results=[{"id": "C1", "passed": True, "on_fail": "fail"}],
                    conf_snapshot={"spark.sql.ansi.enabled": "true", "spark.sql.session.timeZone": "UTC"},
                    telemetry_downgraded_from=None)
    assert r.status == "ATTESTED" and r.deviations == []


def test_view_naming_is_domain_agnostic():
    s = Settings()
    assert view_name(s, "cap_gold.retail.rwa") == "platform_gov.functional_audit_views.cap_gold__retail__rwa"
    assert "t.__run_id" in view_sql(s, "cap_gold.retail.rwa") and "t.*" in view_sql(s, "cap_gold.retail.rwa")


def test_migrations_render_and_split():
    ms = load_migrations()
    assert [m.version for m in ms] == ["001", "002"]
    stmts = split_statements(render(ms[0].sql, Settings()))
    assert any("CREATE TABLE IF NOT EXISTS platform_gov.functional_audit.runs" in x for x in stmts)
    assert not any("${" in x for x in stmts)
    fn = split_statements(render(ms[1].sql, Settings()))
    assert any("CREATE OR REPLACE FUNCTION platform_gov.functional_audit.explain" in x for x in fn)


def test_content_hash_is_order_independent():
    assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})
