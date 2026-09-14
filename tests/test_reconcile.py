from functional_audit.config import Settings
from functional_audit.contracts.model import Contract
from functional_audit.runtime.reconcile import provisional, should_quarantine, exempt_read

S = Settings()


def _contract():
    return Contract.model_validate(dict(
        contract_id="a.b", contract_version=1, requirement_id="R", business_logic="x",
        inputs=[{"object": "c.s.i"}], outputs=[{"object": "c.s.o"}],
        controls=[{"id": "C1", "expr": "x > 0"}], semantics={"ansi": True, "timezone": "UTC"}))


def _run(**over):
    kw = dict(structure_hash="h", binding_hash="b", declared_hash="h", declared_inputs={"c.s.i"},
              observed_reads={"c.s.i"}, conf_snapshot={"spark.sql.ansi.enabled": "true",
                                                       "spark.sql.session.timeZone": "UTC"},
              evidence_incomplete=[], upstream={}, linkage="ENTITY")
    kw.update(over)
    return provisional(_contract(), S, **kw)


def test_clean_pre_write_run_is_attested():
    r = _run()
    assert r.status == "ATTESTED" and r.deviations == [] and "controls" not in r.checks


def test_every_pre_write_dimension_is_flagged():
    r = _run(structure_hash="other", observed_reads={"c.s.i", "c.s.extra", "system.access.x",
                                                     "platform_gov.functional_audit.runs"},
             conf_snapshot={"spark.sql.ansi.enabled": "false", "spark.sql.session.timeZone": "UTC"},
             evidence_incomplete=["input version for c.s.i"],
             upstream={"c.s.i": {"run_id": "r0", "status": "FAILED", "reconciliation_status": None}})
    assert sorted(set(r.codes)) == ["EVIDENCE_INCOMPLETE", "PLAN_MISMATCH", "SEMANTICS_MISMATCH",
                                    "UNDECLARED_INPUT", "UPSTREAM_DEVIATION"]
    assert [d["object"] for d in r.deviations if d["code"] == "UNDECLARED_INPUT"] == ["c.s.extra"]


def test_unsealed_contract_never_reports_plan_mismatch():
    assert _run(declared_hash=None).status == "ATTESTED"


def test_missing_plan_is_incomplete_evidence_not_a_pass():
    assert _run(structure_hash=None).codes == ["EVIDENCE_INCOMPLETE"]


def test_post_write_adds_controls_and_outputs():
    r = _run(control_results=[{"id": "C1", "passed": False, "on_fail": "fail"}],
             declared_outputs={"c.s.o"}, written_outputs=set())
    assert sorted(r.codes) == ["CONTROL_FAILED", "OUTPUT_NOT_WRITTEN"]
    ok = _run(control_results=[{"id": "C1", "passed": True, "on_fail": "fail"}],
              declared_outputs={"c.s.o"}, written_outputs={"c.s.o"})
    assert ok.status == "ATTESTED"


def test_warn_controls_never_deviate_but_unevaluated_controls_do():
    r = _run(control_results=[{"id": "C1", "passed": False, "on_fail": "warn"}], declared_outputs={"c.s.o"},
             written_outputs={"c.s.o"})
    assert r.status == "ATTESTED"
    assert _run(control_results=[], declared_outputs={"c.s.o"}, written_outputs={"c.s.o"}).codes == ["CONTROL_NOT_EVALUATED"]


def test_quarantine_decision():
    failed_fail = [{"id": "C1", "passed": False, "on_fail": "fail"}]
    failed_q = [{"id": "C1", "passed": False, "on_fail": "quarantine"}]
    failed_warn = [{"id": "C1", "passed": False, "on_fail": "warn"}]
    assert should_quarantine(failed_fail, strict=True) and not should_quarantine(failed_fail, strict=False)
    assert should_quarantine(failed_q, strict=False) and not should_quarantine(failed_warn, strict=True)


def test_exemptions_follow_configured_schema():
    assert exempt_read("system.access.table_lineage", S)
    assert exempt_read("platform_gov.functional_audit.runs", S)
    assert not exempt_read("platform_gov.functional_audit_views.x", S)
    assert not exempt_read("cap_gold.retail.rwa", S)
