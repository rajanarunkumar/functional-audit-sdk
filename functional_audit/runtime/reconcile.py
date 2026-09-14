"""Declared-vs-observed reconciliation.
In-process (PROVISIONAL): plan hash, inputs, controls, semantics, undeclared reads.
T+1 (FINAL): lineage + query history ingestion from system tables (see ingest.py)."""
from __future__ import annotations
from dataclasses import dataclass, field

from functional_audit.contracts.model import Contract, OnFail


@dataclass
class ReconcileResult:
    status: str = "ATTESTED"           # ATTESTED | DEVIATION | ERROR
    checks: dict = field(default_factory=dict)
    deviations: list[dict] = field(default_factory=list)

    def deviate(self, code: str, message: str, **detail):
        self.status = "DEVIATION"
        self.deviations.append({"code": code, "message": message, **detail})


def provisional(contract: Contract, *, logic_hash_executed: str, logic_hash_declared: str | None,
                declared_inputs: set[str], observed_reads: set[str], declared_outputs: set[str],
                written_outputs: set[str], control_results: list[dict], conf_snapshot: dict,
                telemetry_downgraded_from: int | None) -> ReconcileResult:
    r = ReconcileResult()

    # plan
    if logic_hash_declared and logic_hash_declared != logic_hash_executed:
        r.deviate("PLAN_MISMATCH", "executed plan hash differs from declared",
                  declared=logic_hash_declared, executed=logic_hash_executed)
    r.checks["plan"] = {"declared": logic_hash_declared, "executed": logic_hash_executed}

    # inputs: everything read must be declared (system tables and the audit schema are exempt)
    undeclared = {o for o in observed_reads - declared_inputs
                  if not o.startswith("system.") and ".functional_audit." not in o}
    for o in sorted(undeclared):
        r.deviate("UNDECLARED_INPUT", f"plan read {o} which the contract does not declare", object=o)
    r.checks["inputs"] = {"declared": sorted(declared_inputs), "observed": sorted(observed_reads)}

    # outputs
    missing = declared_outputs - written_outputs
    for o in sorted(missing):
        r.deviate("OUTPUT_NOT_WRITTEN", f"declared output {o} was not written", object=o)
    r.checks["outputs"] = {"declared": sorted(declared_outputs), "written": sorted(written_outputs)}

    # controls
    failed = [c for c in control_results if not c.get("passed")]
    for c in failed:
        if c.get("on_fail") == OnFail.fail.value:
            r.deviate("CONTROL_FAILED", f"control {c['id']} failed", control=c["id"], detail=c)
    r.checks["controls"] = {"declared": len(contract.controls), "evaluated": len(control_results),
                            "passed": len(control_results) - len(failed)}
    if len(control_results) != len(contract.controls):
        r.deviate("CONTROL_NOT_EVALUATED", "not all declared controls were evaluated")

    # semantics
    sem = contract.semantics
    ansi = conf_snapshot.get("spark.sql.ansi.enabled")
    if ansi is not None and str(ansi).lower() != str(sem.ansi).lower():
        r.deviate("SEMANTICS_MISMATCH", "ANSI mode differs from contract", key="spark.sql.ansi.enabled",
                  declared=sem.ansi, observed=ansi)
    tz = conf_snapshot.get("spark.sql.session.timeZone")
    if tz is not None and tz != sem.timezone:
        r.deviate("SEMANTICS_MISMATCH", "session timezone differs from contract",
                  key="spark.sql.session.timeZone", declared=sem.timezone, observed=tz)
    for k, v in sem.confs.items():
        ob = conf_snapshot.get(k)
        if ob is not None and str(ob) != str(v):
            r.deviate("SEMANTICS_MISMATCH", f"{k} differs from contract", key=k, declared=v, observed=ob)
    r.checks["semantics"] = {"ansi": ansi, "timezone": tz}

    if telemetry_downgraded_from is not None:
        r.checks["telemetry"] = {"downgraded_from": telemetry_downgraded_from}
    return r
