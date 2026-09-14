"""Declared-vs-observed reconciliation.

In-process (PROVISIONAL) it runs twice per stage: before the write, on everything that is
known from the plan and the context (structure, inputs, semantics, upstream state, evidence
gaps) so a strict run can refuse to write; and after the write, adding controls and outputs.
T+1 (FINAL) promotion happens in ingest.py once lineage and query history have landed."""
from __future__ import annotations
from dataclasses import dataclass, field

from functional_audit.config import Settings
from functional_audit.contracts.model import Contract, OnFail

BLOCKING_RUN_STATUSES = ("FAILED", "QUARANTINED", "ABANDONED")


@dataclass
class ReconcileResult:
    status: str = "ATTESTED"           # ATTESTED | DEVIATION | ERROR
    checks: dict = field(default_factory=dict)
    deviations: list[dict] = field(default_factory=list)

    def deviate(self, code: str, message: str, **detail):
        self.status = "DEVIATION"
        self.deviations.append({"code": code, "message": message, **detail})

    @property
    def codes(self) -> list[str]:
        return [d["code"] for d in self.deviations]


def exempt_read(name: str, s: Settings) -> bool:
    """System tables and the audit schema itself are never undeclared inputs."""
    from functional_audit.runtime.plan_hash import canonical_name
    n = canonical_name(name)
    return n.startswith("system.") or n.startswith(canonical_name(s.fq_schema) + ".")


def provisional(contract: Contract, s: Settings, *,
                structure_hash: str | None, binding_hash: str | None, declared_hash: str | None,
                declared_inputs: set[str], observed_reads: set[str], conf_snapshot: dict,
                evidence_incomplete: list[str], upstream: dict[str, dict], linkage: str,
                control_results: list[dict] | None = None, declared_outputs: set[str] | None = None,
                written_outputs: set[str] | None = None) -> ReconcileResult:
    r = ReconcileResult()

    # plan
    if structure_hash is None:
        r.deviate("EVIDENCE_INCOMPLETE", "the session could not produce an execution plan", item="plan")
    elif declared_hash and declared_hash != structure_hash:
        r.deviate("PLAN_MISMATCH", "executed plan structure differs from the sealed contract plan",
                  declared=declared_hash, executed=structure_hash)
    r.checks["plan"] = {"declared": declared_hash, "executed": structure_hash, "binding": binding_hash}

    # inputs: everything read must be declared
    undeclared = {o for o in observed_reads - declared_inputs if not exempt_read(o, s)}
    for o in sorted(undeclared):
        r.deviate("UNDECLARED_INPUT", f"plan read {o} which the contract does not declare", object=o)
    r.checks["inputs"] = {"declared": sorted(declared_inputs), "observed": sorted(observed_reads)}

    # upstream: declared inputs whose latest producing run is not clean
    for o, prod in sorted(upstream.items()):
        r.deviate("UPSTREAM_DEVIATION", f"latest producing run of {o} is {prod.get('status')}/"
                  f"{prod.get('reconciliation_status')}", object=o, **prod)
    r.checks["upstream"] = {o: prod.get("run_id") for o, prod in upstream.items()}

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

    # evidence that could not be captured is a finding, never a silent pass
    for item in evidence_incomplete:
        r.deviate("EVIDENCE_INCOMPLETE", f"could not capture {item}", item=item)
    r.checks["linkage"] = linkage

    if control_results is not None:
        failed = [c for c in control_results if not c.get("passed")]
        for c in failed:
            if c.get("on_fail") in (OnFail.fail.value, OnFail.quarantine.value):
                r.deviate("CONTROL_FAILED", f"control {c['id']} failed", control=c["id"], detail=c)
        r.checks["controls"] = {"declared": len(contract.controls), "evaluated": len(control_results),
                                "passed": len(control_results) - len(failed)}
        if len(control_results) != len(contract.controls):
            r.deviate("CONTROL_NOT_EVALUATED", "not all declared controls were evaluated")

    if declared_outputs is not None:
        written = written_outputs or set()
        for o in sorted(declared_outputs - written):
            r.deviate("OUTPUT_NOT_WRITTEN", f"declared output {o} was not written", object=o)
        r.checks["outputs"] = {"declared": sorted(declared_outputs), "written": sorted(written)}
    return r


def should_quarantine(control_results: list[dict], strict: bool) -> bool:
    """Rows are moved when a quarantine control fails, or a fail control fails under strict."""
    for c in control_results:
        if c.get("passed"):
            continue
        if c.get("on_fail") == OnFail.quarantine.value or (strict and c.get("on_fail") == OnFail.fail.value):
            return True
    return False
