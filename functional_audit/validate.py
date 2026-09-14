"""CI gate. Fails the build when the contract structure is absent or inconsistent.

Checks
  V001  contracts directory exists and every YAML parses against the model
  V002  every @fa.stage(...) / fa.sql(..., contract_id=...) in the code has a contract
  V003  every contract has at least one stage referencing it (warn only)
  V004  duplicate (contract_id, contract_version) across files
  V005  reserved '__' prefix misuse in output keys, controls, telemetry
  V006  stage dependencies inside a calculation form no cycle
  V007  contract_hash changed but contract_version did not (against contracts.lock)
  V008  logic_hash changed but contract_version did not (when plan hashes are supplied to validate())
"""
from __future__ import annotations
import ast
import json
from dataclasses import dataclass, field
from pathlib import Path

from functional_audit.config import RESERVED_PREFIX
from functional_audit.contracts.loader import ContractLoadError, load_path
from functional_audit.contracts.model import Contract

LOCK_FILE = "contracts.lock"


@dataclass
class Finding:
    code: str
    level: str          # ERROR | WARN
    message: str
    path: str | None = None

    def __str__(self) -> str:
        loc = f" [{self.path}]" if self.path else ""
        return f"{self.level} {self.code}{loc}: {self.message}"


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)
    contracts: list[Contract] = field(default_factory=list)

    def error(self, code, msg, path=None):
        self.findings.append(Finding(code, "ERROR", msg, path))

    def warn(self, code, msg, path=None):
        self.findings.append(Finding(code, "WARN", msg, path))

    @property
    def ok(self) -> bool:
        return not any(f.level == "ERROR" for f in self.findings)


# ---- stage discovery ------------------------------------------------------

class _StageVisitor(ast.NodeVisitor):
    """Finds @fa.stage("id") / @stage("id") decorators and fa.sql(..., contract_id="id")."""

    def __init__(self):
        self.refs: list[tuple[str, int]] = []

    def _decorator_id(self, dec) -> str | None:
        if not isinstance(dec, ast.Call):
            return None
        f = dec.func
        name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
        if name != "stage":
            return None
        if dec.args and isinstance(dec.args[0], ast.Constant) and isinstance(dec.args[0].value, str):
            return dec.args[0].value
        for kw in dec.keywords:
            if kw.arg == "contract_id" and isinstance(kw.value, ast.Constant):
                return kw.value.value
        return None

    def visit_FunctionDef(self, node):
        for dec in node.decorator_list:
            cid = self._decorator_id(dec)
            if cid:
                self.refs.append((cid, node.lineno))
        self.generic_visit(node)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Call(self, node):
        f = node.func
        name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
        if name == "sql":
            for kw in node.keywords:
                if kw.arg == "contract_id" and isinstance(kw.value, ast.Constant):
                    self.refs.append((kw.value.value, node.lineno))
        self.generic_visit(node)


def discover_stages(code_root: Path) -> list[tuple[str, str, int]]:
    refs = []
    for p in sorted(code_root.rglob("*.py")):
        if any(part in {".venv", "venv", "site-packages", "build", "dist"} for part in p.parts):
            continue
        try:
            tree = ast.parse(p.read_text())
        except SyntaxError:
            continue
        v = _StageVisitor()
        v.visit(tree)
        refs.extend((cid, str(p), ln) for cid, ln in v.refs)
    return refs


# ---- lock file -------------------------------------------------------------

def read_lock(contracts_root: Path) -> dict:
    p = contracts_root / LOCK_FILE
    if not p.exists():
        return {}
    return json.loads(p.read_text())


def write_lock(contracts_root: Path, contracts: list[Contract], logic_hashes: dict[str, str] | None = None) -> Path:
    lock = {}
    for c in contracts:
        lock[c.contract_id] = {
            "contract_version": c.contract_version,
            "contract_hash": c.contract_hash,
            "logic_hash": (logic_hashes or {}).get(c.contract_id),
        }
    p = contracts_root / LOCK_FILE
    p.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")
    return p


# ---- dependency cycles ----------------------------------------------------------

def dependency_cycles(contracts: list[Contract]) -> list[list[str]]:
    """Cycles among stages of the same calculation, following output -> input edges."""
    by_calc: dict[str, list[Contract]] = {}
    for c in contracts:
        if c.calculation_id:
            by_calc.setdefault(c.calculation_id, []).append(c)
    cycles = []
    for stages in by_calc.values():
        producer = {o.object: c.contract_id for c in stages for o in c.outputs}
        edges = {c.contract_id: sorted({producer[i.object] for i in c.inputs if i.object in producer}) for c in stages}
        state: dict[str, int] = {}
        stack: list[str] = []

        def visit(n):
            state[n] = 1
            stack.append(n)
            for m in edges.get(n, []):
                if state.get(m) == 1:
                    cycles.append(stack[stack.index(m):] + [m])
                elif state.get(m) is None:
                    visit(m)
            stack.pop()
            state[n] = 2

        for n in sorted(edges):
            if state.get(n) is None:
                visit(n)
    return cycles


# ---- validation -------------------------------------------------------------

def validate(contracts_root: Path, code_root: Path | None = None,
             logic_hashes: dict[str, str] | None = None, require_stage_refs: bool = True) -> Report:
    rep = Report()

    # V001
    try:
        loaded = load_path(contracts_root)
    except ContractLoadError as e:
        rep.error("V001", str(e))
        return rep
    if not loaded:
        rep.error("V001", f"no contracts found under {contracts_root}")
        return rep

    contracts = [c for c, _, _ in loaded]
    rep.contracts = contracts
    by_id: dict[str, Contract] = {}

    # V004 + V005
    seen = set()
    for c, _, path in loaded:
        key = (c.contract_id, c.contract_version)
        if key in seen:
            rep.error("V004", f"duplicate contract {c.contract_id} v{c.contract_version}", str(path))
        seen.add(key)
        by_id[c.contract_id] = c
        for o in c.outputs:
            if any(k.startswith(RESERVED_PREFIX) for k in o.key):
                rep.error("V005", f"reserved column in output key: {o.key}", str(path))
        for ctl in c.controls:
            if ctl.expr and RESERVED_PREFIX in ctl.expr:
                rep.error("V005", f"control {ctl.id} references reserved column", str(path))
        for hc in c.telemetry.hash_cols + c.telemetry.numeric_cols + ([c.telemetry.distinct_col] if c.telemetry.distinct_col else []):
            if hc.startswith(RESERVED_PREFIX):
                rep.error("V005", f"telemetry references reserved column {hc}", str(path))

    # V006
    for cyc in dependency_cycles(contracts):
        rep.error("V006", "stage dependency cycle: " + " -> ".join(cyc))

    # V002 / V003 — code references
    if code_root is not None:
        refs = discover_stages(code_root)
        referenced = set()
        for cid, path, ln in refs:
            referenced.add(cid)
            if cid not in by_id:
                rep.error("V002", f"stage references unknown contract '{cid}'", f"{path}:{ln}")
        if require_stage_refs:
            for cid in by_id:
                if cid not in referenced:
                    rep.warn("V003", f"contract '{cid}' is not referenced by any stage")

    # V007 / V008 — drift against lock
    lock = read_lock(contracts_root)
    for c, _, path in loaded:
        prev = lock.get(c.contract_id)
        if not prev:
            continue
        if prev["contract_hash"] != c.contract_hash and prev["contract_version"] == c.contract_version:
            rep.error("V007", f"{c.contract_id}: contract changed but contract_version {c.contract_version} not bumped", str(path))
        if logic_hashes and c.contract_id in logic_hashes and prev.get("logic_hash"):
            if logic_hashes[c.contract_id] != prev["logic_hash"] and prev["contract_version"] == c.contract_version:
                rep.error("V008", f"{c.contract_id}: logic_hash changed but contract_version {c.contract_version} not bumped", str(path))
    return rep
