import json
from pathlib import Path

from functional_audit.validate import validate, write_lock, discover_stages

ROOT = Path(__file__).resolve().parents[1]
EX_C, EX_S = ROOT / "examples" / "contracts", ROOT / "examples"


def test_examples_pass_gate():
    rep = validate(EX_C, EX_S)
    assert rep.ok, [str(f) for f in rep.findings]
    assert not [f for f in rep.findings if f.code == "V003"]


def test_missing_directory_is_v001(tmp_path):
    rep = validate(tmp_path / "nope", tmp_path)
    assert not rep.ok and rep.findings[0].code == "V001"


def test_unknown_contract_reference_is_v002(tmp_path):
    (tmp_path / "c").mkdir()
    (tmp_path / "c" / "a.yaml").write_text(
        "contract_id: a.b\ncontract_version: 1\nrequirement_id: R\nbusiness_logic: x\n"
        "inputs: [{object: c.s.i}]\noutputs: [{object: c.s.o}]\n")
    (tmp_path / "job.py").write_text("import functional_audit as fa\n@fa.stage('a.nope')\ndef f(i): return i\n")
    rep = validate(tmp_path / "c", tmp_path)
    assert [f.code for f in rep.findings if f.level == "ERROR"] == ["V002"]


def test_discovers_decorator_and_sql_forms(tmp_path):
    (tmp_path / "j.py").write_text(
        "from functional_audit import stage, sql\n"
        "@stage(contract_id='p.q')\ndef f(i): ...\n"
        "df = sql('select 1', contract_id='p.r')\n")
    assert {c for c, _, _ in discover_stages(tmp_path)} == {"p.q", "p.r"}


def test_contract_drift_without_version_bump_is_v007(tmp_path):
    c = tmp_path / "c"; c.mkdir()
    y = c / "a.yaml"
    y.write_text("contract_id: a.b\ncontract_version: 1\nrequirement_id: R\nbusiness_logic: v1\n"
                 "inputs: [{object: c.s.i}]\noutputs: [{object: c.s.o}]\n")
    rep = validate(c, None)
    write_lock(c, rep.contracts)
    y.write_text(y.read_text().replace("business_logic: v1", "business_logic: v2"))
    rep = validate(c, None)
    assert [f.code for f in rep.findings if f.level == "ERROR"] == ["V007"]
    y.write_text(y.read_text().replace("contract_version: 1", "contract_version: 2"))
    assert validate(c, None).ok


def test_logic_drift_without_version_bump_is_v008(tmp_path):
    c = tmp_path / "c"; c.mkdir()
    (c / "a.yaml").write_text("contract_id: a.b\ncontract_version: 1\nrequirement_id: R\nbusiness_logic: x\n"
                              "inputs: [{object: c.s.i}]\noutputs: [{object: c.s.o}]\n")
    rep = validate(c, None)
    write_lock(c, rep.contracts, {"a.b": "hash1"})
    rep = validate(c, None, logic_hashes={"a.b": "hash2"})
    assert [f.code for f in rep.findings if f.level == "ERROR"] == ["V008"]
    lock = json.loads((c / "contracts.lock").read_text())
    assert lock["a.b"]["logic_hash"] == "hash1"
