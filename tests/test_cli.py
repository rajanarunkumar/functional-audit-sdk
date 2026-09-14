"""CLI commands that need no Spark session."""
import json
from pathlib import Path

from click.testing import CliRunner

from functional_audit.cli import main

ROOT = Path(__file__).resolve().parents[1]


def test_validate_examples_pass_and_report_counts():
    r = CliRunner().invoke(main, ["validate", "--contracts", str(ROOT / "examples/contracts"), "--code", str(ROOT / "examples")])
    assert r.exit_code == 0 and "2 contract(s), 0 error(s), 0 warning(s)" in r.output


def test_validate_fails_the_build_on_errors(tmp_path):
    (tmp_path / "c").mkdir()
    (tmp_path / "c" / "a.yaml").write_text("contract_id: a.b\ncontract_version: 1\nrequirement_id: R\nbusiness_logic: x\n"
                                           "inputs: [{object: c.s.i}]\noutputs: [{object: c.s.o}]\n")
    (tmp_path / "j.py").write_text("import functional_audit as fa\n@fa.stage('a.missing')\ndef f(i): return i\n")
    r = CliRunner().invoke(main, ["validate", "--contracts", str(tmp_path / "c"), "--code", str(tmp_path)])
    assert r.exit_code == 1 and "V002" in r.output
    r = CliRunner().invoke(main, ["validate", "--contracts", str(tmp_path / "c"), "--code", str(tmp_path / "nowhere"),
                                  "--strict-warnings"])
    assert r.exit_code == 1 and "V003" in r.output


def test_lock_writes_contract_hashes(tmp_path):
    c = tmp_path / "c"
    c.mkdir()
    (c / "a.yaml").write_text("contract_id: a.b\ncontract_version: 2\nrequirement_id: R\nbusiness_logic: x\n"
                              "inputs: [{object: c.s.i}]\noutputs: [{object: c.s.o}]\n")
    r = CliRunner().invoke(main, ["lock", "--contracts", str(c)])
    assert r.exit_code == 0
    lock = json.loads((c / "contracts.lock").read_text())
    assert lock["a.b"]["contract_version"] == 2 and len(lock["a.b"]["contract_hash"]) == 64


def test_init_renders_migrations_and_grants_without_a_session(tmp_path):
    r = CliRunner().invoke(main, ["init", "--catalog", "gov", "--schema", "fa", "--render-to", str(tmp_path / "sql"),
                                  "--grant-writer", "sp-fa-writer", "--grant-reader", "analysts", "--grant-reader", "auditors"])
    assert r.exit_code == 0, r.output
    files = sorted(p.name for p in (tmp_path / "sql").iterdir())
    assert files == ["V001__schema.sql", "V002__explain.sql", "grants.sql"]
    v1 = (tmp_path / "sql" / "V001__schema.sql").read_text()
    assert "gov.fa.runs" in v1 and "${" not in v1
    grants = (tmp_path / "sql" / "grants.sql").read_text()
    assert "GRANT SELECT, MODIFY ON SCHEMA gov.fa TO `sp-fa-writer`" in grants
    assert grants.count("GRANT SELECT ON SCHEMA gov.fa TO") == 2 and "MODIFY" not in grants.split("auditors")[-1]


def test_init_rejects_unsafe_identifiers_and_principals(tmp_path):
    r = CliRunner().invoke(main, ["init", "--catalog", "gov; DROP", "--render-to", str(tmp_path)])
    assert r.exit_code != 0
    r = CliRunner().invoke(main, ["init", "--render-to", str(tmp_path), "--grant-writer", "x`; GRANT ALL"])
    assert r.exit_code != 0


def test_seal_and_attest_argument_guards():
    r = CliRunner().invoke(main, ["seal", "--contract-id", "a.b", "--version", "1"])
    assert r.exit_code == 2 and "--run-id or --hash" in r.output
    r = CliRunner().invoke(main, ["attest", "--decision", "WAIVED", "--run-id", "r"])
    assert r.exit_code == 2 and "--reason" in r.output
    r = CliRunner().invoke(main, ["attest", "--decision", "APPROVED"])
    assert r.exit_code == 2 and "--run-id or both" in r.output
