"""pytest plugin (entry point functional_audit). Provides:
  fa_contracts     -> list[Contract] loaded from --fa-contracts (default functional_audit/contracts)
  fa_validate      -> Report for the repo (contracts + code)
  fa_spark         -> local Spark session (skips if pyspark is missing)
  fa_local_stage   -> run a @fa.stage function against a local Contract without the system schema
and auto-generates one conformance test per contract when --fa-conformance is passed."""
from __future__ import annotations
from pathlib import Path

import pytest


def pytest_addoption(parser):
    g = parser.getgroup("functional_audit")
    g.addoption("--fa-contracts", default="functional_audit/contracts")
    g.addoption("--fa-code", default=".")
    g.addoption("--fa-conformance", action="store_true", default=False)


@pytest.fixture(scope="session")
def fa_contracts(request):
    from functional_audit.contracts.loader import load_path
    return [c for c, _, _ in load_path(Path(request.config.getoption("--fa-contracts")))]


@pytest.fixture(scope="session")
def fa_validate(request):
    from functional_audit.validate import validate
    return validate(Path(request.config.getoption("--fa-contracts")), Path(request.config.getoption("--fa-code")))


@pytest.fixture(scope="session")
def fa_spark():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("functional_audit-tests")
             .config("spark.sql.ansi.enabled", "true").config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false").getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture
def fa_local_stage():
    """Exercise plan hashing + controls for a stage without the system schema."""
    def _run(fn, inputs: dict, contract):
        from functional_audit.runtime import plan_hash, controls
        df = fn(inputs)
        lh, txt = plan_hash.logic_hash(df)
        return {"df": df, "logic_hash": lh, "plan": txt,
                "controls": controls.evaluate(df, contract, df.sparkSession) if contract.controls else []}
    return _run


def pytest_generate_tests(metafunc):
    if "fa_contract" in metafunc.fixturenames:
        from functional_audit.contracts.loader import load_path
        cs = [c for c, _, _ in load_path(Path(metafunc.config.getoption("--fa-contracts")))]
        metafunc.parametrize("fa_contract", cs, ids=[f"{c.contract_id}@v{c.contract_version}" for c in cs])


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--fa-conformance"):
        return


class ConformanceTests:
    """Mix into a test module: `from functional_audit.testing.plugin import ConformanceTests as TestContracts`."""

    def test_contract_has_requirement(self, fa_contract):
        assert fa_contract.requirement_id

    def test_contract_hash_stable(self, fa_contract):
        assert fa_contract.contract_hash == fa_contract.model_copy().contract_hash

    def test_no_reserved_columns(self, fa_contract):
        for o in fa_contract.outputs:
            assert not any(k.startswith("__") for k in o.key)

    def test_controls_have_expr_or_type(self, fa_contract):
        for c in fa_contract.controls:
            assert c.expr or c.type in ("uniqueness", "reconcile")
