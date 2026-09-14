"""pytest plugin (entry point functional_audit). Provides:
  fa_contracts     -> list[Contract] loaded from --fa-contracts (default functional_audit/contracts)
  fa_validate      -> Report for the repo (contracts + code)
  fa_spark         -> local Spark session (skips if pyspark is missing)
  fa_local_stage   -> identify the plan and evaluate controls for a stage against a local Contract,
                      without the system schema
and parametrizes `fa_contract` over every contract in the repo (see ConformanceTests)."""
from __future__ import annotations
from pathlib import Path

import pytest


def pytest_addoption(parser):
    g = parser.getgroup("functional_audit")
    g.addoption("--fa-contracts", default="functional_audit/contracts")
    g.addoption("--fa-code", default=".")


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
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("functional_audit-tests")
             .config("spark.sql.ansi.enabled", "true").config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false").getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture
def fa_local_stage():
    """Exercise plan identity + controls for a stage without the system schema.

    Expectation controls are evaluated with a count() action over the observed DataFrame;
    uniqueness and reconcile controls scan the same DataFrame."""
    def _run(fn, inputs: dict, contract, **kwargs):
        from functional_audit.runtime import plan_hash, controls, telemetry
        df = fn(inputs, **kwargs)
        ident = plan_hash.identify(df)
        df_obs, obs = telemetry.attach(df, controls.observe_exprs(contract))
        df_obs.count()
        results = controls.evaluate(contract, telemetry.collect(obs), df, df.sparkSession) if contract.controls else []
        return {"df": df, "structure_hash": ident.structure_hash, "literals": ident.literals,
                "relations": ident.relations, "plan": ident.canonical_text, "controls": results}
    return _run


def pytest_generate_tests(metafunc):
    if "fa_contract" in metafunc.fixturenames:
        from functional_audit.contracts.loader import load_path
        cs = [c for c, _, _ in load_path(Path(metafunc.config.getoption("--fa-contracts")))]
        metafunc.parametrize("fa_contract", cs, ids=[f"{c.contract_id}@v{c.contract_version}" for c in cs])


class ConformanceTests:
    """Mix into a test module: `from functional_audit.testing.plugin import ConformanceTests as TestContracts`."""

    def test_contract_has_requirement(self, fa_contract):
        assert fa_contract.requirement_id

    def test_contract_hash_ignores_effectivity_only(self, fa_contract):
        changed = fa_contract.model_copy(update={"business_logic": fa_contract.business_logic + " "})
        assert changed.contract_hash != fa_contract.contract_hash

    def test_no_reserved_columns(self, fa_contract):
        for o in fa_contract.outputs:
            assert not any(k.startswith("__") for k in o.key)

    def test_uniqueness_controls_have_keys(self, fa_contract):
        for c in fa_contract.controls:
            if c.type == "uniqueness":
                assert c.expr or fa_contract.outputs[0].key
