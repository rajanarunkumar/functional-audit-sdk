from pathlib import Path
import pytest
from pydantic import ValidationError

from functional_audit.contracts.model import Contract, Bundle, Control, Output
from functional_audit.contracts.loader import load_path, load_file, ContractLoadError

EX = Path(__file__).resolve().parents[1] / "examples" / "contracts"


def _min(**over):
    base = dict(contract_id="x.y", contract_version=1, requirement_id="R1", business_logic="b",
                inputs=[{"object": "c.s.in"}], outputs=[{"object": "c.s.out"}])
    base.update(over)
    return Contract.model_validate(base)


def test_bundle_flattens_to_stage_contracts():
    rows = load_path(EX)
    ids = {c.contract_id for c, _, _ in rows}
    assert ids == {"rwa_retail.ead", "rwa_retail.risk_weight"}
    for c, _, _ in rows:
        assert c.calculation_id == "rwa_retail" and c.bundle_version == 2 and c.contract_version == 2


def test_contract_hash_ignores_effectivity():
    a = _min(effective_from="2026-01-01")
    b = _min(effective_from="2027-01-01")
    assert a.contract_hash == b.contract_hash
    assert _min(business_logic="changed").contract_hash != a.contract_hash


def test_reserved_prefix_rejected_in_keys_and_controls():
    with pytest.raises(ValidationError):
        Output(object="c.s.t", key=["__run_id"])
    with pytest.raises(ValidationError):
        Control(id="c", expr="__run_id IS NOT NULL")


def test_input_cannot_be_output():
    with pytest.raises(ValidationError):
        _min(inputs=[{"object": "c.s.same"}], outputs=[{"object": "c.s.same"}])


def test_fqn_required():
    with pytest.raises(ValidationError):
        _min(inputs=[{"object": "not_fqn"}])


def test_bad_yaml_reports_path(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("contract_id: [")
    with pytest.raises(ContractLoadError) as e:
        load_file(p)
    assert "bad.yaml" in str(e.value)


def test_duplicate_stage_in_bundle_rejected():
    with pytest.raises(ValidationError):
        Bundle.model_validate({"calculation_id": "c", "bundle_version": 1, "stages": [
            _min().model_dump(), _min().model_dump()]})
