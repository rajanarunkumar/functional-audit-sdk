"""Load contract YAML from disk. Bundle stages are flattened to Contract rows;
a stage's contract_version defaults to the bundle version."""
from __future__ import annotations
from pathlib import Path

import yaml
from pydantic import ValidationError

from functional_audit.contracts.model import Bundle, Contract


class ContractLoadError(Exception):
    def __init__(self, path: Path, message: str):
        super().__init__(f"{path}: {message}")
        self.path = path


def _is_bundle(doc: dict) -> bool:
    return "stages" in doc and "calculation_id" in doc


def load_file(path: Path) -> list[tuple[Contract, str]]:
    raw = path.read_text()
    try:
        doc = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise ContractLoadError(path, f"invalid YAML: {e}") from e
    if not isinstance(doc, dict):
        raise ContractLoadError(path, "top-level must be a mapping")
    try:
        if _is_bundle(doc):
            for st in doc["stages"]:
                st.setdefault("contract_version", doc["bundle_version"])
            b = Bundle.model_validate(doc)
            return [(c, raw) for c in b.stages]
        return [(Contract.model_validate(doc), raw)]
    except ValidationError as e:
        raise ContractLoadError(path, str(e)) from e


def load_path(root: Path) -> list[tuple[Contract, str, Path]]:
    if not root.exists():
        raise ContractLoadError(root, "contracts directory does not exist")
    out = []
    for p in sorted(root.rglob("*.y*ml")):
        for c, raw in load_file(p):
            out.append((c, raw, p))
    return out
