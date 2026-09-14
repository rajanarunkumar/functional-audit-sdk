"""Load contract YAML. A file is either one Contract or a Bundle, whose stages are flattened
to Contract rows; a stage's contract_version and effectivity default to the bundle's."""
from __future__ import annotations
from pathlib import Path

import yaml
from pydantic import ValidationError

from functional_audit.contracts.model import Bundle, Contract


class ContractLoadError(Exception):
    def __init__(self, path: Path, message: str):
        super().__init__(f"{path}: {message}")
        self.path = path


def parse_document(raw: str) -> list[Contract]:
    doc = yaml.safe_load(raw)
    if not isinstance(doc, dict):
        raise ValueError("top-level must be a mapping")
    if "stages" in doc and "calculation_id" in doc:
        return list(Bundle.model_validate(doc).stages)
    return [Contract.model_validate(doc)]


def load_file(path: Path) -> list[tuple[Contract, str]]:
    raw = path.read_text()
    try:
        return [(c, raw) for c in parse_document(raw)]
    except yaml.YAMLError as e:
        raise ContractLoadError(path, f"invalid YAML: {e}") from e
    except (ValidationError, ValueError) as e:
        raise ContractLoadError(path, str(e)) from e


def load_path(root: Path) -> list[tuple[Contract, str, Path]]:
    if not root.exists():
        raise ContractLoadError(root, "contracts directory does not exist")
    out = []
    for p in sorted(root.rglob("*.y*ml")):
        for c, raw in load_file(p):
            out.append((c, raw, p))
    return out
