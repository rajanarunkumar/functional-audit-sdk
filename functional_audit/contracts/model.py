"""Contract model. Domain-agnostic: the SDK never interprets business column names."""
from __future__ import annotations
import re
from datetime import date
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from functional_audit.config import RESERVED_PREFIX
from functional_audit.ids import content_hash

FQN = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
CONTRACT_ID = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$")


class RunKind(str, Enum):
    incremental = "incremental"
    backfill = "backfill"
    replay = "replay"
    restatement = "restatement"


class RunPurpose(str, Enum):
    PRODUCTION = "PRODUCTION"
    SHADOW = "SHADOW"
    WHAT_IF = "WHAT_IF"
    ESTIMATE = "ESTIMATE"
    TEST = "TEST"


class PinMode(str, Enum):
    latest = "latest"
    contract_effective = "contract_effective"
    explicit = "explicit"


class OnFail(str, Enum):
    fail = "fail"
    warn = "warn"
    quarantine = "quarantine"


class Input(BaseModel):
    object: str
    pin: PinMode = PinMode.latest
    key: list[str] = Field(default_factory=list)
    schema_ref: Optional[str] = None

    @field_validator("object")
    @classmethod
    def _fqn(cls, v: str) -> str:
        if not FQN.match(v):
            raise ValueError(f"input object must be catalog.schema.table: {v}")
        return v


class Output(BaseModel):
    object: str
    key: list[str] = Field(default_factory=list)
    carry_run_id: bool = True

    @field_validator("object")
    @classmethod
    def _fqn(cls, v: str) -> str:
        if not FQN.match(v):
            raise ValueError(f"output object must be catalog.schema.table: {v}")
        return v

    @field_validator("key")
    @classmethod
    def _no_reserved(cls, v: list[str]) -> list[str]:
        bad = [c for c in v if c.startswith(RESERVED_PREFIX)]
        if bad:
            raise ValueError(f"reserved system columns cannot be keys: {bad}")
        return v


class Control(BaseModel):
    id: str
    expr: Optional[str] = None
    type: str = "expectation"          # expectation | reconcile | uniqueness
    on_fail: OnFail = OnFail.fail
    description: Optional[str] = None
    tolerance: Optional[float] = None
    against: Optional[str] = None

    @field_validator("expr")
    @classmethod
    def _no_reserved(cls, v: Optional[str]) -> Optional[str]:
        if v and RESERVED_PREFIX in v:
            raise ValueError("control expressions cannot reference reserved system columns")
        return v


class Telemetry(BaseModel):
    tier: int = 1
    hash_cols: list[str] = Field(default_factory=list)
    numeric_cols: list[str] = Field(default_factory=list)
    distinct_col: Optional[str] = None

    @field_validator("tier")
    @classmethod
    def _tier(cls, v: int) -> int:
        if v not in (0, 1, 2):
            raise ValueError("telemetry.tier must be 0, 1 or 2")
        return v


class Semantics(BaseModel):
    ansi: bool = True
    timezone: str = "UTC"
    decimal_scale: Optional[int] = None
    confs: dict[str, str] = Field(default_factory=dict)


class Parameter(BaseModel):
    """Declares what a WHAT_IF / ESTIMATE run may override."""
    name: str
    type: str = "string"
    default: Any = None
    description: Optional[str] = None


class Contract(BaseModel):
    contract_id: str
    contract_version: int = Field(ge=1)
    requirement_id: str
    rule_citation: Optional[str] = None
    business_logic: str
    inputs: list[Input]
    outputs: list[Output] = Field(min_length=1)
    controls: list[Control] = Field(default_factory=list)
    telemetry: Telemetry = Field(default_factory=Telemetry)
    semantics: Semantics = Field(default_factory=Semantics)
    parameters: list[Parameter] = Field(default_factory=list)
    effective_from: Optional[date] = None
    effective_to: Optional[date] = None
    change_log: dict[str, str] = Field(default_factory=dict)
    calculation_id: Optional[str] = None
    bundle_version: Optional[int] = None

    @field_validator("contract_id")
    @classmethod
    def _cid(cls, v: str) -> str:
        if not CONTRACT_ID.match(v):
            raise ValueError(f"contract_id must be dotted lowercase snake_case: {v}")
        return v

    @model_validator(mode="after")
    def _consistency(self) -> "Contract":
        ids = [c.id for c in self.controls]
        if len(ids) != len(set(ids)):
            raise ValueError("control ids must be unique")
        if self.effective_from and self.effective_to and self.effective_to < self.effective_from:
            raise ValueError("effective_to precedes effective_from")
        ins = {i.object for i in self.inputs}
        outs = {o.object for o in self.outputs}
        if ins & outs:
            raise ValueError(f"object cannot be both input and output: {ins & outs}")
        return self

    @property
    def contract_hash(self) -> str:
        body = self.model_dump(mode="json", exclude={"effective_from", "effective_to", "change_log"})
        return content_hash(body)

    def body_json(self) -> dict:
        return self.model_dump(mode="json")


class Bundle(BaseModel):
    """One YAML that encompasses all stages of a calculation."""
    calculation_id: str
    bundle_version: int = Field(ge=1)
    effective_from: Optional[date] = None
    effective_to: Optional[date] = None
    stages: list[Contract] = Field(min_length=1)

    @model_validator(mode="after")
    def _propagate(self) -> "Bundle":
        seen = set()
        for s in self.stages:
            if s.contract_id in seen:
                raise ValueError(f"duplicate stage contract_id in bundle: {s.contract_id}")
            seen.add(s.contract_id)
            s.calculation_id = self.calculation_id
            s.bundle_version = self.bundle_version
            s.effective_from = s.effective_from or self.effective_from
            s.effective_to = s.effective_to or self.effective_to
        return self
