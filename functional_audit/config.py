"""Startup properties. Read from spark.functional_audit.* confs when a session exists,
else from environment (FA_*) for CLI and tests. Never from files at runtime."""
from __future__ import annotations
import os
import re
from dataclasses import dataclass

CONF_PREFIX = "spark.functional_audit."
RESERVED_PREFIX = "__"
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def check_identifier(name: str, what: str = "identifier") -> str:
    """Only plain identifiers may be spliced into SQL; everything else is bound as a parameter."""
    if not _IDENT.match(name):
        raise ValueError(f"invalid {what}: {name!r}")
    return name


def check_fqn(name: str) -> str:
    parts = name.split(".")
    if len(parts) not in (2, 3):
        raise ValueError(f"expected catalog.schema.table: {name!r}")
    for p in parts:
        check_identifier(p, "table name part")
    return name


@dataclass
class Settings:
    enabled: bool = True
    catalog: str = "platform_gov"
    schema: str = "functional_audit"
    contracts_path: str = "functional_audit/contracts"
    telemetry_tier: int = 1
    enforce: str = "warn"            # warn | strict
    run_id_column: str = "__run_id"
    plan_inline_max_bytes: int = 256 * 1024
    abandon_after_hours: int = 12    # RUNNING rows older than this are marked ABANDONED by fa reconcile

    @property
    def fq_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def table(self, name: str) -> str:
        return f"{self.fq_schema}.{check_identifier(name, 'table')}"

    def validate(self) -> "Settings":
        check_identifier(self.catalog, "catalog")
        check_identifier(self.schema, "schema")
        check_identifier(self.run_id_column, "run_id_column")
        if not self.run_id_column.startswith(RESERVED_PREFIX):
            raise ValueError(f"run_id_column must start with '{RESERVED_PREFIX}'")
        if self.enforce not in ("warn", "strict"):
            raise ValueError("enforce must be warn or strict")
        if self.telemetry_tier not in (0, 1, 2):
            raise ValueError("telemetry.tier must be 0, 1 or 2")
        return self

    @classmethod
    def from_spark(cls, spark) -> "Settings":
        s = cls.from_env()
        try:
            conf = spark.conf
            get = lambda k, d: conf.get(CONF_PREFIX + k, d)  # noqa: E731
            s.enabled = str(get("enabled", s.enabled)).lower() == "true"
            s.catalog = get("catalog", s.catalog)
            s.schema = get("schema", s.schema)
            s.contracts_path = get("contracts.path", s.contracts_path)
            s.telemetry_tier = int(get("telemetry.tier", s.telemetry_tier))
            s.enforce = get("enforce", s.enforce)
            s.run_id_column = get("run_id_column", s.run_id_column)
            s.abandon_after_hours = int(get("abandon_after_hours", s.abandon_after_hours))
        except Exception:  # pragma: no cover - conf access differences between runtimes
            pass
        return s.validate()

    @classmethod
    def from_env(cls) -> "Settings":
        s = cls()
        s.enabled = os.getenv("FA_ENABLED", "true").lower() == "true"
        s.catalog = os.getenv("FA_CATALOG", s.catalog)
        s.schema = os.getenv("FA_SCHEMA", s.schema)
        s.contracts_path = os.getenv("FA_CONTRACTS_PATH", s.contracts_path)
        s.enforce = os.getenv("FA_ENFORCE", s.enforce)
        s.run_id_column = os.getenv("FA_RUN_ID_COLUMN", s.run_id_column)
        s.telemetry_tier = int(os.getenv("FA_TELEMETRY_TIER", s.telemetry_tier))
        return s.validate()
