"""Startup properties. Read from spark.functional_audit.* confs when a session exists,
else from environment (FA_*) for CLI and tests. Never from files at runtime."""
from __future__ import annotations
import os
from dataclasses import dataclass, field

CONF_PREFIX = "spark.functional_audit."
RESERVED_PREFIX = "__"


@dataclass
class Settings:
    enabled: bool = True
    catalog: str = "platform_gov"
    schema: str = "functional_audit"
    contracts_path: str = "functional_audit/contracts"
    telemetry_tier: int = 1
    telemetry_budget_pct: float = 5.0
    enforce: str = "warn"            # warn | strict
    run_id_column: str = "__run_id"
    plan_inline_max_bytes: int = 256 * 1024
    volume: str = "plans"            # UC volume name under the schema for plan protos
    extra: dict = field(default_factory=dict)

    @property
    def fq_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def table(self, name: str) -> str:
        return f"{self.fq_schema}.{name}"

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
            s.telemetry_budget_pct = float(get("telemetry.budget_pct", s.telemetry_budget_pct))
            s.enforce = get("enforce", s.enforce)
            s.run_id_column = get("run_id_column", s.run_id_column)
        except Exception:  # pragma: no cover - conf access differences between runtimes
            pass
        if not s.run_id_column.startswith(RESERVED_PREFIX):
            raise ValueError(f"run_id_column must start with '{RESERVED_PREFIX}'")
        return s

    @classmethod
    def from_env(cls) -> "Settings":
        s = cls()
        s.catalog = os.getenv("FA_CATALOG", s.catalog)
        s.schema = os.getenv("FA_SCHEMA", s.schema)
        s.contracts_path = os.getenv("FA_CONTRACTS_PATH", s.contracts_path)
        s.enforce = os.getenv("FA_ENFORCE", s.enforce)
        s.run_id_column = os.getenv("FA_RUN_ID_COLUMN", s.run_id_column)
        s.telemetry_tier = int(os.getenv("FA_TELEMETRY_TIER", s.telemetry_tier))
        return s


settings = Settings.from_env()
