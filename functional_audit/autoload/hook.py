"""Session init. Runs at interpreter start via the .pth file. Read-only: reads confs,
checks the schema version lazily on first SparkSession, installs nothing that needs DDL."""
from __future__ import annotations
import logging

log = logging.getLogger("functional_audit")


def _check(spark) -> None:
    from functional_audit.config import Settings
    try:
        s = Settings.from_spark(spark)
        if not s.enabled:
            return
        rows = spark.sql(f"SELECT max(version) FROM {s.table('_migrations')}").collect()
        log.info("functional_audit schema %s at migration %s", s.fq_schema, rows[0][0] if rows else None)
    except Exception as e:  # never block the session
        log.warning("functional_audit: system schema not reachable (%s)", type(e).__name__)


try:
    from pyspark.sql import SparkSession
    _orig = SparkSession.Builder.getOrCreate

    def _patched(self):
        spark = _orig(self)
        if not getattr(spark, "_fa_checked", False):
            try:
                spark._fa_checked = True
            except Exception:
                pass
            _check(spark)
        return spark

    SparkSession.Builder.getOrCreate = _patched  # type: ignore[assignment]
except Exception:
    pass
