"""Versioned, idempotent migrations for the functional_audit system schema.
Runs from CI/CD under the platform principal. Never at session start.

Each applied migration's checksum is recorded; a migration file that changes after it was
applied fails `fa init` instead of silently diverging from the deployed schema."""
from __future__ import annotations
import hashlib
import os
import re
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

from functional_audit.config import Settings

_VERSION_RE = re.compile(r"^V(\d{3})__(.+)\.sql$")
_PRINCIPAL_RE = re.compile(r"^[A-Za-z0-9_@.\- ]+$")
_REQUIRES_RE = re.compile(r"^--\s*requires:\s*(\w+)\s*$", re.M)


class MigrationError(RuntimeError):
    pass


@dataclass
class Migration:
    version: str
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return checksum(self.sql)

    @property
    def requires(self) -> str | None:
        """Engine requirement declared in the file header (`-- requires: databricks`), if any."""
        m = _REQUIRES_RE.search(self.sql)
        return m.group(1).lower() if m else None


def is_databricks(spark) -> bool:
    """True on Databricks Runtime, Databricks Connect and serverless; False on OSS Spark."""
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return True
    try:
        if spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", None):
            return True
    except Exception:
        pass
    return "databricks" in type(spark).__module__ or "databricks" in str(getattr(spark, "version", "")).lower()


def checksum(sql: str) -> str:
    return hashlib.sha256(sql.encode()).hexdigest()


def load_migrations() -> list[Migration]:
    out = []
    pkg = resources.files("functional_audit.init") / "migrations"
    for entry in sorted(pkg.iterdir(), key=lambda e: e.name):
        m = _VERSION_RE.match(entry.name)
        if m:
            out.append(Migration(m.group(1), m.group(2), entry.read_text()))
    return out


def render(sql: str, s: Settings) -> str:
    return sql.replace("${CATALOG}", s.catalog).replace("${SCHEMA}", s.schema)


def split_statements(sql: str) -> list[str]:
    """Split on ';' outside string literals, dropping '--' comments outside literals."""
    stmts, buf, in_str, i, n = [], [], False, 0, len(sql)
    while i < n:
        c = sql[i]
        if in_str:
            buf.append(c)
            if c == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    buf.append("'")
                    i += 1
                else:
                    in_str = False
        elif c == "'":
            in_str = True
            buf.append(c)
        elif c == "-" and sql.startswith("--", i):
            while i < n and sql[i] != "\n":
                i += 1
            continue
        elif c == ";":
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
        else:
            buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def applied(spark, s: Settings) -> dict[str, str | None]:
    """version -> checksum (None for rows written before checksums existed)."""
    try:
        rows = spark.sql(f"SELECT version, checksum FROM {s.table('_migrations')}").collect()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


def apply(spark, s: Settings, sdk_version: str, dry_run: bool = False, databricks: bool | None = None) -> list[str]:
    """Apply pending migrations. Returns the list of versions applied (or planned if dry_run).

    A migration whose header declares `-- requires: databricks` is skipped on other engines and
    applied on the next `fa init` that runs on Databricks."""
    databricks = is_databricks(spark) if databricks is None else databricks
    done = applied(spark, s)
    out = []
    for m in load_migrations():
        if m.version in done:
            if done[m.version] and done[m.version] != m.checksum:
                raise MigrationError(f"migration V{m.version} changed after it was applied "
                                     f"(applied {done[m.version][:12]}, file {m.checksum[:12]})")
            continue
        if m.requires == "databricks" and not databricks:
            continue
        stmts = split_statements(render(m.sql, s))
        if dry_run:
            out.append(m.version)
            continue
        for stmt in stmts:
            spark.sql(stmt)
        from pyspark.sql import functions as F
        (spark.createDataFrame([(m.version, sdk_version, m.checksum)], "version string, sdk_version string, checksum string")
              .withColumn("applied_at", F.current_timestamp()).withColumn("applied_by", F.current_user())
              .write.format("delta").mode("append").saveAsTable(s.table("_migrations")))
        out.append(m.version)
    return out


def grant_statements(s: Settings, writer: str | None, readers: list[str]) -> list[str]:
    """The SDK writes evidence as `writer`; consumers read the schema and the views as `readers`.
    Job principals never receive MODIFY on the audit schema."""
    stmts = []
    for p in ([writer] if writer else []) + list(readers):
        if not _PRINCIPAL_RE.match(p):
            raise ValueError(f"invalid principal: {p!r}")
    if writer:
        stmts += [f"GRANT USE CATALOG ON CATALOG {s.catalog} TO `{writer}`",
                  f"GRANT USE SCHEMA ON SCHEMA {s.fq_schema} TO `{writer}`",
                  f"GRANT SELECT, MODIFY ON SCHEMA {s.fq_schema} TO `{writer}`"]
    for r in readers:
        stmts += [f"GRANT USE CATALOG ON CATALOG {s.catalog} TO `{r}`",
                  f"GRANT USE SCHEMA ON SCHEMA {s.fq_schema} TO `{r}`",
                  f"GRANT SELECT ON SCHEMA {s.fq_schema} TO `{r}`",
                  f"GRANT USE SCHEMA ON SCHEMA {s.fq_schema}_views TO `{r}`",
                  f"GRANT SELECT ON SCHEMA {s.fq_schema}_views TO `{r}`",
                  f"GRANT EXECUTE ON FUNCTION {s.table('explain')} TO `{r}`"]
    return stmts


def preconditions(spark, s: Settings) -> list[tuple[str, bool, str]]:
    """Checks that must hold for technical-audit linkage to work."""
    checks = []
    for tbl in ("system.access.table_lineage", "system.access.column_lineage", "system.query.history"):
        try:
            spark.sql(f"SELECT 1 FROM {tbl} LIMIT 1").collect()
            checks.append((tbl, True, "readable"))
        except Exception as e:
            checks.append((tbl, False, f"not readable: {type(e).__name__}"))
    try:
        spark.conf.set("spark.databricks.queryTags", "fa_precheck:1")
        spark.sql("SELECT 1").collect()
        checks.append(("query_tags", True, "settable; verify in system.query.history after propagation"))
    except Exception as e:
        checks.append(("query_tags", False, f"cannot set: {type(e).__name__}"))
    return checks


def write_rendered(s: Settings, out_dir: Path) -> list[Path]:
    """Emit rendered SQL to disk for review / manual execution."""
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for m in load_migrations():
        p = out_dir / f"V{m.version}__{m.name}.sql"
        p.write_text(render(m.sql, s))
        paths.append(p)
    return paths
