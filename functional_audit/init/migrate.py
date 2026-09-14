"""Versioned, idempotent migrations for the functional_audit system schema.
Runs from CI/CD under the platform principal. Never at session start."""
from __future__ import annotations
import re
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

from functional_audit.config import Settings

_VERSION_RE = re.compile(r"^V(\d{3})__(.+)\.sql$")


@dataclass
class Migration:
    version: str
    name: str
    sql: str


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
    stmts, buf = [], []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                stmts.append(stmt)
            buf = []
    tail = "\n".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def applied_versions(spark, s: Settings) -> set[str]:
    try:
        rows = spark.sql(f"SELECT version FROM {s.table('_migrations')}").collect()
        return {r[0] for r in rows}
    except Exception:
        return set()


def apply(spark, s: Settings, sdk_version: str, dry_run: bool = False) -> list[str]:
    """Apply pending migrations. Returns the list of versions applied (or planned if dry_run)."""
    done = applied_versions(spark, s)
    applied = []
    for m in load_migrations():
        if m.version in done:
            continue
        stmts = split_statements(render(m.sql, s))
        if dry_run:
            applied.append(m.version)
            continue
        for stmt in stmts:
            spark.sql(stmt)
        spark.sql(
            f"INSERT INTO {s.table('_migrations')} VALUES "
            f"('{m.version}', current_timestamp(), current_user(), '{sdk_version}')"
        )
        applied.append(m.version)
    return applied


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
        spark.conf.set("spark.databricks.queryTags", "fa_precheck=1")
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
