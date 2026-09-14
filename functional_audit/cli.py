"""fa — CLI. init | validate | publish | sync-views | reconcile | attest | lock"""
from __future__ import annotations
import os
import subprocess
import sys
from pathlib import Path

import click

from functional_audit import __version__
from functional_audit.config import Settings
from functional_audit.validate import validate as _validate, write_lock


def _spark():
    try:
        from databricks.connect import DatabricksSession  # type: ignore
        return DatabricksSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


def _settings(catalog, schema) -> Settings:
    s = Settings.from_env()
    if catalog:
        s.catalog = catalog
    if schema:
        s.schema = schema
    return s


@click.group()
@click.version_option(__version__, prog_name="fa")
def main():
    """functional_audit CLI."""


@main.command()
@click.option("--contracts", default="functional_audit/contracts", show_default=True, type=click.Path(path_type=Path))
@click.option("--code", default=".", show_default=True, type=click.Path(path_type=Path))
@click.option("--no-stage-refs", is_flag=True, help="Do not warn on contracts without stage references")
@click.option("--strict-warnings", is_flag=True, help="Treat warnings as errors")
def validate(contracts: Path, code: Path, no_stage_refs: bool, strict_warnings: bool):
    """CI gate: fail if the contract structure is absent or inconsistent."""
    rep = _validate(contracts, code, require_stage_refs=not no_stage_refs)
    for f in rep.findings:
        click.echo(str(f))
    n_err = sum(f.level == "ERROR" for f in rep.findings)
    n_warn = sum(f.level == "WARN" for f in rep.findings)
    click.echo(f"{len(rep.contracts)} contract(s), {n_err} error(s), {n_warn} warning(s)")
    if n_err or (strict_warnings and n_warn):
        sys.exit(1)


@main.command()
@click.option("--contracts", default="functional_audit/contracts", type=click.Path(path_type=Path))
def lock(contracts: Path):
    """Write contracts.lock (contract hashes) — run after a reviewed version bump."""
    rep = _validate(contracts, None)
    if not rep.ok:
        for f in rep.findings:
            click.echo(str(f))
        sys.exit(1)
    p = write_lock(contracts, rep.contracts)
    click.echo(f"wrote {p}")


@main.command()
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
@click.option("--dry-run", is_flag=True)
@click.option("--render-to", type=click.Path(path_type=Path), default=None, help="Write rendered SQL to a directory instead of executing")
@click.option("--skip-preconditions", is_flag=True)
def init(catalog, schema, dry_run, render_to, skip_preconditions):
    """Create/upgrade the system schema, tables, explain() TVF, ledger view. Run from CI/CD."""
    from functional_audit.init import migrate
    s = _settings(catalog, schema)
    if render_to:
        for p in migrate.write_rendered(s, render_to):
            click.echo(f"rendered {p}")
        return
    spark = _spark()
    applied = migrate.apply(spark, s, __version__, dry_run=dry_run)
    click.echo(("planned" if dry_run else "applied") + f" migrations: {applied or 'none'}")
    if not skip_preconditions and not dry_run:
        for name, ok, msg in migrate.preconditions(spark, s):
            click.echo(f"{'OK  ' if ok else 'FAIL'} {name}: {msg}")


@main.command()
@click.option("--contracts", default="functional_audit/contracts", type=click.Path(path_type=Path))
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
@click.option("--source-commit", default=None)
def publish(contracts: Path, catalog, schema, source_commit):
    """Publish merged contracts to the contracts table. Run from CD on merge to main."""
    from functional_audit.contracts.loader import load_path
    from functional_audit.runtime.store import Store
    rep = _validate(contracts, None)
    if not rep.ok:
        for f in rep.findings:
            click.echo(str(f))
        sys.exit(1)
    commit = source_commit or _git_sha()
    store = Store(_spark(), _settings(catalog, schema))
    for c, raw, path in load_path(contracts):
        store.publish_contract(c, raw, commit)
        click.echo(f"published {c.contract_id} v{c.contract_version} ({path.name})")


@main.command("sync-views")
@click.argument("tables", nargs=-1)
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
def sync_views(tables, catalog, schema):
    """Create/refresh explain views for governed tables (all known outputs if none given)."""
    from functional_audit.runtime.explain import view_sql, view_name
    s = _settings(catalog, schema)
    spark = _spark()
    if not tables:
        tables = [r[0] for r in spark.sql(f"SELECT DISTINCT object_name FROM {s.table('run_outputs')}").collect()]
    for t in tables:
        spark.sql(view_sql(s, t))
        spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('functional_audit.view_version'='1','functional_audit.view'='{view_name(s, t)}')")
        click.echo(f"view {view_name(s, t)}")


@main.command()
@click.option("--run-id", default=None)
@click.option("--since", default=None, help="ISO timestamp; default = watermark - 2 days")
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
def reconcile(run_id, since, catalog, schema):
    """T+1 technical-audit ingestion and FINAL promotion."""
    from datetime import datetime
    from functional_audit.runtime.ingest import ingest
    res = ingest(_spark(), _settings(catalog, schema), datetime.fromisoformat(since) if since else None, run_id)
    click.echo(res)


@main.command()
@click.option("--run-id", default=None)
@click.option("--contract-id", default=None)
@click.option("--period", default=None)
@click.option("--decision", type=click.Choice(["APPROVED", "WAIVED", "REVOKED"]), required=True)
@click.option("--reason", default=None)
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
def attest(run_id, contract_id, period, decision, reason, catalog, schema):
    """Record a human attestation for a run or a (contract_id, period)."""
    from functional_audit.ids import uuid7
    from functional_audit.runtime.store import Store
    if not run_id and not (contract_id and period):
        raise click.UsageError("provide --run-id or both --contract-id and --period")
    if decision == "WAIVED" and not reason:
        raise click.UsageError("--reason is required for WAIVED")
    spark = _spark()
    s = _settings(catalog, schema)
    if run_id:
        r = spark.sql(f"SELECT run_as_principal, attestation_stage FROM {s.table('runs')} WHERE run_id='{run_id}'").collect()
        if not r:
            raise click.ClickException("run not found")
        me = spark.sql("SELECT current_user()").collect()[0][0]
        if r[0][0] == me:
            raise click.ClickException("segregation of duties: attester cannot be the run principal")
        if r[0][1] != "FINAL":
            raise click.ClickException(f"run is {r[0][1]}, not FINAL")
    Store(spark, s).insert_attestation(uuid7(), run_id, contract_id, period, decision, reason)
    click.echo(f"attested {decision}")


def _git_sha() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return os.getenv("GITHUB_SHA")


if __name__ == "__main__":
    main()
