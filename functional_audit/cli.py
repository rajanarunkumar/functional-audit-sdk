"""fa — CLI. validate | lock | init | publish | sync-views | reconcile | attest | seal"""
from __future__ import annotations
import os
import subprocess
import sys
from pathlib import Path

import click

from functional_audit import __version__
from functional_audit.config import Settings, check_fqn
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
    return s.validate()


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
@click.option("--grant-writer", default=None, help="Principal the SDK writes evidence as (SELECT, MODIFY on the schema)")
@click.option("--grant-reader", multiple=True, help="Principal/group that may read evidence and explain views")
@click.option("--skip-preconditions", is_flag=True)
def init(catalog, schema, dry_run, render_to, grant_writer, grant_reader, skip_preconditions):
    """Create/upgrade the system schema, tables, explain() TVF, ledger view, grants. Run from CI/CD."""
    from functional_audit.init import migrate
    s = _settings(catalog, schema)
    grants = migrate.grant_statements(s, grant_writer, list(grant_reader))
    if render_to:
        for p in migrate.write_rendered(s, render_to):
            click.echo(f"rendered {p}")
        if grants:
            gp = Path(render_to) / "grants.sql"
            gp.write_text(";\n".join(grants) + ";\n")
            click.echo(f"rendered {gp}")
        return
    spark = _spark()
    try:
        applied = migrate.apply(spark, s, __version__, dry_run=dry_run)
    except migrate.MigrationError as e:
        raise click.ClickException(str(e))
    click.echo(("planned" if dry_run else "applied") + f" migrations: {applied or 'none'}")
    if not dry_run:
        for g in grants:
            spark.sql(g)
            click.echo(g)
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
    """Create/refresh explain views for governed tables (all known outputs if none given). Platform CD only."""
    from functional_audit.runtime.explain import view_sql, view_name
    s = _settings(catalog, schema)
    spark = _spark()
    if not tables:
        tables = [r[0] for r in spark.sql(f"SELECT DISTINCT object_name FROM {s.table('run_outputs')}").collect()]
    for t in tables:
        check_fqn(t)
        spark.sql(view_sql(s, t))
        click.echo(f"view {view_name(s, t)}")


@main.command()
@click.option("--run-id", default=None)
@click.option("--since", default=None, help="ISO timestamp; default = watermark - 2 days")
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
def reconcile(run_id, since, catalog, schema):
    """T+1 technical-audit ingestion, FINAL promotion, abandoned-run reaping."""
    from datetime import datetime
    from functional_audit.runtime.ingest import ingest
    res = ingest(_spark(), _settings(catalog, schema), datetime.fromisoformat(since) if since else None, run_id)
    click.echo(res)


@main.command()
@click.option("--contract-id", required=True)
@click.option("--version", "contract_version", required=True, type=int)
@click.option("--run-id", default=None, help="Seal to the plan this run executed")
@click.option("--hash", "structure_hash", default=None, help="Seal to an explicit structure hash")
@click.option("--force", is_flag=True, help="Replace an existing seal (a reviewed re-baseline)")
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
def seal(contract_id, contract_version, run_id, structure_hash, force, catalog, schema):
    """Bind a contract version to a plan structure hash. Later runs with a different plan are PLAN_MISMATCH."""
    from functional_audit.runtime.store import Store
    if not run_id and not structure_hash:
        raise click.UsageError("provide --run-id or --hash")
    store = Store(_spark(), _settings(catalog, schema))
    if run_id:
        row = store.run_row(run_id)
        if not row:
            raise click.ClickException("run not found")
        if row["contract_id"] != contract_id or row["contract_version"] != contract_version:
            raise click.ClickException(f"run belongs to {row['contract_id']} v{row['contract_version']}")
        structure_hash = row["logic_hash_executed"]
        if not structure_hash:
            raise click.ClickException("run has no executed plan hash")
    if not store.seal_logic_hash(contract_id, contract_version, structure_hash, force=force):
        raise click.ClickException("contract not found or already sealed (use --force to re-baseline)")
    click.echo(f"sealed {contract_id} v{contract_version} -> {structure_hash[:12]}")


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
    store = Store(spark, _settings(catalog, schema))
    me = spark.sql("SELECT current_user()").collect()[0][0]
    if run_id:
        row = store.run_row(run_id)
        if not row:
            raise click.ClickException("run not found")
        if row["run_as_principal"] == me:
            raise click.ClickException("segregation of duties: attester cannot be the run principal")
        if row["attestation_stage"] != "FINAL":
            raise click.ClickException(f"run is {row['attestation_stage']}, not FINAL")
    elif decision != "REVOKED":
        runs = store.period_runs(contract_id, period)
        if not runs:
            raise click.ClickException("no PRODUCTION runs for that contract and period")
        blocking = [r["run_id"] for r in runs
                    if r["attestation_stage"] != "FINAL" or r["decision"] not in ("APPROVED", "WAIVED")]
        if blocking:
            raise click.ClickException("period attestation requires every run FINAL and APPROVED/WAIVED; "
                                       f"blocking: {blocking}")
    store.insert_attestation(uuid7(), run_id, contract_id, period, decision, reason)
    click.echo(f"attested {decision}")


def _git_sha() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return os.getenv("GITHUB_SHA")


if __name__ == "__main__":
    main()
