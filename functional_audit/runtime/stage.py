"""@fa.stage — the only authoring surface. Sequence:

 1. run_id, resolve contract           5. build df, hash plan, attach observe()
 2. capture context, write runs row    6. write output with __run_id + commit metadata
 3. set query tags                     7. evidence + controls
 4. pin inputs                         8. provisional reconciliation
"""
from __future__ import annotations
import functools
import inspect
import json
import time
import traceback
from datetime import datetime, timezone
from typing import Callable, Optional

from functional_audit.config import RESERVED_PREFIX, Settings
from functional_audit.contracts.model import Contract, RunKind, RunPurpose
from functional_audit.ids import uuid7, text_hash
from functional_audit.runtime import context as ctxmod, plan_hash, telemetry, controls as ctlmod, reconcile
from functional_audit.runtime.store import Store

_QUERY_TAG_CONF = "spark.databricks.queryTags"
_USER_META_CONF = "spark.databricks.delta.commitInfo.userMetadata"


class StageError(RuntimeError):
    pass


class Inputs(dict):
    """Mapping object_name -> DataFrame, resolved and pinned by the SDK."""
    versions: dict[str, Optional[int]]


class RunOptions:
    def __init__(self, run_kind: RunKind = RunKind.incremental, run_purpose: RunPurpose = RunPurpose.PRODUCTION,
                 reporting_period: Optional[str] = None, backfill_id: Optional[str] = None,
                 supersedes_run_id: Optional[str] = None, scenario_id: Optional[str] = None,
                 scenario_params: Optional[dict] = None, input_versions: Optional[dict[str, int]] = None,
                 as_of_contract_version: Optional[int] = None, write_mode: str = "append"):
        self.run_kind, self.run_purpose = RunKind(run_kind), RunPurpose(run_purpose)
        self.reporting_period, self.backfill_id = reporting_period, backfill_id
        self.supersedes_run_id, self.scenario_id = supersedes_run_id, scenario_id
        self.scenario_params, self.input_versions = scenario_params or {}, input_versions or {}
        self.as_of_contract_version, self.write_mode = as_of_contract_version, write_mode


def _purpose_suffix(p: RunPurpose) -> str:
    return "" if p == RunPurpose.PRODUCTION else f"__{p.value.lower()}"


def _get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def stage(contract_id: str, *, local_contract: Optional[Contract] = None):
    """Decorate a function `(inputs: Inputs, **kwargs) -> DataFrame`."""

    def deco(fn: Callable):
        @functools.wraps(fn)
        def wrapper(*args, run: Optional[RunOptions] = None, spark=None, **kwargs):
            return _execute(contract_id, fn, args, kwargs, run or RunOptions(), spark, local_contract)
        wrapper.__fa_contract_id__ = contract_id
        return wrapper
    return deco


def sql(query: str, *, contract_id: str, run: Optional[RunOptions] = None, spark=None,
        local_contract: Optional[Contract] = None):
    def _fn(inputs, **_):
        return _get_spark().sql(query)
    _fn.__name__ = f"sql_{contract_id}"
    _fn.__fa_source__ = query
    return _execute(contract_id, _fn, (), {}, run or RunOptions(), spark, local_contract)


def _execute(contract_id, fn, args, kwargs, opts: RunOptions, spark, local_contract):
    spark = spark or _get_spark()
    s = Settings.from_spark(spark)
    if not s.enabled:
        return fn(Inputs(), *args, **kwargs)
    store = Store(spark, s)
    run_id = uuid7()

    # 1. contract
    contract = local_contract or store.resolve_contract(contract_id, opts.reporting_period)
    if contract is None:
        msg = f"no effective contract for '{contract_id}' (period={opts.reporting_period})"
        if s.enforce == "strict":
            raise StageError(msg)
        contract = None
    if contract and opts.scenario_params:
        allowed = {p.name for p in contract.parameters}
        bad = set(opts.scenario_params) - allowed
        if bad:
            raise StageError(f"scenario overrides not declared in contract parameters: {sorted(bad)}")

    # 2. context + runs row (before any data is read)
    ctx = ctxmod.capture(spark, contract.semantics.confs if contract else None)
    started = datetime.now(timezone.utc)
    query_tag = f"fa_run_id={run_id};fa_contract={contract_id};fa_ver={contract.contract_version if contract else 0}"
    try:
        src = getattr(fn, "__fa_source__", None) or inspect.getsource(fn)
    except (OSError, TypeError):
        src = fn.__name__
    code_hash = text_hash(src)
    store.insert_run({
        "run_id": run_id, "contract_id": contract_id,
        "contract_version": contract.contract_version if contract else 0,
        "run_kind": opts.run_kind.value, "run_purpose": opts.run_purpose.value,
        "scenario_id": opts.scenario_id, "scenario_params": opts.scenario_params or None,
        "backfill_id": opts.backfill_id, "supersedes_run_id": opts.supersedes_run_id,
        "reporting_period": opts.reporting_period, "code_hash": code_hash,
        **{k: v for k, v in ctx.as_dict().items() if k != "conf_snapshot"},
        "conf_snapshot": ctx.conf_snapshot, "query_tag": query_tag,
        "started_at": started, "status": "RUNNING", "attestation_stage": None,
    })

    # 3. query tags
    prev_tag = _safe_get(spark, _QUERY_TAG_CONF)
    prev_meta = _safe_get(spark, _USER_META_CONF)
    _safe_set(spark, _QUERY_TAG_CONF, query_tag)

    try:
        # 4. pin inputs
        inputs = Inputs()
        inputs.versions = {}
        input_rows = []
        for i in (contract.inputs if contract else []):
            ver = opts.input_versions.get(i.object)
            if ver is None and i.pin.value != "explicit":
                ver = store.latest_version(i.object)
            df_in = spark.read.format("delta").option("versionAsOf", ver).table(i.object) if ver is not None else spark.table(i.object)
            inputs[i.object] = df_in
            inputs.versions[i.object] = ver
            input_rows.append({"object_name": i.object, "delta_version": ver, "pin_mode": i.pin.value, "declared": True})
        store.insert_inputs(run_id, input_rows)

        # 5. build + hash + observe
        df = fn(inputs, *args, **kwargs)
        if df is None:
            raise StageError("stage function returned None; it must return a DataFrame")
        bad = [c for c in df.columns if c.startswith(RESERVED_PREFIX)]
        if bad:
            raise StageError(f"stage produced reserved columns {bad}; the SDK owns '{RESERVED_PREFIX}*'")
        lh, plan_txt = plan_hash.logic_hash(df)
        observed_reads = plan_hash.relations_in_plan(plan_txt)
        proto = plan_hash.plan_proto_bytes(df)

        tier, downgraded = telemetry.effective_tier(contract.telemetry.tier if contract else 0, s.telemetry_tier, False)
        t0 = time.time()
        df_obs, obs = telemetry.attach(df, contract.telemetry, tier) if contract else (df, None)

        # 6. write
        from pyspark.sql import functions as F
        out = contract.outputs[0].object if contract else None
        written = set()
        commit = None
        if out:
            target = out + _purpose_suffix(opts.run_purpose)
            _safe_set(spark, _USER_META_CONF, json.dumps({"run_id": run_id, "contract_id": contract_id,
                                                          "contract_version": contract.contract_version, "logic_hash": lh}))
            df_out = df_obs.withColumn(s.run_id_column, F.lit(run_id))
            writer = df_out.write.format("delta").mode(opts.write_mode).option("mergeSchema", "true")
            writer.saveAsTable(target)
            commit = store.last_commit(target)
            written.add(out)
            num_rows = _metric(commit, "numOutputRows")
            store.insert_output(run_id, target, commit["version"] if commit else None, num_rows)
            _sync_view(spark, s, target)
        else:
            df_obs.count()

        # 7. evidence
        obs_res = telemetry.collect(obs, tier, t0)
        store.insert_evidence(run_id, "observe", {"tier": tier, "metrics": obs_res.metrics,
                                                  "overhead_ms": obs_res.overhead_ms, "downgraded_from": downgraded})
        if commit:
            store.insert_evidence(run_id, "delta_ops", commit)
        store.insert_evidence(run_id, "context", ctx.as_dict())
        control_results = ctlmod.evaluate(df_obs, contract, spark) if contract and contract.controls else []
        store.insert_evidence(run_id, "controls", {"results": control_results})
        if proto and len(proto) <= s.plan_inline_max_bytes:
            store.update_run(run_id, plan_proto_path=None)
        undeclared = observed_reads - {i.object for i in (contract.inputs if contract else [])}
        store.insert_inputs(run_id, [{"object_name": o, "declared": False} for o in sorted(undeclared)
                                     if not o.startswith("system.") and ".functional_audit." not in o])

        # 8. provisional reconciliation
        rec = reconcile.provisional(
            contract, logic_hash_executed=lh, logic_hash_declared=None,
            declared_inputs={i.object for i in contract.inputs}, observed_reads=observed_reads,
            declared_outputs={o.object for o in contract.outputs}, written_outputs=written,
            control_results=control_results, conf_snapshot=ctx.conf_snapshot,
            telemetry_downgraded_from=downgraded) if contract else reconcile.ReconcileResult("ERROR", {}, [{"code": "MISSING_CONTRACT"}])
        store.upsert_reconciliation(run_id, "PROVISIONAL", rec.status, rec.checks, rec.deviations)
        store.update_run(run_id, status="SUCCEEDED", ended_at=datetime.now(timezone.utc),
                         logic_hash_executed=lh, output_commit_version=commit["version"] if commit else None,
                         attestation_stage="PROVISIONAL")
        if rec.status == "DEVIATION" and s.enforce == "strict":
            raise StageError(f"run {run_id} has deviations: {[d['code'] for d in rec.deviations]}")
        df_obs.__fa_run_id__ = run_id
        return df_obs
    except Exception as e:
        store.update_run(run_id, status="FAILED", ended_at=datetime.now(timezone.utc))
        store.insert_evidence(run_id, "error", {"type": type(e).__name__, "message": str(e),
                                                "trace": traceback.format_exc()[-4000:]})
        raise
    finally:
        _safe_set(spark, _QUERY_TAG_CONF, prev_tag)
        _safe_set(spark, _USER_META_CONF, prev_meta)


def _metric(commit, key):
    try:
        return int(commit["operationMetrics"].get(key)) if commit else None
    except Exception:
        return None


def _safe_get(spark, k):
    try:
        return spark.conf.get(k, None)
    except Exception:
        return None


def _safe_set(spark, k, v):
    try:
        if v is None:
            spark.conf.unset(k)
        else:
            spark.conf.set(k, v)
    except Exception:
        pass


def _sync_view(spark, s: Settings, table: str) -> None:
    """Create the explain view for a governed table once (guarded by a table property)."""
    from functional_audit.runtime.explain import view_sql, view_name
    try:
        props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        if props.get("functional_audit.view_version") == "1":
            return
        spark.sql(view_sql(s, table))
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('functional_audit.view_version'='1', "
                  f"'functional_audit.view'='{view_name(s, table)}')")
    except Exception:
        pass
