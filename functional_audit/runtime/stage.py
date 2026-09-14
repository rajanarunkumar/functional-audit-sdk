"""@fa.stage — the only authoring surface. Sequence:

 1. resolve contract, bind parameters       6. reconcile (pre-write); strict refuses to write
 2. capture context, write runs row         7. write output with __run_id + commit metadata
 3. set query tags                          8. controls on the written rows; reconcile (post-write)
 4. pin inputs, check upstream state        9. quarantine this run's rows if controls require it
 5. build df, identify plan                10. flush evidence; seal the contract plan on first clean run
"""
from __future__ import annotations
import functools
import inspect
import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Callable, Optional

from functional_audit.config import RESERVED_PREFIX, Settings, check_fqn
from functional_audit.contracts.model import Contract, PinMode, RunKind, RunPurpose
from functional_audit.ids import uuid7, text_hash
from functional_audit.runtime import context as ctxmod, plan_hash, telemetry, controls as ctlmod, reconcile
from functional_audit.runtime.store import Store, RunEvidence

log = logging.getLogger("functional_audit.stage")

_QUERY_TAG_CONF = "spark.databricks.queryTags"
_USER_META_CONF = "spark.databricks.delta.commitInfo.userMetadata"
QUARANTINE_SUFFIX = "__quarantine"


class StageError(RuntimeError):
    pass


class Inputs(dict):
    """object_name -> DataFrame. Declared inputs are pinned by the SDK before the stage runs;
    any other name resolves unpinned and is recorded as an undeclared read."""

    def __init__(self, spark):
        super().__init__()
        self._spark = spark
        self.versions: dict[str, Optional[int]] = {}
        self.undeclared: set[str] = set()

    def __missing__(self, name: str):
        self.undeclared.add(name)
        df = self._spark.table(name)
        self[name] = df
        return df


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
        return inputs._spark.sql(query)
    _fn.__name__ = f"sql_{contract_id}"
    _fn.__fa_source__ = query
    return _execute(contract_id, _fn, (), {}, run or RunOptions(), spark, local_contract)


# --- helpers ---------------------------------------------------------------------

def _bind_parameters(contract: Contract, opts: RunOptions) -> dict:
    allowed = {p.name for p in contract.parameters}
    bad = set(opts.scenario_params) - allowed
    if bad:
        raise StageError(f"scenario overrides not declared in contract parameters: {sorted(bad)}")
    return {**contract.parameter_defaults(), **opts.scenario_params}


def _accepted_kwargs(fn: Callable, params: dict, given: dict) -> dict:
    """Contract parameters the stage function can receive, never overriding explicit kwargs."""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return {}
    takes_var = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    return {k: v for k, v in params.items() if k not in given and (takes_var or k in sig.parameters)}


def _source_hash(fn: Callable) -> tuple[str, str]:
    src = getattr(fn, "__fa_source__", None)
    if src is not None:
        return text_hash(src), "source"
    try:
        return text_hash(inspect.getsource(fn)), "source"
    except (OSError, TypeError):
        return text_hash(fn.__name__), "name"


def _resolve_input(spark, store: Store, i, opts: RunOptions, contract: Contract):
    """Returns (df, version, gap) where gap names the evidence that could not be captured."""
    ver = opts.input_versions.get(i.object)
    gap = None
    if ver is None:
        if i.pin == PinMode.explicit:
            gap = f"input version for {i.object} (pin: explicit, none supplied)"
        elif i.pin == PinMode.contract_effective:
            ts = datetime.combine(contract.effective_from, datetime.min.time(), tzinfo=timezone.utc)
            ver = store.version_as_of(i.object, ts)
            gap = None if ver is not None else f"input version for {i.object} as of {contract.effective_from}"
        else:
            ver = store.latest_version(i.object)
            gap = None if ver is not None else f"input version for {i.object}"
    if ver is None:
        return spark.table(i.object), None, gap
    return spark.read.format("delta").option("versionAsOf", ver).table(i.object), ver, gap


def _quarantine(spark, store: Store, s: Settings, target: str, run_id: str) -> str:
    """Move this run's rows out of the governed table into <target>__quarantine."""
    from pyspark.sql import functions as F
    q = target + QUARANTINE_SUFFIX
    spark.table(target).where(F.col(s.run_id_column) == run_id).write.format("delta").mode("append").saveAsTable(q)
    store.delete_run_rows(target, run_id)
    return q


def _metric(commit, key):
    try:
        return int(commit["operationMetrics"].get(key)) if commit else None
    except (TypeError, ValueError):
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
    except Exception as e:
        log.debug("could not set %s: %s", k, type(e).__name__)


class _Persist:
    """Evidence writes: fatal under strict, logged under warn so a governance outage
    never takes a production job down with it."""

    def __init__(self, strict: bool):
        self.strict, self.failed = strict, []

    def __call__(self, what: str, fn, *a, **kw):
        try:
            return fn(*a, **kw)
        except Exception as e:
            if self.strict:
                raise StageError(f"could not persist {what}: {e}") from e
            log.exception("functional_audit: could not persist %s", what)
            self.failed.append(what)
            return None


# --- the stage ------------------------------------------------------------------

def _execute(contract_id, fn, args, kwargs, opts: RunOptions, spark, local_contract):
    spark = spark or _get_spark()
    s = Settings.from_spark(spark)
    if not s.enabled:
        log.warning("functional_audit disabled; %s runs ungoverned", contract_id)
        return fn(Inputs(spark), *args, **kwargs)
    strict = s.enforce == "strict"
    store = Store(spark, s)
    persist = _Persist(strict)
    run_id = uuid7()

    # 1. contract + parameters
    contract = local_contract or store.resolve_contract(contract_id, opts.reporting_period, opts.as_of_contract_version)
    if contract is None:
        msg = f"no effective contract for '{contract_id}' (period={opts.reporting_period}, version={opts.as_of_contract_version})"
        if strict:
            raise StageError(msg)
        log.warning("functional_audit: %s; running ungoverned", msg)
        return fn(Inputs(spark), *args, **kwargs)
    params = _bind_parameters(contract, opts)
    fn_kwargs = {**kwargs, **_accepted_kwargs(fn, params, kwargs)}

    # 2. context + runs row (before any data is read)
    ctx = ctxmod.capture(spark, contract.semantics.confs)
    started = datetime.now(timezone.utc)
    query_tag = f"fa_run_id:{run_id},fa_contract:{contract_id},fa_ver:{contract.contract_version}"
    code_hash, code_hash_source = _source_hash(fn)
    persist("runs row", store.insert_run, {
        "run_id": run_id, "contract_id": contract_id, "contract_version": contract.contract_version,
        "run_kind": opts.run_kind.value, "run_purpose": opts.run_purpose.value,
        "scenario_id": opts.scenario_id, "scenario_params": params or None,
        "backfill_id": opts.backfill_id, "supersedes_run_id": opts.supersedes_run_id,
        "reporting_period": opts.reporting_period, "code_hash": code_hash, "code_hash_source": code_hash_source,
        **{k: v for k, v in ctx.as_dict().items() if k != "conf_snapshot"},
        "conf_snapshot": ctx.conf_snapshot, "query_tag": query_tag,
        "started_at": started, "status": "RUNNING", "attestation_stage": None,
    })
    ev = RunEvidence(run_id)
    incomplete: list[str] = []

    # 3. query tags
    prev_tag, prev_meta = _safe_get(spark, _QUERY_TAG_CONF), _safe_get(spark, _USER_META_CONF)
    _safe_set(spark, _QUERY_TAG_CONF, query_tag)
    status = "FAILED"
    try:
        # 4. pin inputs, check upstream
        inputs = Inputs(spark)
        upstream: dict[str, dict] = {}
        for i in contract.inputs:
            df_in, ver, gap = _resolve_input(spark, store, i, opts, contract)
            inputs[i.object], inputs.versions[i.object] = df_in, ver
            if gap:
                incomplete.append(gap)
            ev.inputs.append({"object_name": i.object, "delta_version": ver, "pin_mode": i.pin.value, "declared": True})
            prod = store.latest_producer(i.object)
            if prod and (prod["status"] in reconcile.BLOCKING_RUN_STATUSES or prod.get("reconciliation_status") == "DEVIATION"):
                upstream[i.object] = prod

        # 5. build + identify
        df = fn(inputs, *args, **fn_kwargs)
        if df is None:
            raise StageError("stage function returned None; it must return a DataFrame")
        bad = [c for c in df.columns if c.startswith(RESERVED_PREFIX)]
        if bad:
            raise StageError(f"stage produced reserved columns {bad}; the SDK owns '{RESERVED_PREFIX}*'")
        try:
            ident = plan_hash.identify(df)
        except plan_hash.PlanUnavailable as e:
            if strict:
                raise StageError(str(e)) from e
            ident = None
        proto = plan_hash.plan_proto_bytes(df)
        observed = (ident.relations if ident else set()) | {plan_hash.canonical_name(o) for o in inputs.undeclared}
        for o in sorted(inputs.undeclared):
            ev.inputs.append({"object_name": o, "delta_version": None, "pin_mode": None, "declared": False})
        binding = plan_hash.binding_hash(ident.literals if ident else [], inputs.versions, opts.reporting_period, params)
        declared_inputs = {plan_hash.canonical_name(i.object) for i in contract.inputs}
        linkage = "ENTITY" if ctx.entity_run_id else "WINDOW_ONLY"

        # 6. pre-write reconciliation: a strict run refuses to write on any deviation it can already see
        rec = reconcile.provisional(contract, s, structure_hash=ident.structure_hash if ident else None,
                                    binding_hash=binding, declared_hash=contract.logic_hash_declared,
                                    declared_inputs=declared_inputs, observed_reads=observed,
                                    conf_snapshot=ctx.conf_snapshot, evidence_incomplete=incomplete,
                                    upstream=upstream, linkage=linkage)
        if strict and rec.status == "DEVIATION":
            raise StageError(f"refusing to write: {rec.codes}", )

        # 7. write
        from pyspark.sql import functions as F
        tier = telemetry.effective_tier(contract.telemetry.tier, s.telemetry_tier)
        exprs = telemetry.metric_exprs(contract.telemetry, tier) + ctlmod.observe_exprs(contract)
        df_obs, obs = telemetry.attach(df, exprs)
        out = contract.outputs[0].object
        target = check_fqn(out + _purpose_suffix(opts.run_purpose))
        _safe_set(spark, _USER_META_CONF, json.dumps({"run_id": run_id, "contract_id": contract_id,
                                                      "contract_version": contract.contract_version,
                                                      "logic_hash": ident.structure_hash if ident else None}))
        df_obs.withColumn(s.run_id_column, F.lit(run_id)).write.format("delta").mode(opts.write_mode).saveAsTable(target)
        commit = store.commit_for_run(target, run_id)
        if commit is None:
            incomplete.append(f"commit version for {target}")
        ev.outputs.append({"object_name": target, "commit_version": commit["version"] if commit else None,
                           "num_rows": _metric(commit, "numOutputRows")})

        # 8. controls on what was written, then the full reconciliation
        written_df = spark.table(target).where(F.col(s.run_id_column) == run_id)
        metrics, metrics_source = telemetry.collect(obs), "observe"
        if obs is not None and not metrics:
            metrics, metrics_source = telemetry.scan(written_df, telemetry.metric_exprs(contract.telemetry, tier)), "scan"
        control_results = ctlmod.evaluate(contract, metrics, written_df, spark)
        rec = reconcile.provisional(contract, s, structure_hash=ident.structure_hash if ident else None,
                                    binding_hash=binding, declared_hash=contract.logic_hash_declared,
                                    declared_inputs=declared_inputs, observed_reads=observed,
                                    conf_snapshot=ctx.conf_snapshot, evidence_incomplete=incomplete,
                                    upstream=upstream, linkage=linkage, control_results=control_results,
                                    declared_outputs={out}, written_outputs={out})

        # 9. quarantine
        status = "SUCCEEDED"
        if reconcile.should_quarantine(control_results, strict):
            q = _quarantine(spark, store, s, target, run_id)
            ev.outputs.append({"object_name": q, "commit_version": None, "num_rows": _metric(commit, "numOutputRows")})
            status = "QUARANTINED"
            rec.checks["quarantine"] = q

        # 10. evidence
        ev.add("observe", {"tier": tier, "metrics": metrics, "source": metrics_source})
        if commit:
            ev.add("delta_ops", commit)
        ev.add("context", ctx.as_dict())
        ev.add("controls", {"results": control_results})
        if ident:
            ev.add("plan", {"analyzed": ident.analyzed_text, "optimized": ident.optimized_text})
        persist("evidence", store.flush, ev)
        persist("reconciliation", store.upsert_reconciliation, run_id, "PROVISIONAL", rec.status, rec.checks, rec.deviations)
        persist("run status", store.update_run, run_id, status=status, ended_at=datetime.now(timezone.utc),
                logic_hash_executed=ident.structure_hash if ident else None, binding_hash=binding,
                plan_text=ident.canonical_text if ident else None, literals=ident.literals if ident else None,
                plan_proto=proto if proto and len(proto) <= s.plan_inline_max_bytes else None,
                output_commit_version=commit["version"] if commit else None, attestation_stage="PROVISIONAL")
        if (ident and status == "SUCCEEDED" and rec.status == "ATTESTED" and contract.logic_hash_declared is None
                and local_contract is None and opts.run_purpose == RunPurpose.PRODUCTION):
            if persist("seal", store.seal_logic_hash, contract_id, contract.contract_version, ident.structure_hash):
                log.info("functional_audit: sealed %s v%s to plan %s", contract_id, contract.contract_version,
                         ident.structure_hash[:12])
        if strict and rec.status == "DEVIATION":
            raise StageError(f"run {run_id} {status}: {rec.codes}")
        try:
            df_obs.__fa_run_id__ = run_id
        except Exception:
            pass
        return df_obs
    except Exception as e:
        if status != "QUARANTINED":
            status = "FAILED"
        ev_err = RunEvidence(run_id)
        ev_err.add("error", {"type": type(e).__name__, "message": str(e), "trace": traceback.format_exc()[-4000:]})
        persist("error evidence", store.flush, ev_err)
        persist("run status", store.update_run, run_id, status=status, ended_at=datetime.now(timezone.utc))
        raise
    finally:
        _safe_set(spark, _QUERY_TAG_CONF, prev_tag)
        _safe_set(spark, _USER_META_CONF, prev_meta)
