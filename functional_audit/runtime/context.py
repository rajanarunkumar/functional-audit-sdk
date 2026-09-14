"""Execution-context capture. Everything the Databricks system tables use to key an
event is read here, before any business data is touched."""
from __future__ import annotations
import getpass
import json
import os
from dataclasses import dataclass, asdict, field
from typing import Optional

_CONF_KEYS = {
    "job_id": "spark.databricks.job.id",
    "job_run_id": "spark.databricks.job.runId",
    "parent_run_id": "spark.databricks.job.parentRunId",
    "task_run_id": "spark.databricks.job.taskRunId",
    "task_key": "spark.databricks.job.taskKey",
    "pipeline_id": "pipelines.id",
    "pipeline_update_id": "pipelines.updateId",
    "cluster_id": "spark.databricks.clusterUsageTags.clusterId",
    "warehouse_id": "spark.databricks.sql.warehouseId",
    "workspace_id": "spark.databricks.clusterUsageTags.orgId",
    "notebook_id": "spark.databricks.notebook.id",
    "runtime_version": "spark.databricks.clusterUsageTags.sparkVersion",
}

_SEMANTIC_CONFS = [
    "spark.sql.ansi.enabled",
    "spark.sql.session.timeZone",
    "spark.sql.legacy.timeParserPolicy",
    "spark.sql.parquet.datetimeRebaseModeInRead",
    "spark.sql.decimalOperations.allowPrecisionLoss",
    "spark.sql.execution.pythonUDF.arrow.enabled",
    "spark.databricks.delta.commitInfo.userMetadata",
]


@dataclass
class ExecutionContext:
    workspace_id: Optional[str] = None
    metastore_id: Optional[str] = None
    entity_type: str = "ADHOC"
    entity_id: Optional[str] = None
    entity_run_id: Optional[str] = None
    task_run_id: Optional[str] = None
    task_key: Optional[str] = None
    compute_id: Optional[str] = None
    compute_type: Optional[str] = None
    run_as_principal: Optional[str] = None
    executed_by_principal: Optional[str] = None
    runtime_version: Optional[str] = None
    spark_version: Optional[str] = None
    conf_snapshot: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return asdict(self)


def _get(spark, key: str) -> Optional[str]:
    try:
        v = spark.conf.get(key, None)
        return v if v not in ("", None) else None
    except Exception:
        return None


def capture(spark, semantic_confs: Optional[dict[str, str]] = None) -> ExecutionContext:
    g = {k: _get(spark, v) for k, v in _CONF_KEYS.items()}
    ctx = ExecutionContext()
    ctx.workspace_id = g["workspace_id"]
    ctx.runtime_version = g["runtime_version"]
    try:
        ctx.spark_version = spark.version
    except Exception:
        ctx.spark_version = None

    if g["pipeline_id"]:
        ctx.entity_type, ctx.entity_id, ctx.entity_run_id = "PIPELINE", g["pipeline_id"], g["pipeline_update_id"]
        ctx.compute_type = "PIPELINE"
    elif g["job_id"]:
        ctx.entity_type, ctx.entity_id = "JOB", g["job_id"]
        ctx.entity_run_id = g["parent_run_id"] or g["job_run_id"]
        ctx.task_run_id, ctx.task_key = g["task_run_id"] or g["job_run_id"], g["task_key"]
    elif g["notebook_id"]:
        ctx.entity_type, ctx.entity_id = "NOTEBOOK", g["notebook_id"]

    if g["warehouse_id"]:
        ctx.compute_id, ctx.compute_type = g["warehouse_id"], "WAREHOUSE"
    elif g["cluster_id"]:
        ctx.compute_id = g["cluster_id"]
        ctx.compute_type = ctx.compute_type or ("SERVERLESS" if "serverless" in (g["runtime_version"] or "").lower() else "CLASSIC")

    try:
        ctx.run_as_principal = spark.sql("SELECT current_user()").collect()[0][0]
    except Exception:
        ctx.run_as_principal = os.getenv("USER") or getpass.getuser()
    ctx.executed_by_principal = os.getenv("DATABRICKS_USER") or ctx.run_as_principal

    try:
        ctx.metastore_id = spark.sql("SELECT current_metastore()").collect()[0][0]
    except Exception:
        ctx.metastore_id = None

    snap = {k: _get(spark, k) for k in _SEMANTIC_CONFS}
    for k in (semantic_confs or {}):
        snap[k] = _get(spark, k)
    ctx.conf_snapshot = {k: v for k, v in snap.items() if v is not None}
    return ctx


def context_json(ctx: ExecutionContext) -> str:
    return json.dumps(ctx.as_dict(), sort_keys=True, default=str)
