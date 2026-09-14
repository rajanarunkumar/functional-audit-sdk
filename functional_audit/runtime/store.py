"""Writes to the functional_audit system schema. All writes are append-only and
idempotent on run_id. VARIANT payloads are passed as JSON strings and parsed in SQL."""
from __future__ import annotations
import json
from datetime import datetime, timezone

from functional_audit.config import Settings
from functional_audit.contracts.model import Contract


def _q(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, datetime):
        return f"TIMESTAMP '{v.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _variant(obj) -> str:
    return f"parse_json({_q(json.dumps(obj, default=str, sort_keys=True))})"


class Store:
    def __init__(self, spark, s: Settings):
        self.spark, self.s = spark, s

    # --- contracts ------------------------------------------------------------
    def resolve_contract(self, contract_id: str, period: str | None) -> Contract | None:
        cond = f"contract_id = {_q(contract_id)}"
        if period:
            cond += (f" AND (effective_from IS NULL OR effective_from <= to_date({_q(period)}))"
                     f" AND (effective_to IS NULL OR effective_to >= to_date({_q(period)}))")
        rows = self.spark.sql(
            f"SELECT body_yaml FROM {self.s.table('contracts')} WHERE {cond} "
            f"ORDER BY contract_version DESC LIMIT 1").collect()
        if not rows:
            return None
        import yaml
        doc = yaml.safe_load(rows[0][0])
        if "stages" in doc:                     # bundle: pick our stage
            for st in doc["stages"]:
                if st["contract_id"] == contract_id:
                    st.setdefault("contract_version", doc["bundle_version"])
                    st["calculation_id"], st["bundle_version"] = doc["calculation_id"], doc["bundle_version"]
                    return Contract.model_validate(st)
            return None
        return Contract.model_validate(doc)

    def publish_contract(self, c: Contract, body_yaml: str, source_commit: str | None) -> None:
        t = self.s.table("contracts")
        self.spark.sql(f"""
            INSERT INTO {t} REPLACE ON (contract_id, contract_version)
            SELECT {_q(c.contract_id)}, {c.contract_version}, {_q(c.contract_hash)},
                   {_q(c.calculation_id)}, {_q(c.bundle_version)}, {_q(c.requirement_id)},
                   {_q(c.rule_citation)}, NULL,
                   {_q(c.effective_from)}, {_q(c.effective_to)},
                   {_variant(c.body_json())}, {_q(body_yaml)}, {_q(source_commit)},
                   coalesce((SELECT min(created) FROM {t} WHERE contract_id={_q(c.contract_id)}
                              AND contract_version={c.contract_version}), current_timestamp()),
                   coalesce((SELECT min(created_by) FROM {t} WHERE contract_id={_q(c.contract_id)}
                              AND contract_version={c.contract_version}), current_user()),
                   current_timestamp(), current_user()
        """)
        d = self.s.table("contract_deps")
        self.spark.sql(f"DELETE FROM {d} WHERE contract_id={_q(c.contract_id)} AND contract_version={c.contract_version}")
        rows = [f"({_q(c.contract_id)},{c.contract_version},'INPUT',{_q(i.object)},{_q(i.pin.value)},current_timestamp())" for i in c.inputs]
        rows += [f"({_q(c.contract_id)},{c.contract_version},'OUTPUT',{_q(o.object)},NULL,current_timestamp())" for o in c.outputs]
        self.spark.sql(f"INSERT INTO {d} VALUES {', '.join(rows)}")

    # --- runs -----------------------------------------------------------------
    def insert_run(self, run: dict) -> None:
        cols = ", ".join(run.keys())
        vals = ", ".join(_variant(v) if k in ("conf_snapshot", "scenario_params") and v is not None else _q(v)
                         for k, v in run.items())
        self.spark.sql(f"INSERT INTO {self.s.table('runs')} ({cols}, created) VALUES ({vals}, current_timestamp())")

    def update_run(self, run_id: str, **fields) -> None:
        sets = ", ".join(f"{k} = {_q(v)}" for k, v in fields.items())
        self.spark.sql(f"UPDATE {self.s.table('runs')} SET {sets} WHERE run_id = {_q(run_id)}")

    def insert_inputs(self, run_id: str, rows: list[dict]) -> None:
        if not rows:
            return
        vals = ", ".join(f"({_q(run_id)},{_q(r['object_name'])},{_q(r.get('delta_version'))},"
                         f"{_q(r.get('num_records'))},{_q(r.get('pin_mode'))},{_q(r.get('declared', True))},current_timestamp())"
                         for r in rows)
        self.spark.sql(f"INSERT INTO {self.s.table('run_inputs')} VALUES {vals}")

    def insert_output(self, run_id: str, object_name: str, commit_version, num_rows) -> None:
        self.spark.sql(f"INSERT INTO {self.s.table('run_outputs')} VALUES "
                       f"({_q(run_id)},{_q(object_name)},{_q(commit_version)},{_q(num_rows)},current_timestamp())")

    def insert_evidence(self, run_id: str, evidence_type: str, payload: dict) -> None:
        self.spark.sql(f"INSERT INTO {self.s.table('evidence')} VALUES "
                       f"({_q(run_id)},{_q(evidence_type)},{_variant(payload)},current_timestamp())")

    def upsert_reconciliation(self, run_id: str, stage: str, status: str, checks: dict, deviations: list) -> None:
        t = self.s.table("reconciliations")
        self.spark.sql(f"""
            INSERT INTO {t} REPLACE ON (run_id)
            SELECT {_q(run_id)}, {_q(stage)}, {_q(status)}, {_variant(checks)}, {_variant(deviations)},
                   coalesce((SELECT min(created) FROM {t} WHERE run_id={_q(run_id)}), current_timestamp()),
                   current_timestamp()
        """)

    def insert_attestation(self, attestation_id: str, run_id, contract_id, period, decision, reason) -> None:
        self.spark.sql(f"INSERT INTO {self.s.table('attestations')} VALUES "
                       f"({_q(attestation_id)},{_q(run_id)},{_q(contract_id)},{_q(period)},{_q(decision)},{_q(reason)},"
                       f"current_timestamp(),current_user(),current_timestamp(),current_user())")

    # --- delta helpers ----------------------------------------------------------
    def latest_version(self, table: str) -> int | None:
        try:
            r = self.spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
            return int(r[0]["version"]) if r else None
        except Exception:
            return None

    def snapshot_num_records(self, table: str, version: int | None) -> int | None:
        try:
            r = self.spark.sql(f"DESCRIBE DETAIL {table}").collect()
            return int(r[0]["numFiles"]) if r and version is None else None
        except Exception:
            return None

    def last_commit(self, table: str) -> dict | None:
        try:
            r = self.spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
            if not r:
                return None
            row = r[0].asDict()
            return {"version": int(row["version"]), "operation": row.get("operation"),
                    "operationMetrics": dict(row.get("operationMetrics") or {}),
                    "userMetadata": row.get("userMetadata")}
        except Exception:
            return None
