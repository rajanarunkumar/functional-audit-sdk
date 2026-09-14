"""T+1 technical-audit ingestion. One job per metastore. Watermarked and bounded on both
sides: joins the window's runs against system.access.* and system.query.history, grades
each match (ENTITY > TAG > WINDOW), and promotes reconciliations to FINAL when every
declared output has an ENTITY or TAG match. Also reaps abandoned runs and reports runs
stuck in PROVISIONAL.

The only values spliced into these statements are a timestamp this module formats itself
and a run id validated as a UUID."""
from __future__ import annotations
import re
from datetime import datetime, timedelta, timezone

from functional_audit.config import Settings
from functional_audit.runtime.store import Store

LOOKBACK_DAYS = 2
STALE_PROVISIONAL_DAYS = 3

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_RANK = "CASE match_method WHEN 'ENTITY' THEN 1 WHEN 'TAG' THEN 2 ELSE 3 END"


def _ts(dt: datetime) -> str:
    return "TIMESTAMP '" + dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + "'"


def _watermark(spark, s: Settings) -> datetime:
    try:
        r = spark.sql(f"SELECT max(created) FROM {s.table('lineage_observed')}").collect()[0][0]
        if r:
            return r - timedelta(days=LOOKBACK_DAYS)
    except Exception:
        pass
    return datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS + 5)


def ingest(spark, s: Settings, since: datetime | None = None, run_id: str | None = None) -> dict:
    if run_id is not None and not _UUID.match(run_id):
        raise ValueError(f"run_id must be a UUID: {run_id!r}")
    since = since or _watermark(spark, s)
    since_sql = _ts(since)
    run_filter = f"AND r.run_id = '{run_id}'" if run_id else ""
    runs = f"(SELECT * FROM {s.table('runs')} r WHERE r.started_at >= {since_sql} {run_filter})"
    lineage, queries = s.table("lineage_observed"), s.table("query_observed")

    spark.sql(f"""
    INSERT INTO {lineage}
    SELECT r.run_id, 'TABLE',
           CASE WHEN l.target_table_full_name = o.object_name THEN 'WRITE' ELSE 'READ' END,
           l.source_table_full_name, NULL, l.target_table_full_name, NULL,
           l.entity_run_id, l.event_time,
           CASE WHEN l.entity_run_id = r.entity_run_id THEN 'ENTITY' ELSE 'WINDOW' END,
           current_timestamp()
    FROM {runs} r
    JOIN {s.table('run_outputs')} o ON o.run_id = r.run_id
    JOIN system.access.table_lineage l
      ON l.event_date >= to_date({since_sql})
     AND l.target_table_full_name = o.object_name
     AND ( l.entity_run_id = r.entity_run_id
           OR (r.entity_run_id IS NULL AND l.event_time BETWEEN r.started_at AND coalesce(r.ended_at, r.started_at + INTERVAL 6 HOURS)) )
    WHERE NOT EXISTS (SELECT 1 FROM {lineage} x
                      WHERE x.created >= {since_sql} AND x.run_id = r.run_id AND x.level = 'TABLE'
                        AND x.source_object <=> l.source_table_full_name)
    """)
    spark.sql(f"""
    INSERT INTO {lineage}
    SELECT r.run_id, 'COLUMN', 'READ',
           l.source_table_full_name, l.source_column_name, l.target_table_full_name, l.target_column_name,
           l.entity_run_id, l.event_time,
           CASE WHEN l.entity_run_id = r.entity_run_id THEN 'ENTITY' ELSE 'WINDOW' END,
           current_timestamp()
    FROM {runs} r
    JOIN {s.table('run_outputs')} o ON o.run_id = r.run_id
    JOIN system.access.column_lineage l
      ON l.event_date >= to_date({since_sql})
     AND l.target_table_full_name = o.object_name
     AND ( l.entity_run_id = r.entity_run_id
           OR (r.entity_run_id IS NULL AND l.event_time BETWEEN r.started_at AND coalesce(r.ended_at, r.started_at + INTERVAL 6 HOURS)) )
    WHERE NOT EXISTS (SELECT 1 FROM {lineage} x
                      WHERE x.created >= {since_sql} AND x.run_id = r.run_id AND x.level = 'COLUMN'
                        AND x.source_object <=> l.source_table_full_name AND x.source_column <=> l.source_column_name
                        AND x.target_column <=> l.target_column_name)
    """)
    # query history by tag
    spark.sql(f"""
    INSERT INTO {queries}
    SELECT r.run_id, q.statement_id, sha2(q.statement_text, 256), q.start_time, q.end_time,
           q.produced_rows, coalesce(q.compute.warehouse_id, q.compute.cluster_id), 'TAG', current_timestamp()
    FROM {runs} r
    JOIN system.query.history q
      ON q.start_time >= {since_sql}
     AND q.query_tags IS NOT NULL
     AND q.query_tags['fa_run_id'] = r.run_id
    WHERE NOT EXISTS (SELECT 1 FROM {queries} x
                      WHERE x.created >= {since_sql} AND x.run_id = r.run_id AND x.statement_id = q.statement_id)
    """)
    # a window-matched lineage row for a run whose statements were tag-matched is upgraded to TAG
    spark.sql(f"""
    UPDATE {lineage} SET match_method = 'TAG'
    WHERE created >= {since_sql} AND match_method = 'WINDOW'
      AND run_id IN (SELECT run_id FROM {queries} WHERE created >= {since_sql})
    """)
    # promote to FINAL when every declared output has an exact match (best match per output)
    spark.sql(f"""
    MERGE INTO {s.table('reconciliations')} x
    USING (
      SELECT r.run_id,
             CASE WHEN count(o.object_name) = count(CASE WHEN m.best_rank <= 2 THEN 1 END)
                  THEN 'FINAL' ELSE 'PROVISIONAL' END AS stage
      FROM {runs} r
      JOIN {s.table('run_outputs')} o ON o.run_id = r.run_id
      LEFT JOIN (SELECT run_id, target_object, min({_RANK}) AS best_rank
                   FROM {lineage} WHERE level = 'TABLE' AND direction = 'WRITE'
                  GROUP BY run_id, target_object) m
        ON m.run_id = r.run_id AND m.target_object = o.object_name
      WHERE r.status IN ('SUCCEEDED', 'QUARANTINED')
      GROUP BY r.run_id
    ) p ON p.run_id = x.run_id
    WHEN MATCHED AND p.stage = 'FINAL' AND x.stage <> 'FINAL' THEN
      UPDATE SET stage = 'FINAL', last_altered = current_timestamp()
    """)
    spark.sql(f"UPDATE {s.table('runs')} r SET attestation_stage = 'FINAL' "
              f"WHERE r.started_at >= {since_sql} AND r.attestation_stage <> 'FINAL' "
              f"AND EXISTS (SELECT 1 FROM {s.table('reconciliations')} x WHERE x.run_id = r.run_id AND x.stage = 'FINAL')")
    # reap runs whose process died, and report runs the platform never confirmed
    Store(spark, s).mark_abandoned(s.abandon_after_hours)
    stale_cutoff = _ts(datetime.now(timezone.utc) - timedelta(days=STALE_PROVISIONAL_DAYS))
    stale = spark.sql(f"SELECT count(*) FROM {s.table('runs')} WHERE status IN ('SUCCEEDED', 'QUARANTINED') "
                      f"AND attestation_stage = 'PROVISIONAL' AND started_at < {stale_cutoff}").collect()[0][0]
    counts = {}
    for t in ("lineage_observed", "query_observed"):
        counts[t] = spark.sql(f"SELECT count(*) FROM {s.table(t)} WHERE created >= {since_sql}").collect()[0][0]
    return {"since": since.strftime("%Y-%m-%d %H:%M:%S"), **counts, "stale_provisional": stale}
