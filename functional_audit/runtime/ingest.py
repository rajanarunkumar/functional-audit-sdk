"""T+1 technical-audit ingestion. One job per metastore. Watermarked, partition-pruned,
joins the window's runs against system.access.* and system.query.history, then promotes
reconciliations to FINAL when every declared output has an exact (ENTITY|TAG) match."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone

from functional_audit.config import Settings

LOOKBACK_DAYS = 2


def _watermark(spark, s: Settings) -> datetime:
    try:
        r = spark.sql(f"SELECT max(created) FROM {s.table('lineage_observed')}").collect()[0][0]
        if r:
            return r - timedelta(days=LOOKBACK_DAYS)
    except Exception:
        pass
    return datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS + 5)


def ingest(spark, s: Settings, since: datetime | None = None, run_id: str | None = None) -> dict:
    since = since or _watermark(spark, s)
    since_s = since.strftime("%Y-%m-%d %H:%M:%S")
    run_filter = f"AND r.run_id = '{run_id}'" if run_id else ""
    runs = f"(SELECT * FROM {s.table('runs')} r WHERE r.started_at >= TIMESTAMP '{since_s}' {run_filter})"

    # table + column lineage, matched by entity ids first, then time window
    spark.sql(f"""
    INSERT INTO {s.table('lineage_observed')}
    SELECT r.run_id, 'TABLE',
           CASE WHEN l.target_table_full_name = o.object_name THEN 'WRITE' ELSE 'READ' END,
           l.source_table_full_name, NULL, l.target_table_full_name, NULL,
           l.entity_run_id, l.event_time,
           CASE WHEN l.entity_run_id = r.entity_run_id THEN 'ENTITY' ELSE 'WINDOW' END,
           current_timestamp()
    FROM {runs} r
    JOIN {s.table('run_outputs')} o ON o.run_id = r.run_id
    JOIN system.access.table_lineage l
      ON l.event_date >= to_date(TIMESTAMP '{since_s}')
     AND l.target_table_full_name = o.object_name
     AND ( l.entity_run_id = r.entity_run_id
           OR (r.entity_run_id IS NULL AND l.event_time BETWEEN r.started_at AND coalesce(r.ended_at, r.started_at + INTERVAL 6 HOURS)) )
    WHERE NOT EXISTS (SELECT 1 FROM {s.table('lineage_observed')} x
                      WHERE x.run_id = r.run_id AND x.level = 'TABLE'
                        AND x.source_object <=> l.source_table_full_name)
    """)
    spark.sql(f"""
    INSERT INTO {s.table('lineage_observed')}
    SELECT r.run_id, 'COLUMN', 'READ',
           l.source_table_full_name, l.source_column_name, l.target_table_full_name, l.target_column_name,
           l.entity_run_id, l.event_time,
           CASE WHEN l.entity_run_id = r.entity_run_id THEN 'ENTITY' ELSE 'WINDOW' END,
           current_timestamp()
    FROM {runs} r
    JOIN {s.table('run_outputs')} o ON o.run_id = r.run_id
    JOIN system.access.column_lineage l
      ON l.event_date >= to_date(TIMESTAMP '{since_s}')
     AND l.target_table_full_name = o.object_name
     AND ( l.entity_run_id = r.entity_run_id
           OR (r.entity_run_id IS NULL AND l.event_time BETWEEN r.started_at AND coalesce(r.ended_at, r.started_at + INTERVAL 6 HOURS)) )
    WHERE NOT EXISTS (SELECT 1 FROM {s.table('lineage_observed')} x
                      WHERE x.run_id = r.run_id AND x.level = 'COLUMN'
                        AND x.source_object <=> l.source_table_full_name AND x.source_column <=> l.source_column_name
                        AND x.target_column <=> l.target_column_name)
    """)
    # query history by tag
    spark.sql(f"""
    INSERT INTO {s.table('query_observed')}
    SELECT r.run_id, q.statement_id, sha2(q.statement_text, 256), q.start_time, q.end_time,
           q.produced_rows, coalesce(q.compute.warehouse_id, q.compute.cluster_id), 'TAG', current_timestamp()
    FROM {runs} r
    JOIN system.query.history q
      ON q.start_time >= TIMESTAMP '{since_s}'
     AND q.query_tags IS NOT NULL
     AND q.query_tags['fa_run_id'] = r.run_id
    WHERE NOT EXISTS (SELECT 1 FROM {s.table('query_observed')} x
                      WHERE x.run_id = r.run_id AND x.statement_id = q.statement_id)
    """)
    # promote to FINAL
    spark.sql(f"""
    MERGE INTO {s.table('reconciliations')} x
    USING (
      SELECT r.run_id,
             CASE WHEN count(o.object_name) = count(CASE WHEN m.match_method IN ('ENTITY','TAG') THEN 1 END)
                  THEN 'FINAL' ELSE 'PROVISIONAL' END AS stage
      FROM {runs} r
      JOIN {s.table('run_outputs')} o ON o.run_id = r.run_id
      LEFT JOIN (SELECT run_id, target_object, max(match_method) AS match_method
                   FROM {s.table('lineage_observed')} WHERE level='TABLE' AND direction='WRITE'
                  GROUP BY run_id, target_object) m
        ON m.run_id = r.run_id AND m.target_object = o.object_name
      WHERE r.status = 'SUCCEEDED'
      GROUP BY r.run_id
    ) p ON p.run_id = x.run_id
    WHEN MATCHED AND p.stage = 'FINAL' AND x.stage <> 'FINAL' THEN
      UPDATE SET stage = 'FINAL', last_altered = current_timestamp()
    """)
    spark.sql(f"UPDATE {s.table('runs')} r SET attestation_stage = 'FINAL' "
              f"WHERE EXISTS (SELECT 1 FROM {s.table('reconciliations')} x WHERE x.run_id = r.run_id AND x.stage = 'FINAL') "
              f"AND r.attestation_stage <> 'FINAL'")
    counts = {}
    for t in ("lineage_observed", "query_observed"):
        counts[t] = spark.sql(f"SELECT count(*) FROM {s.table(t)} WHERE created >= TIMESTAMP '{since_s}'").collect()[0][0]
    return {"since": since_s, **counts}
