-- functional_audit V002: explain() SQL table function and ledger view
CREATE OR REPLACE FUNCTION ${CATALOG}.${SCHEMA}.explain(p_run_id STRING)
RETURNS TABLE (
  run_id STRING, contract_id STRING, contract_version INT, calculation_id STRING,
  requirement_id STRING, rule_citation STRING, business_logic STRING,
  logic_hash STRING, logic_hash_declared STRING, binding_hash STRING,
  run_kind STRING, run_purpose STRING, scenario_id STRING,
  supersedes_run_id STRING, reporting_period STRING,
  entity_type STRING, entity_id STRING, entity_run_id STRING, compute_id STRING,
  run_as_principal STRING, started_at TIMESTAMP, ended_at TIMESTAMP, run_status STRING,
  output_commit_version BIGINT,
  inputs_pinned ARRAY<STRUCT<object_name STRING, delta_version BIGINT>>,
  controls VARIANT,
  tables_read ARRAY<STRING>,
  columns_read ARRAY<STRUCT<target_column STRING, source_object STRING, source_column STRING>>,
  reconciliation_stage STRING, reconciliation_status STRING,
  attestation_decision STRING
)
COMMENT 'functional_audit: audit context for one run'
RETURN
  SELECT r.run_id, c.contract_id, c.contract_version, c.calculation_id,
         c.requirement_id, c.rule_citation,
         variant_get(c.body, '$.business_logic', 'string') AS business_logic,
         r.logic_hash_executed, c.logic_hash_declared, r.binding_hash,
         r.run_kind, r.run_purpose, r.scenario_id,
         r.supersedes_run_id, r.reporting_period,
         r.entity_type, r.entity_id, r.entity_run_id, r.compute_id,
         r.run_as_principal, r.started_at, r.ended_at, r.status, r.output_commit_version,
         (SELECT collect_list(struct(object_name, delta_version))
            FROM ${CATALOG}.${SCHEMA}.run_inputs i WHERE i.run_id = r.run_id AND i.declared) AS inputs_pinned,
         (SELECT first(payload) FROM ${CATALOG}.${SCHEMA}.evidence e
           WHERE e.run_id = r.run_id AND e.evidence_type = 'controls') AS controls,
         (SELECT collect_set(source_object) FROM ${CATALOG}.${SCHEMA}.lineage_observed l
           WHERE l.run_id = r.run_id AND l.level = 'TABLE' AND l.direction = 'READ') AS tables_read,
         (SELECT collect_list(struct(target_column, source_object, source_column))
            FROM ${CATALOG}.${SCHEMA}.lineage_observed l
           WHERE l.run_id = r.run_id AND l.level = 'COLUMN') AS columns_read,
         x.stage, x.status,
         (SELECT decision FROM ${CATALOG}.${SCHEMA}.attestations a
           WHERE a.run_id = r.run_id ORDER BY a.created DESC LIMIT 1) AS attestation_decision
  FROM ${CATALOG}.${SCHEMA}.runs r
  JOIN ${CATALOG}.${SCHEMA}.contracts c
    ON c.contract_id = r.contract_id AND c.contract_version = r.contract_version
  LEFT JOIN ${CATALOG}.${SCHEMA}.reconciliations x ON x.run_id = r.run_id
  WHERE r.run_id = p_run_id;

CREATE OR REPLACE VIEW ${CATALOG}.${SCHEMA}.ledger AS
SELECT r.run_id, r.contract_id, r.contract_version, c.calculation_id, c.requirement_id,
       c.rule_citation, r.run_kind, r.run_purpose, r.reporting_period,
       r.logic_hash_executed, c.logic_hash_declared, r.binding_hash,
       r.entity_type, r.entity_id, r.entity_run_id,
       r.run_as_principal, r.started_at, r.ended_at, r.status,
       x.stage AS reconciliation_stage, x.status AS reconciliation_status,
       (SELECT count(*) FROM ${CATALOG}.${SCHEMA}.run_inputs i WHERE i.run_id = r.run_id AND i.declared) AS inputs_declared,
       (SELECT count(*) FROM ${CATALOG}.${SCHEMA}.lineage_observed l
         WHERE l.run_id = r.run_id AND l.level = 'TABLE' AND l.direction = 'READ') AS inputs_observed
FROM ${CATALOG}.${SCHEMA}.runs r
JOIN ${CATALOG}.${SCHEMA}.contracts c
  ON c.contract_id = r.contract_id AND c.contract_version = r.contract_version
LEFT JOIN ${CATALOG}.${SCHEMA}.reconciliations x ON x.run_id = r.run_id;
