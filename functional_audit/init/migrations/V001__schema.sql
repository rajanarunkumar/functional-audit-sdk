-- functional_audit V001: system schema. Templated: ${CATALOG} ${SCHEMA}
CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}
  COMMENT 'functional_audit: transformation contracts, run evidence, lineage, reconciliation, attestation';

CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}_views
  COMMENT 'functional_audit: generated explain views';

CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.plans
  COMMENT 'functional_audit: serialized execution plans';

-- human-touched -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.contracts (
  contract_id          STRING    NOT NULL,
  contract_version     INT       NOT NULL,
  contract_hash        STRING    NOT NULL,
  calculation_id       STRING,
  bundle_version       INT,
  requirement_id       STRING    NOT NULL,
  rule_citation        STRING,
  logic_hash_declared  STRING,
  effective_from       DATE,
  effective_to         DATE,
  body                 VARIANT   NOT NULL,
  body_yaml            STRING    NOT NULL,
  source_commit        STRING,
  created              TIMESTAMP NOT NULL,
  created_by           STRING    NOT NULL,
  last_altered         TIMESTAMP NOT NULL,
  last_altered_by      STRING    NOT NULL,
  CONSTRAINT pk_contracts PRIMARY KEY (contract_id, contract_version) NOT ENFORCED
)
USING DELTA
CLUSTER BY (contract_id, contract_version)
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.enableRowTracking = true,
               'functional_audit.write_class' = 'human');

CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.attestations (
  attestation_id   STRING    NOT NULL,
  run_id           STRING,
  contract_id      STRING,
  period           STRING,
  decision         STRING    NOT NULL,   -- APPROVED | WAIVED | REVOKED
  waiver_reason    STRING,
  created          TIMESTAMP NOT NULL,
  created_by       STRING    NOT NULL,
  last_altered     TIMESTAMP NOT NULL,
  last_altered_by  STRING    NOT NULL,
  CONSTRAINT pk_attestations PRIMARY KEY (attestation_id) NOT ENFORCED
)
USING DELTA
CLUSTER BY (contract_id, run_id)
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'functional_audit.write_class' = 'human');

-- system-only, append-only ---------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.contract_deps (
  contract_id       STRING NOT NULL,
  contract_version  INT    NOT NULL,
  direction         STRING NOT NULL,      -- INPUT | OUTPUT
  object_name       STRING NOT NULL,
  pin_mode          STRING,
  created           TIMESTAMP NOT NULL
)
USING DELTA CLUSTER BY (contract_id, contract_version)
TBLPROPERTIES ('functional_audit.write_class' = 'system');

CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.runs (
  run_id                 STRING    NOT NULL,
  contract_id            STRING    NOT NULL,
  contract_version       INT       NOT NULL,
  run_kind               STRING    NOT NULL,
  run_purpose            STRING    NOT NULL,
  scenario_id            STRING,
  scenario_params        VARIANT,
  backfill_id            STRING,
  supersedes_run_id      STRING,
  reporting_period       STRING,
  logic_hash_executed    STRING,
  code_hash              STRING,
  plan_proto_path        STRING,
  plan_proto             BINARY,
  workspace_id           STRING,
  metastore_id           STRING,
  entity_type            STRING,
  entity_id              STRING,
  entity_run_id          STRING,
  task_run_id            STRING,
  task_key               STRING,
  compute_id             STRING,
  compute_type           STRING,
  run_as_principal       STRING,
  executed_by_principal  STRING,
  query_tag              STRING,
  runtime_version        STRING,
  spark_version          STRING,
  conf_snapshot          VARIANT,
  started_at             TIMESTAMP,
  ended_at               TIMESTAMP,
  output_commit_version  BIGINT,
  status                 STRING    NOT NULL,
  attestation_stage      STRING,
  created                TIMESTAMP NOT NULL,
  CONSTRAINT pk_runs PRIMARY KEY (run_id) NOT ENFORCED
)
USING DELTA CLUSTER BY (contract_id, run_id)
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'functional_audit.write_class' = 'system');

CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.run_inputs (
  run_id        STRING NOT NULL,
  object_name   STRING NOT NULL,
  delta_version BIGINT,
  num_records   BIGINT,
  pin_mode      STRING,
  declared      BOOLEAN NOT NULL,
  created       TIMESTAMP NOT NULL
)
USING DELTA CLUSTER BY (run_id)
TBLPROPERTIES ('functional_audit.write_class' = 'system');

CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.run_outputs (
  run_id          STRING NOT NULL,
  object_name     STRING NOT NULL,
  commit_version  BIGINT,
  num_rows        BIGINT,
  created         TIMESTAMP NOT NULL
)
USING DELTA CLUSTER BY (run_id)
TBLPROPERTIES ('functional_audit.write_class' = 'system');

CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.evidence (
  run_id         STRING  NOT NULL,
  evidence_type  STRING  NOT NULL,   -- observe | delta_ops | controls | overhead | cost | context
  payload        VARIANT NOT NULL,
  created        TIMESTAMP NOT NULL
)
USING DELTA CLUSTER BY (run_id, evidence_type)
TBLPROPERTIES ('functional_audit.write_class' = 'system');

CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.lineage_observed (
  run_id         STRING NOT NULL,
  level          STRING NOT NULL,     -- TABLE | COLUMN
  direction      STRING NOT NULL,     -- READ | WRITE
  source_object  STRING,
  source_column  STRING,
  target_object  STRING,
  target_column  STRING,
  entity_run_id  STRING,
  event_time     TIMESTAMP,
  match_method   STRING NOT NULL,     -- ENTITY | TAG | WINDOW
  created        TIMESTAMP NOT NULL
)
USING DELTA CLUSTER BY (run_id, level)
TBLPROPERTIES ('functional_audit.write_class' = 'system');

CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.query_observed (
  run_id               STRING NOT NULL,
  statement_id         STRING NOT NULL,
  statement_text_hash  STRING,
  event_time           TIMESTAMP,
  ended_at             TIMESTAMP,
  rows_produced        BIGINT,
  compute_id           STRING,
  match_method         STRING NOT NULL,
  created              TIMESTAMP NOT NULL
)
USING DELTA CLUSTER BY (run_id)
TBLPROPERTIES ('functional_audit.write_class' = 'system');

CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.reconciliations (
  run_id        STRING  NOT NULL,
  stage         STRING  NOT NULL,     -- PROVISIONAL | FINAL
  status        STRING  NOT NULL,     -- ATTESTED | DEVIATION | ERROR
  checks        VARIANT NOT NULL,
  deviations    VARIANT,
  created       TIMESTAMP NOT NULL,
  last_altered  TIMESTAMP NOT NULL,
  CONSTRAINT pk_reconciliations PRIMARY KEY (run_id) NOT ENFORCED
)
USING DELTA CLUSTER BY (run_id)
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'functional_audit.write_class' = 'system');

CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}._migrations (
  version     STRING NOT NULL,
  applied_at  TIMESTAMP NOT NULL,
  applied_by  STRING NOT NULL,
  sdk_version STRING NOT NULL
) USING DELTA;
