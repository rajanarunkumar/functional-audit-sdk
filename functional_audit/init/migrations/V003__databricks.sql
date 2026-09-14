-- functional_audit V003: Databricks-only objects.
-- requires: databricks
-- Unity Catalog informational primary/foreign keys (the ER's keys, NOT ENFORCED, visible to the optimizer
-- and to information_schema) and the volume that holds serialized execution plans.

CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.plans
  COMMENT 'functional_audit: serialized execution plans (runs.plan_proto_path)';

ALTER TABLE ${CATALOG}.${SCHEMA}.contracts
  ADD CONSTRAINT pk_contracts PRIMARY KEY (contract_id, contract_version) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.runs
  ADD CONSTRAINT pk_runs PRIMARY KEY (run_id) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.reconciliations
  ADD CONSTRAINT pk_reconciliations PRIMARY KEY (run_id) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.attestations
  ADD CONSTRAINT pk_attestations PRIMARY KEY (attestation_id) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.contract_deps
  ADD CONSTRAINT fk_contract_deps_contract FOREIGN KEY (contract_id, contract_version)
  REFERENCES ${CATALOG}.${SCHEMA}.contracts (contract_id, contract_version) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.runs
  ADD CONSTRAINT fk_runs_contract FOREIGN KEY (contract_id, contract_version)
  REFERENCES ${CATALOG}.${SCHEMA}.contracts (contract_id, contract_version) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.run_inputs
  ADD CONSTRAINT fk_run_inputs_run FOREIGN KEY (run_id) REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.run_outputs
  ADD CONSTRAINT fk_run_outputs_run FOREIGN KEY (run_id) REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.evidence
  ADD CONSTRAINT fk_evidence_run FOREIGN KEY (run_id) REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.lineage_observed
  ADD CONSTRAINT fk_lineage_observed_run FOREIGN KEY (run_id) REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.query_observed
  ADD CONSTRAINT fk_query_observed_run FOREIGN KEY (run_id) REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.reconciliations
  ADD CONSTRAINT fk_reconciliations_run FOREIGN KEY (run_id) REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id) NOT ENFORCED;

ALTER TABLE ${CATALOG}.${SCHEMA}.attestations
  ADD CONSTRAINT fk_attestations_run FOREIGN KEY (run_id) REFERENCES ${CATALOG}.${SCHEMA}.runs (run_id) NOT ENFORCED;
