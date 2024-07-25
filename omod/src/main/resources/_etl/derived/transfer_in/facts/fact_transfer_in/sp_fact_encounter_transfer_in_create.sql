-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_client_tranfer_in
(
  id INT AUTO_INCREMENT,
  client_id INT NOT NULL,
  ti_status VARCHAR(255) NULL,
  ti_date DATE NULL,
  art_start_date DATE,
  treatment_end_date DATE,
  next_visit_date        DATE,
  latest_followup_date   DATE,
  latest_followup_status NVARCHAR(255),
  latest_regimen NVARCHAR(255),
  adherence NVARCHAR(255),
  PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_client_tranfer_in_latest_followup_date_index ON mamba_fact_client_tranfer_in (latest_followup_date);
CREATE INDEX mamba_fact_client_tranfer_in_client_id_index ON mamba_fact_client_tranfer_in (client_id);
-- $END