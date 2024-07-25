-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_client_tranfer_out
(
  id INT AUTO_INCREMENT,
  client_id INT NOT NULL,
  latest_followup_date   DATE,
  art_start_date DATE,
  adherence NVARCHAR(255),
  regimen NVARCHAR(255),
  follow_up_status VARCHAR(255) NULL,
  next_visit_date        DATE,
  treatment_end_date DATE,
  PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_client_tranfer_out_latest_followup_date_index ON mamba_fact_client_tranfer_out (latest_followup_date);
CREATE INDEX mamba_fact_client_tranfer_out_client_id_index ON mamba_fact_client_tranfer_out (client_id);
-- $END