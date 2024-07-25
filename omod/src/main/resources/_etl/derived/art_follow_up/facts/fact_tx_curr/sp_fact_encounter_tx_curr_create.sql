-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_tx_curr
(
    id                     INT AUTO_INCREMENT,
    client_id              INT NULL,
    hiv_confirmed_date     DATE,
    art_start_date         DATE,
    followup_date          DATE,
    weight_in_kg           DOUBLE,
    pregnancy_status       NVARCHAR(255),
    regimen                NVARCHAR(255),
    arv_dose_days          NVARCHAR(255),
    follow_up_status       NVARCHAR(255),
    anitiretroviral_adherence_level NVARCHAR(255),
    next_visit_date        DATE,
    dsd_category           NVARCHAR(255),
    tpt_start_date          DATE,
    tpt_completed_date      DATE,
    tpt_discontinued_date   DATE,
    tuberculosis_treatment_end_date DATE,
    viral_load_received_date DATE,
    viral_load_status    NVARCHAR(255),
    viral_load_eligibility_date DATE,
    nutritional_status   NVARCHAR(50),
    PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_mamba_fact_mamba_fact_tx_curr_art_start_date_index ON mamba_fact_tx_curr (art_start_date);
CREATE INDEX mamba_fact_mamba_fact_mamba_fact_tx_curr_client_id_index ON mamba_fact_tx_curr (client_id);
-- $END

