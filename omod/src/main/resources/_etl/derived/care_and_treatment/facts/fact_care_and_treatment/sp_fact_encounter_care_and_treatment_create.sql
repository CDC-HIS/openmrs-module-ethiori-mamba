-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_care_and_treatment
(
    id                     INT AUTO_INCREMENT,
    client_id              INT NULL,
    weight_in_kg           DOUBLE,
    cd4_count              DOUBLE,
    current_who_hiv_stage  NVARCHAR(255),
    nutritional_status     NVARCHAR(255),
    tb_screening_result    NVARCHAR(255),
    enrollment_date        DATE,
    hiv_confirmed_date     DATE,
    art_start_date         DATE,
    days_difference        INT,
    followup_date          DATE,
    regimen                NVARCHAR(255),
    arv_dose_days          NVARCHAR(255),
    pregnancy_status       NVARCHAR(255),
    breast_feeding_status  NVARCHAR(255),
    follow_up_status       NVARCHAR(255),
    ti                     NVARCHAR(255),
    treatment_end_date     DATE,
    next_visit_date        DATE,
    latest_followup_date   DATE,
    latest_followup_status NVARCHAR(255),
    latest_regimen         NVARCHAR(255),
    latest_arv_dose_days   NVARCHAR(255),
    PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_care_and_treatment_art_start_date_index ON mamba_fact_care_and_treatment (art_start_date);
CREATE INDEX mamba_fact_care_and_treatment_client_id_index ON mamba_fact_care_and_treatment (client_id);
-- $END

