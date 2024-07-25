-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_art_follow_up
(
    id                          INT AUTO_INCREMENT,
    client_id                   INT NULL,
    weight_in_kg                DOUBLE,
    cd4_count                   DOUBLE,
    current_who_hiv_stage       NVARCHAR(255),
    nutritional_status          NVARCHAR(255),
    tb_screening_result         NVARCHAR(255),
    enrollment_date             DATE,
    hiv_confirmed_date          DATE,
    art_start_date              DATE,
    days_difference             INT,
    followup_date               DATE,
    regimen                     NVARCHAR(255),
    arv_dose_days               NVARCHAR(255),
    pregnancy_status            NVARCHAR(255),
    breast_feeding_status       NVARCHAR(255),
    follow_up_status            NVARCHAR(255),
    ti                          NVARCHAR(255),
    treatment_end_date          DATE,
    next_visit_date             DATE,
    latest_followup_date        DATE,
    latest_followup_status      NVARCHAR(255),
    latest_regimen              NVARCHAR(255),
    latest_arv_dose_days        NVARCHAR(255),
    hiv_viral_load_count              INT,
    hiv_viral_load_status       NVARCHAR(255),
    viral_load_test_status      NVARCHAR(255),
    on_antiretroviral_therapy   NVARCHAR(255),
    viral_load_test_indication      NVARCHAR(255),
    antiretroviral_side_effects    NVARCHAR(255),
    anitiretroviral_adherence_level NVARCHAR(255),
    date_of_reported_hiv_viral_load NVARCHAR(255),
    date_viral_load_results_received NVARCHAR(255),
    routine_viral_load_test_indication NVARCHAR(255),
    targeted_viral_load_test_indication NVARCHAR(255),
    PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_care_and_treatment_art_start_date_index ON mamba_fact_care_and_treatment (art_start_date);
CREATE INDEX mamba_fact_care_and_treatment_client_id_index ON mamba_fact_care_and_treatment (client_id);
-- $END