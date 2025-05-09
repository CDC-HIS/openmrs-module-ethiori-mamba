-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_client
(
    id                          INT AUTO_INCREMENT,
    encounter_datetime         DATE,
    encounter_id                INT,
    client_id                   INT NULL,
    weight_in_kg                DOUBLE,
    cd4_count                   DOUBLE,
    current_who_hiv_stage       VARCHAR(255) CHARACTER SET UTF8MB4,
    nutritional_status          VARCHAR(255) CHARACTER SET UTF8MB4,
    tb_screening_result         VARCHAR(255) CHARACTER SET UTF8MB4,
    enrollment_date             DATE,
    hiv_confirmed_date          DATE,
    art_start_date              DATE,
    days_difference             INT,
    followup_date               DATE,
    regimen                     VARCHAR(255) CHARACTER SET UTF8MB4,
    arv_dose_days               VARCHAR(255) CHARACTER SET UTF8MB4,
    pregnancy_status            VARCHAR(255) CHARACTER SET UTF8MB4,
    breast_feeding_status       VARCHAR(255) CHARACTER SET UTF8MB4,
    follow_up_status            VARCHAR(255) CHARACTER SET UTF8MB4,
    ti                          VARCHAR(255) CHARACTER SET UTF8MB4,
    treatment_end_date          DATE,
    next_visit_date             DATE,
    hiv_viral_load_count              INT,
    hiv_viral_load_status       VARCHAR(255) CHARACTER SET UTF8MB4,
    viral_load_test_status      VARCHAR(255) CHARACTER SET UTF8MB4,
    on_antiretroviral_therapy   VARCHAR(255) CHARACTER SET UTF8MB4,
    viral_load_test_indication      VARCHAR(255) CHARACTER SET UTF8MB4,
    antiretroviral_side_effects    VARCHAR(255) CHARACTER SET UTF8MB4,
    anitiretroviral_adherence_level VARCHAR(255) CHARACTER SET UTF8MB4,
    date_of_reported_hiv_viral_load VARCHAR(255) CHARACTER SET UTF8MB4,
    date_viral_load_results_received VARCHAR(255) CHARACTER SET UTF8MB4,
    routine_viral_load_test_indication VARCHAR(255) CHARACTER SET UTF8MB4,
    targeted_viral_load_test_indication VARCHAR(255) CHARACTER SET UTF8MB4,
    dsd_category                        VARCHAR(255) CHARACTER SET UTF8MB4,
    tpt_start_date                      DATE,
    tpt_completed_date                  DATE,
    tpt_discontinued_date               DATE,
    tuberculosis_treatment_end_date     DATE,
    tb_prophylaxis_type                 VARCHAR(255) CHARACTER SET UTF8MB4,
    cotrimoxazole_prophylaxis_start_dat VARCHAR(255) CHARACTER SET UTF8MB4,
    cotrimoxazole_prophylaxis_stop_date VARCHAR(255) CHARACTER SET UTF8MB4,
    patient_diagnosed_with_active_tuber VARCHAR(255) CHARACTER SET UTF8MB4,
    diagnosis_date                      DATE,
    tuberculosis_drug_treatment_start_d VARCHAR(255) CHARACTER SET UTF8MB4,
    date_active_tbrx_completed          DATE,
    fluconazole_start_date              DATE,
    PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_client_art_start_date_index ON mamba_fact_client (art_start_date);
CREATE INDEX mamba_fact_client_client_id_index ON mamba_fact_client (client_id);
CREATE INDEX mamba_fact_client_followup_date_index ON mamba_fact_client (followup_date);
CREATE INDEX mamba_fact_client_regimen_index ON mamba_fact_client (regimen);
-- $END