-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_care_and_treatment
(
    id                                      INT AUTO_INCREMENT,
    client_id                               INT           NULL,
    return_visit_date                       DATE          NULL,
    hiv_confirmed_date                      DATE          NULL,
    art_start_date                          DATE          NULL,
    age_at_enrollment                       INT           NULL,
    days_difference                         INT           NULL,
    enrollment_date                         DATE           NULL,
    pregnancy_status                        NVARCHAR(50)   NULL,
    regimen_at_enrollment                   NVARCHAR(255) NULL,
    arv_dose_at_enrollment                  NVARCHAR(255) NULL,
    last_follow_up_status                   NVARCHAR(255) NULL,
    next_visit_date                         DATE,
    PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_care_and_treatment_art_start_date_index ON mamba_fact_care_and_treatment (art_start_date);
-- $END
