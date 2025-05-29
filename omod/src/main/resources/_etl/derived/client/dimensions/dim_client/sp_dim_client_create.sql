-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_dim_client
(
    client_id         int                                not null
        primary key,
    patient_name      VARCHAR(255) NULL,
    prefix            VARCHAR(255) NULL,
    given_name        VARCHAR(255) NULL,
    middle_name       VARCHAR(255) NULL,
    family_name       VARCHAR(255) NULL,
    mrn               VARCHAR(50)  NULL,
    uan               VARCHAR(50)  NULL,
    patient_uuid      VARCHAR(38),
    current_age       INT,
    mobile_no         VARCHAR(50),
    phone_no         VARCHAR(50),
    date_of_birth     DATE                               NULL,
    sex               VARCHAR(50)  NULL,
    state_province    VARCHAR(255) NULL,
    county_district   VARCHAR(255) NULL,
    city_village      VARCHAR(255) NULL,
    key_population    VARCHAR(50)   NULL,
    marital_status    VARCHAR(50)   NULL,
    education_level   VARCHAR(50)   NULL,
    house_number   VARCHAR(50)   NULL,
    kebele   VARCHAR(50)   NULL,
    coarse_age_group  VARCHAR(255) NULL,
    fine_age_group    VARCHAR(255) NULL,
    constraint client_id
        unique (client_id)
);
CREATE INDEX mamba_dim_client_client_id_index ON mamba_dim_client (client_id);
CREATE INDEX mamba_dim_client_mrn_index ON mamba_dim_client (mrn);
CREATE INDEX mamba_dim_client_uan_index ON mamba_dim_client (uan);
-- CREATE INDEX mamba_dim_client_care_and_treatment_enrollment_date_index ON mamba_dim_client_care_and_treatment (enrollment_date);
-- $END