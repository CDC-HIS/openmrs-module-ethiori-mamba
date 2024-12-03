-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_dim_client
(
    id                INT AUTO_INCREMENT,
    client_id         INT           NOT NULL,
    patient_name      NVARCHAR(255) NULL,
    mrn               NVARCHAR(50)  NULL,
    uan               NVARCHAR(50)  NULL,
    current_age       INT,
    mobile_no         NVARCHAR(50),
    date_of_birth     DATE          NULL,
    sex               NVARCHAR(50)  NULL,
    state_province    NVARCHAR(255) NULL,
    county_district   NVARCHAR(255) NULL,
    city_village      NVARCHAR(255) NULL,
    coarse_age_group  NVARCHAR(255) NULL,
    fine_age_group    NVARCHAR(255) NULL,
    PRIMARY KEY (id)
);
CREATE INDEX mamba_dim_client_client_id_index ON mamba_dim_client (client_id);
CREATE INDEX mamba_dim_client_mrn_index ON mamba_dim_client (mrn);
CREATE INDEX mamba_dim_client_uan_index ON mamba_dim_client (uan);
-- CREATE INDEX mamba_dim_client_care_and_treatment_enrollment_date_index ON mamba_dim_client_care_and_treatment (enrollment_date);
-- $END