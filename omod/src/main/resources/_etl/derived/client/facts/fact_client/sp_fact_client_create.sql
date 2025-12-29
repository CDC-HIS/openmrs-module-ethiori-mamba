-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_client
(
    client_id                      int not null,
    patient_uuid                   CHAR(38),
    mrn                            VARCHAR(50),

    -- Demographics
    patient_name                   VARCHAR(255),
    sex                            VARCHAR(10),
    birthdate                      DATE,
    age                            INT,

    -- Identifiers
    uan                            VARCHAR(50),
    phrh_code                      VARCHAR(50),
    ncd_code                       VARCHAR(50),
    icd_number                     VARCHAR(50),

    -- Registration
    registration_date              DATE,
    hiv_confirmed_date             DATE,
    transfer_in_date               DATE,
    months_on_art                  INT,

    -- Address
    region                         VARCHAR(255),
    zone                           VARCHAR(255),
    woreda                         VARCHAR(255),
    kebele                         VARCHAR(255),
    house_number                   VARCHAR(255),
    mobile_phone                   VARCHAR(50),
    address_completeness           VARCHAR(20),

    -- Clinical Follow-up
    art_start_date                 DATE,
    current_status                 VARCHAR(50),
    current_regimen                VARCHAR(255),
    regimen_dose                   VARCHAR(255),
    regimen_line                   VARCHAR(50),
    tx_curr_end_date               DATE,
    nutritional_status             VARCHAR(255),
    pregnancy_status               VARCHAR(50),
    pmtct_status                   VARCHAR(50),
    family_planning_method         VARCHAR(255),

    last_visit_date                DATE,
    next_appointment_date          DATE,
    days_overdue                   INT,

    -- Viral Load
    last_vl_date                   DATE,
    last_vl_result                 NUMERIC(10, 2),
    is_suppressed                  BOOLEAN,
    vl_status                      VARCHAR(100),
    vl_eligibility_date            DATE,

    -- TPT
    tpt_status                     VARCHAR(50),

    -- TB Treatment
    active_tb_diagnosis_date       DATE,
    tb_treatment_start_date        DATE,
    tb_treatment_discontinued_date DATE,
    tb_treatment_completed_date    DATE,

    -- Other Statuses
    dsd_category                   VARCHAR(100),
    ict_screening_status           VARCHAR(100),
    ncd_screening_status           VARCHAR(100),
    cxca_screening_status          VARCHAR(100),
    target_population              VARCHAR(100),

    -- Metadata
    last_updated                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Indexes for Performance
    INDEX idx_uuid (patient_uuid),
    INDEX idx_mrn (mrn),
    INDEX idx_status (current_status)
);

-- $END