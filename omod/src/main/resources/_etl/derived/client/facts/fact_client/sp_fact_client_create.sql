-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_client_staging
(
    client_id                      INT NOT NULL,
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
    ict_number                     VARCHAR(50),  -- Added

-- Registration & Follow-up Dates
    registration_date              DATE,
    art_start_date                 DATE,         -- Reordered
    months_on_art                  INT,          -- Reordered
    next_appointment_date          DATE,         -- Reordered
    hiv_confirmed_date             DATE,
    transfer_in_date               DATE,

    -- Address
    region                         VARCHAR(255),
    zone                           VARCHAR(255),
    woreda                         VARCHAR(255),
    kebele                         VARCHAR(255),
    house_number                   VARCHAR(255),
    mobile_phone                   VARCHAR(50),
    address_completeness           VARCHAR(20),

    current_status                 VARCHAR(50),
    current_regimen                VARCHAR(255),
    regimen_dose                   VARCHAR(255),
    regimen_line                   VARCHAR(50),
    tx_curr_end_date               DATE,
    nutritional_status             VARCHAR(255),
    pregnancy_status               VARCHAR(50),
    pmtct_status                   VARCHAR(50),
    family_planning_method         VARCHAR(255),
    who_stage                      VARCHAR(50),  -- Added

    last_visit_date                DATE,
    days_overdue                   INT,

    last_vl_date                   DATE,
    last_vl_result                 NUMERIC(10, 2),
    is_suppressed                  BOOLEAN,
    vl_status                      VARCHAR(100),
    vl_eligibility_date            DATE,

    tpt_status                     VARCHAR(50),
    tpt_start_date                 DATE,
    tpt_completed_date             DATE,
    tpt_discontinued_date          DATE,

    active_tb_diagnosis_date       DATE,
    tb_treatment_start_date        DATE,
    tb_treatment_discontinued_date DATE,
    tb_treatment_completed_date    DATE,

    dsd_category                   VARCHAR(100),
    ict_screening_status           VARCHAR(100),
    ncd_screening_status           VARCHAR(100),
    next_ncd_screening_date        DATE,
    cxca_screening_status          VARCHAR(100),
    next_cca_screening_date        DATE,
    systolic_blood_pressure        INT,
    diastolic_blood_pressure       INT,
    target_population              VARCHAR(100),

    -- DQI&U Line List additions
    breast_feeding_status            VARCHAR(10),
    disclosure_stage                 VARCHAR(50),

    pmtct_booking_date               DATE,
    pmtct_booking_date_ec            VARCHAR(100),
    pmtct_discharge_date             DATE,
    pmtct_discharge_date_ec          VARCHAR(100),

    vl_sent_date                     DATE,
    vl_sent_date_ec                  VARCHAR(100),
    vl_received_date                 DATE,
    vl_received_date_ec              VARCHAR(100),

    cd4_result                       NUMERIC(10, 2),
    visitect_cd4_result              VARCHAR(255),

    transfer_in_date_ec              VARCHAR(100),
    active_tb_diagnosis_date_ec      VARCHAR(100),
    tb_treatment_start_date_ec       VARCHAR(100),
    tb_treatment_discontinued_date_ec VARCHAR(100),
    tb_treatment_completed_date_ec   VARCHAR(100),
    tpt_start_date_ec                VARCHAR(100),
    tpt_discontinued_date_ec         VARCHAR(100),
    tpt_completed_date_ec            VARCHAR(100),

    tb_treatment_rx_status           VARCHAR(100),
    pmtct_enrollment_status          VARCHAR(100),
    advanced_hiv_disease             VARCHAR(10),

    ncd_last_screening_date          DATE,
    ncd_screening_eligibility_status VARCHAR(100),
    ncd_screening_eligibility_date   DATE,
    ncd_screening_reason             VARCHAR(255),

    -- DQI corrected status columns (existing columns preserved unchanged)
    vl_status_dqi                    VARCHAR(100),
    tpt_status_dqi                   VARCHAR(100),
    ict_screening_status_dqi         VARCHAR(100),
    ncd_screening_status_dqi         VARCHAR(100),
    cxca_screening_status_dqi        VARCHAR(100),

    last_updated                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_uuid (patient_uuid),
    INDEX idx_mrn (mrn),
    INDEX idx_status (current_status)
    );

-- $END