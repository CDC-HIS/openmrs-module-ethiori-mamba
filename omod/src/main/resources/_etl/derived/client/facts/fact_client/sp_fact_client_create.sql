-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_client (
                                                          patient_uuid CHAR(38),
    mrn VARCHAR(50),

    -- Demographics
    patient_name VARCHAR(255),
    sex VARCHAR(10),
    birthdate DATE,
    age INT,

    art_start_date DATE,
    current_status VARCHAR(50),
    current_regimen VARCHAR(255),
    regimen_line VARCHAR(50),

    last_visit_date DATE,
    next_appointment_date DATE,
    days_overdue INT,

    last_vl_date DATE,
    last_vl_result NUMERIC(10,2),
    is_suppressed BOOLEAN,

-- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Indexes for Performance
    INDEX idx_uuid (patient_uuid),
    INDEX idx_mrn (mrn),
    INDEX idx_status (current_status)
    );

-- $END