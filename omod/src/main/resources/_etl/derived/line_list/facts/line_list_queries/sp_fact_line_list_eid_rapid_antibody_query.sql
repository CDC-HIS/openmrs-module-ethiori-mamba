DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_eid_rapid_antibody_query;

CREATE PROCEDURE sp_fact_line_list_eid_rapid_antibody_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN

    -- EID Rapid Antibody Line List Query

    WITH Enrollment AS (
        SELECT
            e.client_id,
            e.hei_code
        FROM mamba_flat_encounter_hei_enrollment e
        WHERE e.encounter_datetime <= REPORT_END_DATE
    ),

    LatestFollowUp AS (
        SELECT
            f.client_id,
            f.encounter_datetime as follow_up_date,
            ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY f.encounter_datetime DESC) as rn
        FROM mamba_flat_encounter_hei_followup f
        WHERE f.encounter_datetime <= REPORT_END_DATE
    ),

    AntibodyTests AS (
        SELECT
            t.client_id,
            t.hiv_test_result as rapid_antibody_test_result,
            ROW_NUMBER() OVER (PARTITION BY t.client_id ORDER BY t.hiv_test_date DESC) as rn
        FROM mamba_flat_encounter_hei_hiv_test t
        WHERE t.encounter_datetime <= REPORT_END_DATE
          AND t.specimen_type != 'DBS' -- Assuming non-DBS are rapid tests, or use specific test type logic
    )

    SELECT
        c.patient_name as `Full Name`,
        c.sex as `Sex`,
        c.date_of_birth as `DOB`,
        TIMESTAMPDIFF(MONTH, c.date_of_birth, REPORT_END_DATE) as `Age (in months)`,
        c.mrn as `MRN`,
        e.hei_code as `HEI Code`,
        f.follow_up_date as `Follow-up Date`,
        t.rapid_antibody_test_result as `Rapid Antibody Test Result`

    FROM AntibodyTests t
    JOIN mamba_dim_client c ON t.client_id = c.client_id
    LEFT JOIN Enrollment e ON t.client_id = e.client_id
    LEFT JOIN LatestFollowUp f ON t.client_id = f.client_id AND f.rn = 1;

END //

DELIMITER ;
