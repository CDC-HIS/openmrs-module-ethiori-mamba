DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_eid_dna_pcr_query;

CREATE PROCEDURE sp_fact_line_list_eid_dna_pcr_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN

    -- EID DNA PCR Line List Query

    WITH Enrollment AS (
        SELECT
            e.client_id,
            e.hei_code,
            e.arv_prophylaxis as enrollment_arv_prophylaxis,
            e.mother_status
        FROM mamba_flat_encounter_hei_enrollment e
        WHERE e.encounter_datetime <= REPORT_END_DATE
    ),

    LatestFollowUp AS (
        SELECT
            f.client_id,
            f.encounter_datetime as follow_up_date,
            COALESCE(f.infant_feeding_practice_within_the_first_6_months_of_life, f.infant_feeding_practice_older_than_6_months_of_life) as feeding_practice,
            ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY f.encounter_date DESC) as rn
        FROM mamba_flat_encounter_hei_followup f
        WHERE f.encounter_date <= REPORT_END_DATE
    ),

    DNAPCRTests AS (
        SELECT
            t.client_id,
            t.maternal_art_status,
            t.initial_test, -- Assuming this maps to 'Test Indication' or similar logic
            t.specimen_type,
            t.dna_pcr_sample_collection_date,
            t.hiv_test_result as dna_pcr_result,
            t.date_dbs_result_received, -- Date of Result Received by H.F
            DATEDIFF(t.date_dbs_result_received, t.dna_pcr_sample_collection_date) as tat,
            t.specimen_received_date, -- Date of sample received by Lab (approx) or Date of DBS referral
            t.laboratory_name,
            t.sample_quality,
            t.reason_sample_rejected_or_test_not_done,
            t.hiv_test_date as date_test_performed,
            t.platform_used,
            t.telephone_number_of_referringordering_clinician as mothers_phone_no, -- Assuming this field or similar
            ROW_NUMBER() OVER (PARTITION BY t.client_id ORDER BY t.dna_pcr_sample_collection_date DESC) as rn
        FROM mamba_flat_encounter_hei_hiv_test t
        WHERE t.encounter_datetime <= REPORT_END_DATE
          AND (t.test_round = 'Initial test' OR t.specimen_type = 'DBS') -- Filter for PCR relevant tests
    )

    SELECT
        c.patient_name as `Full Name`,
        c.sex as `Sex`,
        c.date_of_birth as `DOB`,
        TIMESTAMPDIFF(MONTH, c.date_of_birth, REPORT_END_DATE) as `Age (in months)`,
        c.mrn as `MRN`,
        e.hei_code as `HEI Code`,
        f.follow_up_date as `Follow-up Date`,
        f.feeding_practice as `Infant on BF?`,
        e.enrollment_arv_prophylaxis as `ARV Prophylaxis`,
        t.maternal_art_status as `Maternal ART Status`,
        t.initial_test as `Test Indication`, -- Mapping check needed
        t.specimen_type as `Specimen Type`,
        t.dna_pcr_sample_collection_date as `Date of Sample Collection (E.C)`,
        t.dna_pcr_result as `DNA PCR Result`,
        t.date_dbs_result_received as `Date of Result Received by H.F (E.C)`,
        t.tat as `TAT (in days)`,
        t.specimen_received_date as `Date of DBS referral to regional lab (E.C)`, -- Mapping check needed
        t.laboratory_name as `Name of Testing Lab`,
        t.specimen_received_date as `Date of sample received by Lab`, -- Mapping check needed
        t.sample_quality as `Sample Quality`,
        t.reason_sample_rejected_or_test_not_done as `Reason for Sample rejection/test not done`,
        t.date_test_performed as `Date test performed by Lab`,
        t.platform_used as `Platform used`,
        t.mothers_phone_no as `Mother's Phone No.`

    FROM DNAPCRTests t
    JOIN mamba_dim_client c ON t.client_id = c.client_id
    LEFT JOIN Enrollment e ON t.client_id = e.client_id
    LEFT JOIN LatestFollowUp f ON t.client_id = f.client_id AND f.rn = 1;

END //

DELIMITER ;
