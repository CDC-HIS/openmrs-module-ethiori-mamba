DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_hei_query;

CREATE PROCEDURE sp_fact_line_list_hei_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN


    WITH Enrollment AS (
        SELECT
            e.client_id,
            e.date_enrolled_in_care as enrollment_date,
            e.hei_code,
            e.mothers_name,
            infant_referred,
            mother_s_pmtct_interventions,
            e.mother_status,
            e.arv_prophylaxis as enrollment_arv_prophylaxis,
            e.weight_text as birth_weight,
            e.date_enrolled_in_care,
            e.referring_facility_name
        FROM mamba_flat_encounter_hei_enrollment e
        WHERE e.date_enrolled_in_care BETWEEN REPORT_START_DATE AND REPORT_END_DATE
    ),

    LatestFollowUp AS (
        SELECT
            f.client_id,
            f.growth_pattern,
            f.delayed_milestones,
            f.reason_for_growth_failure,
            COALESCE(mother_s_breast_condition,mothers_breast_condition) as mothers_breast_condition,
            f.reason_for_red_flag,
            f.followup_date_followup as follow_up_date,
            f.weight_text as current_weight,
             f1.infant_feeding_practice_within_the_first_6_months_of_life,
            f1.infant_feeding_practice_older_than_6_months_of_life,
             COALESCE(f1.infant_feeding_practice_within_the_first_6_months_of_life, f1.infant_feeding_practice_older_than_6_months_of_life) as feeding_practice,
             f1.cotrimoxazole_prophylaxis_dose,
             f1.cotrimoxazole_adherence_level,
             f1.developmental_milestone_for_children,
            f.next_visit_date,
            ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY f.followup_date_followup DESC) as rn
        FROM mamba_flat_encounter_hei_followup f
        left join mamba_flat_encounter_hei_followup_1 f1 on f.encounter_id=f1.encounter_id
        WHERE f.followup_date_followup <= REPORT_END_DATE
    ),

    HIVTesting AS (
        SELECT
            t.client_id,
            -- PCR 1 (Initial)
            MAX(CASE WHEN t.test_round = 'Initial test' THEN t.hiv_test_date END) as pcr_1_date,
            MAX(CASE WHEN t.test_round = 'Initial test' THEN t.hiv_test_result END) as pcr_1_result,
            -- PCR 2 (Confirmatory or subsequent)
            MAX(CASE WHEN t.test_round != 'Initial test' AND t.specimen_type = 'DBS' THEN t.hiv_test_date END) as pcr_2_date,
            MAX(CASE WHEN t.test_round != 'Initial test' AND t.specimen_type = 'DBS' THEN t.hiv_test_result END) as pcr_2_result,
            -- Antibody Test (>= 18 months usually, or rapid)
            MAX(CASE WHEN t.specimen_type != 'DBS' THEN t.hiv_test_date END) as antibody_test_date,
            MAX(CASE WHEN t.specimen_type != 'DBS' THEN t.hiv_test_result END) as antibody_test_result
        FROM mamba_flat_encounter_hei_hiv_test t
        WHERE t.dna_pcr_sample_collection_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
        GROUP BY t.client_id
    ),

    FinalOutcome AS (
        SELECT
            fo.client_id,
            fo.hei_pmtct_final_outcome,
            fo.date_when_final_outcome_was_known,
            ROW_NUMBER() OVER (PARTITION BY fo.client_id ORDER BY fo.encounter_datetime DESC) as rn
        FROM mamba_flat_encounter_hei_final_outcome fo
        WHERE fo.encounter_datetime <= REPORT_END_DATE
    )

    SELECT
        c.patient_name as `Patient Name`,
        c.mrn as `MRN`,
        e.hei_code as `HEI Code`,
        e.infant_referred as `Infant Referred?`,
        e.referring_facility_name as `Referring Facility Name`,
        e.mother_s_pmtct_interventions as `Mother''s PMTCT Intervention`,
        TIMESTAMPDIFF(MONTH, c.date_of_birth, REPORT_END_DATE) as `Age (Months)`,
        c.date_of_birth as `Date of birth`,
        c.date_of_birth as `Date of birth EC.`,
        c.sex as `Sex`,
        
        -- Enrollment Info
        e.enrollment_date as `Enrollment Date`,
        e.enrollment_date as `Enrollment Date EC.`,
        e.mothers_name as `Mother's Name`,
        e.mother_status as `Mother's Status`,
        e.enrollment_arv_prophylaxis as `ARV Prophylaxis at Enrollment`,
        e.birth_weight as `Birth Weight (in gram)`,
        
        -- HIV Testing
        t.pcr_1_date as `DNA-PCR Sample Collection Date 1`,
        t.pcr_1_date as `DNA-PCR Sample Collection Date 1 EC.`,
        t.pcr_1_result as `DNA-PCR Result 1`,
        t.pcr_2_date as `DNA-PCR Sample Collection Date 2`,
        t.pcr_2_date as `DNA-PCR Sample Collection Date 2 EC.`,
        t.pcr_2_result as `DNA-PCR Result 2`,
        t.antibody_test_date as `Antibody Test Date`,
        t.antibody_test_date as `Antibody Test Date EC.`,
        t.antibody_test_result as `Antibody Test Result`,
        
        -- Follow Up
        f.follow_up_date as `Latest Follow-up Date`,
        f.follow_up_date as `Latest Follow-up Date EC.`,
        f.current_weight as `Current Weight`,
        f.growth_pattern as `Growth Pattern`,
        f.reason_for_growth_failure as `Reason for Growth Failure`,
         f.feeding_practice as `Infant Feeding Practice`,
         f.mothers_breast_condition as `Mothers Breast Condition`,
         f.cotrimoxazole_prophylaxis_dose as `CPT Dose`,
         f.developmental_milestone_for_children as `Development Milestone`,
         f.reason_for_red_flag as `Reason for Red Flag`,
        f.next_visit_date as `Next Visit Date`,
        f.next_visit_date as `Next Visit Date EC.`,
        
        -- Final Outcome
        fo.hei_pmtct_final_outcome as `Final Outcome`,
        fo.date_when_final_outcome_was_known as `Final Outcome Date`,
        fo.date_when_final_outcome_was_known as `Final Outcome Date EC.`

    FROM Enrollment e
    JOIN mamba_dim_client c ON e.client_id = c.client_id
    LEFT JOIN LatestFollowUp f ON e.client_id = f.client_id AND f.rn = 1
    LEFT JOIN HIVTesting t ON e.client_id = t.client_id
    LEFT JOIN FinalOutcome fo ON e.client_id = fo.client_id AND fo.rn = 1;

END //

DELIMITER ;