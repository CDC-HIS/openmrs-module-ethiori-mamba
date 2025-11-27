DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_hei_query;

CREATE PROCEDURE sp_fact_line_list_hei_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH Enrollment_tmp as (select client_id,
                                   antenatal_care_provider,
                                   ld_client,
                                   post_natal_care,
                                   art_clinic,
                                   location_of_birth,
                                   date_of_enrollment_or_booking,
                                   ROW_NUMBER() over (PARTITION BY client_id ORDER BY date_of_enrollment_or_booking DESC, encounter_id DESC) as row_num
                            from mamba_flat_encounter_pmtct_enrollment
                            where date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         Enrollment as (select * from Enrollment_tmp where row_num = 1),
         FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id                 AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             assessment_date,
                             treatment_end_date,
                             antiretroviral_art_dispensed_dose_i AS ARTDoseDays,
                             weight_text_                        AS Weight,
                             screening_test_result_tuberculosis  AS TB_SreeningResult,
                             date_of_last_menstrual_period_lmp_     LMP_Date,
                             anitiretroviral_adherence_level     AS AdherenceLevel,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             diagnosis_date                      AS ActiveTBDiagnoseddate,
                             nutritional_status_of_adult,
                             nutritional_supplements_provided,
                             stages_of_disclosure,
                             date_started_on_tuberculosis_prophy,
                             method_of_family_planning,
                             patient_diagnosed_with_active_tuber as ActiveTBDiagnosed,
                             dsd_category,
                             nutritional_screening_result,
                             cd4_count,
                             date_of_event                       as hiv_confirmed_date,
                             date_started_on_tuberculosis_prophy AS inhprophylaxis_started_date,
                             date_completed_tuberculosis_prophyl AS InhprophylaxisCompletedDate,
                             date_active_tbrx_completed,
                             date_of_reported_hiv_viral_load     as viral_load_sent_date,
                             date_viral_load_results_received    AS viral_load_perform_date,
                             date_referred_to_pmtct,
                             viral_load_test_status,
                             CASE visitect_cd4_result
    -- HEI Line List Query
    -- Aggregates data from Enrollment, Follow-up, HIV Testing, and Final Outcome

    SET @row_number = 0;

    WITH Enrollment AS (
        SELECT
            e.client_id,
            e.encounter_date as enrollment_date,
            e.hei_code,
            e.mothers_name,
            e.mother_status,
            e.arv_prophylaxis as enrollment_arv_prophylaxis,
            e.weight_text as birth_weight, -- Assuming weight at enrollment is birth weight or close to it
            e.date_enrolled_in_care,
            e.referring_facility_name
        FROM mamba_flat_encounter_hei_enrollment e
        WHERE e.encounter_date <= REPORT_END_DATE
    ),

    LatestFollowUp AS (
        SELECT
            f.client_id,
            f.encounter_date as follow_up_date,
            f.weight_text as current_weight,
            f.infant_feeding_practice_within_the_first_6_months_of_life,
            f.infant_feeding_practice_older_than_6_months_of_life,
            COALESCE(f.infant_feeding_practice_within_the_first_6_months_of_life, f.infant_feeding_practice_older_than_6_months_of_life) as feeding_practice,
            f.cotrimoxazole_prophylaxis_dose,
            f.cotrimoxazole_adherence_level,
            f.developmental_milestone_for_children,
            f.next_visit_date,
            ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY f.encounter_date DESC) as rn
        FROM mamba_flat_encounter_hei_followup f
        WHERE f.encounter_date <= REPORT_END_DATE
    ),

    HIVTesting AS (
        SELECT
            t.client_id,
            -- PCR 1 (Initial)
            MAX(CASE WHEN t.test_round = 'Initial test' THEN t.hiv_test_date END) as pcr_1_date,
            MAX(CASE WHEN t.test_round = 'Initial test' THEN t.hiv_test_result END) as pcr_1_result,
            -- PCR 2 (Confirmatory or subsequent)
            MAX(CASE WHEN t.test_round != 'Initial test' AND t.specimen_type = 'DBS' THEN t.hiv_test_date END) as pcr_2_date, -- Simplification, refine if needed
            MAX(CASE WHEN t.test_round != 'Initial test' AND t.specimen_type = 'DBS' THEN t.hiv_test_result END) as pcr_2_result,
            -- Antibody Test (>= 18 months usually, or rapid)
            MAX(CASE WHEN t.specimen_type != 'DBS' THEN t.hiv_test_date END) as antibody_test_date,
            MAX(CASE WHEN t.specimen_type != 'DBS' THEN t.hiv_test_result END) as antibody_test_result
        FROM mamba_flat_encounter_hei_hiv_test t
        WHERE t.encounter_date <= REPORT_END_DATE
        GROUP BY t.client_id
    ),

    FinalOutcome AS (
        SELECT
            fo.client_id,
            fo.hei_pmtct_final_outcome,
            fo.date_when_final_outcome_was_known,
            ROW_NUMBER() OVER (PARTITION BY fo.client_id ORDER BY fo.encounter_date DESC) as rn
        FROM mamba_flat_encounter_hei_final_outcome fo
        WHERE fo.encounter_date <= REPORT_END_DATE
    )

    SELECT
        c.patient_name as `Patient Name`,
        c.mrn as `MRN`,
        e.hei_code as `HEI Code`,
        TIMESTAMPDIFF(MONTH, c.date_of_birth, REPORT_END_DATE) as `Age (Months)`,
        c.date_of_birth as `DOB`,
        c.sex as `Sex`,
        
        -- Enrollment Info
        e.enrollment_date as `Enrollment Date`,
        e.mothers_name as `Mother's Name`,
        e.mother_status as `Mother's Status`,
        e.enrollment_arv_prophylaxis as `ARV Prophylaxis at Enrollment`,
        e.birth_weight as `Birth Weight`,
        
        -- HIV Testing
        t.pcr_1_date as `PCR 1 Date`,
        t.pcr_1_result as `PCR 1 Result`,
        t.pcr_2_date as `PCR 2 Date`,
        t.pcr_2_result as `PCR 2 Result`,
        t.antibody_test_date as `Antibody Test Date`,
        t.antibody_test_result as `Antibody Test Result`,
        
        -- Follow Up
        f.follow_up_date as `Latest Follow-up Date`,
        f.current_weight as `Current Weight`,
        f.feeding_practice as `Feeding Practice`,
        f.cotrimoxazole_prophylaxis_dose as `CPT Dose`,
        f.developmental_milestone_for_children as `Developmental Milestones`,
        f.next_visit_date as `Next Visit Date`,
        
        -- Final Outcome
        fo.hei_pmtct_final_outcome as `Final Outcome`,
        fo.date_when_final_outcome_was_known as `Final Outcome Date`

    FROM Enrollment e
    JOIN mamba_dim_client c ON e.client_id = c.client_id
    LEFT JOIN LatestFollowUp f ON e.client_id = f.client_id AND f.rn = 1
    LEFT JOIN HIVTesting t ON e.client_id = t.client_id
    LEFT JOIN FinalOutcome fo ON e.client_id = fo.client_id AND fo.rn = 1;

END //

DELIMITER ;