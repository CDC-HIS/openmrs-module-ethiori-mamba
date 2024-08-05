DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_tx_curr_query;

CREATE PROCEDURE sp_fact_encounter_art_follow_up_tx_curr_query(IN REPORT_END_DATE DATE)
BEGIN
    WITH LatestFollowUp AS (
        SELECT
            client_id,
            hiv_confirmed_date,
            art_start_date,
            followup_date,
            enrollment_date,
            weight_in_kg,
            pregnancy_status,
            regimen,
            arv_dose_days,
            follow_up_status,
            anitiretroviral_adherence_level,
            next_visit_date,
            dsd_category,
            tpt_start_date,
            tpt_completed_date,
            tpt_discontinued_date,
            tuberculosis_treatment_end_date,
            date_viral_load_results_received,
            viral_load_test_status,
            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date DESC) AS rn_desc
        FROM mamba_fact_art_follow_up
        WHERE followup_date <= CAST(REPORT_END_DATE AS DATE)
          AND art_start_date <= CAST(REPORT_END_DATE AS DATE)
          AND regimen IS NOT NULL
    )
    SELECT
        lf.client_id,
        dim.patient_name,
        dim.mrn,
        dim.uan,
        dim.current_age,
        fn_mamba_age_calculator(dim.date_of_birth, lf.enrollment_date) AS age_at_enrollment,
        dim.sex,
        dim.mobile_no,
        lf.hiv_confirmed_date,
        lf.art_start_date,
        lf.followup_date,
        lf.weight_in_kg,
        lf.pregnancy_status,
        lf.regimen,
        lf.arv_dose_days,
        lf.follow_up_status,
        lf.anitiretroviral_adherence_level,
        lf.next_visit_date,
        lf.dsd_category,
        lf.tpt_start_date,
        lf.tpt_completed_date,
        lf.tpt_discontinued_date,
        lf.tuberculosis_treatment_end_date,
        lf.date_viral_load_results_received,
        lf.viral_load_test_status,
        NOW()
    FROM LatestFollowUp lf
             JOIN mamba_dim_client_art_follow_up dim ON lf.client_id = dim.client_id
    WHERE lf.rn_desc = 1
      AND lf.follow_up_status = 'Alive';
END //

DELIMITER ;