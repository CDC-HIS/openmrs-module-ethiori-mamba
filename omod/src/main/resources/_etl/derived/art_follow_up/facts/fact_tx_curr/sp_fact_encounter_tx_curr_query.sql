DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_tx_curr_query;

CREATE PROCEDURE sp_fact_encounter_art_follow_up_tx_curr_query(IN REPORT_END_DATE DATE)
BEGIN
    WITH latest_follow_up AS (
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
        WHERE
            followup_date <= CAST(REPORT_END_DATE AS DATE)
          AND art_start_date <= CAST(REPORT_END_DATE AS DATE)
          AND regimen IS NOT NULL
    )
    SELECT
        subquery.client_id,
        dim_art_follow_up.patient_name,
        dim_art_follow_up.mrn,
        dim_art_follow_up.uan,
        dim_art_follow_up.current_age,
        fn_mamba_age_calculator(dim_art_follow_up.date_of_birth, subquery.enrollment_date) AS age_at_enrollment,
        dim_art_follow_up.sex,
        dim_art_follow_up.mobile_no,
        subquery.hiv_confirmed_date,
        subquery.art_start_date,
        subquery.followup_date,
        subquery.weight_in_kg,
        subquery.pregnancy_status,
        subquery.regimen,
        subquery.arv_dose_days,
        subquery.follow_up_status,
        subquery.anitiretroviral_adherence_level,
        subquery.next_visit_date,
        subquery.dsd_category,
        subquery.tpt_start_date,
        subquery.tpt_completed_date,
        subquery.tpt_discontinued_date,
        subquery.tuberculosis_treatment_end_date,
        subquery.date_viral_load_results_received,
        subquery.viral_load_test_status,
        NOW() AS query_run_time
    FROM latest_follow_up AS subquery
             JOIN mamba_dim_client_art_follow_up AS dim_art_follow_up
                  ON subquery.client_id = dim_art_follow_up.client_id
    WHERE subquery.rn_desc = 1
      AND subquery.follow_up_status = 'Alive'
    GROUP BY subquery.client_id;
END //

DELIMITER ;
