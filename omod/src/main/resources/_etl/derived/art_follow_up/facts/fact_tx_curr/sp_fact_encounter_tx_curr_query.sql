DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_tx_curr_query;

CREATE PROCEDURE sp_fact_encounter_art_follow_up_tx_curr_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    SELECT   subquery.client_id,
             dim_art_follow_up.patient_name,
             dim_art_follow_up.mrn,
             dim_art_follow_up.uan,
             dim_art_follow_up.current_age,
             fn_mamba_age_calculator(date_of_birth, enrollment_date) as age_at_enrollment,
             dim_art_follow_up.sex,
             dim_art_follow_up.mobile_no,
             hiv_confirmed_date,
             art_start_date,
             MAX(CASE WHEN rn_desc = 1 THEN followup_date END)     AS followup_date,
             MAX(CASE WHEN rn_desc = 1 THEN weight_in_kg END)     AS weight_in_kg,
             MAX(CASE WHEN rn_desc = 1 THEN pregnancy_status END)     AS pregnancy_status,
             MAX(CASE WHEN rn_desc = 1 THEN regimen END)     AS regimen,
             MAX(CASE WHEN rn_desc = 1 THEN arv_dose_days END)     AS arv_dose_days,
             MAX(CASE WHEN rn_desc = 1 THEN follow_up_status END)     AS follow_up_status,
             MAX(CASE WHEN rn_desc = 1 THEN anitiretroviral_adherence_level END)     AS anitiretroviral_adherence_level,
             MAX(CASE WHEN rn_desc = 1 THEN next_visit_date END)     AS next_visit_date,
             MAX(CASE WHEN rn_desc = 1 THEN dsd_category END)     AS dsd_category,
             MAX(CASE WHEN rn_desc = 1 THEN tpt_start_date END)     AS tpt_start_date,
             MAX(CASE WHEN rn_desc = 1 THEN tpt_completed_date END)     AS tpt_completed_date,
             MAX(CASE WHEN rn_desc = 1 THEN tpt_discontinued_date END)     AS tpt_discontinued_date,
             MAX(CASE WHEN rn_desc = 1 THEN tuberculosis_treatment_end_date END)     AS tuberculosis_treatment_end_date,
             MAX(CASE WHEN rn_desc = 1 THEN date_viral_load_results_received END)     AS date_viral_load_results_received,
             MAX(CASE WHEN rn_desc = 1 THEN viral_load_test_status END)     AS viral_load_test_status,
             NOW()
    FROM (SELECT client_id,
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
                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date )     AS rn_asc,
                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date DESC) AS rn_desc
          from mamba_fact_art_follow_up
          where followup_date BETWEEN  CAST(REPORT_START_DATE as DATE) AND CAST(REPORT_END_DATE as DATE)
            and art_start_date BETWEEN  CAST(REPORT_START_DATE as DATE) AND CAST(REPORT_END_DATE as DATE)
            and regimen is not null
         ) AS subquery
             join mamba_dim_client_art_follow_up dim_art_follow_up on subquery.client_id=dim_art_follow_up.client_id
    WHERE (rn_asc = 1 OR rn_desc = 1) and follow_up_status='Alive'
    GROUP BY subquery.client_id;
END //

DELIMITER ;
