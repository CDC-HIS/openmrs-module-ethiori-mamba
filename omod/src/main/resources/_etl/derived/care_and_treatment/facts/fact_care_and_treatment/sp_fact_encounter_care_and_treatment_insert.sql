-- $BEGIN
INSERT INTO mamba_fact_care_and_treatment (client_id,
                                           weight_in_kg,
                                           cd4_count,
                                           current_who_hiv_stage,
                                           nutritional_status,
                                           tb_screening_result,
                                           enrollment_date,
                                           hiv_confirmed_date,
                                           art_start_date,
                                           days_difference,
                                           followup_date,
                                           regimen,
                                           arv_dose_days,
                                           pregnancy_status,
                                           breast_feeding_status,
                                           follow_up_status,
                                           ti,
                                           treatment_end_date,
                                           next_visit_date,
                                           latest_followup_date,
                                           latest_followup_status,
                                           latest_regimen,
                                           latest_arv_dose_days)
SELECT subquery.client_id,
       MIN(CASE WHEN rn_asc = 1 THEN weight_kg_ END)                           AS weight_in_kg,
       MIN(CASE WHEN rn_asc = 1 THEN cd4_count END)                            AS cd4_count,
       MIN(CASE WHEN rn_asc = 1 THEN current_who_hiv_stage END)                AS current_who_hiv_stage,
       MIN(CASE WHEN rn_asc = 1 THEN nutritional_screening_result END)         AS nutritional_status,
       MIN(CASE WHEN rn_asc = 1 THEN screening_test_result_tuberculosis END)   AS tb_screening_result,
       int_a.date_enrolled_in_care as enrollment_date,
       MIN(CASE WHEN rn_asc = 1 THEN date_of_hiv_diagnosis END)                AS hiv_confirmed_date,
       MIN(CASE WHEN rn_asc = 1 THEN art_antiretroviral_start_date END)        AS art_start_date,
       DATEDIFF(MIN(CASE WHEN rn_asc = 1 THEN art_antiretroviral_start_date END), MIN(CASE WHEN rn_asc = 1 THEN date_of_hiv_diagnosis END)) AS days_difference,
       MIN(CASE WHEN rn_asc = 1 THEN follow_up_date_followup_ END)             AS followup_date,
       MIN(CASE WHEN rn_asc = 1 THEN regimen END)                              AS regimen,
       MIN(CASE WHEN rn_asc = 1 THEN antiretroviral_art_dispensed_dose_i END)  AS arv_dose_days,
       MIN(CASE WHEN rn_asc = 1 THEN pregnancy_status END)                     AS pregnancy_status,
       MIN(CASE WHEN rn_asc = 1 THEN currently_breastfeeding_child END)        AS breast_feeding_status,
       MIN(CASE WHEN rn_asc = 1 THEN follow_up_status END)                     AS follow_up_status,
       '',
       MAX(CASE WHEN rn_desc = 1 THEN treatment_end_date END)                  AS treatment_end_date,
       MAX(CASE WHEN rn_desc = 1 THEN return_visit_date END)                   AS next_visit_date,
       MAX(CASE WHEN rn_desc = 1 THEN follow_up_date_followup_ END)            AS latest_followup_date,
       MAX(CASE WHEN rn_desc = 1 THEN follow_up_status END)                    AS latest_followup_status,
       MAX(CASE WHEN rn_desc = 1 THEN regimen END)                             AS latest_regimen,
       MAX(CASE WHEN rn_desc = 1 THEN antiretroviral_art_dispensed_dose_i END) AS latest_arv_dose_days
FROM (SELECT enc_follow_up_1.client_id,
             weight_kg_,
             cd4_count,
             current_who_hiv_stage,
             nutritional_screening_result,
             screening_test_result_tuberculosis,
             date_of_hiv_diagnosis,
             art_antiretroviral_start_date,
             follow_up_date_followup_,
             regimen,
             antiretroviral_art_dispensed_dose_i,
             pregnancy_status,
             currently_breastfeeding_child,
             follow_up_status,
             treatment_end_date,
             return_visit_date,
             why_eligible_for_hiv_test_,
             ROW_NUMBER() OVER (PARTITION BY enc_follow_up_1.client_id ORDER BY follow_up_date_followup_ )     AS rn_asc,
             ROW_NUMBER() OVER (PARTITION BY enc_follow_up_1.client_id ORDER BY follow_up_date_followup_ DESC) AS rn_desc
      FROM mamba_flat_encounter_follow_up
               JOIN mamba_flat_encounter_follow_up_1 enc_follow_up_1 on enc_follow_up_1.encounter_id=mamba_flat_encounter_follow_up.encounter_id
      where art_antiretroviral_start_date is not null
        and (why_eligible_for_hiv_test_ != 'Transfer in' or why_eligible_for_hiv_test_ is null)
     ) AS subquery
         LEFT JOIN mamba_flat_encounter_intake_a int_a on subquery.client_id=int_a.client_id
WHERE (rn_asc = 1 OR rn_desc = 1)
GROUP BY subquery.client_id, int_a.date_enrolled_in_care;
-- $END

SELECT @@sql_mode;