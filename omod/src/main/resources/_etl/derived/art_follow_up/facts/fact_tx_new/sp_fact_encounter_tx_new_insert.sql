-- $BEGIN
INSERT INTO mamba_fact_tx_new (client_id,
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
SELECT
    first_follow_up.client_id,
    MAX(first_follow_up.weight_in_kg) AS weight_in_kg,
    MAX(first_follow_up.cd4_count) AS cd4_count,
    MAX(first_follow_up.current_who_hiv_stage) AS current_who_hiv_stage,
    MAX(first_follow_up.nutritional_status) AS nutritional_status,
    MAX(first_follow_up.tb_screening_result) AS tb_screening_result,
    MAX(first_follow_up.enrollment_date) AS enrollment_date,
    MAX(first_follow_up.hiv_confirmed_date) AS hiv_confirmed_date,
    MAX(first_follow_up.art_start_date) AS art_start_date,
    MAX(first_follow_up.days_difference) AS days_difference,
    MAX(first_follow_up.followup_date) AS followup_date,
    MAX(first_follow_up.regimen) AS regimen,
    MAX(first_follow_up.arv_dose_days) AS arv_dose_days,
    MAX(first_follow_up.pregnancy_status) AS pregnancy_status,
    MAX(first_follow_up.breast_feeding_status) AS breast_feeding_status,
    MAX(first_follow_up.follow_up_status) AS follow_up_status,
    MAX(first_follow_up.ti) AS ti,
    MAX(latest_follow_up.treatment_end_date) AS treatment_end_date,
    MAX(latest_follow_up.next_visit_date) AS next_visit_date,
    MAX(latest_follow_up.followup_date) AS latest_followup_date,
    MAX(latest_follow_up.follow_up_status) AS latest_followup_status,
    MAX(latest_follow_up.regimen) AS latest_regimen,
    MAX(latest_follow_up.arv_dose_days) AS latest_arv_dose_days
FROM
    mamba_fact_art_first_follow_up first_follow_up
        JOIN
    mamba_fact_art_latest_follow_up latest_follow_up
    ON
        first_follow_up.client_id = latest_follow_up.client_id
GROUP BY
    first_follow_up.client_id;
-- $END
