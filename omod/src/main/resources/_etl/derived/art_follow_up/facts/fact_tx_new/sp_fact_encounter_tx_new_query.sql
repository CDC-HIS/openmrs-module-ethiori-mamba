DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_tx_new_query;

CREATE PROCEDURE sp_fact_encounter_art_follow_up_tx_new_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
SELECT dim_follow_up.patient_name,
       dim_follow_up.mrn,
       dim_follow_up.uan,
       dim_follow_up.current_age,
       dim_follow_up.sex,
       dim_follow_up.mobile_no,
       fact_tx_new.weight_in_kg,
       fact_tx_new.cd4_count,
       fact_tx_new.current_who_hiv_stage,
       fact_tx_new.nutritional_status,
       fact_tx_new.tb_screening_result,
       fact_tx_new.enrollment_date,
       fact_tx_new.hiv_confirmed_date,
       fact_tx_new.art_start_date,
       fact_tx_new.days_difference,
       fact_tx_new.followup_date,
       fact_tx_new.regimen,
       fact_tx_new.arv_dose_days,
       fact_tx_new.pregnancy_status,
       fact_tx_new.breast_feeding_status,
       fact_tx_new.follow_up_status,
       fact_tx_new.ti,
       fact_tx_new.treatment_end_date,
       fact_tx_new.next_visit_date,
       fact_tx_new.latest_followup_date,
       fact_tx_new.latest_followup_status,
       fact_tx_new.latest_regimen,
       fact_tx_new.latest_arv_dose_days
FROM mamba_fact_tx_new fact_tx_new
         INNER JOIN mamba_dim_client_art_follow_up as dim_follow_up
                    on fact_tx_new.client_id = dim_follow_up.client_id

WHERE fact_tx_new.art_start_date BETWEEN  CAST(REPORT_START_DATE as DATE) AND CAST(REPORT_END_DATE as DATE)
  and dim_follow_up.mrn is not null and fact_tx_new.follow_up_status in ('Alive', 'Restart');
END //

DELIMITER ;
