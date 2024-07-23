DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_encounter_care_and_treatment_tx_new_query;

CREATE PROCEDURE sp_fact_encounter_care_and_treatment_tx_new_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
SELECT dim_ct.patient_name,
       dim_ct.mrn,
       dim_ct.uan,
       dim_ct.current_age,
       dim_ct.sex,
       dim_ct.mobile_no,
       fact_ct.weight_in_kg,
       fact_ct.cd4_count,
       fact_ct.current_who_hiv_stage,
       fact_ct.nutritional_status,
       fact_ct.tb_screening_result,
       fact_ct.enrollment_date,
       fact_ct.hiv_confirmed_date,
       fact_ct.art_start_date,
       fact_ct.days_difference,
       fact_ct.followup_date,
       fact_ct.regimen,
       fact_ct.arv_dose_days,
       fact_ct.pregnancy_status,
       fact_ct.breast_feeding_status,
       fact_ct.follow_up_status,
       fact_ct.ti,
       fact_ct.treatment_end_date,
       fact_ct.next_visit_date,
       fact_ct.latest_followup_date,
       fact_ct.latest_followup_status,
       fact_ct.latest_regimen,
       fact_ct.latest_arv_dose_days
FROM mamba_fact_care_and_treatment fact_ct
         INNER JOIN mamba_dim_client_care_and_treatment as dim_ct
                    on fact_ct.client_id = dim_ct.client_id

WHERE fact_ct.art_start_date BETWEEN  CAST(REPORT_START_DATE as DATE) AND CAST(REPORT_END_DATE as DATE)
  and dim_ct.mrn is not null and fact_ct.follow_up_status in ('Alive', 'Restart');
END //

DELIMITER ;
