DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_encounter_care_and_treatment_tx_new_query;

CREATE PROCEDURE sp_fact_encounter_care_and_treatment_tx_new_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
SELECT dim_ct.patient_name,
       dim_ct.mrn,
       dim_ct.uan,
       fact_ct.age_at_enrollment,
       dim_ct.current_age,
       dim_ct.sex,
       dim_ct.mobile_no,
       fact_ct.enrollment_date,
       fact_ct.hiv_confirmed_date,
       fact_ct.art_start_date,
       fact_ct.days_difference,
       fact_ct.pregnancy_status,
       fact_ct.regimen_at_enrollment,
       fact_ct.arv_dose_at_enrollment,
       fact_ct.last_follow_up_status,
       fact_ct.next_visit_date
FROM mamba_fact_care_and_treatment fact_ct
         INNER JOIN mamba_dim_client_care_and_treatment as dim_ct
                    on fact_ct.client_id = dim_ct.client_id

WHERE fact_ct.art_start_date BETWEEN  CAST(REPORT_START_DATE as DATE) AND CAST(REPORT_END_DATE as DATE);
END //

DELIMITER ;
