
show full processlist ;

SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'analytics_db';
SELECT COUNT(*) AS 'Stored Procedures' FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'analytics_db';
SELECT COUNT(*) AS 'Stored Procedures' FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = 'analytics_db';
# ROUTINE_TYPE = 'PROCEDURE' AND



WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_status,
                         follow_up_date_followup_            AS follow_up_date,
                         art_antiretroviral_start_date       AS art_start_date,
                         treatment_end_date,
                         next_visit_date,
                         regimen,
                         currently_breastfeeding_child          breast_feeding_status,
                         pregnancy_status,
                         was_the_patient_screened_for_tuberc as tb_screened,
                         tb_screening_date,
                         tb_treatment_status,
                         transferred_in_check_this_for_all_t as transferred_in,
                         date_started_on_tuberculosis_prophy,
                         screening_test_result_tuberculosis     screening_result,
                         lf_lam_result,
                         patient_diagnosed_with_active_tuber,
                         diagnosis_date as active_tb_diagnosed_date,
                         tuberculosis_drug_treatment_start_d as tb_treatment_start_date,
                         date_active_tbrx_completed,
                         date_active_tbrx_dc,
                         currently_taking_tuberculosis_proph
                  FROM mamba_flat_encounter_follow_up follow_up
                           LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                     ON follow_up.encounter_id = follow_up_1.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                     ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                     ON follow_up.encounter_id = follow_up_4.encounter_id),
     tmp_latest_follow_up as (SELECT client_id,
                                     follow_up_date                                                                             ,
                                     encounter_id,
                                     follow_up_status,
                                     tb_screening_date,
                                     art_start_date,
                                     tb_screened,
                                     screening_result,
                                     lf_lam_result,
                                     patient_diagnosed_with_active_tuber,
                                     active_tb_diagnosed_date,
                                     tb_treatment_start_date,
                                     date_active_tbrx_completed,
                                     date_active_tbrx_dc,
                                     currently_taking_tuberculosis_proph,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND follow_up_date <= :REPORT_END_DATE),
     tb_art as (
         select tmp_latest_follow_up.*,
                sex,
                date_of_birth,
                mrn,
                uan,
                DATE_ADD(tb_treatment_start_date, INTERVAL 1 YEAR ) as ON_TREATMENT_MONTH,
                (SELECT datim_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,:REPORT_END_DATE)=age) as fine_age_group,
                (SELECT normal_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,:REPORT_END_DATE)=age) as coarse_age_group
         from tmp_latest_follow_up
                  join mamba_dim_client client on client.client_id=tmp_latest_follow_up.client_id
         where follow_up_status in ('Alive','Restart medication')
           and row_num=1
           and art_start_date <= :REPORT_END_DATE
           and (active_tb_diagnosed_date <= :REPORT_END_DATE OR tb_treatment_start_date <= :REPORT_END_DATE)
           and (
             (date_active_tbrx_dc is null and date_active_tbrx_completed is null and  (DATE_ADD(tb_treatment_start_date, INTERVAL 1 YEAR ) >= :REPORT_END_DATE ))
                 OR
             (date_active_tbrx_completed > :REPORT_END_DATE OR date_active_tbrx_dc > :REPORT_END_DATE )
             )
     )
select * from tb_art;
