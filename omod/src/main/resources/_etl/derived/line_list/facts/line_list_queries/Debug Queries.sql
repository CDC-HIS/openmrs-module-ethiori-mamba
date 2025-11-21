call sp_fact_line_list_tx_tbt_denominator_query('2024-10-01', '2024-12-29');
call sp_dim_tb_prev_datim_numerator_query('2024-10-01', '2024-12-29', 0, 'DEBUG');

select fn_ethiopian_to_gregorian_calendar('2017-04-20');


WITH FollowUp as (select follow_up.encounter_id,
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
                         date_started_on_tuberculosis_prophy as tpt_start_date,
                         date_completed_tuberculosis_prophyl as tpt_completed_date,
                         screening_test_result_tuberculosis     screening_result,
                         lf_lam_result,
                         patient_diagnosed_with_active_tuber,
                         diagnosis_date                      as active_tb_diagnosed_date,
                         tuberculosis_drug_treatment_start_d as tb_treatment_start_date,
                         date_of_event                       as date_hiv_confirmed,
                         tb_prophylaxis_type                 as tpt_type,
                         tpt_followup_6h_                    as tpt_followup_status,
                         date_discontinued_tuberculosis_prop as tpt_discontinue_date,
                         tpt_dispensed_dose_in_days_inh_     as tpt_dosday_type,
                         adherence                           as tpt_adherence,
                         anitiretroviral_adherence_level,
                         antiretroviral_art_dispensed_dose_i
                  FROM mamba_flat_encounter_follow_up follow_up
                           LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                     ON follow_up.encounter_id = follow_up_1.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                     ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                     ON follow_up.encounter_id = follow_up_4.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_5 follow_up_5
                                     ON follow_up.encounter_id = follow_up_5.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_6 follow_up_6
                                     ON follow_up.encounter_id = follow_up_6.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_7 follow_up_7
                                     ON follow_up.encounter_id = follow_up_7.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_8 follow_up_8
                                     ON follow_up.encounter_id = follow_up_8.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_9 follow_up_9
                                     ON follow_up.encounter_id = follow_up_9.encounter_id),
     tmp_tpt_start as (select client_id,
                              tpt_start_date,
                              art_start_date,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY tpt_start_date DESC, FollowUp.encounter_id DESC) AS row_num
                       from FollowUp
                       where tpt_start_date >= fn_ethiopian_to_gregorian_calendar(date_add(
                               fn_gregorian_to_ethiopian_calendar('2024-10-01', 'Y-M-D'), INTERVAL -6 MONTH))
                         AND tpt_start_date < '2024-10-01'),
     tpt_started as (select tmp_tpt_start.*,
                            sex,
                            date_of_birth
                     from tmp_tpt_start
                              join mamba_dim_client client on client.client_id = tmp_tpt_start.client_id
                     where row_num = 1),

     tmp_tpt_complete as (select client_id,
                                 tpt_completed_date,
                                 art_start_date,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY tpt_completed_date DESC, FollowUp.encounter_id DESC) AS row_num
                          from FollowUp
                          where tpt_completed_date >= fn_ethiopian_to_gregorian_calendar(date_add(
                                  fn_gregorian_to_ethiopian_calendar('2024-10-01', 'Y-M-D'), INTERVAL -6
                                  MONTH))
                            AND tpt_completed_date < '2024-12-29'),
     tpt_completed as (select tmp_tpt_complete.*
                       from tmp_tpt_complete
                                join tpt_started on tpt_started.client_id = tmp_tpt_complete.client_id
                       where tmp_tpt_complete.row_num = 1),

     tmp_latest_follow_up AS (SELECT *,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC,
                                         encounter_id DESC) AS r_n
                              FROM FollowUp
                              where follow_up_date < '2024-12-29'),


     latest_follow_up AS (select *
                          from tmp_latest_follow_up
                          where r_n = 1)
select *
FROM latest_follow_up AS f_case
    INNER JOIN tpt_completed on f_case.client_id = tpt_completed.client_id
    INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id
