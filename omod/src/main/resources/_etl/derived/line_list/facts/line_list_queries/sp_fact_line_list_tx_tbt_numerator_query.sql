DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_tbt_numerator_query;

CREATE PROCEDURE sp_fact_line_list_tx_tbt_numerator_query(IN REPORT_START_DATE DATE,
                                                          IN REPORT_END_DATE DATE)
BEGIN

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
                                   fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL -6 MONTH))
                             AND tpt_start_date < REPORT_START_DATE),
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
                                      fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL -6
                                      MONTH))
                                AND tpt_completed_date < REPORT_END_DATE),
         tpt_completed as (select tmp_tpt_complete.*
                           from tmp_tpt_complete
                                    join tpt_started on tpt_started.client_id = tmp_tpt_complete.client_id
                           where tmp_tpt_complete.row_num = 1),

         tmp_latest_follow_up AS (SELECT *,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC,
                                             encounter_id DESC) AS r_n
                                  FROM FollowUp
                                  where follow_up_date < REPORT_END_DATE),


         latest_follow_up AS (select *
                              from tmp_latest_follow_up
                              where r_n = 1)

    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)                              as `#`,
           client.patient_uuid                                                    as `Patient GUID`,
           client.patient_name                                                    as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                                           as MRN,
           client.uan                                                             AS UAN,
           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE()))                    as `Age`,
           client.sex                                                             as Sex,
           date_hiv_confirmed                                                     as `Hiv Confirmed Date in EC.`,
           date_hiv_confirmed                                                     as `Hiv Confirmed Date in G.C.`,
           f_case.art_start_date                                                  as `ART Start Date EC.`,
           f_case.art_start_date                                                  as `ART Start Date G.C`,
           f_case.tpt_start_date                                                  as `TPT Start Date in EC.`,
           f_case.tpt_start_date                                                  as `TPT Start Date in G.C.`,
           f_case.tpt_completed_date                                              as `TPT Completed Date in EC.`,
           f_case.tpt_completed_date                                              as `TPT Completed Date in G.C.`,
           f_case.tpt_discontinue_date                                            as `TPT Discontinued Date in EC.`,
           f_case.tpt_discontinue_date                                            as `TPT Discontinued Date in G.C.`,
           f_case.tpt_type                                                        as `TpT Type`,
           CASE f_case.tpt_followup_status
               WHEN 'Alive' THEN 'Alive on ART'
               WHEN 'Restart medication' THEN 'Restart'
               WHEN 'Transferred out' THEN 'TO'
               WHEN 'Stop all' THEN 'Stop'
               WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
               WHEN 'Ran away' THEN 'Drop'
               END                                                                as `TPT Follow-up status`,
           f_case.tpt_dosday_type                                                 as `TPT Dispensed Dose`,
           f_case.tpt_adherence                                                   as `TPT Adherence`,
           f_case.follow_up_date                                                  as `Latest Follow-up Date in EC.`,
           f_case.follow_up_date                                                  as `Latest Follow-up Date in G.C.`,
           f_case.follow_up_status                                                as `Latest Follow-up Status`,
           f_case.regimen                                                         as `Latest Regimen`,
           f_case.antiretroviral_art_dispensed_dose_i                             as `Latest ARV Dose Days`,
           f_case.anitiretroviral_adherence_level                                 as `Latest Adherence`,
           f_case.next_visit_date                                                 as `Next Visit Date in EC.`,
           f_case.next_visit_date                                                 as `Next Visit Date in G.C.`,
           fn_get_ti_status(f_case.client_id, REPORT_START_DATE, REPORT_END_DATE) as `TI?`

    FROM latest_follow_up AS f_case
             INNER JOIN tpt_completed on f_case.client_id = tpt_completed.client_id
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id

    ORDER BY client.patient_name;


END //

DELIMITER ;
