DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tpt_linelist_query;

CREATE PROCEDURE sp_fact_line_list_tpt_linelist_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH FollowUp AS (SELECT follow_up.encounter_id,
                             follow_up.client_id,
                             date_of_event                       as hiv_confirmed_date,
                             art_antiretroviral_start_date       as art_start_date,
                             follow_up_date_followup_            as followup_date,
                             weight_text_                        as weight_in_kg,
                             pregnancy_status,
                             regimen,
                             antiretroviral_art_dispensed_dose_i as art_dose_days,
                             follow_up_status,
                             anitiretroviral_adherence_level,
                             next_visit_date,
                             dsd_category,
                             date_started_on_tuberculosis_prophy as tpt_start_date,
                             date_completed_tuberculosis_prophyl as tpt_completed_date,
                             date_discontinued_tuberculosis_prop as tpt_discontinued_date,
                             date_viral_load_results_received    as viral_load_performed_date,
                             viral_load_test_status,
                             treatment_end_date                  as art_end_date,
                             current_who_hiv_stage,
                             cd4_count,
                             cotrimoxazole_prophylaxis_start_dat,
                             cotrimoxazole_prophylaxis_stop_date,
                             patient_diagnosed_with_active_tuber as active_tb_dx,
                             diagnosis_date,
                             tuberculosis_drug_treatment_start_d,
                             date_active_tbrx_completed,
                             tb_prophylaxis_type                 AS TB_ProphylaxisType,
                             tb_prophylaxis_type_alternate_      AS TB_ProphylaxisTypeALT,
                             tpt_followup_6h_                       tpt_follow_up_inh,
                             why_eligible_reason_,
                             tb_diagnostic_test_result              tb_specimen_type,
                             fluconazole_start_date              AS Fluconazole_Start_Date,
                             fluconazole_stop_date               as Fluconazole_End_Date,
                             transfer_in
                      FROM mamba_flat_encounter_follow_up follow_up
                               join mamba_flat_encounter_follow_up_1 follow_up_1
                                    on follow_up.encounter_id = follow_up_1.encounter_id
                               join mamba_flat_encounter_follow_up_2 follow_up_2
                                    on follow_up.encounter_id = follow_up_2.encounter_id
                               join mamba_flat_encounter_follow_up_3 follow_up_3
                                    on follow_up.encounter_id = follow_up_3.encounter_id
                               join mamba_dim_person person on follow_up.client_id = person_id),
         tmp_tpt_type as (SELECT encounter_id,
                                 client_id,
                                 TB_ProphylaxisType                                                                                                     AS TptType,
                                 TB_ProphylaxisTypeAlt                                                                                                  AS TptTypeAlt,
                                 tpt_follow_up_inh                                                                                                      As TPTFollowup,
                                 followup_date                                                                                                          AS FollowupDate,
                                 ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.followup_date DESC , FollowUp.encounter_id DESC ) AS row_num
                          FROM FollowUp
                          where followup_date <= REPORT_END_DATE
                            and (TB_ProphylaxisType is not null OR TB_ProphylaxisTypeAlt is not null OR
                                 tpt_follow_up_inh is not null)),

         tmp_tpt_start as (select encounter_id,
                                  client_id,
                                  tpt_start_date                                                                                                          as inhprophylaxis_started_date,
                                  ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.tpt_start_date DESC , FollowUp.encounter_id DESC ) AS row_num
                           from FollowUp
                           where tpt_start_date is not null
                             and followup_date <= REPORT_END_DATE),
         tmp_tpt_completed as (select encounter_id,
                                      client_id,
                                      tpt_completed_date                                                                                                          as InhprophylaxisCompletedDate,
                                      ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.tpt_completed_date DESC , FollowUp.encounter_id DESC ) AS row_num
                               from FollowUp
                               where tpt_completed_date is not null
                                 and followup_date <= REPORT_END_DATE),

         tmp_latest_follow_up as (SELECT encounter_id,
                                         client_id,
                                         followup_date                                                                                                         AS FollowupDate,
                                         ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.followup_date DESC, FollowUp.encounter_id DESC ) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND followup_date <= REPORT_END_DATE),
         tpt_type as (select * from tmp_tpt_type where row_num = 1),
         tpt_start as (select * from tmp_tpt_start where row_num = 1),
         tpt_completed as (select * from tmp_tpt_completed where row_num = 1),
         latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),
         tmp_tpt as (SELECT f_case.encounter_id,
                            f_case.client_id,
                            f_case.weight_in_kg,
                            f_case.hiv_confirmed_date,
                            f_case.art_start_date,
                            f_case.followup_date,
                            f_case.why_eligible_reason_,
                            art_dose_days                       as artdosecode,
                            f_case.next_visit_date,
                            f_case.follow_up_status,
                            f_case.follow_up_status             as statuscode,
                            f_case.art_end_date,
                            f_case.current_who_hiv_stage        AS WHOStage,
                            cd4_count                              AdultCD4Count,
                            cd4_count                              ChildCD4Count,
                            cotrimoxazole_prophylaxis_start_dat As CPT_StartDate,
                            cotrimoxazole_prophylaxis_stop_date As CPT_StopDate,
                            tb_specimen_type                    AS TB_SpecimenType,
                            active_tb_dx                        As ActiveTBDiagnosed,
                            diagnosis_date                      As ActiveTBDignosedDate,
                            tuberculosis_drug_treatment_start_d As TBTx_StartDate,
                            date_active_tbrx_completed          As TBTx_CompletedDate,
                     FROM FollowUp AS f_case
                              INNER JOIN latest_follow_up ON f_case.encounter_id = latest_follow_up.encounter_id)

    select sex                                                 as Sex,
           tmp_tpt.weight_in_kg                                as Weight,
           TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) as Age,
           tpt_start.inhprophylaxis_started_date               as `TPT Start Date`,
           tpt_start.inhprophylaxis_started_date               as `TPT Start Date EC.`,
           tpt_completed.InhprophylaxisCompletedDate           as `TPT Completed Date`,
           tpt_completed.InhprophylaxisCompletedDate           as `TPT Completed Date EC.`,
           tpt_type.TptType                                    as `TPT Type`,
           tpt_type.TptTypeAlt                                 as `TPT Type ALT`,
           tmp_tpt.hiv_confirmed_date                          as `Hiv Confirmed Date`,
           tmp_tpt.hiv_confirmed_date                          as `Hiv Confirmed Date EC.`,
           tmp_tpt.art_start_date                              as `Art Start Date`,
           tmp_tpt.art_start_date                              as `Art Start Date EC.`,
           tmp_tpt.followup_date                               as `Follow Up Date`,
           tmp_tpt.followup_date                               as `Follow Up Date EC.`,
           Transfer_In,
           tmp_tpt.artdosecode                                 as ARTDoseDays,
           tmp_tpt.next_visit_date                             as `Next visit Date`,
           tmp_tpt.next_visit_date                             as `Next visit Date EC.`,
           tmp_tpt.follow_up_status                            as `Follow Up Status`,
           tmp_tpt.art_end_date                                as `ART Dose End Date`,
           tmp_tpt.art_end_date                                as `ART Dose End Date EC.`,
           tmp_tpt.WHOStage                                    as WHOStage,
           AdultCD4Count                                       as `Adult CD4 Count`,
           ChildCD4Count                                       as `Child CD4 Count`,
           CPT_StartDate                                       as `CPT Start Date`,
           CPT_StartDate                                       as `CPT Start Date EC.`,
           CPT_StopDate                                        as CPT_StopDate,
           CPT_StopDate                                        as `CPT_StopDate EC.`,
           TB_SpecimenType,
           ActiveTBDiagnosed,
           ActiveTBDignosedDate                                as `Active TB Dignosed Date`,
           ActiveTBDignosedDate                                as `Active TB Dignosed Date EC.`,
           TBTx_StartDate                                      as `TBT Start Date`,
           TBTx_StartDate                                      as `TBT Start Date EC.`,
           TBTx_CompletedDate                                  as `TBT Completed Date`,
           TBTx_CompletedDate                                  as `TBT Completed Date EC.`,
           Fluconazole_Start_Date                              as `Fluconazole Start Date`,
           Fluconazole_Start_Date                              as `Fluconazole Start Date EC.`,
           Fluconazole_End_Date                                as `Fluconazole End Date`,
           Fluconazole_End_Date                                as `Fluconazole End Date EC.`

    FROM FollowUp
             join tmp_tpt on tmp_tpt.encounter_id = FollowUp.encounter_id
             join mamba_dim_client client on tmp_tpt.client_id = client.client_id
             Left join tpt_start on tmp_tpt.client_id = tpt_start.client_id
             Left join tpt_completed on tmp_tpt.client_id = tpt_completed.client_id
             Left join tpt_type on tmp_tpt.client_id = tpt_type.client_id
    where tmp_tpt.art_end_date >= REPORT_END_DATE
      AND tmp_tpt.follow_up_status in ('Alive', 'Restart medication')
      AND tmp_tpt.art_start_date <= REPORT_END_DATE;
END //

DELIMITER ;