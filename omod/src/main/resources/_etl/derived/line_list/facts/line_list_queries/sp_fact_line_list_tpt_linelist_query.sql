DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tpt_linelist_query;

CREATE PROCEDURE sp_fact_line_list_tpt_linelist_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN REPORT_TYPE VARCHAR(50)
)
BEGIN
    DECLARE treatment_type_condition VARCHAR(200);
    DECLARE tpt_query TEXT;

    IF REPORT_TYPE = 'ALL' THEN
        SET treatment_type_condition =
                ' tmp_tpt.cpt_start_date is not null  or tmp_tpt.fpt_start_date is not null  or tmp_tpt.tpt_start_date is not null';
    ELSEIF REPORT_TYPE = 'CPT' THEN
        SET treatment_type_condition = ' tmp_tpt.cpt_start_date is not null';
    ELSEIF REPORT_TYPE = 'FPT' THEN
        SET treatment_type_condition = ' tmp_tpt.fpt_start_date is not null';
    ELSEIF REPORT_TYPE = 'TPT' THEN
        SET treatment_type_condition = ' tmp_tpt.tpt_start_date is not null';
    END IF;

    SET tpt_query = 'WITH FollowUp AS (SELECT follow_up.encounter_id,
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
                             cotrimoxazole_prophylaxis_start_dat as cpt_start_date,
                             cotrimoxazole_prophylaxis_stop_date as cpt_stop_date,
                             patient_diagnosed_with_active_tuber as active_tb_dx,
                             diagnosis_date,
                             tuberculosis_drug_treatment_start_d,
                             date_active_tbrx_completed,
                             tb_prophylaxis_type                 AS TB_ProphylaxisType,
                             tb_prophylaxis_type_alternate_      AS TB_ProphylaxisTypeALT,
                             tpt_followup_6h_                       tpt_follow_up_inh,
                             why_eligible_reason_,
                             tb_diagnostic_test_result              tb_specimen_type,
                             fluconazole_start_date              AS fpt_start_date,
                             fluconazole_stop_date               as fpt_stop_date,
                             transfer_in
                      FROM mamba_flat_encounter_follow_up follow_up
                               left join mamba_flat_encounter_follow_up_1 follow_up_1
                                    on follow_up.encounter_id = follow_up_1.encounter_id
                               left join mamba_flat_encounter_follow_up_2 follow_up_2
                                    on follow_up.encounter_id = follow_up_2.encounter_id
                               left join mamba_flat_encounter_follow_up_3 follow_up_3
                                    on follow_up.encounter_id = follow_up_3.encounter_id
                               left join mamba_dim_person person on follow_up.client_id = person_id),

         tmp_tpt_start as (select encounter_id,
                                  client_id,
                                  tpt_start_date,
                                  ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.tpt_start_date DESC , FollowUp.encounter_id DESC ) AS row_num
                           from FollowUp
                           where tpt_start_date is not null
                             and tpt_start_date between ? and ?
                             and followup_date <= ?),
         tmp_tpt_completed as (select encounter_id,
                                      client_id,
                                      tpt_completed_date,
                                      ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.tpt_completed_date DESC , FollowUp.encounter_id DESC ) AS row_num
                               from FollowUp
                               where tpt_completed_date is not null
                                 and tpt_completed_date between ? and ?
                                 and followup_date <= ?),
         -- CPT
         tmp_cpt_start as (select encounter_id,
                                  client_id,
                                  cpt_start_date,
                                  ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.cpt_start_date DESC , FollowUp.encounter_id DESC ) AS row_num
                           from FollowUp
                           where cpt_start_date is not null
                             and cpt_start_date between ? and ?
                             and followup_date <= ?),
         tmp_cpt_completed as (select encounter_id,
                                      client_id,
                                      cpt_stop_date,
                                      ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.cpt_stop_date DESC , FollowUp.encounter_id DESC ) AS row_num
                               from FollowUp
                               where cpt_stop_date is not null
                                 and cpt_stop_date between ? and ?
                                 and followup_date <= ?),

         -- FPT
         tmp_fpt_start as (select encounter_id,
                                  client_id,
                                  fpt_start_date,
                                  ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.fpt_start_date DESC , FollowUp.encounter_id DESC ) AS row_num
                           from FollowUp
                           where fpt_start_date is not null
                             and fpt_start_date between ? and ?
                             and followup_date <= ?),
         tmp_fpt_completed as (select encounter_id,
                                      client_id,
                                      fpt_stop_date,
                                      ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.fpt_stop_date DESC , FollowUp.encounter_id DESC ) AS row_num
                               from FollowUp
                               where fpt_stop_date is not null
                                 and fpt_stop_date between ? and ?
                                 and followup_date <= ?),


         tmp_latest_follow_up as (SELECT encounter_id,
                                         client_id,
                                         followup_date                                                                                                         AS FollowupDate,
                                         ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.followup_date DESC, FollowUp.encounter_id DESC ) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    and hiv_confirmed_date is not null
                                    AND followup_date <= ?),
         tpt_start as (select * from tmp_tpt_start where row_num = 1),
         tpt_completed as (select * from tmp_tpt_completed where row_num = 1),
         fpt_start as (select * from tmp_fpt_start where row_num = 1),
         fpt_completed as (select * from tmp_fpt_completed where row_num = 1),
         cpt_start as (select * from tmp_cpt_start where row_num = 1),
         cpt_completed as (select * from tmp_cpt_completed where row_num = 1),


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
                            tb_specimen_type                    AS TB_SpecimenType,
                            active_tb_dx                        As ActiveTBDiagnosed,
                            diagnosis_date                      As ActiveTBDignosedDate,
                            tuberculosis_drug_treatment_start_d As TBTx_StartDate,
                            date_active_tbrx_completed          As TBTx_CompletedDate,
                            tpt_start.tpt_start_date,
                            tpt_completed.tpt_completed_date,
                            fpt_start.fpt_start_date,
                            fpt_completed.fpt_stop_date,
                            cpt_start.cpt_start_date,
                            cpt_completed.cpt_stop_date
                     FROM latest_follow_up
                              INNER JOIN FollowUp f_case ON f_case.encounter_id = latest_follow_up.encounter_id
                              Left join tpt_start on latest_follow_up.client_id = tpt_start.client_id
                              Left join tpt_completed on latest_follow_up.client_id = tpt_completed.client_id
                              Left join cpt_start on latest_follow_up.client_id = cpt_start.client_id
                              Left join cpt_completed on latest_follow_up.client_id = cpt_start.client_id
                              Left join fpt_start on latest_follow_up.client_id = fpt_start.client_id
                              Left join fpt_completed on latest_follow_up.client_id = fpt_completed.client_id)

    select patient_name                                        as `Patient Name`,
           sex                                                 as Sex,
           tmp_tpt.weight_in_kg                                as Weight,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as Age,
           tmp_tpt.tpt_start_date                            as `TPT Start Date`,
           tmp_tpt.tpt_start_date                            as `TPT Start Date EC.`,
           tmp_tpt.tpt_completed_date                    as `TPT Completed Date`,
           tmp_tpt.tpt_completed_date                    as `TPT Completed Date EC.`,
           FollowUp.TB_ProphylaxisType                         as `TPT Type`,
           FollowUp.TB_ProphylaxisTypeALT                      as `TPT Type ALT`,
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
           tmp_tpt.cpt_start_date                              as `CPT Start Date`,
           tmp_tpt.cpt_start_date                              as `CPT Start Date EC.`,
           tmp_tpt.cpt_stop_date                               as CPT_StopDate,
           tmp_tpt.cpt_stop_date                               as `CPT_StopDate EC.`,
           TB_SpecimenType,
           ActiveTBDiagnosed,
           ActiveTBDignosedDate                                as `Active TB Dignosed Date`,
           ActiveTBDignosedDate                                as `Active TB Dignosed Date EC.`,
           TBTx_StartDate                                      as `TBT Start Date`,
           TBTx_StartDate                                      as `TBT Start Date EC.`,
           TBTx_CompletedDate                                  as `TBT Completed Date`,
           TBTx_CompletedDate                                  as `TBT Completed Date EC.`,
           tmp_tpt.fpt_start_date                              as `Fluconazole Start Date`,
           tmp_tpt.fpt_start_date                              as `Fluconazole Start Date EC.`,
           tmp_tpt.fpt_stop_date                               as `Fluconazole End Date`,
           tmp_tpt.fpt_stop_date                               as `Fluconazole End Date EC.`

    FROM FollowUp
             join tmp_tpt on tmp_tpt.encounter_id = FollowUp.encounter_id
             join mamba_dim_client client on tmp_tpt.client_id = client.client_id where ';

    SET @sql = CONCAT(tpt_query, treatment_type_condition);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @start_date, @end_date, @end_date, @start_date, @end_date, @end_date, @start_date, @end_date, @end_date, @start_date, @end_date , @end_date, @start_date, @end_date
        , @end_date, @start_date, @end_date, @end_date, @end_date, @end_date;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;