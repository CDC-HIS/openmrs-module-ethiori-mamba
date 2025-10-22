DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ahd_query;

CREATE PROCEDURE sp_fact_line_list_ahd_query(IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             date_started_on_tuberculosis_prophy AS tpt_start_date,
                             date_completed_tuberculosis_prophyl AS tpt_completed_date,
                             tb_prophylaxis_type                 AS tpt_type,
                             tpt_dispensed_dose_in_days_alternat AS TPT_DoseDaysNumberALT,
                             tpt_side_effects                    AS TPT_SideEffect,
                             diagnostic_test                     AS DiagnosticTest,
                             tb_diagnostic_test_result           AS DiagnosticTestResult,
                             lf_lam_result                       AS LF_LAM_result,
                             gene_xpert_result                   AS Gene_Xpert_result,
                             tuberculosis_drug_treatment_start_d AS activetbtreatmentStartDate,
                             tpt_dispensed_dose_in_days_inh_     AS TPT_DoseDaysNumberINH,
                             was_the_patient_screened_for_tuberc AS tb_screened,
                             screening_test_result_tuberculosis  AS tb_screening,
                             Adherence                           AS TPT_Adherance,
                             date_discontinued_tuberculosis_prop AS tpt_discontinued_date,
                             date_active_tbrx_completed          AS ActiveTBTreatmentCompletedDate,
                             date_active_tbrx_dc                 AS activetbtreatmentDisContinuedDate,
                             cervical_cancer_screening_status    AS CCS_ScreenDoneYes,
                             date_of_reported_hiv_viral_load     AS viral_load_sent_date,
                             date_viral_load_results_received    AS viral_load_perform_date,
                             viral_load_test_status,
                             hiv_viral_load                      AS viral_load_count,
                             hiv_viral_load_status,
                             viral_load_test_indication,
                             treatment_end_date,
                             weight_text_                        AS Weight,
                             height,
                             date_of_event                       AS date_hiv_confirmed,
                             current_who_hiv_stage,
                             cd4_count,
                             antiretroviral_art_dispensed_dose_i AS art_dose_days,
                             regimen,
                             anitiretroviral_adherence_level     as adherence,
                             pregnancy_status,
                             method_of_family_planning,
                             crag,
                             cotrimoxazole_prophylaxis_start_dat,
                             cotrimoxazole_prophylaxis_stop_date,
                             current_functional_status,
                             patient_diagnosed_with_active_tuber,
                             fluconazole_start_date              AS Fluconazole_Start_Date,
                             weight_for_age_status               AS NSLessthanFive,
                             nutritional_status_of_older_child_a AS NSAdolescent,
                             nutritional_status_of_adult         AS ns_adult,
                             are_there_any_ois_                  AS No_OI,
                             herpes_zoster                       AS Zoster,
                             bacterial_pneumonia                 AS Bacterial_Pneumonia,
                             extra_pulmonary_tuberculosis_tb     AS Extra_Pulmonary_TB,
                             candidiasis_of_the_esophagus        AS Oesophageal_Candidiasis,
                             candidiasis_vaginal                 AS Vaginal_Candidiasis,
                             mouth_ulcer                         AS Mouth_Ulcer,
                             diarrhea_chronic                    AS Chronic_Diarrhea,
                             acute_diarrhea                      AS Acute_Diarrhea,
                             toxoplasmosis                       AS CNS_Toxoplasmosis,
                             meningitis_cryptococcal             AS Cryptococcal_Meningitis,
                             kaposi_sarcoma_oral                 AS Kaposi_Sarcoma,
                             suspected_cervical_cancer           AS Cervical_Cancer,
                             pulmonary_tuberculosis_tb           AS Pulmonary_TB,
                             candidiasis_oral                    AS Oral_Candidiasis,
                             pneumocystis_carinii_pneumonia_pcp  AS Pneumocystis_Pneumonia,
                             malignant_lymphoma_nonhodgkins      AS NonHodgkins_Lymphoma,
                             female_genital_ulcer_disease        AS Genital_Ulcer,
                             other_opportunistic_illnesses       AS OI_Other,
                             fluconazole_stop_date               as Fluconazole_End_Date,
                             nutritional_screening_result,
                             dsd_category,
                             other_medications_med_1                Med1,
                             other_medications_med2                 Med2,
                             COALESCE(
                                     at_3436_weeks_of_gestation,
                                     viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                     viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                     every_six_months_until_mtct_ends,
                                     six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                     three_months_after_delivery,
                                     at_the_first_antenatal_care_visit,
                                     annual_viral_load_test,
                                     second_viral_load_test_at_12_months_post_art,
                                     first_viral_load_test_at_6_months_or_longer_post_art,
                                     first_viral_load_test_at_3_months_or_longer_post_art
                             )                                   AS routine_viral_load_test_indication,
                             COALESCE(
                                     repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                     suspected_antiretroviral_failure
                             )                                   AS targeted_viral_load_test_indication
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
                                         ON follow_up.encounter_id = follow_up_9.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_10 follow_up_10
                                         ON follow_up.encounter_id = follow_up_10.encounter_id
                      ),
         tmp_latest_follow_up AS (SELECT client_id,
                                         follow_up_date,
                                         follow_up_status,
                                         encounter_id,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         latest_follow_up AS (select *
                              from tmp_latest_follow_up
                              where row_num = 1
                                and follow_up_status not in ('Dead', 'Transferred out')),
         tmp_vl_performed_date as (select encounter_id,
                                          client_id,
                                          viral_load_perform_date,
                                          viral_load_sent_date,
                                          follow_up_date,
                                          viral_load_test_status,
                                          viral_load_test_indication,
                                          routine_viral_load_test_indication,
                                          targeted_viral_load_test_indication,
                                          viral_load_count,
                                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC , FollowUp.encounter_id DESC) AS row_num
                                   from FollowUp
                                   where hiv_viral_load_status = 'Completed'
                                     and viral_load_perform_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         vl_performed_date as (select *
                               from tmp_vl_performed_date
                               where row_num = 1
                                 and (
                                   (viral_load_test_indication = 'Routine viral load test indication'
                                       and routine_viral_load_test_indication in
                                           ('First viral load test at 6 months or longer post ART',
                                            'Second viral load test at 12 months post ART',
                                            'Annual viral load test',
                                            'First viral load test at 3 months or longer post ART',
                                            'At the first antenatal care visit',
                                            'At 34-36 weeks of gestation'
                                                'Three months after delivery',
                                            'Six months after the first viral load test at postnatal period',
                                            'Every six months until MTCT ends'))
                                       OR (viral_load_test_indication = 'Targeted viral load test indication'
                                       and targeted_viral_load_test_indication in
                                           ('Suspected Antiretroviral failure')
                                       and (viral_load_count > 50 or viral_load_test_status in ('Uncontrolled',
                                                                                                'HIV infection with high viral load',
                                                                                                'Low-level viremia'))
                                       )
                                   )),

         tmp_tpt_start AS (SELECT client_id,
                                  tpt_start_date,
                                  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY tpt_start_date DESC, encounter_id DESC) AS row_num
                           FROM FollowUp
                           WHERE tpt_start_date IS NOT NULL
                             AND tpt_start_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         tpt_start as (select * from tmp_tpt_start where row_num = 1),

         tmp_tpt_completed AS (SELECT client_id,
                                      tpt_completed_date,
                                      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY tpt_completed_date DESC, encounter_id DESC) AS row_num
                               FROM FollowUp
                               WHERE tpt_completed_date IS NOT NULL
                                 AND tpt_completed_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         tpt_completed as (select *
                           from tmp_tpt_completed
                           where row_num = 1),

         tmp_tpt_type AS (SELECT client_id,
                                 tpt_type                                                                                   AS TB_ProphylaxisType,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                          FROM FollowUp
                          WHERE tpt_type IS NOT NULL),
         tpt_type as (select * from tmp_tpt_type where row_num = 1),

         tmp_tpt_dose_ALT AS (SELECT client_id,
                                     TPT_DoseDaysNumberALT                                                                      AS TPT_DoseDaysNumberALT,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE TPT_DoseDaysNumberALT IS NOT NULL),
         tpt_dose_ALT as (select * from tmp_tpt_dose_ALT where row_num = 1),

         tmp_tpt_dose_INH AS (SELECT client_id,
                                     TPT_DoseDaysNumberINH                                                                      AS TPT_DoseDaysNumberINH,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE TPT_DoseDaysNumberINH IS NOT NULL),
         tpt_dose_INH as (select * from tmp_tpt_dose_INH where row_num = 1),

         tmp_tpt_side_effect AS (SELECT client_id,
                                        TPT_SideEffect                                                                             AS TPT_SideEffect,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                 FROM FollowUp
                                 WHERE TPT_SideEffect IS NOT NULL),
         tpt_side_effect as (select * from tmp_tpt_side_effect where row_num = 1),

         tmp_tb_diagnostic_test AS (SELECT client_id,
                                           DiagnosticTest                                                                             AS TB_Diagnostic_Test,
                                           ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                    FROM FollowUp
                                    WHERE DiagnosticTest IS NOT NULL
                                      AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         tb_diagnostic_test as (select * from tmp_tb_diagnostic_test where row_num = 1),

         tmp_tb_diagnostic_result AS (SELECT client_id,
                                             DiagnosticTestResult                                                                       AS TB_Diagnostic_Result,
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                      FROM FollowUp
                                      WHERE DiagnosticTestResult IS NOT NULL
                                        AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         tb_diagnostic_result as (select * from tmp_tb_diagnostic_result where row_num = 1),

         tmp_tb_LF_LAM_result AS (SELECT client_id,
                                         LF_LAM_result                                                                              AS LF_LAM_result,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE LF_LAM_result IS NOT NULL
                                    AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         tb_LF_LAM_result as (select * from tmp_tb_LF_LAM_result where row_num = 1),
         tmp_tb_Gene_Xpert_result AS (SELECT client_id,
                                             Gene_Xpert_result                                                                          AS Gene_Xpert_result,
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                      FROM FollowUp
                                      WHERE Gene_Xpert_result IS NOT NULL
                                        AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         tb_Gene_Xpert_result as (select * from tmp_tb_Gene_Xpert_result where row_num = 1),

         tmp_tpt_screened AS (SELECT client_id,
                                     tb_screened                                                                                AS TB_Screened,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE tb_screened = 'Yes'),
         tpt_screened as (select * from tmp_tpt_screened where row_num = 1),
         tmp_tpt_screening AS (SELECT client_id,
                                      encounter_id,
                                      tb_screening                                                                               AS TB_Screening_Result,
                                      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                               FROM FollowUp
                               WHERE tb_screening IS NOT NULL
                                 AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         tpt_screening as (select * from tmp_tpt_screening where row_num = 1),
         tmp_tpt_adherence AS (SELECT client_id,
                                      TPT_Adherance                                                                              AS TPT_Adherence,
                                      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                               FROM FollowUp
                               WHERE TPT_Adherance IS NOT NULL),
         tpt_adherence as (select * from tmp_tpt_adherence where row_num = 1),
         tmp_ActiveTBTreatmentStarted AS (SELECT client_id,
                                                 activetbtreatmentStartDate                                                                             AS ActiveTBTreatmentStartDate,
                                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY activetbtreatmentStartDate DESC, encounter_id DESC) AS row_num
                                          FROM FollowUp
                                          WHERE activetbtreatmentStartDate IS NOT NULL
                                            AND activetbtreatmentStartDate <= COALESCE(REPORT_END_DATE, CURDATE())),
         ActiveTBTreatmentStarted as (select * from tmp_ActiveTBTreatmentStarted where row_num = 1),
         tmp_TBTreatmentCompleted AS (SELECT client_id,
                                             ActiveTBTreatmentCompletedDate                                                                             AS ActiveTBTreatmentCompletedDate,
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ActiveTBTreatmentCompletedDate DESC, encounter_id DESC) AS row_num
                                      FROM FollowUp
                                      WHERE ActiveTBTreatmentCompletedDate IS NOT NULL
                                        AND ActiveTBTreatmentCompletedDate <= COALESCE(REPORT_END_DATE, CURDATE())),
         TBTreatmentCompleted as (select * from tmp_TBTreatmentCompleted where row_num = 1),
         tmp_TBTreatmentDiscontinued AS (SELECT client_id,
                                                activetbtreatmentDisContinuedDate                                                                             AS ActiveTBTreatmentDiscontinuedDate,
                                                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY activetbtreatmentDisContinuedDate DESC, encounter_id DESC) AS row_num
                                         FROM FollowUp
                                         WHERE activetbtreatmentDisContinuedDate IS NOT NULL
                                           AND
                                             activetbtreatmentDisContinuedDate <= COALESCE(REPORT_END_DATE, CURDATE())),
         TBTreatmentDiscontinued as (select * from tmp_TBTreatmentDiscontinued where row_num = 1),
         tmp_cca_screened_tmp AS (SELECT DISTINCT client_id,
                                                  CCS_ScreenDoneYes                                                                          AS CCA_Screened,
                                                  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  where CCS_ScreenDoneYes IS NOT NULL),
         cca_screened AS (select * from tmp_cca_screened_tmp where row_num = 1)
    SELECT client.patient_name                                                             as `Patient Name`,
           client.patient_uuid                                                             as UUID,
           CAST(client.mrn AS CHAR(20))                                                    as mrn,
           client.uan,
           client.sex                                                                      as Sex,
           f_case.Weight                                                                   as Weight,
           TIMESTAMPDIFF(YEAR, client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) as Age,
           f_case.height                                                                   as Height,
           f_case.date_hiv_confirmed                                                       as HIV_Confirmed_Date,
           f_case.art_start_date                                                           as ARTStartDate,
           PERIOD_DIFF(date_format(COALESCE(REPORT_END_DATE, CURDATE()), '%Y%m'),
                       date_format(f_case.art_start_date, '%Y%m'))                         as MonthsOnART,
           f_case.follow_up_date                                                           as FollowUpDate,
           f_case.current_who_hiv_stage                                                    as WHOStage,
           f_case.cd4_count                                                                as CD4Count,
           f_case.art_dose_days                                                            as ARTDoseDays,
           f_case.regimen                                                                  as ARVRegimen,
           CASE f_case.follow_up_status
               WHEN 'Alive' THEN 'Alive on ART'
               WHEN 'Restart medication' THEN 'Restart'
               WHEN 'Transferred out' THEN 'TO'
               WHEN 'Stop all' THEN 'Stop'
               WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
               WHEN 'Ran away' THEN 'Drop'
               END                                                                         AS FollowupStatus,
           f_case.adherence                                                                as AdheranceLevel,
           f_case.pregnancy_status                                                         as IsPregnant,
           f_case.method_of_family_planning                                                as FpMethodUsed,
           f_case.crag                                                                     as CrAg,
           COALESCE(
                   f_case.ns_adult,
                   f_case.NSAdolescent,
                   f_case.NSLessthanFive
           )                                                                               as NutritionalStatus,
           f_case.current_functional_status                                                as FunctionalStatus,
           CONCAT_WS(
                   ', ',
                   NULLIF(f_case.No_OI, ''),
                   NULLIF(f_case.Zoster, ''),
                   NULLIF(f_case.Bacterial_Pneumonia, ''),
                   NULLIF(f_case.Extra_Pulmonary_TB, ''),
                   NULLIF(f_case.Oesophageal_Candidiasis, ''),
                   NULLIF(f_case.Vaginal_Candidiasis, ''),
                   NULLIF(f_case.Mouth_Ulcer, ''),
                   NULLIF(f_case.Chronic_Diarrhea, ''),
                   NULLIF(f_case.Acute_Diarrhea, ''),
                   NULLIF(f_case.CNS_Toxoplasmosis, ''),
                   NULLIF(f_case.Cryptococcal_Meningitis, ''),
                   NULLIF(f_case.Kaposi_Sarcoma, ''),
                   NULLIF(f_case.Cervical_Cancer, ''),
                   NULLIF(f_case.Pulmonary_TB, ''),
                   NULLIF(f_case.Oral_Candidiasis, ''),
                   NULLIF(f_case.Pneumocystis_Pneumonia, ''),
                   NULLIF(f_case.NonHodgkins_Lymphoma, ''),
                   NULLIF(f_case.Genital_Ulcer, ''),
                   NULLIF(f_case.OI_Other, '')
           )                                                                               AS OIs,
           f_case.Med1                                                                     as Med1,
           f_case.Med2                                                                     as Med2,
           f_case.cotrimoxazole_prophylaxis_start_dat                                      as CotrimoxazoleStartDate,
           f_case.cotrimoxazole_prophylaxis_stop_date                                         cortimoxazole_stop_date,
           f_case.Fluconazole_Start_Date                                                   as Fluconazole_Start_Date,
           f_case.Fluconazole_End_Date                                                     as Fluconazole_End_Date,
           tpt_type.TB_ProphylaxisType                                                     as TPT_Type,
           tpt_start.tpt_start_date                                                        as inhprophylaxis_started_date,
           tpt_completed.tpt_completed_date                                                as InhprophylaxisCompletedDate,
           tpt_dose_ALT.TPT_DoseDaysNumberALT                                              as TPT_DoseDaysNumberALT,
           tpt_dose_INH.TPT_DoseDaysNumberINH                                              as TPT_DoseDaysNumberINH,
           COALESCE(tpt_dose_INH.TPT_DoseDaysNumberINH,
                    tpt_dose_ALT.TPT_DoseDaysNumberALT)                                    AS TPT_Dispensed_Dose,
           tpt_side_effect.TPT_SideEffect                                                  as TPT_SideEffect,
           tpt_adherence.TPT_Adherence                                                     as TPT_Adherence,
           tpt_screened.TB_Screened                                                        as tb_screened,
           tpt_screening.TB_Screening_Result                                               as tb_screening_result,
           tb_diagnostic_result.TB_Diagnostic_Result                                       as TB_Diagnostic_Result,
           tb_LF_LAM_result.LF_LAM_result                                                  as LF_LAM_result,
           tb_Gene_Xpert_result.Gene_Xpert_result                                          as Gene_Xpert_result,
           CASE
               WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Smear microscopy only' AND
                    tb_diagnostic_result.TB_Diagnostic_Result = 'Positive'
                   THEN 'Positive'
               WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Smear microscopy only' AND
                    tb_diagnostic_result.TB_Diagnostic_Result = 'Negative'
                   THEN 'Negative'
               ELSE '' END                                                                 AS Smear_Microscopy_Result,
           CASE
               WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Additional test other than Gene-Xpert' AND
                    tb_diagnostic_result.TB_Diagnostic_Result = 'Positive'
                   THEN 'Positive'
               WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Additional test other than Gene-Xpert' AND
                    tb_diagnostic_result.TB_Diagnostic_Result = 'Negative'
                   THEN 'Negative'
               ELSE '' END                                                                 AS Additional_TB_Diagnostic_Test_Result,
           f_case.patient_diagnosed_with_active_tuber                                      as Active_TB,
           ActiveTBTreatmentStarted.ActiveTBTreatmentStartDate                             as ActiveTBTreatmentStartDate,
           TBTreatmentCompleted.ActiveTBTreatmentCompletedDate                             as ActiveTBTreatmentCompletedDate,
           TBTreatmentDiscontinued.ActiveTBTreatmentDiscontinuedDate                       as ActiveTBTreatmentDiscontinuedDate,
           vlperfdate.viral_load_perform_date                                              as Viral_Load_Perform_Date,
           vlperfdate.viral_load_test_status                                               as Viral_Load_Status,
           vlperfdate.viral_load_count                                                     as Viral_Load_count,
           vlperfdate.viral_load_sent_date                                                 as VL_Sent_Date,
           vlperfdate.viral_load_perform_date                                              as Viral_Load_Ref_Date,
           cca_screened.CCA_Screened                                                       as CCA_Screened,
           f_case.dsd_category                                                             as DSD_Category,
           CASE
               WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) < 5 THEN 'Yes'
               WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) >= 5 AND
                    f_case.cd4_count IS NOT NULL AND
                    f_case.cd4_count < 200 THEN 'Yes'
               WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) >= 5 AND
                    f_case.current_who_hiv_stage IS NOT NULL AND
                    (f_case.current_who_hiv_stage = 'WHO stage 3 adult' Or
                     f_case.current_who_hiv_stage = 'WHO stage 3 peds' Or
                     f_case.current_who_hiv_stage = 'WHO stage 4 peds') THEN 'Yes'
               WHEN (TIMESTAMPDIFF(YEAR, client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) >= 5 AND
                     f_case.current_who_hiv_stage IS NOT NULL AND
                     f_case.current_who_hiv_stage = 'WHO stage 4 adult') THEN 'Yes'
               ELSE 'No' END                                                               as AHD,
           f_case.follow_up_status                                                         as current_status
    FROM FollowUp AS f_case
             INNER JOIN latest_follow_up ON f_case.encounter_id = latest_follow_up.encounter_id
             LEFT JOIN mamba_dim_client client on latest_follow_up.client_id = client.client_id
             LEFT JOIN vl_performed_date AS vlperfdate ON vlperfdate.client_id = f_case.client_id
             LEFT JOIN tpt_start ON tpt_start.client_id = f_case.client_id
             LEFT JOIN tpt_completed ON tpt_completed.client_id = f_case.client_id
             LEFT JOIN tpt_type ON tpt_type.client_id = f_case.client_id
             LEFT JOIN tpt_dose_ALT ON tpt_dose_ALT.client_id = f_case.client_id
             LEFT JOIN tpt_dose_INH ON tpt_dose_INH.client_id = f_case.client_id
             LEFT JOIN tpt_side_effect ON tpt_side_effect.client_id = f_case.client_id
             LEFT JOIN
         tpt_screened ON tpt_screened.client_id = f_case.client_id
             LEFT JOIN tpt_screening ON tpt_screening.client_id = f_case.client_id
             LEFT JOIN tpt_adherence ON tpt_adherence.client_id = f_case.client_id
             LEFT JOIN tb_diagnostic_result ON tb_diagnostic_result.client_id = f_case.client_id
             LEFT JOIN tb_diagnostic_test ON tb_diagnostic_test.client_id = f_case.client_id
             LEFT JOIN tb_LF_LAM_result ON tb_LF_LAM_result.client_id = f_case.client_id
             LEFT JOIN tb_Gene_Xpert_result ON tb_Gene_Xpert_result.client_id = f_case.client_id
             LEFT JOIN ActiveTBTreatmentStarted ON ActiveTBTreatmentStarted.client_id = f_case.client_id
             LEFT JOIN TBTreatmentCompleted ON TBTreatmentCompleted.client_id = f_case.client_id
             LEFT JOIN TBTreatmentDiscontinued ON TBTreatmentDiscontinued.client_id = f_case.client_id
             LEFT JOIN cca_screened ON cca_screened.client_id = f_case.client_id;

END //

DELIMITER ;
