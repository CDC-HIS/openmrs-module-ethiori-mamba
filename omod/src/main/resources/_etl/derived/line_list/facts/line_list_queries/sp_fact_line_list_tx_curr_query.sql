DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_curr_query;

CREATE PROCEDURE sp_fact_line_list_tx_curr_query(IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id                       AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_                  AS follow_up_date,
                             art_antiretroviral_start_date AS art_start_date,
                             assessment_date,
                             treatment_end_date,
                             antiretroviral_art_dispensed_dose_i       AS ARTDoseDays,
                             weight_text_                              AS Weight,
                             screening_test_result_tuberculosis        AS TB_SreeningResult,
                             date_of_last_menstrual_period_lmp_           LMP_Date,
                             anitiretroviral_adherence_level           AS AdherenceLevel,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child                breast_feeding_status,
                             pregnancy_status,
                             diagnosis_date                            AS ActiveTBDiagnoseddate,
                             nutritional_status_of_adult,
                             nutritional_supplements_provided,
                             stages_of_disclosure,
                             date_started_on_tuberculosis_prophy,
                             method_of_family_planning,
                             patient_diagnosed_with_active_tuber       as ActiveTBDiagnosed,
                             dsd_category,
                             nutritional_screening_result,
                             cd4_count,
                             date_of_event                             as hiv_confirmed_date,
                             date_started_on_tuberculosis_prophy       AS inhprophylaxis_started_date,
                             date_completed_tuberculosis_prophyl       AS InhprophylaxisCompletedDate,
                             date_active_tbrx_completed,
                             date_of_reported_hiv_viral_load           as viral_load_sent_date,
                             date_viral_load_results_received          AS viral_load_perform_date,
                             viral_load_test_status
                      FROM mamba_flat_encounter_follow_up follow_up
                               JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               ),
         -- TX curr
         tx_curr_all AS (SELECT PatientId,
                                follow_up_date                                                                             AS FollowupDate,
                                encounter_id,
                                treatment_end_date,
                                follow_up_status,
                                ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                         FROM FollowUp
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date IS NOT NULL
                           AND follow_up_date <= REPORT_END_DATE),
         latestDSD_tmp AS (SELECT PatientId,
                                  assessment_date                                                                              AS latestDsdDate,
                                  encounter_id,
                                  dsd_category,
                                  ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY assessment_date DESC, encounter_id DESC ) AS row_num
                           FROM FollowUp
                           WHERE assessment_date IS NOT NULL
                             AND assessment_date <= REPORT_END_DATE),
         tmp_tpt_start as (select encounter_id,
                                  PatientId,
                                  inhprophylaxis_started_date                                                                                                          as inhprophylaxis_started_date,
                                  ROW_NUMBER() OVER (PARTITION BY FollowUp.PatientId ORDER BY FollowUp.inhprophylaxis_started_date DESC , FollowUp.encounter_id DESC ) AS row_num
                           from FollowUp
                           where inhprophylaxis_started_date is not null
                             and follow_up_date <= REPORT_END_DATE),
         tmp_tpt_completed as (select encounter_id,
                                      PatientId,
                                      InhprophylaxisCompletedDate                                                                                                          as InhprophylaxisCompletedDate,
                                      ROW_NUMBER() OVER (PARTITION BY FollowUp.PatientId ORDER BY FollowUp.InhprophylaxisCompletedDate DESC , FollowUp.encounter_id DESC ) AS row_num
                               from FollowUp
                               where InhprophylaxisCompletedDate is not null
                                 and follow_up_date <= REPORT_END_DATE),
         tmp_vl_sent_date as (SELECT PatientId,
                                     encounter_id,
                                     viral_load_sent_date                                                                             as VL_Sent_Date,
                                     ROW_NUMBER() over (PARTITION BY PatientId ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_date <= REPORT_END_DATE),
         tmp_vl_performed_date as (SELECT PatientId,
                                          encounter_id,
                                          viral_load_perform_date,
                                          viral_load_test_status,
                                          ROW_NUMBER() over (PARTITION BY PatientId ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                                   FROM FollowUp
                                   WHERE follow_up_date <= REPORT_END_DATE),
         vl_sent_date as (select * from tmp_vl_sent_date where row_num = 1),
         latestDSD AS (select * from latestDSD_tmp where row_num = 1),
         tx_curr AS (select * from tx_curr_all where row_num = 1),
         tpt_start as (select * from tmp_tpt_start where row_num = 1),
         tpt_completed as (select * from tmp_tpt_completed where row_num = 1),
         vl_performed_date as (select * from tmp_vl_performed_date where row_num = 1)
    select client.patient_name                                                                          as 'Patient Name',
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, art_start_date)                                           as 'Age at Enrollment',
           TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)                                             as 'Current Age',
           Sex,
           Weight,
           cd4_count                                                                                    as CD4,
           fn_gregorian_to_ethiopian_calendar(hiv_confirmed_date, 'Y/M/D')                              as 'HIV Confirmed Date in E.C',
           fn_gregorian_to_ethiopian_calendar(art_start_date, 'Y/M/D')                                  as 'ART Start Date in E.C',
           TB_SreeningResult                                                                            as 'TB Screening Result',
           fn_gregorian_to_ethiopian_calendar(follow_up_date, 'Y/M/D')                                  as 'Follow-up Date in E.C',
           tx_curr.follow_up_status                                                   as 'Follow-up Status',
           Regimen,
           ARTDoseDays                                                                as 'ARV Dose Days',
           AdherenceLevel                                                             as Adherence,
           latestDSD.dsd_category                                                     as 'DSD Category',
           nutritional_screening_result                                               as 'Nutritional Status',
           nutritional_supplements_provided                                           as 'Therapeutic/ Supplementary Food',
           method_of_family_planning                                                  as 'Familiy Planning Method',
           pregnancy_status                                                           as 'Pregnant?',
           breast_feeding_status                                                      as 'Breastfeeding?',
           IF(breast_feeding_status = 'Yes' or pregnancy_status = 'Yes', 'Yes', 'No') as 'On PMTCT?',
           tpt_start.inhprophylaxis_started_date                                      as 'TPT Start Date',
           tpt_completed.InhprophylaxisCompletedDate                                  as 'TPT Completed Date',
           date_active_tbrx_completed                                                 as 'TB Treatment Completed Date',
           vl_sent_date.VL_Sent_Date                                                  as 'VL Sent Date',
           vl_performed_date.viral_load_test_status                                   as 'VL Status',
           next_visit_date                                                               'Next Visit Date in E.C',
           tx_curr.treatment_end_date                                                    'Treatment End Date in E.C.',
           client.mobile_no                                                           as 'Mobile No.'
    from FollowUp
             inner join tx_curr on FollowUp.encounter_id = tx_curr.encounter_id
             left join latestDSD on latestDSD.PatientId = tx_curr.PatientId
             left join tpt_start on tx_curr.PatientId = tpt_start.PatientId
             left join tpt_completed on tx_curr.PatientId = tpt_completed.PatientId
             left join vl_sent_date on tx_curr.PatientId = vl_sent_date.PatientId
             left join vl_performed_date on tx_curr.PatientId = vl_performed_date.PatientId
             left join mamba_dim_client client on tx_curr.PatientId = client.client_id
    where tx_curr.treatment_end_date >= REPORT_END_DATE
      AND tx_curr.follow_up_status in ('Alive', 'Restart medication')
      and TIMESTAMPDIFF(DAY, art_start_date, REPORT_END_DATE) >= 0 order by client.patient_name;
END //

DELIMITER ;