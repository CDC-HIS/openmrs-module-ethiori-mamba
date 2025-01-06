DELIMITER
//

DROP PROCEDURE IF EXISTS sp_fact_tx_curr_query;

CREATE PROCEDURE sp_fact_tx_curr_query(IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id                       AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_                  AS follow_up_date,
                             follow_up_2.art_antiretroviral_start_date AS art_start_date,
                             assessment_date,
                             treatment_end_date,
                             antiretroviral_art_dispensed_dose_i       AS ARTDoseDays,
                             weight_text_                              AS Weight,
                             screening_test_result_tuberculosis        AS TB_SreeningStatus,
                             date_of_last_menstrual_period_lmp_           LMP_Date,
                             anitiretroviral_adherence_level           AS AdherenceLevel,
                             next_visit_date,
                             follow_up.regimen,
                             currently_breastfeeding_child                breast_feeding_status,
                             follow_up_1.pregnancy_status,
                             diagnosis_date                            AS ActiveTBDiagnoseddate,
                             nutritional_status_of_adult,
                             nutritional_supplements_provided,
                             stages_of_disclosure,
                             date_started_on_tuberculosis_prophy,
                             method_of_family_planning,
                             patient_diagnosed_with_active_tuber       as ActiveTBDiagnosed,
                             dsd_category,
                             nutritional_screening_result,
                             inh_start_date,
                             inh_date_completed,
                             cd4_count,
                             date_of_event                             as hiv_confirmed_date
                      FROM mamba_flat_encounter_follow_up follow_up
                               JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               LEFT join mamba_flat_encounter_intake_b intake_b
                                         on follow_up.client_id = intake_b.client_id),
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
         latestDSD AS (select * from latestDSD_tmp where row_num = 1),
         tx_curr AS (select * from tx_curr_all where row_num = 1)


    select client.patient_name                                             as 'Patient Name',
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, art_start_date)              as 'Age at Enrollment',
           TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)             as 'Current Age',
           Sex,
           Weight,
           cd4_count                                                       as CD4,
           fn_gregorian_to_ethiopian_calendar(hiv_confirmed_date, 'Y/M/D') as 'HIV Confirmed Date in E.C',
           fn_gregorian_to_ethiopian_calendar(art_start_date, 'Y/M/D')     as 'ART Start Date in E.C',
           'TB Screening Result',
           fn_gregorian_to_ethiopian_calendar(follow_up_date, 'Y/M/D')     as 'Follow-up Date in E.C',
           tx_curr.follow_up_status                                        as 'Follow-up Status',
           Regimen,
           ARTDoseDays                                                     as 'ARV Dose Days',
           AdherenceLevel                                                  as Adherence,
           latestDSD.dsd_category                                          as 'DSD Category',
           nutritional_screening_result                                    as 'Nutritional Status',
           nutritional_supplements_provided                                as 'Therapeutic/ Supplementary Food',
           method_of_family_planning                                       as 'Familiy Planning Method',
           pregnancy_status                                                as 'Pregnant?',
           breast_feeding_status                                           as 'Breastfeeding?',
           'On PMTCT?',
           'TPT Start Date',
           'TPT Completed Date',
           'TB Treatment Completed Date',
           'VL Sent Date',
           'VL Status',
           next_visit_date                                                    'Next Visit Date in E.C',
           tx_curr.treatment_end_date                                         'Treatment End Date in E.C.',
           client.mobile_no                                                as 'Mobile No.'
    from FollowUp
             inner join tx_curr on FollowUp.encounter_id = tx_curr.encounter_id
             left join latestDSD on latestDSD.PatientId = tx_curr.PatientId
             left join mamba_dim_client client on tx_curr.PatientId = client.client_id
    where tx_curr.treatment_end_date >= REPORT_END_DATE
      AND tx_curr.follow_up_status in ('Alive', 'Restart medication')
      and TIMESTAMPDIFF(DAY, art_start_date, REPORT_END_DATE) >= 0;
END
//

DELIMITER ;