DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_tx_new_query;

CREATE PROCEDURE sp_fact_tx_new_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
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
                             screening_test_result_tuberculosis        AS TB_SreeningResult,
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
                             cd4_count,
                             date_of_event                             as hiv_confirmed_date,
                             follow_up_1.current_who_hiv_stage,
                             transferred_in_check_this_for_all_t          ti_status
                      FROM mamba_flat_encounter_follow_up follow_up
                               JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               LEFT JOIN mamba_flat_encounter_intake_b intake_b
                                         on follow_up.client_id = intake_b.client_id),
         -- TX new
         tmp_tx_new AS (SELECT PatientId,
                               follow_up_date                                                                             AS FollowupDate,
                               encounter_id,
                               art_start_date,
                               follow_up_status,
                               fn_get_ti_status(PatientId, REPORT_START_DATE, REPORT_END_DATE)                            as TIStatus,
                               ti_status,
                               ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                        FROM FollowUp
                        WHERE follow_up_status IS NOT NULL
                          AND art_start_date IS NOT NULL
                          AND follow_up_date <= REPORT_END_DATE),
        tmp_first_follow_up_status as ( SELECT PatientId,
                                           follow_up_date                                                                             AS FollowupDate,
                                           encounter_id,
                                           follow_up_status,
                                           ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date , encounter_id ) AS row_num
                                    FROM FollowUp
                                    where regimen is not null
                                    ),
         first_follow_up_status as (select PatientId,follow_up_status from tmp_first_follow_up_status where row_num=1),
         tx_new as (select *
                    from tmp_tx_new
                    where row_num = 1
                      and art_start_date between REPORT_START_DATE and REPORT_END_DATE
                      and TIStatus = 'NTI'
                      and (ti_status is null or ti_status = 'No'))
    select tx_new.PatientId                                                   AS 'Patient Name',
           client.MRN                                                         AS 'MRN',
           client.uan                                                         AS 'UAN',
           TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)                AS 'Age',
           client.sex                                                         AS 'Sex',
           Weight                                                             AS 'Weight',
           cd4_count                                                          AS 'CD4',
           current_who_hiv_stage                                              AS 'WHO Stage',
           nutritional_screening_result                                       AS 'Nutritional Status',
           TB_SreeningResult                                                  AS 'TB Screening Result',
           current_who_hiv_stage                                              AS 'Enrollment Date in E.C',
           fn_gregorian_to_ethiopian_calendar(hiv_confirmed_date, 'Y/M/D')    AS 'HIV Confirmed Date in E.C',
           fn_gregorian_to_ethiopian_calendar(tx_new.art_start_date, 'Y/M/D') AS 'ART Start Date in E.C',
           TIMESTAMPDIFF(DAY, hiv_confirmed_date, tx_new.art_start_date)      AS 'Days Difference',
           pregnancy_status                                                   AS 'Pregnant?',
           breast_feeding_status                                              AS 'Breastfeeding?',
           regimen                                                            AS 'Regimen',
           ARTDoseDays                                                        AS 'ARV Dose Days',
           next_visit_date                                                    AS 'Next Visit Date in E.C',
           treatment_end_date                                                 AS 'Treatment End Date in E.C',
           mobile_no                                                          AS 'Mobile No.'
    FROM tx_new
             INNER JOIN FollowUp
                        on tx_new.encounter_id = FollowUp.encounter_id
             INNER JOIN first_follow_up_status on tx_new.PatientId=first_follow_up_status.PatientId
             INNER JOIN mamba_dim_client client on tx_new.PatientId = client.client_id
    WHERE first_follow_up_status.follow_up_status in ('Alive','Restart medication')
    ;
END //

DELIMITER ;