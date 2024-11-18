DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_tx_curr_query;

CREATE PROCEDURE sp_fact_encounter_art_follow_up_tx_curr_query(IN REPORT_END_DATE DATE)
BEGIN
    WITH client_identifier as (select person.person_id,
                                      MAX(CASE WHEN pid.identifier_type = 5 THEN pid.identifier END) AS MRN,
                                      MAX(CASE WHEN pid.identifier_type = 6 THEN pid.identifier END) AS UAN,
                                      MAX(CASE WHEN pid.identifier_type = 26 THEN pid.identifier END) AS mobile_no
                               from mamba_dim_person person
                                        left join mamba_dim_patient_identifier pid on person.person_id = pid.patient_id
                               group by person_id),
         FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id                       AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_                  AS follow_up_date,
                             follow_up_2.art_antiretroviral_start_date AS art_start_date,
                             assessment_date,
                             treatment_end_date,
                             antiretroviral_art_dispensed_dose_i       AS ARTDoseDays,
                             gender                                    AS sex,
                             age                                       AS Age,
                             weight_text_                              AS Weight,
                             screening_test_result_tuberculosis        AS TB_SreeningStatus,
                             date_of_last_menstrual_period_lmp_           LMP_Date,
                             anitiretroviral_adherence_level           AS AdherenceLevel,
                             next_visit_date,
                             follow_up.regimen,
                             currently_breastfeeding_child                breast_feeding_status,
                             follow_up_1.pregnancy_status,
                             person.uuid,
                             diagnosis_date                            AS ActiveTBDiagnoseddate,
                             nutritional_status_of_adult,
                             nutritional_supplements_provided,
                             stages_of_disclosure,
                             date_started_on_tuberculosis_prophy as tpt_start_date,
                             method_of_family_planning,
                             patient_diagnosed_with_active_tuber       as ActiveTBDiagnosed,
                             dsd_category,
                             nutritional_screening_result,
                             person_name_long,
                             MRN,
                             uan,
                             birthdate,
                             mobile_no,
                             date_of_event as hiv_confirmed_date,
                             viral_load_test_status,
                             date_viral_load_results_received,
                             date_discontinued_tuberculosis_prop as tpt_discontinued_date,
                             date_completed_tuberculosis_prophyl as tpt_completed_date
                      FROM mamba_flat_encounter_follow_up follow_up
                               JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                    ON follow_up.encounter_id = follow_up_3.encounter_id
                               JOIN mamba_dim_person person on person.person_id = follow_up.client_id
                               join client_identifier pid on follow_up.client_id = pid.person_id),
         -- TX curr
         tx_curr_all AS (SELECT PatientId,
                                follow_up_date                                                                             AS FollowupDate,
                                encounter_id,
                                ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                         FROM FollowUp
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date IS NOT NULL
                           AND follow_up_date <= REPORT_END_DATE
                           AND treatment_end_date >= REPORT_END_DATE
                           AND follow_up_status in ('Alive', 'Restart medication')),
         latestDSD_tmp AS (SELECT PatientId,
                                  assessment_date                                                                               AS latestDsdDate,
                                  encounter_id,
                                  dsd_category,
                                  ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY assessment_date DESC , encounter_id DESC ) AS row_num
                           FROM FollowUp
                           WHERE assessment_date IS NOT NULL
                             AND assessment_date <= REPORT_END_DATE),

         latestDSD AS (select * from latestDSD_tmp where row_num = 1),
         tx_curr AS (select * from tx_curr_all where row_num = 1)
    SELECT FollowUp.PatientId,
           FollowUp.person_name_long,
           FollowUp.mrn,
           FollowUp.uan,
           FollowUp.Age,
           fn_mamba_age_calculator(FollowUp.birthdate, art_start_date) AS age_at_enrollment,
           FollowUp.sex,
           FollowUp.mobile_no,
           FollowUp.hiv_confirmed_date,
           FollowUp.art_start_date,
           FollowUp.follow_up_date,
           FollowUp.Weight,
           FollowUp.pregnancy_status,
           FollowUp.regimen,
           FollowUp.ARTDoseDays,
           FollowUp.follow_up_status,
           FollowUp.AdherenceLevel,
           FollowUp.next_visit_date,
           latestDSD.dsd_category,
           FollowUp.tpt_start_date,
           FollowUp.tpt_completed_date,
           FollowUp.tpt_discontinued_date,
           FollowUp.treatment_end_date,
           FollowUp.date_viral_load_results_received,
           FollowUp.viral_load_test_status,
           NOW()
    FROM FollowUp
             JOIN tx_curr ON FollowUp.encounter_id = tx_curr.encounter_id
             left JOIN latestDSD on tx_curr.PatientId = latestDSD.PatientId;
    END //
DELIMITER ;