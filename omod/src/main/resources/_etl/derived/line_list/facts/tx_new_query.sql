DELIMITER
//

DROP PROCEDURE IF EXISTS sp_fact_tx_new_query;

CREATE PROCEDURE sp_fact_tx_new_query(IN REPORT_START_DATE DATE,IN REPORT_END_DATE DATE)
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

END
//

DELIMITER ;