DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_tpt_query;

CREATE PROCEDURE sp_fact_tpt_query(IN REPORT_END_DATE DATE)
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
                               )
        select * from FollowUp
      ;
END //

DELIMITER ;