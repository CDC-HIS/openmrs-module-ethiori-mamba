-- Incremental delta refresh of the follow-up materialization.
-- Used on etl_incremental_mode = 1 (every ETL run after the first).
--
-- The watermark is the start_time of the most recent COMPLETED ETL run
-- (from _mamba_etl_schedule). We find the set of encounter_ids that
-- have been touched in mamba_z_encounter_obs since the watermark, and
-- re-project only those rows. On a typical 5-minute ETL window with
-- ~1K changed encounters out of ~5M, the work is O(1K) not O(5M).
--
-- Note: the `incremental_record` flag on mamba_z_encounter_obs is NOT
-- used here because it is reset to 0 by
-- sp_mamba_reset_incremental_update_flag_all at the end of
-- sp_mamba_data_processing_increment_and_flatten, which runs BEFORE
-- this SP. The date-based watermark on _mamba_etl_schedule is the
-- reliable signal.

DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_followup_update;

CREATE PROCEDURE sp_fact_hmis_followup_update()
BEGIN
    DECLARE last_etl_time DATETIME DEFAULT NULL;

    -- Find the start_time of the most recent COMPLETED ETL run.
    -- NULL on the first-ever run; in that case _update is never
    -- called (the full path runs _insert instead), so we only
    -- guard against accidental misuse.
    SELECT start_time
      INTO last_etl_time
      FROM _mamba_etl_schedule
     WHERE end_time IS NOT NULL
       AND transaction_status = 'COMPLETED'
     ORDER BY id DESC
     LIMIT 1;

    IF last_etl_time IS NOT NULL THEN
        -- Step 1: delete the existing rows for encounters that have
        -- been touched since the watermark (new obs inserted, or
        -- existing obs voided). The join through mamba_z_encounter_obs
        -- scopes the DELETE to the follow-up encounter type only.
        DELETE t
          FROM mamba_fact_followup t
          JOIN mamba_z_encounter_obs z
            ON t.encounter_id = z.encounter_id
         WHERE z.encounter_type_uuid = '136b2ded-22a3-4831-a39a-088d35a50ef5'
           AND (z.date_created   >= last_etl_time
                OR (z.date_voided IS NOT NULL AND z.date_voided >= last_etl_time));

        -- Step 2: re-insert the touched encounters (skipping voided
        -- encounters) using the same 10-way LEFT JOIN shape as the
        -- full rebuild, but bounded to the delta set via the IN clause.
        INSERT INTO mamba_fact_followup (
            encounter_id, client_id, follow_up_status, follow_up_date_followup_,
            art_antiretroviral_start_date, treatment_end_date, next_visit_date, regimen,
            currently_breastfeeding_child, pregnancy_status,
            transferred_in_check_this_for_all_t, antiretroviral_art_dispensed_dose_i,
            adherence, cd4_count, weight_text_,
            date_viral_load_results_received, hiv_viral_load, viral_load_test_status,
            viral_load_test_indication, viral_load_received_,
            date_of_reported_hiv_viral_load, dsd_category, assessment_date,
            date_started_on_tuberculosis_prophy, date_completed_tuberculosis_prophyl,
            tb_prophylaxis_type, was_the_patient_screened_for_tuberc, tb_screening_date,
            tb_treatment_status, screening_test_result_tuberculosis, lf_lam_result,
            cervical_cancer_screening_status, cervical_cancer_screening_method_strategy,
            via_screening_result, hpv_dna_screening_result, date_hpv_test_was_done,
            date_visual_inspection_of_the_cervi, treatment_of_precancerous_lesions_of_the_cervix,
            treatment_start_date, hpv_dna_result_received_date,
            is_the_client_screened_in_this_facility, nutritional_supplements_provided,
            eats_nutritious_foods, nutritional_screening_result, nutritional_status_of_adult,
            nutritional_status_of_older_child_a, weight_for_age_status,
            method_of_family_planning, on_family_planning
        )
        SELECT
            follow_up.encounter_id,
            follow_up.client_id,
            follow_up_status,
            follow_up_date_followup_,
            art_antiretroviral_start_date,
            treatment_end_date,
            next_visit_date,
            regimen,
            currently_breastfeeding_child,
            pregnancy_status,
            transferred_in_check_this_for_all_t,
            antiretroviral_art_dispensed_dose_i,
            adherence,
            cd4_count,
            weight_text_,
            date_viral_load_results_received,
            hiv_viral_load,
            viral_load_test_status,
            viral_load_test_indication,
            viral_load_received_,
            date_of_reported_hiv_viral_load,
            dsd_category,
            assessment_date,
            date_started_on_tuberculosis_prophy,
            date_completed_tuberculosis_prophyl,
            tb_prophylaxis_type,
            was_the_patient_screened_for_tuberc,
            tb_screening_date,
            tb_treatment_status,
            screening_test_result_tuberculosis,
            lf_lam_result,
            cervical_cancer_screening_status,
            cervical_cancer_screening_method_strategy,
            via_screening_result,
            hpv_dna_screening_result,
            date_hpv_test_was_done,
            date_visual_inspection_of_the_cervi,
            treatment_of_precancerous_lesions_of_the_cervix,
            treatment_start_date,
            hpv_dna_result_received_date,
            is_the_client_screened_in_this_facility,
            nutritional_supplements_provided,
            eats_nutritious_foods,
            nutritional_screening_result,
            nutritional_status_of_adult,
            nutritional_status_of_older_child_a,
            weight_for_age_status,
            method_of_family_planning,
            on_family_planning
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
         WHERE follow_up.encounter_id IN (
                   SELECT DISTINCT z.encounter_id
                     FROM mamba_z_encounter_obs z
                    WHERE z.encounter_type_uuid = '136b2ded-22a3-4831-a39a-088d35a50ef5'
                      AND z.date_created >= last_etl_time
                      AND z.voided = 0
               );

        ANALYZE TABLE mamba_fact_followup;
    END IF;
END //

DELIMITER ;
