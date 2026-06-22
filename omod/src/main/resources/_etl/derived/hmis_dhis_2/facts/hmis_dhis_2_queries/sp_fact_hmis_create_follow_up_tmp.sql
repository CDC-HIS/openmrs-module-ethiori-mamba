DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_create_follow_up_tmp;

CREATE PROCEDURE sp_fact_hmis_create_follow_up_tmp()
BEGIN
    DROP TABLE IF EXISTS tmp_hmis_follow_up;
    DROP TEMPORARY TABLE IF EXISTS tmp_hmis_follow_up;

    CREATE TABLE tmp_hmis_follow_up AS
    SELECT follow_up.encounter_id,
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
                       ON follow_up.encounter_id = follow_up_9.encounter_id;

    CREATE INDEX idx_tmp_hmis_fu_enc  ON tmp_hmis_follow_up (encounter_id);
    CREATE INDEX idx_tmp_hmis_fu_cl   ON tmp_hmis_follow_up (client_id);
    CREATE INDEX idx_tmp_hmis_fu_cl_d ON tmp_hmis_follow_up (client_id, follow_up_date_followup_);
END //

DELIMITER ;
