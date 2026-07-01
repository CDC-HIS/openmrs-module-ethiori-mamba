DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_follow_up_insert;

CREATE PROCEDURE sp_fact_follow_up_insert()
BEGIN
    TRUNCATE TABLE mamba_fact_follow_up_staging;

    INSERT INTO mamba_fact_follow_up_staging (
        encounter_id, client_id, follow_up_status, follow_up_date_followup_,
        art_antiretroviral_start_date, treatment_end_date, next_visit_date, regimen,
        currently_breastfeeding_child, pregnancy_status,
        transferred_in_check_this_for_all_t, antiretroviral_art_dispensed_dose_i,
        adherence, cd4_count, visitect_cd4_result, visitect_cd4_test_date, weight_text_,
        date_viral_load_results_received, hiv_viral_load, viral_load_test_status,
        viral_load_test_indication, viral_load_received_,
        date_of_reported_hiv_viral_load, dsd_category, assessment_date,
        date_started_on_tuberculosis_prophy, date_completed_tuberculosis_prophyl,
        tb_prophylaxis_type, was_the_patient_screened_for_tuberc, tb_screening_date,
        tb_treatment_status, screening_test_result_tuberculosis, lf_lam_result,
        gene_xpert_result, diagnostic_test, tb_diagnostic_test_result, specimen_sent_to_lab,
        tuberculosis_drug_treatment_start_d, date_active_tbrx_completed,
        patient_diagnosed_with_active_tuber, diagnosis_date, date_active_tbrx_dc,
        currently_taking_tuberculosis_proph,
        cervical_cancer_screening_status, cervical_cancer_screening_method_strategy,
        via_screening_result, hpv_dna_screening_result, date_hpv_test_was_done,
        date_visual_inspection_of_the_cervi, treatment_of_precancerous_lesions_of_the_cervix,
        treatment_start_date, hpv_dna_result_received_date,
        cytology_sample_collection_date, cytology_result,
        purpose_for_visit_cervical_screening, date_cytology_result_received,
        is_the_client_screened_in_this_facility, nutritional_supplements_provided,
        eats_nutritious_foods, nutritional_screening_result, nutritional_status_of_adult,
        nutritional_status_of_older_child_a, weight_for_age_status,
        method_of_family_planning, on_family_planning,
        stages_of_disclosure,
        reason_not_eligible_for_tuberculosi,
        date_discontinued_tuberculosis_prop,
        regimen_change,
        current_who_hiv_stage,
        mid_upper_arm_circumference,
        date_of_event,
        eligible_for_cxca_screening,
        reason_for_not_being_eligible,
        other_reason_for_not_being_eligible_for_cxca,
        colposcopy_of_cervix_findings,
        biopsy_result,
        biopsy_result_received_date,
        date_patient_referred_out,
        next_follow_up_screening_date,
        routine_viral_load_test_indication
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
        visitect_cd4_result,
        visitect_cd4_test_date,
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
        gene_xpert_result,
        diagnostic_test,
        tb_diagnostic_test_result,
        specimen_sent_to_lab,
        tuberculosis_drug_treatment_start_d,
        date_active_tbrx_completed,
        patient_diagnosed_with_active_tuber,
        diagnosis_date,
        date_active_tbrx_dc,
        currently_taking_tuberculosis_proph,
        cervical_cancer_screening_status,
        cervical_cancer_screening_method_strategy,
        via_screening_result,
        hpv_dna_screening_result,
        date_hpv_test_was_done,
        date_visual_inspection_of_the_cervi,
        treatment_of_precancerous_lesions_of_the_cervix,
        treatment_start_date,
        hpv_dna_result_received_date,
        cytology_sample_collection_date,
        cytology_result,
        purpose_for_visit_cervical_screening,
        date_cytology_result_received,
        is_the_client_screened_in_this_facility,
        nutritional_supplements_provided,
        eats_nutritious_foods,
        nutritional_screening_result,
        nutritional_status_of_adult,
        nutritional_status_of_older_child_a,
        weight_for_age_status,
        method_of_family_planning,
        on_family_planning,
        follow_up_5.stages_of_disclosure,
        reason_not_eligible_for_tuberculosi,
        date_discontinued_tuberculosis_prop,
        regimen_change,
        current_who_hiv_stage,
        mid_upper_arm_circumference,
        date_of_event,
        eligible_for_cxca_screening,
        reason_for_not_being_eligible,
        other_reason_for_not_being_eligible_for_cxca,
        colposcopy_of_cervix_findings,
        biopsy_result,
        biopsy_result_received_date,
        date_patient_referred_out,
        next_follow_up_screening_date,
        COALESCE(at_3436_weeks_of_gestation,
                 viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                 viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                 every_six_months_until_mtct_ends,
                 six_months_after_the_first_viral_load_test_at_postnatal_peri,
                 three_months_after_delivery,
                 at_the_first_antenatal_care_visit,
                 annual_viral_load_test,
                 second_viral_load_test_at_12_months_post_art,
                 first_viral_load_test_at_6_months_or_longer_post_art,
                 first_viral_load_test_at_3_months_or_longer_post_art) AS routine_viral_load_test_indication
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
                       ON follow_up.encounter_id = follow_up_10.encounter_id;
END //

DELIMITER ;
