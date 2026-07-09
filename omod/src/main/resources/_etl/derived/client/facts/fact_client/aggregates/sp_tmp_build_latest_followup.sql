DELIMITER //

DROP PROCEDURE IF EXISTS sp_tmp_build_latest_followup;

-- Builds a session-scoped temp table holding one row per client with their most
-- recent follow-up encounter values, computed directly off mamba_fact_follow_up
-- (which is indexed on (client_id, follow_up_date_followup_ DESC, encounter_id DESC))
-- instead of via a shared CTE re-scanned by multiple window functions.
-- p_as_of_date = NULL means "no cutoff" (matches the ETL's full-history behavior).
CREATE PROCEDURE sp_tmp_build_latest_followup(IN p_as_of_date DATE)
BEGIN
    DROP TEMPORARY TABLE IF EXISTS mamba_temp_latest_followup;

    CREATE TEMPORARY TABLE mamba_temp_latest_followup AS
    SELECT client_id,
           visit_date,
           next_visit_date,
           follow_up_status,
           regimen,
           regimen_dose,
           treatment_end_date,
           nutritional_status_of_adult,
           nutritional_status_of_older_child_a,
           mid_upper_arm_circumference,
           pregnancy_status,
           art_start_date,
           who_stage,
           currently_breastfeeding_child,
           weight,
           method_of_family_planning,
           dsd_category,
           hiv_confirmed_date,
           transfer_in,
           eligible_for_cxca_screening,
           reason_for_not_being_eligible,
           other_reason_for_not_being_eligible_for_cxca,
           screening_status
    FROM (SELECT client_id,
                 follow_up_date_followup_                                                                             AS visit_date,
                 next_visit_date,
                 follow_up_status,
                 regimen,
                 antiretroviral_art_dispensed_dose_i                                                                  AS regimen_dose,
                 treatment_end_date,
                 nutritional_status_of_adult,
                 nutritional_status_of_older_child_a,
                 mid_upper_arm_circumference,
                 pregnancy_status,
                 art_antiretroviral_start_date                                                                        AS art_start_date,
                 current_who_hiv_stage                                                                                AS who_stage,
                 currently_breastfeeding_child,
                 weight_text_                                                                                         AS weight,
                 method_of_family_planning,
                 dsd_category,
                 date_of_event                                                                                        AS hiv_confirmed_date,
                 transferred_in_check_this_for_all_t                                                                  AS transfer_in,
                 eligible_for_cxca_screening,
                 reason_for_not_being_eligible,
                 other_reason_for_not_being_eligible_for_cxca,
                 cervical_cancer_screening_status                                                                     AS screening_status,
                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
          FROM mamba_fact_follow_up
          WHERE follow_up_date_followup_ IS NOT NULL
            AND (p_as_of_date IS NULL OR follow_up_date_followup_ <= p_as_of_date)) ranked
    WHERE rn = 1;

    ALTER TABLE mamba_temp_latest_followup ADD PRIMARY KEY (client_id);
END //

DELIMITER ;
