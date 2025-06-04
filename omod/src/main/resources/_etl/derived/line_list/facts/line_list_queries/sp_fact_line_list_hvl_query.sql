DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_hvl_query;

CREATE PROCEDURE sp_fact_line_list_hvl_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t as transferred_in,
                             cd4_count,
                             weight_text_                        as weight,
                             current_who_hiv_stage,
                             nutritional_screening_result,
                             screening_test_result_tuberculosis  as TB_SreeningResult,
                             date_of_event                       as hiv_confirmed_date,
                             antiretroviral_art_dispensed_dose_i    ARTDoseDays
                      FROM mamba_flat_encounter_follow_up follow_up
                               left JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               left JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                         ON follow_up.encounter_id = follow_up_4.encounter_id
                      ),
         tmp_latest_follow_up as (SELECT client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         encounter_id,
                                         follow_up_status,
                                         art_start_date,
                                         cd4_count,
                                         weight,
                                         current_who_hiv_stage,
                                         nutritional_screening_result,
                                         TB_SreeningResult,
                                         hiv_confirmed_date,
                                         breast_feeding_status,
                                         ARTDoseDays,
                                         regimen,
                                         next_visit_date,
                                         treatment_end_date,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= REPORT_END_DATE),
         latest_follow_up as (SELECT *
                              from tmp_latest_follow_up
                              where row_num = 1),


END //

DELIMITER ;