DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_otz_query;

CREATE PROCEDURE sp_fact_line_list_otz_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN


    WITH FollowUp as (select follow_up.client_id,
                             follow_up.encounter_id,
                             date_viral_load_results_received    AS viral_load_perform_date,
                             viral_load_received_,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date          art_start_date,
                             viral_load_test_status,
                             hiv_viral_load                      AS viral_load_count,
                             COALESCE(
                                     at_3436_weeks_of_gestation,
                                     viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                     viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                     every_six_months_until_mtct_ends,
                                     six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                     three_months_after_delivery,
                                     at_the_first_antenatal_care_visit,
                                     annual_viral_load_test,
                                     second_viral_load_test_at_12_months_post_art,
                                     first_viral_load_test_at_6_months_or_longer_post_art,
                                     first_viral_load_test_at_3_months_or_longer_post_art
                             )                                   AS routine_viral_load_test_indication,
                             COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                      suspected_antiretroviral_failure
                             )                                   AS targeted_viral_load_test_indication,
                             viral_load_test_indication,
                             pregnancy_status,
                             currently_breastfeeding_child       AS breastfeeding_status,
                             antiretroviral_art_dispensed_dose_i    arv_dispensed_dose,
                             regimen,
                             next_visit_date,
                             treatment_end_date,
                             date_of_event                          date_hiv_confirmed,
                             weight_text_                        as weight,
                             date_of_reported_hiv_viral_load     as viral_load_sent_date,
                             regimen_change,
                             operation_triple_zero_enrollment_da as otz_date,
                             enrolled_to_otz_operation_triple_zo as otz_enrolled,
                             visit_type,
                             nutritional_status_of_adult,
                             nutritional_screening_result,
                             adherence
                      FROM mamba_flat_encounter_follow_up follow_up
                               left JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                         ON follow_up.encounter_id = follow_up_1.encounter_id
                               left JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                         ON follow_up.encounter_id = follow_up_2.encounter_id
                               left JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               left JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                         ON follow_up.encounter_id = follow_up_4.encounter_id),
         tmp_latest_follow_up as (SELECT encounter_id,
                                         client_id,
                                         follow_up_date,
                                         weight,
                                         date_hiv_confirmed,
                                         art_start_date,
                                         visit_type,
                                         nutritional_status_of_adult,
                                         nutritional_screening_result,
                                         adherence,
                                         next_visit_date,
                                         arv_dispensed_dose,
                                         regimen,
                                         follow_up_status,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         latest_follow_up as (select *
                              from tmp_latest_follow_up
                              where row_num = 1 -- temp3
         ),
         tmp_latest_vl_performed_date as (SELECT encounter_id,
                                                 client_id,
                                                 follow_up_date,
                                                 viral_load_perform_date,
                                                 viral_load_sent_date,
                                                 viral_load_count,
                                                 viral_load_test_status,
                                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                                          FROM FollowUp
                                          WHERE follow_up_status IS NOT NULL
                                            AND art_start_date IS NOT NULL
                                            AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         latest_vl_performed_date as (select *
                                      from tmp_latest_vl_performed_date
                                      where row_num = 1 -- temp6
         ),
         tmp_otz_date as (SELECT encounter_id,
                                 client_id,
                                 follow_up_date,
                                 viral_load_perform_date,
                                 viral_load_sent_date,
                                 otz_date,
                                 otz_enrolled,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY otz_date DESC, encounter_id DESC) AS row_num
                          FROM FollowUp
                          WHERE follow_up_status IS NOT NULL
                            AND art_start_date IS NOT NULL
                            AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         otz_date as (select *
                      from tmp_otz_date
                      where row_num = 1 -- temp9
         ),
-- Get baseline VL Performed date between 3 months earlier and 1 month later of enrollment date
         tmp_otz_vl_performed_date as (SELECT FollowUp.encounter_id,
                                              FollowUp.client_id,
                                              FollowUp.follow_up_date,
                                              FollowUp.viral_load_perform_date,
                                              FollowUp.viral_load_sent_date,
                                              FollowUp.otz_date,
                                              FollowUp.viral_load_count,
                                              FollowUp.viral_load_test_status,
                                              ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.viral_load_perform_date DESC, FollowUp.encounter_id DESC) AS row_num
                                       FROM FollowUp
                                                left join otz_date as otz on FollowUp.client_id = otz.client_id
                                       WHERE follow_up_status IS NOT NULL
                                         AND FollowUp.art_start_date IS NOT NULL
                                         AND FollowUp.follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())
                                         AND FollowUp.viral_load_perform_date BETWEEN DATE_ADD(otz.otz_date, INTERVAL -3 MONTH) AND DATE_ADD(otz.otz_date, INTERVAL 1 MONTH)),
         otz_vl_performed_date as (select *
                                   from tmp_otz_vl_performed_date
                                   where row_num = 1 -- temp9
         ),

         tmp_oldest_follow_up as (SELECT encounter_id,
                                         client_id,
                                         follow_up_date,
                                         regimen,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         oldest_follow_up as (select *
                              from tmp_oldest_follow_up
                              where row_num = 1 -- temp13
         ),
         tmp_curr_regimen_start as (select FollowUp.follow_up_date                                                                                     as FirstRegimenDate,
                                           FollowUp.client_id,
                                           ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.follow_up_date, FollowUp.encounter_id) AS row_num
                                    from FollowUp
                                             left join latest_follow_up on FollowUp.client_id = latest_follow_up.client_id
                                    WHERE FollowUp.follow_up_status IS NOT NULL
                                      AND FollowUp.art_start_date IS NOT NULL
                                      AND FollowUp.follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())
                                      and FollowUp.regimen = latest_follow_up.regimen),
         curr_regimen_start as (select * from tmp_curr_regimen_start where row_num = 1)


    SELECT otz.otz_date                                                                        AS EnrollementDate,
           otz.otz_date                                                                        AS `EnrollementDate EC.`,
           otz_enrolled                                                                        AS EnrollementStatus,
           dim_client.patient_name                                                             AS Name,
           TIMESTAMPDIFF(YEAR, dim_client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) as Age,
           dim_client.sex                                                                      as Sex,
           latest_follow_up.weight                                                             AS Weight,
           dim_client.phone_no                                                                 AS PNumber,
           dim_client.mobile_no                                                                AS Mobile,
           dim_client.mrn                                                                      AS MRN,
           dim_client.uan                                                                      AS UART,
           latest_follow_up.date_hiv_confirmed                                                 AS confirmeddate,
           latest_follow_up.date_hiv_confirmed                                                 AS `confirmeddate EC.`,
           latest_follow_up.art_start_date                                                     AS startedDate,
           latest_follow_up.art_start_date                                                     AS `startedDate EC.`,
           latest_follow_up.follow_up_date                                                     AS FollowUpDate,
           latest_follow_up.follow_up_date                                                     AS `FollowUpDate EC.`,
           latest_follow_up.visit_type                                                         AS scheduletype,
           latest_follow_up.nutritional_screening_result                                       AS NutritionalStatus,
           latest_follow_up.adherence                                                          AS Adherance,
           latest_follow_up.next_visit_date                                                    AS nextvisitdate,
           latest_follow_up.next_visit_date                                                    AS `nextvisitdate EC.`,
           latest_follow_up.arv_dispensed_dose                                                 AS Dosedays,
           latest_follow_up.regimen                                                            AS Regimen,
           latest_follow_up.follow_up_status                                                   AS FollowUpStatus,
           otz_vl_performed_date.viral_load_sent_date                                          AS BaselineVLsent,
           otz_vl_performed_date.viral_load_sent_date                                          AS `BaselineVLsent EC.`,
           otz_vl_performed_date.viral_load_perform_date                                       AS BaselineVLReceived,
           otz_vl_performed_date.viral_load_perform_date                                       AS `BaselineVLReceived EC.`,
           otz_vl_performed_date.viral_load_count                                              AS BaselineVLCount,
           otz_vl_performed_date.viral_load_test_status                                        AS BaselineVLStatus,
           latest_vl_performed_date.viral_load_sent_date                                       AS VLsent,
           latest_vl_performed_date.viral_load_sent_date                                       AS `VLsent EC.`,
           latest_vl_performed_date.viral_load_perform_date                                    AS VLReceived,
           latest_vl_performed_date.viral_load_perform_date                                    AS `VLReceived EC.`,
           latest_vl_performed_date.viral_load_count                                           AS VLCount,
           latest_vl_performed_date.viral_load_test_status                                     AS VLStatus,
           oldest_follow_up.regimen                                                            AS OriginalRegimen,
           curr_regimen_start.FirstRegimenDate                                                 AS currentRegimenStart,
           curr_regimen_start.FirstRegimenDate                                                 AS `currentRegimenStart EC.`
    FROM latest_follow_up
             LEFT JOIN mamba_dim_client dim_client
                       ON dim_client.client_id = latest_follow_up.client_id
             LEFT JOIN otz_vl_performed_date
                       ON otz_vl_performed_date.client_id = latest_follow_up.client_id
             LEFT JOIN latest_vl_performed_date
                       ON latest_vl_performed_date.client_id = latest_follow_up.client_id
             LEFT JOIN oldest_follow_up
                       ON oldest_follow_up.client_id = latest_follow_up.client_id
             LEFT JOIN curr_regimen_start
                       ON curr_regimen_start.client_id = latest_follow_up.client_id
             LEFT JOIN otz_date AS otz
                       ON otz.client_id = latest_follow_up.client_id
    WHERE (
        (latest_follow_up.follow_up_status not in ('Dead', 'Transferred Out'))
            OR
        (otz.otz_date is not null OR otz.otz_enrolled is not null)
        )
      AND dim_client.patient_name IS NOT NULL

      AND (
        (TIMESTAMPDIFF(YEAR, dim_client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) >= 10 AND
         TIMESTAMPDIFF(YEAR, dim_client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) <= 24)
            OR
        (otz.otz_date is not null OR otz.otz_enrolled is not null)
        )
      AND (
        (REPORT_START_DATE is not null and REPORT_END_DATE is not null and
         otz.otz_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE)
            OR
        (REPORT_START_DATE is null and REPORT_END_DATE is null and (otz.otz_date <= CURDATE() or otz.otz_date is null))
        );

END //

DELIMITER ;
