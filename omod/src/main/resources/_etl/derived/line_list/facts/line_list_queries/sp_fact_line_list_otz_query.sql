DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_otz_query;

CREATE PROCEDURE sp_fact_line_list_otz_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN


    DECLARE OTZ_MIN_AGE INT DEFAULT 10;
    DECLARE OTZ_MAX_AGE INT DEFAULT 24;


    IF REPORT_START_DATE IS NOT NULL AND REPORT_END_DATE IS NULL THEN
        SET REPORT_END_DATE = CURDATE();
    END IF;
    IF REPORT_END_DATE IS NOT NULL AND REPORT_START_DATE IS NULL THEN
        SET REPORT_START_DATE = '1900-01-01';
    END IF;

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
                                         ON follow_up.encounter_id = follow_up_10.encounter_id
                      ),

         followup_ranked as (SELECT encounter_id,
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
                                    viral_load_perform_date,
                                    viral_load_sent_date,
                                    viral_load_count,
                                    viral_load_test_status,
                                    otz_date,
                                    otz_enrolled,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC)  AS rn_latest,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS rn_latest_vl,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY otz_date DESC, encounter_id DESC)       AS rn_otz,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date, encounter_id)           AS rn_oldest
                             FROM FollowUp
                             WHERE follow_up_status IS NOT NULL
                               AND art_start_date IS NOT NULL
                               AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         latest_follow_up as (select *
                              from followup_ranked
                              where rn_latest = 1
         ),
         latest_vl_performed_date as (select *
                                      from followup_ranked
                                      where rn_latest_vl = 1
         ),
         otz_date as (select *,
                             (otz_date IS NOT NULL OR otz_enrolled IS NOT NULL) AS has_otz_record
                      from followup_ranked
                      where rn_otz = 1
         ),
         otz_window as (select client_id,
                               otz_date,
                               fn_ethiopian_to_gregorian_calendar(fn_add_ethiopian_months(fn_gregorian_to_ethiopian_calendar(otz_date, 'Y-M-D'), -3, 1)) as vl_window_start,
                               fn_ethiopian_to_gregorian_calendar(fn_add_ethiopian_months(fn_gregorian_to_ethiopian_calendar(otz_date, 'Y-M-D'), 1, 1))  as vl_window_end
                        from otz_date
         ),
-- Get baseline VL Performed date between 3 months earlier and 1 month later of enrollment date
         tmp_otz_vl_performed_date as (SELECT /*+ NO_MERGE(otz_window) */ followup_ranked.encounter_id,
                                              followup_ranked.client_id,
                                              followup_ranked.follow_up_date,
                                              followup_ranked.viral_load_perform_date,
                                              followup_ranked.viral_load_sent_date,
                                              followup_ranked.otz_date,
                                              followup_ranked.viral_load_count,
                                              followup_ranked.viral_load_test_status,
                                              ROW_NUMBER() OVER (PARTITION BY followup_ranked.client_id ORDER BY followup_ranked.viral_load_perform_date DESC, followup_ranked.encounter_id DESC) AS row_num
                                       FROM followup_ranked
                                                left join otz_window as otz on followup_ranked.client_id = otz.client_id
                                       WHERE followup_ranked.viral_load_perform_date BETWEEN otz.vl_window_start
                                             AND otz.vl_window_end),
         otz_vl_performed_date as (select *
                                   from tmp_otz_vl_performed_date
                                   where row_num = 1
         ),
         oldest_follow_up as (select *
                              from followup_ranked
                              where rn_oldest = 1
         ),
         tmp_curr_regimen_start as (select followup_ranked.follow_up_date                                                                                     as FirstRegimenDate,
                                           followup_ranked.client_id,
                                           ROW_NUMBER() OVER (PARTITION BY followup_ranked.client_id ORDER BY followup_ranked.follow_up_date, followup_ranked.encounter_id) AS row_num
                                    from followup_ranked
                                             left join latest_follow_up on followup_ranked.client_id = latest_follow_up.client_id
                                    WHERE followup_ranked.regimen = latest_follow_up.regimen),
         curr_regimen_start as (select * from tmp_curr_regimen_start where row_num = 1)


    SELECT dim_client.patient_name                                                             AS `Patient Name`,
           patient_uuid                                                                        as `UUID`,
           otz.otz_date                                                                        AS EnrollementDate,
           otz.otz_date                                                                        AS `EnrollementDate EC.`,
           otz.otz_enrolled                                                                    AS EnrollementStatus,
           dim_client.age                                                                      as Age,
           dim_client.sex                                                                      as Sex,
           latest_follow_up.weight                                                             AS Weight,
           dim_client.phone_no                                                                 AS PNumber,
           dim_client.mobile_no                                                                AS Mobile,
           CAST(dim_client.mrn AS CHAR(20))                                                    AS MRN,
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
           CASE latest_follow_up.follow_up_status
               WHEN 'Alive' THEN 'Alive on ART'
               WHEN 'Restart medication' THEN 'Restart'
               WHEN 'Transferred out' THEN 'TO'
               WHEN 'Stop all' THEN 'Stop'
               WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
               WHEN 'Ran away' THEN 'Drop'
               END                                                                             AS FollowUpStatus,
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
             LEFT JOIN (SELECT client_id,
                               patient_name,
                               patient_uuid,
                               date_of_birth,
                               sex,
                               phone_no,
                               mobile_no,
                               mrn,
                               uan,
                               TIMESTAMPDIFF(YEAR, date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) AS age
                        FROM mamba_dim_client) dim_client
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
            OR otz.has_otz_record
        )
      AND dim_client.patient_name IS NOT NULL
      AND (
        (dim_client.age BETWEEN OTZ_MIN_AGE AND OTZ_MAX_AGE)
            OR otz.has_otz_record
        )
      AND (
        (REPORT_START_DATE is not null and REPORT_END_DATE is not null and
         otz.otz_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE)
            OR
        (REPORT_START_DATE is null and REPORT_END_DATE is null and (otz.otz_date <= CURDATE() or otz.otz_date is null))
        );

END //

DELIMITER ;
