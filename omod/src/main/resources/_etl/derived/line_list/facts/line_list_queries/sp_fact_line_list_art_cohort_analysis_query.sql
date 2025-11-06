DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_art_cohort_analysis_query;

CREATE PROCEDURE sp_fact_line_list_art_cohort_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUpEncounters AS (
        SELECT
            follow_up.encounter_id,
            follow_up.client_id AS PatientId,
            follow_up_status,
            follow_up_date_followup_ AS follow_up_date,
            art_antiretroviral_start_date AS art_start_date,
            treatment_end_date,
            regimen,
            antiretroviral_art_dispensed_dose_i AS ARTDoseDays,
            anitiretroviral_adherence_level AS AdherenceLevel,
            pregnancy_status,
            next_visit_date,
            date_of_reported_hiv_viral_load AS viral_load_sent_date,
            date_viral_load_results_received AS viral_load_received_date,
            viral_load_test_status AS viral_load_result,
            hiv_viral_load AS viral_load_count,
            cd4_count,
            cd4_ AS cd4_percent,
            current_functional_status,
            transferred_in_check_this_for_all_t,
            visitect_cd4_result,
            visitect_cd4_test_date
        FROM mamba_flat_encounter_follow_up follow_up
                 LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1 USING (encounter_id)
                 LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2 USING (encounter_id)
                 LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3 USING (encounter_id)
                 LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4 USING (encounter_id)
                 LEFT JOIN mamba_flat_encounter_follow_up_5 follow_up_5 USING (encounter_id)
                 LEFT JOIN mamba_flat_encounter_follow_up_6 follow_up_6 USING (encounter_id)
                 LEFT JOIN mamba_flat_encounter_follow_up_7 follow_up_7 USING (encounter_id)
                 LEFT JOIN mamba_flat_encounter_follow_up_8 follow_up_8 USING (encounter_id)
                 LEFT JOIN mamba_flat_encounter_follow_up_9 follow_up_9 USING (encounter_id)
        WHERE follow_up.client_id IS NOT NULL
          AND follow_up_date_followup_ IS NOT NULL
          AND follow_up_status IS NOT NULL
    ),

         ART_Initiation AS (
             SELECT
                 PatientId,
                 MIN(art_start_date) AS art_start_date
             FROM FollowUpEncounters
             WHERE art_start_date IS NOT NULL
               AND art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
             GROUP BY PatientId
         ),

         IntervalsDef AS (
             SELECT 0 AS interval_month
             UNION ALL SELECT 6
             UNION ALL SELECT 12
             UNION ALL SELECT 24
             UNION ALL SELECT 36
         ),

         PatientIntervals AS (
             SELECT
                 a.PatientId,
                 a.art_start_date,
                 i.interval_month,
                 fn_ethiopian_to_gregorian_calendar(
                         DATE_ADD(
                                 fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                 INTERVAL i.interval_month MONTH
                         )
                 ) AS interval_end_date,
                 LAG(
                         fn_ethiopian_to_gregorian_calendar(
                                 DATE_ADD(
                                         fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                         INTERVAL i.interval_month MONTH
                                 )
                         ), 1, REPORT_START_DATE
                 ) OVER (PARTITION BY a.PatientId ORDER BY i.interval_month) AS interval_start_date
             FROM ART_Initiation a
                      CROSS JOIN IntervalsDef i
         ),

         LatestFollowUpInInterval AS (
             SELECT
                 pi.PatientId,
                 pi.interval_month,
                 pi.interval_start_date,
                 pi.interval_end_date,
                 f.follow_up_date,
                 f.regimen,
                 f.ARTDoseDays,
                 f.follow_up_status,
                 f.AdherenceLevel,
                 f.pregnancy_status,
                 f.treatment_end_date,
                 f.next_visit_date,
                 f.cd4_count,
                 f.viral_load_count,
                 f.cd4_percent,
                 f.current_functional_status,
                 fn_get_ti_status(pi.PatientId, f.art_start_date, pi.interval_end_date) AS ti_status,
                 ROW_NUMBER() OVER (
                     PARTITION BY pi.PatientId, pi.interval_month
                     ORDER BY f.follow_up_date DESC, f.encounter_id DESC
                     ) AS rn
             FROM PatientIntervals pi
                      LEFT JOIN FollowUpEncounters f
                                ON pi.PatientId = f.PatientId
                                    AND f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date
         ),

         LatestViralLoadSentInInterval AS (
             SELECT
                 pi.PatientId,
                 pi.interval_month,
                 f.viral_load_sent_date,
                 ROW_NUMBER() OVER (
                     PARTITION BY pi.PatientId, pi.interval_month
                     ORDER BY f.viral_load_sent_date DESC, f.encounter_id DESC
                     ) AS rn_vl_sent
             FROM PatientIntervals pi
                      JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
             WHERE f.viral_load_sent_date BETWEEN pi.interval_start_date AND pi.interval_end_date
               AND f.viral_load_sent_date IS NOT NULL
         ),

         LatestViralLoadPerformedInInterval AS (
             SELECT
                 pi.PatientId,
                 pi.interval_month,
                 f.viral_load_received_date,
                 f.viral_load_result,
                 f.cd4_count,
                 f.viral_load_count,
                 f.cd4_percent,
                 f.viral_load_sent_date,
                 ROW_NUMBER() OVER (
                     PARTITION BY pi.PatientId, pi.interval_month
                     ORDER BY f.viral_load_received_date DESC, f.encounter_id DESC
                     ) AS rn_vl_performed
             FROM PatientIntervals pi
                      JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
             WHERE f.viral_load_received_date BETWEEN pi.interval_start_date AND pi.interval_end_date
               AND f.viral_load_received_date IS NOT NULL AND f.viral_load_count > 0
         ),

         LatestVisitectDateInInterval AS (
             SELECT
                 pi.PatientId,
                 pi.interval_month,
                 f.visitect_cd4_test_date,
                 f.visitect_cd4_result,
                 ROW_NUMBER() OVER (
                     PARTITION BY pi.PatientId, pi.interval_month
                     ORDER BY f.visitect_cd4_test_date DESC, f.encounter_id DESC
                     ) AS rn_visitect_performed
             FROM PatientIntervals pi
                      JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
             WHERE f.visitect_cd4_test_date BETWEEN pi.interval_start_date AND pi.interval_end_date
               AND f.visitect_cd4_test_date IS NOT NULL
         ),

         -- Collect latest Follow up for each interval and also join vl performed and visitect tests
         CohortDetails_TMP AS (
             SELECT
                 lfu.PatientId,
                 lfu.interval_start_date,
                 lfu.interval_end_date,
                 lfu.interval_month,
                 CASE
                     WHEN lfu.follow_up_status IN ('Dead', 'Transferred out', 'Stop all', 'Loss to follow-up (LTFU)', 'Ran away')
                         THEN lfu.follow_up_status
                     WHEN lfu.follow_up_status IN ('Alive', 'Restart medication')
                         AND lfu.treatment_end_date >= lfu.interval_end_date
                         THEN 'Active'
                     WHEN lfu.follow_up_status IN ('Alive', 'Restart medication')
                         AND lfu.treatment_end_date < lfu.interval_end_date
                         THEN 'Loss to follow-up (LTFU)'
                     WHEN lfu.follow_up_date IS NULL
                         THEN 'No Follow-up in Interval'
                     ELSE 'Unknown Status'
                     END AS initial_outcome,
                 lfu.follow_up_date,
                 lfu.regimen,
                 lfu.treatment_end_date,
                 lfu.ti_status, viral_load_performed.viral_load_count, lfu.ARTDoseDays, lfu.follow_up_status, lfu.AdherenceLevel,
                 lfu.pregnancy_status, lfu.next_visit_date, viral_load_performed.viral_load_sent_date,
                 viral_load_performed.viral_load_received_date, viral_load_performed.viral_load_result,
                 CASE visitect_performed.visitect_cd4_result
                     WHEN 'VISITECT <=200 copies/ml' THEN 'VISITECT >200 copies/ml'
                     ELSE visitect_performed.visitect_cd4_result
                     END AS visitect_cd4_result,
                 visitect_performed.visitect_cd4_test_date, LatestFollowUpInInterval.cd4_count,
                 viral_load_performed.cd4_percent, lfu.current_functional_status
             FROM (SELECT * FROM LatestFollowUpInInterval WHERE rn = 1) lfu
                      LEFT JOIN (SELECT * FROM LatestViralLoadPerformedInInterval WHERE rn_vl_performed = 1) viral_load_performed
                                ON lfu.PatientId = viral_load_performed.PatientId AND lfu.interval_month = viral_load_performed.interval_month
                      LEFT JOIN (SELECT * FROM LatestVisitectDateInInterval WHERE rn_visitect_performed = 1) visitect_performed
                                ON lfu.PatientId = visitect_performed.PatientId AND lfu.interval_month = visitect_performed.interval_month
         ),

         CohortDetails_With_LAGS AS (
             SELECT
                 *,
                 LAG(initial_outcome, 1) OVER (PARTITION BY PatientId ORDER BY interval_month) AS previous_initial_outcome,
                 LAG(treatment_end_date, 1) OVER (PARTITION BY PatientId ORDER BY interval_month) AS previous_treatment_end_date,
                 LAG(regimen, 1) OVER (PARTITION BY PatientId ORDER BY interval_month) AS previous_regimen
             FROM CohortDetails_TMP
         ),

         CohortDetails_With_Terminal_Flag AS (
             SELECT
                 *,
                 CASE
                     WHEN initial_outcome IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                         THEN 1
                     ELSE 0
                     END AS is_terminal_event
             FROM CohortDetails_With_LAGS
         ),

         CohortDetails_With_Terminal_Group AS (
             SELECT
                 *,
                 SUM(is_terminal_event) OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS terminal_group_id
             FROM CohortDetails_With_Terminal_Flag
         ),

         CohortDetails_With_Last_Terminal_Status AS (
             SELECT
                 *,
                 FIRST_VALUE(initial_outcome) OVER (PARTITION BY PatientId, terminal_group_id ORDER BY interval_month) AS last_terminal_status
             FROM CohortDetails_With_Terminal_Group
         ),

         CohortDetails AS (
             SELECT
                 *,
                 CASE
                     WHEN last_terminal_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                         THEN last_terminal_status

                     WHEN initial_outcome <> 'No Follow-up in Interval'
                         THEN initial_outcome

                     WHEN initial_outcome = 'No Follow-up in Interval' THEN
                         CASE
                             WHEN previous_treatment_end_date IS NOT NULL AND previous_treatment_end_date >= interval_end_date
                                 THEN 'Active - Carry Forward Rx'
                             ELSE 'Loss to follow-up (LTFU)'
                             END

                     ELSE 'Unknown Status - Final Check'
                     END AS final_cohort_outcome,

                 CASE
                     WHEN initial_outcome = 'No Follow-up in Interval' AND
                          previous_treatment_end_date IS NOT NULL AND
                          previous_treatment_end_date >= interval_end_date
                         THEN previous_regimen
                     ELSE regimen
                     END AS final_regimen

             FROM CohortDetails_With_Last_Terminal_Status
                      JOIN mamba_dim_client client on client.client_id = CohortDetails_With_Last_Terminal_Status.PatientId
         )

    SELECT dc.patient_name                                                                       AS 'Patient Name',
           dc.patient_uuid                                                                       AS 'UUID',
           CAST(dc.mrn AS CHAR(20))                                                              AS 'MRN',
           dc.UAN,
           dc.Sex,
           dc.date_of_birth                                                                      AS 'Date of Birth',
           ai.art_start_date                                                                     AS 'ART Start Date',
           YEAR(ai.art_start_date)                                                               AS 'ART Start Year',
           QUARTER(ai.art_start_date)                                                            AS 'ART Start Quarter',

           -- Pivot columns for each interval
           -- 0 Months
           MAX(CASE
                   WHEN co.interval_month = 0 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                AS 'Age at 0 Months',
           MAX(CASE
                   WHEN co.interval_month = 0 THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '5%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '2%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '6%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '3%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'

                           ELSE NULL
                           END
                   ELSE NULL END)                                                                AS 'Regimen Line at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN NULL ELSE NULL END)                          AS 'Outcome at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_date ELSE NULL END)             AS 'Latest Follow-Up Date at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_date ELSE NULL END)             AS 'Latest Follow-Up Date at Zero Months EC.',
           MAX(CASE WHEN co.interval_month = 0 THEN co.final_regimen ELSE NULL END)                    AS 'Latest Regimen at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.ARTDoseDays ELSE NULL END)                AS 'Latest Regimen Dose Days at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_status ELSE NULL END)           AS 'Latest Follow-up Status at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.AdherenceLevel ELSE NULL END)             AS 'Latest Adherence at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.pregnancy_status ELSE NULL END)           AS 'Pregnant at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.treatment_end_date ELSE NULL END)         AS 'Treatment End Date at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.treatment_end_date ELSE NULL END)         AS 'Treatment End Date at Zero Months EC.',
           MAX(CASE WHEN co.interval_month = 0 THEN co.next_visit_date ELSE NULL END)            AS 'Next Visit date at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.next_visit_date ELSE NULL END)            AS 'Next Visit date at Zero Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 0 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at Zero Months',
           MAX(CASE
                   WHEN co.interval_month = 0 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at Zero Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 0 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at Zero Months',
           MAX(CASE
                   WHEN co.interval_month = 0 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at Zero Months EC.',
           MAX(CASE WHEN co.interval_month = 0 THEN co.viral_load_count ELSE NULL END)           AS `Latest Viral Load Count at Zero Months`,
           MAX(CASE WHEN co.interval_month = 0 THEN co.viral_load_result ELSE NULL END)          AS 'Latest Viral Load Status at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.cd4_count ELSE NULL END)                  AS 'Latest CD4 Count at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.cd4_percent ELSE NULL END)                AS 'Latest CD4 % at Zero Months',
           MAX(CASE
                   WHEN co.interval_month = 0 THEN co.visitect_cd4_test_date
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Date at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.visitect_cd4_result ELSE NULL END)        AS 'Latest Visitect Test Result at Zero Months',
           MAX(CASE
                   WHEN co.interval_month = 0 THEN co.current_functional_status
                   ELSE NULL END)                                                                AS 'Latest Function Status at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.ti_status ELSE NULL END)                  AS `Latest TI Status at Zero Months`,

           -- 6 Months
           MAX(CASE
                   WHEN co.interval_month = 6 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                AS 'Age at 6 Months',
           MAX(CASE
                   WHEN co.interval_month = 6 THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '5%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '2%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '6%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '3%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'

                           ELSE NULL
                           END
                   ELSE NULL END)                                                                AS 'Regimen Line at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.final_cohort_outcome ELSE NULL END)                    AS 'Outcome at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_date ELSE NULL END)             AS 'Latest Follow-Up Date at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_date ELSE NULL END)             AS 'Latest Follow-Up Date at 6 Months EC.',
           MAX(CASE WHEN co.interval_month = 6 THEN co.final_regimen ELSE NULL END)                    AS 'Latest Regimen at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.ARTDoseDays ELSE NULL END)                AS 'Latest Regimen Dose Days at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_status ELSE NULL END)           AS 'Latest Follow-up Status at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.AdherenceLevel ELSE NULL END)             AS 'Latest Adherence at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.pregnancy_status ELSE NULL END)           AS 'Pregnant at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.treatment_end_date ELSE NULL END)         AS 'Treatment End Date at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.treatment_end_date ELSE NULL END)         AS 'Treatment End Date at 6 Months EC.',
           MAX(CASE WHEN co.interval_month = 6 THEN co.next_visit_date ELSE NULL END)            AS 'Next Visit date at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.next_visit_date ELSE NULL END)            AS 'Next Visit date at 6 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 6 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at 6 Months',
           MAX(CASE
                   WHEN co.interval_month = 6 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at 6 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 6 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at 6 Months',
           MAX(CASE
                   WHEN co.interval_month = 6 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at 6 Months EC.',
           MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_count ELSE NULL END)           AS `Latest Viral Load Count at 6 Months`,
           MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_result ELSE NULL END)          AS 'Latest Viral Load Status at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.cd4_count ELSE NULL END)                  AS 'Latest CD4 Count at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.cd4_percent ELSE NULL END)                AS 'Latest CD4 % at 6 Months',
           MAX(CASE
                   WHEN co.interval_month = 6 THEN co.visitect_cd4_test_date
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Date at 6 Months GC.',
           MAX(CASE
                   WHEN co.interval_month = 6 THEN co.visitect_cd4_test_date
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Date at 6 Months EC.',
           MAX(CASE WHEN co.interval_month = 6 THEN co.visitect_cd4_result ELSE NULL END)        AS 'Latest Visitect Test Result at 6 Months',
           MAX(CASE
                   WHEN co.interval_month = 6 THEN co.current_functional_status
                   ELSE NULL END)                                                                AS 'Latest Function Status at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.ti_status ELSE NULL END)                  AS `Latest TI Status at 6 Months`,

           -- 12 Months
           MAX(CASE
                   WHEN co.interval_month = 12 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                AS 'Age at 12 Months',
           MAX(CASE
                   WHEN co.interval_month = 12 THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '5%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '2%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '6%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '3%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'

                           ELSE NULL
                           END
                   ELSE NULL END)                                                                AS 'Regimen Line at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.final_cohort_outcome ELSE NULL END)                   AS 'Outcome at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_date ELSE NULL END)            AS 'Latest Follow-Up Date at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_date ELSE NULL END)            AS 'Latest Follow-Up Date at 12 Months EC.',
           MAX(CASE WHEN co.interval_month = 12 THEN co.final_regimen ELSE NULL END)                   AS 'Latest Regimen at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.ARTDoseDays ELSE NULL END)               AS 'Latest Regimen Dose Days at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_status ELSE NULL END)          AS 'Latest Follow-up Status at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.AdherenceLevel ELSE NULL END)            AS 'Latest Adherence at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.pregnancy_status ELSE NULL END)          AS 'Pregnant at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.treatment_end_date ELSE NULL END)        AS 'Treatment End Date at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.treatment_end_date ELSE NULL END)        AS 'Treatment End Date at 12 Months EC.',
           MAX(CASE WHEN co.interval_month = 12 THEN co.next_visit_date ELSE NULL END)           AS 'Next Visit date at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.next_visit_date ELSE NULL END)           AS 'Next Visit date at 12 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 12 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at 12 Months',
           MAX(CASE
                   WHEN co.interval_month = 12 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at 12 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 12 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at 12 Months',
           MAX(CASE
                   WHEN co.interval_month = 12 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at 12 Months EC.',
           MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_count ELSE NULL END)          AS `Latest Viral Load Count at 12 Months`,
           MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_result ELSE NULL END)         AS 'Latest Viral Load Status at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.cd4_count ELSE NULL END)                 AS 'Latest CD4 Count at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.cd4_percent ELSE NULL END)               AS 'Latest CD4 % at 12 Months',
           MAX(CASE
                   WHEN co.interval_month = 12 THEN co.visitect_cd4_test_date
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Date at 12 Months GC.',
           MAX(CASE
                   WHEN co.interval_month = 12 THEN co.visitect_cd4_test_date
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Date at 12 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 12 THEN co.visitect_cd4_result
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Result at 12 Months',
           MAX(CASE
                   WHEN co.interval_month = 12 THEN co.current_functional_status
                   ELSE NULL END)                                                                AS 'Latest Function Status at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.ti_status ELSE NULL END)                 AS `Latest TI Status at 12 Months`,

           -- 24 Months
           MAX(CASE
                   WHEN co.interval_month = 24 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                AS 'Age at 24 Months',
           MAX(CASE
                   WHEN co.interval_month = 24 THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '5%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '2%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '6%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '3%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'

                           ELSE NULL
                           END
                   ELSE NULL END)                                                                AS 'Regimen Line at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.final_cohort_outcome ELSE NULL END)                   AS 'Outcome at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_date ELSE NULL END)            AS 'Latest Follow-Up Date at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_date ELSE NULL END)            AS 'Latest Follow-Up Date at 24 Months EC.',
           MAX(CASE WHEN co.interval_month = 24 THEN co.final_regimen ELSE NULL END)                   AS 'Latest Regimen at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.ARTDoseDays ELSE NULL END)               AS 'Latest Regimen Dose Days at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_status ELSE NULL END)          AS 'Latest Follow-up Status at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.AdherenceLevel ELSE NULL END)            AS 'Latest Adherence at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.pregnancy_status ELSE NULL END)          AS 'Pregnant at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.treatment_end_date ELSE NULL END)        AS 'Treatment End Date at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.treatment_end_date ELSE NULL END)        AS 'Treatment End Date at 24 Months EC.',
           MAX(CASE WHEN co.interval_month = 24 THEN co.next_visit_date ELSE NULL END)           AS 'Next Visit date at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.next_visit_date ELSE NULL END)           AS 'Next Visit date at 24 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 24 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at 24 Months',
           MAX(CASE
                   WHEN co.interval_month = 24 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at 24 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 24 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at 24 Months',
           MAX(CASE
                   WHEN co.interval_month = 24 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at 24 Months EC.',
           MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_count ELSE NULL END)          AS `Latest Viral Load Count at 24 Months`,
           MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_result ELSE NULL END)         AS 'Latest Viral Load Status at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.cd4_count ELSE NULL END)                 AS 'Latest CD4 Count at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.cd4_percent ELSE NULL END)               AS 'Latest CD4 % at 24 Months',
           MAX(CASE
                   WHEN co.interval_month = 24 THEN co.visitect_cd4_test_date
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Date at 24 Months GC.',
           MAX(CASE
                   WHEN co.interval_month = 24 THEN co.visitect_cd4_test_date
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Date at 24 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 24 THEN co.visitect_cd4_result
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Result at 24 Months',
           MAX(CASE
                   WHEN co.interval_month = 24 THEN co.current_functional_status
                   ELSE NULL END)                                                                AS 'Latest Function Status at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.ti_status ELSE NULL END)                 AS `Latest TI Status at 24 Months`,


           -- 36 Months
           MAX(CASE
                   WHEN co.interval_month = 36 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                AS 'Age at 36 Months',
           MAX(CASE
                   WHEN co.interval_month = 36 THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On Original 1st Line Regimen'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '1%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '4%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx')
                               THEN 'On Alternate 1st Line Regimen (Substituted)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '5%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '2%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 2nd Line Regimen (Switched)'

                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND
                                co.final_regimen LIKE '6%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND
                                co.final_regimen LIKE '3%' AND final_cohort_outcome IN ('Active','Active - Carry Forward Rx') THEN 'On 3rd Line Regimen (Switched)'

                           ELSE NULL
                           END
                   ELSE NULL END)                                                                AS 'Regimen Line at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.final_cohort_outcome ELSE NULL END)                   AS 'Outcome at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_date ELSE NULL END)            AS 'Latest Follow-Up Date at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_date ELSE NULL END)            AS 'Latest Follow-Up Date at 36 Months EC.',
           MAX(CASE WHEN co.interval_month = 36 THEN co.final_regimen ELSE NULL END)                   AS 'Latest Regimen at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.ARTDoseDays ELSE NULL END)               AS 'Latest Regimen Dose Days at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_status ELSE NULL END)          AS 'Latest Follow-up Status at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.AdherenceLevel ELSE NULL END)            AS 'Latest Adherence at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.pregnancy_status ELSE NULL END)          AS 'Pregnant at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.treatment_end_date ELSE NULL END)        AS 'Treatment End Date at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.treatment_end_date ELSE NULL END)        AS 'Treatment End Date at 36 Months EC.',
           MAX(CASE WHEN co.interval_month = 36 THEN co.next_visit_date ELSE NULL END)           AS 'Next Visit date at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.next_visit_date ELSE NULL END)           AS 'Next Visit date at 36 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 36 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at 36 Months',
           MAX(CASE
                   WHEN co.interval_month = 36 THEN co.viral_load_sent_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Sent Date at 36 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 36 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at 36 Months',
           MAX(CASE
                   WHEN co.interval_month = 36 THEN co.viral_load_received_date
                   ELSE NULL END)                                                                AS 'Latest Viral Load Received Date at 36 Months EC.',
           MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_count ELSE NULL END)          AS `Latest Viral Load Count at 36 Months`,
           MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_result ELSE NULL END)         AS 'Latest Viral Load Status at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.cd4_count ELSE NULL END)                 AS 'Latest CD4 Count at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.cd4_percent ELSE NULL END)               AS 'Latest CD4 % at 36 Months',
           MAX(CASE
                   WHEN co.interval_month = 36 THEN co.visitect_cd4_test_date
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Date at 36 Months GC.',
           MAX(CASE
                   WHEN co.interval_month = 36 THEN co.visitect_cd4_test_date
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Date at 36 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 36 THEN co.visitect_cd4_result
                   ELSE NULL END)                                                                AS 'Latest Visitect Test Result at 36 Months',
           MAX(CASE
                   WHEN co.interval_month = 36 THEN co.current_functional_status
                   ELSE NULL END)                                                                AS 'Latest Function Status at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.ti_status ELSE NULL END)                 AS `Latest TI Status at 36 Months`

    FROM ART_Initiation ai
             JOIN
         mamba_dim_client dc ON ai.PatientId = dc.client_id
             LEFT JOIN
         CohortDetails co ON ai.PatientId = co.PatientId
    WHERE ai.art_start_date <= COALESCE(REPORT_END_DATE, CURDATE())
    GROUP BY ai.PatientId, dc.patient_name, dc.patient_uuid, dc.mrn, dc.UAN, dc.Sex, dc.date_of_birth, ai.art_start_date
    ORDER BY ai.art_start_date, dc.patient_name;

END //

DELIMITER ;