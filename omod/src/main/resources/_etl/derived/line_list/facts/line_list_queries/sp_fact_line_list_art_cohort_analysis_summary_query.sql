DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_art_cohort_analysis_summary_query;

CREATE PROCEDURE sp_fact_line_list_art_cohort_analysis_summary_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUpEncounters AS (SELECT follow_up.encounter_id,
                                       follow_up.client_id                 AS PatientId,
                                       follow_up_status,
                                       follow_up_date_followup_            AS follow_up_date,
                                       art_antiretroviral_start_date       AS art_start_date,
                                       treatment_end_date,
                                       regimen,
                                       antiretroviral_art_dispensed_dose_i AS ARTDoseDays,
                                       anitiretroviral_adherence_level     AS AdherenceLevel,
                                       pregnancy_status,
                                       next_visit_date,
                                       date_of_reported_hiv_viral_load     AS viral_load_sent_date,
                                       date_viral_load_results_received    AS viral_load_received_date,
                                       viral_load_test_status              AS viral_load_result,
                                       hiv_viral_load                      as viral_load_count,
                                       cd4_count,
                                       cd4_                                as cd4_percent,
                                       current_functional_status,
                                       visitect_cd4_result,
                                       visitect_cd4_test_date
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
                                WHERE follow_up.client_id IS NOT NULL
                                  AND follow_up_date_followup_ IS NOT NULL
                                  AND follow_up_status IS NOT NULL),

         -- Initiated ART in Period
         ART_Initiation AS (SELECT PatientId, MIN(art_start_date) AS art_start_date
                            FROM FollowUpEncounters
                            WHERE art_start_date IS NOT NULL
                              and art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                            GROUP BY PatientId),
         -- To enable Dynamically Generating Interval Periods
         IntervalsDef AS (SELECT 0 AS interval_month
                          UNION ALL
                          SELECT 6
                          UNION ALL
                          SELECT 12
                          UNION ALL
                          SELECT 24
                          UNION ALL
                          SELECT 36),
         -- Generate Intervals For Each Client
         PatientIntervals AS (SELECT a.PatientId,
                                     a.art_start_date,
                                     i.interval_month,
                                     fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                             fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL
                                             i.interval_month MONTH))                              as interval_end_date,
                                     LAG(fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                             fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL
                                             i.interval_month MONTH)), 1,
                                         REPORT_START_DATE)
                                         OVER (PARTITION BY a.PatientId ORDER BY i.interval_month) as interval_start_date
                              FROM ART_Initiation a
                                       CROSS JOIN IntervalsDef i),
         -- Collect Follow-ups for each interval
         LatestFollowUpInInterval AS (SELECT pi.PatientId,
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
                                             f.viral_load_count,
                                             f.current_functional_status,
                                             fn_get_ti_status(pi.PatientId, f.art_start_date,
                                                              pi.interval_end_date) AS ti_status,
                                             ROW_NUMBER() OVER (
                                                 PARTITION BY pi.PatientId, pi.interval_month
                                                 ORDER BY f.follow_up_date DESC, f.encounter_id DESC
                                                 )                                  AS rn
                                      FROM PatientIntervals pi
                                               LEFT JOIN FollowUpEncounters f
                                                         ON pi.PatientId = f.PatientId
                                                             AND
                                                            f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date),

         LatestCd4InInterval AS (SELECT pi.PatientId,
                                        pi.interval_month,
                                        f.cd4_count,
                                        f.cd4_percent,
                                        f.follow_up_date,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY pi.PatientId, pi.interval_month
                                            ORDER BY f.follow_up_date DESC, f.encounter_id DESC
                                            ) AS rn_cd4
                                 FROM PatientIntervals pi
                                          JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
                                 WHERE f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date
                                     AND f.cd4_count IS NOT NULL OR f.cd4_percent IS NOT NULL ),

         LatestViralLoadPerformedInInterval AS (SELECT pi.PatientId,
                                                       pi.interval_month,
                                                       f.viral_load_received_date,
                                                       f.viral_load_result,
                                                       f.viral_load_count,
                                                       f.viral_load_sent_date,
                                                       ROW_NUMBER() OVER (
                                                           PARTITION BY pi.PatientId, pi.interval_month
                                                           ORDER BY f.viral_load_received_date DESC, f.encounter_id DESC
                                                           ) AS rn_vl_performed
                                                FROM PatientIntervals pi
                                                         JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
                                                WHERE f.viral_load_received_date BETWEEN pi.interval_start_date AND pi.interval_end_date
                                                  AND f.viral_load_received_date IS NOT NULL
                                                  AND f.viral_load_count > 0),

         LatestVisitectDateInInterval AS (SELECT pi.PatientId,
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
                                            AND f.visitect_cd4_test_date IS NOT NULL),

         -- Collect latest Follow up for each interval and also join vl performed and visitect tests
         CohortDetails_TMP AS (SELECT lfu.PatientId,
                                      lfu.interval_start_date,
                                      lfu.interval_end_date,
                                      lfu.interval_month,
                                      CASE
                                          WHEN lfu.follow_up_status IN
                                               ('Dead', 'Transferred out', 'Stop all', 'Loss to follow-up (LTFU)',
                                                'Ran away')
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
                                      lfu.ti_status,
                                      viral_load_performed.viral_load_count,
                                      lfu.ARTDoseDays,
                                      lfu.follow_up_status,
                                      lfu.AdherenceLevel,
                                      lfu.pregnancy_status,
                                      lfu.next_visit_date,
                                      cd4_performed.cd4_count,
                                      cd4_performed.cd4_percent,
                                      viral_load_performed.viral_load_sent_date,
                                      viral_load_performed.viral_load_received_date,
                                      viral_load_performed.viral_load_result,
                                      CASE visitect_performed.visitect_cd4_result
                                          WHEN 'VISITECT <=200 copies/ml' THEN 'VISITECT >200 copies/ml'
                                          ELSE visitect_performed.visitect_cd4_result
                                          END AS visitect_cd4_result,
                                      visitect_performed.visitect_cd4_test_date,
                                      lfu.current_functional_status
                               FROM (SELECT * FROM LatestFollowUpInInterval WHERE rn = 1) lfu
                                        LEFT JOIN (SELECT *
                                                   FROM LatestViralLoadPerformedInInterval
                                                   WHERE rn_vl_performed = 1) viral_load_performed
                                                  ON lfu.PatientId = viral_load_performed.PatientId AND
                                                     lfu.interval_month = viral_load_performed.interval_month
                                        LEFT JOIN (SELECT *
                                                   FROM LatestCd4InInterval
                                                   WHERE rn_cd4 = 1) cd4_performed
                                                  ON lfu.PatientId = cd4_performed.PatientId AND
                                                     lfu.interval_month = cd4_performed.interval_month
                                        LEFT JOIN (SELECT *
                                                   FROM LatestVisitectDateInInterval
                                                   WHERE rn_visitect_performed = 1) visitect_performed
                                                  ON lfu.PatientId = visitect_performed.PatientId AND
                                                     lfu.interval_month = visitect_performed.interval_month),

         CohortDetails_With_LAGS AS (SELECT *,
                                            LAG(initial_outcome, 1)
                                                OVER (PARTITION BY PatientId ORDER BY interval_month)             AS previous_initial_outcome,
                                            LAG(treatment_end_date, 1)
                                                OVER (PARTITION BY PatientId ORDER BY interval_month)             AS previous_treatment_end_date,
                                            LAG(regimen, 1) OVER (PARTITION BY PatientId ORDER BY interval_month) AS previous_regimen
                                     FROM CohortDetails_TMP),

         CohortDetails_With_Terminal_Flag AS (SELECT *,
                                                     CASE
                                                         WHEN initial_outcome IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                                                             THEN 1
                                                         ELSE 0
                                                         END AS is_terminal_event
                                              FROM CohortDetails_With_LAGS),

         CohortDetails_With_Terminal_Group AS (SELECT *,
                                                      SUM(is_terminal_event)
                                                          OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS terminal_group_id
                                               FROM CohortDetails_With_Terminal_Flag),

         CohortDetails_With_Last_Terminal_Status AS (SELECT *,
                                                            FIRST_VALUE(initial_outcome)
                                                                        OVER (PARTITION BY PatientId, terminal_group_id ORDER BY interval_month) AS last_terminal_status
                                                     FROM CohortDetails_With_Terminal_Group),

         CohortDetails AS (SELECT *,
                                  CASE
                                      WHEN last_terminal_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                                          THEN last_terminal_status

                                      WHEN initial_outcome <> 'No Follow-up in Interval'
                                          THEN initial_outcome

                                      WHEN initial_outcome = 'No Follow-up in Interval' THEN
                                          CASE
                                              WHEN previous_treatment_end_date IS NOT NULL AND
                                                   previous_treatment_end_date >= interval_end_date
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
                                    JOIN mamba_dim_client client
                                         on client.client_id = CohortDetails_With_Last_Terminal_Status.PatientId)

    SELECT 'A. Started on ART in this clinic: original cohort'                            AS Name,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'B. Transfer in Add+'     AS Name,
           0                         AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month <= 6 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month <= 12 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month <= 24 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month <= 36 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'C. Transfers Out Subtract -'                  AS Name,
           0                                              AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND final_cohort_outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND final_cohort_outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND final_cohort_outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND final_cohort_outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'D. Net current cohort'                                       AS Name,
           0                                                             AS 'Month 0',
           CAST(GREATEST(0, IFNULL(((SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) +
                                     SUM(CASE WHEN interval_month = 6 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                    SUM(CASE
                                            WHEN interval_month = 6 AND final_cohort_outcome = 'Transferred out' THEN 1
                                            ELSE 0 END)), 0)) AS SIGNED) AS 'Month 6',
           CAST(GREATEST(0, IFNULL(((SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) +
                                     SUM(CASE WHEN interval_month = 12 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                    SUM(CASE
                                            WHEN interval_month = 12 AND final_cohort_outcome = 'Transferred out' THEN 1
                                            ELSE 0 END)), 0)) AS SIGNED) AS 'Month 12',
           CAST(GREATEST(0, IFNULL(((SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) +
                                     SUM(CASE WHEN interval_month = 24 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                    SUM(CASE
                                            WHEN interval_month = 24 AND final_cohort_outcome = 'Transferred out' THEN 1
                                            ELSE 0 END)), 0)) AS SIGNED) AS 'Month 24',
           CAST(GREATEST(0, IFNULL(((SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) +
                                     SUM(CASE WHEN interval_month = 36 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                    SUM(CASE
                                            WHEN interval_month = 36 AND final_cohort_outcome = 'Transferred out' THEN 1
                                            ELSE 0 END)), 0)) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'E. On Original 1st Line Regimen' AS Name,
           0                                 AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '4%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '1%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)      AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '4%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '1%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)      AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '4%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '1%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)      AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '4%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '1%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)      AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'F. On Alternate 1st Line Regimen (Substituted)' AS Name,
           0                                                AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '1%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '4%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)                     AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '1%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '4%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)                     AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '1%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '4%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)                     AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '1%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '4%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)                     AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'G. On 2nd Line Regimen (Switched)' AS Name,
           0                                   AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '5%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '2%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)        AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '5%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '2%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)        AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '5%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '2%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)        AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '2%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)        AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'H. On 3rd Line Regimen (Switched)' AS Name,
           0                                   AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '3%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)        AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '3%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)        AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '3%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)        AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                     final_regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                     final_regimen LIKE '3%') AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)        AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'I. Stopped'                                   AS Name,
           0                                              AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND final_cohort_outcome = 'Stop all' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND final_cohort_outcome = 'Stop all' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND final_cohort_outcome = 'Stop all' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND final_cohort_outcome = 'Stop all' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'J. Died'                 AS Name,
           0                         AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 AND final_cohort_outcome = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 AND final_cohort_outcome = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS
                                        'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 AND final_cohort_outcome = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS
                                        'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 AND final_cohort_outcome = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS
                                        'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'K. Lost to Follow-up (DROP)'                  AS Name,
           0                                              AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (final_cohort_outcome = 'Ran away' OR
                                     final_cohort_outcome = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (final_cohort_outcome = 'Ran away' OR
                                     final_cohort_outcome = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (final_cohort_outcome = 'Ran away' OR
                                     final_cohort_outcome = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (final_cohort_outcome = 'Ran away' OR
                                     final_cohort_outcome = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails


    UNION ALL
    SELECT 'Percent of cohort alive and on ART'          AS Name,
           0                                             AS 'Month 0',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 6 AND
                                                 final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(GREATEST(0, IFNULL(((SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) +
                                                                SUM(CASE WHEN interval_month = 6 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                                               SUM(CASE
                                                                       WHEN interval_month = 6 AND final_cohort_outcome = 'Transferred out'
                                                                           THEN 1
                                                                       ELSE 0 END)), 0)), 0),
                                    0)) AS SIGNED), '%') AS 'Month 6',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 12 AND
                                                 final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(GREATEST(0, IFNULL(((SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) +
                                                                SUM(CASE WHEN interval_month = 12 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                                               SUM(CASE
                                                                       WHEN interval_month = 12 AND final_cohort_outcome = 'Transferred out'
                                                                           THEN 1
                                                                       ELSE 0 END)), 0)), 0),
                                    0)) AS SIGNED), '%') AS 'Month 12',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 24 AND
                                                 final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(GREATEST(0, IFNULL(((SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) +
                                                                SUM(CASE WHEN interval_month = 24 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                                               SUM(CASE
                                                                       WHEN interval_month = 24 AND final_cohort_outcome = 'Transferred out'
                                                                           THEN 1
                                                                       ELSE 0 END)), 0)), 0),
                                    0)) AS SIGNED), '%') AS 'Month 24',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 36 AND
                                                 final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(GREATEST(0, IFNULL(((SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) +
                                                                SUM(CASE WHEN interval_month = 36 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                                               SUM(CASE
                                                                       WHEN interval_month = 36 AND final_cohort_outcome = 'Transferred out'
                                                                           THEN 1
                                                                       ELSE 0 END)), 0)), 0),
                                    0)) AS SIGNED), '%') AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'L. Mean CD4 % (for children)'                           AS Name,
           0                                                        AS 'Month 0',
           CAST(ROUND(IFNULL(AVG(CASE
                                     WHEN interval_month = 6 AND
                                          TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15
                                         THEN cd4_percent
                                     ELSE NULL END), 0)) AS SIGNED) AS 'Month 6',
           CAST(ROUND(IFNULL(AVG(CASE
                                     WHEN interval_month = 12 AND
                                          TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15
                                         THEN cd4_percent
                                     ELSE NULL END), 0)) AS SIGNED) AS 'Month 12',
           CAST(ROUND(IFNULL(AVG(CASE
                                     WHEN interval_month = 24 AND
                                          TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15
                                         THEN cd4_percent
                                     ELSE NULL END), 0)) AS SIGNED) AS 'Month 24',
           CAST(ROUND(IFNULL(AVG(CASE
                                     WHEN interval_month = 36 AND
                                          TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15
                                         THEN cd4_percent
                                     ELSE NULL END), 0)) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL

    --  Wherever there is visitect test collect test result
    SELECT 'M. CD4 median or proportion ≥ 200 (optional)' AS Name,
           0                                              AS 'Month 0',
           CAST(ROUND(IFNULL(SUM(CASE
                                     WHEN interval_month = 6 AND
                                          final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') and
                                          ((cd4_count is not null and
                                            cd4_count > 200) OR (visitect_cd4_test_date IS NOT NULL AND visitect_cd4_result IN ('VISITECT <=200 copies/ml','VISITECT >200 copies/ml'))) THEN 1
                                     ELSE 0 END) /
                             NULLIF(SUM(CASE
                                            WHEN interval_month = 6 AND
                                                 final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') and
                                                 (cd4_count is not null OR visitect_cd4_test_date IS NOT NULL)
                                                THEN 1
                                            ELSE 0 END),
                                    0), 0)) AS SIGNED)    AS 'Month 6',

           CAST(ROUND(IFNULL(SUM(CASE
                                     WHEN interval_month = 12 AND
                                          final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') and
                                          ((cd4_count is not null and
                                            cd4_count > 200) OR (visitect_cd4_test_date IS NOT NULL AND visitect_cd4_result IN ('VISITECT <=200 copies/ml','VISITECT >200 copies/ml'))) THEN 1
                                     ELSE 0 END) /
                             NULLIF(SUM(CASE
                                            WHEN interval_month = 12 AND
                                                 final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') and
                                                 (cd4_count is not null OR visitect_cd4_test_date IS NOT NULL)
                                                THEN 1
                                            ELSE 0 END),
                                    0), 0)) AS SIGNED)    AS 'Month 12',
           CAST(ROUND(IFNULL(SUM(CASE
                                     WHEN interval_month = 24 AND
                                          final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') and
                                          ((cd4_count is not null and
                                            cd4_count > 200) OR (visitect_cd4_test_date IS NOT NULL AND visitect_cd4_result IN ('VISITECT <=200 copies/ml','VISITECT >200 copies/ml'))) THEN 1
                                     ELSE 0 END) /
                             NULLIF(SUM(CASE
                                            WHEN interval_month = 24 AND
                                                 final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') and
                                                 (cd4_count is not null OR visitect_cd4_test_date IS NOT NULL)
                                                THEN 1
                                            ELSE 0 END),
                                    0), 0)) AS SIGNED)    AS 'Month 24',
           CAST(ROUND(IFNULL(SUM(CASE
                                     WHEN interval_month = 36 AND
                                          final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') and
                                          ((cd4_count is not null and
                                          cd4_count > 200) OR (visitect_cd4_test_date IS NOT NULL AND visitect_cd4_result IN ('VISITECT <=200 copies/ml','VISITECT >200 copies/ml'))) THEN 1
                                     ELSE 0 END) /
                             NULLIF(SUM(CASE
                                            WHEN interval_month = 36 AND
                                                 final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') and
                                                 (cd4_count is not null OR visitect_cd4_test_date IS NOT NULL)
                                                THEN 1
                                            ELSE 0 END),
                                    0), 0)) AS SIGNED)    AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'N. Viral Load tested'                         AS Name,
           0                                              AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    viral_load_received_date IS NOT NULL  AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    viral_load_received_date IS NOT NULL  AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    viral_load_received_date IS NOT NULL  AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    viral_load_received_date IS NOT NULL  AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'O. Viral load Suppressed ( < 50 copies/ml)'   AS Name,
           0                                              AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND viral_load_received_date IS NOT NULL AND
                                      viral_load_count < 50 AND
                                      final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND viral_load_received_date IS NOT NULL AND
                                    viral_load_count < 50 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND viral_load_received_date IS NOT NULL AND
                                    viral_load_count < 50 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND viral_load_received_date IS NOT NULL AND
                                    viral_load_count < 50 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                   THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Functional Status [# W or A or B / D * 100]' AS Name,
           0                                             AS 'Month 0',
           0                                             AS 'Month 6',
           0                                             AS 'Month 12',
           0                                             AS 'Month 24',
           0                                             AS 'Month 36'
    UNION ALL
    SELECT 'Percentage Working'                                     AS Name,
           0                                                        AS 'Month 0',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 6 AND
                                                 current_functional_status = 'Physically able to work' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 6 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 6',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 12 AND
                                                 current_functional_status = 'Physically able to work' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 12 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 12',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 24 AND
                                                 current_functional_status = 'Physically able to work' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 24 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 24',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 36 AND
                                                 current_functional_status = 'Physically able to work' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 36 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Percentage Ambulatory'                                  AS Name,
           0                                                        AS 'Month 0',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 6 AND
                                                 current_functional_status = 'Patient ambulatory' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 6 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 6',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 12 AND
                                                 current_functional_status = 'Patient ambulatory' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 12 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 12',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 24 AND
                                                 current_functional_status = 'Patient ambulatory' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 24 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 24',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 36 AND
                                                 current_functional_status = 'Patient ambulatory' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 36 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Percentage Bedridden'                                   AS Name,
           0                                                        AS 'Month 0',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 6 AND
                                                 current_functional_status = 'Bedridden' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 6 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 6',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 12 AND
                                                 current_functional_status = 'Bedridden' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 12 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 12',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 24 AND
                                                 current_functional_status = 'Bedridden' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 24 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 24',
           CONCAT(CAST(ROUND(IFNULL(SUM(CASE
                                            WHEN interval_month = 36 AND
                                                 current_functional_status = 'Bedridden' AND
                                                 final_cohort_outcome = 'Active' THEN 1
                                            ELSE 0 END) * 100 /
                                    NULLIF(CAST(SUM(CASE
                                                        WHEN interval_month = 36 AND
                                                             current_functional_status IS NOT NULL AND
                                                             final_cohort_outcome = 'Active' THEN 1
                                                        ELSE 0 END) AS DECIMAL),
                                           0), 0)) AS SIGNED), '%') AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Number of persons who picked up ARVs each month for 6 months' AS Name,
           0                                                              AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) -
                IFNULL(SUM(CASE WHEN interval_month = 6 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                      AS 'Month 6',
           0                                                              AS 'Month 12',
           0                                                              AS 'Month 24',
           0                                                              AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Number of persons who picked up ARVs each month for 12 months' AS Name,
           0                                                               AS 'Month 0',
           0                                                               AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) -
                IFNULL(SUM(CASE WHEN interval_month = 12 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                       AS 'Month 12',
           0                                                               AS 'Month 24',
           0                                                               AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Number of persons who picked up ARVs each month for 24 months' AS Name,
           0                                                               AS 'Month 0',
           0                                                               AS 'Month 6',
           0                                                               AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) -
                IFNULL(SUM(CASE WHEN interval_month = 24 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                       AS 'Month 24',
           0                                                               AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Number of persons who picked up ARVs each month for 36 months ' AS Name,
           0                                                                AS 'Month 0',
           0                                                                AS 'Month 6',
           0                                                                AS 'Month 12',
           0                                                                AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) -
                IFNULL(SUM(CASE WHEN interval_month = 36 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                        AS 'Month 36'
    FROM CohortDetails;

END //

DELIMITER ;