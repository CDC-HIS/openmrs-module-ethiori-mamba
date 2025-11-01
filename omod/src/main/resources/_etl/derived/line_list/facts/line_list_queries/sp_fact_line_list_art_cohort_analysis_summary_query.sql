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
                                       current_functional_status
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

         -- CTE to determine the initial ART start date for each patient.
         ART_Initiation AS (SELECT PatientId, MIN(art_start_date) AS art_start_date
                            FROM FollowUpEncounters
                            WHERE art_start_date IS NOT NULL
                              and art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                            GROUP BY PatientId),

         -- Defines the cohort analysis intervals in months.
         IntervalsDef AS (SELECT 0 AS interval_month
                          UNION ALL
                          SELECT 6
                          UNION ALL
                          SELECT 12
                          UNION ALL
                          SELECT 24
                          UNION ALL
                          SELECT 36),

         -- Creates an evaluation point for each patient at each interval.
         PatientIntervals AS (SELECT a.PatientId,
                                     a.art_start_date,
                                     i.interval_month,
                                     DATE_ADD(REPORT_START_DATE, INTERVAL i.interval_month MONTH)  as interval_end_date,
                                     LAG(DATE_ADD(REPORT_START_DATE, INTERVAL i.interval_month MONTH), 1,
                                         REPORT_START_DATE)
                                         OVER (PARTITION BY a.PatientId ORDER BY i.interval_month) as interval_start_date
                              FROM ART_Initiation a
                                       CROSS JOIN IntervalsDef i),

         -- Finds the latest follow-up for each patient within each interval to determine their status and general clinical details.
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
                                             f.cd4_count,
                                             f.viral_load_count,
                                             f.cd4_percent,
                                             f.current_functional_status,
                                             fn_get_ti_status(pi.PatientId, f.art_start_date,
                                                              pi.interval_end_date)                                                                               as ti_status,
                                             ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.follow_up_date DESC, f.encounter_id DESC) as rn
                                      FROM PatientIntervals pi
                                               LEFT JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId AND
                                                                                 f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date),
         -- Finds the latest Viral Load Sent record for each patient within each interval, independent of the main follow-up.
         LatestViralLoadSentInInterval AS (SELECT pi.PatientId,
                                                  pi.interval_month,
                                                  f.viral_load_sent_date,
                                                  ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.viral_load_sent_date DESC, f.encounter_id DESC) as rn_vl_sent
                                           FROM PatientIntervals pi
                                                    JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
                                           WHERE f.viral_load_sent_date <= pi.interval_end_date
                                             and f.viral_load_sent_date >= pi.art_start_date
                                             AND f.viral_load_sent_date IS NOT NULL),
         -- Finds the latest Viral Load Performed record for each patient within each interval, independent of the main follow-up.
         LatestViralLoadPerformedInInterval AS (SELECT pi.PatientId,
                                                       pi.interval_month,
                                                       f.viral_load_received_date,
                                                       f.viral_load_result,
                                                       f.cd4_count,
                                                       f.cd4_percent,
                                                       ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.viral_load_received_date DESC, f.encounter_id DESC) as rn_vl_performed
                                                FROM PatientIntervals pi
                                                         JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
                                                WHERE f.viral_load_received_date <= pi.interval_end_date
                                                  and f.viral_load_received_date >= pi.art_start_date
                                                  AND f.viral_load_received_date IS NOT NULL),

         -- Combines the latest follow-up details with the latest viral load details for each interval.
         CohortDetails AS (SELECT lfu.PatientId,
                                  lfu.interval_end_date,
                                  lfu.interval_month,
                                  lfu.ti_status,
                                  CASE
                                      WHEN lfu.follow_up_status IN
                                           ('Dead', 'Transferred out', 'Stop all', 'Loss to follow-up (LTFU)',
                                            'Ran away')
                                          THEN lfu.follow_up_status
                                      WHEN lfu.follow_up_status IN ('Alive', 'Restart medication') AND
                                           lfu.treatment_end_date >= lfu.interval_end_date THEN 'Active'
                                      ELSE 'Lost to follow-up'
                                      END AS outcome,
                                  lfu.follow_up_date,
                                  lfu.regimen,
                                  lfu.viral_load_count,
                                  lfu.ARTDoseDays,
                                  lfu.follow_up_status,
                                  lfu.AdherenceLevel,
                                  lfu.pregnancy_status,
                                  lfu.treatment_end_date,
                                  lfu.next_visit_date,
                                  viral_load_sent.viral_load_sent_date,
                                  viral_load_performed.viral_load_received_date,
                                  viral_load_performed.viral_load_result,
                                  viral_load_performed.cd4_count,
                                  viral_load_performed.cd4_percent,
                                  lfu.current_functional_status,
                                  client.date_of_birth
                           FROM (SELECT * FROM LatestFollowUpInInterval WHERE rn = 1) lfu
                                    LEFT JOIN (SELECT * FROM LatestViralLoadSentInInterval WHERE rn_vl_sent = 1) viral_load_sent
                                              ON lfu.PatientId = viral_load_sent.PatientId AND
                                                 lfu.interval_month = viral_load_sent.interval_month
                                    LEFT JOIN (SELECT *
                                               FROM LatestViralLoadPerformedInInterval
                                               WHERE rn_vl_performed = 1) viral_load_performed
                                              ON lfu.PatientId = viral_load_performed.PatientId AND
                                                 lfu.interval_month = viral_load_performed.interval_month
                                    join mamba_dim_client client on lfu.PatientId = client.client_id)
    SELECT 'Started on ART in this clinic: original cohort'                               AS Name,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    WHERE follow_up_date IS NOT NULL
    UNION ALL
    SELECT 'Transfer in Add+'        AS Name,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 AND ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Transfers Out Subtract -'                     AS Name,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month <= 6 AND outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month <= 12 AND outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month <= 24 AND outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month <= 36 AND outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Net current cohort'                             AS Name, -- outcome active
           CAST(IFNULL(((SUM(CASE WHEN interval_month = 0 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN interval_month = 0 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                        SUM(CASE
                                WHEN interval_month = 0 AND outcome = 'Transferred out' THEN 1
                                ELSE 0 END)), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(((SUM(CASE WHEN interval_month = 0 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN interval_month = 6 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                        SUM(CASE
                                WHEN interval_month <= 6 AND outcome = 'Transferred out' THEN 1
                                ELSE 0 END)), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(((SUM(CASE WHEN interval_month = 0 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN interval_month = 12 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                        SUM(CASE
                                WHEN interval_month <= 12 AND outcome = 'Transferred out' THEN 1
                                ELSE 0 END)), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(((SUM(CASE WHEN interval_month = 0 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN interval_month = 24 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                        SUM(CASE
                                WHEN interval_month <= 24 AND outcome = 'Transferred out' THEN 1
                                ELSE 0 END)), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(((SUM(CASE WHEN interval_month = 0 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN interval_month = 36 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                        SUM(CASE
                                WHEN interval_month <= 36 AND outcome = 'Transferred out' THEN 1
                                ELSE 0 END)), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'On Original 1st Line Regimen' AS Name,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)   AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)   AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)   AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)   AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)   AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'On Alternate 1st Line Regimen (Substituted)' AS Name, -- outcome active
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)                  AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)                  AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)                  AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)                  AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)                  AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'On 2nd Line Regimen (Switched)' AS Name, -- outcome active
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'On 3rd Line Regimen (Switched)' AS Name,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                                        OR
                                     TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%') AND outcome = 'Active'
                                   THEN 1
                               ELSE 0 END
                       ), 0) AS SIGNED)     AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Stopped'                                      AS Name,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND follow_up_status = 'Stop all' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND follow_up_status = 'Stop all' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND follow_up_status = 'Stop all' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND follow_up_status = 'Stop all' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND follow_up_status = 'Stop all' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Died'                    AS Name,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 AND follow_up_status = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 AND follow_up_status = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 AND follow_up_status = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS
                                        'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 AND follow_up_status = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS
                                        'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 AND follow_up_status = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS
                                        'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Lost to Follow-up (DROP)'                     AS Name,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND
                                    (follow_up_status = 'Ran away' OR
                                     follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    (follow_up_status = 'Ran away' OR
                                     follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    (follow_up_status = 'Ran away' OR
                                     follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    (follow_up_status = 'Ran away' OR
                                     follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    (follow_up_status = 'Ran away' OR
                                     follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails


    UNION ALL
    SELECT 'Percent of cohort alive and on ART'              AS Name,
           CAST(ROUND(IFNULL(SUM(CASE WHEN interval_month = 0 AND outcome = 'Active' THEN 1 ELSE 0 END) /
                             NULLIF(SUM(CASE WHEN interval_month = 0 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END),
                                    0) * 100, 0)) AS SIGNED) AS 'Month 0',
           CAST(ROUND(IFNULL(SUM(CASE WHEN interval_month = 6 AND outcome = 'Active' THEN 1 ELSE 0 END) /
                             NULLIF(SUM(CASE WHEN interval_month = 6 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END),
                                    0) * 100, 0)) AS SIGNED) AS 'Month 6',
           CAST(ROUND(IFNULL(SUM(CASE WHEN interval_month = 12 AND outcome = 'Active' THEN 1 ELSE 0 END) /
                             NULLIF(SUM(CASE WHEN interval_month = 12 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END),
                                    0) * 100, 0)) AS SIGNED) AS 'Month 12',
           CAST(ROUND(IFNULL(SUM(CASE WHEN interval_month = 24 AND outcome = 'Active' THEN 1 ELSE 0 END) /
                             NULLIF(SUM(CASE WHEN interval_month = 24 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END),
                                    0) * 100, 0)) AS SIGNED) AS 'Month 24',
           CAST(ROUND(IFNULL(SUM(CASE WHEN interval_month = 36 AND outcome = 'Active' THEN 1 ELSE 0 END) /
                             NULLIF(SUM(CASE WHEN interval_month = 36 AND follow_up_date IS NOT NULL THEN 1 ELSE 0 END),
                                    0) * 100, 0)) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Mean CD4 % (for children)'                              AS Name, -- outcome active
           CAST(ROUND(IFNULL(AVG(CASE
                                     WHEN interval_month = 0 AND
                                          TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15
                                         THEN cd4_percent
                                     ELSE NULL END), 0)) AS SIGNED) AS 'Month 0',
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
    SELECT 'CD4 median or proportion ≥ 200 (optional)'                                     AS Name,  -- finish here
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 and outcome = 'Active' and cd4_count is not null and
                                    cd4_count > 200 THEN 1
                               ELSE 0 END), 0) AS SIGNED)
               / CAST(IFNULL(
                   SUM(CASE WHEN interval_month = 0 and outcome = 'Active' and cd4_count is not null THEN 1 ELSE 0 END),
                   0) AS SIGNED)                                                           AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END), 0) AS SIGNED)  AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Viral Load tested'                            AS Name,   -- outcome active
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND viral_load_received_date IS NOT NULL THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND viral_load_received_date IS NOT NULL THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND viral_load_received_date IS NOT NULL THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND viral_load_received_date IS NOT NULL THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND viral_load_received_date IS NOT NULL THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Viral load Suppressed ( < 50 copies/ml)'      AS Name, -- outcome active
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND viral_load_received_date IS NOT NULL AND
                                    viral_load_count BETWEEN 0 AND 50 THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND viral_load_received_date IS NOT NULL AND
                                    viral_load_count BETWEEN 0 AND 50 THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND viral_load_received_date IS NOT NULL AND
                                    viral_load_count BETWEEN 0 AND 50 THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND viral_load_received_date IS NOT NULL AND
                                    viral_load_count BETWEEN 0 AND 50 THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND viral_load_received_date IS NOT NULL AND
                                    viral_load_count BETWEEN 0 AND 50 THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Functional Status [# W or A or B / D * 100]' AS Name,
           0                                             AS 'Month 0',
           0                                             AS 'Month 6',
           0                                             AS 'Month 12',
           0                                             AS 'Month 24',
           0                                             AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Percentage Working'                           AS Name,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND current_functional_status = 'Physically able to work' and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND current_functional_status = 'Physically able to work' and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND current_functional_status = 'Physically able to work' and outcome = 'Active' AND
                                    current_functional_status = '' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND current_functional_status = 'Physically able to work' and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND current_functional_status = 'Physically able to work' and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Percentage Ambulatory'                        AS Name,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 0 AND current_functional_status = 'Patient ambulatory' and outcome = 'Active' THEN 1  -- Add percentage denominator that current_functional_status is not null and outcome active
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND current_functional_status = 'Patient ambulatory' and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND current_functional_status = 'Patient ambulatory' and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND current_functional_status = 'Patient ambulatory' and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND current_functional_status = 'Patient ambulatory' and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Percentage Bedridden'    AS Name,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 AND current_functional_status = 'Bedridden' and outcome = 'Active' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 AND current_functional_status = 'Bedridden' and outcome = 'Active' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 AND current_functional_status = 'Bedridden' and outcome = 'Active' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 AND current_functional_status = 'Bedridden' and outcome = 'Active' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 AND current_functional_status = 'Bedridden' and outcome = 'Active' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Number of persons who picked up ARVs each month for 6 months'                  AS Name,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 and follow_up_date is not null THEN 1 ELSE 0 END), 0) AS SIGNED)  AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END), 0) AS SIGNED)  AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Number of persons who picked up ARVs each month for 12 months'                 AS Name,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED)  AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END), 0) AS SIGNED)  AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Number of persons who picked up ARVs each month for 24 months'                 AS Name,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED)  AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END), 0) AS SIGNED)  AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails
    UNION ALL
    SELECT 'Number of persons who picked up ARVs each month for 36 months '                AS Name,   -- Active and no ti or deduct ti
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED)  AS 'Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END), 0) AS SIGNED)  AS 'Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 24',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Month 36'
    FROM CohortDetails;

END //

DELIMITER ;