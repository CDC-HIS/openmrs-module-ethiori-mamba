DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_art_cohort_analysis_summary_query;

CREATE PROCEDURE sp_fact_line_list_art_cohort_analysis_summary_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUpEncounters AS (SELECT follow_up.encounter_id,
                                       follow_up.client_id                                                     AS PatientId,
                                       follow_up_status,
                                       follow_up_date_followup_                                                AS follow_up_date,
                                       art_antiretroviral_start_date                                           AS art_start_date,
                                       treatment_end_date,
                                       regimen,
                                       antiretroviral_art_dispensed_dose_i                                     AS ARTDoseDays,
                                       anitiretroviral_adherence_level                                         AS AdherenceLevel,
                                       pregnancy_status,
                                       next_visit_date,
                                       date_of_reported_hiv_viral_load                                         AS viral_load_sent_date,
                                       date_viral_load_results_received                                        AS viral_load_received_date,
                                       viral_load_test_status                                                  AS viral_load_result,
                                       hiv_viral_load                                                          AS viral_load_count,
                                       cd4_count,
                                       cd4_                                                                    AS cd4_percent,
                                       current_functional_status,
                                       visitect_cd4_result,
                                       visitect_cd4_test_date,
                                       transferred_in_check_this_for_all_t,
                                       -- Standardize TI Flag
                                       CASE WHEN transferred_in_check_this_for_all_t = 'Yes' THEN 1 ELSE 0 END as is_ti_visit
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
                                WHERE follow_up_date_followup_ IS NOT NULL
                                  AND follow_up_status IS NOT NULL),

         ART_Initiation AS (SELECT PatientId,
                                   MIN(art_start_date) AS art_start_date
                            FROM FollowUpEncounters
                            WHERE art_start_date IS NOT NULL
                              AND art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                            GROUP BY PatientId),

         IntervalsDef AS (SELECT 0 AS interval_month
                          UNION ALL
                          SELECT 6
                          UNION ALL
                          SELECT 12
                          UNION ALL
                          SELECT 24
                          UNION ALL
                          SELECT 36),

         PatientIntervals AS (SELECT a.PatientId,
                                     a.art_start_date,
                                     i.interval_month,
                                     fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                             fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL
                                             i.interval_month MONTH))                              AS interval_end_date,
                                     LAG(fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                             fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL
                                             i.interval_month MONTH)), 1, REPORT_START_DATE)
                                         OVER (PARTITION BY a.PatientId ORDER BY i.interval_month) AS interval_start_date
                              FROM ART_Initiation a
                                       CROSS JOIN IntervalsDef i),

         StrictIntervalData AS (SELECT pi.PatientId,
                                       pi.interval_month,
                                       pi.interval_end_date,

                                       f.follow_up_status                                 AS strict_status,
                                       f.treatment_end_date                               AS strict_tx_end_date,
                                       f.regimen                                          AS strict_regimen,
                                       f.current_functional_status,
                                       f.cd4_count,
                                       f.cd4_percent,
                                       f.visitect_cd4_result,
                                       f.visitect_cd4_test_date,
                                       f.viral_load_received_date,
                                       f.viral_load_count,
                                       f.is_ti_visit,

                                       MAX(CASE WHEN f.is_ti_visit = 1 THEN 1 ELSE 0 END) as ti_in_this_interval

                                FROM PatientIntervals pi
                                         LEFT JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
                                    AND f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date
                                    AND f.follow_up_date = (SELECT MAX(sub_f.follow_up_date)
                                                            FROM FollowUpEncounters sub_f
                                                            WHERE sub_f.PatientId = pi.PatientId
                                                              AND sub_f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date)
                                GROUP BY pi.PatientId, pi.interval_month, pi.interval_end_date),

         StateCalculation AS (SELECT sid.*,

                                     MAX(ti_in_this_interval)
                                         OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as is_cumulative_ti,

                                     MAX(CASE
                                             WHEN strict_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                                                 THEN 1
                                             ELSE 0 END)
                                         OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as has_terminal_event,

                                     MAX(CASE
                                             WHEN strict_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                                                 THEN strict_status
                                             ELSE NULL END)
                                         OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as specific_terminal_event,

                                     MAX(strict_tx_end_date)
                                         OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_tx_end_date_so_far

                              FROM StrictIntervalData sid),

         CohortDetails AS (SELECT sc.*,
                                  client.date_of_birth,

                                  CASE
                                      WHEN has_terminal_event = 1 THEN specific_terminal_event
                                      WHEN strict_status IS NOT NULL THEN 'Active'
                                      WHEN strict_status IS NULL AND max_tx_end_date_so_far >= interval_end_date
                                          THEN 'Active - Carry Forward Rx'
                                      ELSE 'Loss to follow-up (LTFU)'
                                      END AS final_cohort_outcome

                           FROM StateCalculation sc
                                    JOIN mamba_dim_client client ON sc.PatientId = client.client_id),

         EthHeaders AS (SELECT MAX(CASE
                                       WHEN interval_month = 0 THEN CONCAT(CASE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                                                                                                             fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                                             '-', 2),
                                                                                                     '-',
                                                                                                     -1) AS UNSIGNED)
                                                                               WHEN 1 THEN 'Meskerem'
                                                                               WHEN 2 THEN 'Tikimt'
                                                                               WHEN 3 THEN 'Hidar'
                                                                               WHEN 4 THEN 'Tahsas'
                                                                               WHEN 5 THEN 'Tir'
                                                                               WHEN 6 THEN 'Yekatit'
                                                                               WHEN 7 THEN 'Megabit'
                                                                               WHEN 8 THEN 'Miyazia'
                                                                               WHEN 9 THEN 'Ginbot'
                                                                               WHEN 10 THEN 'Sene'
                                                                               WHEN 11 THEN 'Hamle'
                                                                               WHEN 12 THEN 'Nehase'
                                                                               WHEN 13 THEN 'Pagume' END, ' ',
                                                                           SUBSTRING_INDEX(
                                                                                   fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                   '-', 1))
                                       ELSE '' END) as m0_header,
                               MAX(CASE
                                       WHEN interval_month = 6 THEN CONCAT(CASE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                                                                                                             fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                                             '-', 2),
                                                                                                     '-',
                                                                                                     -1) AS UNSIGNED)
                                                                               WHEN 1 THEN 'Meskerem'
                                                                               WHEN 2 THEN 'Tikimt'
                                                                               WHEN 3 THEN 'Hidar'
                                                                               WHEN 4 THEN 'Tahsas'
                                                                               WHEN 5 THEN 'Tir'
                                                                               WHEN 6 THEN 'Yekatit'
                                                                               WHEN 7 THEN 'Megabit'
                                                                               WHEN 8 THEN 'Miyazia'
                                                                               WHEN 9 THEN 'Ginbot'
                                                                               WHEN 10 THEN 'Sene'
                                                                               WHEN 11 THEN 'Hamle'
                                                                               WHEN 12 THEN 'Nehase'
                                                                               WHEN 13 THEN 'Pagume' END, ' ',
                                                                           SUBSTRING_INDEX(
                                                                                   fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                   '-', 1))
                                       ELSE '' END) as m6_header,
                               MAX(CASE
                                       WHEN interval_month = 12 THEN CONCAT(CASE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                                                                                                              fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                                              '-', 2),
                                                                                                      '-',
                                                                                                      -1) AS UNSIGNED)
                                                                                WHEN 1 THEN 'Meskerem'
                                                                                WHEN 2 THEN 'Tikimt'
                                                                                WHEN 3 THEN 'Hidar'
                                                                                WHEN 4 THEN 'Tahsas'
                                                                                WHEN 5 THEN 'Tir'
                                                                                WHEN 6 THEN 'Yekatit'
                                                                                WHEN 7 THEN 'Megabit'
                                                                                WHEN 8 THEN 'Miyazia'
                                                                                WHEN 9 THEN 'Ginbot'
                                                                                WHEN 10 THEN 'Sene'
                                                                                WHEN 11 THEN 'Hamle'
                                                                                WHEN 12 THEN 'Nehase'
                                                                                WHEN 13 THEN 'Pagume' END, ' ',
                                                                            SUBSTRING_INDEX(
                                                                                    fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                    '-', 1))
                                       ELSE '' END) as m12_header,
                               MAX(CASE
                                       WHEN interval_month = 24 THEN CONCAT(CASE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                                                                                                              fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                                              '-', 2),
                                                                                                      '-',
                                                                                                      -1) AS UNSIGNED)
                                                                                WHEN 1 THEN 'Meskerem'
                                                                                WHEN 2 THEN 'Tikimt'
                                                                                WHEN 3 THEN 'Hidar'
                                                                                WHEN 4 THEN 'Tahsas'
                                                                                WHEN 5 THEN 'Tir'
                                                                                WHEN 6 THEN 'Yekatit'
                                                                                WHEN 7 THEN 'Megabit'
                                                                                WHEN 8 THEN 'Miyazia'
                                                                                WHEN 9 THEN 'Ginbot'
                                                                                WHEN 10 THEN 'Sene'
                                                                                WHEN 11 THEN 'Hamle'
                                                                                WHEN 12 THEN 'Nehase'
                                                                                WHEN 13 THEN 'Pagume' END, ' ',
                                                                            SUBSTRING_INDEX(
                                                                                    fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                    '-', 1))
                                       ELSE '' END) as m24_header,
                               MAX(CASE
                                       WHEN interval_month = 36 THEN CONCAT(CASE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                                                                                                              fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                                              '-', 2),
                                                                                                      '-',
                                                                                                      -1) AS UNSIGNED)
                                                                                WHEN 1 THEN 'Meskerem'
                                                                                WHEN 2 THEN 'Tikimt'
                                                                                WHEN 3 THEN 'Hidar'
                                                                                WHEN 4 THEN 'Tahsas'
                                                                                WHEN 5 THEN 'Tir'
                                                                                WHEN 6 THEN 'Yekatit'
                                                                                WHEN 7 THEN 'Megabit'
                                                                                WHEN 8 THEN 'Miyazia'
                                                                                WHEN 9 THEN 'Ginbot'
                                                                                WHEN 10 THEN 'Sene'
                                                                                WHEN 11 THEN 'Hamle'
                                                                                WHEN 12 THEN 'Nehase'
                                                                                WHEN 13 THEN 'Pagume' END, ' ',
                                                                            SUBSTRING_INDEX(
                                                                                    fn_gregorian_to_ethiopian_calendar(interval_end_date, 'Y-M-D'),
                                                                                    '-', 1))
                                       ELSE '' END) as m36_header
                        FROM CohortDetails)

    SELECT 'Cohort Intervals (Ethiopian Calendar)' AS Name,
           m0_header                               AS 'Month 0',
           m6_header                               AS 'Month 6',
           m12_header                              AS 'Month 12',
           m24_header                              AS 'Month 24',
           m36_header                              AS 'Month 36'
    FROM EthHeaders

    UNION ALL

    SELECT 'A. Started on ART in this clinic: original cohort',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END), 0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'B. Transfer in Add+',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 1 THEN 1 ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 AND is_cumulative_ti = 1 THEN 1 ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 AND is_cumulative_ti = 1 THEN 1 ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 AND is_cumulative_ti = 1 THEN 1 ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 AND is_cumulative_ti = 1 THEN 1 ELSE 0 END), 0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'C. Transfers Out Subtract -',
           0,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 AND final_cohort_outcome = 'Transferred out' THEN 1 ELSE 0 END),
                       0) AS SIGNED),
           CAST(IFNULL(
                   SUM(CASE WHEN interval_month = 12 AND final_cohort_outcome = 'Transferred out' THEN 1 ELSE 0 END),
                   0) AS SIGNED),
           CAST(IFNULL(
                   SUM(CASE WHEN interval_month = 24 AND final_cohort_outcome = 'Transferred out' THEN 1 ELSE 0 END),
                   0) AS SIGNED),
           CAST(IFNULL(
                   SUM(CASE WHEN interval_month = 36 AND final_cohort_outcome = 'Transferred out' THEN 1 ELSE 0 END),
                   0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'D. Net current cohort (A + B - C)',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED),

           CAST(GREATEST(0, (
               SUM(CASE WHEN interval_month = 6 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END) +
               SUM(CASE WHEN interval_month = 6 AND is_cumulative_ti = 1 THEN 1 ELSE 0 END) -
               SUM(CASE WHEN interval_month = 6 AND final_cohort_outcome = 'Transferred out' THEN 1 ELSE 0 END)
               )) AS SIGNED),
           CAST(GREATEST(0, (
               SUM(CASE WHEN interval_month = 12 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END) +
               SUM(CASE WHEN interval_month = 12 AND is_cumulative_ti = 1 THEN 1 ELSE 0 END) -
               SUM(CASE WHEN interval_month = 12 AND final_cohort_outcome = 'Transferred out' THEN 1 ELSE 0 END)
               )) AS SIGNED),
           CAST(GREATEST(0, (
               SUM(CASE WHEN interval_month = 24 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END) +
               SUM(CASE WHEN interval_month = 24 AND is_cumulative_ti = 1 THEN 1 ELSE 0 END) -
               SUM(CASE WHEN interval_month = 24 AND final_cohort_outcome = 'Transferred out' THEN 1 ELSE 0 END)
               )) AS SIGNED),
           CAST(GREATEST(0, (
               SUM(CASE WHEN interval_month = 36 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END) +
               SUM(CASE WHEN interval_month = 36 AND is_cumulative_ti = 1 THEN 1 ELSE 0 END) -
               SUM(CASE WHEN interval_month = 36 AND final_cohort_outcome = 'Transferred out' THEN 1 ELSE 0 END)
               )) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'E. On Original 1st Line Regimen',
           0,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '4%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '1%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '4%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '1%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '4%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '1%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '4%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '1%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'F. On Alternate 1st Line Regimen (Substituted)',
           0,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '1%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '4%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '1%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '4%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '1%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '4%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '1%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '4%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'G. On 2nd Line Regimen (Switched)',
           0,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '5%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '2%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '5%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '2%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '5%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '2%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    ((TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND
                                      strict_regimen LIKE '6%') OR
                                     (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND
                                      strict_regimen LIKE '2%'))
                                   AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'I. Stopped',
           0,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 AND final_cohort_outcome = 'Stop all' THEN 1 ELSE 0 END),
                       0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 AND final_cohort_outcome = 'Stop all' THEN 1 ELSE 0 END),
                       0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 AND final_cohort_outcome = 'Stop all' THEN 1 ELSE 0 END),
                       0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 AND final_cohort_outcome = 'Stop all' THEN 1 ELSE 0 END),
                       0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'J. Died',
           0,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 AND final_cohort_outcome = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 AND final_cohort_outcome = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 AND final_cohort_outcome = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED),
           CAST(IFNULL(SUM(CASE WHEN interval_month = 36 AND final_cohort_outcome = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'K. Lost to Follow-up (DROP)',
           0,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND
                                    final_cohort_outcome IN ('Ran away', 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND
                                    final_cohort_outcome IN ('Ran away', 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND
                                    final_cohort_outcome IN ('Ran away', 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND
                                    final_cohort_outcome IN ('Ran away', 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'Percent of Net Facility Cohort Alive and on ART',
           '100%',
           -- Month 6
           CONCAT(ROUND(
                      -- NUMERATOR: Active patients who are NOT TI
                          (SUM(CASE
                                   WHEN interval_month = 6
                                       AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                       AND is_cumulative_ti = 0 -- <--- FILTER ADDED
                                       THEN 1
                                   ELSE 0 END) * 100.0)
                              /
                          NULLIF(
                              -- DENOMINATOR: Original Cohort - Transfers Out (of Original Cohort only)
                                  (
                                      SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END) -- Original Cohort
                                          -
                                      SUM(CASE
                                              WHEN interval_month = 6
                                                  AND final_cohort_outcome = 'Transferred out'
                                                  AND is_cumulative_ti =
                                                      0 -- <--- FILTER ADDED (Only subtract TO if they were facility patients)
                                                  THEN 1
                                              ELSE 0 END)
                                      ), 0), 0), '%'),

           -- Month 12
           CONCAT(ROUND(
                          (SUM(CASE
                                   WHEN interval_month = 12
                                       AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                       AND is_cumulative_ti = 0
                                       THEN 1
                                   ELSE 0 END) * 100.0)
                              /
                          NULLIF(
                                  (
                                      SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END)
                                          -
                                      SUM(CASE
                                              WHEN interval_month = 12
                                                  AND final_cohort_outcome = 'Transferred out'
                                                  AND is_cumulative_ti = 0
                                                  THEN 1
                                              ELSE 0 END)
                                      ), 0), 0), '%'),

           -- Month 24
           CONCAT(ROUND(
                          (SUM(CASE
                                   WHEN interval_month = 24
                                       AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                       AND is_cumulative_ti = 0
                                       THEN 1
                                   ELSE 0 END) * 100.0)
                              /
                          NULLIF(
                                  (
                                      SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END)
                                          -
                                      SUM(CASE
                                              WHEN interval_month = 24
                                                  AND final_cohort_outcome = 'Transferred out'
                                                  AND is_cumulative_ti = 0
                                                  THEN 1
                                              ELSE 0 END)
                                      ), 0), 0), '%'),

           -- Month 36
           CONCAT(ROUND(
                          (SUM(CASE
                                   WHEN interval_month = 36
                                       AND final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx')
                                       AND is_cumulative_ti = 0
                                       THEN 1
                                   ELSE 0 END) * 100.0)
                              /
                          NULLIF(
                                  (
                                      SUM(CASE WHEN interval_month = 0 AND is_cumulative_ti = 0 THEN 1 ELSE 0 END)
                                          -
                                      SUM(CASE
                                              WHEN interval_month = 36
                                                  AND final_cohort_outcome = 'Transferred out'
                                                  AND is_cumulative_ti = 0
                                                  THEN 1
                                              ELSE 0 END)
                                      ), 0), 0), '%')
    FROM CohortDetails

    UNION ALL

    SELECT 'L. Mean CD4 % (for children)',
           0,
           CAST(ROUND(IFNULL(AVG(CASE
                                     WHEN interval_month = 6 AND
                                          TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 THEN cd4_percent
                                     ELSE NULL END), 0)) AS SIGNED),
           CAST(ROUND(IFNULL(AVG(CASE
                                     WHEN interval_month = 12 AND
                                          TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 THEN cd4_percent
                                     ELSE NULL END), 0)) AS SIGNED),
           CAST(ROUND(IFNULL(AVG(CASE
                                     WHEN interval_month = 24 AND
                                          TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 THEN cd4_percent
                                     ELSE NULL END), 0)) AS SIGNED),
           CAST(ROUND(IFNULL(AVG(CASE
                                     WHEN interval_month = 36 AND
                                          TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 THEN cd4_percent
                                     ELSE NULL END), 0)) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'N. Viral Load tested',
           0,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND viral_load_received_date IS NOT NULL AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND viral_load_received_date IS NOT NULL AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND viral_load_received_date IS NOT NULL AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND viral_load_received_date IS NOT NULL AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED)
    FROM CohortDetails

    UNION ALL

    SELECT 'O. Viral load Suppressed ( < 50 copies/ml)',
           0,
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 AND viral_load_count < 50 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 AND viral_load_count < 50 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 AND viral_load_count < 50 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED),
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 36 AND viral_load_count < 50 AND
                                    final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                               ELSE 0 END), 0) AS SIGNED)
    FROM CohortDetails;
END //

DELIMITER ;