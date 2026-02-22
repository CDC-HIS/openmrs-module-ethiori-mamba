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
                          SELECT 7
                          UNION ALL
                          SELECT 13
                          UNION ALL
                          SELECT 25
                          UNION ALL
                          SELECT 37),

         PatientIntervals AS (SELECT a.PatientId,
                                     a.art_start_date,
                                     i.interval_month,
                                     CASE
                                         WHEN i.interval_month = 0 THEN a.art_start_date
                                         ELSE fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                                 fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                                 INTERVAL i.interval_month MONTH))
                                         END AS interval_end_date,
                                     CASE
                                         WHEN i.interval_month = 0 THEN REPORT_START_DATE
                                         ELSE COALESCE(LAG(fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                                 fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                                 INTERVAL i.interval_month MONTH)))
                                                           OVER (PARTITION BY a.PatientId ORDER BY i.interval_month),
                                                       REPORT_START_DATE)
                                         END AS interval_start_date
                              FROM ART_Initiation a
                                       CROSS JOIN IntervalsDef i),

         StrictIntervalData AS (SELECT pi.PatientId,
                                       pi.interval_month,
                                       pi.interval_end_date,
                                       MAX(f.follow_up_status)                            AS strict_status,
                                       MAX(f.treatment_end_date)                          AS strict_tx_end_date,
                                       MAX(f.regimen)                                     AS strict_regimen,
                                       MAX(f.current_functional_status)                   AS current_functional_status,
                                       MAX(f.cd4_count)                                   AS cd4_count,
                                       MAX(f.cd4_percent)                                 AS cd4_percent,
                                       MAX(f.visitect_cd4_result)                         AS visitect_cd4_result,
                                       MAX(f.visitect_cd4_test_date)                      AS visitect_cd4_test_date,
                                       MAX(f.viral_load_received_date)                    AS viral_load_received_date,
                                       MAX(f.viral_load_count)                            AS viral_load_count,
                                       MAX(f.is_ti_visit)                                 AS is_ti_visit,
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

         CohortHeaderCalc AS (SELECT interval_month,
                                     CASE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                                                                       fn_gregorian_to_ethiopian_calendar(MAX(interval_end_date), 'Y-M-D'),
                                                                       '-', 2), '-', -1) AS UNSIGNED)
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
                                         WHEN 13 THEN 'Pagume'
                                         END             AS et_month,
                                     CAST(SUBSTRING_INDEX(
                                             fn_gregorian_to_ethiopian_calendar(MAX(interval_end_date), 'Y-M-D'), '-',
                                             1) AS CHAR) as et_year
                              FROM CohortDetails
                              WHERE interval_month IN (0, 6, 12, 24, 36)
                              GROUP BY interval_month),

         CohortEnriched AS (SELECT interval_month,
                                   is_cumulative_ti,
                                   final_cohort_outcome,
                                   cd4_percent,

                                   CASE
                                       WHEN final_cohort_outcome IN ('Active', 'Active - Carry Forward Rx') THEN 1
                                       ELSE 0 END                                                   AS is_active,
                                   CASE
                                       WHEN TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 THEN 'PEDS'
                                       ELSE 'ADULT' END                                             AS age_group,
                                   LEFT(strict_regimen, 1)                                          as reg_prefix,

                                   CASE WHEN viral_load_received_date IS NOT NULL THEN 1 ELSE 0 END AS has_vl_test,
                                   CASE WHEN viral_load_count < 50 THEN 1 ELSE 0 END                AS is_suppressed
                            FROM CohortDetails),

         MonthlyStats AS (SELECT interval_month,
                                 SUM(CASE WHEN is_cumulative_ti = 0 THEN 1 ELSE 0 END)                     AS count_base,
                                 SUM(CASE WHEN is_cumulative_ti = 1 THEN 1 ELSE 0 END)                     AS count_ti,
                                 SUM(CASE WHEN final_cohort_outcome = 'Transferred out' THEN 1 ELSE 0 END) AS count_to,

                                 SUM(CASE WHEN final_cohort_outcome = 'Stop all' THEN 1 ELSE 0 END)        AS count_stop,
                                 SUM(CASE WHEN final_cohort_outcome = 'Dead' THEN 1 ELSE 0 END)            AS count_dead,
                                 SUM(CASE
                                         WHEN final_cohort_outcome IN ('Ran away', 'Loss to follow-up (LTFU)') THEN 1
                                         ELSE 0 END)                                                       AS count_ltfu,

                                 SUM(CASE WHEN is_cumulative_ti = 0 AND is_active = 1 THEN 1 ELSE 0 END)   as count_orig_active,
                                 SUM(CASE
                                         WHEN is_cumulative_ti = 0 AND final_cohort_outcome = 'Transferred out' THEN 1
                                         ELSE 0 END)                                                       as count_orig_to,

                                 SUM(CASE
                                         WHEN is_active = 1 AND ((age_group = 'ADULT' AND reg_prefix = '1') OR
                                                                 (age_group = 'PEDS' AND reg_prefix = '4')) THEN 1
                                         ELSE 0 END)                                                       AS reg_orig,
                                 SUM(CASE
                                         WHEN is_active = 1 AND ((age_group = 'ADULT' AND reg_prefix = '4') OR
                                                                 (age_group = 'PEDS' AND reg_prefix = '1')) THEN 1
                                         ELSE 0 END)                                                       AS reg_alt,
                                 SUM(CASE
                                         WHEN is_active = 1 AND (
                                             (age_group = 'ADULT' AND reg_prefix = '2') OR
                                             (age_group = 'PEDS' AND ((interval_month < 37 AND reg_prefix = '5') OR
                                                                      (interval_month = 37 AND reg_prefix = '6')))
                                             ) THEN 1
                                         ELSE 0 END)                                                       AS reg_2nd,

                                 AVG(CASE WHEN age_group = 'PEDS' THEN cd4_percent ELSE NULL END)          as avg_peds_cd4_pct,
                                 SUM(CASE WHEN is_active = 1 AND has_vl_test = 1 THEN 1 ELSE 0 END)        as count_vl_tested,
                                 SUM(CASE WHEN is_active = 1 AND is_suppressed = 1 THEN 1 ELSE 0 END)      as count_vl_suppressed

                          FROM CohortEnriched
                          GROUP BY interval_month)

    SELECT 'Cohort Intervals (Ethiopian Calendar)'                                            AS Name,
           MAX(CASE WHEN interval_month = 0 THEN CONCAT(et_month, ' ', et_year) ELSE '' END)  AS 'Month 0',
           MAX(CASE WHEN interval_month = 7 THEN CONCAT(et_month, ' ', et_year) ELSE '' END)  AS 'Month 6',
           MAX(CASE WHEN interval_month = 13 THEN CONCAT(et_month, ' ', et_year) ELSE '' END) AS 'Month 12',
           MAX(CASE WHEN interval_month = 25 THEN CONCAT(et_month, ' ', et_year) ELSE '' END) AS 'Month 24',
           MAX(CASE WHEN interval_month = 37 THEN CONCAT(et_month, ' ', et_year) ELSE '' END) AS 'Month 36'
    FROM CohortHeaderCalc

    UNION ALL

    SELECT 'A. Started on ART in this clinic: original cohort',
           CAST(IFNULL(MAX(CASE WHEN interval_month = 0 THEN count_base END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 0 THEN count_base END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 0 THEN count_base END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 0 THEN count_base END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 0 THEN count_base END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'B. Transfer in Add+',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN count_ti END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN count_ti END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN count_ti END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN count_ti END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'C. Transfers Out Subtract -',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN count_to END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN count_to END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN count_to END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN count_to END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'D. Net current cohort (A + B - C)',
           0,
           CAST(GREATEST(0, IFNULL(MAX(CASE WHEN interval_month = 7 THEN (count_base + count_ti) - count_to END),
                                   0)) AS SIGNED),
           CAST(GREATEST(0, IFNULL(MAX(CASE WHEN interval_month = 13 THEN (count_base + count_ti) - count_to END),
                                   0)) AS SIGNED),
           CAST(GREATEST(0, IFNULL(MAX(CASE WHEN interval_month = 25 THEN (count_base + count_ti) - count_to END),
                                   0)) AS SIGNED),
           CAST(GREATEST(0, IFNULL(MAX(CASE WHEN interval_month = 37 THEN (count_base + count_ti) - count_to END),
                                   0)) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'E. On Original 1st Line Regimen',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN reg_orig END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN reg_orig END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN reg_orig END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN reg_orig END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'F. On Alternate 1st Line Regimen (Substituted)',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN reg_alt END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN reg_alt END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN reg_alt END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN reg_alt END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'G. On 2nd Line Regimen (Switched)',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN reg_2nd END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN reg_2nd END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN reg_2nd END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN reg_2nd END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'I. Stopped',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN count_stop END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN count_stop END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN count_stop END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN count_stop END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'J. Died',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN count_dead END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN count_dead END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN count_dead END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN count_dead END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'K. Lost to Follow-up (DROP)',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN count_ltfu END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN count_ltfu END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN count_ltfu END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN count_ltfu END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'Percent of Net Facility Cohort Alive and on ART',
           '100%',
           CONCAT(ROUND(IFNULL((MAX(CASE WHEN interval_month = 7 THEN count_orig_active END) * 100.0) / NULLIF(
                   MAX(CASE WHEN interval_month = 0 THEN count_base END) -
                   MAX(CASE WHEN interval_month = 7 THEN count_orig_to END), 0), 0), 0), '%'),
           CONCAT(ROUND(IFNULL((MAX(CASE WHEN interval_month = 13 THEN count_orig_active END) * 100.0) / NULLIF(
                   MAX(CASE WHEN interval_month = 0 THEN count_base END) -
                   MAX(CASE WHEN interval_month = 13 THEN count_orig_to END), 0), 0), 0), '%'),
           CONCAT(ROUND(IFNULL((MAX(CASE WHEN interval_month = 25 THEN count_orig_active END) * 100.0) / NULLIF(
                   MAX(CASE WHEN interval_month = 0 THEN count_base END) -
                   MAX(CASE WHEN interval_month = 25 THEN count_orig_to END), 0), 0), 0), '%'),
           CONCAT(ROUND(IFNULL((MAX(CASE WHEN interval_month = 37 THEN count_orig_active END) * 100.0) / NULLIF(
                   MAX(CASE WHEN interval_month = 0 THEN count_base END) -
                   MAX(CASE WHEN interval_month = 37 THEN count_orig_to END), 0), 0), 0), '%')
    FROM MonthlyStats

    UNION ALL

    SELECT 'L. Mean CD4 % (for children)',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN avg_peds_cd4_pct END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN avg_peds_cd4_pct END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN avg_peds_cd4_pct END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN avg_peds_cd4_pct END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'N. Viral Load tested',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN count_vl_tested END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN count_vl_tested END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN count_vl_tested END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN count_vl_tested END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'O. Viral load Suppressed ( < 50 copies/ml)',
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 7 THEN count_vl_suppressed END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 13 THEN count_vl_suppressed END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 25 THEN count_vl_suppressed END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 37 THEN count_vl_suppressed END), 0) AS SIGNED)
    FROM MonthlyStats;
END //

DELIMITER ;