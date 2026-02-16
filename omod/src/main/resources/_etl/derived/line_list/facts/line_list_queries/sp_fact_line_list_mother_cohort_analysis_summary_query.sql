DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_mother_cohort_analysis_summary_query;

CREATE PROCEDURE sp_fact_line_list_mother_cohort_analysis_summary_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUpEncounters AS (
        SELECT follow_up.encounter_id,
               follow_up.client_id                 AS PatientId,
               follow_up_status,
               follow_up_date_followup_            AS follow_up_date,
               treatment_end_date,
               regimen,
               transferred_in_check_this_for_all_t,
               CASE WHEN transferred_in_check_this_for_all_t = 'Yes' THEN 1 ELSE 0 END as is_ti_visit,
               follow_up.encounter_id AS enc_id
        FROM mamba_flat_encounter_follow_up follow_up
                 LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1 ON follow_up.encounter_id = follow_up_1.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2 ON follow_up.encounter_id = follow_up_2.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3 ON follow_up.encounter_id = follow_up_3.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4 ON follow_up.encounter_id = follow_up_4.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_5 follow_up_5 ON follow_up.encounter_id = follow_up_5.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_6 follow_up_6 ON follow_up.encounter_id = follow_up_6.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_7 follow_up_7 ON follow_up.encounter_id = follow_up_7.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_8 follow_up_8 ON follow_up.encounter_id = follow_up_8.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_9 follow_up_9 ON follow_up.encounter_id = follow_up_9.encounter_id
        WHERE follow_up_date_followup_ IS NOT NULL
          AND follow_up_status IS NOT NULL
    ),

         PMTCT_ENROLLMENT AS (
             SELECT enrollment.client_id                          AS PatientId,
                    MIN(enrollment.date_of_enrollment_or_booking) AS date_of_enrollment_or_booking
             FROM mamba_flat_encounter_pmtct_enrollment enrollment
             WHERE date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE
             GROUP BY enrollment.client_id
         ),

         IntervalsDef AS (
             SELECT 0 AS interval_month UNION ALL SELECT 4 UNION ALL SELECT 7 UNION ALL SELECT 13 UNION ALL SELECT 25
         ),

         PatientIntervals AS (
             SELECT a.PatientId,
                    a.date_of_enrollment_or_booking,
                    i.interval_month,
                    CASE
                        WHEN i.interval_month = 0 THEN a.date_of_enrollment_or_booking
                        ELSE fn_ethiopian_to_gregorian_calendar(DATE_ADD(fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL i.interval_month MONTH))
                        END AS interval_end_date,
                    CASE
                        WHEN i.interval_month = 0 THEN REPORT_START_DATE
                        ELSE COALESCE(LAG(fn_ethiopian_to_gregorian_calendar(DATE_ADD(fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL i.interval_month MONTH))) OVER (PARTITION BY a.PatientId ORDER BY i.interval_month), REPORT_START_DATE)
                        END AS interval_start_date
             FROM PMTCT_ENROLLMENT a
                      CROSS JOIN IntervalsDef i
         ),

         StrictIntervalData AS (
             SELECT
                 pi.PatientId,
                 pi.interval_month,
                 pi.interval_end_date,

                 MAX(CASE WHEN f.is_ti_visit = 1 THEN 1 ELSE 0 END) as ti_in_this_interval,

                 SUBSTRING_INDEX(GROUP_CONCAT(f.follow_up_status ORDER BY f.follow_up_date DESC SEPARATOR '||'), '||', 1) as strict_status,
                 SUBSTRING_INDEX(GROUP_CONCAT(f.treatment_end_date ORDER BY f.follow_up_date DESC SEPARATOR '||'), '||', 1) as strict_tx_end_date

             FROM PatientIntervals pi
                      LEFT JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
                 AND f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date
             GROUP BY pi.PatientId, pi.interval_month, pi.interval_end_date
         ),

         StateCalculation AS (
             SELECT
                 sid.*,

                 MAX(ti_in_this_interval) OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as is_cumulative_ti,

                 COUNT(strict_status) OVER (PARTITION BY PatientId ORDER BY interval_month) as status_partition,

                 MAX(strict_tx_end_date) OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_tx_end_date_so_far
             FROM StrictIntervalData sid
         ),

         FilledState AS (
             SELECT
                 sc.*,
                 FIRST_VALUE(strict_status) OVER (PARTITION BY PatientId, status_partition ORDER BY interval_month) as last_known_status
             FROM StateCalculation sc
         ),

         CohortDetails AS (
             SELECT
                 fs.PatientId,
                 fs.interval_month,
                 fs.interval_end_date,
                 fs.is_cumulative_ti,

                 CASE
                     -- A. Terminal States are permanent (Rolled over)
                     WHEN strict_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away') THEN strict_status
                     WHEN last_known_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away') THEN last_known_status

                     -- B. Visit in this interval
                     WHEN strict_status IS NOT NULL THEN 'Active' -- Alive/Restart

                 -- C. No visit, but meds cover the end of this interval (Rollover Active)
                     WHEN strict_status IS NULL AND max_tx_end_date_so_far >= interval_end_date THEN 'Active'

                     -- D. No visit, meds expired
                     ELSE 'Lost to follow-up'
                     END AS final_outcome
             FROM FilledState fs
         ),

         SummaryCounts AS (
             SELECT
                 interval_month,
                 CASE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(fn_gregorian_to_ethiopian_calendar(MAX(interval_end_date), 'Y-M-D'),'-', 2), '-', -1) AS UNSIGNED)
                     WHEN 1 THEN 'Meskerem' WHEN 2 THEN 'Tikimt' WHEN 3 THEN 'Hidar' WHEN 4 THEN 'Tahsas'
                     WHEN 5 THEN 'Tir' WHEN 6 THEN 'Yekatit' WHEN 7 THEN 'Megabit' WHEN 8 THEN 'Miyazia'
                     WHEN 9 THEN 'Ginbot' WHEN 10 THEN 'Sene' WHEN 11 THEN 'Hamle' WHEN 12 THEN 'Nehase' WHEN 13 THEN 'Pagume'
                     END AS ethiopian_month_name,
                 CAST(SUBSTRING_INDEX(fn_gregorian_to_ethiopian_calendar(MAX(interval_end_date), 'Y-M-D'), '-', 1) AS CHAR) as ethiopian_year,

                 COUNT(DISTINCT PatientId) AS Total_Base,
                 SUM(CASE WHEN is_cumulative_ti = 1 THEN 1 ELSE 0 END) AS Total_TI,
                 SUM(CASE WHEN final_outcome IN ('Transferred out', 'Stop all') THEN 1 ELSE 0 END) AS Total_TO,
                 SUM(CASE WHEN final_outcome = 'Active' THEN 1 ELSE 0 END) AS Total_Active,
                 SUM(CASE WHEN final_outcome IN ('Lost to follow-up', 'Ran away') THEN 1 ELSE 0 END) AS Total_LTFU,
                 SUM(CASE WHEN final_outcome = 'Dead' THEN 1 ELSE 0 END) AS Total_Dead

             FROM CohortDetails
             GROUP BY interval_month
         )

    SELECT '' AS Name,
           MAX(CASE WHEN interval_month = 0 THEN CONCAT(ethiopian_month_name, ' ', ethiopian_year) ELSE '-' END) AS 'Maternal Cohort Month 0',
           MAX(CASE WHEN interval_month = 4 THEN CONCAT(ethiopian_month_name, ' ', ethiopian_year) ELSE '-' END) AS 'Maternal Cohort Month 3',
           MAX(CASE WHEN interval_month = 7 THEN CONCAT(ethiopian_month_name, ' ', ethiopian_year) ELSE '-' END) AS 'Maternal Cohort Month 6',
           MAX(CASE WHEN interval_month = 13 THEN CONCAT(ethiopian_month_name, ' ', ethiopian_year) ELSE '-' END) AS 'Maternal Cohort Month 12',
           MAX(CASE WHEN interval_month = 25 THEN CONCAT(ethiopian_month_name, ' ', ethiopian_year) ELSE '-' END) AS 'Maternal Cohort Month 24'
    FROM SummaryCounts

    UNION ALL

    SELECT 'A. Enrolled in PMTCT in this facility (Month 0)',
           MAX(CASE WHEN interval_month = 0 THEN Total_Base ELSE 0 END),
           MAX(CASE WHEN interval_month = 0 THEN Total_Base ELSE 0 END),
           MAX(CASE WHEN interval_month = 0 THEN Total_Base ELSE 0 END),
           MAX(CASE WHEN interval_month = 0 THEN Total_Base ELSE 0 END),
           MAX(CASE WHEN interval_month = 0 THEN Total_Base ELSE 0 END)
    FROM SummaryCounts

    UNION ALL

    SELECT 'B. Total number of Transfer in (TI) Cumulative',
           0,
           MAX(CASE WHEN interval_month = 4 THEN Total_TI ELSE 0 END),
           MAX(CASE WHEN interval_month = 7 THEN Total_TI ELSE 0 END),
           MAX(CASE WHEN interval_month = 13 THEN Total_TI ELSE 0 END),
           MAX(CASE WHEN interval_month = 25 THEN Total_TI ELSE 0 END)
    FROM SummaryCounts

    UNION ALL

    SELECT 'C. Total Number of Transfer out (TO) Cumulative',
           0,
           MAX(CASE WHEN interval_month = 4 THEN Total_TO ELSE 0 END),
           MAX(CASE WHEN interval_month = 7 THEN Total_TO ELSE 0 END),
           MAX(CASE WHEN interval_month = 13 THEN Total_TO ELSE 0 END),
           MAX(CASE WHEN interval_month = 25 THEN Total_TO ELSE 0 END)
    FROM SummaryCounts

    UNION ALL

    SELECT 'D. Net Current Cohort (A + B - C)',
           MAX(CASE WHEN interval_month = 0 THEN Total_Base ELSE 0 END),
           MAX(CASE WHEN interval_month = 4 THEN (Total_Base + Total_TI - Total_TO) ELSE 0 END),
           MAX(CASE WHEN interval_month = 7 THEN (Total_Base + Total_TI - Total_TO) ELSE 0 END),
           MAX(CASE WHEN interval_month = 13 THEN (Total_Base + Total_TI - Total_TO) ELSE 0 END),
           MAX(CASE WHEN interval_month = 25 THEN (Total_Base + Total_TI - Total_TO) ELSE 0 END)
    FROM SummaryCounts

    UNION ALL

    SELECT 'E. Mothers Alive and on ART',
           MAX(CASE WHEN interval_month = 0 THEN Total_Active ELSE 0 END),
           MAX(CASE WHEN interval_month = 4 THEN Total_Active ELSE 0 END),
           MAX(CASE WHEN interval_month = 7 THEN Total_Active ELSE 0 END),
           MAX(CASE WHEN interval_month = 13 THEN Total_Active ELSE 0 END),
           MAX(CASE WHEN interval_month = 25 THEN Total_Active ELSE 0 END)
    FROM SummaryCounts

    UNION ALL

    SELECT 'F. Lost to F/U',
           0,
           MAX(CASE WHEN interval_month = 4 THEN Total_LTFU ELSE 0 END),
           MAX(CASE WHEN interval_month = 7 THEN Total_LTFU ELSE 0 END),
           MAX(CASE WHEN interval_month = 13 THEN Total_LTFU ELSE 0 END),
           MAX(CASE WHEN interval_month = 25 THEN Total_LTFU ELSE 0 END)
    FROM SummaryCounts

    UNION ALL

    SELECT 'G. Known Dead',
           0,
           MAX(CASE WHEN interval_month = 4 THEN Total_Dead ELSE 0 END),
           MAX(CASE WHEN interval_month = 7 THEN Total_Dead ELSE 0 END),
           MAX(CASE WHEN interval_month = 13 THEN Total_Dead ELSE 0 END),
           MAX(CASE WHEN interval_month = 25 THEN Total_Dead ELSE 0 END)
    FROM SummaryCounts

    UNION ALL

    SELECT 'H. % Alive and on ART (E/D)',
           CONCAT(ROUND((MAX(CASE WHEN interval_month = 0 THEN Total_Active ELSE 0 END) / NULLIF(MAX(CASE WHEN interval_month = 0 THEN Total_Base ELSE 0 END),0))*100, 1), '%'),
           CONCAT(ROUND((MAX(CASE WHEN interval_month = 4 THEN Total_Active ELSE 0 END) / NULLIF(MAX(CASE WHEN interval_month = 4 THEN (Total_Base + Total_TI - Total_TO) ELSE 0 END),0))*100, 1), '%'),
           CONCAT(ROUND((MAX(CASE WHEN interval_month = 7 THEN Total_Active ELSE 0 END) / NULLIF(MAX(CASE WHEN interval_month = 7 THEN (Total_Base + Total_TI - Total_TO) ELSE 0 END),0))*100, 1), '%'),
           CONCAT(ROUND((MAX(CASE WHEN interval_month = 13 THEN Total_Active ELSE 0 END) / NULLIF(MAX(CASE WHEN interval_month = 13 THEN (Total_Base + Total_TI - Total_TO) ELSE 0 END),0))*100, 1), '%'),
           CONCAT(ROUND((MAX(CASE WHEN interval_month = 25 THEN Total_Active ELSE 0 END) / NULLIF(MAX(CASE WHEN interval_month = 25 THEN (Total_Base + Total_TI - Total_TO) ELSE 0 END),0))*100, 1), '%')
    FROM SummaryCounts;

END //

DELIMITER ;
