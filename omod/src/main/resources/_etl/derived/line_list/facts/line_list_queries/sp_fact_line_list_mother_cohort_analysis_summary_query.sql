DELIMITER
//

DROP PROCEDURE IF EXISTS sp_fact_line_list_mother_cohort_analysis_summary_query;

CREATE PROCEDURE sp_fact_line_list_mother_cohort_analysis_summary_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH Cohort_List AS (SELECT client_id
                         FROM mamba_flat_encounter_pmtct_enrollment
                         WHERE date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE),

         Follow_UP AS (SELECT follow_up.client_id                 as PatientId,
                              follow_up_status,
                              follow_up_date_followup_            AS follow_up_date,
                              treatment_end_date,
                              regimen,
                              follow_up.encounter_id,
                              antiretroviral_art_dispensed_dose_i AS ARTDoseDays,
                              anitiretroviral_adherence_level     AS AdherenceLevel,
                              transferred_in_check_this_for_all_t,
                              pregnancy_status,
                              next_visit_date,
                              current_functional_status
                       FROM mamba_flat_encounter_follow_up follow_up
                                INNER JOIN Cohort_List cl ON follow_up.client_id = cl.client_id
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

         TI_Patients AS (SELECT DISTINCT PatientId
                         FROM Follow_UP
                         WHERE transferred_in_check_this_for_all_t = 'Yes'
                           and follow_up_date BETWEEN REPORT_START_DATE AND fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                 fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL 25 MONTH))),

         PMTCT_ENROLLMENT AS (SELECT enrollment.client_id                          as PatientId,
                                     MAX(enrollment.date_of_enrollment_or_booking) AS date_of_enrollment_or_booking,
                                     CASE
                                         WHEN TI_Patients.PatientId IS NOT NULL THEN 1
                                         ELSE 0
                                         END                                       AS ever_ti
                              FROM mamba_flat_encounter_pmtct_enrollment enrollment
                                       LEFT JOIN TI_Patients ON TI_Patients.PatientId = enrollment.client_id
                              WHERE date_of_enrollment_or_booking IS NOT NULL
                                and date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                              GROUP BY enrollment.client_id, ever_ti),

         IntervalsDef AS (SELECT 0 AS interval_month
                          UNION ALL
                          SELECT 4
                          UNION ALL
                          SELECT 7
                          UNION ALL
                          SELECT 13
                          UNION ALL
                          SELECT 25),

         PatientIntervals AS (SELECT a.PatientId,
                                     a.date_of_enrollment_or_booking,
                                     i.interval_month,
                                     a.ever_ti,
                                     CASE
                                         WHEN i.interval_month = 0 THEN a.date_of_enrollment_or_booking
                                         ELSE fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                                 fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                                 INTERVAL i.interval_month MONTH))
                                         END as interval_end_date,
                                     CASE
                                         WHEN i.interval_month = 0 THEN REPORT_START_DATE
                                         ELSE LAG(fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                                 fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                                 INTERVAL i.interval_month MONTH)), 1,
                                                  REPORT_START_DATE)
                                                  OVER (PARTITION BY a.PatientId ORDER BY i.interval_month)
                                         END
                                             as interval_start_date
                              FROM PMTCT_ENROLLMENT a
                                       CROSS JOIN IntervalsDef i),

         LatestFollowUpInInterval AS (SELECT pi.PatientId,
                                             pi.interval_month,
                                             pi.interval_end_date,
                                             pi.interval_start_date,
                                             pi.ever_ti,
                                             f.follow_up_status,
                                             f.follow_up_date,
                                             f.treatment_end_date,
                                             f.regimen,
                                             f.ARTDoseDays,
                                             f.transferred_in_check_this_for_all_t,
                                             MAX(f.transferred_in_check_this_for_all_t)
                                                 OVER (PARTITION BY pi.PatientId, pi.interval_month)  as interval_ti,
                                             f.AdherenceLevel,
                                             f.pregnancy_status,
                                             f.next_visit_date,
                                             f.current_functional_status,

                                             ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month
                                                 ORDER BY f.follow_up_date DESC, f.encounter_id DESC) as rn

                                      FROM PatientIntervals pi
                                               LEFT JOIN Follow_UP f ON pi.PatientId = f.PatientId
                                          AND f.follow_up_date <= pi.interval_end_date),

         CohortDetails AS (SELECT lfu.PatientId,
                                  lfu.interval_end_date,
                                  lfu.interval_start_date,
                                  lfu.interval_month,
                                  lfu.ever_ti,
                                  lfu.interval_ti,
                                  CASE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                                                                    fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'),
                                                                    '-', 2), '-', -1) AS UNSIGNED)
                                      WHEN 1 THEN CONCAT('Meskerem ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 2 THEN CONCAT('Tikimt ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 3 THEN CONCAT('Hidar ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 4 THEN CONCAT('Tahsas ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 5 THEN CONCAT('Tir ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 6 THEN CONCAT('Yekatit ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 7 THEN CONCAT('Megabit ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 8 THEN CONCAT('Miyazia ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 9 THEN CONCAT('Ginbot ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 10 THEN CONCAT('Sene ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 11 THEN CONCAT('Hamle ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 12 THEN CONCAT('Nehase ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      WHEN 13 THEN CONCAT('Pagume ', CAST(SUBSTRING_INDEX(
                                              fn_gregorian_to_ethiopian_calendar(lfu.interval_end_date, 'Y-M-D'), '-',
                                              1) AS UNSIGNED))
                                      END AS ethiopian_month,

                                  CASE
                                      -- 1. Explicit Terminal States (Persist forever once they happen)
                                      WHEN lfu.follow_up_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                                          THEN lfu.follow_up_status

                                      -- 2. Active Logic (Must be Alive + Meds covering report date)
                                      WHEN lfu.follow_up_status IN ('Alive', 'Restart medication')
                                          AND lfu.treatment_end_date >= lfu.interval_end_date
                                          THEN 'Active'

                                      -- 3. Default (Alive but meds expired = LTFU)
                                      ELSE 'Lost to follow-up'
                                      END AS outcome,

                                  lfu.follow_up_date,
                                  lfu.regimen,
                                  lfu.ARTDoseDays,
                                  lfu.follow_up_status,
                                  lfu.AdherenceLevel,
                                  lfu.pregnancy_status,
                                  lfu.treatment_end_date,
                                  lfu.next_visit_date,
                                  lfu.current_functional_status
                           FROM (SELECT * FROM LatestFollowUpInInterval WHERE rn = 1) lfu
                                    JOIN mamba_dim_client client
                                         ON lfu.PatientId = client.client_id),

         SummaryCounts AS (SELECT co.interval_month,
                                  co.ethiopian_month,

                                  COUNT(DISTINCT CASE WHEN ever_ti = 0 THEN PatientId END)                       AS Total_Facility_Enrolled,
                                  COUNT(DISTINCT CASE WHEN interval_ti = 'Yes' THEN PatientId END)               AS Total_TI,

                                  SUM(CASE WHEN co.outcome IN ('Transferred out', 'Stop all') THEN 1 ELSE 0 END) AS Total_TO,
                                  SUM(CASE WHEN co.outcome = 'Active' THEN 1 ELSE 0 END)                         AS Total_Active,
                                  SUM(CASE
                                          WHEN co.outcome IN ('Lost to follow-up', 'Ran away') THEN 1
                                          ELSE 0 END)                                                            AS Total_LTFU,
                                  SUM(CASE WHEN co.outcome = 'Dead' THEN 1 ELSE 0 END)                           AS Total_Dead

                           FROM CohortDetails co
                           GROUP BY co.interval_month, ethiopian_month)

    SELECT ''                                                                 AS Name,
           MAX(CASE WHEN interval_month = 0 THEN ethiopian_month ELSE 0 END)  AS 'Maternal Cohort Month 0',
           MAX(CASE WHEN interval_month = 4 THEN ethiopian_month ELSE 0 END)  AS 'Maternal Cohort Month 3',
           MAX(CASE WHEN interval_month = 7 THEN ethiopian_month ELSE 0 END)  AS 'Maternal Cohort Month 6',
           MAX(CASE WHEN interval_month = 13 THEN ethiopian_month ELSE 0 END) AS 'Maternal Cohort Month 12',
           MAX(CASE WHEN interval_month = 25 THEN ethiopian_month ELSE 0 END) AS 'Maternal Cohort Month 24'
    FROM SummaryCounts

    UNION ALL

-- Row A: Enrolled in PMTCT (Baseline)
    SELECT 'A. Enrolled in PMTCT in this facility during this month and year (Month 0)' AS Name,
           COALESCE(SUM(CASE WHEN interval_month = 0 THEN Total_Facility_Enrolled ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 0 THEN Total_Facility_Enrolled ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 0 THEN Total_Facility_Enrolled ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 0 THEN Total_Facility_Enrolled ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 0 THEN Total_Facility_Enrolled ELSE 0 END), 0)
    FROM SummaryCounts

    UNION ALL

-- Row B: Transfer In (TI)
    SELECT 'B. Total number of Transfer in (TI) since Month 0' AS Name,
           0,
           COALESCE(SUM(CASE WHEN interval_month = 4 THEN Total_TI ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 7 THEN Total_TI ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 13 THEN Total_TI ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 25 THEN Total_TI ELSE 0 END), 0)
    FROM SummaryCounts

    UNION ALL

-- Row C: Transfer Out (TO)
    SELECT 'C. Total Number of Transfer out (TO) since Month 0' AS Name,
           0,
           COALESCE(SUM(CASE WHEN interval_month = 4 THEN Total_TO ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 7 THEN Total_TO ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 13 THEN Total_TO ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 25 THEN Total_TO ELSE 0 END), 0)
    FROM SummaryCounts

    UNION ALL

-- Row D: Net Current Cohort (A + B - C)
    SELECT 'D. Number of mothers in the current cohort = Net Current Cohort (A+B-C)' AS Name,
           0,
           COALESCE(SUM(CASE WHEN interval_month = 4 THEN (Total_Facility_Enrolled + Total_TI - Total_TO) ELSE 0 END),
                    0),
           COALESCE(SUM(CASE WHEN interval_month = 7 THEN (Total_Facility_Enrolled + Total_TI - Total_TO) ELSE 0 END),
                    0),
           COALESCE(SUM(CASE WHEN interval_month = 13 THEN (Total_Facility_Enrolled + Total_TI - Total_TO) ELSE 0 END),
                    0),
           COALESCE(SUM(CASE WHEN interval_month = 25 THEN (Total_Facility_Enrolled + Total_TI - Total_TO) ELSE 0 END),
                    0)
    FROM SummaryCounts

    UNION ALL

-- Row E: Mothers Alive and on ART
    SELECT 'E. Mothers Alive and on ART' AS Name,
           0,
           COALESCE(SUM(CASE WHEN interval_month = 4 THEN Total_Active ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 7 THEN Total_Active ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 13 THEN Total_Active ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 25 THEN Total_Active ELSE 0 END), 0)
    FROM SummaryCounts

    UNION ALL

-- Row F: Lost to F/U
    SELECT 'F. Lost to F/U (not seen>1 month after scheduled appointment)' AS Name,
           0,
           COALESCE(SUM(CASE WHEN interval_month = 4 THEN Total_LTFU ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 7 THEN Total_LTFU ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 13 THEN Total_LTFU ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 25 THEN Total_LTFU ELSE 0 END), 0)
    FROM SummaryCounts

    UNION ALL

-- Row G: Known Dead
    SELECT 'G. Known Dead' AS Name,
           0,
           COALESCE(SUM(CASE WHEN interval_month = 4 THEN Total_Dead ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 7 THEN Total_Dead ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 13 THEN Total_Dead ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN interval_month = 25 THEN Total_Dead ELSE 0 END), 0)
    FROM SummaryCounts

    UNION ALL

-- Row H: % Alive and on ART (E/D * 100)
    SELECT 'H. % of mothers in net current cohort Alive and on ART [(E/D)x100%]' AS Name,
           0,
           COALESCE(SUM(CASE
                            WHEN interval_month = 4 THEN CONCAT(ROUND(
                                                                        (Total_Active /
                                                                         NULLIF((Total_Facility_Enrolled + Total_TI - Total_TO), 0)) *
                                                                        100, 1), ' %')
                            ELSE '0.0 %' END), '0.0 %'),
           COALESCE(SUM(CASE
                            WHEN interval_month = 7 THEN CONCAT(ROUND(
                                                                        (Total_Active /
                                                                         NULLIF((Total_Facility_Enrolled + Total_TI - Total_TO), 0)) *
                                                                        100, 1), ' %')
                            ELSE '0.0 %' END), '0.0 %'),
           COALESCE(SUM(CASE
                            WHEN interval_month = 13 THEN CONCAT(ROUND(
                                                                         (Total_Active /
                                                                          NULLIF((Total_Facility_Enrolled + Total_TI - Total_TO), 0)) *
                                                                         100, 1), ' %')
                            ELSE '0.0 %' END), '0.0 %'),
           COALESCE(SUM(CASE
                            WHEN interval_month = 25 THEN CONCAT(ROUND(
                                                                         (Total_Active /
                                                                          NULLIF((Total_Facility_Enrolled + Total_TI - Total_TO), 0)) *
                                                                         100, 1), ' %')
                            ELSE '0.0 %' END), '0.0 %')
    FROM SummaryCounts

    UNION ALL

-- Row I: % Lost to F/U (F/D * 100)
    SELECT 'I. % of mothers in net current cohort Lost to F/U [(F/D)x100%]' AS Name,
           0,
           COALESCE(SUM(CASE
                            WHEN interval_month = 4 THEN CONCAT(ROUND(
                                                                        (Total_LTFU / NULLIF((Total_Facility_Enrolled + Total_TI - Total_TO), 0)) *
                                                                        100, 1), ' %')
                            ELSE '0.0 %' END), '0.0 %'),
           COALESCE(SUM(CASE
                            WHEN interval_month = 7 THEN CONCAT(ROUND(
                                                                        (Total_LTFU / NULLIF((Total_Facility_Enrolled + Total_TI - Total_TO), 0)) *
                                                                        100, 1), ' %')
                            ELSE '0.0 %' END), '0.0 %'),
           COALESCE(SUM(CASE
                            WHEN interval_month = 13 THEN CONCAT(ROUND(
                                                                         (Total_LTFU / NULLIF((Total_Facility_Enrolled + Total_TI - Total_TO), 0)) *
                                                                         100, 1), ' %')
                            ELSE '0.0 %' END), '0.0 %'),
           COALESCE(SUM(CASE
                            WHEN interval_month = 25 THEN CONCAT(ROUND(
                                                                         (Total_LTFU / NULLIF((Total_Facility_Enrolled + Total_TI - Total_TO), 0)) *
                                                                         100, 1), ' %')
                            ELSE '0.0 %' END), '0.0 %')
    FROM SummaryCounts;

END
//

DELIMITER ;
