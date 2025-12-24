DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_mother_cohort_analysis_summary;

CREATE PROCEDURE sp_fact_mother_cohort_analysis_summary(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

WITH Follow_UP AS (SELECT follow_up.client_id                 as PatientId,
                          follow_up_status,
                          follow_up_date_followup_            AS follow_up_date,
                          art_antiretroviral_start_date       AS art_start_date,
                          treatment_end_date,
                          regimen,
                          follow_up.encounter_id,
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

     -- CTE to determine the initial ART start date for each patient.
     PMTCT_ENROLLMENT AS (SELECT client_id as PatientId,
                                 MIN(date_of_enrollment_or_booking) AS art_start_date,
                                 -- Placeholder for Transfer In logic.
                                 -- If your table has a 'transfer_in' column, replace '0' with that column name.
                                 0 AS is_transfer_in
                          FROM mamba_flat_encounter_pmtct_enrollment
                          WHERE date_of_enrollment_or_booking IS NOT NULL
                            and date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                          GROUP BY PatientId),

     -- Defines the cohort analysis intervals in months.
     IntervalsDef AS (SELECT 0 AS interval_month
                      UNION ALL
                      SELECT 3 -- Added Month 3
                      UNION ALL
                      SELECT 6
                      UNION ALL
                      SELECT 12
                      UNION ALL
                      SELECT 24),

     -- Creates an evaluation point for each patient at each interval.
     PatientIntervals AS (SELECT a.PatientId,
                                 a.art_start_date,
                                 a.is_transfer_in,
                                 i.interval_month,
                                 fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                         fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                         INTERVAL i.interval_month MONTH)) as interval_end_date
                          FROM PMTCT_ENROLLMENT a
                                   CROSS JOIN IntervalsDef i),

     -- Finds the latest follow-up for each patient AS OF the interval end date (Snapshot Logic).
     LatestFollowUpInInterval AS (SELECT pi.PatientId,
                                         pi.interval_month,
                                         pi.interval_end_date,
                                         f.follow_up_status,
                                         f.follow_up_date,
                                         f.treatment_end_date,
                                         f.regimen,
                                         f.ARTDoseDays,
                                         f.AdherenceLevel,
                                         f.pregnancy_status,
                                         f.next_visit_date,
                                         f.current_functional_status,
                                         ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month
                                                 ORDER BY f.follow_up_date DESC, f.encounter_id DESC) as rn
                                  FROM PatientIntervals pi
                                           LEFT JOIN Follow_UP f ON pi.PatientId = f.PatientId
                                      -- Look for visits occurring on or before the milestone
                                      AND f.follow_up_date <= pi.interval_end_date),

     -- Combines the latest follow-up details and determines status.
     CohortDetails AS (SELECT lfu.PatientId,
                              lfu.interval_end_date,
                              lfu.interval_month,
                              CASE
                                  -- Logic for Terminal States (Carry Forward)
                                  WHEN lfu.follow_up_status IN
                                       ('Dead', 'Transferred out', 'Stop all', 'Ran away') THEN lfu.follow_up_status

                                  -- Logic for Active vs LTFU based on Drug Coverage
                                  WHEN lfu.follow_up_status IN ('Alive', 'Restart medication') AND
                                       lfu.treatment_end_date >= lfu.interval_end_date THEN 'Active'

                                  -- Default to LTFU if alive but drugs ran out before interval end
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
                                     ON lfu.PatientId = client.client_id AND client.sex = 'FEMALE'),

     -- Aggregated Counts per Interval
     SummaryCounts AS (
         SELECT
             co.interval_month,
             COUNT(DISTINCT ai.PatientId) AS Total_Enrolled_Month0, -- Base cohort size
             SUM(CASE WHEN ai.is_transfer_in = 1 THEN 1 ELSE 0 END) AS Total_TI, -- Placeholder TI logic
             SUM(CASE WHEN co.outcome = 'Transferred out' THEN 1 ELSE 0 END) AS Total_TO,
             SUM(CASE WHEN co.outcome = 'Active' THEN 1 ELSE 0 END) AS Total_Active,
             SUM(CASE WHEN co.outcome = 'Lost to follow-up' THEN 1 ELSE 0 END) AS Total_LTFU,
             SUM(CASE WHEN co.outcome = 'Dead' THEN 1 ELSE 0 END) AS Total_Dead
         FROM PMTCT_ENROLLMENT ai
                  LEFT JOIN CohortDetails co ON ai.PatientId = co.PatientId
         GROUP BY co.interval_month
     )

-- Final Row-Based Output
-- Row A: Enrolled in PMTCT
SELECT
    'A. Enrolled in PMTCT in this facility during this month and year (Month 0)' AS Name,
    COALESCE(MAX(CASE WHEN interval_month = 0 THEN Total_Enrolled_Month0 ELSE 0 END), 0) AS 'Maternal Cohort Month 0',
    0 AS 'Maternal Cohort Month 3',
    0 AS 'Maternal Cohort Month 6',
    0 AS 'Maternal Cohort Month 12',
    0 AS 'Maternal Cohort Month 24'
FROM SummaryCounts

UNION ALL

-- Row B: Transfer In (TI)
SELECT
    'B. Total number of Transfer in (TI) since Month 0' AS Name,
    COALESCE(MAX(CASE WHEN interval_month = 0 THEN Total_TI ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 3 THEN Total_TI ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 6 THEN Total_TI ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 12 THEN Total_TI ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 24 THEN Total_TI ELSE 0 END), 0)
FROM SummaryCounts

UNION ALL

-- Row C: Transfer Out (TO)
SELECT
    'C. Total Number of Transfer out (TO) since Month 0' AS Name,
    COALESCE(MAX(CASE WHEN interval_month = 0 THEN Total_TO ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 3 THEN Total_TO ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 6 THEN Total_TO ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 12 THEN Total_TO ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 24 THEN Total_TO ELSE 0 END), 0)
FROM SummaryCounts

UNION ALL

-- Row D: Net Current Cohort (A + B - C)
SELECT
    'D. Number of mothers in the current cohort = Net Current Cohort (A+B-C)' AS Name,
    COALESCE(MAX(CASE WHEN interval_month = 0 THEN (Total_Enrolled_Month0 + Total_TI - Total_TO) ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 3 THEN (Total_Enrolled_Month0 + Total_TI - Total_TO) ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 6 THEN (Total_Enrolled_Month0 + Total_TI - Total_TO) ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 12 THEN (Total_Enrolled_Month0 + Total_TI - Total_TO) ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 24 THEN (Total_Enrolled_Month0 + Total_TI - Total_TO) ELSE 0 END), 0)
FROM SummaryCounts

UNION ALL

-- Row E: Mothers Alive and on ART
SELECT
    'E. Mothers Alive and on ART' AS Name,
    COALESCE(MAX(CASE WHEN interval_month = 0 THEN Total_Active ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 3 THEN Total_Active ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 6 THEN Total_Active ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 12 THEN Total_Active ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 24 THEN Total_Active ELSE 0 END), 0)
FROM SummaryCounts

UNION ALL

-- Row F: Lost to F/U
SELECT
    'F. Lost to F/U (not seen>1 month after scheduled appointment)' AS Name,
    COALESCE(MAX(CASE WHEN interval_month = 0 THEN Total_LTFU ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 3 THEN Total_LTFU ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 6 THEN Total_LTFU ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 12 THEN Total_LTFU ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 24 THEN Total_LTFU ELSE 0 END), 0)
FROM SummaryCounts

UNION ALL

-- Row G: Known Dead
SELECT
    'G. Known Dead' AS Name,
    COALESCE(MAX(CASE WHEN interval_month = 0 THEN Total_Dead ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 3 THEN Total_Dead ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 6 THEN Total_Dead ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 12 THEN Total_Dead ELSE 0 END), 0),
    COALESCE(MAX(CASE WHEN interval_month = 24 THEN Total_Dead ELSE 0 END), 0)
FROM SummaryCounts

UNION ALL

-- Row H: % Alive and on ART (E/D * 100)
SELECT
    'H. % of mothers in net current cohort Alive and on ART [(E/D)x100%]' AS Name,
    COALESCE(MAX(CASE WHEN interval_month = 0 THEN CONCAT(ROUND((Total_Active / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %'),
    COALESCE(MAX(CASE WHEN interval_month = 3 THEN CONCAT(ROUND((Total_Active / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %'),
    COALESCE(MAX(CASE WHEN interval_month = 6 THEN CONCAT(ROUND((Total_Active / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %'),
    COALESCE(MAX(CASE WHEN interval_month = 12 THEN CONCAT(ROUND((Total_Active / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %'),
    COALESCE(MAX(CASE WHEN interval_month = 24 THEN CONCAT(ROUND((Total_Active / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %')
FROM SummaryCounts

UNION ALL

-- Row I: % Lost to F/U (F/D * 100)
SELECT
    'I. % of mothers in net current cohort Lost to F/U [(F/D)x100%]' AS Name,
    COALESCE(MAX(CASE WHEN interval_month = 0 THEN CONCAT(ROUND((Total_LTFU / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %'),
    COALESCE(MAX(CASE WHEN interval_month = 3 THEN CONCAT(ROUND((Total_LTFU / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %'),
    COALESCE(MAX(CASE WHEN interval_month = 6 THEN CONCAT(ROUND((Total_LTFU / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %'),
    COALESCE(MAX(CASE WHEN interval_month = 12 THEN CONCAT(ROUND((Total_LTFU / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %'),
    COALESCE(MAX(CASE WHEN interval_month = 24 THEN CONCAT(ROUND((Total_LTFU / NULLIF((Total_Enrolled_Month0 + Total_TI - Total_TO), 0)) * 100, 1), ' %') ELSE '0.0 %' END), '0.0 %')
FROM SummaryCounts;

END //

DELIMITER ;
