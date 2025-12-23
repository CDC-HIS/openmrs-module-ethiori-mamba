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
     PMTCT_ENROLLMENT AS (SELECT client_id as PatientId, MIN(date_of_enrollment_or_booking) AS art_start_date
                          FROM mamba_flat_encounter_pmtct_enrollment
                          WHERE date_of_enrollment_or_booking IS NOT NULL
                            and date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                          GROUP BY PatientId),

     -- Defines the cohort analysis intervals in months.
     IntervalsDef AS (SELECT 0 AS interval_month
                      UNION ALL
                      SELECT 6
                      UNION ALL
                      SELECT 12
                      UNION ALL
                      SELECT 24),

     -- Creates an evaluation point for each patient at each interval.
     PatientIntervals AS (SELECT a.PatientId,
                                 a.art_start_date,
                                 i.interval_month,
                                 fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                         fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                         INTERVAL i.interval_month MONTH)) as interval_end_date
                          FROM PMTCT_ENROLLMENT a
                                   CROSS JOIN IntervalsDef i),

     -- Finds the latest follow-up for each patient AS OF the interval end date.
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
                                      -- CHANGED: Look for visits occurring on or before the milestone
                                      AND f.follow_up_date <= pi.interval_end_date),

     -- Combines the latest follow-up details and pre-calculates Regimen Line logic.
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
                              lfu.current_functional_status,
                              -- Pre-calculate Regimen Line Logic
                              CASE
                                  WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, lfu.interval_end_date) < 15 AND
                                       lfu.regimen LIKE '4%' THEN 'On Original 1st Line Regimen'
                                  WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, lfu.interval_end_date) >= 15 AND
                                       lfu.regimen LIKE '1%' THEN 'On Original 1st Line Regimen'

                                  WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, lfu.interval_end_date) < 15 AND
                                       lfu.regimen LIKE '1%' THEN 'On Alternate 1st Line Regimen (Substituted)'
                                  WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, lfu.interval_end_date) >= 15 AND
                                       lfu.regimen LIKE '4%' THEN 'On Alternate 1st Line Regimen (Substituted)'

                                  WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, lfu.interval_end_date) < 15 AND
                                       lfu.regimen LIKE '5%' THEN 'On 2nd Line Regimen (Switched)'
                                  WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, lfu.interval_end_date) >= 15 AND
                                       lfu.regimen LIKE '2%' THEN 'On 2nd Line Regimen (Switched)'

                                  WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, lfu.interval_end_date) < 15 AND
                                       lfu.regimen LIKE '6%' THEN 'On 3rd Line Regimen (Switched)'
                                  WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, lfu.interval_end_date) >= 15 AND
                                       lfu.regimen LIKE '3%' THEN 'On 3rd Line Regimen (Switched)'
                                  ELSE NULL
                                  END AS regimen_line_category
                       FROM (SELECT * FROM LatestFollowUpInInterval WHERE rn = 1) lfu
                                JOIN mamba_dim_client client
                                     ON lfu.PatientId = client.client_id AND client.sex = 'FEMALE')

-- Final SELECT statement to generate the SUMMARY
SELECT
    -- Total Cohort Count
    COALESCE(COUNT(DISTINCT ai.PatientId), 0) AS 'Total Cohort',

        -- 0 Months Summary
    COALESCE(SUM(CASE WHEN co.interval_month = 0 AND co.outcome = 'Active' THEN 1 ELSE 0 END), 0) AS 'Active at 0 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 0 AND co.outcome = 'Dead' THEN 1 ELSE 0 END), 0) AS 'Dead at 0 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 0 AND co.outcome = 'Lost to follow-up' THEN 1 ELSE 0 END), 0) AS 'LTFU at 0 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 0 AND co.outcome = 'Transferred out' THEN 1 ELSE 0 END), 0) AS 'Transferred Out at 0 Months',

        -- 6 Months Summary
    COALESCE(SUM(CASE WHEN co.interval_month = 6 AND co.outcome = 'Active' THEN 1 ELSE 0 END), 0) AS 'Active at 6 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 6 AND co.outcome = 'Dead' THEN 1 ELSE 0 END), 0) AS 'Dead at 6 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 6 AND co.outcome = 'Lost to follow-up' THEN 1 ELSE 0 END), 0) AS 'LTFU at 6 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 6 AND co.outcome = 'Transferred out' THEN 1 ELSE 0 END), 0) AS 'Transferred Out at 6 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 6 AND co.regimen_line_category LIKE '%Original 1st Line%' THEN 1 ELSE 0 END), 0) AS 'Original 1st Line at 6 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 6 AND co.regimen_line_category LIKE '%Alternate 1st Line%' THEN 1 ELSE 0 END), 0) AS 'Alternate 1st Line at 6 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 6 AND co.regimen_line_category LIKE '%2nd Line%' THEN 1 ELSE 0 END), 0) AS '2nd Line at 6 Months',

        -- 12 Months Summary
    COALESCE(SUM(CASE WHEN co.interval_month = 12 AND co.outcome = 'Active' THEN 1 ELSE 0 END), 0) AS 'Active at 12 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 12 AND co.outcome = 'Dead' THEN 1 ELSE 0 END), 0) AS 'Dead at 12 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 12 AND co.outcome = 'Lost to follow-up' THEN 1 ELSE 0 END), 0) AS 'LTFU at 12 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 12 AND co.outcome = 'Transferred out' THEN 1 ELSE 0 END), 0) AS 'Transferred Out at 12 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 12 AND co.regimen_line_category LIKE '%Original 1st Line%' THEN 1 ELSE 0 END), 0) AS 'Original 1st Line at 12 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 12 AND co.regimen_line_category LIKE '%Alternate 1st Line%' THEN 1 ELSE 0 END), 0) AS 'Alternate 1st Line at 12 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 12 AND co.regimen_line_category LIKE '%2nd Line%' THEN 1 ELSE 0 END), 0) AS '2nd Line at 12 Months',

        -- 24 Months Summary
    COALESCE(SUM(CASE WHEN co.interval_month = 24 AND co.outcome = 'Active' THEN 1 ELSE 0 END), 0) AS 'Active at 24 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 24 AND co.outcome = 'Dead' THEN 1 ELSE 0 END), 0) AS 'Dead at 24 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 24 AND co.outcome = 'Lost to follow-up' THEN 1 ELSE 0 END), 0) AS 'LTFU at 24 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 24 AND co.outcome = 'Transferred out' THEN 1 ELSE 0 END), 0) AS 'Transferred Out at 24 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 24 AND co.regimen_line_category LIKE '%Original 1st Line%' THEN 1 ELSE 0 END), 0) AS 'Original 1st Line at 24 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 24 AND co.regimen_line_category LIKE '%Alternate 1st Line%' THEN 1 ELSE 0 END), 0) AS 'Alternate 1st Line at 24 Months',
    COALESCE(SUM(CASE WHEN co.interval_month = 24 AND co.regimen_line_category LIKE '%2nd Line%' THEN 1 ELSE 0 END), 0) AS '2nd Line at 24 Months'

FROM PMTCT_ENROLLMENT ai
         LEFT JOIN
     CohortDetails co ON ai.PatientId = co.PatientId
WHERE ai.art_start_date <= COALESCE(REPORT_END_DATE, CURDATE());

END //

DELIMITER ;
