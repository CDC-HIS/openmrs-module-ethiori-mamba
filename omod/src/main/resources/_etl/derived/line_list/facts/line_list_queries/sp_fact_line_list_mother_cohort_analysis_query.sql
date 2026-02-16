DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_mother_cohort_analysis_query;

CREATE PROCEDURE sp_fact_line_list_mother_cohort_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

WITH FollowUpEncounters AS (
    SELECT follow_up.encounter_id,
           follow_up.client_id                 AS PatientId,
           follow_up_status,
           follow_up_date_followup_            AS follow_up_date,
           art_antiretroviral_start_date       AS art_start_date,
           treatment_end_date,
           regimen,
           currently_breastfeeding_child,
           pregnancy_status,
           antiretroviral_art_dispensed_dose_i AS ARTDoseDays,
           anitiretroviral_adherence_level     AS AdherenceLevel,
           next_visit_date,
           date_of_reported_hiv_viral_load     AS viral_load_sent_date,
           date_viral_load_results_received    AS viral_load_received_date,
           viral_load_test_status              AS viral_load_result,
           hiv_viral_load                      AS viral_load_count,
           cd4_count,
           cd4_                                AS cd4_percent,
           current_functional_status,
           visitect_cd4_result,
           visitect_cd4_test_date
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
    WHERE follow_up.client_id IS NOT NULL
      AND follow_up_date_followup_ IS NOT NULL
      AND follow_up_status IS NOT NULL
),

     PMTCT_ENROLLMENT AS (
         SELECT enrollment.client_id                          AS PatientId,
                MIN(enrollment.date_of_enrollment_or_booking) AS date_of_enrollment_or_booking
         FROM mamba_flat_encounter_pmtct_enrollment enrollment
         WHERE date_of_enrollment_or_booking IS NOT NULL
           AND date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE
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
             pi.interval_start_date,
             pi.interval_end_date,

             f.follow_up_status AS strict_status,
             f.treatment_end_date AS strict_tx_end_date,
             f.regimen AS strict_regimen,
             f.pregnancy_status AS strict_pregnancy,
             f.currently_breastfeeding_child AS strict_breastfeeding,

             f.follow_up_date,
             f.ARTDoseDays,
             f.AdherenceLevel,
             f.next_visit_date,
             f.current_functional_status

         FROM PatientIntervals pi
                  LEFT JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
             AND f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date
             AND f.follow_up_date = (
                 SELECT MAX(sub_f.follow_up_date)
                 FROM FollowUpEncounters sub_f
                 WHERE sub_f.PatientId = pi.PatientId
                   AND sub_f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date
             )
     ),

     StateCalculation AS (
         SELECT
             sid.*,
             COUNT(strict_status) OVER (PARTITION BY PatientId ORDER BY interval_month) as status_partition,
                 MAX(strict_tx_end_date) OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_tx_end_date_so_far
         FROM StrictIntervalData sid
     ),

     FilledState AS (
         SELECT
             sc.*,
             FIRST_VALUE(strict_status) OVER (PARTITION BY PatientId, status_partition ORDER BY interval_month) as last_known_status,
                 FIRST_VALUE(strict_regimen) OVER (PARTITION BY PatientId, status_partition ORDER BY interval_month) as last_known_regimen,
                 FIRST_VALUE(strict_pregnancy) OVER (PARTITION BY PatientId, status_partition ORDER BY interval_month) as last_known_pregnancy,
                 FIRST_VALUE(strict_breastfeeding) OVER (PARTITION BY PatientId, status_partition ORDER BY interval_month) as last_known_breastfeeding
         FROM StateCalculation sc
     ),

     CohortDetails AS (
         SELECT
             fs.PatientId,
             client.patient_name,
             client.patient_uuid,
             client.mrn,
             client.UAN,
             client.Sex,
             client.date_of_birth,
             fs.interval_month,
             fs.interval_start_date,
             fs.interval_end_date,

             CASE
                 WHEN strict_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away') THEN strict_status
                 WHEN last_known_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away') THEN last_known_status
                 WHEN strict_status IS NOT NULL THEN strict_status
                 WHEN strict_status IS NULL AND max_tx_end_date_so_far >= interval_end_date THEN 'Active'
                 ELSE 'Loss to follow-up (LTFU)'
                 END AS final_outcome,

             -- B. Final Regimen (Carried Forward)
             CASE
                 WHEN strict_regimen IS NOT NULL THEN strict_regimen
                 WHEN max_tx_end_date_so_far >= interval_end_date THEN last_known_regimen
                 ELSE NULL
                 END AS final_regimen,

             -- C. Strict Visit Data (Keep NULL if they didn't come)
             fs.follow_up_date AS strict_visit_date,
             fs.ARTDoseDays,
             fs.AdherenceLevel,
             fs.strict_tx_end_date AS treatment_end_date_visit,
             fs.next_visit_date,
             fs.strict_status AS follow_up_status_visit,
             fs.current_functional_status,

             CASE WHEN strict_pregnancy IS NOT NULL THEN strict_pregnancy ELSE last_known_pregnancy END AS final_pregnancy,
             CASE WHEN strict_breastfeeding IS NOT NULL THEN strict_breastfeeding ELSE last_known_breastfeeding END AS final_breastfeeding

         FROM FilledState fs
                  JOIN mamba_dim_client client ON fs.PatientId = client.client_id
     )

SELECT
    co.patient_name                                                                          AS 'Patient Name',
        co.patient_uuid                                                                          AS 'UUID',
        co.mrn                                                                                   AS 'MRN',
        co.UAN,
    co.Sex,
    co.date_of_birth                                                                         AS 'Date of Birth',
        co.date_of_birth                                                                         AS 'Date of Birth EC.', -- Assuming conversion happens in app or duplicate col
        ai.date_of_enrollment_or_booking                                                         AS 'Date of Enrollment or Booking',
        ai.date_of_enrollment_or_booking                                                         AS 'Date of Enrollment or Booking EC.',

       -- === 0 MONTHS ===
        MAX(CASE WHEN co.interval_month = 0 THEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 0 Months',
        MAX(CASE WHEN co.interval_month = 0 THEN
                     CASE
                         WHEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date) < 15 AND co.final_regimen LIKE '4%' THEN 'On Original 1st Line'
                         WHEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date) < 15 AND co.final_regimen LIKE '1%' THEN 'On Alternate 1st Line'
                         WHEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date) >= 15 AND co.final_regimen LIKE '1%' THEN 'On Original 1st Line'
                         WHEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date) >= 15 AND co.final_regimen LIKE '4%' THEN 'On Alternate 1st Line'
                         ELSE 'Other'
                         END
                 ELSE NULL END) AS 'Regimen Line at Zero Months',

        MAX(CASE WHEN co.interval_month = 0 THEN co.strict_visit_date ELSE NULL END)             AS 'Latest Follow-Up Date at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.final_regimen ELSE NULL END)                 AS 'Latest Regimen at Zero Months',
        COALESCE(MAX(CASE WHEN co.interval_month = 0 THEN co.ARTDoseDays ELSE NULL END), 0)      AS 'Latest Regimen Dose Days at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_status_visit ELSE NULL END)        AS 'Latest Follow-up Status at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.final_pregnancy ELSE NULL END)               AS 'Pregnancy Status at Month Zero',
        MAX(CASE WHEN co.interval_month = 0 THEN co.final_breastfeeding ELSE NULL END)           AS 'Breast Feeding Status at Month Zero',
        MAX(CASE WHEN co.interval_month = 0 THEN co.treatment_end_date_visit ELSE NULL END)      AS 'Last TX_Curr Date at Zero Months',

       -- === 3 MONTHS (Interval 4) ===
        MAX(CASE WHEN co.interval_month = 4 THEN co.interval_start_date ELSE NULL END)           AS 'Interval start date at 3 Months EC.',
        MAX(CASE WHEN co.interval_month = 4 THEN co.interval_end_date ELSE NULL END)             AS 'Interval end date at 3 Months EC.',
        MAX(CASE WHEN co.interval_month = 4 THEN co.final_outcome ELSE NULL END)                 AS 'Outcome at 3 Months',
        MAX(CASE WHEN co.interval_month = 4 THEN co.strict_visit_date ELSE NULL END)             AS 'Latest Follow-Up Date at 3 Months',
        MAX(CASE WHEN co.interval_month = 4 THEN co.final_regimen ELSE NULL END)                 AS 'Latest Regimen at 3 Months',
        MAX(CASE WHEN co.interval_month = 4 THEN co.follow_up_status_visit ELSE NULL END)        AS 'Latest Follow-up Status at 3 Months',
        MAX(CASE WHEN co.interval_month = 4 THEN co.final_pregnancy ELSE NULL END)               AS 'Pregnant at 3 Months',
        MAX(CASE WHEN co.interval_month = 4 THEN co.final_breastfeeding ELSE NULL END)           AS 'Breast Feeding Status at 3 Months',
        MAX(CASE WHEN co.interval_month = 4 THEN co.treatment_end_date_visit ELSE NULL END)      AS 'Last TX_Curr Date at 3 Months',

       -- === 6 MONTHS (Interval 7) ===
        MAX(CASE WHEN co.interval_month = 7 THEN co.interval_start_date ELSE NULL END)           AS 'Interval start date at 6 Months EC.',
        MAX(CASE WHEN co.interval_month = 7 THEN co.interval_end_date ELSE NULL END)             AS 'Interval end date at 6 Months EC.',
        MAX(CASE WHEN co.interval_month = 7 THEN co.final_outcome ELSE NULL END)                 AS 'Outcome at 6 Months',
        MAX(CASE WHEN co.interval_month = 7 THEN co.strict_visit_date ELSE NULL END)             AS 'Latest Follow-Up Date at 6 Months',
        MAX(CASE WHEN co.interval_month = 7 THEN co.final_regimen ELSE NULL END)                 AS 'Latest Regimen at 6 Months',
        MAX(CASE WHEN co.interval_month = 7 THEN co.follow_up_status_visit ELSE NULL END)        AS 'Latest Follow-up Status at 6 Months',
        MAX(CASE WHEN co.interval_month = 7 THEN co.final_pregnancy ELSE NULL END)               AS 'Pregnant at 6 Months',
        MAX(CASE WHEN co.interval_month = 7 THEN co.final_breastfeeding ELSE NULL END)           AS 'Breast Feeding Status at 6 Months',
        MAX(CASE WHEN co.interval_month = 7 THEN co.treatment_end_date_visit ELSE NULL END)      AS 'Last TX_Curr Date at 6 Months',

       -- === 12 MONTHS (Interval 13) ===
        MAX(CASE WHEN co.interval_month = 13 THEN co.interval_start_date ELSE NULL END)          AS 'Interval start date at 12 Months EC.',
        MAX(CASE WHEN co.interval_month = 13 THEN co.interval_end_date ELSE NULL END)            AS 'Interval end date at 12 Months EC.',
        MAX(CASE WHEN co.interval_month = 13 THEN co.final_outcome ELSE NULL END)                AS 'Outcome at 12 Months',
        MAX(CASE WHEN co.interval_month = 13 THEN co.strict_visit_date ELSE NULL END)            AS 'Latest Follow-Up Date at 12 Months',
        MAX(CASE WHEN co.interval_month = 13 THEN co.final_regimen ELSE NULL END)                AS 'Latest Regimen at 12 Months',
        MAX(CASE WHEN co.interval_month = 13 THEN co.follow_up_status_visit ELSE NULL END)       AS 'Latest Follow-up Status at 12 Months',
        MAX(CASE WHEN co.interval_month = 13 THEN co.final_pregnancy ELSE NULL END)              AS 'Pregnant at 12 Months',
        MAX(CASE WHEN co.interval_month = 13 THEN co.final_breastfeeding ELSE NULL END)          AS 'Breast Feeding Status at 12 Months',
        MAX(CASE WHEN co.interval_month = 13 THEN co.treatment_end_date_visit ELSE NULL END)     AS 'Last TX_Curr Date at 12 Months',

       -- === 24 MONTHS (Interval 25) ===
        MAX(CASE WHEN co.interval_month = 25 THEN co.interval_start_date ELSE NULL END)          AS 'Interval start date at 24 Months EC.',
        MAX(CASE WHEN co.interval_month = 25 THEN co.interval_end_date ELSE NULL END)            AS 'Interval end date at 24 Months EC.',
        MAX(CASE WHEN co.interval_month = 25 THEN co.final_outcome ELSE NULL END)                AS 'Outcome at 24 Months',
        MAX(CASE WHEN co.interval_month = 25 THEN co.strict_visit_date ELSE NULL END)            AS 'Latest Follow-Up Date at 24 Months',
        MAX(CASE WHEN co.interval_month = 25 THEN co.final_regimen ELSE NULL END)                AS 'Latest Regimen at 24 Months',
        MAX(CASE WHEN co.interval_month = 25 THEN co.follow_up_status_visit ELSE NULL END)       AS 'Latest Follow-up Status at 24 Months',
        MAX(CASE WHEN co.interval_month = 25 THEN co.final_pregnancy ELSE NULL END)              AS 'Pregnant at 24 Months',
        MAX(CASE WHEN co.interval_month = 25 THEN co.final_breastfeeding ELSE NULL END)          AS 'Breast Feeding Status at 24 Months',
        MAX(CASE WHEN co.interval_month = 25 THEN co.treatment_end_date_visit ELSE NULL END)     AS 'Last TX_Curr Date at 24 Months'

FROM PMTCT_ENROLLMENT ai
         JOIN CohortDetails co ON ai.PatientId = co.PatientId
GROUP BY ai.PatientId, ai.date_of_enrollment_or_booking
ORDER BY ai.date_of_enrollment_or_booking;

END //

DELIMITER ;
