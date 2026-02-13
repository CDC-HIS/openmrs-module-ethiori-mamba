DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_mother_cohort_analysis_query;

CREATE PROCEDURE sp_fact_line_list_mother_cohort_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH Follow_UP AS (SELECT follow_up.client_id                 as PatientId,
                              follow_up_status,
                              follow_up_date_followup_            AS follow_up_date,
                              art_antiretroviral_start_date       AS art_start_date,
                              treatment_end_date,
                              regimen,
                              follow_up.encounter_id,
                              currently_breastfeeding_child,
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

         PMTCT_ENROLLMENT AS (SELECT enrollment.client_id                          as PatientId,
                                     MAX(enrollment.date_of_enrollment_or_booking) AS date_of_enrollment_or_booking,
                                     MAX(f4.pregnancy_status)                      AS pregnancy_status,
                                     MAX(f7.currently_breastfeeding_child)         AS currently_breastfeeding_child
                              FROM mamba_flat_encounter_pmtct_enrollment enrollment
                              LEFT JOIN mamba_flat_encounter_follow_up_4 f4 ON enrollment.client_id = f4.client_id
                              LEFT JOIN mamba_flat_encounter_follow_up_7 f7 ON enrollment.client_id = f4.client_id
                              WHERE date_of_enrollment_or_booking IS NOT NULL
                                and date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                              GROUP BY enrollment.client_id
                           ),

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
                                             pi.interval_start_date,
                                             pi.interval_end_date,
                                             f.follow_up_status,
                                             f.follow_up_date,
                                             f.treatment_end_date,
                                             f.regimen,
                                             f.pregnancy_status,
                                             f.currently_breastfeeding_child,
                                             f.ARTDoseDays,
                                             f.AdherenceLevel,
                                             f.next_visit_date,
                                             f.current_functional_status,
                                             ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month
                                                 ORDER BY f.follow_up_date DESC, f.encounter_id DESC) as rn
                                      FROM PatientIntervals pi
                                               LEFT JOIN Follow_UP f ON pi.PatientId = f.PatientId
                                          AND f.follow_up_date <= pi.interval_end_date),

         CohortDetails AS (
             SELECT lfu.PatientId,
                    client.patient_name,
                    client.patient_uuid,
                    client.mrn,
                    client.UAN,
                    client.Sex,
                    client.date_of_birth,
                    lfu.interval_start_date,
                    lfu.interval_end_date,
                    lfu.interval_month,
                    lfu.follow_up_date,
                    CASE
                        WHEN lfu.follow_up_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                            THEN lfu.follow_up_status
                        WHEN lfu.follow_up_status IN ('Alive', 'Restart medication')
                            AND lfu.treatment_end_date >= lfu.interval_end_date
                            THEN 'Active'
                        ELSE 'Lost to follow-up'
                        END AS outcome,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.follow_up_date
                        ELSE NULL
                        END AS follow_up_date_visit,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.regimen
                        ELSE NULL
                        END AS regimen,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.ARTDoseDays
                        ELSE NULL
                        END AS ARTDoseDays,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.follow_up_status
                        ELSE NULL
                        END AS follow_up_status_visit,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.AdherenceLevel
                        ELSE NULL
                        END AS AdherenceLevel,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.treatment_end_date
                        ELSE NULL
                        END AS treatment_end_date_visit,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.pregnancy_status
                        ELSE NULL
                        END AS pregnancy_status,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.currently_breastfeeding_child
                        ELSE NULL
                        END AS currently_breastfeeding_child,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.current_functional_status
                        ELSE NULL
                        END AS current_functional_status,

                    CASE
                        WHEN lfu.follow_up_date >= lfu.interval_start_date THEN lfu.next_visit_date
                        ELSE NULL
                        END AS next_visit_date,


                    CASE

                        WHEN lfu.follow_up_date < lfu.interval_start_date THEN NULL


                        WHEN TIMESTAMPDIFF(YEAR, client.date_of_birth, lfu.interval_end_date) < 15 THEN
                            CASE
                                WHEN lfu.regimen LIKE '4%' THEN 'On Original 1st Line Regimen'
                                WHEN lfu.regimen LIKE '1%' THEN 'On Alternate 1st Line Regimen (Substituted)'
                                WHEN lfu.regimen LIKE '5%' THEN 'On 2nd Line Regimen (Switched)'
                                WHEN lfu.regimen LIKE '6%' THEN 'On 3rd Line Regimen (Switched)'
                                ELSE NULL
                                END

                        ELSE
                            CASE
                                WHEN lfu.regimen LIKE '1%' THEN 'On Original 1st Line Regimen'
                                WHEN lfu.regimen LIKE '4%' THEN 'On Alternate 1st Line Regimen (Substituted)'
                                WHEN lfu.regimen LIKE '2%' THEN 'On 2nd Line Regimen (Switched)'
                                WHEN lfu.regimen LIKE '3%' THEN 'On 3rd Line Regimen (Switched)'
                                ELSE NULL
                                END
                        END AS regimen_line_category

             FROM (SELECT * FROM LatestFollowUpInInterval WHERE rn = 1) lfu
                      JOIN mamba_dim_client client ON lfu.PatientId = client.client_id
         )

    SELECT co.patient_name                                                                          AS 'Patient Name',
           co.patient_uuid                                                                          AS 'UUID',
           co.mrn                                                                                   AS 'MRN',
           co.UAN,
           co.Sex,
           co.date_of_birth                                                                         AS 'Date of Birth',
           co.date_of_birth                                                                         AS 'Date of Birth EC.',
           ai.date_of_enrollment_or_booking                                                         AS 'Date of Enrollment or Booking',
           ai.date_of_enrollment_or_booking                                                         AS 'Date of Enrollment or Booking EC.',

           -- 0 Months
           MAX(CASE
                   WHEN co.interval_month = 0 THEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                   AS 'Age at 0 Months',
           MAX(CASE
                   WHEN co.interval_month = 0 THEN co.regimen_line_category
                   ELSE NULL END)                                                                   AS 'Regimen Line at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at Zero Months EC.',
           MAX(CASE WHEN co.interval_month = 0 THEN co.regimen ELSE NULL END)                       AS 'Latest Regimen at Zero Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 0 THEN co.ARTDoseDays ELSE NULL END),
                    0)                                                                              AS 'Latest Regimen Dose Days at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_status_visit ELSE NULL END)              AS 'Latest Follow-up Status at Zero Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 0 THEN co.AdherenceLevel ELSE NULL END),
                    0)                                                                              AS 'Latest Adherence at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN ai.pregnancy_status ELSE NULL END)              AS 'Pregnancy Status at Month Zero',
           MAX(CASE
                   WHEN co.interval_month = 0 THEN ai.currently_breastfeeding_child
                   ELSE NULL END)                                                                   AS 'Breast Feeding Status at Month Zero',
           MAX(CASE WHEN co.interval_month = 0 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at Zero Months EC.',
           MAX(CASE WHEN co.interval_month = 0 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at Zero Months EC.',


           -- 3 Months
           MAX(CASE WHEN co.interval_month = 4 THEN co.interval_start_date ELSE NULL END)           AS 'Interval start date at 3 Months EC.',
           MAX(CASE WHEN co.interval_month = 4 THEN co.interval_end_date ELSE NULL END)             AS 'Interval end date at 3 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 4 THEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                   AS 'Age at 3 Months',
           MAX(CASE
                   WHEN co.interval_month = 4 THEN co.regimen_line_category
                   ELSE NULL END)                                                                   AS 'Regimen Line at 3 Months',
           MAX(CASE WHEN co.interval_month = 4 THEN co.outcome ELSE NULL END)                       AS 'Outcome at 3 Months',
           MAX(CASE WHEN co.interval_month = 4 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at 3 Months',
           MAX(CASE WHEN co.interval_month = 4 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at 3 Months EC.',
           MAX(CASE WHEN co.interval_month = 4 THEN co.regimen ELSE NULL END)                       AS 'Latest Regimen at 3 Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 4 THEN co.ARTDoseDays ELSE NULL END),
                    0)                                                                              AS 'Latest Regimen Dose Days at 3 Months',
           MAX(CASE WHEN co.interval_month = 4 THEN co.follow_up_status_visit ELSE NULL END)              AS 'Latest Follow-up Status at 3 Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 4 THEN co.AdherenceLevel ELSE NULL END),
                    0)                                                                              AS 'Latest Adherence at 3 Months',
           MAX(CASE WHEN co.interval_month = 4 THEN co.pregnancy_status ELSE NULL END)              AS 'Pregnant at 3 Months',
           MAX(CASE
                   WHEN co.interval_month = 4 THEN co.currently_breastfeeding_child
                   ELSE NULL END)                                                                   AS 'Breast Feeding Status at 3 Months',
           MAX(CASE WHEN co.interval_month = 4 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at 3 Months',
           MAX(CASE WHEN co.interval_month = 4 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at 3 Months EC.',
           MAX(CASE WHEN co.interval_month = 4 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at 3 Months',
           MAX(CASE WHEN co.interval_month = 4 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at 3 Months EC.',


           -- 6 Months
           MAX(CASE WHEN co.interval_month = 7 THEN co.interval_start_date ELSE NULL END)           AS 'Interval start date at 6 Months EC.',
           MAX(CASE WHEN co.interval_month = 7 THEN co.interval_end_date ELSE NULL END)             AS 'Interval end date at 6 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 7 THEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                   AS 'Age at 6 Months',
           MAX(CASE
                   WHEN co.interval_month = 7 THEN co.regimen_line_category
                   ELSE NULL END)                                                                   AS 'Regimen Line at 6 Months',
           MAX(CASE WHEN co.interval_month = 7 THEN co.outcome ELSE NULL END)                       AS 'Outcome at 6 Months',
           MAX(CASE WHEN co.interval_month = 7 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at 6 Months',
           MAX(CASE WHEN co.interval_month = 7 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at 6 Months EC.',
           MAX(CASE WHEN co.interval_month = 7 THEN co.regimen ELSE NULL END)                       AS 'Latest Regimen at 6 Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 7 THEN co.ARTDoseDays ELSE NULL END),
                    0)                                                                              AS 'Latest Regimen Dose Days at 6 Months',
           MAX(CASE WHEN co.interval_month = 7 THEN co.follow_up_status_visit ELSE NULL END)              AS 'Latest Follow-up Status at 6 Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 7 THEN co.AdherenceLevel ELSE NULL END),
                    0)                                                                              AS 'Latest Adherence at 6 Months',
           MAX(CASE WHEN co.interval_month = 7 THEN co.pregnancy_status ELSE NULL END)              AS 'Pregnant at 6 Months',
           MAX(CASE
                   WHEN co.interval_month = 7 THEN co.currently_breastfeeding_child
                   ELSE NULL END)                                                                   AS 'Breast Feeding Status at 6 Months',
           MAX(CASE WHEN co.interval_month = 7 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at 6 Months',
           MAX(CASE WHEN co.interval_month = 7 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at 6 Months EC.',
           MAX(CASE WHEN co.interval_month = 7 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at 6 Months',
           MAX(CASE WHEN co.interval_month = 7 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at 6 Months EC.',

           -- 12 Months
           MAX(CASE WHEN co.interval_month = 13 THEN co.interval_start_date ELSE NULL END)           AS 'Interval start date at 12 Months EC.',
           MAX(CASE WHEN co.interval_month = 13 THEN co.interval_end_date ELSE NULL END)             AS 'Interval end date at 12 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 13 THEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                   AS 'Age at 12 Months',
           MAX(CASE
                   WHEN co.interval_month = 13 THEN co.regimen_line_category
                   ELSE NULL END)                                                                   AS 'Regimen Line at 12 Months',
           MAX(CASE WHEN co.interval_month = 13 THEN co.outcome ELSE NULL END)                       AS 'Outcome at 12 Months',
           MAX(CASE WHEN co.interval_month = 13 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at 12 Months',
           MAX(CASE WHEN co.interval_month = 13 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at 12 Months EC.',
           MAX(CASE WHEN co.interval_month = 13 THEN co.regimen ELSE NULL END)                       AS 'Latest Regimen at 12 Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 13 THEN co.ARTDoseDays ELSE NULL END),
                    0)                                                                              AS 'Latest Regimen Dose Days at 12 Months',
           MAX(CASE WHEN co.interval_month = 13 THEN co.follow_up_status_visit ELSE NULL END)              AS 'Latest Follow-up Status at 12 Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 13 THEN co.AdherenceLevel ELSE NULL END),
                    0)                                                                              AS 'Latest Adherence at 12 Months',
           MAX(CASE WHEN co.interval_month = 13 THEN co.pregnancy_status ELSE NULL END)              AS 'Pregnant at 12 Months',
           MAX(CASE
                   WHEN co.interval_month = 13 THEN co.currently_breastfeeding_child
                   ELSE NULL END)                                                                   AS 'Breast Feeding Status at 12 Months',
           MAX(CASE WHEN co.interval_month = 13 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at 12 Months',
           MAX(CASE WHEN co.interval_month = 13 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at 12 Months EC.',
           MAX(CASE WHEN co.interval_month = 13 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at 12 Months',
           MAX(CASE WHEN co.interval_month = 13 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at 12 Months EC.',

           -- 24 Months
           MAX(CASE WHEN co.interval_month = 25 THEN co.interval_start_date ELSE NULL END)           AS 'Interval start date at 24 Months EC.',
           MAX(CASE WHEN co.interval_month = 25 THEN co.interval_end_date ELSE NULL END)             AS 'Interval end date at 24 Months EC.',
           MAX(CASE
                   WHEN co.interval_month = 25 THEN TIMESTAMPDIFF(YEAR, co.date_of_birth, co.interval_end_date)
                   ELSE NULL END)                                                                   AS 'Age at 24 Months',
           MAX(CASE
                   WHEN co.interval_month = 25 THEN co.regimen_line_category
                   ELSE NULL END)                                                                   AS 'Regimen Line at 24 Months',
           MAX(CASE WHEN co.interval_month = 25 THEN co.outcome ELSE NULL END)                       AS 'Outcome at 24 Months',
           MAX(CASE WHEN co.interval_month = 25 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at 24 Months',
           MAX(CASE WHEN co.interval_month = 25 THEN co.follow_up_date ELSE NULL END)                AS 'Latest Follow-Up Date at 24 Months EC.',
           MAX(CASE WHEN co.interval_month = 25 THEN co.regimen ELSE NULL END)                       AS 'Latest Regimen at 24 Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 25 THEN co.ARTDoseDays ELSE NULL END),
                    0)                                                                              AS 'Latest Regimen Dose Days at 24 Months',
           MAX(CASE WHEN co.interval_month = 25 THEN co.follow_up_status_visit ELSE NULL END)              AS 'Latest Follow-up Status at 24 Months',
           COALESCE(MAX(CASE WHEN co.interval_month = 25 THEN co.AdherenceLevel ELSE NULL END),
                    0)                                                                              AS 'Latest Adherence at 24 Months',
           MAX(CASE WHEN co.interval_month = 25 THEN co.pregnancy_status ELSE NULL END)              AS 'Pregnant at 24 Months',
           MAX(CASE
                   WHEN co.interval_month = 25 THEN co.currently_breastfeeding_child
                   ELSE NULL END)                                                                   AS 'Breast Feeding Status at 24 Months',
           MAX(CASE WHEN co.interval_month = 25 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at 24 Months',
           MAX(CASE WHEN co.interval_month = 25 THEN co.treatment_end_date_visit ELSE NULL END)            AS 'Last TX_Curr Date at 24 Months EC.',
           MAX(CASE WHEN co.interval_month = 25 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at 24 Months',
           MAX(CASE WHEN co.interval_month = 25 THEN co.next_visit_date ELSE NULL END)               AS 'Next Visit date at 24 Months EC.'


    FROM PMTCT_ENROLLMENT ai

             LEFT JOIN
         CohortDetails co ON ai.PatientId = co.PatientId
    GROUP BY ai.PatientId,
             ai.date_of_enrollment_or_booking
    ORDER BY ai.date_of_enrollment_or_booking;

END //

DELIMITER ;
