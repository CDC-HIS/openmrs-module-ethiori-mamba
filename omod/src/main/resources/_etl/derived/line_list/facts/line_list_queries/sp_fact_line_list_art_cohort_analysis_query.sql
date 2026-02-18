DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_art_cohort_analysis_query;

CREATE PROCEDURE sp_fact_line_list_art_cohort_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUpEncounters AS (
        SELECT follow_up.encounter_id,
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
        WHERE follow_up_date_followup_ IS NOT NULL
          AND follow_up_status IS NOT NULL
    ),

         ART_Initiation AS (
             SELECT PatientId, MIN(art_start_date) AS art_start_date
             FROM FollowUpEncounters
             WHERE art_start_date IS NOT NULL
               AND art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
             GROUP BY PatientId
         ),

         IntervalsDef AS (
             SELECT 0 AS interval_month UNION ALL SELECT 6 UNION ALL SELECT 12 UNION ALL SELECT 24 UNION ALL SELECT 36
         ),

         PatientIntervals AS (
             SELECT a.PatientId,
                    a.art_start_date,
                    i.interval_month,
                    CASE
                        WHEN i.interval_month = 0 THEN a.art_start_date
                        ELSE fn_ethiopian_to_gregorian_calendar(DATE_ADD(fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL i.interval_month MONTH))
                        END AS interval_end_date,
                    CASE
                        WHEN i.interval_month = 0 THEN REPORT_START_DATE
                        ELSE COALESCE(LAG(fn_ethiopian_to_gregorian_calendar(DATE_ADD(fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL i.interval_month MONTH))) OVER (PARTITION BY a.PatientId ORDER BY i.interval_month), REPORT_START_DATE)
                        END AS interval_start_date
             FROM ART_Initiation a
                      CROSS JOIN IntervalsDef i
         ),

         StrictIntervalData AS (
             SELECT
                 pi.PatientId,
                 pi.interval_month,
                 pi.interval_start_date,
                 pi.interval_end_date,

                 f.follow_up_status AS strict_status,
                 f.follow_up_date,
                 f.treatment_end_date,
                 f.regimen,
                 f.ARTDoseDays,
                 f.AdherenceLevel,
                 f.pregnancy_status,
                 f.next_visit_date,
                 f.current_functional_status,

                 fn_get_ti_status(pi.PatientId, pi.interval_start_date, pi.interval_end_date) AS ti_status,

                 (SELECT viral_load_count FROM FollowUpEncounters vl WHERE vl.PatientId = pi.PatientId AND vl.viral_load_received_date BETWEEN pi.interval_start_date AND pi.interval_end_date ORDER BY vl.viral_load_received_date DESC LIMIT 1) as viral_load_count,
                 (SELECT viral_load_result FROM FollowUpEncounters vl WHERE vl.PatientId = pi.PatientId AND vl.viral_load_received_date BETWEEN pi.interval_start_date AND pi.interval_end_date ORDER BY vl.viral_load_received_date DESC LIMIT 1) as viral_load_result,
                 (SELECT viral_load_sent_date FROM FollowUpEncounters vl WHERE vl.PatientId = pi.PatientId AND vl.viral_load_received_date BETWEEN pi.interval_start_date AND pi.interval_end_date ORDER BY vl.viral_load_received_date DESC LIMIT 1) as viral_load_sent_date,
                 (SELECT viral_load_received_date FROM FollowUpEncounters vl WHERE vl.PatientId = pi.PatientId AND vl.viral_load_received_date BETWEEN pi.interval_start_date AND pi.interval_end_date ORDER BY vl.viral_load_received_date DESC LIMIT 1) as viral_load_received_date,

                 (SELECT cd4_count FROM FollowUpEncounters cd WHERE cd.PatientId = pi.PatientId AND cd.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date AND cd.cd4_count IS NOT NULL ORDER BY cd.follow_up_date DESC LIMIT 1) as cd4_count,
                 (SELECT cd4_percent FROM FollowUpEncounters cd WHERE cd.PatientId = pi.PatientId AND cd.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date AND cd.cd4_percent IS NOT NULL ORDER BY cd.follow_up_date DESC LIMIT 1) as cd4_percent,

                 (SELECT visitect_cd4_result FROM FollowUpEncounters vs WHERE vs.PatientId = pi.PatientId AND vs.visitect_cd4_test_date BETWEEN pi.interval_start_date AND pi.interval_end_date ORDER BY vs.visitect_cd4_test_date DESC LIMIT 1) as visitect_cd4_result,
                 (SELECT visitect_cd4_test_date FROM FollowUpEncounters vs WHERE vs.PatientId = pi.PatientId AND vs.visitect_cd4_test_date BETWEEN pi.interval_start_date AND pi.interval_end_date ORDER BY vs.visitect_cd4_test_date DESC LIMIT 1) as visitect_cd4_test_date

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

         CohortDetails AS (
             SELECT
                 sid.*,

                 MAX(CASE WHEN strict_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away') THEN strict_status ELSE NULL END)
                     OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_terminal_status,

                 CASE
                     WHEN MAX(CASE WHEN strict_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away') THEN 1 ELSE 0 END)
                              OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) = 1
                         THEN
                         MAX(CASE WHEN strict_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away') THEN strict_status ELSE NULL END)
                             OVER (PARTITION BY PatientId ORDER BY interval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

                     WHEN strict_status IS NOT NULL THEN strict_status

                     ELSE 'Loss to follow-up (LTFU)'
                     END AS final_cohort_outcome

             FROM StrictIntervalData sid
         )

    SELECT dc.patient_name                                                                  AS 'Patient Name',
           dc.patient_uuid                                                                  AS 'UUID',
           CAST(dc.mrn AS CHAR(20))                                                         AS 'MRN',
           dc.UAN,
           dc.Sex,
           dc.date_of_birth                                                                 AS 'Date of Birth',
           dc.date_of_birth                                                                 AS 'Date of Birth EC.',
           ai.art_start_date                                                                AS 'ART Start Date',
           ai.art_start_date                                                                AS 'ART Start Date EC.',
           YEAR(ai.art_start_date)                                                          AS 'ART Start Year',
           QUARTER(ai.art_start_date)                                                       AS 'ART Start Quarter',

           MAX(CASE WHEN co.interval_month = 0 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 0 Months',
           MAX(CASE
                   WHEN co.interval_month = 0 AND co.regimen IS NOT NULL THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' THEN 'On Alternate 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' THEN 'On Alternate 1st Line'
                           ELSE 'Other'
                           END
                   ELSE NULL END)                                                           AS 'Regimen Line at Zero Months',

           MAX(CASE WHEN co.interval_month = 0 THEN co.final_cohort_outcome ELSE NULL END)  AS 'Outcome at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_date ELSE NULL END)        AS 'Latest Follow-Up Date at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_date ELSE NULL END)        AS 'Latest Follow-Up Date at Zero Months EC.',
           MAX(CASE WHEN co.interval_month = 0 THEN co.regimen ELSE NULL END)               AS 'Latest Regimen at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.ARTDoseDays ELSE NULL END)           AS 'Latest Regimen Dose Days at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.strict_status ELSE NULL END)         AS 'Latest Follow-up Status at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.AdherenceLevel ELSE NULL END)        AS 'Latest Adherence at Zero Months',
           MAX(CASE WHEN co.interval_month = 0 THEN co.viral_load_count ELSE NULL END)      AS `Latest Viral Load Count at Zero Months`,

           -- === 6 MONTHS ===
           MAX(CASE WHEN co.interval_month = 6 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 6 Months',
           MAX(CASE
                   WHEN co.interval_month = 6 AND co.regimen IS NOT NULL THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' THEN 'On Alternate 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' THEN 'On Alternate 1st Line'
                           ELSE 'Other'
                           END
                   ELSE NULL END)                                                           AS 'Regimen Line at 6 Months',

           MAX(CASE WHEN co.interval_month = 6 THEN co.final_cohort_outcome ELSE NULL END)  AS 'Outcome at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_date ELSE NULL END)        AS 'Latest Follow-Up Date at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_date ELSE NULL END)        AS 'Latest Follow-Up Date at 6 Months EC.',
           MAX(CASE WHEN co.interval_month = 6 THEN co.regimen ELSE NULL END)               AS 'Latest Regimen at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.ARTDoseDays ELSE NULL END)           AS 'Latest Regimen Dose Days at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.strict_status ELSE NULL END)         AS 'Latest Follow-up Status at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.AdherenceLevel ELSE NULL END)        AS 'Latest Adherence at 6 Months',
           MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_count ELSE NULL END)      AS `Latest Viral Load Count at 6 Months`,

           -- === 12 MONTHS ===
           MAX(CASE WHEN co.interval_month = 12 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 12 Months',
           MAX(CASE
                   WHEN co.interval_month = 12 AND co.regimen IS NOT NULL THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' THEN 'On Alternate 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' THEN 'On Alternate 1st Line'
                           ELSE 'Other'
                           END
                   ELSE NULL END)                                                           AS 'Regimen Line at 12 Months',

           MAX(CASE WHEN co.interval_month = 12 THEN co.final_cohort_outcome ELSE NULL END) AS 'Outcome at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_date ELSE NULL END)       AS 'Latest Follow-Up Date at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_date ELSE NULL END)       AS 'Latest Follow-Up Date at 12 Months EC.',
           MAX(CASE WHEN co.interval_month = 12 THEN co.regimen ELSE NULL END)              AS 'Latest Regimen at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.ARTDoseDays ELSE NULL END)          AS 'Latest Regimen Dose Days at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.strict_status ELSE NULL END)        AS 'Latest Follow-up Status at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.AdherenceLevel ELSE NULL END)       AS 'Latest Adherence at 12 Months',
           MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_count ELSE NULL END)     AS `Latest Viral Load Count at 12 Months`,

           -- === 24 MONTHS ===
           MAX(CASE WHEN co.interval_month = 24 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 24 Months',
           MAX(CASE
                   WHEN co.interval_month = 24 AND co.regimen IS NOT NULL THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' THEN 'On Alternate 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' THEN 'On Alternate 1st Line'
                           ELSE 'Other'
                           END
                   ELSE NULL END)                                                           AS 'Regimen Line at 24 Months',

           MAX(CASE WHEN co.interval_month = 24 THEN co.final_cohort_outcome ELSE NULL END) AS 'Outcome at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_date ELSE NULL END)       AS 'Latest Follow-Up Date at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_date ELSE NULL END)       AS 'Latest Follow-Up Date at 24 Months EC.',
           MAX(CASE WHEN co.interval_month = 24 THEN co.regimen ELSE NULL END)              AS 'Latest Regimen at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.ARTDoseDays ELSE NULL END)          AS 'Latest Regimen Dose Days at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.strict_status ELSE NULL END)        AS 'Latest Follow-up Status at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.AdherenceLevel ELSE NULL END)       AS 'Latest Adherence at 24 Months',
           MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_count ELSE NULL END)     AS `Latest Viral Load Count at 24 Months`,

           -- === 36 MONTHS ===
           MAX(CASE WHEN co.interval_month = 36 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 36 Months',
           MAX(CASE
                   WHEN co.interval_month = 36 AND co.regimen IS NOT NULL THEN
                       CASE
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' THEN 'On Alternate 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' THEN 'On Original 1st Line'
                           WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' THEN 'On Alternate 1st Line'
                           ELSE 'Other'
                           END
                   ELSE NULL END)                                                           AS 'Regimen Line at 36 Months',

           MAX(CASE WHEN co.interval_month = 36 THEN co.final_cohort_outcome ELSE NULL END) AS 'Outcome at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_date ELSE NULL END)       AS 'Latest Follow-Up Date at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_date ELSE NULL END)       AS 'Latest Follow-Up Date at 36 Months EC.',
           MAX(CASE WHEN co.interval_month = 36 THEN co.regimen ELSE NULL END)              AS 'Latest Regimen at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.ARTDoseDays ELSE NULL END)          AS 'Latest Regimen Dose Days at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.strict_status ELSE NULL END)        AS 'Latest Follow-up Status at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.AdherenceLevel ELSE NULL END)       AS 'Latest Adherence at 36 Months',
           MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_count ELSE NULL END)     AS `Latest Viral Load Count at 36 Months`

    FROM ART_Initiation ai
             JOIN mamba_dim_client dc ON ai.PatientId = dc.client_id
             LEFT JOIN CohortDetails co ON ai.PatientId = co.PatientId
    WHERE ai.art_start_date <= COALESCE(REPORT_END_DATE, CURDATE())
    GROUP BY ai.PatientId, dc.patient_name, dc.patient_uuid, dc.mrn, dc.UAN, dc.Sex, dc.date_of_birth, ai.art_start_date
    ORDER BY ai.art_start_date, dc.patient_name;

END //

DELIMITER ;