DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_art_cohort_analysis_query;

CREATE PROCEDURE sp_fact_line_list_art_cohort_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUpEncounters AS (
        SELECT
            follow_up.encounter_id,
            follow_up.client_id AS PatientId,
            follow_up_status,
            follow_up_date_followup_ AS follow_up_date,
            art_antiretroviral_start_date AS art_start_date,
            treatment_end_date,
            regimen,
            antiretroviral_art_dispensed_dose_i AS ARTDoseDays,
            anitiretroviral_adherence_level AS AdherenceLevel,
            pregnancy_status,
            next_visit_date,
            date_of_reported_hiv_viral_load AS viral_load_sent_date,
            date_viral_load_results_received AS viral_load_received_date,
            viral_load_test_status AS viral_load_result,
            hiv_viral_load as viral_load_count,
            cd4_count,
            cd4_ as cd4_percent,
            current_functional_status,
            transferred_in_check_this_for_all_t
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
        WHERE follow_up.client_id IS NOT NULL AND follow_up_date_followup_ IS NOT NULL AND follow_up_status IS NOT NULL
    ),

    -- CTE to determine the initial ART start date for each patient.
    ART_Initiation AS (
        SELECT PatientId, MIN(art_start_date) AS art_start_date
        FROM FollowUpEncounters
        WHERE art_start_date IS NOT NULL
        and art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
        GROUP BY PatientId
    ),

    -- Defines the cohort analysis intervals in months.
         IntervalsDef AS (
             SELECT 0 AS interval_month
             UNION ALL
             SELECT 6
             UNION ALL
             SELECT 12
             UNION ALL
             SELECT 24
             UNION ALL
             SELECT 36
         ),

         -- Creates an evaluation point for each patient at each interval.
         PatientIntervals AS (
             SELECT
                 a.PatientId,
                 a.art_start_date,
                 i.interval_month,
                 DATE_ADD(REPORT_START_DATE, INTERVAL i.interval_month MONTH) as interval_end_date,
                 LAG(DATE_ADD(REPORT_START_DATE, INTERVAL i.interval_month MONTH), 1, REPORT_START_DATE) OVER (PARTITION BY a.PatientId ORDER BY i.interval_month) as interval_start_date
             FROM ART_Initiation a
                      CROSS JOIN IntervalsDef i
         ),

         -- Finds the latest follow-up for each patient within each interval to determine their status and general clinical details.
         LatestFollowUpInInterval AS (
             SELECT
                 pi.PatientId,
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
                 ROW_NUMBER() OVER(PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.follow_up_date DESC, f.encounter_id DESC) as rn
             FROM PatientIntervals pi
                      LEFT JOIN  FollowUpEncounters f ON pi.PatientId = f.PatientId WHERE f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date

         ),
         -- Finds the latest Viral Load Sent record for each patient within each interval, independent of the main follow-up.
         LatestViralLoadSentInInterval AS (
             SELECT
                 pi.PatientId,
                 pi.interval_month,
                 f.viral_load_sent_date,
                 ROW_NUMBER() OVER(PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.viral_load_sent_date DESC, f.encounter_id DESC) as rn_vl_sent
             FROM PatientIntervals pi
                      JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
             WHERE f.viral_load_sent_date <= pi.interval_end_date and f.viral_load_sent_date>=pi.art_start_date AND f.viral_load_sent_date IS NOT NULL
         ),
         -- Finds the latest Viral Load Performed record for each patient within each interval, independent of the main follow-up.
         LatestViralLoadPerformedInInterval AS (
             SELECT
                 pi.PatientId,
                 pi.interval_month,
                 f.viral_load_received_date,
                 f.viral_load_result,
                 ROW_NUMBER() OVER(PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.viral_load_received_date DESC, f.encounter_id DESC) as rn_vl_performed
             FROM PatientIntervals pi
                      JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
             WHERE f.viral_load_received_date <= pi.interval_end_date and f.viral_load_received_date>=pi.art_start_date AND f.viral_load_received_date IS NOT NULL
         ),

         -- Combines the latest follow-up details with the latest viral load details for each interval.
         CohortDetails AS (
             SELECT
                 lfu.PatientId,
                 lfu.interval_end_date,
                 lfu.interval_month,
                 CASE
                     WHEN lfu.follow_up_status IN ('Dead', 'Transferred out', 'Stop all', 'Loss to follow-up (LTFU)','Ran away') THEN lfu.follow_up_status
                     WHEN lfu.follow_up_status IN ('Alive', 'Restart medication') AND lfu.treatment_end_date >= lfu.interval_end_date THEN 'Active'
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
                 lfu.cd4_count,
                 lfu.cd4_percent,
                 lfu.current_functional_status
             FROM (SELECT * FROM LatestFollowUpInInterval WHERE rn = 1) lfu
                      LEFT JOIN (SELECT * FROM LatestViralLoadSentInInterval WHERE rn_vl_sent = 1) viral_load_sent
                                ON lfu.PatientId = viral_load_sent.PatientId AND lfu.interval_month = viral_load_sent.interval_month
                      LEFT JOIN (SELECT * FROM LatestViralLoadPerformedInInterval WHERE rn_vl_performed = 1) viral_load_performed
                                ON lfu.PatientId = viral_load_performed.PatientId AND lfu.interval_month = viral_load_performed.interval_month
         )
    -- Final SELECT statement to generate the line list, pivoting all details into columns for each interval.
    SELECT
        dc.patient_name AS 'Patient Name',
        dc.patient_uuid AS 'UUID',
        CAST(dc.mrn AS CHAR(20)) AS 'MRN',
        dc.UAN,
        dc.Sex,
        dc.date_of_birth AS 'Date of Birth',
        ai.art_start_date AS 'ART Start Date',
        YEAR(ai.art_start_date) AS 'ART Start Year',
        QUARTER(ai.art_start_date) AS 'ART Start Quarter',

        -- Pivot columns for each interval
        -- 0 Months
        MAX(CASE WHEN co.interval_month = 0 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 0 Months',
        MAX(CASE WHEN co.interval_month = 0 THEN
                     CASE
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '5%' AND outcome = 'Active' THEN 'On 2nd Line Regimen (Switched)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '2%' AND outcome = 'Active' THEN 'On 2nd Line Regimen (Switched)'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '6%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '3%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'

                         ELSE NULL
                         END
                 ELSE NULL END) AS 'Regimen Line at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.outcome ELSE NULL END) AS 'Outcome at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at Zero Months EC.',
        MAX(CASE WHEN co.interval_month = 0 THEN co.regimen ELSE NULL END) AS 'Latest Regimen at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.ARTDoseDays ELSE NULL END) AS 'Latest Regimen Dose Days at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.follow_up_status ELSE NULL END) AS 'Latest Follow-up Status at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.AdherenceLevel ELSE NULL END) AS 'Latest Adherence at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at Zero Months EC.',
        MAX(CASE WHEN co.interval_month = 0 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at Zero Months EC.',
        MAX(CASE WHEN co.interval_month = 0 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at Zero Months EC.',
        MAX(CASE WHEN co.interval_month = 0 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at Zero Months EC.',
        MAX(CASE WHEN co.interval_month = 0 THEN co.viral_load_count ELSE NULL END) AS `Latest Viral Load Count at Zero Months`,
        MAX(CASE WHEN co.interval_month = 0 THEN co.viral_load_result ELSE NULL END) AS 'Latest Viral Load Status at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.cd4_count ELSE NULL END) AS 'Latest CD4 Count at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.cd4_percent ELSE NULL END) AS 'Latest CD4 % at Zero Months',
        MAX(CASE WHEN co.interval_month = 0 THEN co.current_functional_status ELSE NULL END) AS 'Latest Function Status at Zero Months',

        -- 6 Months
        MAX(CASE WHEN co.interval_month = 6 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN
            CASE
                WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'
                WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'

                WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'
                WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'

                WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '5%' AND outcome = 'Active' THEN 'On 2nd Line Regimen (Switched)'
                WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '2%' AND outcome = 'Active' THEN 'On 2nd Line Regimen (Switched)'

                WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '6%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'
                WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '3%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'

                ELSE NULL
            END
        ELSE NULL END) AS 'Regimen Line at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.outcome ELSE NULL END) AS 'Outcome at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at 6 Months EC.',
        MAX(CASE WHEN co.interval_month = 6 THEN co.regimen ELSE NULL END) AS 'Latest Regimen at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.ARTDoseDays ELSE NULL END) AS 'Latest Regimen Dose Days at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_status ELSE NULL END) AS 'Latest Follow-up Status at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.AdherenceLevel ELSE NULL END) AS 'Latest Adherence at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 6 Months EC.',
        MAX(CASE WHEN co.interval_month = 6 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at 6 Months EC.',
        MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at 6 Months EC.',
        MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at 6 Months EC.',
        MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_count ELSE NULL END) AS `Latest Viral Load Count at 6 Months`,
        MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_result ELSE NULL END) AS 'Latest Viral Load Status at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.cd4_count ELSE NULL END) AS 'Latest CD4 Count at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.cd4_percent ELSE NULL END) AS 'Latest CD4 % at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.current_functional_status ELSE NULL END) AS 'Latest Function Status at 6 Months',

        -- 12 Months
        MAX(CASE WHEN co.interval_month = 12 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN
                     CASE
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '5%' AND outcome = 'Active' THEN 'On 2nd Line Regimen (Switched)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '2%' AND outcome = 'Active' THEN 'On 2nd Line Regimen (Switched)'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '6%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '3%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'

                         ELSE NULL
                         END
                 ELSE NULL END) AS 'Regimen Line at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.outcome ELSE NULL END) AS 'Outcome at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at 12 Months EC.',
        MAX(CASE WHEN co.interval_month = 12 THEN co.regimen ELSE NULL END) AS 'Latest Regimen at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.ARTDoseDays ELSE NULL END) AS 'Latest Regimen Dose Days at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_status ELSE NULL END) AS 'Latest Follow-up Status at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.AdherenceLevel ELSE NULL END) AS 'Latest Adherence at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 12 Months EC.',
        MAX(CASE WHEN co.interval_month = 12 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at 12 Months EC.',
        MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at 12 Months EC.',
        MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at 12 Months EC.',
        MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_count ELSE NULL END) AS `Latest Viral Load Count at 12 Months`,
        MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_result ELSE NULL END) AS 'Latest Viral Load Status at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.cd4_count ELSE NULL END) AS 'Latest CD4 Count at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.cd4_percent ELSE NULL END) AS 'Latest CD4 % at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.current_functional_status ELSE NULL END) AS 'Latest Function Status at 12 Months',

        -- 24 Months
        MAX(CASE WHEN co.interval_month = 24 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN
                     CASE
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '5%'AND outcome = 'Active'  THEN 'On 2nd Line Regimen (Switched)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '2%' AND outcome = 'Active' THEN 'On 2nd Line Regimen (Switched)'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '6%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '3%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'

                         ELSE NULL
                         END
                 ELSE NULL END) AS 'Regimen Line at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.outcome ELSE NULL END) AS 'Outcome at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at 24 Months EC.',
        MAX(CASE WHEN co.interval_month = 24 THEN co.regimen ELSE NULL END) AS 'Latest Regimen at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.ARTDoseDays ELSE NULL END) AS 'Latest Regimen Dose Days at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_status ELSE NULL END) AS 'Latest Follow-up Status at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.AdherenceLevel ELSE NULL END) AS 'Latest Adherence at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 24 Months EC.',
        MAX(CASE WHEN co.interval_month = 24 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at 24 Months EC.',
        MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at 24 Months EC.',
        MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at 24 Months EC.',
        MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_count ELSE NULL END) AS `Latest Viral Load Count at 24 Months`,
        MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_result ELSE NULL END) AS 'Latest Viral Load Status at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.cd4_count ELSE NULL END) AS 'Latest CD4 Count at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.cd4_percent ELSE NULL END) AS 'Latest CD4 % at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.current_functional_status ELSE NULL END) AS 'Latest Function Status at 24 Months',


        -- 36 Months
        MAX(CASE WHEN co.interval_month = 36 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN
                     CASE
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Original 1st Line Regimen'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '1%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '4%' AND outcome = 'Active' THEN 'On Alternate 1st Line Regimen (Substituted)'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '5%' AND outcome = 'Active' THEN 'On 2nd Line Regimen (Switched)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '2%' AND outcome = 'Active' THEN 'On 2nd Line Regimen (Switched)'

                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) < 15 AND co.regimen LIKE '6%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'
                         WHEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) >= 15 AND co.regimen LIKE '3%' AND outcome = 'Active' THEN 'On 3rd Line Regimen (Switched)'

                         ELSE NULL
                         END
                 ELSE NULL END) AS 'Regimen Line at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.outcome ELSE NULL END) AS 'Outcome at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_date ELSE NULL END) AS 'Latest Follow-Up Date at 36 Months EC.',
        MAX(CASE WHEN co.interval_month = 36 THEN co.regimen ELSE NULL END) AS 'Latest Regimen at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.ARTDoseDays ELSE NULL END) AS 'Latest Regimen Dose Days at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_status ELSE NULL END) AS 'Latest Follow-up Status at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.AdherenceLevel ELSE NULL END) AS 'Latest Adherence at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 36 Months EC.',
        MAX(CASE WHEN co.interval_month = 36 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit date at 36 Months EC.',
        MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_sent_date ELSE NULL END) AS 'Latest Viral Load Sent Date at 36 Months EC.',
        MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_received_date ELSE NULL END) AS 'Latest Viral Load Received Date at 36 Months EC.',
        MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_count ELSE NULL END) AS `Latest Viral Load Count at 36 Months`,
        MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_result ELSE NULL END) AS 'Latest Viral Load Status at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.cd4_count ELSE NULL END) AS 'Latest CD4 Count at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.cd4_percent ELSE NULL END) AS 'Latest CD4 % at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.current_functional_status ELSE NULL END) AS 'Latest Function Status at 36 Months'

    FROM
        ART_Initiation ai
    JOIN
        mamba_dim_client dc ON ai.PatientId = dc.client_id
    LEFT JOIN
        CohortDetails co ON ai.PatientId = co.PatientId
    WHERE ai.art_start_date <= COALESCE(REPORT_END_DATE, CURDATE())
    GROUP BY
        ai.PatientId, dc.patient_name, dc.patient_uuid, dc.mrn, dc.UAN, dc.Sex, dc.date_of_birth, ai.art_start_date
    ORDER BY
        ai.art_start_date, dc.patient_name;

END //

DELIMITER ;