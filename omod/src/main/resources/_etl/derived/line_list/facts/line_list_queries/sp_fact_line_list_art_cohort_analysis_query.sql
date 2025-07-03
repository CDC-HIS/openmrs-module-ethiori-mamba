DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_art_cohort_analysis_query;

CREATE PROCEDURE sp_fact_line_list_art_cohort_analysis_query(IN REPORT_END_DATE DATE)
BEGIN

    -- This procedure generates a line list for WHO ART Cohort Analysis.
    -- It tracks outcomes and clinical details of patients at standard intervals after ART initiation.

    -- CTE to get all follow-up encounters from the flat tables, including all required clinical details.
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
            cd4_count
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
        GROUP BY PatientId
    ),

    -- Defines the cohort analysis intervals in months.
    IntervalsDef AS (
        SELECT 6 AS interval_month UNION ALL SELECT 12 UNION ALL SELECT 24 UNION ALL SELECT 36 UNION ALL SELECT 48 UNION ALL SELECT 60
    ),

    -- Creates an evaluation point for each patient at each interval.
    PatientIntervals AS (
        SELECT
            a.PatientId,
            a.art_start_date,
            i.interval_month,
            DATE_ADD(a.art_start_date, INTERVAL i.interval_month MONTH) as interval_end_date
        FROM ART_Initiation a
        CROSS JOIN IntervalsDef i
        WHERE DATE_ADD(a.art_start_date, INTERVAL i.interval_month MONTH) <= COALESCE(REPORT_END_DATE, CURDATE())
    ),

    -- Finds the latest follow-up for each patient within each interval to determine their status and general clinical details.
    LatestFollowUpInInterval AS (
        SELECT
            pi.PatientId,
            pi.interval_month,
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
            ROW_NUMBER() OVER(PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.follow_up_date DESC, f.encounter_id DESC) as rn
        FROM PatientIntervals pi
        JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
        WHERE f.follow_up_date <= pi.interval_end_date
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
        WHERE f.follow_up_date <= pi.interval_end_date AND f.viral_load_sent_date IS NOT NULL
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
        WHERE f.follow_up_date <= pi.interval_end_date AND f.viral_load_received_date IS NOT NULL
    ),

    -- Combines the latest follow-up details with the latest viral load details for each interval.
    CohortDetails AS (
        SELECT
            lfu.PatientId,
            lfu.interval_end_date,
            lfu.interval_month,
            CASE
                WHEN lfu.follow_up_status IN ('Died', 'Transferred out', 'Stopped medication', 'Lost to follow-up') THEN lfu.follow_up_status
                WHEN lfu.follow_up_status IN ('Alive', 'Restart medication') AND lfu.treatment_end_date >= lfu.interval_end_date THEN 'Active'
                ELSE 'Lost to follow-up'
            END AS outcome,
            lfu.follow_up_date,
            lfu.regimen,
            CASE LEFT(lfu.regimen)
                WHEN '1' THEN
                    'ADULT FIRST LINE'
                WHEN '2' THEN
                    'ADULT SECOND LINE'
                WHEN '3' THEN
                    'ADULT THIRD LINE'
                WHEN '4' THEN
                    'CHILD FIRST LINE'
                WHEN '5' THEN
                    'CHILD SECOND LINE'
                WHEN '6' THEN
                    'CHILD THIRD LINE'
                END AS Line,
            lfu.ARTDoseDays,
            lfu.follow_up_status,
            lfu.AdherenceLevel,
            lfu.pregnancy_status,
            lfu.treatment_end_date,
            lfu.next_visit_date,
            viral_load_sent.viral_load_sent_date,
            viral_load_performed.viral_load_received_date,
            viral_load_performed.viral_load_result,
            lfu.cd4_count
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
        MAX(CASE WHEN co.interval_month = 6 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.outcome ELSE NULL END) AS 'Outcome at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_date ELSE NULL END) AS 'Follow-Up Date at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.regimen ELSE NULL END) AS 'Regimen at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.ARTDoseDays ELSE NULL END) AS 'Dose Days at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.follow_up_status ELSE NULL END) AS 'Follow-up Status at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.AdherenceLevel ELSE NULL END) AS 'Adherence at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit Date at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_sent_date ELSE NULL END) AS 'Viral Load Sent at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_received_date ELSE NULL END) AS 'Viral Load Received at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.viral_load_result ELSE NULL END) AS 'Viral Load Result at 6 Months',
        MAX(CASE WHEN co.interval_month = 6 THEN co.cd4_count ELSE NULL END) AS 'CD4 Count at 6 Months',

        MAX(CASE WHEN co.interval_month = 12 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.outcome ELSE NULL END) AS 'Outcome at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_date ELSE NULL END) AS 'Follow-Up Date at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.regimen ELSE NULL END) AS 'Regimen at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.ARTDoseDays ELSE NULL END) AS 'Dose Days at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.follow_up_status ELSE NULL END) AS 'Follow-up Status at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.AdherenceLevel ELSE NULL END) AS 'Adherence at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit Date at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_sent_date ELSE NULL END) AS 'Viral Load Sent at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_received_date ELSE NULL END) AS 'Viral Load Received at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.viral_load_result ELSE NULL END) AS 'Viral Load Result at 12 Months',
        MAX(CASE WHEN co.interval_month = 12 THEN co.cd4_count ELSE NULL END) AS 'CD4 Count at 12 Months',

        MAX(CASE WHEN co.interval_month = 24 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.outcome ELSE NULL END) AS 'Outcome at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_date ELSE NULL END) AS 'Follow-Up Date at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.regimen ELSE NULL END) AS 'Regimen at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.ARTDoseDays ELSE NULL END) AS 'Dose Days at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.follow_up_status ELSE NULL END) AS 'Follow-up Status at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.AdherenceLevel ELSE NULL END) AS 'Adherence at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit Date at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_sent_date ELSE NULL END) AS 'Viral Load Sent at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_received_date ELSE NULL END) AS 'Viral Load Received at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.viral_load_result ELSE NULL END) AS 'Viral Load Result at 24 Months',
        MAX(CASE WHEN co.interval_month = 24 THEN co.cd4_count ELSE NULL END) AS 'CD4 Count at 24 Months',

        MAX(CASE WHEN co.interval_month = 36 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.outcome ELSE NULL END) AS 'Outcome at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_date ELSE NULL END) AS 'Follow-Up Date at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.regimen ELSE NULL END) AS 'Regimen at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.ARTDoseDays ELSE NULL END) AS 'Dose Days at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.follow_up_status ELSE NULL END) AS 'Follow-up Status at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.AdherenceLevel ELSE NULL END) AS 'Adherence at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit Date at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_sent_date ELSE NULL END) AS 'Viral Load Sent at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_received_date ELSE NULL END) AS 'Viral Load Received at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.viral_load_result ELSE NULL END) AS 'Viral Load Result at 36 Months',
        MAX(CASE WHEN co.interval_month = 36 THEN co.cd4_count ELSE NULL END) AS 'CD4 Count at 36 Months',

        MAX(CASE WHEN co.interval_month = 48 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.outcome ELSE NULL END) AS 'Outcome at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.follow_up_date ELSE NULL END) AS 'Follow-Up Date at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.regimen ELSE NULL END) AS 'Regimen at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.ARTDoseDays ELSE NULL END) AS 'Dose Days at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.follow_up_status ELSE NULL END) AS 'Follow-up Status at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.AdherenceLevel ELSE NULL END) AS 'Adherence at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit Date at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.viral_load_sent_date ELSE NULL END) AS 'Viral Load Sent at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.viral_load_received_date ELSE NULL END) AS 'Viral Load Received at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.viral_load_result ELSE NULL END) AS 'Viral Load Result at 48 Months',
        MAX(CASE WHEN co.interval_month = 48 THEN co.cd4_count ELSE NULL END) AS 'CD4 Count at 48 Months',

        MAX(CASE WHEN co.interval_month = 60 THEN TIMESTAMPDIFF(YEAR, dc.date_of_birth, co.interval_end_date) ELSE NULL END) AS 'Age at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.outcome ELSE NULL END) AS 'Outcome at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.follow_up_date ELSE NULL END) AS 'Follow-Up Date at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.regimen ELSE NULL END) AS 'Regimen at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.ARTDoseDays ELSE NULL END) AS 'Dose Days at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.follow_up_status ELSE NULL END) AS 'Follow-up Status at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.AdherenceLevel ELSE NULL END) AS 'Adherence at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.pregnancy_status ELSE NULL END) AS 'Pregnant at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.treatment_end_date ELSE NULL END) AS 'Treatment End Date at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.next_visit_date ELSE NULL END) AS 'Next Visit Date at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.viral_load_sent_date ELSE NULL END) AS 'Viral Load Sent at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.viral_load_received_date ELSE NULL END) AS 'Viral Load Received at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.viral_load_result ELSE NULL END) AS 'Viral Load Result at 60 Months',
        MAX(CASE WHEN co.interval_month = 60 THEN co.cd4_count ELSE NULL END) AS 'CD4 Count at 60 Months'

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