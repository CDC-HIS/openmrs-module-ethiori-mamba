DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_mother_cohort_analysis_summary_query;

CREATE PROCEDURE sp_fact_line_list_mother_cohort_analysis_summary_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE
    DATE)
BEGIN

    WITH FollowUpEncounters AS (SELECT follow_up.encounter_id,
                                       follow_up.client_id                 AS PatientId,
                                       follow_up_status,
                                       follow_up.encounter_id              as follow_up_encounter_id,
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
                                       hiv_viral_load                      as viral_load_count,
                                       cd4_count,
                                       cd4_                                as cd4_percent,
                                       current_functional_status
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


         PMTCT_INITIAL AS (SELECT client_id as PatientId, MIN(date_of_enrollment_or_booking) AS enrollment_date
                           FROM mamba_flat_encounter_pmtct_enrollment
                           WHERE date_of_enrollment_or_booking IS NOT NULL
                             and date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                           GROUP BY PatientId),

         -- Defines the cohort analysis intervals in months.
         IntervalsDef AS (SELECT 0 AS interval_month
                          UNION ALL
                          SELECT 3 as interval_month
                          UNION ALL
                          SELECT 6 as interval_month
                          UNION ALL
                          SELECT 12 interval_month
                          UNION ALL
                          SELECT 24 interval_month),

         -- Creates an evaluation point for each patient at each interval.
         PatientIntervals AS (SELECT a.PatientId,
                                     a.enrollment_date,
                                     i.interval_month,
                                     fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                             fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL
                                             i.interval_month MONTH))                              as interval_end_date,
                                     LAG(fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                             fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL
                                             i.interval_month MONTH)), 1,
                                         REPORT_START_DATE)
                                         OVER (PARTITION BY a.PatientId ORDER BY i.interval_month) as interval_start_date
                              FROM PMTCT_INITIAL a
                                       CROSS JOIN IntervalsDef i),


         -- Finds the latest follow-up for each patient within each interval to determine their status and general clinical details.
         LatestFollowUpInInterval AS (SELECT pi.PatientId,
                                             pi.interval_month,
                                             pi.interval_start_date,
                                             pi.interval_end_date,
                                             pi.enrollment_date,
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
                                             fn_get_ti_status(pi.PatientId, f.art_start_date,
                                                              pi.interval_end_date)                                                                               as ti_status,
                                             ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month
                                                 ORDER BY f.follow_up_date DESC, f.encounter_id DESC) as rn
                                      FROM PatientIntervals pi
                                               LEFT JOIN FollowUpEncounters f
                                                   ON pi.PatientId = f.PatientId AND
                                                   f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date),


         -- Combines the latest follow-up details with the interval month.
         CohortDetails AS (SELECT lfu.PatientId,
                                  lfu.interval_end_date,
                                  lfu.interval_month,
                                  lfu.ti_status,
                                  CASE
                                      WHEN lfu.follow_up_status IN
                                           ('Dead', 'Transferred out', 'Stop all', 'Loss to follow-up (LTFU)',
                                            'Ran away')
                                          THEN lfu.follow_up_status
                                      WHEN lfu.follow_up_status IN ('Alive', 'Restart medication') AND
                                           lfu.treatment_end_date >= lfu.interval_end_date THEN 'Active'
                                      ELSE 'Loss to follow-up (LTFU)'
                                      END AS outcome,
                                  lfu.follow_up_date,
                                  lfu.regimen,
                                  lfu.viral_load_count,
                                  lfu.ARTDoseDays,
                                  lfu.follow_up_status,
                                  lfu.enrollment_date,
                                  lfu.AdherenceLevel,
                                  lfu.pregnancy_status,
                                  lfu.treatment_end_date,
                                  lfu.next_visit_date,
                                  lfu.current_functional_status,
                                  client.date_of_birth
                           FROM (SELECT * FROM LatestFollowUpInInterval WHERE rn = 1) lfu
                                    join mamba_dim_client client
                                         on lfu.PatientId = client.client_id and client.sex = 'FEMALE')

    select 'A. enrolled in PMTCT in this facility during this month and year (Month 0)'   as
                                                                                             Name,
           CAST(IFNULL(SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 0',
           ''                                                                             AS 'Maternal Cohort Month 3',
           ''                                                                             AS 'Maternal Cohort Month 6',
           ''                                                                             AS 'Maternal Cohort Month 12',
           ''                                                                             AS 'Maternal Cohort Month 24'
    from CohortDetails
    UNION ALL
    select 'B. Total number of Transfer in (TI) since Month 0' as Name,
           ''                                                  AS 'Maternal Cohort Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 3 and ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                           AS 'Maternal Cohort Month 3',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 and ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                           AS 'Maternal Cohort Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 and ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                           AS 'Maternal Cohort Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 and ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                           AS 'Maternal Cohort Month 24'
    from CohortDetails
    UNION ALL

    select 'C. Total Number of Transfer out (TO) since Month 0' as Name,
           ''                                                   AS 'Maternal Cohort Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 3 and outcome = 'Transferred out' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                            AS 'Maternal Cohort Month 3',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 and outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED)       AS 'Maternal Cohort Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 and outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED)       AS 'Maternal Cohort Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 and outcome = 'Transferred out' THEN 1
                               ELSE 0 END), 0) AS SIGNED)       AS 'Maternal Cohort Month 24'
    from CohortDetails

    UNION ALL

    select 'D. Number of mothers  in the current cohort = Net Current Cohort (A+B-C' as Name,
           ''                                                                        AS 'Maternal Cohort Month 0',
           CAST(IFNULL(((SUM(CASE WHEN interval_month = 0 AND enrollment_date is not null THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN interval_month = 0 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                        SUM(CASE
                                WHEN interval_month = 0 AND outcome = 'Transferred out' THEN 1
                                ELSE 0 END)), 0) AS SIGNED)                          AS 'Maternal Cohort Month 3',

           CAST(IFNULL(((SUM(CASE WHEN interval_month = 0 AND follow_up_date is not null THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN interval_month = 6 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                        SUM(CASE
                                WHEN interval_month = 6 AND outcome = 'Transferred out' THEN 1
                                ELSE 0 END)), 0) AS SIGNED)                          AS 'Maternal Cohort Month 6',

           CAST(IFNULL(((SUM(CASE WHEN interval_month = 0 AND follow_up_date is not null THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN interval_month = 12 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                        SUM(CASE
                                WHEN interval_month = 12 AND outcome = 'Transferred out' THEN 1
                                ELSE 0 END)), 0) AS SIGNED)                          AS 'Maternal Cohort Month 12',

           CAST(IFNULL(((SUM(CASE WHEN interval_month = 0 AND follow_up_date is not null THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN interval_month = 24 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                        SUM(CASE
                                WHEN interval_month <= 24 AND outcome = 'Transferred out' THEN 1
                                ELSE 0 END)), 0) AS SIGNED)                          AS 'Maternal Cohort Month 24'

    from CohortDetails

    UNION ALL

    select 'E. Mothers Alive and on ART'                  as Name,
           ''                                             AS 'Maternal Cohort Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 3 and outcome = 'Active' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                      AS 'Maternal Cohort Month 3',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 and outcome = 'Active' THEN 1
                               ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 24'
    from CohortDetails

    UNION ALL
    select 'F. Lost to F/U (not seen>1 month after scheduled appointment)' as Name,
           ''                                                              AS 'Maternal Cohort Month 0',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 3 and
                                    (follow_up_status = 'Ran away' or
                                     follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED)                  AS 'Maternal Cohort Month 3',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 6 and
                                    (follow_up_status = 'Ran away' or
                                     follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED)                  AS 'Month 6',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 12 and
                                    (follow_up_status = 'Ran away' or
                                     follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED)                  AS 'Month 12',
           CAST(IFNULL(SUM(CASE
                               WHEN interval_month = 24 and
                                    (follow_up_status = 'Ran away' or
                                     follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                               ELSE 0 END), 0) AS SIGNED)                  AS 'Month 24'
    from CohortDetails
    UNION ALL
    select 'G. Known Dead'           as Name,
           ''                        AS 'Maternal Cohort Month 0',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 3 and follow_up_status = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Maternal Cohort Month 3',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 6 and follow_up_status = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS 'Maternal Cohort Month 6',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 12 and follow_up_status = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS
                                        'Maternal Cohort Month 12',
           CAST(IFNULL(SUM(CASE WHEN interval_month = 24 and follow_up_status = 'Dead' THEN 1 ELSE 0 END),
                       0) AS SIGNED) AS
                                        'Maternal Cohort Month 24'
    from CohortDetails
    UNION ALL
    select 'H. % of mothers in net current cohort Alive and on ART [(E/D)x100%]' as Name,
           ''                                                                    AS 'Maternal Cohort Month 0',
          concat( ROUND((IFNULL(SUM(CASE WHEN interval_month = 3 and outcome = 'Active' THEN 1 ELSE 0 END) /
                         IFNULL(((SUM(CASE WHEN interval_month = 3 AND enrollment_date is not null THEN 1 ELSE 0 END) +
                                  SUM(CASE WHEN interval_month = 3 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                 SUM(CASE
                                         WHEN interval_month = 3 AND outcome = 'Transferred out' THEN 1
                                         ELSE 0 END)), 1)
               , 0)) / 100, 1),' %')                                                   AS 'Maternal Cohort Month 3',

           CONCAT(ROUND((IFNULL(SUM(CASE WHEN interval_month = 6 and outcome = 'Active' THEN 1 ELSE 0 END) /
                         IFNULL(((SUM(CASE WHEN interval_month = 6 AND enrollment_date is not null THEN 1 ELSE 0 END) +
                                  SUM(CASE WHEN interval_month = 6 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                 SUM(CASE
                                         WHEN interval_month = 6 AND outcome = 'Transferred out' THEN 1
                                         ELSE 0 END)), 1)
               , 0)) / 100, 1),' %')
                                                                                 AS 'Maternal Cohort Month 6',

           CONCAT(ROUND((IFNULL(SUM(CASE WHEN interval_month = 12 and outcome = 'Active' THEN 1 ELSE 0 END) /
                         IFNULL(((SUM(CASE WHEN interval_month = 12 AND enrollment_date is not null THEN 1 ELSE 0 END) +
                                  SUM(CASE WHEN interval_month = 12 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                 SUM(CASE
                                         WHEN interval_month = 12 AND outcome = 'Transferred out' THEN 1
                                         ELSE 0 END)), 1)
               , 0)) / 100, 1),' %')
                                                                                 AS 'Maternal Cohort Month 12',

          CONCAT( ROUND((IFNULL(SUM(CASE WHEN interval_month = 24 and outcome = 'Active' THEN 1 ELSE 0 END) /
                         IFNULL(((SUM(CASE WHEN interval_month = 24 AND enrollment_date is not null THEN 1 ELSE 0 END) +
                                  SUM(CASE WHEN interval_month = 24 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                 SUM(CASE
                                         WHEN interval_month = 24 AND outcome = 'Transferred out' THEN 1
                                         ELSE 0 END)), 1)
               , 0)) / 100, 1),' %' )                                                  AS 'Maternal Cohort Month 24'
    from CohortDetails
    UNION ALL
    select 'I. % of mothers in net current cohort Lost to F/U [(F/D)x100%]' as Name,
           ''                                                               AS 'Maternal Cohort Month 0',
           CONCAT(ROUND((IFNULL(SUM(CASE
                                 WHEN interval_month = 3 and (follow_up_status = 'Ran away' or
                                                              follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                                 ELSE 0 END) /
                         IFNULL(((SUM(CASE WHEN interval_month = 3 AND enrollment_date is not null THEN 1 ELSE 0 END) +
                                  SUM(CASE WHEN interval_month = 3 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                 SUM(CASE
                                         WHEN interval_month = 3 AND outcome = 'Transferred out' THEN 1
                                         ELSE 0 END)), 1)
               , 0)) / 100, 1), ' %')                                              AS 'Maternal Cohort Month 3',

          CONCAT( ROUND((IFNULL(SUM(CASE
                                 WHEN interval_month = 6 and (follow_up_status = 'Ran away' or
                                                              follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                                 ELSE 0 END) /
                         IFNULL(((SUM(CASE WHEN interval_month = 6 AND enrollment_date is not null THEN 1 ELSE 0 END) +
                                  SUM(CASE WHEN interval_month = 6 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                 SUM(CASE
                                         WHEN interval_month = 6 AND outcome = 'Transferred out' THEN 1
                                         ELSE 0 END)), 1)
               , 0)) / 100, 1),' %')                                           AS 'Maternal Cohort Month 6',

           CONCAT(ROUND((IFNULL(SUM(CASE
                                 WHEN interval_month = 12 and (follow_up_status = 'Ran away' or
                                                               follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                                 ELSE 0 END) /
                         IFNULL(((SUM(CASE WHEN interval_month = 12 AND enrollment_date is not null THEN 1 ELSE 0 END) +
                                  SUM(CASE WHEN interval_month = 12 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                 SUM(CASE
                                         WHEN interval_month = 12 AND outcome = 'Transferred out' THEN 1
                                         ELSE 0 END)), 1)
               , 0)) / 100, 1),' %')
                                                                            AS 'Maternal Cohort Month 12',

           CONCAT(ROUND((IFNULL(SUM(CASE
                                 WHEN interval_month = 24 and (follow_up_status = 'Ran away' or
                                                               follow_up_status = 'Loss to follow-up (LTFU)') THEN 1
                                 ELSE 0 END) /
                         IFNULL(((SUM(CASE WHEN interval_month = 24 AND enrollment_date is not null THEN 1 ELSE 0 END) +
                                  SUM(CASE WHEN interval_month = 24 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
                                 SUM(CASE
                                         WHEN interval_month = 24 AND outcome = 'Transferred out' THEN 1
                                         ELSE 0 END)), 1)
               , 0)) / 100, 1), ' %')                                              AS 'Maternal Cohort Month 24'
    from CohortDetails;

END //
DELIMITER ;




