call sp_fact_line_list_art_cohort_analysis_query('2022-08-27', '2022-09-30');
select * from mamba_dim_client where patient_uuid='d7d6f997b4ee4e8c99becdd50fe5bbf0';
WITH FollowUpEncounters AS (SELECT follow_up.encounter_id,
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

     -- CTE to determine the initial ART start date for each patient.
     ART_Initiation AS (SELECT PatientId, MIN(art_start_date) AS art_start_date
                        FROM FollowUpEncounters
                        WHERE art_start_date IS NOT NULL
                          and art_start_date BETWEEN '2022-08-27' AND '2022-09-30'
                        GROUP BY PatientId),

     -- Defines the cohort analysis intervals in months.
     IntervalsDef AS (SELECT 0 AS interval_month
                      UNION ALL
                      SELECT 6
                      UNION ALL
                      SELECT 12
                      UNION ALL
                      SELECT 24
                      UNION ALL
                      SELECT 36),

     -- Creates an evaluation point for each patient at each interval.
     PatientIntervals AS (SELECT a.PatientId,
                                 a.art_start_date,
                                 i.interval_month,
                                 DATE_ADD(a.art_start_date, INTERVAL i.interval_month MONTH)   as interval_end_date,
                                 LAG(DATE_ADD(a.art_start_date, INTERVAL i.interval_month MONTH), 1, a.art_start_date)
                                     OVER (PARTITION BY a.PatientId ORDER BY i.interval_month) as interval_start_date
                          FROM ART_Initiation a
                                   CROSS JOIN IntervalsDef i),

     -- Finds the latest follow-up for each patient within each interval to determine their status and general clinical details.
     LatestFollowUpInInterval AS (SELECT pi.PatientId,
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
                                         fn_get_ti_status(pi.PatientId, f.art_start_date,
                                                          pi.interval_end_date)                                                                               as ti_status,
                                         ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.follow_up_date DESC, f.encounter_id DESC) as rn
                                  FROM PatientIntervals pi
                                           LEFT JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId AND
                                                                             f.follow_up_date BETWEEN pi.interval_start_date AND pi.interval_end_date),
     -- Finds the latest Viral Load Sent record for each patient within each interval, independent of the main follow-up.
     LatestViralLoadSentInInterval AS (SELECT pi.PatientId,
                                              pi.interval_month,
                                              f.viral_load_sent_date,
                                              ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.viral_load_sent_date DESC, f.encounter_id DESC) as rn_vl_sent
                                       FROM PatientIntervals pi
                                                JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
                                       WHERE f.follow_up_date <= pi.interval_end_date
                                         and f.follow_up_date >= pi.art_start_date
                                         AND f.viral_load_sent_date IS NOT NULL),
     -- Finds the latest Viral Load Performed record for each patient within each interval, independent of the main follow-up.
     LatestViralLoadPerformedInInterval AS (SELECT pi.PatientId,
                                                   pi.interval_month,
                                                   f.viral_load_received_date,
                                                   f.viral_load_result,
                                                   ROW_NUMBER() OVER (PARTITION BY pi.PatientId, pi.interval_month ORDER BY f.viral_load_received_date DESC, f.encounter_id DESC) as rn_vl_performed
                                            FROM PatientIntervals pi
                                                     JOIN FollowUpEncounters f ON pi.PatientId = f.PatientId
                                            WHERE f.follow_up_date <= pi.interval_end_date
                                              and f.follow_up_date >= pi.art_start_date
                                              AND f.viral_load_received_date IS NOT NULL),

     -- Combines the latest follow-up details with the latest viral load details for each interval.
     CohortDetails AS (SELECT lfu.PatientId,
                              lfu.interval_end_date,
                              lfu.interval_month,
                              lfu.ti_status,
                              CASE
                                  WHEN lfu.follow_up_status IN
                                       ('Dead', 'Transferred out', 'Stop all', 'Loss to follow-up (LTFU)', 'Ran away')
                                      THEN lfu.follow_up_status
                                  WHEN lfu.follow_up_status IN ('Alive', 'Restart medication') AND
                                       lfu.treatment_end_date >= lfu.interval_end_date THEN 'Active'
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
                              lfu.current_functional_status,
                              client.date_of_birth
                       FROM (SELECT * FROM LatestFollowUpInInterval WHERE rn = 1) lfu
                                LEFT JOIN (SELECT * FROM LatestViralLoadSentInInterval WHERE rn_vl_sent = 1) viral_load_sent
                                          ON lfu.PatientId = viral_load_sent.PatientId AND
                                             lfu.interval_month = viral_load_sent.interval_month
                                LEFT JOIN (SELECT *
                                           FROM LatestViralLoadPerformedInInterval
                                           WHERE rn_vl_performed = 1) viral_load_performed
                                          ON lfu.PatientId = viral_load_performed.PatientId AND
                                             lfu.interval_month = viral_load_performed.interval_month
                                join mamba_dim_client client on lfu.PatientId = client.client_id)
select 'Started on ART in this clinic: original cohort'     as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails where interval_month = 0 and follow_up_date is not null
UNION ALL
select 'Transfer in Add+'                                   as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END) AS 'Month 36'

from CohortDetails
where ti_status = 'TI'
UNION ALL
select 'Transfers Out Subtract -'                            as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)   AS 'Month 0',
       SUM(CASE WHEN interval_month <= 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month <= 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month <= 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month <= 36 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
where outcome = 'Transferred out'
UNION ALL
select 'Net current cohort'                                                                   as Name,
       ((SUM(CASE WHEN interval_month = 0 AND follow_up_date is not null THEN 1 ELSE 0 END) +
         SUM(CASE WHEN interval_month = 0 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
        SUM(CASE WHEN interval_month = 0 AND outcome = 'Transferred out' THEN 1 ELSE 0 END))  AS 'Month 0',

       ((SUM(CASE WHEN interval_month = 0 AND follow_up_date is not null THEN 1 ELSE 0 END) +
         SUM(CASE WHEN interval_month = 6 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
        SUM(CASE WHEN interval_month <= 6 AND outcome = 'Transferred out' THEN 1 ELSE 0 END))  AS 'Month 6',

       ((SUM(CASE WHEN interval_month = 0 AND follow_up_date is not null THEN 1 ELSE 0 END) +
         SUM(CASE WHEN interval_month = 12 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
        SUM(CASE WHEN interval_month <= 12 AND outcome = 'Transferred out' THEN 1 ELSE 0 END)) AS 'Month 12',

       ((SUM(CASE WHEN interval_month = 0 AND follow_up_date is not null THEN 1 ELSE 0 END) +
         SUM(CASE WHEN interval_month = 24 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
        SUM(CASE WHEN interval_month <= 24 AND outcome = 'Transferred out' THEN 1 ELSE 0 END)) AS 'Month 24',

       ((SUM(CASE WHEN interval_month = 0 AND follow_up_date is not null THEN 1 ELSE 0 END) +
         SUM(CASE WHEN interval_month = 36 AND ti_status = 'TI' THEN 1 ELSE 0 END)) -
        SUM(CASE WHEN interval_month <= 36 AND outcome = 'Transferred out' THEN 1 ELSE 0 END)) AS 'Month 36'
from CohortDetails
UNION ALL
select 'On Original 1st Line Regimen' as Name,
       SUM(CASE
               WHEN interval_month = 0 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%')
                   THEN 1
               ELSE 0 END
       )                              AS 'Month 0',
       SUM(CASE
               WHEN interval_month = 6 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%')
                   THEN 1
               ELSE 0 END
       )                              AS 'Month 6',
       SUM(CASE
               WHEN interval_month = 12 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%')
                   THEN 1
               ELSE 0 END
       )                              AS 'Month 12',
       SUM(CASE
               WHEN interval_month = 24 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%')
                   THEN 1
               ELSE 0 END
       )                              AS 'Month 24',
       SUM(CASE
               WHEN interval_month = 36 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '4%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '1%')
                   THEN 1
               ELSE 0 END
       )                              AS 'Month 36'
from CohortDetails
UNION ALL
select 'On Alternate 1st Line Regimen (Substituted)' as Name,
       SUM(CASE
               WHEN interval_month = 0 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%')
                   THEN 1
               ELSE 0 END
       )                                             AS 'Month 0',
       SUM(CASE
               WHEN interval_month = 6 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%')
                   THEN 1
               ELSE 0 END
       )                                             AS 'Month 6',
       SUM(CASE
               WHEN interval_month = 12 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%')
                   THEN 1
               ELSE 0 END
       )                                             AS 'Month 12',
       SUM(CASE
               WHEN interval_month = 24 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%')
                   THEN 1
               ELSE 0 END
       )                                             AS 'Month 24',
       SUM(CASE
               WHEN interval_month = 36 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '1%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '4%')
                   THEN 1
               ELSE 0 END
       )                                             AS 'Month 36'
from CohortDetails
UNION ALL
select 'On 2nd Line Regimen (Switched)' as Name,
       SUM(CASE
               WHEN interval_month = 0 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 0',
       SUM(CASE
               WHEN interval_month = 6 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 6',
       SUM(CASE
               WHEN interval_month = 12 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 12',
       SUM(CASE
               WHEN interval_month = 24 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 24',
       SUM(CASE
               WHEN interval_month = 36 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '5%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '2%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 36'
from CohortDetails
UNION ALL
select 'On 3rd Line Regimen (Switched)' as Name,
       SUM(CASE
               WHEN interval_month = 0 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 0',
       SUM(CASE
               WHEN interval_month = 6 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 6',
       SUM(CASE
               WHEN interval_month = 12 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 12',
       SUM(CASE
               WHEN interval_month = 24 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 24',
       SUM(CASE
               WHEN interval_month = 36 AND
                    (TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) < 15 AND regimen LIKE '6%'
                        OR TIMESTAMPDIFF(YEAR, date_of_birth, interval_end_date) >= 15 AND regimen LIKE '3%')
                   THEN 1
               ELSE 0 END
       )                                AS 'Month 36'
from CohortDetails
UNION ALL
select 'Stopped'                                            as Name,
       SUM(CASE WHEN interval_month = 0 and follow_up_status='Stop all' THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 and follow_up_status='Stop all' THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 and follow_up_status='Stop all' THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 and follow_up_status='Stop all' THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 and follow_up_status='Stop all' THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Died'                                               as Name,
       SUM(CASE WHEN interval_month = 0 and follow_up_status='Dead' THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 and follow_up_status='Dead' THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 and follow_up_status='Dead' THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 and follow_up_status='Dead' THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 and follow_up_status='Dead' THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Lost to Follow-up (DROP)'                           as Name,
       SUM(CASE WHEN interval_month = 0 and (follow_up_status='Ran away' or follow_up_status='Loss to follow-up (LTFU)') THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 and (follow_up_status='Ran away' or follow_up_status='Loss to follow-up (LTFU)') THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 and (follow_up_status='Ran away' or follow_up_status='Loss to follow-up (LTFU)') THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 and (follow_up_status='Ran away' or follow_up_status='Loss to follow-up (LTFU)') THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 and (follow_up_status='Ran away' or follow_up_status='Loss to follow-up (LTFU)') THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Percent of cohort alive and on ART'                 as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Mean CD4 % (for children)'                          as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'CD4 median or proportion ≥ 200 (optional)'          as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Viral Load tested'                                  as Name,
       SUM(CASE WHEN interval_month = 0 and viral_load_received_date is not null THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 and viral_load_received_date is not null THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 and viral_load_received_date is not null THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 and viral_load_received_date is not null THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 and viral_load_received_date is not null THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Viral load Suppressed ( < 50 copies/ml'             as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Functional Status [# W or A or B / D * 100]'        as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Percentage Working'                                 as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Percentage Ambulatory'                              as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Percentage Bedridden'                               as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)  AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)  AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END) AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END) AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END) AS 'Month 36'
from CohortDetails
UNION ALL
select 'Number of persons who picked up ARVs each month for 6 months' as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)            AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)            AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END)           AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END)           AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END)           AS 'Month 36'
from CohortDetails
UNION ALL
select 'Number of persons who picked up ARVs each month for 12 months' as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)             AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)             AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END)            AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END)            AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END)            AS 'Month 36'
from CohortDetails
UNION ALL
select 'Number of persons who picked up ARVs each month for 24 months' as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)             AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)             AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END)            AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END)            AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END)            AS 'Month 36'
from CohortDetails
UNION ALL
select 'Number of persons who picked up ARVs each month for 36 months ' as Name,
       SUM(CASE WHEN interval_month = 0 THEN 1 ELSE 0 END)              AS 'Month 0',
       SUM(CASE WHEN interval_month = 6 THEN 1 ELSE 0 END)              AS 'Month 6',
       SUM(CASE WHEN interval_month = 12 THEN 1 ELSE 0 END)             AS 'Month 12',
       SUM(CASE WHEN interval_month = 24 THEN 1 ELSE 0 END)             AS 'Month 24',
       SUM(CASE WHEN interval_month = 36 THEN 1 ELSE 0 END)             AS 'Month 36'
from CohortDetails