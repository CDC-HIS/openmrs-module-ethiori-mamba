DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_art_intr_query;

CREATE PROCEDURE sp_fact_hmis_art_intr_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (SELECT follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t AS transferred_in
                      FROM mamba_flat_encounter_follow_up follow_up
                               LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                         ON follow_up.encounter_id = follow_up_1.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                         ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                         ON follow_up.encounter_id = follow_up_4.encounter_id),
         -- TX curr start
         tmp_latest_follow_up_start AS (SELECT client_id,
                                               follow_up_date,
                                               encounter_id,
                                               follow_up_status,
                                               treatment_end_date,
                                               art_start_date,
                                               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                        FROM FollowUp
                                        WHERE follow_up_status IS NOT NULL
                                          AND art_start_date IS NOT NULL
                                          AND follow_up_date <= REPORT_START_DATE),
         tx_curr_start AS (select *
                           from tmp_latest_follow_up_start
                           where row_num = 1
                             AND follow_up_status in ('Alive', 'Restart medication')
                             AND treatment_end_date >= REPORT_START_DATE),
         -- TX curr
         tmp_latest_follow_up_end AS (SELECT client_id,
                                             follow_up_date,
                                             encounter_id,
                                             follow_up_status,
                                             treatment_end_date,
                                             art_start_date,
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                      FROM FollowUp
                                      WHERE follow_up_status IS NOT NULL
                                        AND art_start_date IS NOT NULL
                                        AND follow_up_date <= REPORT_END_DATE),
         latest_follow_up_status as (select follow_up_status, client_id from tmp_latest_follow_up_end where row_num = 1),
         tx_curr_end AS (select *
                         from tmp_latest_follow_up_end
                         where row_num = 1
                           AND follow_up_status in ('Alive', 'Restart medication')
                           AND treatment_end_date >= REPORT_END_DATE),

         tmp_first_follow_up as (SELECT client_id,
                                        follow_up_date,
                                        encounter_id,
                                        transferred_in,
                                        pregnancy_status,
                                        follow_up_status,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                                 FROM FollowUp
                                 WHERE art_start_date IS NOT NULL),
         first_follow_up as (select * from tmp_first_follow_up where row_num = 1),

         tx_new_tmp as (select tmp_latest_follow_up_end.*
                        from tmp_latest_follow_up_end
                                 join first_follow_up on tmp_latest_follow_up_end.client_id = first_follow_up.client_id
                        where art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                          AND (first_follow_up.transferred_in is null or first_follow_up.transferred_in != 'Yes')
                          AND first_follow_up.follow_up_status in ('Alive', 'Restart medication')
                          AND tmp_latest_follow_up_end.row_num = 1),
         tx_new as (select *
                    from tx_new_tmp),

         started_art as (select *
                         from tx_new
                         union ALL
                         select *
                         from tx_curr_start),
         interrupted_art as (select started_art.*,
                                    client.sex,
                                    client.date_of_birth,
                                    latest_follow_up_status.follow_up_status as latest_follow_up_status
                             from started_art
                                      join mamba_dim_client client on started_art.client_id = client.client_id
                                      join latest_follow_up_status
                                           on started_art.client_id = latest_follow_up_status.client_id
                             where started_art.client_id not in (select client_id from tx_curr_end)),
         restarted_art as (select tx_curr_end.*,
                                  client.sex,
                                  client.date_of_birth,
                                  mrn,
                                  uan
                           from tx_curr_end
                                    join mamba_dim_client client on tx_curr_end.client_id = client.client_id
                           where tx_curr_end.client_id not in (select client_id from started_art) and
                               tx_curr_end.client_id not in (select client_id from tmp_latest_follow_up_start where row_num=1
                                                                                                                and follow_up_status='Transferred out')
                           # tmp_latest_follow_up_start is used to confirm that they are not TO on their latest follow up (before start date) as they would be ignored by started_art as they don't qualify
         )
    SELECT 'HIV_ART_INTR'                                     AS S_NO,
           'Number of ART Clients that interrupted Treatment' as Activity,
           COUNT(*)                                           as Value
    FROM interrupted_art
-- Number of ART Clients Interrupted treatment by outcome
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT'                                       AS S_NO,
           'Number of ART Clients Interrupted treatment by outcome' as Activity,
           COUNT(*)                                                 as Value
    FROM interrupted_art
-- Lost after treatemnt < 3month
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.1'            AS S_NO,
           'Lost after treatemnt < 3month' as Activity,
           COUNT(*)                        as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
-- < 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.1. 1' AS S_NO,
           '< 15 years, Male'      as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Male'
-- < 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.1. 2' AS S_NO,
           '< 15 years, Female'    as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Female'
-- >= 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.1. 3' AS S_NO,
           '>= 15 years, Male'     as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Male'
-- >= 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.1. 4' AS S_NO,
           '>= 15 years, Female'   as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Female'
-- Lost after treatement > 3month
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.2'             AS S_NO,
           'Lost after treatement > 3month' as Activity,
           COUNT(*)                         as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) > 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
-- < 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.2. 1' AS S_NO,
           '< 15 years, Male'      as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) > 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Male'
-- < 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.2. 2' AS S_NO,
           '< 15 years, Female'    as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) > 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Female'
-- >= 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.2. 3' AS S_NO,
           '>= 15 years, Male'     as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) > 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Male'
-- >= 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.2. 4' AS S_NO,
           '>= 15 years, Female'   as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) > 3
      AND latest_follow_up_status not in ('Transferred out', 'Stop all', 'Dead')
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Female'
-- Transferred out
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.3' AS S_NO,
           'Transferred out'    as Activity,
           COUNT(*)             as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Transferred out'
-- < 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.3. 1' AS S_NO,
           '< 15 years, Male'      as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Transferred out'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Male'
-- < 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.3. 2' AS S_NO,
           '< 15 years, Female'    as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Transferred out'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Female'
-- >= 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.3. 3' AS S_NO,
           '>= 15 years, Male'     as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Transferred out'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Male'
-- >= 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.3. 4' AS S_NO,
           '>= 15 years, Female'   as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Transferred out'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Female'
-- Refused (stopped) treatement
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.4'           AS S_NO,
           'Refused (stopped) treatement' as Activity,
           COUNT(*)                       as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Stop all'
-- < 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.4. 1' AS S_NO,
           '< 15 years, Male'      as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Stop all'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Male'
-- < 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.4. 2' AS S_NO,
           '< 15 years, Female'    as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Stop all'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Female'
-- >= 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.4. 3' AS S_NO,
           '>= 15 years, Male'     as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Stop all'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Male'
-- >= 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.4. 4' AS S_NO,
           '>= 15 years, Female'   as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Stop all'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Female'
-- Died
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.5' AS S_NO,
           'Died'               as Activity,
           COUNT(*)             as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Dead'
-- < 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.5. 1' AS S_NO,
           '< 15 years, Male'      as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Dead'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Male'
-- < 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.5. 2' AS S_NO,
           '< 15 years, Female'    as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Dead'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Female'
-- >= 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.5. 3' AS S_NO,
           '>= 15 years, Male'     as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Dead'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Male'
-- >= 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_INTR_OUT.5. 4' AS S_NO,
           '>= 15 years, Female'   as Activity,
           COUNT(*)                as Value
    FROM interrupted_art
    WHERE latest_follow_up_status = 'Dead'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Female'
-- Number of ART clients restarted ARV treatment in the reporting period
    UNION ALL
    SELECT 'HIV_ART_RE_ARV'                                                        AS S_NO,
           'Number of ART clients restarted ARV treatment in the reporting period' as Activity,
           COUNT(*)                                                                as Value
    FROM restarted_art
-- < 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_RE_ARV. 1' AS S_NO,
           '< 15 years, Male'  as Activity,
           COUNT(*)            as Value
    FROM restarted_art
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Male'
-- < 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_RE_ARV. 2'  AS S_NO,
           '< 15 years, Female' as Activity,
           COUNT(*)             as Value
    FROM restarted_art
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
      AND sex = 'Female'
-- >= 15 years, Male
    UNION ALL
    SELECT 'HIV_ART_RE_ARV. 3' AS S_NO,
           '>= 15 years, Male' as Activity,
           COUNT(*)            as Value
    FROM restarted_art
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Male'
-- >= 15 years, Female
    UNION ALL
    SELECT 'HIV_ART_RE_ARV. 4'   AS S_NO,
           '>= 15 years, Female' as Activity,
           COUNT(*)              as Value
    FROM restarted_art
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
      AND sex = 'Female';
END //

DELIMITER ;