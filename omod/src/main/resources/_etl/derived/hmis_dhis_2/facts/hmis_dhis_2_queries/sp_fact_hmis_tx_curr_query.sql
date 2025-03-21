DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_tx_curr_query;

CREATE PROCEDURE sp_fact_hmis_tx_curr_query(
    IN REPORT_END_DATE DATE
)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id           AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_      AS follow_up_date,
                             art_antiretroviral_start_date AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child    breast_feeding_status,
                             pregnancy_status

                      FROM mamba_flat_encounter_follow_up follow_up
                               JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id),
         -- TX curr
         tx_curr_all AS (SELECT PatientId,
                                follow_up_date                                                                             AS FollowupDate,
                                encounter_id,
                                follow_up_status,
                                treatment_end_date,
                                ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                         FROM FollowUp
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date IS NOT NULL
                           AND follow_up_date <= REPORT_END_DATE),

         tx_curr AS (select *
                     from tx_curr_all
                     where row_num = 1
                       AND follow_up_status in ('Alive', 'Restart medication')
                       AND treatment_end_date > REPORT_END_DATE),
         tx_curr_with_client as (select tx_curr.*,
                                        client.sex,
                                        client.date_of_birth,
                                        pregnancy_status,
                                        regimen,
                                        left(regimen, 1) as regimen_line
                                 from FollowUp
                                          inner join tx_curr on FollowUp.encounter_id = tx_curr.encounter_id
                                          left join mamba_dim_client client on tx_curr.PatientId = client.client_id)
-- Number of adults and children who are currently on ART by age, sex and regimen category
    SELECT 'HIV_TX_CURR_ALL'                                                                         AS S_NO,
           'Number of adults and children who are currently on ART by age, sex and regimen category' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM tx_curr_with_client

-- Number of children (<15) who are currently on ART
    UNION ALL
    SELECT 'HIV_TX_CURR_U15'                                   AS S_NO,
           'Number of children (<15) who are currently on ART' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15


-- 1 Children currently on ART aged <1 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U1'                       AS S_NO,
           'Children currently on ART aged <1 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
-- 1.1 Children currently on ART aged <1 yr First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U1.1'                                               AS S_NO,
           'Children currently on ART aged <1 yr First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '1' or regimen_line = '4')
-- 1.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U1.1.1' AS S_NO,
           'Male'               as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 1.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U1.1.2' AS S_NO,
           'Female'             as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 1.2 Children currently on ART aged <1 yr Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U1.2'                                                AS S_NO,
           'Children currently on ART aged <1 yr Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '2' or regimen_line = '5')
-- 1.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U1.2.1' AS S_NO,
           'Male'               as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 1.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U1.2.2' AS S_NO,
           'Female'             as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 1.3 Children currently on ART aged <1 yr Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U1.3'                                               AS S_NO,
           'Children currently on ART aged <1 yr Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '3' or regimen_line = '6')
-- 1.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U1.3.1' AS S_NO,
           'Male'               as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 1.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U1.3.2' AS S_NO,
           'Female'             as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 5 Children currently on ART aged 1-4 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U5'                        AS S_NO,
           'Children currently on ART aged 1-4 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
-- 5.1 Children currently on ART aged 1-4 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U5.1'                                                   AS S_NO,
           'Children currently on ART aged 1-4 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '1' or regimen_line = '4')
-- 5.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U5.1.1' AS S_NO,
           'Male'               as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 5.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U5.1.2' AS S_NO,
           'Female'             as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 5.2 Children currently on ART aged 1-4 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U5.2'                                                    AS S_NO,
           'Children currently on ART aged 1-4 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '2' or regimen_line = '5')
-- 5.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U5.2.1' AS S_NO,
           'Male'               as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 5.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U5.2.2' AS S_NO,
           'Female'             as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 5.3 Children currently on ART aged 1-4 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U5.3'                                                   AS S_NO,
           'Children currently on ART aged 1-4 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '3' or regimen_line = '6')
-- 5.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U5.3.1' AS S_NO,
           'Male'               as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- Female 5.3.2
    UNION ALL
    SELECT 'HIV_TX_CURR_U5.3.2' AS S_NO,
           'Female'             as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 9 Children currently on ART aged 5-9 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U9'                        AS S_NO,
           'Children currently on ART aged 5-9 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
-- 9.1 Children currently on ART aged 5-9 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U9.1'                                                   AS S_NO,
           'Children currently on ART aged 5-9 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '1' or regimen_line = '4')
-- 9.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U9.1.1' AS S_NO,
           'Male'               as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 9.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U9.1.2' AS S_NO,
           'Female'             as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 9.2 Children currently on ART aged 5-9 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U9.2'                                                    AS S_NO,
           'Children currently on ART aged 5-9 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '2' or regimen_line = '5')
-- 9.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U9.2.1' AS S_NO,
           'Male'               as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 9.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U9.2.2' AS S_NO,
           'Female'             as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 9.3 Children currently on ART aged 5-9 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U9.3'                                                   AS S_NO,
           'Children currently on ART aged 5-9 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '3' or regimen_line = '6')
-- 9.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U9.3.1' AS S_NO,
           'Male'               as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 9.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U9.3.2' AS S_NO,
           'Female'             as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 14 Children currently on ART aged 10-14 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U14'                         AS S_NO,
           'Children currently on ART aged 10-14 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
-- 14.1 Children currently on ART aged 10-14 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U14.1'                                                    AS S_NO,
           'Children currently on ART aged 10-14 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '1' or regimen_line = '4')
-- 14.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U14.1.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 14.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U14.1.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 14.2 Children currently on ART aged 10-14 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U14.2'                                                     AS S_NO,
           'Children currently on ART aged 10-14 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '2' or regimen_line = '5')
-- 14.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U14.2.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 14.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U14.2.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 14.3 Children currently on ART aged 10-14 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U14.3'                                                    AS S_NO,
           'Children currently on ART aged 10-14 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '3' or regimen_line = '6')
-- 14.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U14.3.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 14.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U14.3.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- Number of adults who are currently on ART (>=15)
    UNION ALL
    SELECT 'HIV_TX_CURR_ADULT'                                AS S_NO,
           'Number of adults who are currently on ART (>=15)' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15


-- 19 Adult currently on ART aged 15-19 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U19'                   AS S_NO,
           'Adult currently on ART aged 15-19' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
-- 19.1 Adult currently on ART aged 15-19 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U19.1'                                                 AS S_NO,
           'Adult currently on ART aged 15-19 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '1' or regimen_line = '4')
-- 19.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U19.1.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 19.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U19.1.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 19.2 Adult currently on ART aged 15-19 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U19.2'                                                  AS S_NO,
           'Adult currently on ART aged 15-19 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '2' or regimen_line = '5')
-- 19.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U19.2.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 19.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U19.2.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 19.3 Adult currently on ART aged 15-19 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U19.3'                                                 AS S_NO,
           'Adult currently on ART aged 15-19 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '3' or regimen_line = '6')
-- 19.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U19.3.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 19.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U19.3.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 24 Adult currently on ART aged 20-24 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U24'                      AS S_NO,
           'Adult currently on ART aged 20-24 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
-- 24.1 Adult currently on ART aged 20-24 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U24.1'                                                 AS S_NO,
           'Adult currently on ART aged 20-24 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND (regimen_line = '1' or regimen_line = '4')
-- 24.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U24.1.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 24.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U24.1.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 24.2 Adult currently on ART aged 20-24 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U24.2'                                                  AS S_NO,
           'Adult currently on ART aged 20-24 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND (regimen_line = '2' or regimen_line = '5')
-- 24.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U24.2.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 24.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U24.2.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 24.3 Adult currently on ART aged 20-24 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U24.3'                                                 AS S_NO,
           'Adult currently on ART aged 20-24 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND (regimen_line = '3' or regimen_line = '6')
-- 24.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U24.3.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 24.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U24.3.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 29 Adult currently on ART aged 25-29 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U29'                      AS S_NO,
           'Adult currently on ART aged 25-29 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
-- 29.1 Adult currently on ART aged 25-29 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.1'                                                 AS S_NO,
           'Adult currently on ART aged 25-29 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND (regimen_line = '1' or regimen_line = '4')
-- 29.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.1.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 29.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.1.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 29.2 Adult currently on ART aged 25-29 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.2'                                                  AS S_NO,
           'Adult currently on ART aged 25-29 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND (regimen_line = '2' or regimen_line = '5')
-- 29.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.2.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 29.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.2.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 29.3 Adult currently on ART aged 25-29 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.3'                                                 AS S_NO,
           'Adult currently on ART aged 25-29 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND (regimen_line = '3' or regimen_line = '6')
-- 29.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.3.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 29.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.3.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 34 Adult currently on ART aged 30-34 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U34'                      AS S_NO,
           'Adult currently on ART aged 30-34 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
-- 34.1 Adult currently on ART aged 30-34 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U34.1'                                                 AS S_NO,
           'Adult currently on ART aged 30-34 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND (regimen_line = '1' or regimen_line = '4')
-- 34.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U34.1.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 34.1.2 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U34.1.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 34.2 Adult currently on ART aged 30-34 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U34.2'                                                  AS S_NO,
           'Adult currently on ART aged 30-34 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND (regimen_line = '2' or regimen_line = '5')
-- 34.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U34.2.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 34.2.2 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U34.2.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 34.3 Adult currently on ART aged 30-34 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U34.3'                                                 AS S_NO,
           'Adult currently on ART aged 30-34 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND (regimen_line = '3' or regimen_line = '6')
-- 34.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U34.3.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 34.3.2 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U34.3.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 39 Adult currently on ART aged 35-39 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U39'                      AS S_NO,
           'Adult currently on ART aged 35-39 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
-- 39.1 Adult currently on ART aged 35-39 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U39.1'                                                 AS S_NO,
           'Adult currently on ART aged 35-39 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND (regimen_line = '1' or regimen_line = '4')
-- 39.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U39.1.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 39.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U39.1.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 39.2 Adult currently on ART aged 35-39 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U39.2'                                                  AS S_NO,
           'Adult currently on ART aged 35-39 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND (regimen_line = '2' or regimen_line = '5')
-- 39.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U39.2.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 39.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U39.2.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 39.3 Adult currently on ART aged 35-39 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U39.3'                                                 AS S_NO,
           'Adult currently on ART aged 35-39 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND (regimen_line = '3' or regimen_line = '6')
-- 39.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U39.3.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 39.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U39.3.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 44 Adult currently on ART aged 40-44 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U44'                      AS S_NO,
           'Adult currently on ART aged 40-44 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
-- 44.1 Adult currently on ART aged 40-44 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U44.1'                                                 AS S_NO,
           'Adult currently on ART aged 40-44 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND (regimen_line = '1' or regimen_line = '4')
-- 44.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U44.1.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 44.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U44.1.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 44.2 Adult currently on ART aged 40-44 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U44.2'                                                  AS S_NO,
           'Adult currently on ART aged 40-44 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND (regimen_line = '2' or regimen_line = '5')
-- 44.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U44.2.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 44.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U44.2.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 44.3 Adult currently on ART aged 40-44 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U44.3'                                                 AS S_NO,
           'Adult currently on ART aged 40-44 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND (regimen_line = '3' or regimen_line = '6')
-- 44.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U44.3.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 44.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U44.3.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 49 Adult currently on ART aged 45-49 yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U49'                      AS S_NO,
           'Adult currently on ART aged 45-49 yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
-- 49.1 Adult currently on ART aged 45-49 yr on First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.1'                                                 AS S_NO,
           'Adult currently on ART aged 45-49 yr on First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND (regimen_line = '1' or regimen_line = '4')
-- 49.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.1.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 49.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.1.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 49.2 Adult currently on ART aged 45-49 yr on Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.2'                                                  AS S_NO,
           'Adult currently on ART aged 45-49 yr on Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND (regimen_line = '2' or regimen_line = '5')
-- 49.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.2.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 49.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.2.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 49.3 Adult currently on ART aged 45-49 yr on Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.3'                                                 AS S_NO,
           'Adult currently on ART aged 45-49 yr on Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND (regimen_line = '3' or regimen_line = '6')
-- 49.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.3.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 49.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.3.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'


-- 50 Adult currently on ART aged 50+ yr
    UNION ALL
    SELECT 'HIV_TX_CURR_U50'                    AS S_NO,
           'Adult currently on ART aged 50+ yr' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
-- 50.1 Adult currently on ART aged 50+ yr First-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U50.1'                                            AS S_NO,
           'Adult currently on ART aged 50+ yr First-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND (regimen_line = '1' or regimen_line = '4')
-- 50.1.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U50.1.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 50.1.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U50.1.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
-- 50.2 Adult currently on ART aged 50+ yr Second-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U50.2'                                             AS S_NO,
           'Adult currently on ART aged 50+ yr Second-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND (regimen_line = '2' or regimen_line = '5')
-- 50.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U50.2.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 50.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U50.2.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
-- 50.3 Adult currently on ART aged 50+ yr Third-line regimen by sex
    UNION ALL
    SELECT 'HIV_TX_CURR_U50.3'                                            AS S_NO,
           'Adult currently on ART aged 50+ yr Third-line regimen by sex' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND (regimen_line = '3' or regimen_line = '6')
-- 50.3.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U50.3.1' AS S_NO,
           'Male'                as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 50.3.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U50.3.2' AS S_NO,
           'Female'              as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'

-- Currently on ART by pregnancy status
    UNION ALL
    SELECT 'HIV_TX_CURR_PREG'                     AS S_NO,
           'Currently on ART by pregnancy status' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE sex = 'Female'
-- Pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_PREG.1' AS S_NO,
           'Pregnant'           as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE pregnancy_status = 'Yes'
-- Non pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_PREG.2' AS S_NO,
           'Non pregnant'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE (pregnancy_status = 'No' or pregnancy_status is null)
      AND sex = 'Female'

-- 19 Number of children (<19) who are currently on ART by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19'                                               AS S_NO,
           'Number of children (<19) who are currently on ART by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) <= 19

-- 1 Children currently on ART aged <1 yr by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1'                                   AS S_NO,
           'Children currently on ART aged <1 yr by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
-- 1.1 Children currently on ART aged <1 yr on first line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.1'                                                       AS S_NO,
           'Children currently on ART aged <1 yr on first line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '1' or regimen_line = '4')
-- 1.1.1 4f - AZT+3TC+LPVr - 0343175a-e931-4f6c-a0c2-99e55637bda9
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.1.1' AS S_NO,
           '4f - AZT+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '4f - AZT+3TC+LPVr'
-- 1.1.2 4g - ABC+3TC+LPVr - e4353cd1-5a0e-4870-8af4-f012ca0145fe
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.1.2' AS S_NO,
           '4g - ABC+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '4g - ABC+3TC+LPVr'
-- 1.1.3 4j - ABC+3TC+DTG - eae01634-65ec-4174-a95c-622456663727
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.1.3' AS S_NO,
           '4j - ABC+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '4j - ABC+3TC+DTG'
-- 1.1.4 4K - AZT+3TC+DTG - dabcfa6d-56e5-4e5d-a2f2-faf4e18b9211
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.1.4' AS S_NO,
           '4K - AZT+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '4K - AZT+3TC+DTG'
-- 1.1.5 4h=other first line - 582c6b67-2f0a-4cc0-b7c0-632b4b387a17
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.1.5'            AS S_NO,
           '4h - Other Child 1st line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen not in ('4f - AZT+3TC+LPVr', '4g - ABC+3TC+LPVr', '4j - ABC+3TC+DTG', '4K - AZT+3TC+DTG')
      AND (regimen_line = '1' or regimen_line = '4')
-- 1.2 Children currently on ART aged <1 yr on second line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.2'                                                        AS S_NO,
           'Children currently on ART aged <1 yr on second line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '2' or regimen_line = '5')
-- 1.2.1 5e - ABC+3TC+LPVr - fbb4db0f-4cf4-4ca3-bde0-e20eb065e7da
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.2.1' AS S_NO,
           '5e - ABC+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '5e - ABC+3TC+LPVr'
-- 1.2.2 5f - AZT+3TC+LPV/r - 9ef4d67d-0e05-422a-aac5-80eaf71daadc
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.2.2' AS S_NO,
           '5f - AZT+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '5f - AZT+3TC+LPVr'

    # -- 1.2.3 5f - AZT+3TC+LPV/r - 9ef4d67d-0e05-422a-aac5-80eaf71daadc // TO DO review regimen
# UNION ALL
# SELECT 'HIV_TX_CURR_REG_U1.2.3' AS S_NO,
#        '5f - AZT+3TC+LPVr'      as Activity,
#        COUNT(*)
# FROM tx_curr_with_client
# WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
#   AND regimen = '5f - AZT+3TC+LPVr'

-- 1.2.4 5m - ABC+3TC+DTG - 3ed5d8a0-c957-4439-8f86-6c2538e69d3a
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.2.4' AS S_NO,
           '5m - ABC+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '5m - ABC+3TC+DTG'
-- 1.2.5 5n - AZT+3TC+DTG - 10c0f78a-cd78-40cd-9824-8d24dc95bb90
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.2.5' AS S_NO,
           '5n - AZT+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '5n - AZT+3TC+DTG'
-- 1.2.6 5j - Other Child 2nd line regimen (5j=other secondline) - 0cc9e467-c1ce-4eff-879d-8936a81b6ea3
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.2.6'            AS S_NO,
           '5j - Other Child 2nd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen not in
          ('5e - ABC+3TC+LPVr', '5f - AZT+3TC+LPVr', '5j - ABC+3TC+LPVr', '5m - ABC+3TC+DTG', '5n - AZT+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
-- 1.3 Children currently on ART aged <1 yr on Third line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.3'                                                       AS S_NO,
           'Children currently on ART aged <1 yr on third line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND (regimen_line = '3' or regimen_line = '6')
-- 1.3.1 6c - DRVr+DTG+AZT+3TC - 5c8d27eb-4642-4c6f-b131-1182df8d1276
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.3.1' AS S_NO,
           '6c - DRVr+DTG+AZT+3TC'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '6c - DRVr+DTG+AZT+3TC'
-- 1.3.2 6f - DRV/r+DTG+ABC+3TC - e3eb6ae1-fd70-47b2-b1a6-702fae87b061
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.3.2' AS S_NO,
           '6f - DRV/r+DTG+ABC+3TC' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen = '6f - DRV/r+DTG+ABC+3TC'
-- 1.3.3 6e - Other Child 3rd line regimen - 57eef726-e977-4efd-996b-a94860341320
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U1.3.3'            AS S_NO,
           '6e - Other Child 3rd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND regimen not in ('6c - DRVr+DTG+AZT+3TC', '6f - DRV/r+DTG+ABC+3TC')
      AND (regimen_line = '3' or regimen_line = '6')


-- 4 Children currently on ART aged 1-4 yr by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4'                                    AS S_NO,
           'Children currently on ART aged 1-4 yr by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
-- 4.1 Children currently on ART aged 1-4 yr on First line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.1'                                                        AS S_NO,
           'Children currently on ART aged 1-4 yr on First line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '1' or regimen_line = '4')
-- 4.1.1 4d - AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.1.1' AS S_NO,
           '4d - AZT+3TC+EFV'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '4d - AZT+3TC+EFV'
-- 4.1.2 4f - AZT+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.1.2' AS S_NO,
           '4f - AZT+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '4f - AZT+3TC+LPVr'
-- 4.1.3 4g - ABC+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.1.3' AS S_NO,
           '4g - ABC+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '4g - ABC+3TC+LPVr'
-- 4.1.4 4g - ABC+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.1.4' AS S_NO,
           '4j - ABC+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '4j - ABC+3TC+DTG'
-- 4.1.5 4K - AZT+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.1.5' AS S_NO,
           '4K - AZT+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '4K - AZT+3TC+DTG'
-- 4.1.6 4L - ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.1.6' AS S_NO,
           '4L - ABC+3TC+EFV'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '4L - ABC+3TC+EFV'
-- 4.1.7 4L - ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.1.7'            AS S_NO,
           '4h - Other Child 1st line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen not in ('4d - AZT+3TC+EFV', '4f - AZT+3TC+LPVr', '4g - ABC+3TC+LPVr',
                          '4j - ABC+3TC+DTG', '4K - AZT+3TC+DTG', '4L - ABC+3TC+EFV')
      AND (regimen_line = '1' or regimen_line = '4')
-- 4.2 Children currently on ART aged 1-4 yr on Second-line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.2'                                                         AS S_NO,
           'Children currently on ART aged 1-4 yr on Second-line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '2' or regimen_line = '5')
-- 4.2.1 5e - ABC+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.2.1' AS S_NO,
           '5e - ABC+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '5e - ABC+3TC+LPVr'
-- 4.2.2 5f - AZT+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.2.2' AS S_NO,
           '5f - AZT+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '5f - AZT+3TC+LPVr'
-- 4.2.3 5h - ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.2.3' AS S_NO,
           '5h - ABC+3TC+EFV'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '5h - ABC+3TC+EFV'
-- 4.2.4 5m - ABC+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.2.4' AS S_NO,
           '5m - ABC+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '5m - ABC+3TC+DTG'
-- 4.2.5 5n - AZT+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.2.5' AS S_NO,
           '5n - AZT+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '5n - AZT+3TC+DTG'
-- 4.2.6 5j - Other Child 2nd line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.2.6'            AS S_NO,
           '5j - Other Child 2nd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen not in
          ('5e - ABC+3TC+LPVr', '5f - AZT+3TC+LPVr', '5h - ABC+3TC+EFV', '5m - ABC+3TC+DTG', '5n - AZT+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
-- 4.3 Children currently on ART aged 1-4 yr on Third Line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.3'                                                        AS S_NO,
           'Children currently on ART aged 1-4 yr on Third Line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND (regimen_line = '3' or regimen_line = '6')
-- 4.3.1 6c - DRVr+DTG+AZT+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.3.1' AS S_NO,
           '6c - DRVr+DTG+AZT+3TC'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '6c - DRVr+DTG+AZT+3TC'
-- 4.3.2 6f - DRV/r+DTG+ABC+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.3.2' AS S_NO,
           '6f - DRV/r+DTG+ABC+3TC' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '6f - DRV/r+DTG+ABC+3TC'
-- 4.3.3 6g - DRV/r+ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.3.3' AS S_NO,
           '6g - DRV/r+ABC+3TC+EFV' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '6g - DRV/r+ABC+3TC+EFV'
-- 4.3.4 6h - DRV/r+AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.3.4' AS S_NO,
           '6h - DRV/r+AZT+3TC+EFV' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen = '6h - DRV/r+AZT+3TC+EFV'
-- 4.3.5 6e - Other Child 3rd line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U4.3.5'            AS S_NO,
           '6e - Other Child 3rd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND regimen not in
          ('6c - DRVr+DTG+AZT+3TC', '6f - DRV/r+DTG+ABC+3TC', '6g - DRV/r+ABC+3TC+EFV', '6h - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')


-- 9 Children currently on ART aged 5-9 yr on by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9'                                       AS S_NO,
           'Children currently on ART aged 5-9 yr on by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9

-- 9.1 Children currently on ART aged 5-9 yr on First line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1'                                                        AS S_NO,
           'Children currently on ART aged 5-9 yr on First line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '1' or regimen_line = '4')
-- 9.1.1 4d - AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1.1' AS S_NO,
           '4d - AZT+3TC+EFV'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '4d - AZT+3TC+EFV'
-- 9.1.2 4e - TDF+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1.2' AS S_NO,
           '4e - TDF+3TC+EFV'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '4e - TDF+3TC+EFV'
-- 9.1.3 4f - AZT+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1.3' AS S_NO,
           '4f - AZT+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '4f - AZT+3TC+LPVr'
-- 9.1.4 4g - ABC+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1.4' AS S_NO,
           '4g - ABC+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '4g - ABC+3TC+LPVr'
-- 9.1.5 4i - TDF+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1.5' AS S_NO,
           '4i - TDF+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '4i - TDF+3TC+DTG'
-- 9.1.6 4j - ABC+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1.6' AS S_NO,
           '4j - ABC+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '4j - ABC+3TC+DTG'
-- 9.1.7 4K - AZT+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1.7' AS S_NO,
           '4K - AZT+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '4K - AZT+3TC+DTG'
-- 9.1.8 4L - ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1.8' AS S_NO,
           '4L - ABC+3TC+EFV'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '4L - ABC+3TC+EFV'
-- 9.1.9 4h - Other Child 1st line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.1.9'            AS S_NO,
           '4h - Other Child 1st line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen not in ('4d - AZT+3TC+EFV', '4e - TDF+3TC+EFV', '4f - AZT+3TC+LPVr', '4g - ABC+3TC+LPVr',
                          '4i - TDF+3TC+DTG', '4j - ABC+3TC+DTG', '4K - AZT+3TC+DTG', '4L - ABC+3TC+EFV')
      AND (regimen_line = '1' or regimen_line = '4')
-- 9.2 Children currently on ART aged 5-9 yr on Second line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2'                                                         AS S_NO,
           'Children currently on ART aged 5-9 yr on Second line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '2' or regimen_line = '5')
-- 9.2.1 5e - ABC+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2.1' AS S_NO,
           '5e - ABC+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '5e - ABC+3TC+LPVr'
-- 9.2.2 5f - AZT+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2.2' AS S_NO,
           '5f - AZT+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '5f - AZT+3TC+LPVr'
-- 9.2.3 5g - TDF+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2.3' AS S_NO,
           '5g - TDF+3TC+EFV'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '5g - TDF+3TC+EFV'
-- 9.2.4 5h - ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2.4' AS S_NO,
           '5h - ABC+3TC+EFV'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '5h - ABC+3TC+EFV'
-- 9.2.5 5i - TDF+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2.5' AS S_NO,
           '5i - TDF+3TC+LPVr'      as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '5i - TDF+3TC+LPVr'
-- 9.2.6 5m - ABC+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2.6' AS S_NO,
           '5m - ABC+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '5m - ABC+3TC+DTG'
-- 9.2.7 5n - AZT+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2.7' AS S_NO,
           '5n - AZT+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '5n - AZT+3TC+DTG'
-- 9.2.8 5o - TDF+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2.8' AS S_NO,
           '5o - TDF+3TC+DTG'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '5o - TDF+3TC+DTG'
-- 9.2.9 5j - Other Child 2nd line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.2.9'            AS S_NO,
           '5j - Other Child 2nd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen not in ('5e - ABC+3TC+LPVr', '5f - AZT+3TC+LPVr', '5g - TDF+3TC+EFV', '5h - ABC+3TC+EFV',
                          '5i - TDF+3TC+LPVr', '5m - ABC+3TC+DTG', '5n - AZT+3TC+DTG', '5o - TDF+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
-- 9.3 Children currently on ART aged 5-9 yr on Third Line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.3'                                                        AS S_NO,
           'Children currently on ART aged 5-9 yr on Third Line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND (regimen_line = '3' or regimen_line = '6')
-- 9.3.1 6c - DRVr+DTG+AZT+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.3.1' AS S_NO,
           '6c - DRVr+DTG+AZT+3TC'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '6c - DRVr+DTG+AZT+3TC'
-- 9.3.2 6d - DRVr+DTG+TDF+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.3.2' AS S_NO,
           '6d - DRVr+DTG+TDF+3TC'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '6d - DRVr+DTG+TDF+3TC'
-- 9.3.3 6f - DRV/r+DTG+ABC+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.3.3' AS S_NO,
           '6f - DRV/r+DTG+ABC+3TC' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '6f - DRV/r+DTG+ABC+3TC'
-- 9.3.4 6g - DRV/r+ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.3.4' AS S_NO,
           '6g - DRV/r+ABC+3TC+EFV' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '6g - DRV/r+ABC+3TC+EFV'
-- 9.3.5 6h - DRV/r+AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.3.5' AS S_NO,
           '6h - DRV/r+AZT+3TC+EFV' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen = '6h - DRV/r+AZT+3TC+EFV'
-- 9.3.6 6e - Other Child 3rd line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U9.3.6'            AS S_NO,
           '6e - Other Child 3rd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND regimen not in ('6c - DRVr+DTG+AZT+3TC', '6d - DRVr+DTG+TDF+3TC', '6f - DRV/r+DTG+ABC+3TC',
                          '6g - DRV/r+ABC+3TC+EFV', '6h - DRV/r+AZT+3TC+EFV')

      AND (regimen_line = '3' or regimen_line = '6')

-- 14 Children currently on ART aged 10-14 year by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14'                                       AS S_NO,
           'Children currently on ART aged 10-14 year by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14

-- 14.1 Children currently on ART aged 10-14 year on First line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1'                                                           AS S_NO,
           'Children currently on ART aged 10-14 year on First line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '1' or regimen_line = '4')
-- 14.1.1 4d - AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1.1' AS S_NO,
           '4d - AZT+3TC+EFV'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '4d - AZT+3TC+EFV'
-- 14.1.2 4e - TDF+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1.2' AS S_NO,
           '4e - TDF+3TC+EFV'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '4e - TDF+3TC+EFV'
-- 14.1.3 4f - AZT+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1.3' AS S_NO,
           '4f - AZT+3TC+LPVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '4f - AZT+3TC+LPVr'
-- 14.1.4 4g - ABC+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1.4' AS S_NO,
           '4g - ABC+3TC+LPVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '4g - ABC+3TC+LPVr'
-- 14.1.5 4i - TDF+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1.5' AS S_NO,
           '4i - TDF+3TC+DTG'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '4i - TDF+3TC+DTG'
-- 14.1.6 4j - ABC+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1.6' AS S_NO,
           '4j - ABC+3TC+DTG'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '4j - ABC+3TC+DTG'
-- 14.1.7 4K - AZT+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1.7' AS S_NO,
           '4K - AZT+3TC+DTG'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '4K - AZT+3TC+DTG'
-- 14.1.8 4L - ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1.8' AS S_NO,
           '4L - ABC+3TC+EFV'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '4L - ABC+3TC+EFV'
-- 14.1.9 4h - Other Child 1st line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.1.9'           AS S_NO,
           '4h - Other Child 1st line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen not in ('4d - AZT+3TC+EFV', '4e - TDF+3TC+EFV', '4f - AZT+3TC+LPVr', '4g - ABC+3TC+LPVr',
                          '4i - TDF+3TC+DTG', '4j - ABC+3TC+DTG', '4K - AZT+3TC+DTG', '4L - ABC+3TC+EFV')
      AND (regimen_line = '1' or regimen_line = '4')
-- 14.2 Children currently on ART aged 10-14 year on Second line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2'                                                            AS S_NO,
           'Children currently on ART aged 10-14 year on Second line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '2' or regimen_line = '5')
-- 14.2.1 5e - ABC+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2.1' AS S_NO,
           '5e - ABC+3TC+LPVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '5e - ABC+3TC+LPVr'
-- 14.2.2 5f - AZT+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2.2' AS S_NO,
           '5f - AZT+3TC+LPVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '5f - AZT+3TC+LPVr'
-- 14.2.3 5g - TDF+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2.3' AS S_NO,
           '5g - TDF+3TC+EFV'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '5g - TDF+3TC+EFV'
-- 14.2.4 5h - ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2.4' AS S_NO,
           '5h - ABC+3TC+EFV'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '5h - ABC+3TC+EFV'
-- 14.2.5 5h - ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2.5' AS S_NO,
           '5i - TDF+3TC+LPVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '5i - TDF+3TC+LPVr'
-- 14.2.6 5m - ABC+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2.6' AS S_NO,
           '5m - ABC+3TC+DTG'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '5m - ABC+3TC+DTG'
-- 14.2.7 5n - AZT+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2.7' AS S_NO,
           '5n - AZT+3TC+DTG'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '5n - AZT+3TC+DTG'
-- 14.2.8 5o - TDF+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2.8' AS S_NO,
           '5o - TDF+3TC+DTG'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '5o - TDF+3TC+DTG'
-- 14.2.9 5j - Other Child 2nd line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.2.9'           AS S_NO,
           '5j - Other Child 2nd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen not in ('5e - ABC+3TC+LPVr', '5f - AZT+3TC+LPVr', '5g - TDF+3TC+EFV', '5h - ABC+3TC+EFV',
                          '5i - TDF+3TC+LPVr', '5m - ABC+3TC+DTG', '5n - AZT+3TC+DTG', '5o - TDF+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
-- 14.3 Children currently on ART aged 10-14 year on Third Line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.3'                                                           AS S_NO,
           'Children currently on ART aged 10-14 year on Third Line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND (regimen_line = '3' or regimen_line = '6')
-- 14.3.1 6c - DRVr+DTG+AZT+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.3.1' AS S_NO,
           '6c - DRVr+DTG+AZT+3TC'   as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '6c - DRVr+DTG+AZT+3TC'
-- 14.3.2 6d - DRVr+DTG+TDF+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.3.2' AS S_NO,
           '6d - DRVr+DTG+TDF+3TC'   as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '6d - DRVr+DTG+TDF+3TC'
-- 14.3.3 6f - DRV/r+DTG+ABC+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.3.3' AS S_NO,
           '6f - DRV/r+DTG+ABC+3TC'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '6f - DRV/r+DTG+ABC+3TC'
-- 14.3.4 6g - DRV/r+ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.3.4' AS S_NO,
           '6g - DRV/r+ABC+3TC+EFV'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '6g - DRV/r+ABC+3TC+EFV'
-- 14.3.5 6h - DRV/r+AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.3.5' AS S_NO,
           '6h - DRV/r+AZT+3TC+EFV'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen = '6h - DRV/r+AZT+3TC+EFV'
-- 14.3.6 6e - Other Child 3rd line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U14.3.6'           AS S_NO,
           '6e - Other Child 3rd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND regimen not in
          ('6c - DRVr+DTG+AZT+3TC', '6d - DRVr+DTG+TDF+3TC', '6f - DRV/r+DTG+ABC+3TC', '6g - DRV/r+ABC+3TC+EFV',
           '6h - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')

-- 19 Adult currently on ART aged 15-19 by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19'                               AS S_NO,
           'Adult currently on ART aged 15-19 by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19

-- 19.1 Adult currently on ART aged 15-19 on First line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.1'                                                   AS S_NO,
           'Adult currently on ART aged 15-19 on First line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '1' or regimen_line = '4')
-- 19.1.1 1d - AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.1.1' AS S_NO,
           '1d - AZT+3TC+EFV'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '1d - AZT+3TC+EFV'
-- 19.1.2 1e - TDF+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.1.2' AS S_NO,
           '1e - TDF+3TC+EFV'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '1e - TDF+3TC+EFV'
-- 19.1.3 1g - ABC+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.1.3' AS S_NO,
           '1g - ABC+3TC+EFV'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '1g - ABC+3TC+EFV'
-- 19.1.4 1j - TDF+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.1.4' AS S_NO,
           '1j - TDF+3TC+DTG'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '1j - TDF+3TC+DTG'
-- 19.1.5 1k - AZT+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.1.5' AS S_NO,
           '1k - AZT+3TC+DTG'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '1k - AZT+3TC+DTG'
-- 19.1.6 1i - Other Adult 1st line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.1.6'           AS S_NO,
           '1i - Other Adult 1st line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen not in
          ('1d - AZT+3TC+EFV', '1e - TDF+3TC+EFV', '1g - ABC+3TC+EFV', '1j - TDF+3TC+DTG', '1k - AZT+3TC+DTG')
      AND (regimen_line = '1' or regimen_line = '4')
-- 19.2 Adult currently on ART aged 15-19 on Second line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.2'                                                    AS S_NO,
           'Adult currently on ART aged 15-19 on Second line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '2' or regimen_line = '5')
-- 19.2.1 1d - AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.2.1' AS S_NO,
           '2e - AZT+3TC+LPVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '2e - AZT+3TC+LPVr'
-- 19.2.2 2f - AZT+3TC+ATVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.2.2' AS S_NO,
           '2f - AZT+3TC+ATVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '2f - AZT+3TC+ATVr'
-- 19.2.3 2f - AZT+3TC+ATVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.2.3' AS S_NO,
           '2g - TDF+3TC+LPVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '2g - TDF+3TC+LPVr'
-- 19.2.4 2f - AZT+3TC+ATVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.2.4' AS S_NO,
           '2h - TDF+3TC+ATVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '2h - TDF+3TC+ATVr'
-- 19.2.5 2i - ABC+3TC+LPVr
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.2.5' AS S_NO,
           '2i - ABC+3TC+LPVr'       as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '2i - ABC+3TC+LPVr'
-- 19.2.6 2j -TDF+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.2.6' AS S_NO,
           '2j -TDF+3TC+DTG'         as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '2j -TDF+3TC+DTG'
-- 19.2.7 2k - AZT+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.2.7' AS S_NO,
           '2k - AZT+3TC+DTG'        as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '2k - AZT+3TC+DTG'
-- 19.2.8 2L - Other Adult 2nd line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.2.8'           AS S_NO,
           '2L - Other Adult 2nd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen not in ('2e - AZT+3TC+LPVr', '2f - AZT+3TC+ATVr', '2g - TDF+3TC+LPVr', '2h - TDF+3TC+ATVr',
                          '2i - ABC+3TC+LPVr', '2j -TDF+3TC+DTG', '2k - AZT+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')

-- 19.3 Adult currently on ART aged 15-19 on Third Line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3'                                                   AS S_NO,
           'Adult currently on ART aged 15-19 on Third Line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND (regimen_line = '3' or regimen_line = '6')
-- 19.3.1 3a -  DRV/r+DTG+AZT+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3.1' AS S_NO,
           '3a -  DRV/r+DTG+AZT+3TC' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3a -  DRV/r+DTG+AZT+3TC'
-- 19.3.2 3b - DRV/r+DTG+TDF+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3.2' AS S_NO,
           '3b - DRV/r+DTG+TDF+3TC'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3b - DRV/r+DTG+TDF+3TC'
-- 19.3.3 3c - DRV/r+ABC+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3.3' AS S_NO,
           '3c - DRV/r+ABC+3TC+DTG'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3c - DRV/r+ABC+3TC+DTG'
-- 19.3.4 3e - DRV/r+TDF+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3.4' AS S_NO,
           '3e - DRV/r+TDF+3TC+EFV'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3e - DRV/r+TDF+3TC+EFV'
-- 19.3.5 3f - DRV/r+AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3.5' AS S_NO,
           '3f - DRV/r+AZT+3TC+EFV'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3f - DRV/r+AZT+3TC+EFV'
-- 19.3.6 3d - Other Adult 3rd line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3.6'           AS S_NO,
           '3d - Other Adult 3rd line regimen' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen not in ('3a -  DRV/r+DTG+AZT+3TC', '3b - DRV/r+DTG+TDF+3TC', '3c - DRV/r+ABC+3TC+DTG',
                          '3e - DRV/r+TDF+3TC+EFV', '3f - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')
-- 20 Adults >=20 years currently on ART by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20'                                AS S_NO,
           'Adults >=20 years currently on ART by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
-- 20.1 Adults >=20 years currently on First line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1'                                             AS S_NO,
           'Adults >=20 years currently on First line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND (regimen_line = '1' or regimen_line = '4')
-- 20.1.1 1d - AZT+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.1' AS S_NO,
           '1d - AZT+3TC+EFV, Male'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1d - AZT+3TC+EFV'
      AND sex = 'Male'
-- 20.1.2 1d - AZT+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.2'             AS S_NO,
           '1d - AZT+3TC+EFV, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1d - AZT+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.3 1d - AZT+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.3'                 AS S_NO,
           '1d - AZT+3TC+EFV, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1d - AZT+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.4 1e - TDF+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.4' AS S_NO,
           '1e - TDF+3TC+EFV, Male'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1e - TDF+3TC+EFV'
      AND sex = 'Male'
-- 20.1.5 1e - TDF+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.5'             AS S_NO,
           '1e - TDF+3TC+EFV, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1e - TDF+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.6 1e - TDF+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.6'                 AS S_NO,
           '1e - TDF+3TC+EFV, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1e - TDF+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.7 1g - ABC+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.7' AS S_NO,
           '1g - ABC+3TC+EFV, Male'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1g - ABC+3TC+EFV'
      AND sex = 'Male'
-- 20.1.8 1g - ABC+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.8'             AS S_NO,
           '1g - ABC+3TC+EFV, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1g - ABC+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.9 1g - ABC+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.9'                 AS S_NO,
           '1g - ABC+3TC+EFV, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1g - ABC+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.10 1j - TDF+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.10' AS S_NO,
           '1j - TDF+3TC+DTG, Male'   as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1j - TDF+3TC+DTG'
      AND sex = 'Male'
-- 20.1.11 1j - TDF+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.11'            AS S_NO,
           '1j - TDF+3TC+DTG, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1j - TDF+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.12 1j - TDF+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.12'                AS S_NO,
           '1j - TDF+3TC+DTG, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1j - TDF+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.13 1k - AZT+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.13' AS S_NO,
           '1k - AZT+3TC+DTG, Male'   as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1k - AZT+3TC+DTG'
      AND sex = 'Male'
-- 20.1.14 1k - AZT+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.14'            AS S_NO,
           '1k - AZT+3TC+DTG, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1k - AZT+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.15 1k - AZT+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.15'                AS S_NO,
           '1k - AZT+3TC+DTG, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1k - AZT+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.16 1i - Other Adult 1st line regimen, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.16'                AS S_NO,
           '1i - Other Adult 1st line regimen, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in
          ('1d - AZT+3TC+EFV', '1e - TDF+3TC+EFV', '1g - ABC+3TC+EFV', '1j - TDF+3TC+DTG', '1k - AZT+3TC+DTG')
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 20.1.17 1i - Other Adult 1st line regimen, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.17'                             AS S_NO,
           '1i - Other Adult 1st line regimen, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in
          ('1d - AZT+3TC+EFV', '1e - TDF+3TC+EFV', '1g - ABC+3TC+EFV', '1j - TDF+3TC+DTG', '1k - AZT+3TC+DTG')
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.18 1i - Other Adult 1st line regimen, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.1.18'                                 AS S_NO,
           '1i - Other Adult 1st line regimen, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in
          ('1d - AZT+3TC+EFV', '1e - TDF+3TC+EFV', '1g - ABC+3TC+EFV', '1j - TDF+3TC+DTG', '1k - AZT+3TC+DTG')
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2 Adults >=20 years currently on Second line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2'                                              AS S_NO,
           'Adults >=20 years currently on Second line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND (regimen_line = '2' or regimen_line = '5')
-- 20.2.1 2e - AZT+3TC+LPVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.1' AS S_NO,
           '2e - AZT+3TC+LPVr, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2e - AZT+3TC+LPVr'
      AND sex = 'Male'
-- 20.2.2 2e - AZT+3TC+LPVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.2'              AS S_NO,
           '2e - AZT+3TC+LPVr, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2e - AZT+3TC+LPVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.3 2e - AZT+3TC+LPVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.3'                  AS S_NO,
           '2e - AZT+3TC+LPVr, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2e - AZT+3TC+LPVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.4 2f - AZT+3TC+ATVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.4' AS S_NO,
           '2f - AZT+3TC+ATVr, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2f - AZT+3TC+ATVr'
      AND sex = 'Male'
-- 20.2.5 2f - AZT+3TC+ATVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.5'              AS S_NO,
           '2f - AZT+3TC+ATVr, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2f - AZT+3TC+ATVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.6 2f - AZT+3TC+ATVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.6'                  AS S_NO,
           '2f - AZT+3TC+ATVr, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2f - AZT+3TC+ATVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.7 2g - TDF+3TC+LPVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.7' AS S_NO,
           '2g - TDF+3TC+LPVr, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2g - TDF+3TC+LPVr'
      AND sex = 'Male'
-- 20.2.8 2g - TDF+3TC+LPVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.8'              AS S_NO,
           '2g - TDF+3TC+LPVr, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2g - TDF+3TC+LPVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.9 2g - TDF+3TC+LPVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.9'                  AS S_NO,
           '2g - TDF+3TC+LPVr, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2g - TDF+3TC+LPVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.10 2h - TDF+3TC+ATVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.10' AS S_NO,
           '2h - TDF+3TC+ATVr, Male'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2h - TDF+3TC+ATVr'
      AND sex = 'Male'
-- 20.2.11 2h - TDF+3TC+ATVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.11'             AS S_NO,
           '2h - TDF+3TC+ATVr, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2h - TDF+3TC+ATVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.12 2h - TDF+3TC+ATVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.12'                 AS S_NO,
           '2h - TDF+3TC+ATVr, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2h - TDF+3TC+ATVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.13 2i - ABC+3TC+LPVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.13' AS S_NO,
           '2i - ABC+3TC+LPVr, Male'  as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2i - ABC+3TC+LPVr'
      AND sex = 'Male'
-- 20.2.14 2i - ABC+3TC+LPVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.14'             AS S_NO,
           '2i - ABC+3TC+LPVr, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2i - ABC+3TC+LPVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.15 2i - ABC+3TC+LPVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.15'                 AS S_NO,
           '2i - ABC+3TC+LPVr, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2i - ABC+3TC+LPVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.16 2j -TDF+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.16' AS S_NO,
           '2j -TDF+3TC+DTG, Male'    as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2j -TDF+3TC+DTG'
      AND sex = 'Male'
-- 20.2.17 2j -TDF+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.17'           AS S_NO,
           '2j -TDF+3TC+DTG, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2j -TDF+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.18 2j -TDF+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.18'               AS S_NO,
           '2j -TDF+3TC+DTG, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2j -TDF+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.19 2k - AZT+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.19' AS S_NO,
           '2k - AZT+3TC+DTG, Male'   as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2k - AZT+3TC+DTG'
      AND sex = 'Male'
-- 20.2.20 2k - AZT+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.20'            AS S_NO,
           '2k - AZT+3TC+DTG, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2k - AZT+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.21 2k - AZT+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.21'                AS S_NO,
           '2k - AZT+3TC+DTG, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2k - AZT+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.22 2L - Other Adult 2nd line regimen, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.22'                AS S_NO,
           '2L - Other Adult 2nd line regimen, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('2e - AZT+3TC+LPVr', '2f - AZT+3TC+ATVr', '2g - TDF+3TC+LPVr', '2h - TDF+3TC+ATVr',
                          '2i - ABC+3TC+LPVr', '2j -TDF+3TC+DTG', '2k - AZT+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 20.2.23 2L - Other Adult 2nd line regimen, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.23'                             AS S_NO,
           '2L - Other Adult 2nd line regimen, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('2e - AZT+3TC+LPVr', '2f - AZT+3TC+ATVr', '2g - TDF+3TC+LPVr', '2h - TDF+3TC+ATVr',
                          '2i - ABC+3TC+LPVr', '2j -TDF+3TC+DTG', '2k - AZT+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.24 2L - Other Adult 2nd line regimen, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.2.24'                                 AS S_NO,
           '2L - Other Adult 2nd line regimen, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('2e - AZT+3TC+LPVr', '2f - AZT+3TC+ATVr', '2g - TDF+3TC+LPVr', '2h - TDF+3TC+ATVr',
                          '2i - ABC+3TC+LPVr', '2j -TDF+3TC+DTG', '2k - AZT+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)

-- 20.3 Adults >=20 years currently on Third Line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3'                                             AS S_NO,
           'Adults >=20 years currently on Third Line regimen by regimen type' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND (regimen_line = '3' or regimen_line = '6')
-- 20.3.1 3a -  DRV/r+DTG+AZT+3TC, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.1'       AS S_NO,
           '3a -  DRV/r+DTG+AZT+3TC, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3a -  DRV/r+DTG+AZT+3TC'
      AND sex = 'Male'
-- 20.3.2 3a -  DRV/r+DTG+AZT+3TC, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.2'                    AS S_NO,
           '3a -  DRV/r+DTG+AZT+3TC, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3a -  DRV/r+DTG+AZT+3TC'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.3 3a -  DRV/r+DTG+AZT+3TC, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.3'                        AS S_NO,
           '3a -  DRV/r+DTG+AZT+3TC, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3a -  DRV/r+DTG+AZT+3TC'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.3.4 3b - DRV/r+DTG+TDF+3TC, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.4'      AS S_NO,
           '3b - DRV/r+DTG+TDF+3TC, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3b - DRV/r+DTG+TDF+3TC'
      AND sex = 'Male'
-- 20.3.5 3b - DRV/r+DTG+TDF+3TC, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.5'                   AS S_NO,
           '3b - DRV/r+DTG+TDF+3TC, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3b - DRV/r+DTG+TDF+3TC'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.6 3b - DRV/r+DTG+TDF+3TC, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.6'                       AS S_NO,
           '3b - DRV/r+DTG+TDF+3TC, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3b - DRV/r+DTG+TDF+3TC'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.3.7 3c - DRV/r+ABC+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.7'      AS S_NO,
           '3c - DRV/r+ABC+3TC+DTG, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3c - DRV/r+ABC+3TC+DTG'
      AND sex = 'Male'
-- 20.3.8 3c - DRV/r+ABC+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.8'                   AS S_NO,
           '3c - DRV/r+ABC+3TC+DTG, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3c - DRV/r+ABC+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.9 3c - DRV/r+ABC+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.9'                       AS S_NO,
           '3c - DRV/r+ABC+3TC+DTG, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3c - DRV/r+ABC+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.3.10 3e - DRV/r+TDF+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.10'     AS S_NO,
           '3e - DRV/r+TDF+3TC+EFV, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3e - DRV/r+TDF+3TC+EFV'
      AND sex = 'Male'
-- 20.3.11 3e - DRV/r+TDF+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.11'                  AS S_NO,
           '3e - DRV/r+TDF+3TC+EFV, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3e - DRV/r+TDF+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.12 3e - DRV/r+TDF+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.12'                      AS S_NO,
           '3e - DRV/r+TDF+3TC+EFV, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3e - DRV/r+TDF+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.3.13 3f - DRV/r+AZT+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.13'     AS S_NO,
           '3f - DRV/r+AZT+3TC+EFV, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3f - DRV/r+AZT+3TC+EFV'
      AND sex = 'Male'
-- 20.3.14 3f - DRV/r+AZT+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.14'                  AS S_NO,
           '3f - DRV/r+AZT+3TC+EFV, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3f - DRV/r+AZT+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.15 3f - DRV/r+AZT+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.15'                      AS S_NO,
           '3f - DRV/r+AZT+3TC+EFV, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3f - DRV/r+AZT+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)

-- 20.3.16 3d - Other Adult 3rd line regimen, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.16'                AS S_NO,
           '3d - Other Adult 3rd line regimen, Male' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('3a -  DRV/r+DTG+AZT+3TC', '3b - DRV/r+DTG+TDF+3TC', '3c - DRV/r+ABC+3TC+DTG',
                          '3e - DRV/r+TDF+3TC+EFV', '3f - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 20.3.17 3d - Other Adult 3rd line regimen, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.17'                             AS S_NO,
           '3d - Other Adult 3rd line regimen, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('3a -  DRV/r+DTG+AZT+3TC', '3b - DRV/r+DTG+TDF+3TC', '3c - DRV/r+ABC+3TC+DTG',
                          '3e - DRV/r+TDF+3TC+EFV', '3f - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.18 3d - Other Adult 3rd line regimen, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U20.3.18'                                 AS S_NO,
           '3d - Other Adult 3rd line regimen, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_curr_with_client
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('3a -  DRV/r+DTG+AZT+3TC', '3b - DRV/r+DTG+TDF+3TC', '3c - DRV/r+ABC+3TC+DTG',
                          '3e - DRV/r+TDF+3TC+EFV', '3f - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null);
END //

DELIMITER ;




